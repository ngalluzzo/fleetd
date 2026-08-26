use std::future::pending;

use axum::extract::ws::{CloseFrame, Message as WebSocketMessage, WebSocket};
use tokio::{
    sync::broadcast,
    time::{Instant, Interval, MissedTickBehavior, interval_at, timeout},
};

use crate::{
    browser_stream_edge::{
        APPLICATION_FRAME_SEND_DEADLINE, BrowserStreamCursor, BrowserStreamServerFrame,
        CREDENTIAL_REVALIDATION_INTERVAL,
    },
    stream_grant_broker::ActiveStreamSlot,
};
use fleetd_kernel::{
    auth::{AuthService, Principal},
    message_commit_hint::MessageCommitWake,
    store::Store,
};
use fleetd_proto::model::Message;

const REPLAY_PAGE_SIZE: u32 = 500;

/// Principal shape already authorized to read one channel stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AuthorizedStreamPrincipal {
    Operator,
    Agent { viewer_agent_id: String },
}

/// Exact authority and replay position for one channel stream.
///
/// HTTP and WebSocket access checks happen before this value is constructed.
/// The replay/live engine consumes only this already-authorized scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AuthorizedChannelStream {
    channel_id: String,
    after: i64,
    credential_id: String,
    principal: AuthorizedStreamPrincipal,
}

impl AuthorizedChannelStream {
    pub(crate) fn from_principal(channel_id: String, after: i64, principal: &Principal) -> Self {
        let stream_principal = match principal {
            Principal::Operator { .. } => AuthorizedStreamPrincipal::Operator,
            Principal::Agent { agent_id, .. } => AuthorizedStreamPrincipal::Agent {
                viewer_agent_id: agent_id.clone(),
            },
        };
        Self {
            channel_id,
            after,
            credential_id: principal.credential_id().to_owned(),
            principal: stream_principal,
        }
    }

    pub(crate) fn channel_id(&self) -> &str {
        &self.channel_id
    }

    pub(crate) const fn after(&self) -> i64 {
        self.after
    }

    pub(crate) fn credential_id(&self) -> &str {
        &self.credential_id
    }

    pub(crate) fn issuing_principal(&self) -> Principal {
        match &self.principal {
            AuthorizedStreamPrincipal::Operator => Principal::Operator {
                credential_id: self.credential_id.clone(),
            },
            AuthorizedStreamPrincipal::Agent { viewer_agent_id } => Principal::Agent {
                credential_id: self.credential_id.clone(),
                agent_id: viewer_agent_id.clone(),
            },
        }
    }

    pub(crate) fn viewer_agent_id(&self) -> Option<&str> {
        match &self.principal {
            AuthorizedStreamPrincipal::Operator => None,
            AuthorizedStreamPrincipal::Agent { viewer_agent_id } => Some(viewer_agent_id),
        }
    }
}

/// Runs durable replay followed by live continuation for an authorized native
/// channel stream. Native clients continue receiving one raw [`Message`] JSON
/// object per text frame.
pub(crate) async fn run_native_channel_stream(
    socket: WebSocket,
    store: Store,
    receiver: broadcast::Receiver<MessageCommitWake>,
    authorization: AuthorizedChannelStream,
) {
    run_channel_stream(socket, store, receiver, authorization, StreamWire::Native).await;
}

/// Runs the same replay/live center with the tagged browser wire, exact
/// credential revalidation, send deadlines, and active-capacity ownership.
pub(crate) async fn run_browser_channel_stream(
    socket: WebSocket,
    store: Store,
    receiver: broadcast::Receiver<MessageCommitWake>,
    authorization: AuthorizedChannelStream,
    auth: AuthService,
    active_slot: ActiveStreamSlot,
) {
    let principal = authorization.issuing_principal();
    let mut revalidation = interval_at(
        Instant::now() + CREDENTIAL_REVALIDATION_INTERVAL,
        CREDENTIAL_REVALIDATION_INTERVAL,
    );
    revalidation.set_missed_tick_behavior(MissedTickBehavior::Delay);
    run_channel_stream(
        socket,
        store,
        receiver,
        authorization,
        StreamWire::Browser {
            auth,
            principal,
            _active_slot: active_slot,
            revalidation,
        },
    )
    .await;
}

enum StreamWire {
    Native,
    Browser {
        auth: AuthService,
        principal: Principal,
        _active_slot: ActiveStreamSlot,
        revalidation: Interval,
    },
}

#[derive(Clone, Copy)]
enum StreamTermination {
    ClientClosed,
    InvalidClientMessage,
    GrantRejected,
    Internal,
}

async fn run_channel_stream(
    mut socket: WebSocket,
    store: Store,
    mut receiver: broadcast::Receiver<MessageCommitWake>,
    authorization: AuthorizedChannelStream,
    mut wire: StreamWire,
) {
    let mut cursor = authorization.after();
    if let Err(termination) = wire
        .send_ready(&mut socket, authorization.channel_id(), cursor)
        .await
    {
        wire.finish(&mut socket, termination).await;
        return;
    }
    if let Err(termination) = replay(&mut socket, &store, &authorization, &mut cursor, &wire).await
    {
        wire.finish(&mut socket, termination).await;
        return;
    }
    loop {
        let event = tokio::select! {
            incoming = socket.recv() => StreamEvent::Incoming(incoming),
            wake = receiver.recv() => StreamEvent::Wake(wake),
            () = wire.wait_for_revalidation() => StreamEvent::Revalidate,
        };
        let result = match event {
            StreamEvent::Incoming(incoming) => wire.accept_incoming(incoming),
            StreamEvent::Wake(Ok(MessageCommitWake::Committed(message)))
                if message.channel_id == authorization.channel_id() && message.seq > cursor =>
            {
                replay(&mut socket, &store, &authorization, &mut cursor, &wire).await
            }
            StreamEvent::Wake(Ok(MessageCommitWake::Committed(_))) => Ok(()),
            StreamEvent::Wake(
                Ok(MessageCommitWake::External) | Err(broadcast::error::RecvError::Lagged(_)),
            ) => replay(&mut socket, &store, &authorization, &mut cursor, &wire).await,
            StreamEvent::Wake(Err(broadcast::error::RecvError::Closed)) => {
                Err(StreamTermination::Internal)
            }
            StreamEvent::Revalidate => wire.revalidate().await,
        };
        if let Err(termination) = result {
            wire.finish(&mut socket, termination).await;
            return;
        }
    }
}

enum StreamEvent {
    Incoming(Option<Result<WebSocketMessage, axum::Error>>),
    Wake(Result<MessageCommitWake, broadcast::error::RecvError>),
    Revalidate,
}

impl StreamWire {
    async fn send_ready(
        &self,
        socket: &mut WebSocket,
        channel_id: &str,
        after: i64,
    ) -> Result<(), StreamTermination> {
        let Self::Browser { .. } = self else {
            return Ok(());
        };
        let cursor = BrowserStreamCursor::new(after).ok_or(StreamTermination::Internal)?;
        self.send_serialized(socket, &BrowserStreamServerFrame::ready(channel_id, cursor))
            .await
    }

    async fn send_message(
        &self,
        socket: &mut WebSocket,
        message: &Message,
    ) -> Result<(), StreamTermination> {
        self.revalidate().await?;
        match self {
            Self::Native => self.send_serialized(socket, message).await,
            Self::Browser { .. } => {
                self.send_serialized(socket, &BrowserStreamServerFrame::message(message.clone()))
                    .await
            }
        }
    }

    async fn send_serialized<T: serde::Serialize>(
        &self,
        socket: &mut WebSocket,
        value: &T,
    ) -> Result<(), StreamTermination> {
        let serialized = serde_json::to_string(value).map_err(|_| StreamTermination::Internal)?;
        let send = socket.send(WebSocketMessage::Text(serialized.into()));
        match self {
            Self::Native => send.await.map_err(|_| StreamTermination::ClientClosed),
            Self::Browser { .. } => timeout(APPLICATION_FRAME_SEND_DEADLINE, send)
                .await
                .map_err(|_| StreamTermination::ClientClosed)?
                .map_err(|_| StreamTermination::ClientClosed),
        }
    }

    fn accept_incoming(
        &self,
        incoming: Option<Result<WebSocketMessage, axum::Error>>,
    ) -> Result<(), StreamTermination> {
        match (self, incoming) {
            (_, None | Some(Err(_) | Ok(WebSocketMessage::Close(_)))) => {
                Err(StreamTermination::ClientClosed)
            }
            (Self::Native, Some(Ok(_)))
            | (
                Self::Browser { .. },
                Some(Ok(WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_))),
            ) => Ok(()),
            (
                Self::Browser { .. },
                Some(Ok(WebSocketMessage::Text(_) | WebSocketMessage::Binary(_))),
            ) => Err(StreamTermination::InvalidClientMessage),
        }
    }

    async fn wait_for_revalidation(&mut self) {
        match self {
            Self::Native => pending().await,
            Self::Browser { revalidation, .. } => {
                revalidation.tick().await;
            }
        }
    }

    async fn revalidate(&self) -> Result<(), StreamTermination> {
        let Self::Browser {
            auth, principal, ..
        } = self
        else {
            return Ok(());
        };
        match auth.revalidate_principal(principal).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(StreamTermination::GrantRejected),
            Err(_) => Err(StreamTermination::Internal),
        }
    }

    async fn finish(&self, socket: &mut WebSocket, termination: StreamTermination) {
        let Self::Browser { .. } = self else {
            return;
        };
        let (code, reason) = match termination {
            StreamTermination::ClientClosed => return,
            StreamTermination::InvalidClientMessage => (4_400, "invalid_handshake"),
            StreamTermination::GrantRejected => (4_401, "grant_rejected"),
            StreamTermination::Internal => (1_011, "internal_error"),
        };
        let close = socket.send(WebSocketMessage::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        })));
        let _ = timeout(APPLICATION_FRAME_SEND_DEADLINE, close).await;
    }
}

async fn replay(
    socket: &mut WebSocket,
    store: &Store,
    authorization: &AuthorizedChannelStream,
    cursor: &mut i64,
    wire: &StreamWire,
) -> Result<(), StreamTermination> {
    loop {
        let page = store
            .list_messages(
                authorization.channel_id(),
                authorization.viewer_agent_id(),
                *cursor,
                REPLAY_PAGE_SIZE,
            )
            .await
            .map_err(|_| StreamTermination::Internal)?;
        let count = page.messages.len();
        for message in page.messages {
            wire.send_message(socket, &message).await?;
            *cursor = message.seq;
        }
        if count < REPLAY_PAGE_SIZE as usize {
            return Ok(());
        }
    }
}
