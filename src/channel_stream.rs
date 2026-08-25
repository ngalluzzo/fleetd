use axum::extract::ws::{Message as WebSocketMessage, WebSocket};
use tokio::sync::broadcast;

use crate::{auth::Principal, model::Message, store::Store};

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
    mut socket: WebSocket,
    store: Store,
    mut receiver: broadcast::Receiver<Message>,
    authorization: AuthorizedChannelStream,
) {
    let mut cursor = authorization.after();
    if replay(
        &mut socket,
        &store,
        authorization.channel_id(),
        authorization.viewer_agent_id(),
        &mut cursor,
    )
    .await
    .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(WebSocketMessage::Close(_)) | Err(_)) | None => return,
                    Some(Ok(_)) => {}
                }
            }
            message = receiver.recv() => {
                match message {
                    Ok(message)
                        if message.channel_id == authorization.channel_id()
                            && message.seq > cursor
                            && message_visible_to(authorization.viewer_agent_id(), &message) =>
                    {
                        cursor = message.seq;
                        if send_raw_message(&mut socket, &message).await.is_err() {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if replay(
                            &mut socket,
                            &store,
                            authorization.channel_id(),
                            authorization.viewer_agent_id(),
                            &mut cursor,
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

/// Applies read visibility: operators see everything, while a member sees
/// broadcasts plus direct messages they sent or received.
fn message_visible_to(viewer_agent_id: Option<&str>, message: &Message) -> bool {
    let Some(agent_id) = viewer_agent_id else {
        return true;
    };
    message
        .recipient_id
        .as_deref()
        .is_none_or(|recipient| recipient == agent_id)
        || message.sender_id == agent_id
}

async fn replay(
    socket: &mut WebSocket,
    store: &Store,
    channel_id: &str,
    viewer_agent_id: Option<&str>,
    cursor: &mut i64,
) -> Result<(), ()> {
    loop {
        let page = store
            .list_messages(channel_id, viewer_agent_id, *cursor, REPLAY_PAGE_SIZE)
            .await
            .map_err(|_| ())?;
        let count = page.messages.len();
        for message in page.messages {
            *cursor = message.seq;
            send_raw_message(socket, &message).await?;
        }
        if count < REPLAY_PAGE_SIZE as usize {
            return Ok(());
        }
    }
}

async fn send_raw_message(socket: &mut WebSocket, message: &Message) -> Result<(), ()> {
    let serialized = serde_json::to_string(message).map_err(|_| ())?;
    socket
        .send(WebSocketMessage::Text(serialized.into()))
        .await
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn message(sender_id: &str, recipient_id: Option<&str>) -> Message {
        Message {
            seq: 1,
            id: "message-id".to_owned(),
            channel_id: "channel-id".to_owned(),
            sender_id: sender_id.to_owned(),
            recipient_id: recipient_id.map(str::to_owned),
            kind: "unknown/v7".to_owned(),
            payload: json!({"extension": true}),
            correlation_id: None,
            causation_id: None,
            created_at_ms: 1,
        }
    }

    #[test]
    fn principal_relative_visibility_is_exact() {
        let viewer = "viewer";
        assert!(message_visible_to(Some(viewer), &message("sender", None)));
        assert!(message_visible_to(
            Some(viewer),
            &message("sender", Some(viewer))
        ));
        assert!(message_visible_to(
            Some(viewer),
            &message(viewer, Some("recipient"))
        ));
        assert!(!message_visible_to(
            Some(viewer),
            &message("sender", Some("recipient"))
        ));
        assert!(message_visible_to(
            None,
            &message("sender", Some("recipient"))
        ));
    }
}
