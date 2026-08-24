use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message as WebSocketMessage, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast;

use crate::{
    error::FleetError,
    model::{AddMember, CreateAgent, CreateChannel, CreateMessage, Message},
    store::Store,
};

/// Shared dependencies for the HTTP and WebSocket interfaces.
#[derive(Clone)]
pub struct AppState {
    store: Store,
    messages: broadcast::Sender<Message>,
}

impl AppState {
    /// Creates application state over the supplied durable store.
    #[must_use]
    pub fn new(store: Store) -> Self {
        let (messages, _) = broadcast::channel(1_024);
        Self { store, messages }
    }
}

/// Builds fleetd's versioned API.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/agents", post(create_agent).get(list_agents))
        .route("/v1/channels", post(create_channel).get(list_channels))
        .route("/v1/channels/{channel_id}/members", post(add_member))
        .route(
            "/v1/channels/{channel_id}/messages",
            post(append_message).get(list_messages),
        )
        .route("/v1/channels/{channel_id}/stream", get(stream))
        .with_state(state)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn create_agent(
    State(state): State<AppState>,
    Json(input): Json<CreateAgent>,
) -> Result<(StatusCode, Json<crate::model::Agent>), FleetError> {
    let agent = state.store.create_agent(input).await?;
    Ok((StatusCode::CREATED, Json(agent)))
}

async fn list_agents(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::model::Agent>>, FleetError> {
    Ok(Json(state.store.list_agents().await?))
}

async fn create_channel(
    State(state): State<AppState>,
    Json(input): Json<CreateChannel>,
) -> Result<(StatusCode, Json<crate::model::Channel>), FleetError> {
    let channel = state.store.create_channel(input).await?;
    Ok((StatusCode::CREATED, Json(channel)))
}

async fn list_channels(
    State(state): State<AppState>,
) -> Result<Json<Vec<crate::model::Channel>>, FleetError> {
    Ok(Json(state.store.list_channels().await?))
}

async fn add_member(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(input): Json<AddMember>,
) -> Result<StatusCode, FleetError> {
    state.store.add_member(&channel_id, &input.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn append_message(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Json(input): Json<CreateMessage>,
) -> Result<(StatusCode, Json<Message>), FleetError> {
    let message = state.store.append_message(&channel_id, input).await?;
    let _unused = state.messages.send(message.clone());
    Ok((StatusCode::CREATED, Json(message)))
}

#[derive(Deserialize)]
struct PageQuery {
    #[serde(default)]
    after: i64,
    #[serde(default = "default_page_limit")]
    limit: u32,
}

const fn default_page_limit() -> u32 {
    100
}

async fn list_messages(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<crate::model::MessagePage>, FleetError> {
    Ok(Json(
        state
            .store
            .list_messages(&channel_id, query.after, query.limit)
            .await?,
    ))
}

#[derive(Deserialize)]
struct StreamQuery {
    #[serde(default)]
    after: i64,
}

async fn stream(
    State(state): State<AppState>,
    Path(channel_id): Path<String>,
    Query(query): Query<StreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, FleetError> {
    state
        .store
        .list_messages(&channel_id, query.after, 1)
        .await?;
    let receiver = state.messages.subscribe();
    Ok(upgrade
        .on_upgrade(move |socket| {
            stream_messages(socket, state.store, receiver, channel_id, query.after)
        })
        .into_response())
}

async fn stream_messages(
    mut socket: WebSocket,
    store: Store,
    mut receiver: broadcast::Receiver<Message>,
    channel_id: String,
    mut cursor: i64,
) {
    if replay(&mut socket, &store, &channel_id, &mut cursor)
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
                    Ok(message) if message.channel_id == channel_id && message.seq > cursor => {
                        cursor = message.seq;
                        if send_message(&mut socket, &message).await.is_err() {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if replay(&mut socket, &store, &channel_id, &mut cursor).await.is_err() {
                            return;
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        }
    }
}

async fn replay(
    socket: &mut WebSocket,
    store: &Store,
    channel_id: &str,
    cursor: &mut i64,
) -> Result<(), ()> {
    loop {
        let page = store
            .list_messages(channel_id, *cursor, 500)
            .await
            .map_err(|_| ())?;
        let count = page.messages.len();
        for message in page.messages {
            *cursor = message.seq;
            send_message(socket, &message).await?;
        }
        if count < 500 {
            return Ok(());
        }
    }
}

async fn send_message(socket: &mut WebSocket, message: &Message) -> Result<(), ()> {
    let serialized = serde_json::to_string(message).map_err(|_| ())?;
    socket
        .send(WebSocketMessage::Text(serialized.into()))
        .await
        .map_err(|_| ())
}
