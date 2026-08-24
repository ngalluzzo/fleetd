use axum::{
    Extension, Json, Router,
    extract::{
        Path, Query, Request, State, WebSocketUpgrade,
        ws::{Message as WebSocketMessage, WebSocket},
    },
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::broadcast;

use crate::{
    auth::{AuthService, Principal},
    error::FleetError,
    model::{
        AckDelivery, AddMember, ArmInvocation, BlockDelivery, ClaimDeliveries, CompleteInvocation,
        CreateAgent, CreateChannel, CreateMessage, Message, ResolveDeliveryBlock, RetryDelivery,
        SendMessage,
    },
    store::Store,
};

/// Shared dependencies for the HTTP and WebSocket interfaces.
#[derive(Clone)]
pub struct AppState {
    store: Store,
    auth: AuthService,
    messages: broadcast::Sender<Message>,
}

impl AppState {
    /// Creates application state over the supplied durable store.
    #[must_use]
    pub fn new(store: Store) -> Self {
        let (messages, _) = broadcast::channel(1_024);
        Self {
            auth: AuthService::new(store.clone()),
            store,
            messages,
        }
    }
}

/// Builds fleetd's versioned API.
pub fn router(state: AppState) -> Router {
    let protected = Router::new()
        .route("/v1/agents", post(create_agent).get(list_agents))
        .route(
            "/v1/agents/{agent_id}/credentials/rotate",
            post(rotate_agent_credential),
        )
        .route("/v1/channels", post(create_channel).get(list_channels))
        .route("/v1/channels/{channel_id}/members", post(add_member))
        .route(
            "/v1/channels/{channel_id}/messages",
            post(append_message).get(list_messages),
        )
        .route("/v1/channels/{channel_id}/stream", get(stream))
        .route(
            "/v1/agents/{agent_id}/deliveries/claim",
            post(claim_deliveries),
        )
        .route(
            "/v1/agents/{agent_id}/deliveries/{message_id}/ack",
            post(acknowledge_delivery),
        )
        .route(
            "/v1/agents/{agent_id}/deliveries/{message_id}/retry",
            post(retry_delivery),
        )
        .route(
            "/v1/agents/{agent_id}/deliveries/{message_id}/block",
            post(block_delivery),
        )
        .route("/v1/delivery-blocks", get(list_delivery_blocks))
        .route(
            "/v1/delivery-blocks/{block_id}/resolve",
            post(resolve_delivery_block),
        )
        .route(
            "/v1/agents/{agent_id}/invocations/reserve",
            post(reserve_invocations),
        )
        .route(
            "/v1/agents/{agent_id}/invocations/{invocation_id}/arm",
            post(arm_invocation),
        )
        .route(
            "/v1/agents/{agent_id}/invocations/{invocation_id}/complete",
            post(complete_invocation),
        )
        .route("/v1/invocations", get(list_invocations))
        .route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    Router::new()
        .route("/health", get(health))
        .merge(protected)
        .with_state(state)
}

async fn authenticate(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Result<Response, FleetError> {
    let header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(FleetError::Unauthorized)?;
    let token = parse_bearer_token(header).ok_or(FleetError::Unauthorized)?;
    let principal = state.auth.authenticate(token).await?;
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

fn parse_bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some(token)
}

async fn health() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}

async fn create_agent(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(input): Json<CreateAgent>,
) -> Result<(StatusCode, Json<crate::model::RegisteredAgent>), FleetError> {
    require_operator(&principal)?;
    let registration = state.auth.register_agent(input).await?;
    Ok((StatusCode::CREATED, Json(registration)))
}

async fn list_agents(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<crate::model::Agent>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(state.store.list_agents().await?))
}

async fn rotate_agent_credential(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(agent_id): Path<String>,
) -> Result<Json<crate::model::IssuedCredential>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(state.auth.rotate_agent_credential(&agent_id).await?))
}

async fn create_channel(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(input): Json<CreateChannel>,
) -> Result<(StatusCode, Json<crate::model::Channel>), FleetError> {
    require_operator(&principal)?;
    let channel = state.store.create_channel(input).await?;
    Ok((StatusCode::CREATED, Json(channel)))
}

async fn list_channels(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<crate::model::Channel>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(state.store.list_channels().await?))
}

async fn add_member(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Json(input): Json<AddMember>,
) -> Result<StatusCode, FleetError> {
    require_operator(&principal)?;
    state.store.add_member(&channel_id, &input.agent_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn claim_deliveries(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(agent_id): Path<String>,
    Json(input): Json<ClaimDeliveries>,
) -> Result<Json<crate::model::ClaimBatch>, FleetError> {
    require_bound_agent(&principal, &agent_id)?;
    Ok(Json(state.store.claim_deliveries(&agent_id, input).await?))
}

async fn acknowledge_delivery(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((agent_id, message_id)): Path<(String, String)>,
    Json(input): Json<AckDelivery>,
) -> Result<StatusCode, FleetError> {
    require_bound_agent(&principal, &agent_id)?;
    state
        .store
        .acknowledge_delivery(&agent_id, &message_id, &input.lease_token)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn retry_delivery(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((agent_id, message_id)): Path<(String, String)>,
    Json(input): Json<RetryDelivery>,
) -> Result<StatusCode, FleetError> {
    require_bound_agent(&principal, &agent_id)?;
    state
        .store
        .retry_delivery(&agent_id, &message_id, input)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn block_delivery(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((agent_id, message_id)): Path<(String, String)>,
    Json(input): Json<BlockDelivery>,
) -> Result<(StatusCode, Json<crate::model::BlockedDelivery>), FleetError> {
    require_bound_agent(&principal, &agent_id)?;
    let (blocked, created) = state
        .store
        .block_delivery(&agent_id, &message_id, input)
        .await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(blocked)))
}

#[derive(Deserialize)]
struct DeliveryBlockQuery {
    agent: Option<String>,
}

async fn list_delivery_blocks(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<DeliveryBlockQuery>,
) -> Result<Json<Vec<crate::model::BlockedDelivery>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        state
            .store
            .list_blocked_deliveries(query.agent.as_deref())
            .await?,
    ))
}

async fn resolve_delivery_block(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(block_id): Path<i64>,
    Json(input): Json<ResolveDeliveryBlock>,
) -> Result<StatusCode, FleetError> {
    require_operator(&principal)?;
    state.store.resolve_delivery_block(block_id, input).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn reserve_invocations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(agent_id): Path<String>,
    Json(input): Json<ClaimDeliveries>,
) -> Result<Json<crate::model::InvocationBatch>, FleetError> {
    require_bound_agent(&principal, &agent_id)?;
    Ok(Json(
        state.store.reserve_invocations(&agent_id, input).await?,
    ))
}

async fn arm_invocation(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((agent_id, invocation_id)): Path<(String, String)>,
    Json(input): Json<ArmInvocation>,
) -> Result<Json<crate::model::Invocation>, FleetError> {
    require_bound_agent(&principal, &agent_id)?;
    Ok(Json(
        state
            .store
            .arm_invocation(&agent_id, &invocation_id, input)
            .await?,
    ))
}

async fn complete_invocation(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path((agent_id, invocation_id)): Path<(String, String)>,
    Json(input): Json<CompleteInvocation>,
) -> Result<(StatusCode, Json<crate::model::InvocationCompletion>), FleetError> {
    require_bound_agent(&principal, &agent_id)?;
    let (completion, created) = state
        .store
        .complete_invocation(&agent_id, &invocation_id, input)
        .await?;
    if created {
        let _unused = state.messages.send(completion.result.clone());
    }
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(completion)))
}

#[derive(Deserialize)]
struct InvocationQuery {
    agent: Option<String>,
}

async fn list_invocations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<InvocationQuery>,
) -> Result<Json<Vec<crate::model::Invocation>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        state.store.list_invocations(query.agent.as_deref()).await?,
    ))
}

async fn append_message(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Json(input): Json<SendMessage>,
) -> Result<(StatusCode, Json<Message>), FleetError> {
    let input: CreateMessage = input.attributed_to(require_agent(&principal)?);
    let result = state
        .store
        .append_message_idempotent(&channel_id, input)
        .await?;
    if result.created {
        let _unused = state.messages.send(result.message.clone());
    }
    let status = if result.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(result.message)))
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
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<crate::model::MessagePage>, FleetError> {
    require_channel_access(&state, &principal, &channel_id).await?;
    Ok(Json(
        state
            .store
            .list_messages(&channel_id, principal.agent_id(), query.after, query.limit)
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
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Query(query): Query<StreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, FleetError> {
    require_channel_access(&state, &principal, &channel_id).await?;
    state
        .store
        .list_messages(&channel_id, principal.agent_id(), query.after, 1)
        .await?;
    let receiver = state.messages.subscribe();
    let viewer = principal.agent_id().map(str::to_owned);
    Ok(upgrade
        .on_upgrade(move |socket| {
            stream_messages(
                socket,
                state.store,
                receiver,
                channel_id,
                viewer,
                query.after,
            )
        })
        .into_response())
}

fn require_operator(principal: &Principal) -> Result<(), FleetError> {
    if principal.is_operator() {
        return Ok(());
    }
    Err(FleetError::Forbidden(
        "operator credential required".to_owned(),
    ))
}

fn require_agent(principal: &Principal) -> Result<&str, FleetError> {
    principal
        .agent_id()
        .ok_or_else(|| FleetError::Forbidden("agent credential required".to_owned()))
}

fn require_bound_agent(principal: &Principal, expected_agent_id: &str) -> Result<(), FleetError> {
    if require_agent(principal)? == expected_agent_id {
        return Ok(());
    }
    Err(FleetError::Forbidden(
        "credential is bound to another agent".to_owned(),
    ))
}

async fn require_channel_access(
    state: &AppState,
    principal: &Principal,
    channel_id: &str,
) -> Result<(), FleetError> {
    if principal.is_operator() {
        return Ok(());
    }
    let agent_id = require_agent(principal)?;
    if state.store.is_member(channel_id, agent_id).await? {
        return Ok(());
    }
    Err(FleetError::Forbidden(
        "agent is not a member of this channel".to_owned(),
    ))
}

async fn stream_messages(
    mut socket: WebSocket,
    store: Store,
    mut receiver: broadcast::Receiver<Message>,
    channel_id: String,
    viewer: Option<String>,
    mut cursor: i64,
) {
    if replay(
        &mut socket,
        &store,
        &channel_id,
        viewer.as_deref(),
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
                        if message.channel_id == channel_id
                            && message.seq > cursor
                            && message_visible_to(viewer.as_deref(), &message) =>
                    {
                        cursor = message.seq;
                        if send_message(&mut socket, &message).await.is_err() {
                            return;
                        }
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        if replay(
                            &mut socket,
                            &store,
                            &channel_id,
                            viewer.as_deref(),
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
            .list_messages(channel_id, viewer_agent_id, *cursor, 500)
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
