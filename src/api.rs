use axum::{
    Extension, Json, Router,
    extract::{
        Path, Query, Request, State, WebSocketUpgrade,
        ws::{CloseFrame, Message as WebSocketMessage, WebSocket},
    },
    http::{
        HeaderMap, StatusCode,
        header::{AUTHORIZATION, CACHE_CONTROL},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use tokio::{sync::broadcast, time::timeout};
use utoipa::{IntoParams, Modify, OpenApi, ToSchema};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    auth::{AuthService, Principal},
    browser_stream_edge::{
        APPLICATION_FRAME_SEND_DEADLINE, BROWSER_STREAM_PROTOCOL, BrowserStreamEdgeState,
        BrowserStreamGrant, BrowserStreamGrantIssueRequest, BrowserStreamGrantIssueResponse,
        BrowserStreamPath, BrowserStreamProtocol, BrowserStreamRedemptionRequest,
        FIRST_FRAME_DEADLINE, MAX_REDEMPTION_FRAME_BYTES,
    },
    channel_stream::{
        AuthorizedChannelStream, run_browser_channel_stream, run_native_channel_stream,
    },
    error::{ErrorResponse, FleetError},
    message_commit_hint::{MessageCommitHintBridge, MessageCommitWake},
    model::{
        AckDelivery, AddMember, ArmInvocation, BlockDelivery, ClaimDeliveries, CompleteInvocation,
        CreateAgent, CreateChannel, CreateMessage, Message, OpenDirectConversation, RenameChannel,
        ResolveDeliveryBlock, RetryDelivery, SendMessage,
    },
    store::{Store, now_ms},
    stream_grant_broker::{StreamGrantBroker, StreamGrantBrokerError},
};

const BEARER_AUTH: &str = "bearerAuth";

#[derive(OpenApi)]
#[openapi(
    info(
        title = "fleetd API",
        version = "1.3.0",
        description = "Versioned control-plane contract for cooperating software agents."
    ),
    tags(
        (name = "system", description = "Process health and API discovery"),
        (name = "agents", description = "Agent identity and credential administration"),
        (name = "channels", description = "Channel membership and durable messaging"),
        (name = "deliveries", description = "Leased agent inbox delivery"),
        (name = "invocations", description = "Crash-safe managed invocation fencing"),
        (name = "operations", description = "Operator-visible worker and harness evidence")
    ),
    modifiers(&SecurityAddon),
    components(schemas(
        crate::browser_stream_edge::BrowserStreamCursor,
        crate::browser_stream_edge::BrowserStreamGrant,
        crate::browser_stream_edge::BrowserStreamGrantIssueRequest,
        crate::browser_stream_edge::BrowserStreamGrantIssueResponse,
        crate::browser_stream_edge::BrowserStreamPath,
        crate::browser_stream_edge::BrowserStreamProtocol,
        crate::browser_stream_edge::BrowserStreamRedemptionMessageType,
        crate::browser_stream_edge::BrowserStreamRedemptionRequest,
        crate::browser_stream_edge::BrowserStreamServerFrame
    ))
)]
struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};

        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme(
                BEARER_AUTH,
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
    }
}

#[derive(Serialize, ToSchema)]
struct HealthResponse {
    status: String,
}

/// Shared dependencies for the HTTP and WebSocket interfaces.
pub struct AppState {
    store: Store,
    auth: AuthService,
    messages: broadcast::Sender<MessageCommitWake>,
    stream_grants: StreamGrantBroker,
    browser_stream: Option<BrowserStreamEdgeState>,
    message_commit_hints: Option<std::sync::Arc<MessageCommitHintBridge>>,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            auth: self.auth.clone(),
            messages: self.messages.clone(),
            stream_grants: self.stream_grants.clone(),
            browser_stream: self.browser_stream.clone(),
            message_commit_hints: self.message_commit_hints.clone(),
        }
    }
}

impl AppState {
    /// Creates application state over the supplied durable store.
    #[must_use]
    pub fn new(store: Store) -> Self {
        let (messages, _) = broadcast::channel(1_024);
        let auth = AuthService::new(store.clone());
        Self {
            stream_grants: StreamGrantBroker::new(auth.clone()),
            auth,
            store,
            messages,
            browser_stream: None,
            message_commit_hints: None,
        }
    }

    /// Enables content-free wakeups for durable message commits made by local
    /// writer processes using the same database.
    ///
    /// # Errors
    ///
    /// Returns an error when the private local datagram cannot be bound or is
    /// already owned by another daemon for the same database.
    pub fn with_external_message_commit_hints(
        mut self,
        database_path: impl AsRef<std::path::Path>,
    ) -> Result<Self, FleetError> {
        self.message_commit_hints = Some(std::sync::Arc::new(MessageCommitHintBridge::bind(
            database_path.as_ref(),
            self.messages.clone(),
        )?));
        Ok(self)
    }

    /// Enables the origin-bound browser stream edge for the exact bound HTTP
    /// listener. Call this only after an ephemeral port has been resolved.
    ///
    /// # Errors
    ///
    /// Returns an error for an unbound or non-loopback address.
    pub fn with_browser_stream_listener(
        mut self,
        listen_address: std::net::SocketAddr,
    ) -> Result<Self, FleetError> {
        self.browser_stream = Some(
            BrowserStreamEdgeState::for_http_listener(listen_address)
                .map_err(|error| FleetError::Invalid(error.to_string()))?,
        );
        Ok(self)
    }

    /// Returns the exact browser origin accepted by this daemon, when enabled.
    #[must_use]
    pub fn browser_origin(&self) -> Option<&str> {
        self.browser_stream
            .as_ref()
            .map(BrowserStreamEdgeState::canonical_origin)
    }
}

/// Builds fleetd's versioned API.
pub fn router(state: AppState) -> Router {
    let protected: Router<AppState> = protected_contract().into();
    let protected =
        protected.route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    let public: Router<AppState> = public_contract().into();
    let browser: Router<AppState> = browser_contract().into();
    public
        .merge(crate::operator_surface::routes())
        .merge(crate::conversation_surface::routes())
        .merge(browser)
        .merge(protected)
        .with_state(state)
}

/// Returns the exact `OpenAPI` document collected from the registered handlers.
#[must_use]
pub fn openapi_document() -> utoipa::openapi::OpenApi {
    public_contract()
        .merge(browser_contract())
        .merge(protected_contract())
        .into_openapi()
}

fn public_contract() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(health))
        .routes(routes!(serve_openapi))
}

fn browser_contract() -> OpenApiRouter<AppState> {
    OpenApiRouter::default().routes(routes!(browser_channel_stream))
}

fn protected_contract() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(create_agent, list_agents))
        .routes(routes!(rotate_agent_credential))
        .routes(routes!(create_channel, list_channels))
        .routes(routes!(list_conversations, open_direct_conversation))
        .routes(routes!(rename_channel, archive_channel))
        .routes(routes!(add_member, list_channel_members))
        .routes(routes!(append_message, list_messages))
        .routes(routes!(issue_browser_stream_grant))
        .routes(routes!(stream))
        .routes(routes!(claim_deliveries))
        .routes(routes!(acknowledge_delivery))
        .routes(routes!(retry_delivery))
        .routes(routes!(block_delivery))
        .routes(routes!(list_delivery_blocks))
        .routes(routes!(resolve_delivery_block))
        .routes(routes!(reserve_invocations))
        .routes(routes!(arm_invocation))
        .routes(routes!(complete_invocation))
        .routes(routes!(list_invocations))
        .routes(routes!(list_plugin_generations))
        .routes(routes!(list_invocation_observations))
        .routes(routes!(list_session_bindings))
}

#[utoipa::path(
    get,
    path = "/openapi.json",
    operation_id = "getOpenApiDocument",
    tag = "system",
    summary = "Read the API contract",
    responses((
        status = 200,
        description = "The fleetd OpenAPI 3.1 document",
        body = serde_json::Value
    ))
)]
async fn serve_openapi() -> Json<utoipa::openapi::OpenApi> {
    Json(openapi_document())
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

#[utoipa::path(
    get,
    path = "/health",
    operation_id = "getHealth",
    tag = "system",
    summary = "Check process health",
    responses((status = 200, description = "fleetd is running", body = HealthResponse))
)]
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_owned(),
    })
}

#[utoipa::path(
    post,
    path = "/v1/agents",
    operation_id = "createAgent",
    tag = "agents",
    summary = "Register an agent",
    description = "Operator-only. Returns the new credential token exactly once.",
    security(("bearerAuth" = [])),
    request_body = CreateAgent,
    responses(
        (status = 201, description = "Agent registered", body = crate::model::RegisteredAgent),
        (status = 400, description = "Invalid registration", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 409, description = "Agent name conflicts with existing state", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn create_agent(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(input): Json<CreateAgent>,
) -> Result<(StatusCode, Json<crate::model::RegisteredAgent>), FleetError> {
    require_operator(&principal)?;
    let registration = state.auth.register_agent(input).await?;
    Ok((StatusCode::CREATED, Json(registration)))
}

#[utoipa::path(
    get,
    path = "/v1/agents",
    operation_id = "listAgents",
    tag = "agents",
    summary = "List agents",
    description = "Operator-only.",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Registered agents", body = [crate::model::Agent]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_agents(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<crate::model::Agent>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(state.store.list_agents().await?))
}

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/credentials/rotate",
    operation_id = "rotateAgentCredential",
    tag = "agents",
    summary = "Rotate an agent credential",
    description = "Operator-only. Immediately revokes earlier credentials and returns the replacement token exactly once.",
    security(("bearerAuth" = [])),
    params(("agent_id" = String, Path, description = "Stable agent ID")),
    responses(
        (status = 200, description = "Replacement credential", body = crate::model::IssuedCredential),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Agent not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn rotate_agent_credential(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(agent_id): Path<String>,
) -> Result<Json<crate::model::IssuedCredential>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(state.auth.rotate_agent_credential(&agent_id).await?))
}

#[utoipa::path(
    post,
    path = "/v1/channels",
    operation_id = "createChannel",
    tag = "channels",
    summary = "Create a channel",
    description = "Operator-only. Initial membership is committed with the channel.",
    security(("bearerAuth" = [])),
    request_body = CreateChannel,
    responses(
        (status = 201, description = "Channel created", body = crate::model::Channel),
        (status = 400, description = "Invalid channel", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Initial member not found", body = ErrorResponse),
        (status = 409, description = "Channel conflicts with existing state", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn create_channel(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(input): Json<CreateChannel>,
) -> Result<(StatusCode, Json<crate::model::Channel>), FleetError> {
    require_operator(&principal)?;
    let channel = state.store.create_channel(input).await?;
    Ok((StatusCode::CREATED, Json(channel)))
}

#[utoipa::path(
    get,
    path = "/v1/channels",
    operation_id = "listChannels",
    tag = "channels",
    summary = "List channels",
    description = "Operator-only.",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Channels", body = [crate::model::Channel]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_channels(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<crate::model::Channel>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(state.store.list_channels().await?))
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct ConversationQuery {
    /// Include archived shared channels. Defaults to false.
    #[serde(default)]
    include_archived: bool,
}

#[utoipa::path(
    get,
    path = "/v1/conversations",
    operation_id = "listConversations",
    tag = "channels",
    summary = "List shared and direct conversations",
    description = "Operator-only. Returns one bounded presentation projection for both conversation kinds. Archived shared channels are omitted unless explicitly requested.",
    security(("bearerAuth" = [])),
    params(ConversationQuery),
    responses(
        (status = 200, description = "Conversation summaries", body = [crate::model::ConversationSummary]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_conversations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<ConversationQuery>,
) -> Result<Json<Vec<crate::model::ConversationSummary>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        state
            .store
            .list_conversations(query.include_archived)
            .await?,
    ))
}

#[utoipa::path(
    post,
    path = "/v1/direct-conversations",
    operation_id = "openDirectConversation",
    tag = "channels",
    summary = "Open a one-to-one direct conversation",
    description = "Operator-only. Exactly two distinct participants are required. The exact unordered pair is created atomically or returned idempotently; participant delivery modes are immutable.",
    security(("bearerAuth" = [])),
    request_body = OpenDirectConversation,
    responses(
        (status = 200, description = "Existing exact-pair conversation", body = crate::model::ConversationSummary),
        (status = 201, description = "Direct conversation created", body = crate::model::ConversationSummary),
        (status = 400, description = "Invalid participant pair", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Participant not found", body = ErrorResponse),
        (status = 409, description = "Existing pair uses different immutable delivery modes", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn open_direct_conversation(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(input): Json<OpenDirectConversation>,
) -> Result<(StatusCode, Json<crate::model::ConversationSummary>), FleetError> {
    require_operator(&principal)?;
    let result = state.store.open_direct_conversation(input).await?;
    let status = if result.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(result.conversation)))
}

#[utoipa::path(
    patch,
    path = "/v1/channels/{channel_id}",
    operation_id = "renameChannel",
    tag = "channels",
    summary = "Rename a shared channel",
    description = "Operator-only. Direct conversation names are derived from their participants; archived channels are immutable.",
    security(("bearerAuth" = [])),
    params(("channel_id" = String, Path, description = "Shared channel ID")),
    request_body = RenameChannel,
    responses(
        (status = 200, description = "Renamed channel", body = crate::model::Channel),
        (status = 400, description = "Invalid channel name", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 409, description = "Direct, archived, or duplicate channel name", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn rename_channel(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Json(input): Json<RenameChannel>,
) -> Result<Json<crate::model::Channel>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        state.store.rename_channel(&channel_id, input.name).await?,
    ))
}

#[utoipa::path(
    post,
    path = "/v1/channels/{channel_id}/archive",
    operation_id = "archiveChannel",
    tag = "channels",
    summary = "Archive a shared channel",
    description = "Operator-only and idempotent. Archive is one-way, retains permanent membership and immutable history, and rejects new messages.",
    security(("bearerAuth" = [])),
    params(("channel_id" = String, Path, description = "Shared channel ID")),
    responses(
        (status = 200, description = "Archived channel", body = crate::model::Channel),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 409, description = "Direct conversations cannot be archived", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn archive_channel(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
) -> Result<Json<crate::model::Channel>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(state.store.archive_channel(&channel_id).await?))
}

#[utoipa::path(
    post,
    path = "/v1/channels/{channel_id}/members",
    operation_id = "addChannelMember",
    tag = "channels",
    summary = "Add a channel member",
    description = "Operator-only. Membership is permanent for the channel lifetime.",
    security(("bearerAuth" = [])),
    params(("channel_id" = String, Path, description = "Channel ID")),
    request_body = AddMember,
    responses(
        (status = 204, description = "Member added or already present"),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Channel or agent not found", body = ErrorResponse),
        (status = 409, description = "Existing membership uses another delivery mode", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn add_member(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Json(input): Json<AddMember>,
) -> Result<StatusCode, FleetError> {
    require_operator(&principal)?;
    state
        .store
        .add_member_with_mode(&channel_id, &input.agent_id, input.delivery_mode)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/v1/channels/{channel_id}/members",
    operation_id = "listChannelMembers",
    tag = "channels",
    summary = "List exact channel memberships",
    description = "Available to the operator or a member of this exact channel. The bounded projection omits opaque agent metadata.",
    security(("bearerAuth" = [])),
    params(("channel_id" = String, Path, description = "Channel ID")),
    responses(
        (status = 200, description = "Exact channel memberships", body = [crate::model::ChannelMember]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Channel membership required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_channel_members(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
) -> Result<Json<Vec<crate::model::ChannelMember>>, FleetError> {
    require_channel_access(&state, &principal, &channel_id).await?;
    Ok(Json(state.store.list_channel_members(&channel_id).await?))
}

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/deliveries/claim",
    operation_id = "claimDeliveries",
    tag = "deliveries",
    summary = "Lease inbox deliveries",
    description = "Requires the credential bound to the path agent. Returns an empty batch when no work is eligible.",
    security(("bearerAuth" = [])),
    params(("agent_id" = String, Path, description = "Agent ID bound to the credential")),
    request_body = ClaimDeliveries,
    responses(
        (status = 200, description = "Leased delivery batch", body = crate::model::ClaimBatch),
        (status = 400, description = "Invalid lease bounds", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Credential is not bound to this agent", body = ErrorResponse),
        (status = 404, description = "Agent not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn claim_deliveries(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(agent_id): Path<String>,
    Json(input): Json<ClaimDeliveries>,
) -> Result<Json<crate::model::ClaimBatch>, FleetError> {
    require_bound_agent(&principal, &agent_id)?;
    Ok(Json(state.store.claim_deliveries(&agent_id, input).await?))
}

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/deliveries/{message_id}/ack",
    operation_id = "acknowledgeDelivery",
    tag = "deliveries",
    summary = "Acknowledge a delivery",
    description = "Requires the bound agent and active lease. Exact settlement replays are idempotent.",
    security(("bearerAuth" = [])),
    params(
        ("agent_id" = String, Path, description = "Agent ID bound to the credential"),
        ("message_id" = String, Path, description = "Delivered message ID")
    ),
    request_body = AckDelivery,
    responses(
        (status = 204, description = "Delivery acknowledged"),
        (status = 400, description = "Invalid lease token", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Credential is not bound to this agent", body = ErrorResponse),
        (status = 404, description = "Delivery not found", body = ErrorResponse),
        (status = 409, description = "Lease is expired, stale, or mismatched", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/deliveries/{message_id}/retry",
    operation_id = "retryDelivery",
    tag = "deliveries",
    summary = "Release a delivery for retry",
    description = "Requires the bound agent and active lease. An armed invocation cannot be retried as ordinary failure.",
    security(("bearerAuth" = [])),
    params(
        ("agent_id" = String, Path, description = "Agent ID bound to the credential"),
        ("message_id" = String, Path, description = "Delivered message ID")
    ),
    request_body = RetryDelivery,
    responses(
        (status = 204, description = "Delivery scheduled for retry"),
        (status = 400, description = "Invalid retry request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Credential is not bound to this agent", body = ErrorResponse),
        (status = 404, description = "Delivery not found", body = ErrorResponse),
        (status = 409, description = "Lease conflict or invocation already armed", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/deliveries/{message_id}/block",
    operation_id = "blockDelivery",
    tag = "deliveries",
    summary = "Park an ambiguously executed delivery",
    description = "Requires the bound agent and active lease. First creation returns 201; an exact replay returns 200.",
    security(("bearerAuth" = [])),
    params(
        ("agent_id" = String, Path, description = "Agent ID bound to the credential"),
        ("message_id" = String, Path, description = "Delivered message ID")
    ),
    request_body = BlockDelivery,
    responses(
        (status = 200, description = "Existing block returned for an exact replay", body = crate::model::BlockedDelivery),
        (status = 201, description = "Delivery blocked", body = crate::model::BlockedDelivery),
        (status = 400, description = "Invalid block evidence", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Credential is not bound to this agent", body = ErrorResponse),
        (status = 404, description = "Delivery not found", body = ErrorResponse),
        (status = 409, description = "Lease conflict or changed replay evidence", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct DeliveryBlockQuery {
    /// Limit results to one agent ID.
    agent: Option<String>,
}

#[utoipa::path(
    get,
    path = "/v1/delivery-blocks",
    operation_id = "listDeliveryBlocks",
    tag = "deliveries",
    summary = "List unresolved delivery blocks",
    description = "Operator-only. Results may be limited to one agent.",
    security(("bearerAuth" = [])),
    params(DeliveryBlockQuery),
    responses(
        (status = 200, description = "Unresolved delivery blocks", body = [crate::model::BlockedDelivery]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/delivery-blocks/{block_id}/resolve",
    operation_id = "resolveDeliveryBlock",
    tag = "deliveries",
    summary = "Resolve a blocked delivery",
    description = "Operator-only. An identical decision is idempotent; a changed second decision conflicts.",
    security(("bearerAuth" = [])),
    params(("block_id" = i64, Path, minimum = 1, description = "Positive delivery block ID")),
    request_body = ResolveDeliveryBlock,
    responses(
        (status = 204, description = "Block resolved"),
        (status = 400, description = "Invalid resolution", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Block not found", body = ErrorResponse),
        (status = 409, description = "Block changed or was resolved differently", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/invocations/reserve",
    operation_id = "reserveInvocations",
    tag = "invocations",
    summary = "Lease and reserve managed invocations",
    description = "Requires the bound agent. Atomically creates one durable invocation fence per leased delivery.",
    security(("bearerAuth" = [])),
    params(("agent_id" = String, Path, description = "Agent ID bound to the credential")),
    request_body = ClaimDeliveries,
    responses(
        (status = 200, description = "Reserved invocation batch", body = crate::model::InvocationBatch),
        (status = 400, description = "Invalid lease bounds", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Credential is not bound to this agent", body = ErrorResponse),
        (status = 404, description = "Agent not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/invocations/{invocation_id}/arm",
    operation_id = "armInvocation",
    tag = "invocations",
    summary = "Arm an invocation for effectful dispatch",
    description = "Requires the bound agent and both active tokens. The durable arm must commit before external dispatch.",
    security(("bearerAuth" = [])),
    params(
        ("agent_id" = String, Path, description = "Agent ID bound to the credential"),
        ("invocation_id" = String, Path, description = "Invocation ID")
    ),
    request_body = ArmInvocation,
    responses(
        (status = 200, description = "Armed invocation", body = crate::model::Invocation),
        (status = 400, description = "Invalid tokens", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Credential is not bound to this agent", body = ErrorResponse),
        (status = 404, description = "Invocation not found", body = ErrorResponse),
        (status = 409, description = "Lease, fence, or invocation state conflict", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/invocations/{invocation_id}/complete",
    operation_id = "completeInvocation",
    tag = "invocations",
    summary = "Publish a result and complete an invocation",
    description = "Requires the bound agent and a live armed invocation. The result, input acknowledgement, and terminal state commit atomically. First completion returns 201; an exact replay returns 200.",
    security(("bearerAuth" = [])),
    params(
        ("agent_id" = String, Path, description = "Agent ID bound to the credential"),
        ("invocation_id" = String, Path, description = "Invocation ID")
    ),
    request_body = CompleteInvocation,
    responses(
        (status = 200, description = "Existing completion returned for an exact replay", body = crate::model::InvocationCompletion),
        (status = 201, description = "Invocation completed", body = crate::model::InvocationCompletion),
        (status = 400, description = "Invalid completion", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Credential is not bound to this agent", body = ErrorResponse),
        (status = 404, description = "Invocation not found", body = ErrorResponse),
        (status = 409, description = "Lease, fence, state, or changed replay conflict", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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
        let _unused = state.messages.send(MessageCommitWake::Committed(Box::new(
            completion.result.clone(),
        )));
    }
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(completion)))
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct InvocationQuery {
    /// Limit results to one agent ID.
    agent: Option<String>,
}

#[utoipa::path(
    get,
    path = "/v1/invocations",
    operation_id = "listInvocations",
    tag = "invocations",
    summary = "List managed invocations",
    description = "Operator-only. Returns the latest durable invocation records, optionally for one agent.",
    security(("bearerAuth" = [])),
    params(InvocationQuery),
    responses(
        (status = 200, description = "Managed invocations", body = [crate::model::Invocation]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/plugin-generations",
    operation_id = "listPluginGenerations",
    tag = "operations",
    summary = "List durable plugin generation evidence",
    description = "Operator-only. Reports exact ready-generation identity, liveness, profile, runtime, and shutdown evidence.",
    security(("bearerAuth" = [])),
    params(InvocationQuery),
    responses(
        (status = 200, description = "Plugin generation evidence", body = [crate::operations::PluginGeneration]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_plugin_generations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<InvocationQuery>,
) -> Result<Json<Vec<crate::operations::PluginGeneration>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        state
            .store
            .list_plugin_generations(query.agent.as_deref())
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/v1/invocation-observations",
    operation_id = "listInvocationObservations",
    tag = "operations",
    summary = "List bounded managed-turn observations",
    description = "Operator-only. Reports event counts, chain digests, terminal state, and usage without duplicating raw transcripts.",
    security(("bearerAuth" = [])),
    params(InvocationQuery),
    responses(
        (status = 200, description = "Bounded invocation observations", body = [crate::operations::InvocationObservation]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_invocation_observations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<InvocationQuery>,
) -> Result<Json<Vec<crate::operations::InvocationObservation>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        state
            .store
            .list_invocation_observations(query.agent.as_deref())
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/v1/session-bindings",
    operation_id = "listSessionBindings",
    tag = "operations",
    summary = "List durable native-session ownership",
    description = "Operator-only. Reports exact binding generations, owner epochs, active invocations, persistence, and uncertainty.",
    security(("bearerAuth" = [])),
    params(InvocationQuery),
    responses(
        (status = 200, description = "Durable session binding records", body = [crate::session_binding::SessionBinding]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_session_bindings(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<InvocationQuery>,
) -> Result<Json<Vec<crate::session_binding::SessionBinding>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        state
            .store
            .list_session_bindings(query.agent.as_deref())
            .await?,
    ))
}

#[utoipa::path(
    post,
    path = "/v1/channels/{channel_id}/messages",
    operation_id = "createChannelMessage",
    tag = "channels",
    summary = "Append a channel message",
    description = "Agent-only. The server derives sender_id from the credential. First idempotent append returns 201; an exact replay returns 200.",
    security(("bearerAuth" = [])),
    params(("channel_id" = String, Path, description = "Channel ID")),
    request_body = SendMessage,
    responses(
        (status = 200, description = "Existing message returned for an exact idempotency replay", body = Message),
        (status = 201, description = "Message appended", body = Message),
        (status = 400, description = "Invalid message", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Agent credential and sender or recipient channel membership required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 409, description = "Idempotency key was reused for different content", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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
        let _unused = state.messages.send(MessageCommitWake::Committed(Box::new(
            result.message.clone(),
        )));
    }
    let status = if result.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(result.message)))
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct PageQuery {
    /// Exclusive global message sequence cursor.
    #[param(minimum = 0, default = 0)]
    #[serde(default)]
    after: i64,
    /// Requested page size. Values above 500 are clamped to 500.
    #[param(minimum = 1, maximum = 500, default = 100)]
    #[serde(default = "default_page_limit")]
    limit: u32,
}

const fn default_page_limit() -> u32 {
    100
}

#[utoipa::path(
    get,
    path = "/v1/channels/{channel_id}/messages",
    operation_id = "listChannelMessages",
    tag = "channels",
    summary = "Read channel history",
    description = "Operators or channel members. Direct-message visibility is filtered to the authenticated member.",
    security(("bearerAuth" = [])),
    params(
        ("channel_id" = String, Path, description = "Channel ID"),
        PageQuery
    ),
    responses(
        (status = 200, description = "Messages strictly after the cursor", body = crate::model::MessagePage),
        (status = 400, description = "Invalid cursor", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Channel membership required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
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

#[utoipa::path(
    post,
    path = "/v1/channels/{channel_id}/stream-grants",
    operation_id = "createBrowserChannelStreamGrant",
    tag = "channels",
    summary = "Mint a single-use browser channel-stream grant",
    description = "Operators or exact channel members. The grant is process-local, expires after 15 seconds, and is returned once with Cache-Control: no-store.",
    security(("bearerAuth" = [])),
    params(("channel_id" = String, Path, description = "Channel ID")),
    request_body = BrowserStreamGrantIssueRequest,
    responses(
        (status = 201, description = "Single-use browser stream grant", body = BrowserStreamGrantIssueResponse,
            headers(("Cache-Control" = String, description = "Always no-store"))
        ),
        (status = 400, description = "Invalid cursor or protocol", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Channel membership required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 429, description = "Unused grant capacity exhausted", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn issue_browser_stream_grant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Json(input): Json<BrowserStreamGrantIssueRequest>,
) -> Result<(StatusCode, HeaderMap, Json<BrowserStreamGrantIssueResponse>), FleetError> {
    require_channel_access(&state, &principal, &channel_id).await?;
    state
        .store
        .list_messages(&channel_id, principal.agent_id(), input.after.get(), 1)
        .await?;
    let authorization =
        AuthorizedChannelStream::from_principal(channel_id, input.after.get(), &principal);
    let issued = state
        .stream_grants
        .issue(authorization, input.protocol.as_str())
        .map_err(|error| map_stream_grant_issue_error(&error))?;
    let (raw_grant, lifetime) = issued.into_parts();
    let grant = BrowserStreamGrant::parse(raw_grant)
        .map_err(|_| FleetError::Credential("generated stream grant was invalid".to_owned()))?;
    let lifetime_ms = i64::try_from(lifetime.as_millis()).unwrap_or(i64::MAX);
    let response = BrowserStreamGrantIssueResponse {
        grant,
        expires_at_ms: now_ms().saturating_add(lifetime_ms),
        websocket_path: BrowserStreamPath::ChannelStream,
        protocol: BrowserStreamProtocol::V1,
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        "no-store".parse().expect("static header value"),
    );
    Ok((StatusCode::CREATED, headers, Json(response)))
}

fn map_stream_grant_issue_error(error: &StreamGrantBrokerError) -> FleetError {
    match error {
        StreamGrantBrokerError::Capacity => {
            FleetError::ResourceExhausted("browser stream grant capacity".to_owned())
        }
        StreamGrantBrokerError::InvalidScope | StreamGrantBrokerError::Rejected => {
            FleetError::Invalid("invalid browser stream grant scope".to_owned())
        }
        StreamGrantBrokerError::Entropy | StreamGrantBrokerError::Revalidation(_) => {
            FleetError::Credential("browser stream grant issuance failed".to_owned())
        }
    }
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct StreamQuery {
    /// Exclusive global message sequence cursor to replay before live delivery.
    #[param(minimum = 0, default = 0)]
    #[serde(default)]
    after: i64,
}

#[utoipa::path(
    get,
    path = "/v1/channels/{channel_id}/stream",
    operation_id = "streamChannelMessages",
    tag = "channels",
    summary = "Replay and stream channel messages",
    description = "WebSocket upgrade for operators or channel members. Each server text frame is one Message JSON object. Reconnect with the highest durably processed seq as `after`. Client frames other than Close are ignored.",
    security(("bearerAuth" = [])),
    params(
        ("channel_id" = String, Path, description = "Channel ID"),
        StreamQuery
    ),
    responses(
        (status = 101, description = "WebSocket protocol switched"),
        (status = 400, description = "Invalid cursor or upgrade request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Channel membership required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    ),
    extensions(
        ("x-fleetd-websocket" = json!({
            "direction": "server-to-client",
            "frameType": "text",
            "messageSchema": { "$ref": "#/components/schemas/Message" },
            "ordering": "ascending seq after replay cursor",
            "clientMessages": "ignored except Close"
        }))
    )
)]
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
    let authorization =
        AuthorizedChannelStream::from_principal(channel_id, query.after, &principal);
    debug_assert_eq!(authorization.credential_id(), principal.credential_id());
    Ok(upgrade
        .on_upgrade(move |socket| {
            run_native_channel_stream(socket, state.store, receiver, authorization)
        })
        .into_response())
}

#[utoipa::path(
    get,
    path = "/v1/browser/channel-stream",
    operation_id = "openBrowserChannelStream",
    tag = "channels",
    summary = "Redeem a browser channel-stream grant",
    description = "Same-origin WebSocket upgrade. No bearer or grant appears in the URI, headers, or subprotocol. The first application frame must redeem the single-use grant.",
    responses(
        (status = 101, description = "Browser WebSocket protocol switched",
            headers(("Sec-WebSocket-Protocol" = String, description = "fleetd.channel-stream.browser.v1"))
        ),
        (status = 400, description = "Invalid WebSocket upgrade", body = ErrorResponse),
        (status = 403, description = "Origin, authority, or protocol rejected", body = ErrorResponse),
        (status = 503, description = "Browser stream edge unavailable or at capacity", body = ErrorResponse)
    ),
    extensions(
        ("x-fleetd-websocket" = json!({
            "direction": "bidirectional-authentication-then-server-to-client",
            "frameType": "text",
            "subprotocol": BROWSER_STREAM_PROTOCOL,
            "firstClientMessageSchema": { "$ref": "#/components/schemas/BrowserStreamRedemptionRequest" },
            "serverMessageSchema": { "$ref": "#/components/schemas/BrowserStreamServerFrame" },
            "ordering": "ready, then ascending message.seq after the grant cursor",
            "clientMessagesAfterRedemption": "unsupported"
        }))
    )
)]
async fn browser_channel_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Some(edge) = &state.browser_stream else {
        return browser_upgrade_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "browser stream edge is not configured",
        );
    };
    if edge.validate_upgrade_headers(&headers).is_err() {
        return browser_upgrade_error(StatusCode::FORBIDDEN, "browser stream upgrade rejected");
    }
    if !state.stream_grants.has_global_active_capacity() {
        return browser_upgrade_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "browser stream capacity exhausted",
        );
    }
    let Some(pre_authentication_slot) = edge.try_acquire_pre_authentication_slot() else {
        return browser_upgrade_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "browser stream capacity exhausted",
        );
    };
    upgrade
        .protocols([BROWSER_STREAM_PROTOCOL])
        .max_message_size(MAX_REDEMPTION_FRAME_BYTES)
        .max_frame_size(MAX_REDEMPTION_FRAME_BYTES)
        .on_upgrade(move |socket| {
            redeem_browser_channel_stream(socket, state, pre_authentication_slot)
        })
        .into_response()
}

fn browser_upgrade_error(status: StatusCode, message: &'static str) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.to_owned(),
        }),
    )
        .into_response()
}

async fn redeem_browser_channel_stream(
    mut socket: WebSocket,
    state: AppState,
    pre_authentication_slot: tokio::sync::OwnedSemaphorePermit,
) {
    let first_frame_deadline = tokio::time::Instant::now() + FIRST_FRAME_DEADLINE;
    let redemption = loop {
        match tokio::time::timeout_at(first_frame_deadline, socket.recv()).await {
            Err(_) => {
                close_browser_socket(&mut socket, 4_408, "grant_timeout").await;
                return;
            }
            Ok(Some(Ok(WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_)))) => {}
            Ok(Some(Ok(WebSocketMessage::Text(text)))) => {
                if let Ok(redemption) =
                    BrowserStreamRedemptionRequest::parse_text_frame(text.as_str())
                {
                    break redemption;
                }
                close_browser_socket(&mut socket, 4_400, "invalid_handshake").await;
                return;
            }
            Ok(_) => {
                close_browser_socket(&mut socket, 4_400, "invalid_handshake").await;
                return;
            }
        }
    };
    let redeemed = match state
        .stream_grants
        .redeem(
            redemption.grant.expose_secret(),
            BrowserStreamProtocol::V1.as_str(),
        )
        .await
    {
        Ok(redeemed) => redeemed,
        Err(StreamGrantBrokerError::Revalidation(_)) => {
            close_browser_socket(&mut socket, 1_011, "internal_error").await;
            return;
        }
        Err(_) => {
            close_browser_socket(&mut socket, 4_401, "grant_rejected").await;
            return;
        }
    };
    let (authorization, active_slot) = redeemed.into_parts();
    let principal = authorization.issuing_principal();
    let access = require_channel_access(&state, &principal, authorization.channel_id()).await;
    if let Err(error) = access {
        let (code, reason) = match error {
            FleetError::Database(_)
            | FleetError::Migration(_)
            | FleetError::Serialization(_)
            | FleetError::Credential(_)
            | FleetError::Io(_) => (1_011, "internal_error"),
            _ => (4_401, "grant_rejected"),
        };
        close_browser_socket(&mut socket, code, reason).await;
        return;
    }
    match state
        .store
        .list_messages(
            authorization.channel_id(),
            authorization.viewer_agent_id(),
            authorization.after(),
            1,
        )
        .await
    {
        Ok(_) => {}
        Err(FleetError::NotFound { .. } | FleetError::Invalid(_)) => {
            close_browser_socket(&mut socket, 4_401, "grant_rejected").await;
            return;
        }
        Err(_) => {
            close_browser_socket(&mut socket, 1_011, "internal_error").await;
            return;
        }
    }
    let receiver = state.messages.subscribe();
    drop(pre_authentication_slot);
    run_browser_channel_stream(
        socket,
        state.store,
        receiver,
        authorization,
        state.auth,
        active_slot,
    )
    .await;
}

async fn close_browser_socket(socket: &mut WebSocket, code: u16, reason: &'static str) {
    let close = socket.send(WebSocketMessage::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    })));
    let _ = timeout(APPLICATION_FRAME_SEND_DEADLINE, close).await;
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
