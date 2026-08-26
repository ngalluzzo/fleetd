//! Channel lifecycle, conversation discovery, and membership.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use fleetd_conversation as conversation;
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa_axum::{router::OpenApiRouter, routes};

use fleetd_kernel::{auth::Principal, error::ErrorResponse};
use fleetd_proto::model::{AddMember, CreateChannel, OpenDirectConversation, RenameChannel};

use super::{
    AppState,
    error::ApiError,
    guard::{require_channel_access, require_operator},
};

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(create_channel, list_channels))
        .routes(routes!(list_conversations, open_direct_conversation))
        .routes(routes!(rename_channel, archive_channel))
        .routes(routes!(add_member, list_channel_members))
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
        (status = 201, description = "Channel created", body = fleetd_proto::model::Channel),
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
) -> Result<(StatusCode, Json<fleetd_proto::model::Channel>), ApiError> {
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
        (status = 200, description = "Channels", body = [fleetd_proto::model::Channel]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_channels(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<fleetd_proto::model::Channel>>, ApiError> {
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
        (status = 200, description = "Conversation summaries", body = [fleetd_proto::model::ConversationSummary]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_conversations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<ConversationQuery>,
) -> Result<Json<Vec<fleetd_proto::model::ConversationSummary>>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        conversation::list(&state.store, query.include_archived).await?,
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
        (status = 200, description = "Existing exact-pair conversation", body = fleetd_proto::model::ConversationSummary),
        (status = 201, description = "Direct conversation created", body = fleetd_proto::model::ConversationSummary),
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
) -> Result<(StatusCode, Json<fleetd_proto::model::ConversationSummary>), ApiError> {
    require_operator(&principal)?;
    // The substrate opens the pair; presenting it is a read model above the
    // substrate. Composing them here is what keeps the kernel unaware of it.
    let (channel, created) = state.store.open_direct_pair(input).await?;
    let conversation = conversation::summary(&state.store, &channel.id).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(conversation)))
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
        (status = 200, description = "Renamed channel", body = fleetd_proto::model::Channel),
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
) -> Result<Json<fleetd_proto::model::Channel>, ApiError> {
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
        (status = 200, description = "Archived channel", body = fleetd_proto::model::Channel),
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
) -> Result<Json<fleetd_proto::model::Channel>, ApiError> {
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
) -> Result<StatusCode, ApiError> {
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
        (status = 200, description = "Exact channel memberships", body = [fleetd_proto::model::ChannelMember]),
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
) -> Result<Json<Vec<fleetd_proto::model::ChannelMember>>, ApiError> {
    require_channel_access(&state, &principal, &channel_id).await?;
    Ok(Json(state.store.list_channel_members(&channel_id).await?))
}
