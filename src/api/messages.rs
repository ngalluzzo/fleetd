//! Durable channel message append and replay.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    auth::Principal,
    error::{ErrorResponse, FleetError},
    message_commit_hint::MessageCommitWake,
    model::{CreateMessage, Message, SendMessage},
};

use super::{
    AppState,
    guard::{require_agent, require_channel_access},
};

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default().routes(routes!(append_message, list_messages))
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
