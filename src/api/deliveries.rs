//! Leased agent inbox delivery and durable blocking.

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
    model::{AckDelivery, BlockDelivery, ClaimDeliveries, ResolveDeliveryBlock, RetryDelivery},
};

use super::{
    AppState,
    guard::{require_bound_agent, require_operator},
};

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(claim_deliveries))
        .routes(routes!(acknowledge_delivery))
        .routes(routes!(retry_delivery))
        .routes(routes!(block_delivery))
        .routes(routes!(list_delivery_blocks))
        .routes(routes!(resolve_delivery_block))
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
