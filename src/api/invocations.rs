//! Crash-safe managed invocation fencing.

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
    model::{ArmInvocation, ClaimDeliveries, CompleteInvocation},
};

use super::{
    AppState,
    guard::{require_bound_agent, require_operator},
};

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(reserve_invocations))
        .routes(routes!(arm_invocation))
        .routes(routes!(complete_invocation))
        .routes(routes!(list_invocations))
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
