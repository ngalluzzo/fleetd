//! Agent identity and credential administration.

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::StatusCode,
};
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{auth::Principal, error::ErrorResponse, model::CreateAgent};

use super::{AppState, error::ApiError, guard::require_operator};

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(create_agent, list_agents))
        .routes(routes!(rotate_agent_credential))
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
) -> Result<(StatusCode, Json<crate::model::RegisteredAgent>), ApiError> {
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
) -> Result<Json<Vec<crate::model::Agent>>, ApiError> {
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
) -> Result<Json<crate::model::IssuedCredential>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(state.auth.rotate_agent_credential(&agent_id).await?))
}
