//! Operator-owned desired execution for stable agent identities.

use axum::{Extension, Json, extract::{Path, State}};
use utoipa_axum::{router::OpenApiRouter, routes};

use fleetd_kernel::{auth::Principal, error::ErrorResponse};
use fleetd_proto::operations::{AgentSeatConfiguration, ConfigureAgentSeat};

use super::{AppState, error::ApiError, guard::require_operator};

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::new()
        .routes(routes!(list_agent_seat_configurations))
        .routes(routes!(configure_agent_seat))
        .routes(routes!(restart_agent_seat))
}

#[utoipa::path(
    get,
    path = "/v1/agent-seat-configurations",
    operation_id = "listAgentSeatConfigurations",
    tag = "seats",
    summary = "List desired agent execution",
    description = "Operator-only. Lists profile references, standing instructions, desired state, and restart revisions. Executables, arguments, tool grants, environment, and harness credentials are private to the machine-local profile catalog and never cross this boundary.",
    security(("bearerAuth" = [])),
    responses(
        (status = 200, description = "Durable desired execution", body = [AgentSeatConfiguration]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_agent_seat_configurations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
) -> Result<Json<Vec<AgentSeatConfiguration>>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(fleetd_execution::seat_configuration::list(&state.store).await?))
}

#[utoipa::path(
    put,
    path = "/v1/agents/{agent_id}/seat-configuration",
    operation_id = "configureAgentSeat",
    tag = "seats",
    summary = "Configure desired execution for an agent",
    description = "Operator-only. Selects one machine-approved runtime profile and standing instructions, then starts or stops the stable agent identity. An exact replay is idempotent; an actual change advances its restart revision.",
    security(("bearerAuth" = [])),
    params(("agent_id" = String, Path, description = "Stable agent ID")),
    request_body = ConfigureAgentSeat,
    responses(
        (status = 200, description = "Current durable configuration", body = AgentSeatConfiguration),
        (status = 400, description = "Invalid profile or instructions", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Agent not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn configure_agent_seat(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(agent_id): Path<String>,
    Json(request): Json<ConfigureAgentSeat>,
) -> Result<Json<AgentSeatConfiguration>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        fleetd_execution::seat_configuration::configure(&state.store, &agent_id, &request).await?,
    ))
}

#[utoipa::path(
    post,
    path = "/v1/agents/{agent_id}/seat-restart",
    operation_id = "restartAgentSeat",
    tag = "seats",
    summary = "Restart a running agent",
    description = "Operator-only. Advances the durable revision of a running seat so the machine-local supervisor replaces its runtime. It does not expose or change executable details.",
    security(("bearerAuth" = [])),
    params(("agent_id" = String, Path, description = "Stable agent ID")),
    responses(
        (status = 200, description = "Configuration with advanced revision", body = AgentSeatConfiguration),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Seat configuration not found", body = ErrorResponse),
        (status = 409, description = "Stopped seats cannot be restarted", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn restart_agent_seat(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(agent_id): Path<String>,
) -> Result<Json<AgentSeatConfiguration>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        fleetd_execution::seat_configuration::restart(&state.store, &agent_id).await?,
    ))
}
