//! Operator read models for worker and harness evidence.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa_axum::{router::OpenApiRouter, routes};

use fleetd_kernel::{auth::Principal, error::ErrorResponse};

use super::{AppState, error::ApiError, guard::require_operator};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct AgentQuery {
    /// Limit results to one agent ID.
    agent: Option<String>,
}

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(list_agent_seats))
        .routes(routes!(list_plugin_generations))
        .routes(routes!(list_invocation_observations))
        .routes(routes!(list_session_bindings))
        .routes(routes!(trace_invocation))
        .routes(routes!(read_fleet_health))
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct FleetHealthQuery {
    /// Limit the report to one agent ID.
    agent: Option<String>,
    /// Bound how many delivery rows the census reads.
    #[serde(default = "default_census_limit")]
    #[param(default = 500, minimum = 1, maximum = 500)]
    delivery_limit: u32,
}

const fn default_census_limit() -> u32 {
    500
}

#[utoipa::path(
    get,
    path = "/v1/invocations/{invocation_id}/trace",
    operation_id = "traceInvocation",
    tag = "operations",
    summary = "Trace one managed invocation",
    description = "Operator-only. Joins the exact source and result envelopes to bounded invocation, native-session, and plugin-generation evidence.",
    security(("bearerAuth" = [])),
    params(("invocation_id" = String, Path, description = "Stable invocation ID")),
    responses(
        (status = 200, description = "Exact durable invocation trace", body = fleetd_execution::operations::InvocationTrace),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Invocation not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn trace_invocation(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(invocation_id): Path<String>,
) -> Result<Json<fleetd_execution::operations::InvocationTrace>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        fleetd_execution::operations::invocation_trace(&state.store, &invocation_id).await?,
    ))
}

#[utoipa::path(
    get,
    path = "/v1/fleet-health",
    operation_id = "readFleetHealth",
    tag = "operations",
    summary = "Read what the fleet is doing now",
    description = "Operator-only. One durable read reporting the current plugin generation per agent, the current generation of each session binding, the invocations still owed an outcome, and a delivery census including leases whose window has closed.",
    security(("bearerAuth" = [])),
    params(FleetHealthQuery),
    responses(
        (status = 200, description = "Bounded fleet health report", body = fleetd_execution::health::FleetHealth),
        (status = 400, description = "Invalid census bounds", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn read_fleet_health(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<FleetHealthQuery>,
) -> Result<Json<fleetd_execution::health::FleetHealth>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        fleetd_execution::health::fleet_health(
            &state.store,
            query.agent.as_deref(),
            query.delivery_limit,
        )
        .await?,
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
    params(AgentQuery),
    responses(
        (status = 200, description = "Plugin generation evidence", body = [fleetd_execution::operations::PluginGeneration]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_plugin_generations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<AgentQuery>,
) -> Result<Json<Vec<fleetd_execution::operations::PluginGeneration>>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        fleetd_execution::operations::list_plugin_generations(&state.store, query.agent.as_deref())
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
    params(AgentQuery),
    responses(
        (status = 200, description = "Bounded invocation observations", body = [fleetd_execution::operations::InvocationObservation]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_invocation_observations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<AgentQuery>,
) -> Result<Json<Vec<fleetd_execution::operations::InvocationObservation>>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        fleetd_execution::operations::list_invocation_observations(
            &state.store,
            query.agent.as_deref(),
        )
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
    params(AgentQuery),
    responses(
        (status = 200, description = "Durable session binding records", body = [fleetd_execution::session_binding::SessionBinding]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_session_bindings(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<AgentQuery>,
) -> Result<Json<Vec<fleetd_execution::session_binding::SessionBinding>>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        fleetd_execution::session_binding::list_session_bindings(
            &state.store,
            query.agent.as_deref(),
        )
        .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/v1/agent-seats",
    operation_id = "listAgentSeats",
    tag = "operations",
    summary = "List current agent-seat state",
    description = "Operator-only. Projects each stable agent identity as unmanaged, idle, working, interrupted, recovery-required, or offline from exact durable generation, session, invocation, progress, and delivery evidence. Lease and fence credentials are never returned.",
    security(("bearerAuth" = [])),
    params(AgentQuery),
    responses(
        (status = 200, description = "Current credential-free agent-seat projections", body = [fleetd_execution::operations::AgentSeat]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_agent_seats(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<AgentQuery>,
) -> Result<Json<Vec<fleetd_execution::operations::AgentSeat>>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        fleetd_execution::operations::list_agent_seats(&state.store, query.agent.as_deref()).await?,
    ))
}
