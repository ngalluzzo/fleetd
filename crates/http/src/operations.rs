//! Operator read models for worker and harness evidence.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
};
use serde::Deserialize;
use utoipa::{IntoParams, OpenApi};
use utoipa_axum::{router::OpenApiRouter, routes};

use fleetd_execution::operations::{EvidenceCursor, EvidencePage};
use fleetd_kernel::{auth::Principal, error::ErrorResponse, error::FleetError};

use super::{AppState, error::ApiError, guard::require_operator};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct AgentQuery {
    /// Limit results to one agent ID.
    agent: Option<String>,
}

/// The schema the evidence page query contributes to the contract.
///
/// `EvidenceOrder` is only ever a query parameter, so no route body mentions
/// it and nothing registers it implicitly. It is declared here, beside the
/// routes that accept it, rather than in the module that composes the
/// contract.
#[derive(OpenApi)]
#[openapi(components(schemas(fleetd_execution::operations::EvidenceOrder)))]
struct Schemas;

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(Schemas::openapi())
        .routes(routes!(list_agent_seats))
        .routes(routes!(list_plugin_generations))
        .routes(routes!(list_invocation_observations))
        .routes(routes!(list_session_bindings))
        .routes(routes!(trace_invocation))
        .routes(routes!(read_fleet_health))
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct EvidencePageQuery {
    /// Limit results to one agent ID.
    agent: Option<String>,
    /// Exclusive change-clock cursor. Must be supplied with `after_id`.
    #[param(minimum = 0)]
    after_ms: Option<i64>,
    /// Exclusive row-ID tiebreak. Must be supplied with `after_ms`.
    after_id: Option<String>,
    /// Requested page size. Values above 500 are clamped to 500.
    #[param(default = 500, minimum = 1, maximum = 500)]
    #[serde(default = "default_evidence_limit")]
    limit: u32,
    /// Report only rows whose evidence can never change again.
    #[param(default = false)]
    #[serde(default)]
    settled: bool,
    /// Direction the change clock is walked. Walk `oldest` to archive.
    #[param(default = "newest")]
    #[serde(default)]
    order: fleetd_execution::operations::EvidenceOrder,
}

const fn default_evidence_limit() -> u32 {
    500
}

impl EvidencePageQuery {
    /// Resolves the request into one exact page, rejecting a half cursor.
    ///
    /// A cursor is two halves and addresses nothing without both. Reading a
    /// half cursor as "start from the beginning" would silently rewind a
    /// collector that dropped one parameter, so it fails instead.
    fn page(&self) -> Result<(Option<EvidenceCursor>, Option<String>), ApiError> {
        let cursor = match (self.after_ms, self.after_id.as_deref()) {
            (Some(changed_at_ms), Some(id)) => Some(EvidenceCursor {
                changed_at_ms,
                id: id.to_owned(),
            }),
            (None, None) => None,
            _ => {
                return Err(ApiError::from(FleetError::Invalid(
                    "after_ms and after_id must be supplied together".to_owned(),
                )));
            }
        };
        Ok((cursor, self.agent.clone()))
    }
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
    description = "Operator-only. Reports exact ready-generation identity, liveness, profile, runtime, and shutdown evidence. Ordered by `last_heartbeat_at_ms`, the clock every durable change to a generation advances and which freezes once one is stopped. A collector walks `order=oldest` and resumes from the last row's `last_heartbeat_at_ms` and `id`.",
    security(("bearerAuth" = [])),
    params(EvidencePageQuery),
    responses(
        (status = 200, description = "One page of plugin generation evidence", body = [fleetd_execution::operations::PluginGeneration]),
        (status = 400, description = "Invalid cursor or page bounds", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_plugin_generations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<EvidencePageQuery>,
) -> Result<Json<Vec<fleetd_execution::operations::PluginGeneration>>, ApiError> {
    require_operator(&principal)?;
    let (cursor, agent) = query.page()?;
    Ok(Json(
        fleetd_execution::operations::list_plugin_generations(
            &state.store,
            &EvidencePage {
                agent_id: agent.as_deref(),
                after: cursor.as_ref(),
                limit: query.limit,
                settled: query.settled,
                order: query.order,
            },
        )
        .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/v1/invocation-observations",
    operation_id = "listInvocationObservations",
    tag = "operations",
    summary = "List bounded managed-turn observations",
    description = "Operator-only. Reports event counts, chain digests, terminal state, and usage without duplicating raw transcripts. Ordered by `updated_at_ms`, the clock every folded event advances and which freezes once an invocation is terminal. A collector walks `order=oldest` and resumes from the last row's `updated_at_ms` and `invocation_id`; `settled=true` reports only terminal rows, whose evidence never changes again.",
    security(("bearerAuth" = [])),
    params(EvidencePageQuery),
    responses(
        (status = 200, description = "One page of bounded invocation observations", body = [fleetd_execution::operations::InvocationObservation]),
        (status = 400, description = "Invalid cursor or page bounds", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_invocation_observations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<EvidencePageQuery>,
) -> Result<Json<Vec<fleetd_execution::operations::InvocationObservation>>, ApiError> {
    require_operator(&principal)?;
    let (cursor, agent) = query.page()?;
    Ok(Json(
        fleetd_execution::operations::list_invocation_observations(
            &state.store,
            &EvidencePage {
                agent_id: agent.as_deref(),
                after: cursor.as_ref(),
                limit: query.limit,
                settled: query.settled,
                order: query.order,
            },
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
