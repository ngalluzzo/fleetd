//! Operator read models for worker and harness evidence.

use axum::{
    Extension, Json,
    extract::{Query, State},
};
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    auth::Principal,
    error::{ErrorResponse, FleetError},
};

use super::{AppState, guard::require_operator};

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct AgentQuery {
    /// Limit results to one agent ID.
    agent: Option<String>,
}

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(list_plugin_generations))
        .routes(routes!(list_invocation_observations))
        .routes(routes!(list_session_bindings))
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
        (status = 200, description = "Plugin generation evidence", body = [crate::operations::PluginGeneration]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_plugin_generations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<AgentQuery>,
) -> Result<Json<Vec<crate::operations::PluginGeneration>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        crate::operations::list_plugin_generations(&state.store, query.agent.as_deref()).await?,
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
        (status = 200, description = "Bounded invocation observations", body = [crate::operations::InvocationObservation]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_invocation_observations(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<AgentQuery>,
) -> Result<Json<Vec<crate::operations::InvocationObservation>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        crate::operations::list_invocation_observations(&state.store, query.agent.as_deref())
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
        (status = 200, description = "Durable session binding records", body = [crate::session_binding::SessionBinding]),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_session_bindings(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<AgentQuery>,
) -> Result<Json<Vec<crate::session_binding::SessionBinding>>, FleetError> {
    require_operator(&principal)?;
    Ok(Json(
        crate::session_binding::list_session_bindings(&state.store, query.agent.as_deref()).await?,
    ))
}
