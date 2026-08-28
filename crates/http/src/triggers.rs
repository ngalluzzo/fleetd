//! Inbound trigger registration and firing.
//!
//! Two audiences, and the split between them is the authority model. An
//! operator registers, reads, retires, and rotates; a trigger does exactly one
//! thing, with a credential that can reach nothing else.
//!
//! There is no route here for a trigger to read anything. That is
//! [ADR 0031](../../../docs/adr/0031-inbound-triggers.md)'s no-back-channel rule
//! as a surface: a trigger that can see results is a workflow engine, and
//! workflow belongs outside the daemon.

use axum::{
    Extension, Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use utoipa::IntoParams;
use utoipa_axum::{router::OpenApiRouter, routes};

use fleetd_execution::trigger::fire;
use fleetd_kernel::{auth::Principal, error::ErrorResponse};
use fleetd_proto::{
    model::IssuedCredential,
    trigger::{
        RegisterTrigger, RegisteredTrigger, RetireTrigger, Trigger, TriggerFired, TriggerOccurrence,
    },
};

use super::{
    AppState,
    error::ApiError,
    guard::{require_bound_trigger, require_operator},
};

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(register_trigger))
        .routes(routes!(list_triggers))
        .routes(routes!(get_trigger))
        .routes(routes!(retire_trigger))
        .routes(routes!(rotate_trigger_credential))
        .routes(routes!(fire_trigger))
}

#[derive(Debug, Deserialize, IntoParams)]
pub(super) struct ListTriggersQuery {
    /// Restrict the listing to triggers registered against one channel.
    channel_id: Option<String>,
}

#[utoipa::path(
    post,
    path = "/v1/triggers",
    operation_id = "registerTrigger",
    tag = "triggers",
    summary = "Register an inbound trigger",
    description = "Operator-only. Declares what a trigger may create and returns its credential token exactly once.",
    security(("bearerAuth" = [])),
    request_body = RegisterTrigger,
    responses(
        (status = 201, description = "Trigger registered", body = RegisteredTrigger),
        (status = 400, description = "Invalid declaration", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Channel or sender not found", body = ErrorResponse),
        (status = 409, description = "Trigger name conflicts with existing state", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn register_trigger(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Json(input): Json<RegisterTrigger>,
) -> Result<(StatusCode, Json<RegisteredTrigger>), ApiError> {
    require_operator(&principal)?;
    let registered = state.auth.register_trigger(input).await?;
    Ok((StatusCode::CREATED, Json(registered)))
}

#[utoipa::path(
    get,
    path = "/v1/triggers",
    operation_id = "listTriggers",
    tag = "triggers",
    summary = "List inbound triggers",
    description = "Operator-only. Each registration carries when it last created work, so a trigger that stopped firing is a fact rather than an absence.",
    security(("bearerAuth" = [])),
    params(ListTriggersQuery),
    responses(
        (status = 200, description = "Registered triggers", body = Vec<Trigger>),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn list_triggers(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Query(query): Query<ListTriggersQuery>,
) -> Result<Json<Vec<Trigger>>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        state.store.list_triggers(query.channel_id.as_deref()).await?,
    ))
}

#[utoipa::path(
    get,
    path = "/v1/triggers/{trigger_id}",
    operation_id = "getTrigger",
    tag = "triggers",
    summary = "Read one inbound trigger",
    security(("bearerAuth" = [])),
    params(("trigger_id" = String, Path, description = "Stable trigger ID")),
    responses(
        (status = 200, description = "Trigger registration", body = Trigger),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Trigger not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn get_trigger(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(trigger_id): Path<String>,
) -> Result<Json<Trigger>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(state.store.get_trigger(&trigger_id).await?))
}

#[utoipa::path(
    post,
    path = "/v1/triggers/{trigger_id}/retire",
    operation_id = "retireTrigger",
    tag = "triggers",
    summary = "End a trigger's standing grant",
    description = "Operator-only. Revokes every credential that could fire the trigger and keeps the registration as a record. Retiring an already-retired trigger reports it unchanged.",
    security(("bearerAuth" = [])),
    params(("trigger_id" = String, Path, description = "Stable trigger ID")),
    request_body = RetireTrigger,
    responses(
        (status = 200, description = "Retired trigger", body = Trigger),
        (status = 400, description = "Invalid retirement reason", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Trigger not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn retire_trigger(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(trigger_id): Path<String>,
    Json(input): Json<RetireTrigger>,
) -> Result<Json<Trigger>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        state.auth.retire_trigger(&trigger_id, &input.reason).await?,
    ))
}

#[utoipa::path(
    post,
    path = "/v1/triggers/{trigger_id}/credentials/rotate",
    operation_id = "rotateTriggerCredential",
    tag = "triggers",
    summary = "Rotate a trigger credential",
    description = "Operator-only. Immediately revokes earlier credentials and returns the replacement token exactly once. A retired trigger has no replacement to issue.",
    security(("bearerAuth" = [])),
    params(("trigger_id" = String, Path, description = "Stable trigger ID")),
    responses(
        (status = 200, description = "Replacement credential", body = IssuedCredential),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Operator credential required", body = ErrorResponse),
        (status = 404, description = "Trigger not found", body = ErrorResponse),
        (status = 409, description = "Trigger is retired", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn rotate_trigger_credential(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(trigger_id): Path<String>,
) -> Result<Json<IssuedCredential>, ApiError> {
    require_operator(&principal)?;
    Ok(Json(
        state.auth.rotate_trigger_credential(&trigger_id).await?,
    ))
}

#[utoipa::path(
    post,
    path = "/v1/triggers/{trigger_id}/occurrences",
    operation_id = "fireTrigger",
    tag = "triggers",
    summary = "Report that a trigger fired",
    description = "The trigger's own credential only. Sender, channel, correlation, causation, and the durable idempotency key come from the registration; repeating an occurrence identifier is absorbed exactly and reports `created: false`.",
    security(("bearerAuth" = [])),
    params(("trigger_id" = String, Path, description = "Stable trigger ID")),
    request_body = TriggerOccurrence,
    responses(
        (status = 200, description = "Occurrence accepted", body = TriggerFired),
        (status = 400, description = "Invalid occurrence", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Credential is bound to another trigger, or the kind was never declared", body = ErrorResponse),
        (status = 404, description = "Trigger not found", body = ErrorResponse),
        (status = 409, description = "Trigger is retired", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn fire_trigger(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(trigger_id): Path<String>,
    Json(occurrence): Json<TriggerOccurrence>,
) -> Result<Json<TriggerFired>, ApiError> {
    require_bound_trigger(&principal, &trigger_id)?;
    Ok(Json(fire(&state.store, &trigger_id, occurrence).await?))
}
