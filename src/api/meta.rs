//! Unauthenticated process health and contract discovery.

use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;
use utoipa_axum::{router::OpenApiRouter, routes};

use super::{AppState, openapi_document};

#[derive(Serialize, ToSchema)]
struct HealthResponse {
    status: String,
}

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(health))
        .routes(routes!(serve_openapi))
}

#[utoipa::path(
    get,
    path = "/health",
    operation_id = "getHealth",
    tag = "system",
    summary = "Check process health",
    responses((status = 200, description = "fleetd is running", body = HealthResponse))
)]
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_owned(),
    })
}

#[utoipa::path(
    get,
    path = "/openapi.json",
    operation_id = "getOpenApiDocument",
    tag = "system",
    summary = "Read the API contract",
    responses((
        status = 200,
        description = "The fleetd OpenAPI 3.1 document",
        body = serde_json::Value
    ))
)]
async fn serve_openapi() -> Json<utoipa::openapi::OpenApi> {
    Json(openapi_document())
}
