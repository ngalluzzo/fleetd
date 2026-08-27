use axum::{Extension, Json, extract::State};
use utoipa_axum::{router::OpenApiRouter, routes};
pub(super) fn routes() -> OpenApiRouter<crate::AppState> {
    let mut router = OpenApiRouter::default().routes(routes!(list_agents));
    let components = router
        .get_openapi_mut()
        .components
        .get_or_insert_with(Default::default);
    components
        .add_security_scheme(
            "bearerAuth",
            utoipa::openapi::security::SecurityScheme::Http(
                utoipa::openapi::security::Http::new(
                    utoipa::openapi::security::HttpAuthScheme::Bearer,
                ),
            ),
        );
    router
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
        (
            status = 200,
            description = "Registered agents",
            body = Vec<fleetd_proto::model::Agent>
        ),
        (
            status = 401,
            description = "Missing or invalid credential",
            body = fleetd_kernel::error::ErrorResponse
        ),
        (
            status = 403,
            description = "Operator credential required",
            body = fleetd_kernel::error::ErrorResponse
        ),
        (
            status = 500,
            description = "Internal failure",
            body = fleetd_kernel::error::ErrorResponse
        )
    )
)]
async fn list_agents(
    State(state): State<crate::AppState>,
    Extension(principal): Extension<fleetd_kernel::auth::Principal>,
) -> Result<Json<Vec<fleetd_proto::model::Agent>>, crate::error::ApiError> {
    let output = super::list_agents_operation(&state, &principal).await?;
    Ok(Json(output))
}
