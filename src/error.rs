use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE},
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;
use utoipa::ToSchema;

/// Stable JSON envelope returned for fleetd domain errors.
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

/// Errors produced by fleetd's domain and persistence boundary.
#[derive(Debug, Error)]
pub enum FleetError {
    #[error("{entity} not found: {id}")]
    NotFound { entity: &'static str, id: String },
    #[error("agent {agent_id} is not a member of channel {channel_id}")]
    NotMember {
        agent_id: String,
        channel_id: String,
    },
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("authentication required")]
    Unauthorized,
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("lease conflict: {0}")]
    LeaseConflict(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("credential error: {0}")]
    Credential(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl IntoResponse for FleetError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::NotMember { .. } | Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Conflict(_) | Self::LeaseConflict(_) => StatusCode::CONFLICT,
            Self::Database(_)
            | Self::Migration(_)
            | Self::Serialization(_)
            | Self::Credential(_)
            | Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let public_message = match &self {
            Self::Database(_)
            | Self::Migration(_)
            | Self::Serialization(_)
            | Self::Credential(_)
            | Self::Io(_) => {
                tracing::error!(error = %self, "request failed");
                "internal server error".to_owned()
            }
            _ => self.to_string(),
        };
        let body = Json(ErrorResponse {
            error: public_message,
        });
        let mut response = (status, body).into_response();
        if status == StatusCode::UNAUTHORIZED {
            response
                .headers_mut()
                .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        response
    }
}
