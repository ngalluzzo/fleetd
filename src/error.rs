use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

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
}

impl IntoResponse for FleetError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::NotFound { .. } => StatusCode::NOT_FOUND,
            Self::NotMember { .. } => StatusCode::FORBIDDEN,
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::Conflict(_) | Self::LeaseConflict(_) => StatusCode::CONFLICT,
            Self::Database(_) | Self::Migration(_) | Self::Serialization(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        let body = Json(json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}
