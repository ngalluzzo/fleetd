//! Domain and persistence errors raised by the kernel.
//!
//! How one of these becomes an HTTP response is the HTTP layer's decision; see
//! `api::error::ApiError`.

use thiserror::Error;

pub use fleetd_proto::error::ErrorResponse;

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
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),
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
