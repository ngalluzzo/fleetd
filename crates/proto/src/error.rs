//! Stable error envelope shared by every fleetd HTTP consumer.

use serde::Serialize;
use utoipa::ToSchema;

/// Stable JSON envelope returned for fleetd domain errors.
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}
