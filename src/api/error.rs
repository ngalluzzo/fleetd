//! The HTTP rendering of a domain error.
//!
//! Status codes and response bodies are an HTTP concern, so they live with the
//! HTTP layer rather than with the error the kernel raises. `?` converts on the
//! way out, which keeps handler bodies unchanged.

use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::WWW_AUTHENTICATE},
    response::{IntoResponse, Response},
};

use crate::error::{ErrorResponse, FleetError};

/// One domain error on its way to a client.
#[derive(Debug)]
pub struct ApiError(FleetError);

impl<E: Into<FleetError>> From<E> for ApiError {
    fn from(error: E) -> Self {
        Self(error.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            FleetError::NotFound { .. } => StatusCode::NOT_FOUND,
            FleetError::NotMember { .. } | FleetError::Forbidden(_) => StatusCode::FORBIDDEN,
            FleetError::Invalid(_) => StatusCode::BAD_REQUEST,
            FleetError::Unauthorized => StatusCode::UNAUTHORIZED,
            FleetError::Conflict(_) | FleetError::LeaseConflict(_) => StatusCode::CONFLICT,
            FleetError::ResourceExhausted(_) => StatusCode::TOO_MANY_REQUESTS,
            FleetError::Database(_)
            | FleetError::Migration(_)
            | FleetError::Serialization(_)
            | FleetError::Credential(_)
            | FleetError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let public_message = match &self.0 {
            FleetError::Database(_)
            | FleetError::Migration(_)
            | FleetError::Serialization(_)
            | FleetError::Credential(_)
            | FleetError::Io(_) => {
                tracing::error!(error = %self.0, "request failed");
                "internal server error".to_owned()
            }
            error => error.to_string(),
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
