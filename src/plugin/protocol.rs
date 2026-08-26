//! Host-side lifecycle transport payloads and manifest negotiation.
//!
//! The manifest types themselves are wire types owned by [`fleetd_proto`]. What
//! stays here is what only the launching host can decide: which lifecycle
//! version it speaks, which plugin identity it expected, and which interfaces
//! it requires.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use fleetd_proto::plugin::{
    LIFECYCLE_PROTOCOL_VERSION, PluginIdentity, PluginInterface, PluginManifest,
    PluginNotification, validate_identifier,
};

use super::supervisor::PluginError;

/// Negotiates one manifest against the exact expectations of its launcher.
///
/// # Errors
///
/// Returns an error for a lifecycle version mismatch, an unexpected plugin
/// identity, a malformed manifest, or a missing required interface.
pub(crate) fn negotiate(
    manifest: &PluginManifest,
    expected_id: &str,
    required: &[PluginInterface],
) -> Result<(), PluginError> {
    if manifest.protocol_version != LIFECYCLE_PROTOCOL_VERSION {
        return Err(PluginError::ProtocolVersion {
            expected: LIFECYCLE_PROTOCOL_VERSION,
            actual: manifest.protocol_version,
        });
    }
    if manifest.plugin.id != expected_id {
        return Err(PluginError::IdentityMismatch {
            expected: expected_id.to_owned(),
            actual: manifest.plugin.id.clone(),
        });
    }
    manifest
        .validate_shape()
        .map_err(PluginError::InvalidManifest)?;
    for required_interface in required {
        if !manifest.declares(required_interface) {
            return Err(PluginError::MissingInterface {
                interface: required_interface.to_string(),
            });
        }
    }
    Ok(())
}

#[derive(Serialize)]
pub(crate) struct InitializeParams<'a> {
    pub protocol_version: u32,
    pub instance_id: &'a str,
    pub host_version: &'static str,
    pub config: &'a Value,
}

#[derive(Deserialize)]
pub(crate) struct HealthResult {
    pub status: String,
}

#[derive(Deserialize)]
pub(crate) struct ShutdownResult {
    pub accepted: bool,
}
