//! Experimental lifecycle contract for one OpenAI-compatible inference route.
//!
//! This interface describes mechanism only: a ready loopback endpoint, the
//! exact backend process that owns it, and the model route exposed there. It
//! makes no claim about model quality, skills, roles, or suitable work.

use semver::Version;
use serde::{Deserialize, Serialize};

use crate::plugin::PluginInterface;

pub const INTERFACE_ID: &str = "fleetd.inference-openai";
pub const INTERFACE_VERSION: &str = "0.1.0";

/// Returns the exact experimental interface required by a host.
///
/// # Panics
///
/// Panics only if this module's static semantic-version constant is invalid.
#[must_use]
pub fn interface() -> PluginInterface {
    PluginInterface::new(
        INTERFACE_ID,
        Version::parse(INTERFACE_VERSION).expect("static interface version is valid"),
    )
}

/// Exact process identity observed before a backend was admitted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackendIdentity {
    pub name: String,
    pub version: String,
    pub executable_digest: String,
}

/// One route exposed by the backend.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRoute {
    pub id: String,
    pub name: String,
    /// Exact model revision when the backend can establish one. Missing is not
    /// an assertion that the configured route is content-addressed.
    pub revision: Option<String>,
}

/// Credential-free loopback endpoint consumed by a harness plugin.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub base_url: String,
    pub model: ModelRoute,
}

/// Optional provider-native observation endpoint.
///
/// The host may expose this to an external collector, but Fleetd assigns no
/// shared meaning to its fields.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverEndpoint {
    pub url: String,
    pub media_type: String,
}

/// Result of `inference.openai.describe`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeResult {
    pub backend: BackendIdentity,
    pub endpoint: Endpoint,
    pub profile_digest: String,
    pub observer: Option<ObserverEndpoint>,
}
