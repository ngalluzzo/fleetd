//! Typed host-side client for `fleetd.inference-openai@0.1.0`.

use std::net::SocketAddr;

pub use fleetd_proto::inference_openai::{
    BackendIdentity, DescribeResult, Endpoint, ModelRoute, ObserverEndpoint, interface,
};

use crate::{PluginError, PluginManifest, PluginProcess, ShutdownOutcome};

const MAX_IDENTITY_BYTES: usize = 2_048;

/// One initialized backend plugin and the model server process it owns.
pub struct InferenceOpenAiClient {
    process: PluginProcess,
}

impl InferenceOpenAiClient {
    pub(crate) fn new(process: PluginProcess) -> Result<Self, PluginError> {
        let required = interface();
        if !process.manifest().interfaces.contains(&required) {
            return Err(PluginError::MissingInterface {
                interface: required.to_string(),
            });
        }
        Ok(Self { process })
    }

    #[must_use]
    pub const fn manifest(&self) -> &PluginManifest {
        self.process.manifest()
    }

    #[must_use]
    pub fn process_id(&self) -> Option<u32> {
        self.process.process_id()
    }

    /// Reads and validates the exact route supplied to a harness plugin.
    ///
    /// # Errors
    ///
    /// Returns an error for transport failure, malformed identity, or a route
    /// that is not a credential-free explicit loopback HTTP endpoint.
    pub async fn describe(&self) -> Result<DescribeResult, PluginError> {
        let result: DescribeResult = self
            .process
            .protocol_call("inference.openai.describe", &serde_json::json!({}))
            .await?;
        validate_bounded("backend name", &result.backend.name)?;
        validate_bounded("backend version", &result.backend.version)?;
        validate_digest(
            "backend executable digest",
            &result.backend.executable_digest,
        )?;
        validate_bounded("model ID", &result.endpoint.model.id)?;
        validate_bounded("model name", &result.endpoint.model.name)?;
        if let Some(revision) = &result.endpoint.model.revision {
            validate_bounded("model revision", revision)?;
        }
        validate_digest("backend profile digest", &result.profile_digest)?;
        validate_loopback_url("inference base URL", &result.endpoint.base_url)?;
        if let Some(observer) = &result.observer {
            validate_loopback_url("inference observer URL", &observer.url)?;
            validate_bounded("inference observer media type", &observer.media_type)?;
        }
        Ok(result)
    }

    /// Performs the lifecycle and backend readiness probe.
    ///
    /// # Errors
    ///
    /// Returns an error when the plugin transport fails or the backend reports
    /// that its configured route is unavailable.
    pub async fn health(&mut self) -> Result<(), PluginError> {
        self.process.health().await
    }

    /// Stops the backend plugin and its model-server process group.
    ///
    /// # Errors
    ///
    /// Returns an error when graceful shutdown or forced process cleanup
    /// cannot be completed.
    pub async fn shutdown(self) -> Result<ShutdownOutcome, PluginError> {
        self.process.shutdown().await
    }
}

fn validate_bounded(label: &str, value: &str) -> Result<(), PluginError> {
    if value.trim().is_empty()
        || value.len() > MAX_IDENTITY_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(protocol(format!(
            "{label} must contain between 1 and {MAX_IDENTITY_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_digest(label: &str, value: &str) -> Result<(), PluginError> {
    let digest = value
        .strip_prefix("sha256:")
        .ok_or_else(|| protocol(format!("{label} must use a lowercase sha256 digest")))?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(protocol(format!(
            "{label} must use a 64-character sha256 digest"
        )));
    }
    Ok(())
}

fn validate_loopback_url(label: &str, value: &str) -> Result<(), PluginError> {
    if value.len() > MAX_IDENTITY_BYTES || value.contains('@') {
        return Err(protocol(format!(
            "{label} must be a credential-free loopback HTTP URL"
        )));
    }
    let rest = value.strip_prefix("http://").ok_or_else(|| {
        protocol(format!(
            "{label} must be a credential-free loopback HTTP URL"
        ))
    })?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let address: SocketAddr = authority.parse().map_err(|_| {
        protocol(format!(
            "{label} must contain an explicit loopback IP and port"
        ))
    })?;
    if !address.ip().is_loopback() || path.contains('?') || path.contains('#') {
        return Err(protocol(format!(
            "{label} must be a credential-free loopback HTTP URL"
        )));
    }
    Ok(())
}

fn protocol(message: String) -> PluginError {
    PluginError::Protocol(message)
}

#[cfg(test)]
mod tests {
    use super::validate_loopback_url;

    #[test]
    fn inference_endpoints_are_explicit_loopback_urls() {
        validate_loopback_url("endpoint", "http://127.0.0.1:8080/v1").expect("loopback endpoint");
        assert!(validate_loopback_url("endpoint", "https://models.example/v1").is_err());
        assert!(validate_loopback_url("endpoint", "http://user@127.0.0.1:8080/v1").is_err());
    }
}
