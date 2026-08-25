use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::supervisor::PluginError;
use crate::gooir::{CapabilityOfferSet, ExactIdentity};

pub const LIFECYCLE_PROTOCOL_VERSION: u32 = 1;

/// Human and machine identity reported by a plugin.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginIdentity {
    pub id: String,
    pub name: String,
    pub version: Version,
}

/// Negotiated lifecycle version, identity, and GOOIR capability offers.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginManifest {
    pub protocol_version: u32,
    pub plugin: PluginIdentity,
    pub capability_offers: CapabilityOfferSet,
}

impl PluginManifest {
    pub(crate) fn validate(
        &self,
        expected_id: &str,
        required: &[ExactIdentity],
    ) -> Result<(), PluginError> {
        if self.protocol_version != LIFECYCLE_PROTOCOL_VERSION {
            return Err(PluginError::ProtocolVersion {
                expected: LIFECYCLE_PROTOCOL_VERSION,
                actual: self.protocol_version,
            });
        }
        if self.plugin.id != expected_id {
            return Err(PluginError::IdentityMismatch {
                expected: expected_id.to_owned(),
                actual: self.plugin.id.clone(),
            });
        }
        validate_identifier("plugin", &self.plugin.id).map_err(PluginError::InvalidManifest)?;
        if self.plugin.name.trim().is_empty() || self.plugin.name.len() > 128 {
            return Err(PluginError::InvalidManifest(
                "plugin name must contain between 1 and 128 bytes".to_owned(),
            ));
        }
        self.capability_offers
            .validate()
            .map_err(|error| PluginError::InvalidManifest(error.to_string()))?;
        for required_capability in required {
            if !self.capability_offers.offers(required_capability) {
                return Err(PluginError::MissingCapability {
                    capability: required_capability.to_string(),
                });
            }
        }
        Ok(())
    }
}

/// An asynchronous plugin event not interpreted by the lifecycle transport.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PluginNotification {
    pub method: String,
    #[serde(default)]
    pub params: Value,
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

pub(crate) fn validate_identifier(kind: &str, identifier: &str) -> Result<(), String> {
    if identifier.is_empty() || identifier.len() > 128 {
        return Err(format!(
            "{kind} identifier must contain between 1 and 128 bytes"
        ));
    }
    let mut previous_was_separator = true;
    let valid = identifier.bytes().all(|byte| match byte {
        b'a'..=b'z' | b'0'..=b'9' => {
            previous_was_separator = false;
            true
        }
        b'.' | b'-' if !previous_was_separator => {
            previous_was_separator = true;
            true
        }
        _ => false,
    }) && !previous_was_separator;
    if !valid {
        return Err(format!(
            "{kind} identifier contains unsupported characters: {identifier}"
        ));
    }
    Ok(())
}
