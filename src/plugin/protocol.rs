use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

use super::supervisor::PluginError;

pub const LIFECYCLE_PROTOCOL_VERSION: u32 = 1;

/// Human and machine identity reported by a plugin.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginIdentity {
    pub id: String,
    pub name: String,
    pub version: Version,
}

/// One exact operational interface spoken by a plugin process.
///
/// An interface identifies a wire protocol implemented by the process. It says
/// nothing about the semantic work an agent using that process can perform.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct PluginInterface {
    pub id: String,
    pub version: Version,
}

impl PluginInterface {
    /// Creates an exact interface identity.
    #[must_use]
    pub fn new(id: impl Into<String>, version: Version) -> Self {
        Self {
            id: id.into(),
            version,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        validate_identifier("plugin interface", &self.id)
    }
}

impl std::fmt::Display for PluginInterface {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}@{}", self.id, self.version)
    }
}

/// Negotiated lifecycle version, identity, and operational interfaces.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PluginManifest {
    pub protocol_version: u32,
    pub plugin: PluginIdentity,
    pub interfaces: Vec<PluginInterface>,
}

impl PluginManifest {
    pub(crate) fn validate(
        &self,
        expected_id: &str,
        required: &[PluginInterface],
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
        if self.interfaces.is_empty() {
            return Err(PluginError::InvalidManifest(
                "plugin must expose at least one operational interface".to_owned(),
            ));
        }
        let mut seen = BTreeSet::new();
        for interface in &self.interfaces {
            interface.validate().map_err(PluginError::InvalidManifest)?;
            if !seen.insert(interface.clone()) {
                return Err(PluginError::InvalidManifest(format!(
                    "duplicate plugin interface {interface}"
                )));
            }
        }
        for required_interface in required {
            if !seen.contains(required_interface) {
                return Err(PluginError::MissingInterface {
                    interface: required_interface.to_string(),
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
