//! Plugin lifecycle manifest types.
//!
//! An interface identifies a wire protocol a plugin process implements. It says
//! nothing about the semantic work an agent using that process can perform.

use std::collections::BTreeSet;

use semver::Version;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

    /// Checks that this interface identifier is well formed.
    ///
    /// # Errors
    ///
    /// Returns a description of the first violated identifier rule.
    pub fn validate(&self) -> Result<(), String> {
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
    /// Checks manifest shape: identifier rules, name bounds, and a non-empty
    /// set of unique interfaces.
    ///
    /// Lifecycle version and expected-identity negotiation belong to the host
    /// that launched the process, not to the wire type.
    ///
    /// # Errors
    ///
    /// Returns a description of the first violated rule.
    pub fn validate_shape(&self) -> Result<(), String> {
        validate_identifier("plugin", &self.plugin.id)?;
        if self.plugin.name.trim().is_empty() || self.plugin.name.len() > 128 {
            return Err("plugin name must contain between 1 and 128 bytes".to_owned());
        }
        if self.interfaces.is_empty() {
            return Err("plugin must expose at least one operational interface".to_owned());
        }
        let mut seen = BTreeSet::new();
        for interface in &self.interfaces {
            interface.validate()?;
            if !seen.insert(interface.clone()) {
                return Err(format!("duplicate plugin interface {interface}"));
            }
        }
        Ok(())
    }

    /// Returns whether this manifest declares the exact interface.
    #[must_use]
    pub fn declares(&self, interface: &PluginInterface) -> bool {
        self.interfaces.contains(interface)
    }
}

/// An asynchronous plugin event not interpreted by the lifecycle transport.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PluginNotification {
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// Checks one plugin or interface identifier against the lifecycle rules.
///
/// # Errors
///
/// Returns a description of the first violated identifier rule.
pub fn validate_identifier(kind: &str, identifier: &str) -> Result<(), String> {
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
