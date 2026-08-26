//! Configuration checks and launch material shared by every ACP harness plugin.
//!
//! A plugin owns which fields its configuration has and what they mean. It does
//! not own how an executable is resolved, how a directory is required to exist,
//! or how a launch profile is content-addressed. Those answers have to be the
//! same for every harness, and each plugin keeping its own copy of them is how
//! they drift: two plugins that hash a profile differently disagree about when
//! a profile changed, and a check that is tightened in one is not tightened in
//! the other.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::runtime::DriverError;

/// Rejects invalid harness configuration, naming the harness in every message.
///
/// Binding the name once keeps the wording uniform across plugins and leaves a
/// plugin to supply only its own field labels.
#[derive(Clone, Copy, Debug)]
pub struct ConfigChecks {
    harness: &'static str,
}

impl ConfigChecks {
    /// Binds these checks to one harness's display name, such as `"Codex"`.
    #[must_use]
    pub const fn new(harness: &'static str) -> Self {
        Self { harness }
    }

    /// Requires a path the caller supplied to be absolute.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is relative.
    pub fn absolute(self, label: &str, path: &Path) -> Result<(), DriverError> {
        if path.is_absolute() {
            return Ok(());
        }
        Err(DriverError::InvalidConfig(format!(
            "{} {label} must be an absolute path",
            self.harness
        )))
    }

    /// Requires an existing absolute directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is relative or is not a directory.
    pub fn directory(self, label: &str, path: &Path) -> Result<(), DriverError> {
        if path.is_absolute() && path.is_dir() {
            return Ok(());
        }
        Err(DriverError::InvalidConfig(format!(
            "{} {label} must be an existing absolute directory: {}",
            self.harness,
            path.display()
        )))
    }

    /// Requires a non-empty value once trimmed.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is empty or only whitespace.
    pub fn non_empty(self, label: &str, value: &str) -> Result<(), DriverError> {
        if value.trim().is_empty() {
            return Err(DriverError::InvalidConfig(format!(
                "{} {label} must not be empty",
                self.harness
            )));
        }
        Ok(())
    }

    /// Requires a non-empty, control-free value within a byte bound.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is empty, too long, or holds a control
    /// character.
    pub fn bounded(self, label: &str, value: &str, limit: usize) -> Result<(), DriverError> {
        if value.trim().is_empty() || value.len() > limit || value.chars().any(char::is_control) {
            return Err(DriverError::InvalidConfig(format!(
                "{} {label} must contain between 1 and {limit} bytes",
                self.harness
            )));
        }
        Ok(())
    }

    /// Resolves a configured executable to the canonical file it names.
    ///
    /// # Errors
    ///
    /// Returns an error when the path cannot be canonicalized or does not name
    /// a file.
    pub fn resolved_executable(self, label: &str, path: &Path) -> Result<PathBuf, DriverError> {
        let executable = std::fs::canonicalize(path).map_err(|error| {
            DriverError::InvalidConfig(format!(
                "{} {label} could not be resolved at {}: {error}",
                self.harness,
                path.display()
            ))
        })?;
        if !executable.is_file() {
            return Err(DriverError::InvalidConfig(format!(
                "{} {label} must be a file: {}",
                self.harness,
                executable.display()
            )));
        }
        Ok(executable)
    }
}

/// The environment every ACP harness starts from.
///
/// `HOME` and `PATH` are always set; `TERM` and `TMPDIR` are forwarded only
/// when configured. A plugin adds its own names on top of this, and nothing
/// else is inherited -- the host launches the runtime with exactly this map,
/// and rejects any name outside the plugin's declared allowlist.
#[must_use]
pub fn base_environment(
    home: &Path,
    path: String,
    term: Option<String>,
    tmpdir: Option<&Path>,
) -> BTreeMap<String, String> {
    let mut environment = BTreeMap::from([
        ("HOME".to_owned(), home.to_string_lossy().into_owned()),
        ("PATH".to_owned(), path),
    ]);
    if let Some(term) = term {
        environment.insert("TERM".to_owned(), term);
    }
    if let Some(tmpdir) = tmpdir {
        environment.insert("TMPDIR".to_owned(), tmpdir.to_string_lossy().into_owned());
    }
    environment
}

/// Content-addresses the exact bytes of a file.
///
/// # Errors
///
/// Returns an error when the file cannot be read.
pub fn executable_digest(path: &Path) -> Result<String, DriverError> {
    let bytes = std::fs::read(path)?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

/// Content-addresses one launch profile from the material that distinguishes it.
///
/// The plugin decides what belongs in `material` -- that is its policy. How the
/// material becomes an address is not negotiable, because the daemon compares
/// these strings to decide whether a profile changed.
///
/// # Errors
///
/// Returns an error when the material cannot be encoded.
pub fn profile_digest(material: &Value) -> Result<String, DriverError> {
    let encoded = serde_json::to_vec(material)?;
    Ok(format!("sha256:{:x}", Sha256::digest(encoded)))
}
