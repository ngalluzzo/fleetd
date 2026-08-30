//! Shared checks for strict backend-owned launch configuration.

use std::{
    fs::File,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::BackendError;

/// Uniform validation and digest helpers for backend integrations.
#[derive(Clone, Copy, Debug)]
pub struct ConfigChecks {
    backend: &'static str,
}

impl ConfigChecks {
    #[must_use]
    pub const fn new(backend: &'static str) -> Self {
        Self { backend }
    }

    /// Requires a bounded non-control string.
    ///
    /// # Errors
    ///
    /// Returns an error when the value is empty, too large, or contains a
    /// control character.
    pub fn bounded(self, label: &str, value: &str, limit: usize) -> Result<(), BackendError> {
        if value.trim().is_empty() || value.len() > limit || value.chars().any(char::is_control) {
            return Err(BackendError::InvalidConfig(format!(
                "{} {label} must contain between 1 and {limit} bytes",
                self.backend
            )));
        }
        Ok(())
    }

    /// Requires an executable path whose resolved target is an existing file.
    ///
    /// The exact absolute path is retained for launch because Python virtual
    /// environments rely on the invoked symlink path to discover their prefix.
    ///
    /// # Errors
    ///
    /// Returns an error when the path cannot be resolved or does not name a
    /// file.
    pub fn executable(self, label: &str, path: &Path) -> Result<PathBuf, BackendError> {
        if !path.is_absolute() {
            return Err(BackendError::InvalidConfig(format!(
                "{} {label} must be an absolute path: {}",
                self.backend,
                path.display()
            )));
        }
        let target = std::fs::canonicalize(path).map_err(|error| {
            BackendError::InvalidConfig(format!(
                "{} {label} could not be resolved at {}: {error}",
                self.backend,
                path.display()
            ))
        })?;
        if !target.is_file() {
            return Err(BackendError::InvalidConfig(format!(
                "{} {label} must be a file: {}",
                self.backend,
                target.display()
            )));
        }
        Ok(path.to_path_buf())
    }

    /// Requires an existing absolute file.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is relative, does not name a file, or
    /// cannot be resolved.
    pub fn file(self, label: &str, path: &Path) -> Result<PathBuf, BackendError> {
        if !path.is_absolute() || !path.is_file() {
            return Err(BackendError::InvalidConfig(format!(
                "{} {label} must be an existing absolute file: {}",
                self.backend,
                path.display()
            )));
        }
        std::fs::canonicalize(path).map_err(BackendError::Io)
    }

    /// Requires an existing absolute directory.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is relative, does not name a directory,
    /// or cannot be resolved.
    pub fn directory(self, label: &str, path: &Path) -> Result<PathBuf, BackendError> {
        if !path.is_absolute() || !path.is_dir() {
            return Err(BackendError::InvalidConfig(format!(
                "{} {label} must be an existing absolute directory: {}",
                self.backend,
                path.display()
            )));
        }
        std::fs::canonicalize(path).map_err(BackendError::Io)
    }
}

/// Requires one explicit, credential-free loopback HTTP URL.
///
/// # Errors
///
/// Returns an error when the URL is not bounded, explicit loopback HTTP, or
/// contains credentials, a query, or a fragment.
pub fn validate_loopback_url(label: &str, value: &str) -> Result<(), BackendError> {
    if value.len() > 2_048 || value.contains('@') {
        return Err(invalid_loopback(label));
    }
    let rest = value
        .strip_prefix("http://")
        .ok_or_else(|| invalid_loopback(label))?;
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let address: SocketAddr = authority.parse().map_err(|_| invalid_loopback(label))?;
    if !address.ip().is_loopback() || path.contains('?') || path.contains('#') {
        return Err(invalid_loopback(label));
    }
    Ok(())
}

fn invalid_loopback(label: &str) -> BackendError {
    BackendError::InvalidConfig(format!(
        "{label} must be a credential-free explicit loopback HTTP URL"
    ))
}

/// Streams a file into a stable SHA-256 identity.
///
/// # Errors
///
/// Returns an error when the file cannot be opened or read.
pub fn file_digest(path: &Path) -> Result<String, BackendError> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1_024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("sha256:{:x}", digest.finalize()))
}

/// Content-addresses the exact vendor-owned launch material.
///
/// # Errors
///
/// Returns an error when the supplied JSON value cannot be serialized.
pub fn profile_digest(material: &Value) -> Result<String, BackendError> {
    let encoded = serde_json::to_vec(material)?;
    Ok(format!("sha256:{:x}", Sha256::digest(encoded)))
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs::File, os::unix::fs::symlink};

    use tempfile::tempdir;

    use super::ConfigChecks;

    #[test]
    fn executable_validation_preserves_an_approved_symlink_path() {
        let directory = tempdir().expect("temporary directory");
        let target = directory.path().join("python-target");
        let virtual_environment_python = directory.path().join("python");
        File::create(&target).expect("executable target");
        symlink(&target, &virtual_environment_python).expect("virtual environment symlink");

        let executable = ConfigChecks::new("test")
            .executable("Python", &virtual_environment_python)
            .expect("approved symlink");

        assert_eq!(executable, virtual_environment_python);
    }
}
