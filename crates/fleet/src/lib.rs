//! Where a local fleet lives on this machine.
//!
//! One JSON file names the listen address, the database, and the operator
//! token file; every relative path in it resolves against the file's own
//! directory, so a fleet directory can be moved or copied whole.
//!
//! This is deliberately not part of any surface. `fleetd init` creates a
//! fleet, `fleetd serve` reads one, and a second surface -- a desktop app, a
//! packaging script -- finds the same fleet by reading the same file, rather
//! than reimplementing the layout.

use std::{
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use fleetd_kernel::{auth::AuthService, store::Store};
use serde::{Deserialize, Serialize};

/// The default location of a fleet configuration, relative to the working
/// directory.
pub const DEFAULT_CONFIG_PATH: &str = ".fleetd/config.json";

const SCHEMA_VERSION: u32 = 1;

/// What can go wrong finding, reading, or creating a local fleet.
#[derive(Debug, thiserror::Error)]
pub enum FleetConfigError {
    #[error(
        "fleetd cannot listen beyond loopback until authenticated transport is configured: {0}"
    )]
    NotLoopback(SocketAddr),
    #[error("unsupported fleet configuration schema version {found}; expected {SCHEMA_VERSION}")]
    UnsupportedSchema { found: u32 },
    #[error("fleet configuration server must start with http:// or https://, found {0}")]
    ServerScheme(String),
    #[error("fleet configuration {path} is invalid: {source}")]
    Invalid {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error("fleet configuration already exists at {0}; refusing to overwrite it")]
    AlreadyExists(PathBuf),
    #[error("could not read or write fleet configuration {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(transparent)]
    Fleet(#[from] fleetd_kernel::error::FleetError),
}

/// One local fleet's configuration, exactly as it is written to disk.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FleetConfig {
    pub schema_version: u32,
    pub server: String,
    pub listen: SocketAddr,
    pub database: PathBuf,
    pub operator_token_file: PathBuf,
}

/// The same configuration with every path made absolute against the
/// configuration file's own directory.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedFleet {
    pub server: String,
    pub listen: SocketAddr,
    pub database: PathBuf,
    pub operator_token_file: PathBuf,
}

impl Default for FleetConfig {
    fn default() -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            server: "http://127.0.0.1:7419".to_owned(),
            listen: SocketAddr::from(([127, 0, 0, 1], 7419)),
            database: PathBuf::from("fleetd.db"),
            operator_token_file: PathBuf::from("operator.token"),
        }
    }
}

impl FleetConfig {
    /// Validates the configuration and makes its paths absolute.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported schema version, a listen address
    /// beyond loopback, or a server without an http/https scheme.
    pub fn resolve(&self, config_path: &Path) -> Result<ResolvedFleet, FleetConfigError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(FleetConfigError::UnsupportedSchema {
                found: self.schema_version,
            });
        }
        validate_listen_address(self.listen)?;
        if !self.server.starts_with("http://") && !self.server.starts_with("https://") {
            return Err(FleetConfigError::ServerScheme(self.server.clone()));
        }
        let base = config_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        Ok(ResolvedFleet {
            server: base_url(&self.server).to_owned(),
            listen: self.listen,
            database: against(base, &self.database),
            operator_token_file: against(base, &self.operator_token_file),
        })
    }
}

/// Reads one fleet configuration, falling back to the defaults when the file
/// does not exist.
///
/// A missing file is not an error: it means an uninitialized directory, whose
/// defaults are the same ones `create` would write.
///
/// # Errors
///
/// Returns an error when the file exists but cannot be read, is not valid
/// JSON for this schema, or does not validate.
pub fn load(config_path: &Path) -> Result<ResolvedFleet, FleetConfigError> {
    if !config_path.exists() {
        return FleetConfig::default().resolve(config_path);
    }
    let raw = std::fs::read(config_path).map_err(|source| FleetConfigError::Io {
        path: config_path.to_owned(),
        source,
    })?;
    let config: FleetConfig =
        serde_json::from_slice(&raw).map_err(|source| FleetConfigError::Invalid {
            path: config_path.to_owned(),
            source,
        })?;
    config.resolve(config_path)
}

/// What creating a fleet produced.
#[derive(Clone, Debug, PartialEq)]
pub struct CreatedFleet {
    pub config_path: PathBuf,
    pub resolved: ResolvedFleet,
    pub operator_token_file: PathBuf,
}

/// Creates one local fleet: its directory, its database, and its operator
/// credential.
///
/// Refuses to overwrite an existing configuration, so running it twice is a
/// clear error rather than a silently reissued credential.
///
/// # Errors
///
/// Returns an error when a configuration already exists, the listen address is
/// not loopback, or the database or credential cannot be created.
pub async fn create(
    config_path: &Path,
    listen: SocketAddr,
) -> Result<CreatedFleet, FleetConfigError> {
    validate_listen_address(listen)?;
    if config_path.exists() {
        return Err(FleetConfigError::AlreadyExists(config_path.to_owned()));
    }
    let config = FleetConfig {
        server: format!("http://{listen}"),
        listen,
        ..FleetConfig::default()
    };
    let resolved = config.resolve(config_path)?;
    if let Some(parent) = resolved.database.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|source| FleetConfigError::Io {
            path: parent.to_owned(),
            source,
        })?;
    }
    let store = Store::open(&resolved.database).await?;
    let bootstrap = AuthService::new(store)
        .ensure_operator_credential(&resolved.operator_token_file)
        .await?;
    persist(config_path, &config)?;
    Ok(CreatedFleet {
        config_path: config_path.to_owned(),
        resolved,
        operator_token_file: bootstrap.token_path,
    })
}

/// Rejects a listen address that is not loopback.
///
/// # Errors
///
/// Returns an error for any address outside loopback.
pub fn validate_listen_address(address: SocketAddr) -> Result<(), FleetConfigError> {
    if address.ip().is_loopback() {
        Ok(())
    } else {
        Err(FleetConfigError::NotLoopback(address))
    }
}

/// Trims a trailing slash so a configured server concatenates with a path.
#[must_use]
pub fn base_url(server: &str) -> &str {
    server.trim_end_matches('/')
}

/// Writes the configuration atomically, refusing to clobber an existing file.
fn persist(path: &Path, config: &FleetConfig) -> Result<(), FleetConfigError> {
    let io = |source: std::io::Error| FleetConfigError::Io {
        path: path.to_owned(),
        source,
    };
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(io)?;
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(io)?;
    let encoded =
        serde_json::to_vec_pretty(config).map_err(|source| FleetConfigError::Invalid {
            path: path.to_owned(),
            source,
        })?;
    temporary.write_all(&encoded).map_err(io)?;
    temporary.write_all(b"\n").map_err(io)?;
    temporary.as_file().sync_all().map_err(io)?;
    temporary
        .persist_noclobber(path)
        .map_err(|error| FleetConfigError::Io {
            path: path.to_owned(),
            source: error.error,
        })?;
    Ok(())
}

fn against(base: &Path, configured: &Path) -> PathBuf {
    if configured.is_absolute() {
        configured.to_owned()
    } else {
        base.join(configured)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_CONFIG_PATH, FleetConfig, FleetConfigError, base_url, load, validate_listen_address,
    };
    use std::{net::SocketAddr, path::Path};

    #[test]
    fn relative_paths_resolve_against_the_configuration_directory() {
        let resolved = FleetConfig::default()
            .resolve(Path::new("/srv/fleet/.fleetd/config.json"))
            .expect("resolve");
        assert_eq!(resolved.database, Path::new("/srv/fleet/.fleetd/fleetd.db"));
        assert_eq!(
            resolved.operator_token_file,
            Path::new("/srv/fleet/.fleetd/operator.token")
        );
    }

    #[test]
    fn an_absolute_path_is_left_alone() {
        let config = FleetConfig {
            database: Path::new("/var/lib/fleetd/fleetd.db").to_owned(),
            ..FleetConfig::default()
        };
        let resolved = config
            .resolve(Path::new("/srv/fleet/.fleetd/config.json"))
            .expect("resolve");
        assert_eq!(resolved.database, Path::new("/var/lib/fleetd/fleetd.db"));
    }

    #[test]
    fn a_bare_configuration_name_resolves_beside_itself() {
        let resolved = FleetConfig::default()
            .resolve(Path::new("config.json"))
            .expect("resolve");
        assert_eq!(resolved.database, Path::new("./fleetd.db"));
    }

    #[test]
    fn a_missing_configuration_reads_as_the_defaults() {
        let resolved = load(Path::new("/nonexistent/fleet/config.json")).expect("load defaults");
        assert_eq!(resolved.listen, FleetConfig::default().listen);
    }

    #[test]
    fn a_listen_address_beyond_loopback_is_refused() {
        let public: SocketAddr = "0.0.0.0:7419".parse().expect("parse");
        assert!(matches!(
            validate_listen_address(public),
            Err(FleetConfigError::NotLoopback(_))
        ));
        let loopback: SocketAddr = "127.0.0.1:7419".parse().expect("parse");
        assert!(validate_listen_address(loopback).is_ok());
        let six: SocketAddr = "[::1]:7419".parse().expect("parse");
        assert!(validate_listen_address(six).is_ok());
    }

    #[test]
    fn a_future_schema_version_is_refused_rather_than_guessed() {
        let config = FleetConfig {
            schema_version: 2,
            ..FleetConfig::default()
        };
        assert!(matches!(
            config.resolve(Path::new("config.json")),
            Err(FleetConfigError::UnsupportedSchema { found: 2 })
        ));
    }

    #[test]
    fn a_server_without_a_scheme_is_refused() {
        let config = FleetConfig {
            server: "127.0.0.1:7419".to_owned(),
            ..FleetConfig::default()
        };
        assert!(matches!(
            config.resolve(Path::new("config.json")),
            Err(FleetConfigError::ServerScheme(_))
        ));
    }

    #[test]
    fn a_trailing_slash_does_not_survive_into_request_paths() {
        assert_eq!(base_url("http://127.0.0.1:7419/"), "http://127.0.0.1:7419");
        assert_eq!(base_url("http://127.0.0.1:7419"), "http://127.0.0.1:7419");
        let config = FleetConfig {
            server: "http://127.0.0.1:7419/".to_owned(),
            ..FleetConfig::default()
        };
        let resolved = config.resolve(Path::new("config.json")).expect("resolve");
        assert_eq!(resolved.server, "http://127.0.0.1:7419");
    }

    #[test]
    fn the_default_path_is_inside_a_dot_directory() {
        assert_eq!(DEFAULT_CONFIG_PATH, ".fleetd/config.json");
    }
}
