use std::{collections::BTreeMap, fs, path::PathBuf};

use fleetd_acp_host::{DriverConfig, DriverError, PluginDefinition, RuntimeConfig, serve};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const PLUGIN_ID: &str = "fleetd.harness.codex";
const ALLOWED_ENVIRONMENT: &[&str] =
    &["CODEX_HOME", "HOME", "NO_BROWSER", "PATH", "TERM", "TMPDIR"];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CodexConfig {
    executable: PathBuf,
    expected_version: String,
    home: PathBuf,
    codex_home: PathBuf,
    path: String,
    #[serde(default = "default_no_browser")]
    no_browser: bool,
    #[serde(default)]
    term: Option<String>,
    #[serde(default)]
    tmpdir: Option<PathBuf>,
}

fn default_no_browser() -> bool {
    true
}

#[tokio::main]
async fn main() {
    let definition = PluginDefinition::new(
        PLUGIN_ID,
        "fleetd Codex harness",
        env!("CARGO_PKG_VERSION"),
        ALLOWED_ENVIRONMENT,
        prepare_config,
    );
    if let Err(error) = serve(definition).await {
        eprintln!("fleetd Codex harness failed: {error}");
        std::process::exit(1);
    }
}

fn prepare_config(value: Value) -> Result<DriverConfig, DriverError> {
    let config: CodexConfig = serde_json::from_value(value)?;
    validate_config(&config)?;
    let executable = fs::canonicalize(&config.executable).map_err(|error| {
        DriverError::InvalidConfig(format!(
            "Codex adapter executable could not be resolved at {}: {error}",
            config.executable.display()
        ))
    })?;
    if !executable.is_file() {
        return Err(DriverError::InvalidConfig(format!(
            "Codex adapter executable must be a file: {}",
            executable.display()
        )));
    }
    let profile_digest = profile_digest(&config, &executable)?;
    let mut environment = BTreeMap::from([
        (
            "HOME".to_owned(),
            config.home.to_string_lossy().into_owned(),
        ),
        (
            "CODEX_HOME".to_owned(),
            config.codex_home.to_string_lossy().into_owned(),
        ),
        ("PATH".to_owned(), config.path),
    ]);
    if config.no_browser {
        environment.insert("NO_BROWSER".to_owned(), "1".to_owned());
    }
    if let Some(term) = config.term {
        environment.insert("TERM".to_owned(), term);
    }
    if let Some(tmpdir) = config.tmpdir {
        environment.insert("TMPDIR".to_owned(), tmpdir.to_string_lossy().into_owned());
    }
    Ok(DriverConfig {
        profile_digest,
        runtime: RuntimeConfig {
            expected_name: "Codex".to_owned(),
            expected_version: config.expected_version,
            executable: executable.clone(),
            identity_path: executable,
            args: Vec::new(),
            environment,
        },
    })
}

fn validate_config(config: &CodexConfig) -> Result<(), DriverError> {
    if !config.executable.is_absolute() {
        return Err(DriverError::InvalidConfig(
            "Codex adapter executable must be an absolute path".to_owned(),
        ));
    }
    if config.expected_version.trim().is_empty() {
        return Err(DriverError::InvalidConfig(
            "Codex expected_version must not be empty".to_owned(),
        ));
    }
    for (label, directory) in [("home", &config.home), ("codex_home", &config.codex_home)] {
        if !directory.is_absolute() || !directory.is_dir() {
            return Err(DriverError::InvalidConfig(format!(
                "Codex {label} must be an existing absolute directory: {}",
                directory.display()
            )));
        }
    }
    if config.path.trim().is_empty() {
        return Err(DriverError::InvalidConfig(
            "Codex PATH must not be empty".to_owned(),
        ));
    }
    if let Some(tmpdir) = &config.tmpdir
        && (!tmpdir.is_absolute() || !tmpdir.is_dir())
    {
        return Err(DriverError::InvalidConfig(format!(
            "Codex tmpdir must be an existing absolute directory: {}",
            tmpdir.display()
        )));
    }
    Ok(())
}

fn profile_digest(config: &CodexConfig, executable: &PathBuf) -> Result<String, DriverError> {
    let executable_bytes = fs::read(executable)?;
    let executable_digest = format!("sha256:{:x}", Sha256::digest(executable_bytes));
    let material = json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "executable": executable,
        "executable_digest": executable_digest,
        "expected_version": config.expected_version,
        "home": config.home,
        "codex_home": config.codex_home,
        "path": config.path,
        "no_browser": config.no_browser,
        "term": config.term,
        "tmpdir": config.tmpdir,
    });
    let encoded = serde_json::to_vec(&material)?;
    Ok(format!("sha256:{:x}", Sha256::digest(encoded)))
}

#[cfg(test)]
mod tests {
    use std::env;

    use serde_json::json;

    use super::prepare_config;

    fn value(codex_home: &std::path::Path) -> serde_json::Value {
        json!({
            "executable": env::current_exe().expect("test executable"),
            "expected_version": "1.6.2",
            "home": env::current_dir().expect("current directory"),
            "codex_home": codex_home,
            "path": "/usr/bin:/bin",
            "term": "xterm-256color",
            "tmpdir": env::temp_dir(),
        })
    }

    #[test]
    fn owns_codex_specific_launch_policy() {
        let prepared = prepare_config(value(&env::current_dir().expect("current directory")))
            .expect("valid config");

        assert_eq!(prepared.runtime.expected_name, "Codex");
        assert!(prepared.runtime.args.is_empty());
        assert_eq!(
            prepared.runtime.environment["CODEX_HOME"],
            env::current_dir()
                .expect("current directory")
                .to_string_lossy()
        );
        assert_eq!(prepared.runtime.environment["NO_BROWSER"], "1");
    }

    #[test]
    fn codex_home_changes_the_effective_profile() {
        let first = prepare_config(value(&env::current_dir().expect("current directory")))
            .expect("first config");
        let second = prepare_config(value(&env::temp_dir())).expect("second config");

        assert_ne!(first.profile_digest, second.profile_digest);
    }

    #[test]
    fn rejects_credential_fields() {
        let mut input = value(&env::current_dir().expect("current directory"));
        input["api_key"] = json!("must-not-cross-plugin-config");

        let error = prepare_config(input).expect_err("unknown credential field must fail");
        assert!(error.to_string().contains("unknown field"));
    }
}
