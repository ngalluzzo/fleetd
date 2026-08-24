use std::{collections::BTreeMap, fs, path::PathBuf};

use fleetd_acp_host::{DriverConfig, DriverError, PluginDefinition, RuntimeConfig, serve};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const PLUGIN_ID: &str = "fleetd.harness.opencode";
const ALLOWED_ENVIRONMENT: &[&str] = &["HOME", "OPENCODE_CONFIG_CONTENT", "PATH", "TERM", "TMPDIR"];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeConfig {
    executable: PathBuf,
    expected_version: String,
    model: String,
    home: PathBuf,
    path: String,
    #[serde(default)]
    term: Option<String>,
    #[serde(default)]
    tmpdir: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let definition = PluginDefinition::new(
        PLUGIN_ID,
        "fleetd OpenCode harness",
        env!("CARGO_PKG_VERSION"),
        ALLOWED_ENVIRONMENT,
        prepare_config,
    );
    if let Err(error) = serve(definition).await {
        eprintln!("fleetd OpenCode harness failed: {error}");
        std::process::exit(1);
    }
}

fn prepare_config(value: Value) -> Result<DriverConfig, DriverError> {
    let config: OpenCodeConfig = serde_json::from_value(value)?;
    validate_config(&config)?;
    let executable = fs::canonicalize(&config.executable).map_err(|error| {
        DriverError::InvalidConfig(format!(
            "OpenCode executable could not be resolved at {}: {error}",
            config.executable.display()
        ))
    })?;
    if !executable.is_file() {
        return Err(DriverError::InvalidConfig(format!(
            "OpenCode executable must be a file: {}",
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
            "OPENCODE_CONFIG_CONTENT".to_owned(),
            json!({"model": config.model}).to_string(),
        ),
        ("PATH".to_owned(), config.path),
    ]);
    if let Some(term) = config.term {
        environment.insert("TERM".to_owned(), term);
    }
    if let Some(tmpdir) = config.tmpdir {
        environment.insert("TMPDIR".to_owned(), tmpdir.to_string_lossy().into_owned());
    }
    Ok(DriverConfig {
        profile_digest,
        runtime: RuntimeConfig {
            expected_name: "OpenCode".to_owned(),
            expected_version: config.expected_version,
            executable: executable.clone(),
            identity_path: executable,
            args: vec!["acp".to_owned()],
            environment,
        },
    })
}

fn validate_config(config: &OpenCodeConfig) -> Result<(), DriverError> {
    if !config.executable.is_absolute() {
        return Err(DriverError::InvalidConfig(
            "OpenCode executable must be an absolute path".to_owned(),
        ));
    }
    if config.expected_version.trim().is_empty() {
        return Err(DriverError::InvalidConfig(
            "OpenCode expected_version must not be empty".to_owned(),
        ));
    }
    let Some((provider, model)) = config.model.split_once('/') else {
        return Err(DriverError::InvalidConfig(
            "OpenCode model must use provider/model form".to_owned(),
        ));
    };
    if provider.trim().is_empty() || model.trim().is_empty() {
        return Err(DriverError::InvalidConfig(
            "OpenCode model must use provider/model form".to_owned(),
        ));
    }
    if !config.home.is_absolute() || !config.home.is_dir() {
        return Err(DriverError::InvalidConfig(format!(
            "OpenCode home must be an existing absolute directory: {}",
            config.home.display()
        )));
    }
    if config.path.trim().is_empty() {
        return Err(DriverError::InvalidConfig(
            "OpenCode PATH must not be empty".to_owned(),
        ));
    }
    if let Some(tmpdir) = &config.tmpdir
        && (!tmpdir.is_absolute() || !tmpdir.is_dir())
    {
        return Err(DriverError::InvalidConfig(format!(
            "OpenCode tmpdir must be an existing absolute directory: {}",
            tmpdir.display()
        )));
    }
    Ok(())
}

fn profile_digest(config: &OpenCodeConfig, executable: &PathBuf) -> Result<String, DriverError> {
    let executable_bytes = fs::read(executable)?;
    let executable_digest = format!("sha256:{:x}", Sha256::digest(executable_bytes));
    let material = json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "executable": executable,
        "executable_digest": executable_digest,
        "expected_version": config.expected_version,
        "model": config.model,
        "home": config.home,
        "path": config.path,
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

    fn value(model: &str) -> serde_json::Value {
        json!({
            "executable": env::current_exe().expect("test executable"),
            "expected_version": "1.4.0",
            "model": model,
            "home": env::current_dir().expect("current directory"),
            "path": "/usr/bin:/bin",
            "term": "xterm-256color",
            "tmpdir": env::temp_dir(),
        })
    }

    #[test]
    fn owns_opencode_specific_launch_policy() {
        let prepared = prepare_config(value("zai-coding-plan/glm-5.3")).expect("valid config");

        assert_eq!(prepared.runtime.expected_name, "OpenCode");
        assert_eq!(prepared.runtime.args, ["acp"]);
        assert_eq!(
            prepared.runtime.environment["OPENCODE_CONFIG_CONTENT"],
            r#"{"model":"zai-coding-plan/glm-5.3"}"#
        );
    }

    #[test]
    fn model_route_changes_the_effective_profile() {
        let first = prepare_config(value("zai-coding-plan/glm-5.3")).expect("first config");
        let second = prepare_config(value("minimax/minimax-m2.5")).expect("second config");

        assert_ne!(first.profile_digest, second.profile_digest);
    }

    #[test]
    fn rejects_credential_fields() {
        let mut input = value("zai-coding-plan/glm-5.3");
        input["api_key"] = json!("must-not-cross-plugin-config");

        let error = prepare_config(input).expect_err("unknown credential field must fail");
        assert!(error.to_string().contains("unknown field"));
    }
}
