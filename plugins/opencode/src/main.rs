use std::{collections::BTreeMap, fs, net::SocketAddr, path::PathBuf};

use fleetd_acp_host::{DriverConfig, DriverError, PluginDefinition, RuntimeConfig, serve};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

const PLUGIN_ID: &str = "fleetd.harness.opencode";
const OPENCODE_POLICY_VERSION: u32 = 2;
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
    #[serde(default)]
    openai_compatible: Option<LoopbackOpenAiCompatible>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LoopbackOpenAiCompatible {
    provider_id: String,
    provider_name: String,
    base_url: String,
    model_id: String,
    model_name: String,
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
    let mut opencode_config = json!({
        "model": config.model,
        "permission": {"task": "deny"}
    });
    if let Some(provider) = &config.openai_compatible {
        let mut providers = Map::new();
        providers.insert(
            provider.provider_id.clone(),
            json!({
                "npm": "@ai-sdk/openai-compatible",
                "name": provider.provider_name,
                "options": {"baseURL": provider.base_url},
                "models": {
                    provider.model_id.clone(): {"name": provider.model_name}
                }
            }),
        );
        opencode_config["provider"] = Value::Object(providers);
    }
    let mut environment = BTreeMap::from([
        (
            "HOME".to_owned(),
            config.home.to_string_lossy().into_owned(),
        ),
        (
            "OPENCODE_CONFIG_CONTENT".to_owned(),
            opencode_config.to_string(),
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
    validate_bounded("model", &config.model, 1_024)?;
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
    if let Some(provider) = &config.openai_compatible {
        validate_provider_identifier("provider_id", &provider.provider_id)?;
        validate_bounded("provider_name", &provider.provider_name, 128)?;
        validate_bounded("model_id", &provider.model_id, 512)?;
        validate_bounded("model_name", &provider.model_name, 256)?;
        validate_loopback_base_url(&provider.base_url)?;
        if config.model != format!("{}/{}", provider.provider_id, provider.model_id) {
            return Err(DriverError::InvalidConfig(
                "OpenCode model route must exactly match openai_compatible provider_id/model_id"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_provider_identifier(field: &str, value: &str) -> Result<(), DriverError> {
    validate_bounded(field, value, 128)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(DriverError::InvalidConfig(format!(
            "OpenCode {field} contains unsupported characters"
        )));
    }
    Ok(())
}

fn validate_bounded(field: &str, value: &str, limit: usize) -> Result<(), DriverError> {
    if value.trim().is_empty() || value.len() > limit || value.chars().any(char::is_control) {
        return Err(DriverError::InvalidConfig(format!(
            "OpenCode {field} must contain between 1 and {limit} bytes"
        )));
    }
    Ok(())
}

fn validate_loopback_base_url(value: &str) -> Result<(), DriverError> {
    validate_bounded("base_url", value, 2_048)?;
    let Some(rest) = value.strip_prefix("http://") else {
        return Err(DriverError::InvalidConfig(
            "OpenCode compatible base_url must use loopback HTTP".to_owned(),
        ));
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let address: SocketAddr = authority.parse().map_err(|_| {
        DriverError::InvalidConfig(
            "OpenCode compatible base_url must contain an explicit loopback IP and port".to_owned(),
        )
    })?;
    if !address.ip().is_loopback()
        || path.contains('?')
        || path.contains('#')
        || value.contains('@')
    {
        return Err(DriverError::InvalidConfig(
            "OpenCode compatible base_url must be a credential-free loopback HTTP URL".to_owned(),
        ));
    }
    Ok(())
}

fn profile_digest(config: &OpenCodeConfig, executable: &PathBuf) -> Result<String, DriverError> {
    let executable_bytes = fs::read(executable)?;
    let executable_digest = format!("sha256:{:x}", Sha256::digest(executable_bytes));
    let material = json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "policy_version": OPENCODE_POLICY_VERSION,
        "executable": executable,
        "executable_digest": executable_digest,
        "expected_version": config.expected_version,
        "model": config.model,
        "home": config.home,
        "path": config.path,
        "term": config.term,
        "tmpdir": config.tmpdir,
        "openai_compatible": config.openai_compatible,
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
            r#"{"model":"zai-coding-plan/glm-5.3","permission":{"task":"deny"}}"#
        );
    }

    #[test]
    fn model_route_changes_the_effective_profile() {
        let first = prepare_config(value("zai-coding-plan/glm-5.3")).expect("first config");
        let second = prepare_config(value("minimax/minimax-m2.5")).expect("second config");

        assert_ne!(first.profile_digest, second.profile_digest);
    }

    #[test]
    fn owns_a_typed_credential_free_loopback_provider() {
        let mut input = value("fleet-local//models/qwen-27b");
        input["openai_compatible"] = json!({
            "provider_id": "fleet-local",
            "provider_name": "Fleet local inference",
            "base_url": "http://127.0.0.1:18082/v1",
            "model_id": "/models/qwen-27b",
            "model_name": "Qwen 27B"
        });

        let prepared = prepare_config(input).expect("valid loopback provider");
        let effective: serde_json::Value =
            serde_json::from_str(&prepared.runtime.environment["OPENCODE_CONFIG_CONTENT"])
                .expect("effective OpenCode config");

        assert_eq!(effective["model"], "fleet-local//models/qwen-27b");
        assert_eq!(
            effective["provider"]["fleet-local"]["options"]["baseURL"],
            "http://127.0.0.1:18082/v1"
        );
        assert_eq!(
            effective["provider"]["fleet-local"]["npm"],
            "@ai-sdk/openai-compatible"
        );
        assert_eq!(effective["permission"]["task"], "deny");
    }

    #[test]
    fn rejects_remote_or_mismatched_compatible_providers() {
        let mut remote = value("fleet-local/qwen");
        remote["openai_compatible"] = json!({
            "provider_id": "fleet-local",
            "provider_name": "remote",
            "base_url": "https://models.example/v1",
            "model_id": "qwen",
            "model_name": "Qwen"
        });
        assert!(prepare_config(remote).is_err());

        let mut mismatch = value("fleet-local/other");
        mismatch["openai_compatible"] = json!({
            "provider_id": "fleet-local",
            "provider_name": "local",
            "base_url": "http://127.0.0.1:18082/v1",
            "model_id": "qwen",
            "model_name": "Qwen"
        });
        assert!(prepare_config(mismatch).is_err());
    }

    #[test]
    fn rejects_credential_fields() {
        let mut input = value("zai-coding-plan/glm-5.3");
        input["api_key"] = json!("must-not-cross-plugin-config");

        let error = prepare_config(input).expect_err("unknown credential field must fail");
        assert!(error.to_string().contains("unknown field"));
    }
}
