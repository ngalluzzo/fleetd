use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use fleetd_acp_host::{
    DriverConfig, DriverError, PluginDefinition, RuntimeConfig,
    config::{ConfigChecks, base_environment, executable_digest, profile_digest as digest_profile},
    serve,
};
use fleetd_proto::inference_openai::DescribeResult as InferenceDescribeResult;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

const PLUGIN_ID: &str = "fleetd.harness.opencode";
const CHECKS: ConfigChecks = ConfigChecks::new("OpenCode");
const OPENCODE_POLICY_VERSION: u32 = 4;
const ALLOWED_ENVIRONMENT: &[&str] = &["HOME", "OPENCODE_CONFIG_CONTENT", "PATH", "TERM", "TMPDIR"];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OpenCodeConfig {
    executable: PathBuf,
    expected_version: String,
    #[serde(default)]
    model: Option<String>,
    home: PathBuf,
    path: String,
    #[serde(default)]
    term: Option<String>,
    #[serde(default)]
    tmpdir: Option<PathBuf>,
    /// OpenAI-compatible reasoning preference applied to every model request.
    #[serde(default)]
    reasoning_effort: Option<ReasoningEffort>,
    #[serde(default)]
    openai_compatible: Option<LoopbackOpenAiCompatible>,
    /// Machine-resolved backend route. The approved-profile supervisor injects
    /// this only after the backend plugin has reached readiness.
    #[serde(default)]
    inference: Option<InferenceDescribeResult>,
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
    Xhigh,
}

impl ReasoningEffort {
    const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::Xhigh => "xhigh",
        }
    }

    const fn enables_reasoning(self) -> bool {
        !matches!(self, Self::None)
    }
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
    let executable = CHECKS.resolved_executable("executable", &config.executable)?;
    let profile_digest = profile_digest(&config, &executable)?;
    let (model, provider) = effective_inference(&config)?;
    let mut opencode_config = json!({
        "model": model,
        "permission": {"task": "deny"}
    });
    if let Some(provider) = provider {
        let mut model = json!({"name": provider.model_name});
        if let Some(effort) = config.reasoning_effort {
            model["reasoning"] = json!(effort.enables_reasoning());
            model["options"] = json!({"reasoningEffort": effort.as_str()});
        }
        let mut providers = Map::new();
        providers.insert(
            provider.provider_id,
            json!({
                "npm": "@ai-sdk/openai-compatible",
                "name": provider.provider_name,
                "options": {"baseURL": provider.base_url},
                "models": {
                    provider.model_id: model
                }
            }),
        );
        opencode_config["provider"] = Value::Object(providers);
    }
    let mut environment = base_environment(
        &config.home,
        config.path,
        config.term,
        config.tmpdir.as_deref(),
    );
    environment.insert(
        "OPENCODE_CONFIG_CONTENT".to_owned(),
        opencode_config.to_string(),
    );
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
    CHECKS.absolute("executable", &config.executable)?;
    CHECKS.non_empty("expected_version", &config.expected_version)?;
    if config.inference.is_some() && config.openai_compatible.is_some() {
        return Err(DriverError::InvalidConfig(
            "OpenCode inference and openai_compatible routes are mutually exclusive".to_owned(),
        ));
    }
    if config.inference.is_some() && config.model.is_some() {
        return Err(DriverError::InvalidConfig(
            "OpenCode model is resolved by inference and must not also be configured".to_owned(),
        ));
    }
    if config.reasoning_effort.is_some()
        && config.inference.is_none()
        && config.openai_compatible.is_none()
    {
        return Err(DriverError::InvalidConfig(
            "OpenCode reasoning_effort requires a resolved or configured OpenAI-compatible provider"
                .to_owned(),
        ));
    }
    if let Some(model) = &config.model {
        validate_model_route(model)?;
    } else if config.inference.is_none() {
        return Err(DriverError::InvalidConfig(
            "OpenCode requires either model or a resolved inference backend".to_owned(),
        ));
    }
    CHECKS.directory("home", &config.home)?;
    CHECKS.non_empty("PATH", &config.path)?;
    if let Some(tmpdir) = &config.tmpdir {
        CHECKS.directory("tmpdir", tmpdir)?;
    }
    if let Some(provider) = &config.openai_compatible {
        validate_provider_identifier("provider_id", &provider.provider_id)?;
        CHECKS.bounded("provider_name", &provider.provider_name, 128)?;
        CHECKS.bounded("model_id", &provider.model_id, 512)?;
        CHECKS.bounded("model_name", &provider.model_name, 256)?;
        validate_loopback_base_url(&provider.base_url)?;
        let expected_model = format!("{}/{}", provider.provider_id, provider.model_id);
        if config.model.as_deref() != Some(expected_model.as_str()) {
            return Err(DriverError::InvalidConfig(
                "OpenCode model route must exactly match openai_compatible provider_id/model_id"
                    .to_owned(),
            ));
        }
    }
    if let Some(inference) = &config.inference {
        CHECKS.bounded("inference backend name", &inference.backend.name, 128)?;
        CHECKS.bounded("inference backend version", &inference.backend.version, 128)?;
        validate_digest(
            "inference backend executable digest",
            &inference.backend.executable_digest,
        )?;
        validate_digest("inference profile digest", &inference.profile_digest)?;
        validate_loopback_base_url(&inference.endpoint.base_url)?;
        CHECKS.bounded("inference model ID", &inference.endpoint.model.id, 512)?;
        CHECKS.bounded("inference model name", &inference.endpoint.model.name, 256)?;
        if let Some(revision) = &inference.endpoint.model.revision {
            CHECKS.bounded("inference model revision", revision, 512)?;
        }
    }
    Ok(())
}

fn validate_model_route(model: &str) -> Result<(), DriverError> {
    CHECKS.bounded("model", model, 1_024)?;
    let Some((provider, model)) = model.split_once('/') else {
        return Err(DriverError::InvalidConfig(
            "OpenCode model must use provider/model form".to_owned(),
        ));
    };
    if provider.trim().is_empty() || model.trim().is_empty() {
        return Err(DriverError::InvalidConfig(
            "OpenCode model must use provider/model form".to_owned(),
        ));
    }
    Ok(())
}

fn validate_digest(label: &str, value: &str) -> Result<(), DriverError> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(DriverError::InvalidConfig(format!(
            "OpenCode {label} must be a SHA-256 digest"
        )));
    };
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DriverError::InvalidConfig(format!(
            "OpenCode {label} must be a SHA-256 digest"
        )));
    }
    Ok(())
}

fn effective_inference(
    config: &OpenCodeConfig,
) -> Result<(String, Option<LoopbackOpenAiCompatible>), DriverError> {
    if let Some(inference) = &config.inference {
        let provider_id = "fleetd-inference".to_owned();
        let model = format!("{provider_id}/{}", inference.endpoint.model.id);
        return Ok((
            model,
            Some(LoopbackOpenAiCompatible {
                provider_id,
                provider_name: format!("Fleetd · {}", inference.backend.name),
                base_url: inference.endpoint.base_url.clone(),
                model_id: inference.endpoint.model.id.clone(),
                model_name: inference.endpoint.model.name.clone(),
            }),
        ));
    }
    Ok((
        config
            .model
            .clone()
            .ok_or_else(|| DriverError::InvalidConfig("OpenCode model is missing".to_owned()))?,
        config.openai_compatible.clone(),
    ))
}

fn validate_provider_identifier(field: &str, value: &str) -> Result<(), DriverError> {
    CHECKS.bounded(field, value, 128)?;
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

fn validate_loopback_base_url(value: &str) -> Result<(), DriverError> {
    CHECKS.bounded("base_url", value, 2_048)?;
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

/// The exact material that makes one `OpenCode` launch profile distinct.
fn profile_digest(config: &OpenCodeConfig, executable: &Path) -> Result<String, DriverError> {
    digest_profile(&json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "policy_version": OPENCODE_POLICY_VERSION,
        "executable": executable,
        "executable_digest": executable_digest(executable)?,
        "expected_version": config.expected_version,
        "model": config.model,
        "home": config.home,
        "path": config.path,
        "term": config.term,
        "tmpdir": config.tmpdir,
        "reasoning_effort": config.reasoning_effort,
        "openai_compatible": config.openai_compatible,
        "inference": config.inference,
    }))
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
    fn consumes_a_supervisor_resolved_inference_backend() {
        let mut input = value("placeholder/model");
        input.as_object_mut().expect("object").remove("model");
        input["inference"] = json!({
            "backend": {
                "name": "MLX-VLM",
                "version": "0.6.15",
                "executable_digest": format!("sha256:{}", "a".repeat(64))
            },
            "endpoint": {
                "base_url": "http://127.0.0.1:18082/v1",
                "model": {
                    "id": "/models/qwen",
                    "name": "Qwen",
                    "revision": null
                }
            },
            "profile_digest": format!("sha256:{}", "b".repeat(64)),
            "observer": {
                "url": "http://127.0.0.1:18082/metrics",
                "media_type": "application/json"
            }
        });
        input["reasoning_effort"] = json!("high");

        let prepared = prepare_config(input).expect("resolved inference route");
        let effective: serde_json::Value =
            serde_json::from_str(&prepared.runtime.environment["OPENCODE_CONFIG_CONTENT"])
                .expect("effective OpenCode config");
        assert_eq!(effective["model"], "fleetd-inference//models/qwen");
        assert_eq!(
            effective["provider"]["fleetd-inference"]["options"]["baseURL"],
            "http://127.0.0.1:18082/v1"
        );
        assert_eq!(
            effective["provider"]["fleetd-inference"]["models"]["/models/qwen"]["name"],
            "Qwen"
        );
        assert_eq!(
            effective["provider"]["fleetd-inference"]["models"]["/models/qwen"]["reasoning"],
            true
        );
        assert_eq!(
            effective["provider"]["fleetd-inference"]["models"]["/models/qwen"]["options"]["reasoningEffort"],
            "high"
        );
    }

    #[test]
    fn reasoning_effort_changes_the_effective_profile() {
        let mut low = value("fleet-local/qwen");
        low["openai_compatible"] = json!({
            "provider_id": "fleet-local",
            "provider_name": "Fleet local inference",
            "base_url": "http://127.0.0.1:18082/v1",
            "model_id": "qwen",
            "model_name": "Qwen"
        });
        low["reasoning_effort"] = json!("low");
        let mut high = low.clone();
        high["reasoning_effort"] = json!("high");

        let low = prepare_config(low).expect("low reasoning effort");
        let high = prepare_config(high).expect("high reasoning effort");

        assert_ne!(low.profile_digest, high.profile_digest);
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
