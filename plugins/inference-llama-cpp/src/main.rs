use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use fleetd_inference_host::{
    BackendError, BackendLaunch, PluginDefinition, RuntimeConfig,
    config::{ConfigChecks, file_digest, profile_digest},
    serve,
};
use fleetd_proto::inference_openai::{
    BackendIdentity, DescribeResult, Endpoint, ModelRoute, ObserverEndpoint,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const PLUGIN_ID: &str = "fleetd.inference.llama-cpp";
const CHECKS: ConfigChecks = ConfigChecks::new("llama.cpp");

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    executable: PathBuf,
    expected_version: String,
    model: PathBuf,
    model_id: String,
    model_name: String,
    #[serde(default)]
    model_revision: Option<String>,
    port: u16,
    #[serde(default)]
    context_size: Option<u32>,
    #[serde(default = "default_parallel")]
    parallel: u16,
    #[serde(default = "enabled")]
    jinja: bool,
    #[serde(default = "enabled")]
    metrics: bool,
    #[serde(default = "default_startup_timeout_ms")]
    startup_timeout_ms: u64,
}

#[tokio::main]
async fn main() {
    let definition = PluginDefinition::new(
        PLUGIN_ID,
        "fleetd llama.cpp inference backend",
        env!("CARGO_PKG_VERSION"),
        &[],
        prepare_config,
    );
    if let Err(error) = serve(definition).await {
        eprintln!("fleetd llama.cpp inference backend failed: {error}");
        std::process::exit(1);
    }
}

fn prepare_config(value: Value) -> Result<BackendLaunch, BackendError> {
    let config: Config = serde_json::from_value(value)?;
    validate(&config)?;
    let executable = CHECKS.executable("executable", &config.executable)?;
    let model = CHECKS.file("model", &config.model)?;
    let executable_digest = file_digest(&executable)?;
    let address = format!("127.0.0.1:{}", config.port);
    let origin = format!("http://{address}");
    let mut args = vec![
        "--host".to_owned(),
        "127.0.0.1".to_owned(),
        "--port".to_owned(),
        config.port.to_string(),
        "--model".to_owned(),
        model.to_string_lossy().into_owned(),
        "--alias".to_owned(),
        config.model_id.clone(),
        "--parallel".to_owned(),
        config.parallel.to_string(),
    ];
    if let Some(context_size) = config.context_size {
        args.extend(["--ctx-size".to_owned(), context_size.to_string()]);
    }
    if config.jinja {
        args.push("--jinja".to_owned());
    }
    if config.metrics {
        args.push("--metrics".to_owned());
    }
    let profile_digest = profile_digest(&json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "executable": executable,
        "executable_digest": executable_digest,
        "model": model,
        "model_metadata": std::fs::metadata(&model).map(|metadata| metadata.len())?,
        "config": config,
    }))?;
    Ok(BackendLaunch {
        runtime: RuntimeConfig {
            executable,
            version_args: vec!["--version".to_owned()],
            expected_version: config.expected_version.clone(),
            args,
            environment: BTreeMap::new(),
        },
        description: DescribeResult {
            backend: BackendIdentity {
                name: "llama.cpp".to_owned(),
                version: config.expected_version,
                executable_digest,
            },
            endpoint: Endpoint {
                base_url: format!("{origin}/v1"),
                model: ModelRoute {
                    id: config.model_id,
                    name: config.model_name,
                    revision: config.model_revision,
                },
            },
            profile_digest,
            observer: config.metrics.then(|| ObserverEndpoint {
                url: format!("{origin}/metrics"),
                media_type: "text/plain; version=0.0.4".to_owned(),
            }),
        },
        health_url: format!("{origin}/health"),
        models_url: format!("{origin}/v1/models"),
        startup_timeout: Duration::from_millis(config.startup_timeout_ms),
    })
}

fn validate(config: &Config) -> Result<(), BackendError> {
    CHECKS.bounded("expected version", &config.expected_version, 128)?;
    CHECKS.bounded("model ID", &config.model_id, 512)?;
    CHECKS.bounded("model name", &config.model_name, 256)?;
    if let Some(revision) = &config.model_revision {
        CHECKS.bounded("model revision", revision, 512)?;
    }
    if config.port == 0 {
        return Err(BackendError::InvalidConfig(
            "llama.cpp port must be non-zero".to_owned(),
        ));
    }
    if config.parallel == 0 || config.parallel > 64 {
        return Err(BackendError::InvalidConfig(
            "llama.cpp parallel must contain between 1 and 64 slots".to_owned(),
        ));
    }
    if config.context_size == Some(0) {
        return Err(BackendError::InvalidConfig(
            "llama.cpp context_size must be greater than zero when supplied".to_owned(),
        ));
    }
    if config.startup_timeout_ms == 0 || config.startup_timeout_ms > 30 * 60 * 1_000 {
        return Err(BackendError::InvalidConfig(
            "llama.cpp startup timeout must be between 1 millisecond and 30 minutes".to_owned(),
        ));
    }
    Ok(())
}

const fn default_parallel() -> u16 {
    1
}

const fn enabled() -> bool {
    true
}

const fn default_startup_timeout_ms() -> u64 {
    10 * 60 * 1_000
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::prepare_config;

    #[test]
    fn owns_a_strict_llama_server_launch() {
        let executable = std::env::current_exe().expect("test executable");
        let launch = prepare_config(json!({
            "executable": executable,
            "expected_version": "1.0.0",
            "model": executable,
            "model_id": "qwen-local",
            "model_name": "Qwen local",
            "port": 18082,
            "context_size": 32768,
            "parallel": 1
        }))
        .expect("valid launch");
        assert!(
            launch
                .runtime
                .args
                .windows(2)
                .any(|pair| pair == ["--alias", "qwen-local"])
        );
        assert_eq!(
            launch.description.endpoint.base_url,
            "http://127.0.0.1:18082/v1"
        );
        assert!(launch.description.profile_digest.starts_with("sha256:"));
    }

    #[test]
    fn rejects_generic_arguments_and_unknown_fields() {
        let executable = std::env::current_exe().expect("test executable");
        let result = prepare_config(json!({
            "executable": executable,
            "expected_version": "1.0.0",
            "model": executable,
            "model_id": "qwen-local",
            "model_name": "Qwen local",
            "port": 18082,
            "args": ["--api-key", "secret"]
        }));
        assert!(result.is_err());
    }
}
