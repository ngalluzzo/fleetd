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

const PLUGIN_ID: &str = "fleetd.inference.mlx-vlm";
const CHECKS: ConfigChecks = ConfigChecks::new("MLX-VLM");
const ALLOWED_ENVIRONMENT: &[&str] = [
    "APC_ENABLED",
    "APC_HASH",
    "APC_NUM_BLOCKS",
    "HOME",
    "TMPDIR",
]
.as_slice();

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Config {
    python_executable: PathBuf,
    expected_version: String,
    home: PathBuf,
    #[serde(default)]
    tmpdir: Option<PathBuf>,
    model: PathBuf,
    model_id: String,
    model_name: String,
    #[serde(default)]
    model_revision: Option<String>,
    port: u16,
    #[serde(default)]
    draft_model: Option<PathBuf>,
    #[serde(default)]
    draft_kind: Option<DraftKind>,
    #[serde(default)]
    draft_block_size: Option<u16>,
    #[serde(default)]
    max_kv_size: Option<u32>,
    #[serde(default)]
    max_tokens: Option<u32>,
    #[serde(default)]
    max_num_seqs: Option<u16>,
    #[serde(default)]
    enable_thinking: bool,
    #[serde(default)]
    thinking_budget: Option<u32>,
    #[serde(default)]
    kv_bits: Option<String>,
    #[serde(default)]
    apc_num_blocks: Option<u32>,
    #[serde(default)]
    trust_remote_code: bool,
    #[serde(default = "default_startup_timeout_ms")]
    startup_timeout_ms: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum DraftKind {
    Dflash,
    Eagle3,
    Mtp,
}

impl DraftKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Dflash => "dflash",
            Self::Eagle3 => "eagle3",
            Self::Mtp => "mtp",
        }
    }
}

#[tokio::main]
async fn main() {
    let definition = PluginDefinition::new(
        PLUGIN_ID,
        "fleetd MLX-VLM inference backend",
        env!("CARGO_PKG_VERSION"),
        ALLOWED_ENVIRONMENT,
        prepare_config,
    );
    if let Err(error) = serve(definition).await {
        eprintln!("fleetd MLX-VLM inference backend failed: {error}");
        std::process::exit(1);
    }
}

fn prepare_config(value: Value) -> Result<BackendLaunch, BackendError> {
    let config: Config = serde_json::from_value(value)?;
    validate(&config)?;
    let python = CHECKS.executable("Python executable", &config.python_executable)?;
    let home = CHECKS.directory("home", &config.home)?;
    let tmpdir = config
        .tmpdir
        .as_deref()
        .map(|path| CHECKS.directory("tmpdir", path))
        .transpose()?;
    let model = CHECKS.directory("model", &config.model)?;
    let draft_model = config
        .draft_model
        .as_deref()
        .map(|path| CHECKS.directory("draft model", path))
        .transpose()?;
    let executable_digest = file_digest(&python)?;
    let origin = format!("http://127.0.0.1:{}", config.port);
    let args = runtime_args(&config, &model, draft_model.as_deref());
    let environment = runtime_environment(&config, &home, tmpdir.as_deref());
    let profile_digest = profile_digest(&json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "python": python,
        "python_digest": executable_digest,
        "model": model,
        "draft_model": draft_model,
        "config": config,
    }))?;
    Ok(BackendLaunch {
        runtime: RuntimeConfig {
            executable: python,
            version_args: vec![
                "-c".to_owned(),
                "import importlib.metadata as m; print(m.version('mlx-vlm'))".to_owned(),
            ],
            expected_version: config.expected_version.clone(),
            args,
            environment,
        },
        description: DescribeResult {
            backend: BackendIdentity {
                name: "MLX-VLM".to_owned(),
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
            observer: Some(ObserverEndpoint {
                url: format!("{origin}/metrics"),
                media_type: "application/json".to_owned(),
            }),
        },
        health_url: format!("{origin}/health"),
        models_url: format!("{origin}/v1/models"),
        startup_timeout: Duration::from_millis(config.startup_timeout_ms),
    })
}

fn runtime_args(
    config: &Config,
    model: &std::path::Path,
    draft_model: Option<&std::path::Path>,
) -> Vec<String> {
    let mut args = vec![
        "-m".to_owned(),
        "mlx_vlm.server".to_owned(),
        "--host".to_owned(),
        "127.0.0.1".to_owned(),
        "--port".to_owned(),
        config.port.to_string(),
        "--model".to_owned(),
        model.to_string_lossy().into_owned(),
    ];
    if let Some(draft_model) = &draft_model {
        args.extend([
            "--draft-model".to_owned(),
            draft_model.to_string_lossy().into_owned(),
        ]);
    }
    if let Some(kind) = config.draft_kind {
        args.extend(["--draft-kind".to_owned(), kind.as_str().to_owned()]);
    }
    if let Some(size) = config.draft_block_size {
        args.extend(["--draft-block-size".to_owned(), size.to_string()]);
    }
    if let Some(size) = config.max_kv_size {
        args.extend(["--max-kv-size".to_owned(), size.to_string()]);
    }
    if let Some(size) = config.max_tokens {
        args.extend(["--max-tokens".to_owned(), size.to_string()]);
    }
    if let Some(count) = config.max_num_seqs {
        args.extend(["--max-num-seqs".to_owned(), count.to_string()]);
    }
    if config.enable_thinking {
        args.push("--enable-thinking".to_owned());
    }
    if let Some(budget) = config.thinking_budget {
        args.extend(["--thinking-budget".to_owned(), budget.to_string()]);
    }
    if let Some(bits) = &config.kv_bits {
        args.extend(["--kv-bits".to_owned(), bits.clone()]);
    }
    if config.trust_remote_code {
        args.push("--trust-remote-code".to_owned());
    }
    args
}

fn runtime_environment(
    config: &Config,
    home: &std::path::Path,
    tmpdir: Option<&std::path::Path>,
) -> BTreeMap<String, String> {
    let mut environment =
        BTreeMap::from([("HOME".to_owned(), home.to_string_lossy().into_owned())]);
    if let Some(tmpdir) = &tmpdir {
        environment.insert("TMPDIR".to_owned(), tmpdir.to_string_lossy().into_owned());
    }
    if let Some(blocks) = config.apc_num_blocks {
        environment.extend([
            ("APC_ENABLED".to_owned(), "1".to_owned()),
            ("APC_HASH".to_owned(), "sha256".to_owned()),
            ("APC_NUM_BLOCKS".to_owned(), blocks.to_string()),
        ]);
    }
    environment
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
            "MLX-VLM port must be non-zero".to_owned(),
        ));
    }
    if config.draft_kind.is_some() && config.draft_model.is_none() {
        return Err(BackendError::InvalidConfig(
            "MLX-VLM draft_kind requires draft_model".to_owned(),
        ));
    }
    if config.thinking_budget.is_some() && config.draft_model.is_some() {
        return Err(BackendError::InvalidConfig(
            "MLX-VLM thinking_budget is not supported with speculative decoding".to_owned(),
        ));
    }
    if config
        .draft_block_size
        .is_some_and(|value| value == 0 || value > 64)
    {
        return Err(BackendError::InvalidConfig(
            "MLX-VLM draft_block_size must contain between 1 and 64".to_owned(),
        ));
    }
    if config.max_kv_size == Some(0)
        || config.max_tokens == Some(0)
        || config.max_num_seqs == Some(0)
        || config.thinking_budget == Some(0)
        || config.apc_num_blocks == Some(0)
    {
        return Err(BackendError::InvalidConfig(
            "MLX-VLM cache, output, and concurrency bounds must be greater than zero when supplied"
                .to_owned(),
        ));
    }
    if let Some(bits) = &config.kv_bits {
        CHECKS.bounded("kv_bits", bits, 16)?;
        if bits.parse::<f32>().is_err() {
            return Err(BackendError::InvalidConfig(
                "MLX-VLM kv_bits must be numeric".to_owned(),
            ));
        }
    }
    if config.startup_timeout_ms == 0 || config.startup_timeout_ms > 30 * 60 * 1_000 {
        return Err(BackendError::InvalidConfig(
            "MLX-VLM startup timeout must be between 1 millisecond and 30 minutes".to_owned(),
        ));
    }
    Ok(())
}

const fn default_startup_timeout_ms() -> u64 {
    15 * 60 * 1_000
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::prepare_config;

    #[test]
    fn owns_a_strict_mlx_vlm_launch() {
        let executable = std::env::current_exe().expect("test executable");
        let directory = std::env::temp_dir();
        let launch = prepare_config(json!({
            "python_executable": executable,
            "expected_version": "0.6.15",
            "home": directory,
            "tmpdir": directory,
            "model": directory,
            "model_id": "/models/qwen",
            "model_name": "Qwen",
            "port": 18082,
            "apc_num_blocks": 4096,
            "max_kv_size": 262_144,
            "max_tokens": 8192,
            "max_num_seqs": 1,
            "enable_thinking": true,
            "thinking_budget": 4096
        }))
        .expect("valid launch");
        assert!(
            launch
                .runtime
                .args
                .windows(2)
                .any(|pair| pair == ["--host", "127.0.0.1"])
        );
        assert_eq!(launch.runtime.environment["APC_HASH"], "sha256");
        assert!(
            launch
                .runtime
                .args
                .windows(2)
                .any(|pair| pair == ["--max-tokens", "8192"])
        );
        assert!(
            launch
                .runtime
                .args
                .windows(2)
                .any(|pair| pair == ["--max-num-seqs", "1"])
        );
        assert!(
            launch
                .runtime
                .args
                .iter()
                .any(|argument| argument == "--enable-thinking")
        );
        assert!(
            launch
                .runtime
                .args
                .windows(2)
                .any(|pair| pair == ["--thinking-budget", "4096"])
        );
        assert_eq!(
            launch.description.observer.expect("metrics").url,
            "http://127.0.0.1:18082/metrics"
        );
    }

    #[test]
    fn refuses_an_unpaired_draft_kind() {
        let executable = std::env::current_exe().expect("test executable");
        let directory = std::env::temp_dir();
        let result = prepare_config(json!({
            "python_executable": executable,
            "expected_version": "0.6.15",
            "home": directory,
            "model": directory,
            "model_id": "qwen",
            "model_name": "Qwen",
            "port": 18082,
            "draft_kind": "mtp"
        }));
        assert!(result.is_err());
    }

    #[test]
    fn refuses_a_thinking_budget_with_speculative_decoding() {
        let executable = std::env::current_exe().expect("test executable");
        let directory = std::env::temp_dir();
        let result = prepare_config(json!({
            "python_executable": executable,
            "expected_version": "0.6.15",
            "home": directory,
            "model": directory,
            "model_id": "qwen",
            "model_name": "Qwen",
            "port": 18082,
            "draft_model": directory,
            "draft_kind": "mtp",
            "enable_thinking": true,
            "thinking_budget": 4096
        }));
        match result {
            Err(error) => assert!(error.to_string().contains("speculative decoding")),
            Ok(_) => panic!("budgeted speculative decoding must fail"),
        }
    }
}
