use std::{
    fs::{self, OpenOptions},
    io::Write as _,
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
use serde_json::{Value, json};

const PLUGIN_ID: &str = "fleetd.harness.deepseek";
const CHECKS: ConfigChecks = ConfigChecks::new("DeepSeek Harness");
const DEEPSEEK_POLICY_VERSION: u32 = 3;
const PROFILE_NAME: &str = "acp";
const PROVIDER_ID: &str = "fleetd-inference";
const ALLOWED_ENVIRONMENT: &[&str] = &[
    "DSH_HOME",
    "DSH_PERMISSION_MODE",
    "DSH_TELEMETRY_DISABLED",
    "HOME",
    "PATH",
    "TERM",
    "TMPDIR",
];

const EMPTY_HOME_PATCH: &str = "[]\n";
const EMPTY_SETTINGS: &str = "{}\n";
const PROFILE_PATCH: &str = "[]\n";
const PROFILE_WORKSPACE: &str =
    "packages:\n  - .\n\nnodeLinker: hoisted\nautoInstallPeers: false\n";
const PROFILE_MANIFEST: &str = "{\n  \"name\": \"dsh-profile-acp\",\n  \"private\": true,\n  \"dependencies\": {},\n  \"dsh\": {\n    \"profile\": {\n      \"bundles\": [\n        \"@deepseek-ai/dsh-base\",\n        \"@deepseek-ai/dsh-acp-app\"\n      ],\n      \"patchReload\": \"startup\"\n    }\n  }\n}\n";

/// Launch authority for the official `DeepSeek` Harness ACP profile.
///
/// A profile selects either a DSH-owned provider/model pair or a
/// Fleetd-supervised local inference backend. In provider mode, DSH resolves
/// the route and credential from its private `DSH_HOME`; raw provider
/// credentials are deliberately absent from this schema and child environment.
/// In local mode, the approved-profile supervisor injects the credential-free
/// loopback route only after its inference backend has reached readiness.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DeepSeekConfig {
    executable: PathBuf,
    /// Exact `agentInfo.version` reported by the DSH ACP server.
    expected_version: String,
    home: PathBuf,
    dsh_home: PathBuf,
    path: String,
    tools_mode: ToolsMode,
    /// DSH-owned provider route. `provider` and `model` must appear together
    /// and are mutually exclusive with `inference`.
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    model: Option<String>,
    /// Local OpenAI-compatible request policy. These fields are valid only
    /// beside an injected `inference` descriptor.
    #[serde(default)]
    reasoning_effort: Option<ReasoningEffort>,
    #[serde(default)]
    max_output_tokens: Option<u32>,
    #[serde(default)]
    context_window: Option<u32>,
    #[serde(default)]
    stream_idle_timeout_ms: Option<u32>,
    /// Machine-resolved backend route. Approved profiles must not pre-resolve it.
    #[serde(default)]
    inference: Option<InferenceDescribeResult>,
    #[serde(default)]
    term: Option<String>,
    #[serde(default)]
    tmpdir: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum ToolsMode {
    Native,
    Ptc,
}

impl ToolsMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Ptc => "ptc",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum ReasoningEffort {
    None,
    Low,
    Medium,
    Xhigh,
}

impl ReasoningEffort {
    const fn dsh_level(self) -> &'static str {
        match self {
            Self::None => "off",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::Xhigh => "xhigh",
        }
    }
}

struct Composition {
    source: String,
    digest: String,
    path: PathBuf,
}

enum EffectiveRoute<'a> {
    HarnessProvider {
        provider: &'a str,
        model: &'a str,
    },
    LocalInference {
        inference: &'a InferenceDescribeResult,
        reasoning_effort: ReasoningEffort,
        max_output_tokens: u32,
        context_window: u32,
        stream_idle_timeout_ms: u32,
    },
}

#[tokio::main]
async fn main() {
    let definition = PluginDefinition::new(
        PLUGIN_ID,
        "fleetd DeepSeek Harness",
        env!("CARGO_PKG_VERSION"),
        ALLOWED_ENVIRONMENT,
        prepare_config,
    );
    if let Err(error) = serve(definition).await {
        eprintln!("fleetd DeepSeek Harness failed: {error}");
        std::process::exit(1);
    }
}

fn prepare_config(value: Value) -> Result<DriverConfig, DriverError> {
    let config: DeepSeekConfig = serde_json::from_value(value)?;
    validate_config(&config)?;
    let executable = CHECKS.resolved_executable("executable", &config.executable)?;
    let composition = materialize_composition(&config)?;
    let profile_digest = profile_digest(&config, &executable, &composition)?;
    let mut environment = base_environment(
        &config.home,
        config.path.clone(),
        config.term.clone(),
        config.tmpdir.as_deref(),
    );
    environment.insert(
        "DSH_HOME".to_owned(),
        config.dsh_home.to_string_lossy().into_owned(),
    );
    // DSH's own file policy remains ask-on-write. Fleetd's typed allow_once
    // controller and outer Seatbelt profile are the authoritative boundary.
    environment.insert(
        "DSH_PERMISSION_MODE".to_owned(),
        "workspace-write".to_owned(),
    );
    // Defense in depth beside the disabled composition row. DSH treats any
    // non-empty value as a hard privacy opt-out.
    environment.insert("DSH_TELEMETRY_DISABLED".to_owned(), "1".to_owned());
    Ok(DriverConfig {
        profile_digest,
        runtime: RuntimeConfig {
            expected_name: "deepseek-harness-acp".to_owned(),
            expected_version: config.expected_version,
            executable: executable.clone(),
            identity_path: executable,
            args: vec![
                "--profile".to_owned(),
                PROFILE_NAME.to_owned(),
                "--patch".to_owned(),
                composition.path.to_string_lossy().into_owned(),
            ],
            environment,
        },
    })
}

fn validate_config(config: &DeepSeekConfig) -> Result<(), DriverError> {
    CHECKS.absolute("executable", &config.executable)?;
    CHECKS.non_empty("expected_version", &config.expected_version)?;
    for (label, directory) in [("home", &config.home), ("dsh_home", &config.dsh_home)] {
        CHECKS.directory(label, directory)?;
    }
    CHECKS.non_empty("PATH", &config.path)?;
    if let Some(tmpdir) = &config.tmpdir {
        CHECKS.directory("tmpdir", tmpdir)?;
    }
    effective_route(config).map(|_| ())
}

fn effective_route(config: &DeepSeekConfig) -> Result<EffectiveRoute<'_>, DriverError> {
    match (
        config.provider.as_deref(),
        config.model.as_deref(),
        config.inference.as_ref(),
    ) {
        (Some(provider), Some(model), None) => {
            CHECKS.bounded("provider", provider, 128)?;
            CHECKS.bounded("model", model, 512)?;
            if !provider
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            {
                return Err(DriverError::InvalidConfig(
                    "DeepSeek Harness provider contains unsupported characters".to_owned(),
                ));
            }
            if config.reasoning_effort.is_some()
                || config.max_output_tokens.is_some()
                || config.context_window.is_some()
                || config.stream_idle_timeout_ms.is_some()
            {
                return Err(DriverError::InvalidConfig(
                    "DeepSeek Harness provider mode leaves reasoning and token policy to DSH settings"
                        .to_owned(),
                ));
            }
            Ok(EffectiveRoute::HarnessProvider { provider, model })
        }
        (None, None, Some(inference)) => {
            validate_inference(inference)?;
            let reasoning_effort = config.reasoning_effort.ok_or_else(|| {
                DriverError::InvalidConfig(
                    "DeepSeek Harness local inference requires reasoning_effort".to_owned(),
                )
            })?;
            let max_output_tokens = config.max_output_tokens.ok_or_else(|| {
                DriverError::InvalidConfig(
                    "DeepSeek Harness local inference requires max_output_tokens".to_owned(),
                )
            })?;
            let context_window = config.context_window.ok_or_else(|| {
                DriverError::InvalidConfig(
                    "DeepSeek Harness local inference requires context_window".to_owned(),
                )
            })?;
            let stream_idle_timeout_ms = config.stream_idle_timeout_ms.ok_or_else(|| {
                DriverError::InvalidConfig(
                    "DeepSeek Harness local inference requires stream_idle_timeout_ms".to_owned(),
                )
            })?;
            if max_output_tokens == 0 || max_output_tokens > context_window {
                return Err(DriverError::InvalidConfig(
                    "DeepSeek Harness max_output_tokens must be positive and no greater than context_window"
                        .to_owned(),
                ));
            }
            if stream_idle_timeout_ms == 0 {
                return Err(DriverError::InvalidConfig(
                    "DeepSeek Harness stream_idle_timeout_ms must be greater than zero".to_owned(),
                ));
            }
            Ok(EffectiveRoute::LocalInference {
                inference,
                reasoning_effort,
                max_output_tokens,
                context_window,
                stream_idle_timeout_ms,
            })
        }
        (Some(_), None, _) | (None, Some(_), _) => Err(DriverError::InvalidConfig(
            "DeepSeek Harness provider and model must be configured together".to_owned(),
        )),
        (Some(_), Some(_), Some(_)) => Err(DriverError::InvalidConfig(
            "DeepSeek Harness provider/model and inference routes are mutually exclusive"
                .to_owned(),
        )),
        (None, None, None) => Err(DriverError::InvalidConfig(
            "DeepSeek Harness requires either provider/model or an injected inference route"
                .to_owned(),
        )),
    }
}

fn validate_inference(inference: &InferenceDescribeResult) -> Result<(), DriverError> {
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
    Ok(())
}

fn validate_digest(label: &str, value: &str) -> Result<(), DriverError> {
    let Some(digest) = value.strip_prefix("sha256:") else {
        return Err(DriverError::InvalidConfig(format!(
            "DeepSeek Harness {label} must be a SHA-256 digest"
        )));
    };
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DriverError::InvalidConfig(format!(
            "DeepSeek Harness {label} must be a SHA-256 digest"
        )));
    }
    Ok(())
}

fn validate_loopback_base_url(value: &str) -> Result<(), DriverError> {
    CHECKS.bounded("inference base URL", value, 2_048)?;
    let Some(rest) = value.strip_prefix("http://") else {
        return Err(DriverError::InvalidConfig(
            "DeepSeek Harness inference base URL must use loopback HTTP".to_owned(),
        ));
    };
    let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
    let address: SocketAddr = authority.parse().map_err(|_| {
        DriverError::InvalidConfig(
            "DeepSeek Harness inference base URL must contain an explicit loopback IP and port"
                .to_owned(),
        )
    })?;
    if !address.ip().is_loopback()
        || path.contains('?')
        || path.contains('#')
        || value.contains('@')
    {
        return Err(DriverError::InvalidConfig(
            "DeepSeek Harness inference base URL must be a credential-free loopback HTTP URL"
                .to_owned(),
        ));
    }
    Ok(())
}

fn materialize_composition(config: &DeepSeekConfig) -> Result<Composition, DriverError> {
    let source = composition_source(config)?;
    let digest = digest_profile(&json!({
        "format": "fleetd-dsh-overlay-v1",
        "source": source,
    }))?;
    let digest_name = digest
        .strip_prefix("sha256:")
        .ok_or_else(|| DriverError::Runtime("composition digest lost its algorithm".to_owned()))?;
    let composition_dir = config.dsh_home.join("fleetd").join("compositions");
    fs::create_dir_all(&composition_dir)?;
    let path = composition_dir.join(format!("{digest_name}.cordis.patch.yml"));
    ensure_exact_file(&path, source.as_bytes())?;

    // Composition remains Fleetd-owned in both modes. Provider mode deliberately
    // leaves DSH's settings and credential documents intact; local inference
    // eliminates the settings layer so no ambient route can replace the exact
    // supervisor-injected backend.
    ensure_exact_file(
        &config.dsh_home.join("cordis.patch.yml"),
        EMPTY_HOME_PATCH.as_bytes(),
    )?;
    if matches!(
        effective_route(config)?,
        EffectiveRoute::LocalInference { .. }
    ) {
        ensure_exact_file(
            &config.dsh_home.join("settings.yaml"),
            EMPTY_SETTINGS.as_bytes(),
        )?;
    }
    let profile_dir = config.dsh_home.join("profiles").join(PROFILE_NAME);
    fs::create_dir_all(&profile_dir)?;
    ensure_exact_file(
        &profile_dir.join("package.json"),
        PROFILE_MANIFEST.as_bytes(),
    )?;
    ensure_exact_file(
        &profile_dir.join("cordis.patch.yml"),
        PROFILE_PATCH.as_bytes(),
    )?;
    ensure_exact_file(
        &profile_dir.join("pnpm-workspace.yaml"),
        PROFILE_WORKSPACE.as_bytes(),
    )?;
    Ok(Composition {
        source,
        digest,
        path,
    })
}

fn ensure_exact_file(path: &Path, expected: &[u8]) -> Result<(), DriverError> {
    if let Ok(actual) = fs::read(path) {
        if actual == expected {
            return Ok(());
        }
        return Err(DriverError::InvalidConfig(format!(
            "DeepSeek Harness managed file drifted from its content-addressed profile: {}",
            path.display()
        )));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(expected)?;
            file.sync_all()?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let actual = fs::read(path)?;
            if actual != expected {
                return Err(DriverError::InvalidConfig(format!(
                    "DeepSeek Harness managed file changed while it was materialized: {}",
                    path.display()
                )));
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn composition_source(config: &DeepSeekConfig) -> Result<String, DriverError> {
    let code_runtime = if matches!(config.tools_mode, ToolsMode::Ptc) {
        "\n- insert:\n    - id: code-runtime\n      name: '@deepseek-ai/dsh-code-runtime-worker-thread'\n"
    } else {
        ""
    };
    let route = match effective_route(config)? {
        EffectiveRoute::HarnessProvider { provider, model } => {
            let provider = yaml_string(provider)?;
            let model = yaml_string(model)?;
            format!(
                "- id: agent-default-model\n  config:\n    provider: {provider}\n    model: {model}\n\n\
- id: acp\n  config:\n    provider: {provider}\n    model: {model}\n\n"
            )
        }
        EffectiveRoute::LocalInference {
            inference,
            reasoning_effort,
            max_output_tokens,
            context_window,
            stream_idle_timeout_ms,
        } => {
            let base_url = yaml_string(&inference.endpoint.base_url)?;
            let model_id = yaml_string(&inference.endpoint.model.id)?;
            let model_name = yaml_string(&inference.endpoint.model.name)?;
            let provider_name = yaml_string(&format!("Fleetd · {}", inference.backend.name))?;
            format!(
                "- id: settings\n  disabled: true\n\n\
- id: credentials\n  disabled: true\n\n\
- id: llm-deepseek\n  disabled: true\n\n\
- id: agent-default-model\n  config:\n    provider: {PROVIDER_ID}\n    model: {model_id}\n\n\
- id: llm-pi-ai\n  config:\n    providers:\n      {PROVIDER_ID}:\n        displayName: {provider_name}\n        api: openai-completions\n        baseURL: {base_url}\n        headers:\n          Authorization: \"Bearer fleetd-local\"\n        reasoning: {}\n        streamIdleTimeoutMs: {stream_idle_timeout_ms}\n        retryPolicy:\n          mode: normal\n          maxRetries: 0\n        compat:\n          supportsStore: false\n          supportsDeveloperRole: false\n          supportsReasoningEffort: true\n          supportsUsageInStreaming: true\n          supportsFinishReason: true\n          maxTokensField: max_tokens\n          supportsStrictMode: false\n          thinkingFormat: openai\n        models:\n          - id: {model_id}\n            name: {model_name}\n            contextWindow: {context_window}\n            maxTokens: {max_output_tokens}\n            input: [text]\n            reasoningEfforts:\n              off: none\n              low: low\n              medium: medium\n              xhigh: xhigh\n\n\
- id: acp\n  config:\n    provider: {PROVIDER_ID}\n    model: {model_id}\n\n",
                reasoning_effort.dsh_level(),
            )
        }
    };
    Ok(format!(
        "# Generated by fleetd; digest-addressed and not a user-editable DSH profile.\n\
{route}\
- id: session-telemetry-otel\n  disabled: true\n\n\
- id: tools\n  config:\n    mode: {}\n\n\
- id: web\n  disabled: true\n\n\
- id: web-search-deepseek\n  disabled: true\n\n\
- id: web-fetch-http\n  disabled: true\n\n\
- id: tool-web\n  disabled: true\n\n\
- id: subagent\n  disabled: true\n\n\
- id: subagent-spawn-in-process\n  disabled: true\n\n\
- id: subagent-fork-in-process\n  disabled: true\n\n\
- id: tool-subagent-control\n  disabled: true\n\n\
- id: tool-subagent-list-agents\n  disabled: true\n\n\
- id: tool-subagent\n  disabled: true\n\n\
- id: tool-subagent-fork\n  disabled: true\n\n\
- id: tool-subagent-report\n  disabled: true\n\n\
- id: workflow-worker-thread\n  disabled: true\n\n\
- id: tool-workflow\n  disabled: true\n\n\
- id: tool-ralph\n  disabled: true\n\
{code_runtime}",
        config.tools_mode.as_str(),
    ))
}

fn yaml_string(value: &str) -> Result<String, DriverError> {
    Ok(serde_json::to_string(value)?)
}

/// The exact non-secret material that distinguishes one DSH launch profile.
fn profile_digest(
    config: &DeepSeekConfig,
    executable: &Path,
    composition: &Composition,
) -> Result<String, DriverError> {
    digest_profile(&json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "policy_version": DEEPSEEK_POLICY_VERSION,
        "executable": executable,
        "executable_digest": executable_digest(executable)?,
        "expected_version": config.expected_version,
        "profile": PROFILE_NAME,
        "permission_mode": "workspace-write",
        "telemetry": "disabled",
        "home": config.home,
        "dsh_home": config.dsh_home,
        "path": config.path,
        "term": config.term,
        "tmpdir": config.tmpdir,
        "tools_mode": config.tools_mode,
        "provider": config.provider,
        "model": config.model,
        "reasoning_effort": config.reasoning_effort,
        "max_output_tokens": config.max_output_tokens,
        "context_window": config.context_window,
        "stream_idle_timeout_ms": config.stream_idle_timeout_ms,
        "inference": config.inference,
        "composition_digest": composition.digest,
        "composition_source": composition.source,
    }))
}

#[cfg(test)]
mod tests {
    use std::env;

    use serde_json::json;
    use tempfile::TempDir;

    use super::{PROFILE_MANIFEST, prepare_config};

    fn value(dsh_home: &std::path::Path) -> serde_json::Value {
        json!({
            "executable": env::current_exe().expect("test executable"),
            "expected_version": "0.0.1",
            "home": dsh_home,
            "dsh_home": dsh_home,
            "path": "/usr/bin:/bin",
            "term": "xterm-256color",
            "tmpdir": dsh_home,
            "tools_mode": "ptc",
            "reasoning_effort": "none",
            "max_output_tokens": 8192,
            "context_window": 262_144,
            "stream_idle_timeout_ms": 300_000,
            "inference": {
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
                "observer": null
            }
        })
    }

    fn provider_value(dsh_home: &std::path::Path) -> serde_json::Value {
        json!({
            "executable": env::current_exe().expect("test executable"),
            "expected_version": "0.0.1",
            "home": dsh_home,
            "dsh_home": dsh_home,
            "path": "/usr/bin:/bin",
            "term": "xterm-256color",
            "tmpdir": dsh_home,
            "tools_mode": "ptc",
            "provider": "zai",
            "model": "glm-5.3"
        })
    }

    #[test]
    fn owns_a_typed_local_ptc_launch_policy() {
        let home = TempDir::new().expect("temporary DSH home");
        let prepared = prepare_config(value(home.path())).expect("valid config");

        assert_eq!(prepared.runtime.expected_name, "deepseek-harness-acp");
        assert_eq!(&prepared.runtime.args[..3], ["--profile", "acp", "--patch"]);
        let patch = std::fs::read_to_string(&prepared.runtime.args[3]).expect("generated patch");
        assert!(patch.contains("mode: ptc"));
        assert!(patch.contains("id: code-runtime"));
        assert!(patch.contains("reasoning: off"));
        assert!(patch.contains("maxTokens: 8192"));
        assert!(patch.contains("supportsDeveloperRole: false"));
        assert!(patch.contains("session-telemetry-otel\n  disabled: true"));
        assert_eq!(
            prepared.runtime.environment["DSH_HOME"],
            home.path().to_string_lossy()
        );
        assert_eq!(
            prepared.runtime.environment["DSH_PERMISSION_MODE"],
            "workspace-write"
        );
        assert_eq!(prepared.runtime.environment["DSH_TELEMETRY_DISABLED"], "1");
        assert_eq!(
            std::fs::read_to_string(home.path().join("profiles/acp/package.json"))
                .expect("profile manifest"),
            PROFILE_MANIFEST
        );
        assert!(
            !prepared
                .runtime
                .environment
                .contains_key("DEEPSEEK_API_KEY")
        );
    }

    #[test]
    fn preserves_dsh_owned_provider_settings_and_credentials() {
        let home = TempDir::new().expect("temporary DSH home");
        let settings = "llm-pi-ai:\n  providers:\n    zai:\n      apiKeyEnv: ZAI_API_KEY\n";
        let credentials = "managed-by: dsh\n";
        std::fs::write(home.path().join("settings.yaml"), settings).expect("write DSH settings");
        std::fs::write(home.path().join(".credentials.yaml"), credentials)
            .expect("write DSH credentials");

        let prepared = prepare_config(provider_value(home.path())).expect("provider config");
        let patch = std::fs::read_to_string(&prepared.runtime.args[3]).expect("generated patch");

        assert!(patch.contains("provider: \"zai\""));
        assert!(patch.contains("model: \"glm-5.3\""));
        assert!(!patch.contains("- id: settings\n  disabled: true"));
        assert!(!patch.contains("- id: credentials\n  disabled: true"));
        assert!(!patch.contains("- id: llm-pi-ai\n  config:"));
        assert_eq!(
            std::fs::read_to_string(home.path().join("settings.yaml"))
                .expect("preserved DSH settings"),
            settings
        );
        assert_eq!(
            std::fs::read_to_string(home.path().join(".credentials.yaml"))
                .expect("preserved DSH credentials"),
            credentials
        );
        assert!(!prepared.runtime.environment.contains_key("ZAI_API_KEY"));
    }

    #[test]
    fn native_mode_omits_the_code_runtime() {
        let home = TempDir::new().expect("temporary DSH home");
        let mut input = value(home.path());
        input["tools_mode"] = json!("native");
        let prepared = prepare_config(input).expect("native config");
        let patch = std::fs::read_to_string(&prepared.runtime.args[3]).expect("generated patch");

        assert!(patch.contains("mode: native"));
        assert!(!patch.contains("id: code-runtime"));
    }

    #[test]
    fn materialized_layers_are_fail_closed_against_drift() {
        let home = TempDir::new().expect("temporary DSH home");
        prepare_config(value(home.path())).expect("first config");
        std::fs::write(home.path().join("settings.yaml"), "llm-pi-ai: {}\n")
            .expect("tamper with managed settings");

        let error = prepare_config(value(home.path())).expect_err("drift must fail");
        assert!(error.to_string().contains("managed file drifted"));
    }

    #[test]
    fn effective_controls_change_the_profile_digest() {
        let home = TempDir::new().expect("temporary DSH home");
        let first = prepare_config(value(home.path())).expect("first config");
        let mut second_value = value(home.path());
        second_value["reasoning_effort"] = json!("xhigh");
        let second = prepare_config(second_value).expect("second config");

        assert_ne!(first.profile_digest, second.profile_digest);
    }

    #[test]
    fn provider_identity_changes_the_profile_digest() {
        let home = TempDir::new().expect("temporary DSH home");
        let first = prepare_config(provider_value(home.path())).expect("first config");
        let mut second_value = provider_value(home.path());
        second_value["model"] = json!("glm-5.3-flash");
        let second = prepare_config(second_value).expect("second config");

        assert_ne!(first.profile_digest, second.profile_digest);
    }

    #[test]
    fn provider_and_local_inference_routes_are_exactly_one() {
        let home = TempDir::new().expect("temporary DSH home");
        let mut both = value(home.path());
        both["provider"] = json!("zai");
        both["model"] = json!("glm-5.3");
        assert!(
            prepare_config(both)
                .expect_err("routes must be exclusive")
                .to_string()
                .contains("mutually exclusive")
        );

        let mut half = provider_value(home.path());
        half.as_object_mut().expect("object").remove("model");
        assert!(
            prepare_config(half)
                .expect_err("provider pair must be complete")
                .to_string()
                .contains("configured together")
        );

        let mut provider_policy = provider_value(home.path());
        provider_policy["reasoning_effort"] = json!("xhigh");
        assert!(
            prepare_config(provider_policy)
                .expect_err("provider policy belongs to DSH")
                .to_string()
                .contains("leaves reasoning and token policy to DSH settings")
        );
    }

    #[test]
    fn rejects_remote_routes_credentials_and_invalid_caps() {
        let home = TempDir::new().expect("temporary DSH home");
        let mut remote = value(home.path());
        remote["inference"]["endpoint"]["base_url"] = json!("https://models.example/v1");
        assert!(prepare_config(remote).is_err());

        let mut credential = value(home.path());
        credential["api_key"] = json!("must-not-cross-plugin-config");
        assert!(prepare_config(credential).is_err());

        let mut oversized = value(home.path());
        oversized["max_output_tokens"] = json!(262_145);
        assert!(prepare_config(oversized).is_err());
    }
}
