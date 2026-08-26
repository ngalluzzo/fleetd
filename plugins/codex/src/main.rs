use std::path::{Path, PathBuf};

use fleetd_acp_host::{
    DriverConfig, DriverError, PluginDefinition, RuntimeConfig,
    config::{ConfigChecks, base_environment, executable_digest, profile_digest as digest_profile},
    serve,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const PLUGIN_ID: &str = "fleetd.harness.codex";
const CHECKS: ConfigChecks = ConfigChecks::new("Codex");
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
    let executable = CHECKS.resolved_executable("adapter executable", &config.executable)?;
    let profile_digest = profile_digest(&config, &executable)?;
    let mut environment = base_environment(
        &config.home,
        config.path,
        config.term,
        config.tmpdir.as_deref(),
    );
    environment.insert(
        "CODEX_HOME".to_owned(),
        config.codex_home.to_string_lossy().into_owned(),
    );
    if config.no_browser {
        environment.insert("NO_BROWSER".to_owned(), "1".to_owned());
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
    CHECKS.absolute("adapter executable", &config.executable)?;
    CHECKS.non_empty("expected_version", &config.expected_version)?;
    for (label, directory) in [("home", &config.home), ("codex_home", &config.codex_home)] {
        CHECKS.directory(label, directory)?;
    }
    CHECKS.non_empty("PATH", &config.path)?;
    if let Some(tmpdir) = &config.tmpdir {
        CHECKS.directory("tmpdir", tmpdir)?;
    }
    Ok(())
}

/// The exact material that makes one Codex launch profile distinct.
fn profile_digest(config: &CodexConfig, executable: &Path) -> Result<String, DriverError> {
    digest_profile(&json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "executable": executable,
        "executable_digest": executable_digest(executable)?,
        "expected_version": config.expected_version,
        "home": config.home,
        "codex_home": config.codex_home,
        "path": config.path,
        "no_browser": config.no_browser,
        "term": config.term,
        "tmpdir": config.tmpdir,
    }))
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
