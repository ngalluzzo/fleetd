//! Claude Code harness plugin.
//!
//! Claude Code does not speak ACP itself. It is driven through an ACP adapter,
//! so this plugin launches the adapter and pins both identities: the adapter is
//! what reports `agentInfo` over ACP, and Claude Code is what actually does the
//! work. A profile that named only the adapter would call two different Claude
//! Code versions the same profile and resume a native session across them.
//!
//! Still unqualified -- no turn has run through the ACP adapter -- but the two
//! things previously believed to block that were measured and are not what they
//! were thought to be.
//!
//! Claude Code's refusal to run inside another Claude Code session is driven by
//! `CLAUDECODE` in the environment. This plugin inherits nothing, so the
//! variable is already absent and the refusal never fires. Measured directly:
//! a `claude -p` run with the full environment minus `CLAUDECODE` succeeds,
//! and the earlier conclusion that this integration could not be exercised from
//! inside a session was wrong.
//!
//! Subscription state is intentionally not inherited. Claude Code releases have
//! used both account-scoped system storage and an owner-only credential file;
//! Fleetd must provision whichever exact mechanism a seat declares rather than
//! granting the adapter ambient home access. `USER` and `LOGNAME` still name the
//! selected account, while optional `CLAUDE_CONFIG_DIR` deliberately redirects
//! configuration and credential lookup into a private directory.
//!
//! An operator supplying `ANTHROPIC_API_KEY` is the adapter's other documented
//! path, and this plugin deliberately does not carry it: a provider credential
//! in worker desired state is what every other harness plugin here refuses, and
//! opening that is a decision about the plugin contract rather than about this
//! integration.

use std::path::{Path, PathBuf};

use fleetd_acp_host::{
    DriverConfig, DriverError, PluginDefinition, RuntimeConfig,
    config::{ConfigChecks, base_environment, executable_digest, profile_digest as digest_profile},
    serve,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const PLUGIN_ID: &str = "fleetd.harness.claude";
const CHECKS: ConfigChecks = ConfigChecks::new("Claude Code");

/// The exact ACP identity the adapter reports.
///
/// The host refuses a runtime whose `agentInfo.name` differs, so this is the
/// adapter's package name rather than "Claude Code": the adapter is the process
/// on the other end of the protocol.
const ADAPTER_AGENT_NAME: &str = "@agentclientprotocol/claude-agent-acp";

/// Nothing outside this list reaches the adapter, and nothing is inherited.
///
/// `CLAUDE_CODE_EXECUTABLE` is what makes the underlying binary exact instead of
/// whatever `PATH` happens to resolve. `IS_SANDBOX` is deliberately absent: it
/// changes the harness's own safety behaviour, which is not this plugin's to
/// weaken. `CLAUDECODE` is absent for a different reason -- it is what makes
/// Claude Code refuse to run inside another Claude Code session, and inheriting
/// nothing is what keeps a seat launchable from an operator's own session.
///
/// `USER` and `LOGNAME` name the selected local account. They are identity
/// rather than authority: neither carries a secret.
const ALLOWED_ENVIRONMENT: &[&str] = &[
    "CLAUDE_CODE_EXECUTABLE",
    "CLAUDE_CODE_TMPDIR",
    "CLAUDE_CONFIG_DIR",
    "HOME",
    "LOGNAME",
    "MAX_THINKING_TOKENS",
    "PATH",
    "TERM",
    "TMPDIR",
    "USER",
];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ClaudeConfig {
    /// The ACP adapter executable, which is what speaks the protocol.
    executable: PathBuf,
    /// The adapter version, matched exactly against its reported `agentInfo`.
    expected_version: String,
    /// The Claude Code binary the adapter drives.
    claude_executable: PathBuf,
    /// The local account Claude Code runs as. Supplied rather than inherited,
    /// like every other value here.
    user: String,
    /// Claude Code's own configuration directory. Optional, and omitting it is
    /// the normal case. Supply it only to choose private redirected state on
    /// purpose. Fleetd never reads it and never places anything in it.
    #[serde(default)]
    config_dir: Option<PathBuf>,
    home: PathBuf,
    path: String,
    #[serde(default)]
    term: Option<String>,
    #[serde(default)]
    tmpdir: Option<PathBuf>,
    #[serde(default)]
    max_thinking_tokens: Option<u64>,
}

#[tokio::main]
async fn main() {
    let definition = PluginDefinition::new(
        PLUGIN_ID,
        "fleetd Claude Code harness",
        env!("CARGO_PKG_VERSION"),
        ALLOWED_ENVIRONMENT,
        prepare_config,
    );
    if let Err(error) = serve(definition).await {
        eprintln!("fleetd Claude Code harness failed: {error}");
        std::process::exit(1);
    }
}

fn prepare_config(value: Value) -> Result<DriverConfig, DriverError> {
    let config: ClaudeConfig = serde_json::from_value(value)?;
    validate_config(&config)?;
    let executable = CHECKS.resolved_executable("adapter executable", &config.executable)?;
    let claude = CHECKS.resolved_executable("claude_executable", &config.claude_executable)?;
    let profile_digest = profile_digest(&config, &executable, &claude)?;
    let mut environment = base_environment(
        &config.home,
        config.path,
        config.term,
        config.tmpdir.as_deref(),
    );
    environment.insert(
        "CLAUDE_CODE_EXECUTABLE".to_owned(),
        claude.to_string_lossy().into_owned(),
    );
    environment.insert("USER".to_owned(), config.user.clone());
    environment.insert("LOGNAME".to_owned(), config.user);
    // Claude Code otherwise creates its per-uid runtime directory at the
    // ambient `/tmp/claude-<uid>`. Keep sockets and cache-break state inside
    // the seat's already-declared private temp root so strict Seatbelt does
    // not need write access to another Claude session's shared runtime state.
    if let Some(tmpdir) = &config.tmpdir {
        environment.insert(
            "CLAUDE_CODE_TMPDIR".to_owned(),
            tmpdir.to_string_lossy().into_owned(),
        );
    }
    if let Some(config_dir) = &config.config_dir {
        environment.insert(
            "CLAUDE_CONFIG_DIR".to_owned(),
            config_dir.to_string_lossy().into_owned(),
        );
    }
    if let Some(budget) = config.max_thinking_tokens {
        environment.insert("MAX_THINKING_TOKENS".to_owned(), budget.to_string());
    }
    Ok(DriverConfig {
        profile_digest,
        runtime: RuntimeConfig {
            expected_name: ADAPTER_AGENT_NAME.to_owned(),
            expected_version: config.expected_version,
            executable: executable.clone(),
            identity_path: executable,
            args: Vec::new(),
            environment,
        },
    })
}

fn validate_config(config: &ClaudeConfig) -> Result<(), DriverError> {
    CHECKS.absolute("adapter executable", &config.executable)?;
    CHECKS.absolute("claude_executable", &config.claude_executable)?;
    CHECKS.non_empty("expected_version", &config.expected_version)?;
    CHECKS.non_empty("user", &config.user)?;
    CHECKS.directory("home", &config.home)?;
    if let Some(config_dir) = &config.config_dir {
        CHECKS.directory("config_dir", config_dir)?;
    }
    CHECKS.non_empty("PATH", &config.path)?;
    if let Some(tmpdir) = &config.tmpdir {
        CHECKS.directory("tmpdir", tmpdir)?;
    }
    Ok(())
}

/// The exact material that makes one Claude Code launch profile distinct.
///
/// Both executables are content-addressed. Upgrading Claude Code changes the
/// profile even though the adapter's reported identity has not moved, which is
/// the point: a native session opened against one Claude Code version must not
/// silently resume under another.
fn profile_digest(
    config: &ClaudeConfig,
    executable: &Path,
    claude: &Path,
) -> Result<String, DriverError> {
    digest_profile(&json!({
        "plugin": PLUGIN_ID,
        "plugin_version": env!("CARGO_PKG_VERSION"),
        "executable": executable,
        "executable_digest": executable_digest(executable)?,
        "expected_version": config.expected_version,
        "claude_executable": claude,
        "claude_executable_digest": executable_digest(claude)?,
        "user": config.user,
        "config_dir": config.config_dir,
        "home": config.home,
        "path": config.path,
        "term": config.term,
        "tmpdir": config.tmpdir,
        "max_thinking_tokens": config.max_thinking_tokens,
    }))
}

#[cfg(test)]
mod tests {
    use std::env;

    use serde_json::json;

    use super::{ADAPTER_AGENT_NAME, prepare_config};

    fn value() -> serde_json::Value {
        json!({
            "executable": env::current_exe().expect("test executable"),
            "expected_version": "0.16.2",
            "claude_executable": env::current_exe().expect("test executable"),
            "user": "operator",
            "home": env::current_dir().expect("current directory"),
            "path": "/usr/bin:/bin",
            "term": "xterm-256color",
            "tmpdir": env::temp_dir(),
        })
    }

    #[test]
    fn the_adapter_is_pinned_and_claude_is_supplied_exactly() {
        let prepared = prepare_config(value()).expect("prepare a Claude Code launch");
        assert_eq!(prepared.runtime.expected_name, ADAPTER_AGENT_NAME);
        assert_eq!(prepared.runtime.expected_version, "0.16.2");
        assert!(prepared.runtime.args.is_empty());
        // The adapter resolves Claude Code from the environment rather than
        // from PATH, so the binary that runs is the one the operator named.
        assert_eq!(
            prepared.runtime.environment["CLAUDE_CODE_EXECUTABLE"],
            env::current_exe()
                .expect("test executable")
                .to_string_lossy()
        );
        // Account identity is explicit rather than inherited.
        assert_eq!(prepared.runtime.environment["USER"], "operator");
        assert_eq!(prepared.runtime.environment["LOGNAME"], "operator");
        assert_eq!(
            prepared.runtime.environment["CLAUDE_CODE_TMPDIR"],
            env::temp_dir().to_string_lossy()
        );
        // Absent unless an operator asked for redirected private state.
        assert!(
            !prepared
                .runtime
                .environment
                .contains_key("CLAUDE_CONFIG_DIR")
        );
        assert!(
            !prepared
                .runtime
                .environment
                .contains_key("MAX_THINKING_TOKENS")
        );
        // Inheriting this is what makes Claude Code refuse to run inside
        // another Claude Code session.
        assert!(!prepared.runtime.environment.contains_key("CLAUDECODE"));
    }

    /// A config directory is a deliberate choice of file-based credentials, so
    /// it reaches the harness when supplied and rotates the profile.
    #[test]
    fn a_supplied_config_directory_is_passed_and_rotates_the_profile() {
        let baseline = prepare_config(value()).expect("prepare").profile_digest;
        let mut with_dir = value();
        with_dir["config_dir"] = json!(env::current_dir().expect("current directory"));
        let prepared = prepare_config(with_dir).expect("prepare");
        assert!(
            prepared
                .runtime
                .environment
                .contains_key("CLAUDE_CONFIG_DIR")
        );
        assert_ne!(prepared.profile_digest, baseline);
    }

    /// The account is part of what makes a launch profile distinct: two seats
    /// authenticating as different accounts are not the same profile, and a
    /// native session must not resume across them.
    #[test]
    fn the_account_participates_in_the_profile() {
        let baseline = prepare_config(value()).expect("prepare").profile_digest;
        let mut other = value();
        other["user"] = json!("someone-else");
        assert_ne!(
            prepare_config(other).expect("prepare").profile_digest,
            baseline
        );
    }

    /// Upgrading Claude Code must rotate the profile even though the adapter's
    /// reported identity has not moved, or a native session would resume across
    /// two different harnesses.
    #[test]
    fn the_profile_covers_claude_code_and_not_only_the_adapter() {
        let baseline = prepare_config(value()).expect("prepare").profile_digest;

        let mut other_claude = value();
        other_claude["claude_executable"] = json!(
            env::current_dir()
                .expect("current directory")
                .join("Cargo.toml")
        );
        let rotated = prepare_config(other_claude)
            .expect("prepare")
            .profile_digest;
        assert_ne!(baseline, rotated);

        let mut thinking = value();
        thinking["max_thinking_tokens"] = json!(8192);
        let with_budget = prepare_config(thinking).expect("prepare");
        assert_ne!(baseline, with_budget.profile_digest);
        assert_eq!(
            with_budget.runtime.environment["MAX_THINKING_TOKENS"],
            "8192"
        );
    }

    /// Nothing outside the declared allowlist may reach the adapter.
    #[test]
    fn every_supplied_environment_name_is_allowlisted() {
        let prepared = prepare_config(value()).expect("prepare");
        for name in prepared.runtime.environment.keys() {
            assert!(
                super::ALLOWED_ENVIRONMENT.contains(&name.as_str()),
                "{name} is not in the plugin's declared environment allowlist"
            );
        }
    }

    #[test]
    fn a_relative_executable_is_refused() {
        let mut relative = value();
        relative["claude_executable"] = json!("claude");
        assert!(prepare_config(relative).is_err());
    }
}
