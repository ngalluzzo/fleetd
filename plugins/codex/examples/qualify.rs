use std::{path::PathBuf, time::Duration};

use fleetd::{
    Binding, Capability, ExecutionFence, HarnessAcpNotification, OpenSession, OpenSessionMode,
    PermissionOutcome, PermissionResolution, PluginProcess, PluginSpec, PromptBlock, StartTurn,
    ToolBudget, TurnPolicy, TurnSource,
};
use serde_json::json;
use uuid::Uuid;

struct Arguments {
    plugin: String,
    codex: String,
    expected_version: String,
    home: String,
    codex_home: String,
    path: String,
    cwd: String,
    prompt: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = parse_arguments()?;
    let process = PluginProcess::start(plugin_spec(&arguments)).await?;
    let mut harness = process.into_harness_acp()?;
    let description = harness.describe().await?;
    println!("{}", serde_json::to_string_pretty(&description)?);

    let binding_id = Uuid::new_v4().to_string();
    let session = harness
        .open_session(&OpenSession {
            binding: Binding {
                binding_id: binding_id.clone(),
                binding_generation: 1,
                owner_epoch: 1,
            },
            mode: OpenSessionMode::Create,
            working_directory: arguments.cwd,
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            resolved_mcp_grants: Vec::new(),
            profile_digest: description.profile_digest,
        })
        .await?;
    println!("session_ref={}", session.session_ref);

    if let Some(prompt) = arguments.prompt {
        run_prompt(&mut harness, binding_id, session.session_ref, prompt).await?;
    }
    harness.shutdown().await?;
    Ok(())
}

fn parse_arguments() -> Result<Arguments, Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    Ok(Arguments {
        plugin: required(&mut arguments, "Codex plugin executable")?,
        codex: required(&mut arguments, "Codex adapter executable")?,
        expected_version: required(&mut arguments, "expected Codex version")?,
        home: required(&mut arguments, "home directory")?,
        codex_home: required(&mut arguments, "CODEX_HOME directory")?,
        path: required(&mut arguments, "explicit runtime PATH")?,
        cwd: required(&mut arguments, "working directory")?,
        prompt: arguments.next(),
    })
}

fn plugin_spec(arguments: &Arguments) -> PluginSpec {
    PluginSpec::new("fleetd.harness.codex", PathBuf::from(&arguments.plugin))
        .with_config(json!({
            "executable": arguments.codex,
            "expected_version": arguments.expected_version,
            "home": arguments.home,
            "codex_home": arguments.codex_home,
            "path": arguments.path,
            "term": std::env::var("TERM").unwrap_or_else(|_| "xterm-256color".to_owned()),
            "tmpdir": std::env::temp_dir(),
        }))
        .require(Capability {
            name: "harness.acp".to_owned(),
            version: 1,
        })
        .with_initialize_timeout(Duration::from_secs(30))
        .with_request_timeout(Duration::from_secs(30))
        .with_shutdown_timeout(Duration::from_secs(5))
}

async fn run_prompt(
    harness: &mut fleetd::HarnessAcpClient,
    binding_id: String,
    session_ref: String,
    prompt: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let fence = ExecutionFence {
        binding_id,
        binding_generation: 1,
        owner_epoch: 1,
        invocation_id: Uuid::new_v4().to_string(),
        fence_token: Uuid::new_v4().to_string(),
    };
    harness
        .start_turn(&StartTurn {
            fence: fence.clone(),
            session_ref,
            source: TurnSource {
                agent_id: "codex-qualification".to_owned(),
                message_id: Uuid::new_v4().to_string(),
                channel_id: "codex-qualification".to_owned(),
                sender_id: "qualification-operator".to_owned(),
                correlation_id: None,
                causation_id: None,
            },
            prompt: vec![PromptBlock::Text { text: prompt }],
            policy: qualification_policy(),
        })
        .await?;
    loop {
        match harness.next_notification().await? {
            HarnessAcpNotification::TurnEvent(event) => {
                println!("event {} {}", event.event_seq, event.classification);
            }
            HarnessAcpNotification::PermissionRequested(permission) => {
                harness
                    .resolve_permission(&PermissionResolution {
                        fence: fence.clone(),
                        permission_id: permission.permission_id,
                        outcome: PermissionOutcome::Cancelled,
                    })
                    .await?;
            }
            HarnessAcpNotification::TurnTerminal(terminal) => {
                println!("{}", serde_json::to_string_pretty(&terminal)?);
                return Ok(());
            }
        }
    }
}

fn qualification_policy() -> TurnPolicy {
    TurnPolicy {
        idle_timeout_ms: 120_000,
        wall_timeout_ms: 600_000,
        cancel_drain_timeout_ms: 15_000,
        max_captured_output_bytes: 512 * 1024,
        permission_policy: "controller".to_owned(),
        tool_budget: ToolBudget {
            limit: 8,
            required_enforcement: "observe_then_cancel".to_owned(),
        },
        token_budget: None,
    }
}

fn required(
    arguments: &mut impl Iterator<Item = String>,
    label: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    arguments
        .next()
        .ok_or_else(|| format!("missing {label}").into())
}
