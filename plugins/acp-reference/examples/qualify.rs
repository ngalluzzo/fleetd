use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use fleetd::{
    Binding, Capability, ExecutionFence, HarnessAcpClient, HarnessAcpNotification, OpenSession,
    OpenSessionMode, PermissionOutcome, PermissionResolution, PluginProcess, PluginSpec,
    PromptBlock, StartTurn, ToolBudget, TurnPolicy, TurnSource,
};
use serde_json::json;
use uuid::Uuid;

struct QualificationArgs {
    driver: String,
    node: String,
    adapter: String,
    expected_name: String,
    expected_version: String,
    cwd: String,
    runtime_args: Vec<String>,
    prompt: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = parse_arguments()?;
    let profile_digest = format!(
        "qualification:{}:{}",
        arguments.expected_name, arguments.expected_version
    );
    let process = PluginProcess::start(plugin_spec(&arguments, &profile_digest)).await?;
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
            profile_digest,
        })
        .await?;
    println!("session_ref={}", session.session_ref);

    if let Some(prompt) = arguments.prompt {
        run_prompt(&mut harness, binding_id, session.session_ref, prompt).await?;
    }
    harness.shutdown().await?;
    Ok(())
}

fn parse_arguments() -> Result<QualificationArgs, Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let driver = required(&mut arguments, "driver executable")?;
    let node = required(&mut arguments, "Node executable")?;
    let adapter = required(&mut arguments, "adapter script")?;
    let expected_name = required(&mut arguments, "expected ACP name")?;
    let expected_version = required(&mut arguments, "expected ACP version")?;
    let cwd = required(&mut arguments, "working directory")?;
    let runtime_args = serde_json::from_str(&required(
        &mut arguments,
        "JSON array of additional runtime arguments",
    )?)?;
    Ok(QualificationArgs {
        driver,
        node,
        adapter,
        expected_name,
        expected_version,
        cwd,
        runtime_args,
        prompt: arguments.next(),
    })
}

fn plugin_spec(arguments: &QualificationArgs, profile_digest: &str) -> PluginSpec {
    let mut inner_args = vec![arguments.adapter.clone()];
    inner_args.extend(arguments.runtime_args.clone());
    PluginSpec::new(
        "fleetd.acp-reference",
        PathBuf::from(arguments.driver.clone()),
    )
    .with_config(json!({
        "profile_digest": profile_digest,
        "runtime": {
            "expected_name": arguments.expected_name,
            "expected_version": arguments.expected_version,
            "executable": arguments.node,
            "identity_path": arguments.adapter,
            "args": inner_args,
            "environment": allowed_environment(),
        }
    }))
    .require(Capability {
        name: "harness.acp".to_owned(),
        version: 1,
    })
    .with_initialize_timeout(Duration::from_secs(30))
    .with_request_timeout(Duration::from_secs(30))
    .with_shutdown_timeout(Duration::from_secs(5))
}

fn allowed_environment() -> BTreeMap<String, String> {
    ["HOME", "PATH", "TERM", "TMPDIR"]
        .into_iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| (name.to_owned(), value))
        })
        .collect()
}

async fn run_prompt(
    harness: &mut HarnessAcpClient,
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
                agent_id: "qualification-agent".to_owned(),
                message_id: Uuid::new_v4().to_string(),
                channel_id: "qualification-channel".to_owned(),
                sender_id: "qualification-operator".to_owned(),
                correlation_id: None,
                causation_id: None,
            },
            prompt: vec![PromptBlock::Text { text: prompt }],
            policy: qualification_policy(),
        })
        .await?;
    drain_prompt(harness, &fence).await
}

async fn drain_prompt(
    harness: &mut HarnessAcpClient,
    fence: &ExecutionFence,
) -> Result<(), Box<dyn std::error::Error>> {
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
