#![cfg(unix)]

use std::{path::PathBuf, time::Duration};

use fleetd::{
    Binding, Capability, ExecutionFence, HarnessAcpNotification, OpenSession, OpenSessionMode,
    PluginProcess, PluginSpec, PromptBlock, StartTurn, ToolBudget, TurnPolicy, TurnSource,
};
use serde_json::json;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_opencode.py")
}

fn plugin_spec() -> PluginSpec {
    PluginSpec::new(
        "fleetd.harness.opencode",
        env!("CARGO_BIN_EXE_fleetd-harness-opencode"),
    )
    .with_config(json!({
        "executable": fixture_path(),
        "expected_version": "1.4.0",
        "model": "zai-coding-plan/glm-5.3",
        "home": std::env::temp_dir(),
        "path": "/usr/bin:/bin",
        "term": "xterm-256color",
        "tmpdir": std::env::temp_dir(),
    }))
    .require(Capability {
        name: "harness.acp".to_owned(),
        version: 1,
    })
    .with_initialize_timeout(Duration::from_secs(5))
    .with_request_timeout(Duration::from_secs(5))
    .with_shutdown_timeout(Duration::from_secs(5))
}

#[tokio::test]
async fn opencode_is_a_distinct_typed_harness_plugin() {
    let process = PluginProcess::start(plugin_spec())
        .await
        .expect("start OpenCode plugin");
    assert_eq!(process.manifest().plugin.id, "fleetd.harness.opencode");
    let mut harness = process.into_harness_acp().expect("typed harness");
    let description = harness.describe().await.expect("describe");
    assert_eq!(description.runtime.name, "OpenCode");
    assert_eq!(description.runtime.version, "1.4.0");
    assert!(description.profile_digest.starts_with("sha256:"));

    let session = harness
        .open_session(&OpenSession {
            binding: Binding {
                binding_id: "binding-1".to_owned(),
                binding_generation: 1,
                owner_epoch: 1,
            },
            mode: OpenSessionMode::Create,
            working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            profile_digest: description.profile_digest,
        })
        .await
        .expect("open OpenCode session");
    harness
        .start_turn(&StartTurn {
            fence: ExecutionFence {
                binding_id: "binding-1".to_owned(),
                binding_generation: 1,
                owner_epoch: 1,
                invocation_id: "invocation-1".to_owned(),
                fence_token: "fence-1".to_owned(),
            },
            session_ref: session.session_ref,
            source: TurnSource {
                agent_id: "agent-1".to_owned(),
                message_id: "message-1".to_owned(),
                channel_id: "channel-1".to_owned(),
                sender_id: "sender-1".to_owned(),
                correlation_id: None,
                causation_id: None,
            },
            prompt: vec![PromptBlock::Text {
                text: "test prompt".to_owned(),
            }],
            policy: TurnPolicy {
                idle_timeout_ms: 2_000,
                wall_timeout_ms: 5_000,
                cancel_drain_timeout_ms: 500,
                max_captured_output_bytes: 4_096,
                permission_policy: "controller".to_owned(),
                tool_budget: ToolBudget {
                    limit: 8,
                    required_enforcement: "observe_then_cancel".to_owned(),
                },
                token_budget: None,
            },
        })
        .await
        .expect("start turn");

    let event = harness.next_notification().await.expect("turn event");
    assert!(matches!(event, HarnessAcpNotification::TurnEvent(_)));
    let terminal = harness.next_notification().await.expect("terminal");
    let HarnessAcpNotification::TurnTerminal(terminal) = terminal else {
        panic!("expected terminal");
    };
    assert_eq!(
        terminal.assistant_messages[0].content[0]["text"],
        "OpenCode answer"
    );
    harness.shutdown().await.expect("shutdown OpenCode plugin");
}
