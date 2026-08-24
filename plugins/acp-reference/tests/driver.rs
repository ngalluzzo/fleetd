#![cfg(unix)]

use std::{path::PathBuf, time::Duration};

use fleetd::{
    Binding, Capability, ExecutionFence, HarnessAcpNotification, OpenSession, OpenSessionMode,
    PluginProcess, PluginSpec, PromptBlock, StartTurn, ToolBudget, TurnPolicy, TurnSource,
};
use serde_json::json;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_acp_agent.py")
}

fn driver_spec() -> PluginSpec {
    PluginSpec::new(
        "fleetd.acp-reference",
        env!("CARGO_BIN_EXE_fleetd-acp-reference"),
    )
    .with_config(json!({
        "profile_digest": "sha256:mock-profile",
        "runtime": {
            "expected_name": "mock-acp",
            "expected_version": "1.0.0",
            "executable": "/usr/bin/python3",
            "identity_path": fixture_path(),
            "args": [fixture_path()],
            "environment": {}
        }
    }))
    .require(Capability {
        name: "harness.acp".to_owned(),
        version: 1,
    })
    .with_initialize_timeout(Duration::from_secs(5))
    .with_request_timeout(Duration::from_secs(5))
}

#[tokio::test]
async fn real_driver_translates_one_typed_acp_turn() {
    let process = PluginProcess::start(driver_spec())
        .await
        .expect("start ACP driver");
    let mut harness = process.into_harness_acp().expect("typed harness");
    let description = harness.describe().await.expect("describe");
    assert_eq!(description.runtime.name, "mock-acp");
    assert_eq!(description.profile_digest, "sha256:mock-profile");
    assert_eq!(
        description.raw_initialize_result["_meta"]["mock"]["preserved"],
        true
    );

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
            resolved_mcp_grants: Vec::new(),
            profile_digest: "sha256:mock-profile".to_owned(),
        })
        .await
        .expect("open session");
    assert_eq!(session.session_ref, "native-session-1");
    assert_eq!(
        session.raw_session_result["_meta"]["new"]["preserved"],
        true
    );

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
    let HarnessAcpNotification::TurnEvent(event) = event else {
        panic!("expected turn event");
    };
    assert_eq!(event.raw_update["mockUnknownField"]["preserved"], true);
    let terminal = harness.next_notification().await.expect("terminal");
    let HarnessAcpNotification::TurnTerminal(terminal) = terminal else {
        panic!("expected terminal");
    };
    assert_eq!(terminal.stop_reason, "end_turn");
    assert_eq!(
        terminal.assistant_messages[0].content[0]["text"],
        "mock answer"
    );
    assert_eq!(
        terminal.raw_prompt_response["_meta"]["prompt"]["preserved"],
        true
    );
    harness.shutdown().await.expect("shutdown driver");
}
