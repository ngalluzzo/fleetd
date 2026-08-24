#![cfg(unix)]

use std::{path::PathBuf, time::Duration};

use fleetd::{
    Binding, Capability, ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage,
    ManagedHarnessController, ManagedTurn, ManagedTurnOutcome, OpenSession, OpenSessionMode,
    PluginProcess, PluginSpec, PromptBlock, Store, ToolBudget, TurnPolicy,
};
use serde_json::json;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_harness_plugin.py")
}

fn harness_spec(mode: &str) -> PluginSpec {
    PluginSpec::new("mock.harness", "/usr/bin/python3")
        .with_arg(fixture_path())
        .with_arg(mode)
        .require(Capability {
            name: "harness.acp".to_owned(),
            version: 1,
        })
        .with_request_timeout(Duration::from_secs(2))
}

async fn fixture() -> (tempfile::TempDir, Store, fleetd::Agent, fleetd::Invocation) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let sender = store
        .create_agent(CreateAgent {
            name: "controller-sender".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create sender");
    let receiver = store
        .create_agent(CreateAgent {
            name: "controller-receiver".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create receiver");
    let channel = store
        .create_channel(CreateChannel {
            name: "controller-test".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), receiver.id.clone()],
        })
        .await
        .expect("create channel");
    store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: sender.id.clone(),
                idempotency_key: None,
                recipient_id: Some(receiver.id.clone()),
                kind: "work.request/v1".to_owned(),
                payload: json!({"task": "managed test"}),
                correlation_id: Some("controller-test".to_owned()),
                causation_id: None,
            },
        )
        .await
        .expect("append request");
    let invocation = store
        .reserve_invocations(
            &receiver.id,
            ClaimDeliveries {
                limit: 1,
                lease_duration_ms: 30_000,
            },
        )
        .await
        .expect("reserve invocation")
        .invocations
        .pop()
        .expect("one invocation");
    (directory, store, sender, invocation)
}

fn policy() -> TurnPolicy {
    TurnPolicy {
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
    }
}

async fn open_harness(mode: &str) -> (fleetd::HarnessAcpClient, String, Binding) {
    let process = PluginProcess::start(harness_spec(mode))
        .await
        .expect("start harness");
    let harness = process.into_harness_acp().expect("typed harness");
    let binding = Binding {
        binding_id: "controller-binding".to_owned(),
        binding_generation: 1,
        owner_epoch: 1,
    };
    let session = harness
        .open_session(&OpenSession {
            binding: binding.clone(),
            mode: OpenSessionMode::Create,
            working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            profile_digest: "sha256:profile".to_owned(),
        })
        .await
        .expect("open session");
    (harness, session.session_ref, binding)
}

#[tokio::test]
async fn managed_controller_arms_before_turn_and_atomically_completes() {
    let (_directory, store, sender, invocation) = fixture().await;
    let (mut harness, session_ref, binding) = open_harness("healthy").await;
    let outcome = ManagedHarnessController::new(&store)
        .run(
            &mut harness,
            ManagedTurn {
                invocation,
                binding,
                session_ref,
                prompt: vec![PromptBlock::Text {
                    text: "perform managed work".to_owned(),
                }],
                policy: policy(),
                result_kind: "work.result/v1".to_owned(),
            },
        )
        .await
        .expect("run managed turn");
    let ManagedTurnOutcome::Completed(completion) = outcome else {
        panic!("expected completion");
    };
    assert_eq!(
        completion.result.recipient_id.as_deref(),
        Some(sender.id.as_str())
    );
    assert_eq!(completion.result.payload["status"], "completed");
    assert_eq!(
        completion.result.payload["assistant_messages"][0]["content"][0]["text"],
        "done"
    );
    harness.shutdown().await.expect("shutdown harness");
}

#[tokio::test]
async fn managed_controller_parks_post_arm_protocol_ambiguity() {
    let (_directory, store, _sender, invocation) = fixture().await;
    let input_message = invocation.message.id.clone();
    let (mut harness, session_ref, binding) = open_harness("wrong-fence").await;
    let outcome = ManagedHarnessController::new(&store)
        .run(
            &mut harness,
            ManagedTurn {
                invocation,
                binding,
                session_ref,
                prompt: vec![PromptBlock::Text {
                    text: "perform ambiguous work".to_owned(),
                }],
                policy: policy(),
                result_kind: "work.result/v1".to_owned(),
            },
        )
        .await
        .expect("ambiguity must settle by blocking");
    let ManagedTurnOutcome::Blocked(blocked) = outcome else {
        panic!("expected blocked delivery");
    };
    assert_eq!(blocked.message.id, input_message);
    assert!(blocked.reason.contains("turn evidence failed"));
}
