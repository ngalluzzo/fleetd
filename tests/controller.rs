#![cfg(unix)]

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use fleetd::{
    AcquireSessionBinding, Capability, ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage,
    Invocation, InvocationState, ManagedHarnessController, ManagedTurn, ManagedTurnCapability,
    ManagedTurnOutcome, OpenSession, OpenSessionMode, PluginProcess, PluginSpec, PromptBlock,
    SessionAcquisitionMode, SessionBindingState, SessionPersistence, Store, ToolBudget, TurnPolicy,
    TurnResultCapture,
};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};

struct RecordingCapability {
    store: Store,
    activated_after_arm: Arc<AtomicBool>,
    deactivated: Arc<AtomicBool>,
}

impl ManagedTurnCapability for RecordingCapability {
    fn activate<'a>(&'a self, invocation: &'a Invocation) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let armed = self
                .store
                .list_invocations(Some(&invocation.agent_id))
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|candidate| candidate.id == invocation.id)
                .is_some_and(|candidate| candidate.state == InvocationState::DispatchArmed);
            self.activated_after_arm.store(armed, Ordering::SeqCst);
            if armed {
                Ok(())
            } else {
                Err("capability activated before dispatch arm".to_owned())
            }
        })
    }

    fn deactivate<'a>(&'a self, _invocation_id: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            self.deactivated.store(true, Ordering::SeqCst);
        })
    }
}

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

async fn open_harness(
    store: &Store,
    agent_id: &str,
    mode: &str,
) -> (fleetd::HarnessAcpClient, String, fleetd::Binding) {
    let process = PluginProcess::start(harness_spec(mode))
        .await
        .expect("start harness");
    let harness = process.into_harness_acp().expect("typed harness");
    let description = harness.describe().await.expect("describe harness");
    let acquired = store
        .acquire_session_binding(
            agent_id,
            AcquireSessionBinding {
                lane_policy: "per-agent".to_owned(),
                lane_key: "primary".to_owned(),
                owner_instance_id: "controller-test-process".to_owned(),
                profile_digest: description.profile_digest.clone(),
                compatibility_digest: "sha256:mock-harness-v1".to_owned(),
                working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
                additional_directories: Vec::new(),
            },
        )
        .await
        .expect("acquire durable session binding");
    let open_mode = match acquired.mode {
        SessionAcquisitionMode::Create => OpenSessionMode::Create,
        SessionAcquisitionMode::Resume { session_ref } => OpenSessionMode::Resume { session_ref },
    };
    let binding = acquired.session.binding;
    let session = harness
        .open_session(&OpenSession {
            binding: binding.clone(),
            mode: open_mode,
            working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            resolved_mcp_grants: Vec::new(),
            profile_digest: description.profile_digest,
        })
        .await
        .expect("open session");
    store
        .record_session_opened(agent_id, &binding, &session.session_ref)
        .await
        .expect("persist native session reference");
    (harness, session.session_ref, binding)
}

#[tokio::test]
async fn managed_controller_arms_before_turn_and_atomically_completes() {
    let (_directory, store, sender, invocation) = fixture().await;
    let agent_id = invocation.agent_id.clone();
    let invocation_id = invocation.id.clone();
    let (mut harness, session_ref, binding) = open_harness(&store, &agent_id, "healthy").await;
    let activated_after_arm = Arc::new(AtomicBool::new(false));
    let deactivated = Arc::new(AtomicBool::new(false));
    let capability: Arc<dyn ManagedTurnCapability> = Arc::new(RecordingCapability {
        store: store.clone(),
        activated_after_arm: Arc::clone(&activated_after_arm),
        deactivated: Arc::clone(&deactivated),
    });
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
                capabilities: vec![capability],
                result_kind: "work.result/v1".to_owned(),
                result_capture: TurnResultCapture::Transcript,
                result_context: json!({"adapter": "fixture"}),
            },
        )
        .await
        .expect("run managed turn");
    assert!(activated_after_arm.load(Ordering::SeqCst));
    assert!(deactivated.load(Ordering::SeqCst));
    let ManagedTurnOutcome::Completed(completion) = outcome else {
        panic!("expected completion");
    };
    assert_eq!(
        completion.result.recipient_id.as_deref(),
        Some(sender.id.as_str())
    );
    assert_eq!(completion.result.payload["status"], "completed");
    assert_eq!(
        completion.result.payload["result_context"],
        json!({"adapter": "fixture"})
    );
    assert_eq!(
        completion.result.payload["assistant_messages"][0]["content"][0]["text"],
        "done"
    );
    let session = store
        .list_session_bindings(Some(&agent_id))
        .await
        .expect("list durable session")
        .pop()
        .expect("one session");
    assert_eq!(session.state, SessionBindingState::Ready);
    assert_eq!(
        session.last_quiescent_invocation_id.as_deref(),
        Some(invocation_id.as_str())
    );
    assert_eq!(
        session.session_persistence,
        Some(SessionPersistence::RuntimeClaimed)
    );
    harness.shutdown().await.expect("shutdown harness");
}

#[tokio::test]
async fn managed_controller_parks_post_arm_protocol_ambiguity() {
    let (_directory, store, _sender, invocation) = fixture().await;
    let input_message = invocation.message.id.clone();
    let agent_id = invocation.agent_id.clone();
    let invocation_id = invocation.id.clone();
    let (mut harness, session_ref, binding) = open_harness(&store, &agent_id, "wrong-fence").await;
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
                capabilities: Vec::new(),
                result_kind: "work.result/v1".to_owned(),
                result_capture: TurnResultCapture::Transcript,
                result_context: Value::Null,
            },
        )
        .await
        .expect("ambiguity must settle by blocking");
    let ManagedTurnOutcome::Blocked(blocked) = outcome else {
        panic!("expected blocked delivery");
    };
    assert_eq!(blocked.message.id, input_message);
    assert!(blocked.reason.contains("turn evidence failed"));
    let session = store
        .list_session_bindings(Some(&agent_id))
        .await
        .expect("list uncertain session")
        .pop()
        .expect("one session");
    assert_eq!(session.state, SessionBindingState::Uncertain);
    assert_eq!(
        session.active_invocation_id.as_deref(),
        Some(invocation_id.as_str())
    );
    assert_eq!(session.uncertain_reason, Some(blocked.reason));
}
