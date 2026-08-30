#![cfg(unix)]

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use fleetd::execution::invocation;
use fleetd::execution::operations;
use fleetd::execution::session_binding;
use fleetd::{
    execution::controller::{
        ManagedHarnessController, ManagedTurn, ManagedTurnGrant, ManagedTurnOutcome,
        TurnResultCapture,
    },
    execution::operations::NewPluginGeneration,
    execution::permission::PermissionPolicy,
    execution::session_binding::{
        AcquireSessionBinding, SessionAcquisitionMode, SessionBindingState,
    },
    model::{
        ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage, Invocation, InvocationState,
    },
    plugin::{
        OpenSession, OpenSessionMode, PluginProcess, PluginSpec, PromptBlock, SessionPersistence,
        ToolBudget, TurnPolicy, harness_acp_interface,
    },
    store::Store,
};
use futures_util::future::BoxFuture;
use serde_json::{Value, json};

mod common;

struct RecordingGrant {
    store: Store,
    activated_after_arm: Arc<AtomicBool>,
    deactivated: Arc<AtomicBool>,
}

impl ManagedTurnGrant for RecordingGrant {
    fn activate<'a>(&'a self, invocation: &'a Invocation) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            let armed = invocation::list_invocations(&self.store, Some(&invocation.agent_id))
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|candidate| candidate.id == invocation.id)
                .is_some_and(|candidate| candidate.state == InvocationState::DispatchArmed);
            self.activated_after_arm.store(armed, Ordering::SeqCst);
            if armed {
                Ok(())
            } else {
                Err("grant activated before dispatch arm".to_owned())
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
        .require_interface(harness_acp_interface())
        .with_request_timeout(Duration::from_secs(2))
}

async fn fixture() -> (
    tempfile::TempDir,
    Store,
    fleetd::model::Agent,
    fleetd::model::Invocation,
) {
    let common::TempStore {
        directory, store, ..
    } = common::temp_store().await;
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
            members: Vec::new(),
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
    let invocation = invocation::reserve_invocations(
        &store,
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
) -> (
    fleetd::plugin::HarnessAcpClient,
    String,
    fleetd::plugin::Binding,
    String,
) {
    let process = PluginProcess::start(harness_spec(mode))
        .await
        .expect("start harness");
    let harness = process.into_harness_acp().expect("typed harness");
    let description = harness.describe().await.expect("describe harness");
    let generation_id = "controller-test-generation".to_owned();
    operations::record_plugin_generation(
        store,
        NewPluginGeneration {
            id: generation_id.clone(),
            agent_id: agent_id.to_owned(),
            plugin: harness.manifest().plugin.clone(),
            interfaces: harness.manifest().interfaces.clone(),
            process_id: harness.process_id(),
            description: description.clone(),
            compatibility_digest: "sha256:mock-harness-v1".to_owned(),
            heartbeat_interval_ms: 5_000,
        },
    )
    .await
    .expect("record plugin generation");
    let acquired = session_binding::acquire_session_binding(
        store,
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
    session_binding::record_session_opened(store, agent_id, &binding, &session.session_ref)
        .await
        .expect("persist native session reference");
    (harness, session.session_ref, binding, generation_id)
}

#[tokio::test]
async fn managed_controller_arms_before_turn_and_atomically_completes() {
    let (_directory, store, sender, invocation) = fixture().await;
    let agent_id = invocation.agent_id.clone();
    let invocation_id = invocation.id.clone();
    let source_message_id = invocation.message.id.clone();
    let (mut harness, session_ref, binding, generation_id) =
        open_harness(&store, &agent_id, "healthy").await;
    let activated_after_arm = Arc::new(AtomicBool::new(false));
    let deactivated = Arc::new(AtomicBool::new(false));
    let grant: Arc<dyn ManagedTurnGrant> = Arc::new(RecordingGrant {
        store: store.clone(),
        activated_after_arm: Arc::clone(&activated_after_arm),
        deactivated: Arc::clone(&deactivated),
    });
    let outcome = ManagedHarnessController::new(&store)
        .run(
            &mut harness,
            ManagedTurn {
                invocation,
                generation_id,
                binding,
                session_ref,
                prompt: vec![PromptBlock::Text {
                    text: "perform managed work".to_owned(),
                }],
                policy: policy(),
                grants: vec![grant],
                result_kind: "work.result/v1".to_owned(),
                result_capture: TurnResultCapture::Transcript,
                result_context: json!({"adapter": "fixture"}),
                permission_policy: PermissionPolicy::Deny,
                interruption: None,
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
    let observation = operations::list_invocation_observations(
        &store,
        &operations::EvidencePage::newest(Some(&agent_id)),
    )
    .await
    .expect("list durable invocation evidence")
    .pop()
    .expect("one invocation observation");
    assert_eq!(observation.invocation_id, invocation_id.as_str());
    assert_eq!(observation.source_message_id, source_message_id);
    assert_eq!(
        observation.result_message_id.as_deref(),
        Some(completion.result.id.as_str())
    );
    assert_eq!(observation.event_count, 1);
    assert_eq!(observation.counts.assistant, 1);
    assert!(observation.event_chain_digest.is_some());
    assert_eq!(
        observation.execution_certainty,
        Some(fleetd::model::ExecutionCertainty::OutcomeKnown)
    );
    assert_eq!(observation.session_quiescent, Some(true));
    assert_eq!(observation.usage, Some(json!({})));
    let session = session_binding::list_session_bindings(&store, Some(&agent_id))
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
async fn sandboxed_policy_selects_only_the_typed_allow_once_option() {
    let (_directory, store, _sender, invocation) = fixture().await;
    let agent_id = invocation.agent_id.clone();
    let (mut harness, session_ref, binding, generation_id) =
        open_harness(&store, &agent_id, "permission").await;
    let outcome = ManagedHarnessController::new(&store)
        .run(
            &mut harness,
            ManagedTurn {
                invocation,
                generation_id,
                binding,
                session_ref,
                prompt: vec![PromptBlock::Text {
                    text: "perform one bounded write".to_owned(),
                }],
                policy: policy(),
                grants: Vec::new(),
                result_kind: "work.result/v1".to_owned(),
                result_capture: TurnResultCapture::Transcript,
                result_context: Value::Null,
                permission_policy: PermissionPolicy::AllowOnce,
                interruption: None,
            },
        )
        .await
        .expect("resolve one-turn consent and settle");
    let ManagedTurnOutcome::Completed(completion) = outcome else {
        panic!("allow-once request should complete");
    };
    assert_eq!(
        completion.result.payload["assistant_messages"][0]["content"][0]["text"],
        "permission:allow_once"
    );
    let observation = operations::list_invocation_observations(
        &store,
        &operations::EvidencePage::newest(Some(&agent_id)),
    )
    .await
    .expect("read permission evidence")
    .pop()
    .expect("one observation");
    assert_eq!(observation.counts.permission, 1);
    assert_eq!(observation.counts.assistant, 1);
    assert_eq!(observation.event_count, 2);
    harness.shutdown().await.expect("shutdown harness");
}

#[tokio::test]
async fn managed_controller_marks_unavailable_structured_capture_failed() {
    let (_directory, store, _sender, invocation) = fixture().await;
    let agent_id = invocation.agent_id.clone();
    let (mut harness, session_ref, binding, generation_id) =
        open_harness(&store, &agent_id, "healthy").await;
    let outcome = ManagedHarnessController::new(&store)
        .run(
            &mut harness,
            ManagedTurn {
                invocation,
                generation_id,
                binding,
                session_ref,
                prompt: vec![PromptBlock::Text {
                    text: "return structured work".to_owned(),
                }],
                policy: policy(),
                grants: Vec::new(),
                result_kind: "work.attempt/v2".to_owned(),
                result_capture: TurnResultCapture::FinalAssistantJson,
                result_context: Value::Null,
                permission_policy: PermissionPolicy::Deny,
                interruption: None,
            },
        )
        .await
        .expect("settle malformed structured attempt");
    let ManagedTurnOutcome::Completed(completion) = outcome else {
        panic!("known terminal must settle durably");
    };
    assert_eq!(completion.result.payload["status"], "failed");
    assert_eq!(completion.result.payload["stop_reason"], "end_turn");
    assert_eq!(
        completion.result.payload["structured_result"],
        json!({"status": "unavailable", "reason": "malformed_final_json"})
    );
    harness.shutdown().await.expect("shutdown harness");
}

#[tokio::test]
async fn managed_controller_preserves_host_cancellation_over_runtime_end_turn() {
    let (_directory, store, _sender, invocation) = fixture().await;
    let agent_id = invocation.agent_id.clone();
    let (mut harness, session_ref, binding, generation_id) =
        open_harness(&store, &agent_id, "cancel-end-turn").await;
    let mut short_policy = policy();
    short_policy.wall_timeout_ms = 20;
    let outcome = ManagedHarnessController::new(&store)
        .run(
            &mut harness,
            ManagedTurn {
                invocation,
                generation_id,
                binding,
                session_ref,
                prompt: vec![PromptBlock::Text {
                    text: "run until the host deadline".to_owned(),
                }],
                policy: short_policy,
                grants: Vec::new(),
                result_kind: "work.attempt/v1".to_owned(),
                result_capture: TurnResultCapture::Transcript,
                result_context: Value::Null,
                permission_policy: PermissionPolicy::Deny,
                interruption: None,
            },
        )
        .await
        .expect("settle known host cancellation");
    let ManagedTurnOutcome::Completed(completion) = outcome else {
        panic!("known cancelled terminal must settle durably");
    };
    assert_eq!(completion.result.payload["status"], "failed");
    assert_eq!(
        completion.result.payload["stop_reason"],
        "host_wall_deadline"
    );
    assert_eq!(completion.result.payload["runtime_stop_reason"], "end_turn");
    harness.shutdown().await.expect("shutdown harness");
}

#[tokio::test]
async fn managed_controller_parks_post_arm_protocol_ambiguity() {
    let (_directory, store, _sender, invocation) = fixture().await;
    let input_message = invocation.message.id.clone();
    let agent_id = invocation.agent_id.clone();
    let invocation_id = invocation.id.clone();
    let (mut harness, session_ref, binding, generation_id) =
        open_harness(&store, &agent_id, "wrong-fence").await;
    let outcome = ManagedHarnessController::new(&store)
        .run(
            &mut harness,
            ManagedTurn {
                invocation,
                generation_id,
                binding,
                session_ref,
                prompt: vec![PromptBlock::Text {
                    text: "perform ambiguous work".to_owned(),
                }],
                policy: policy(),
                grants: Vec::new(),
                result_kind: "work.result/v1".to_owned(),
                result_capture: TurnResultCapture::Transcript,
                result_context: Value::Null,
                permission_policy: PermissionPolicy::Deny,
                interruption: None,
            },
        )
        .await
        .expect("ambiguity must settle by blocking");
    let ManagedTurnOutcome::Blocked(blocked) = outcome else {
        panic!("expected blocked delivery");
    };
    assert_eq!(blocked.message.id, input_message);
    assert!(blocked.reason.contains("turn evidence failed"));
    let session = session_binding::list_session_bindings(&store, Some(&agent_id))
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
