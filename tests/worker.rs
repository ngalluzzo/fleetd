#![cfg(unix)]

use std::{path::PathBuf, time::Duration};

use fleetd::{
    model::{
        ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage, ExecutionCertainty,
        InvocationState,
    },
    operations::{
        PluginGenerationDisposition, PluginGenerationHealth, PluginGenerationState,
        PluginShutdownOutcome,
    },
    plugin::{PluginSpec, ToolBudget, TurnPolicy, harness_acp_interface},
    session_binding::SessionBindingState,
    store::Store,
    worker::{
        ContinuousHarnessWorker, ContinuousWorkerConfig, ContinuousWorkerError,
        EnvelopeTurnAdapter, InboundAcceptance, PreparedTurn, TurnAdapter,
    },
};
use serde_json::json;
use tokio_util::sync::CancellationToken;

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

fn envelope_adapter() -> EnvelopeTurnAdapter {
    EnvelopeTurnAdapter::new("work.result/v1", ["work.request/v1"])
        .expect("valid fixture envelope adapter")
}

#[test]
fn inbound_acceptance_is_bounded_exact_and_canonical() {
    assert!(InboundAcceptance::exact_v1(Vec::<String>::new()).is_err());
    assert!(InboundAcceptance::exact_v1(["work/v1", "work/v1"]).is_err());
    assert!(InboundAcceptance::exact_v1([" "]).is_err());
    assert!(InboundAcceptance::exact_v1(["x".repeat(257)]).is_err());
    assert!(InboundAcceptance::exact_v1((0..129).map(|index| format!("work/{index}"))).is_err());

    let left = InboundAcceptance::exact_v1(["work.beta/v1", "work.alpha/v1"])
        .expect("valid unordered set");
    let right =
        InboundAcceptance::exact_v1(["work.alpha/v1", "work.beta/v1"]).expect("valid ordered set");
    assert_eq!(left, right);
}

fn worker_config(agent_id: &str, mode: &str, working_directory: PathBuf) -> ContinuousWorkerConfig {
    ContinuousWorkerConfig {
        agent_id: agent_id.to_owned(),
        plugin: harness_spec(mode),
        working_directory,
        additional_directories: Vec::new(),
        mcp_grants: Vec::new(),
        compatibility_digest: None,
        lease_duration: Duration::from_secs(70),
        poll_interval: Duration::from_millis(10),
        restart_backoff: Duration::from_millis(10),
        pre_arm_retry_delay: Duration::ZERO,
        turn_policy: policy(),
    }
}

struct Fixture {
    directory: tempfile::TempDir,
    store: Store,
    sender_id: String,
    receiver_id: String,
    channel_id: String,
}

async fn fixture(message_count: usize) -> Fixture {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let sender = store
        .create_agent(CreateAgent {
            name: "worker-sender".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create sender");
    let receiver = store
        .create_agent(CreateAgent {
            name: "worker-receiver".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create receiver");
    let channel = store
        .create_channel(CreateChannel {
            name: "worker-test".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), receiver.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");
    for sequence in 0..message_count {
        store
            .append_message(
                &channel.id,
                CreateMessage {
                    sender_id: sender.id.clone(),
                    idempotency_key: None,
                    recipient_id: Some(receiver.id.clone()),
                    kind: "work.request/v1".to_owned(),
                    payload: json!({"task": format!("worker test {sequence}")}),
                    correlation_id: Some("worker-test".to_owned()),
                    causation_id: None,
                },
            )
            .await
            .expect("append request");
    }
    Fixture {
        directory,
        store,
        sender_id: sender.id,
        receiver_id: receiver.id,
        channel_id: channel.id,
    }
}

async fn append_direct_kind(
    fixture: &Fixture,
    kind: &str,
    sequence: usize,
) -> fleetd::model::Message {
    fixture
        .store
        .append_message(
            &fixture.channel_id,
            CreateMessage {
                sender_id: fixture.sender_id.clone(),
                idempotency_key: None,
                recipient_id: Some(fixture.receiver_id.clone()),
                kind: kind.to_owned(),
                payload: json!({"task": format!("selected worker test {sequence}")}),
                correlation_id: Some("selected-worker-test".to_owned()),
                causation_id: None,
            },
        )
        .await
        .expect("append selected request")
}

#[tokio::test]
async fn continuous_worker_drains_multiple_turns_on_one_session_lane() {
    let fixture = fixture(2).await;
    let worker = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(
            &fixture.receiver_id,
            "healthy",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        ),
        envelope_adapter(),
    )
    .expect("valid worker");

    let report = worker
        .run_until(CancellationToken::new(), Some(2))
        .await
        .expect("drain two turns");

    assert_eq!(report.completed, 2);
    assert_eq!(report.blocked, 0);
    assert_eq!(report.plugin_generations, 1);
    assert_eq!(report.reservations, 2);
    let generations = fixture
        .store
        .list_plugin_generations(Some(&fixture.receiver_id))
        .await
        .expect("list durable plugin generations");
    assert_eq!(generations.len(), 1);
    assert_eq!(generations[0].state, PluginGenerationState::Stopped);
    assert_eq!(generations[0].health, PluginGenerationHealth::Stopped);
    assert_eq!(
        generations[0].stop_disposition,
        Some(PluginGenerationDisposition::Stopped)
    );
    assert_eq!(
        generations[0].shutdown_outcome,
        Some(PluginShutdownOutcome::Graceful)
    );
    let observations = fixture
        .store
        .list_invocation_observations(Some(&fixture.receiver_id))
        .await
        .expect("list bounded invocation observations");
    assert_eq!(observations.len(), 2);
    assert!(observations.iter().all(|observation| {
        observation.generation_id == generations[0].id
            && observation.event_count == 1
            && observation.counts.assistant == 1
            && observation.event_chain_digest.is_some()
            && observation.execution_certainty == Some(ExecutionCertainty::OutcomeKnown)
            && observation.session_quiescent == Some(true)
            && observation.usage == Some(json!({}))
    }));
    let sessions = fixture
        .store
        .list_session_bindings(Some(&fixture.receiver_id))
        .await
        .expect("list sessions");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].state, SessionBindingState::Ready);
    assert_eq!(sessions[0].binding.binding_generation, 1);
    assert_eq!(sessions[0].binding.owner_epoch, 1);
    let history = fixture
        .store
        .list_messages(&fixture.channel_id, Some(&fixture.sender_id), 0, 100)
        .await
        .expect("list history");
    assert_eq!(
        history
            .messages
            .iter()
            .filter(|message| message.kind == "work.result/v1")
            .count(),
        2
    );
}

#[tokio::test]
async fn worker_skips_earlier_unaccepted_delivery_without_leasing_it() {
    let fixture = fixture(0).await;
    let skipped = append_direct_kind(&fixture, "work.result/v1", 0).await;
    let accepted = append_direct_kind(&fixture, "work.request/v1", 1).await;
    let worker = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(
            &fixture.receiver_id,
            "healthy",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        ),
        envelope_adapter(),
    )
    .expect("valid worker");

    let report = worker
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("execute accepted message");

    assert_eq!(report.reservations, 1);
    assert_eq!(report.completed, 1);
    let invocation = fixture
        .store
        .list_invocations(Some(&fixture.receiver_id))
        .await
        .expect("list invocations")
        .pop()
        .expect("one accepted invocation");
    assert_eq!(invocation.message.id, accepted.id);

    let skipped_claim = fixture
        .store
        .claim_deliveries(
            &fixture.receiver_id,
            ClaimDeliveries {
                limit: 1,
                lease_duration_ms: 10_000,
            },
        )
        .await
        .expect("claim skipped delivery through the unfiltered kernel API");
    assert_eq!(skipped_claim.deliveries.len(), 1);
    assert_eq!(skipped_claim.deliveries[0].message.id, skipped.id);
    assert_eq!(skipped_claim.deliveries[0].attempt, 1);
}

#[tokio::test]
async fn changed_inbound_contract_rotates_the_session_compatibility_generation() {
    let fixture = fixture(0).await;
    append_direct_kind(&fixture, "work.alpha/v1", 0).await;
    append_direct_kind(&fixture, "work.beta/v1", 1).await;
    let working_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let first = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(&fixture.receiver_id, "healthy", working_directory.clone()),
        EnvelopeTurnAdapter::new("work.result/v1", ["work.alpha/v1"]).expect("valid alpha adapter"),
    )
    .expect("valid first worker");
    first
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("complete alpha turn");

    let second = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(&fixture.receiver_id, "healthy", working_directory),
        EnvelopeTurnAdapter::new("work.result/v1", ["work.beta/v1"]).expect("valid beta adapter"),
    )
    .expect("valid second worker");
    second
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("complete beta turn");

    let sessions = fixture
        .store
        .list_session_bindings(Some(&fixture.receiver_id))
        .await
        .expect("list rotated bindings");
    assert_eq!(sessions.len(), 2);
    assert!(sessions.iter().any(|session| {
        session.binding.binding_generation == 1 && session.state == SessionBindingState::Retired
    }));
    assert!(sessions.iter().any(|session| {
        session.binding.binding_generation == 2 && session.state == SessionBindingState::Ready
    }));
}

#[tokio::test]
async fn fresh_worker_generation_adopts_and_resumes_ready_session() {
    let fixture = fixture(2).await;
    let working_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let first = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(&fixture.receiver_id, "healthy", working_directory.clone()),
        envelope_adapter(),
    )
    .expect("valid first worker");
    first
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("first generation completes");

    let second = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(&fixture.receiver_id, "healthy", working_directory),
        envelope_adapter(),
    )
    .expect("valid second worker");
    second
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("second generation resumes");

    let sessions = fixture
        .store
        .list_session_bindings(Some(&fixture.receiver_id))
        .await
        .expect("list sessions");
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].state, SessionBindingState::Ready);
    assert_eq!(sessions[0].binding.binding_generation, 1);
    assert_eq!(sessions[0].binding.owner_epoch, 2);
}

#[tokio::test]
async fn session_open_crash_releases_unarmed_work_and_restarts_generation() {
    let fixture = fixture(1).await;
    let marker = fixture.directory.path().join("open-failed-once");
    let mut config = worker_config(
        &fixture.receiver_id,
        "healthy",
        PathBuf::from(env!("CARGO_MANIFEST_DIR")),
    );
    config.plugin = PluginSpec::new("mock.harness", "/usr/bin/python3")
        .with_arg(fixture_path())
        .with_arg("fail-open-once")
        .with_arg(&marker)
        .require_interface(harness_acp_interface())
        .with_request_timeout(Duration::from_secs(2));
    let worker = ContinuousHarnessWorker::new(&fixture.store, config, envelope_adapter())
        .expect("valid worker");

    let report = worker
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("replacement generation completes");

    assert_eq!(report.plugin_generations, 2);
    assert_eq!(report.operational_restarts, 1);
    assert_eq!(report.reservations, 2);
    assert_eq!(report.pre_arm_retries, 1);
    assert_eq!(report.completed, 1);
    let sessions = fixture
        .store
        .list_session_bindings(Some(&fixture.receiver_id))
        .await
        .expect("list generations");
    assert_eq!(sessions.len(), 2);
    assert!(sessions.iter().any(|session| {
        session.binding.binding_generation == 2 && session.state == SessionBindingState::Ready
    }));
}

#[tokio::test]
async fn cancellation_during_idle_poll_shuts_down_cleanly() {
    let fixture = fixture(0).await;
    let worker = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(
            &fixture.receiver_id,
            "healthy",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        ),
        envelope_adapter(),
    )
    .expect("valid worker");
    let cancellation = CancellationToken::new();
    let trigger = cancellation.clone();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        trigger.cancel();
    });

    let report = worker.run(cancellation).await.expect("cancel worker");
    canceller.await.expect("join cancellation task");

    assert_eq!(report.completed, 0);
    assert_eq!(report.reservations, 0);
    assert!(report.idle_polls > 0);
}

struct CancellingAdapter {
    cancellation: CancellationToken,
    delegate: EnvelopeTurnAdapter,
}

impl TurnAdapter for CancellingAdapter {
    fn inbound_acceptance(&self) -> &InboundAcceptance {
        self.delegate.inbound_acceptance()
    }

    fn prepare(&self, invocation: &fleetd::model::Invocation) -> Result<PreparedTurn, String> {
        self.cancellation.cancel();
        self.delegate.prepare(invocation)
    }
}

#[tokio::test]
async fn cancellation_after_reservation_releases_work_before_dispatch() {
    let fixture = fixture(1).await;
    let cancellation = CancellationToken::new();
    let worker = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(
            &fixture.receiver_id,
            "healthy",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        ),
        CancellingAdapter {
            cancellation: cancellation.clone(),
            delegate: envelope_adapter(),
        },
    )
    .expect("valid worker");

    let report = worker.run(cancellation).await.expect("cancel before arm");

    assert_eq!(report.reservations, 1);
    assert_eq!(report.pre_arm_retries, 1);
    assert_eq!(report.settled_turns(), 0);
    assert!(
        fixture
            .store
            .list_session_bindings(Some(&fixture.receiver_id))
            .await
            .expect("list sessions")
            .is_empty()
    );
    let invocation = fixture
        .store
        .list_invocations(Some(&fixture.receiver_id))
        .await
        .expect("list invocations")
        .pop()
        .expect("one invocation");
    assert_eq!(invocation.state, InvocationState::Terminal);
    assert_eq!(
        invocation.execution_certainty,
        Some(ExecutionCertainty::NotStarted)
    );
}

#[tokio::test]
async fn post_arm_protocol_ambiguity_is_blocked_without_reexecution() {
    let fixture = fixture(1).await;
    let worker = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(
            &fixture.receiver_id,
            "wrong-fence",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        ),
        envelope_adapter(),
    )
    .expect("valid worker");

    let report = worker
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("ambiguity settles by blocking");

    assert_eq!(report.blocked, 1);
    assert_eq!(report.completed, 0);
    assert_eq!(report.operational_restarts, 0);
    let blocked = fixture
        .store
        .list_blocked_deliveries(Some(&fixture.receiver_id))
        .await
        .expect("list blocks");
    assert_eq!(blocked.len(), 1);
    let session = fixture
        .store
        .list_session_bindings(Some(&fixture.receiver_id))
        .await
        .expect("list sessions")
        .pop()
        .expect("one session");
    assert_eq!(session.state, SessionBindingState::Uncertain);
}

struct RejectingAdapter {
    inbound_acceptance: InboundAcceptance,
}

impl TurnAdapter for RejectingAdapter {
    fn inbound_acceptance(&self) -> &InboundAcceptance {
        &self.inbound_acceptance
    }

    fn prepare(&self, _invocation: &fleetd::model::Invocation) -> Result<PreparedTurn, String> {
        Err("unsupported work kind".to_owned())
    }
}

#[tokio::test]
async fn adapter_failure_releases_only_the_unarmed_reservation() {
    let fixture = fixture(1).await;
    let worker = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(
            &fixture.receiver_id,
            "healthy",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        ),
        RejectingAdapter {
            inbound_acceptance: InboundAcceptance::exact_v1(["work.request/v1"])
                .expect("valid fixture acceptance"),
        },
    )
    .expect("valid worker");

    let error = worker
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect_err("adapter failure stops the worker");
    assert!(matches!(error, ContinuousWorkerError::Adapter(_)));
    let invocation = fixture
        .store
        .list_invocations(Some(&fixture.receiver_id))
        .await
        .expect("list invocations")
        .pop()
        .expect("one invocation");
    assert_eq!(invocation.state, InvocationState::Terminal);
    assert_eq!(
        invocation.execution_certainty,
        Some(ExecutionCertainty::NotStarted)
    );
    assert!(
        fixture
            .store
            .list_blocked_deliveries(Some(&fixture.receiver_id))
            .await
            .expect("list blocks")
            .is_empty()
    );
}

#[tokio::test]
async fn worker_rejects_unknown_or_duplicate_mcp_grants_before_startup() {
    let fixture = fixture(0).await;
    let mut config = worker_config(
        &fixture.receiver_id,
        "healthy",
        PathBuf::from(env!("CARGO_MANIFEST_DIR")),
    );
    config.mcp_grants = vec!["fleet.messaging.unknown".to_owned()];
    let error = ContinuousHarnessWorker::new(&fixture.store, config, envelope_adapter())
        .err()
        .expect("unknown grant must fail");
    assert!(matches!(error, ContinuousWorkerError::InvalidConfig(_)));

    let mut config = worker_config(
        &fixture.receiver_id,
        "healthy",
        PathBuf::from(env!("CARGO_MANIFEST_DIR")),
    );
    config.mcp_grants = vec![
        fleetd::message_grant_broker::PUBLISH_DURABLE_MESSAGE_GRANT.to_owned(),
        fleetd::message_grant_broker::PUBLISH_DURABLE_MESSAGE_GRANT.to_owned(),
    ];
    let error = ContinuousHarnessWorker::new(&fixture.store, config, envelope_adapter())
        .err()
        .expect("duplicate grant must fail");
    assert!(matches!(error, ContinuousWorkerError::InvalidConfig(_)));
}
