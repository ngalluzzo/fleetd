#![cfg(unix)]

use std::{path::PathBuf, time::Duration};

use fleetd::{
    CAPABILITY_WORK_ATTEMPT_KIND, CAPABILITY_WORK_REQUEST_KIND, Capability,
    CapabilityAttemptProjection, CapabilityProviderDescriptor, CapabilityWorkRequest,
    CapabilityWorkTurnAdapter, ClaimDeliveries, ContinuousHarnessWorker, ContinuousWorkerConfig,
    ContinuousWorkerError, CreateAgent, CreateChannel, CreateMessage, EnvelopeTurnAdapter,
    ExecutionCertainty, InvocationState, PluginSpec, PreparedTurn, SessionBindingState, Store,
    ToolBudget, TurnAdapter, TurnPolicy, extract_capability_message,
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
        .require(Capability {
            name: "harness.acp".to_owned(),
            version: 1,
        })
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

fn gooir_request() -> CapabilityWorkRequest {
    serde_json::from_str(include_str!("fixtures/gooir_runnable_web_request.json"))
        .expect("decode GOOIR request")
}

fn provider(request: &CapabilityWorkRequest) -> CapabilityProviderDescriptor {
    CapabilityProviderDescriptor {
        id: fleetd::ExactIdentity::new("dev.fleetd.provider", "fixture_runnable_web", "0.1.0"),
        capability: request.body.capability.clone(),
        implementation_digest: format!("sha256:{}", "a".repeat(64)),
    }
}

async fn append_gooir_request(
    fixture: &Fixture,
    request: &CapabilityWorkRequest,
) -> fleetd::Message {
    fixture
        .store
        .append_message(
            &fixture.channel_id,
            CreateMessage {
                sender_id: fixture.sender_id.clone(),
                idempotency_key: Some(format!("capability-request/{}", request.request_id)),
                recipient_id: Some(fixture.receiver_id.clone()),
                kind: CAPABILITY_WORK_REQUEST_KIND.to_owned(),
                payload: serde_json::to_value(request).expect("encode GOOIR request"),
                correlation_id: Some(request.request_id.clone()),
                causation_id: None,
            },
        )
        .await
        .expect("append capability request")
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
        EnvelopeTurnAdapter::new("work.result/v1"),
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
async fn fresh_worker_generation_adopts_and_resumes_ready_session() {
    let fixture = fixture(2).await;
    let working_directory = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let first = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(&fixture.receiver_id, "healthy", working_directory.clone()),
        EnvelopeTurnAdapter::new("work.result/v1"),
    )
    .expect("valid first worker");
    first
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("first generation completes");

    let second = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(&fixture.receiver_id, "healthy", working_directory),
        EnvelopeTurnAdapter::new("work.result/v1"),
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
async fn gooir_capability_request_binds_exact_work_to_one_owned_session_lane() {
    let fixture = fixture(0).await;
    let request = gooir_request();
    request.validate().expect("validate GOOIR request");
    let source = append_gooir_request(&fixture, &request).await;
    let adapter =
        CapabilityWorkTurnAdapter::new([provider(&request)]).expect("configure exact capability");
    let worker = ContinuousHarnessWorker::new(
        &fixture.store,
        worker_config(
            &fixture.receiver_id,
            "capability-candidate",
            PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        ),
        adapter,
    )
    .expect("valid capability worker");

    let report = worker
        .run_until(CancellationToken::new(), Some(1))
        .await
        .expect("execute capability request");

    assert_eq!(report.completed, 1);
    let invocation = fixture
        .store
        .list_invocations(Some(&fixture.receiver_id))
        .await
        .expect("list invocations")
        .pop()
        .expect("one invocation");
    assert_eq!(invocation.state, InvocationState::Terminal);
    assert_eq!(invocation.message.id, source.id);
    assert_eq!(
        invocation.message.payload,
        serde_json::to_value(&request).expect("encode request again")
    );
    let session = fixture
        .store
        .list_session_bindings(Some(&fixture.receiver_id))
        .await
        .expect("list session bindings")
        .pop()
        .expect("one capability session");
    assert_eq!(session.lane_policy, "per-work-contract");
    assert_eq!(session.lane_key, request.request_id);
    assert_eq!(session.binding.owner_epoch, 1);
    assert_eq!(session.last_quiescent_invocation_id, Some(invocation.id));
    let history = fixture
        .store
        .list_messages(&fixture.channel_id, Some(&fixture.sender_id), 0, 100)
        .await
        .expect("list work history");
    let attempt = history
        .messages
        .iter()
        .find(|message| message.kind == CAPABILITY_WORK_ATTEMPT_KIND)
        .expect("capability attempt result");
    assert_eq!(attempt.correlation_id, Some(request.request_id.clone()));
    assert_eq!(attempt.causation_id, Some(source.id));
    let projection = extract_capability_message(&request, attempt)
        .expect("lift raw attempt into exact candidate");
    let CapabilityAttemptProjection::Candidate(candidate) = projection else {
        panic!("mock provider emitted a candidate")
    };
    assert_eq!(candidate.body.provider, provider(&request));
    assert_eq!(
        candidate.body.outputs[0].payload["artifact"],
        "mock-runnable-web"
    );
}

#[tokio::test]
async fn capability_adapter_rejects_an_unadmitted_exact_capability() {
    let fixture = fixture(0).await;
    let request = gooir_request();
    append_gooir_request(&fixture, &request).await;
    let reservation = fixture
        .store
        .reserve_invocations(
            &fixture.receiver_id,
            ClaimDeliveries {
                limit: 1,
                lease_duration_ms: 70_000,
            },
        )
        .await
        .expect("reserve exact request");
    let invocation = reservation
        .invocations
        .first()
        .expect("one reserved request");
    let adapter = CapabilityWorkTurnAdapter::new([CapabilityProviderDescriptor {
        id: fleetd::ExactIdentity::new("dev.fleetd.provider", "different", "0.1.0"),
        capability: fleetd::ExactIdentity::new(
            "dev.fleetd.capability",
            "different_capability",
            "0.1.0",
        ),
        implementation_digest: format!("sha256:{}", "b".repeat(64)),
    }])
    .expect("configure another capability");

    let error = adapter
        .prepare(invocation)
        .expect_err("unadmitted capability must fail closed");

    assert!(error.contains("does not admit exact capability"));
    assert_eq!(invocation.state, InvocationState::Reserved);
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
        .require(Capability {
            name: "harness.acp".to_owned(),
            version: 1,
        })
        .with_request_timeout(Duration::from_secs(2));
    let worker = ContinuousHarnessWorker::new(
        &fixture.store,
        config,
        EnvelopeTurnAdapter::new("work.result/v1"),
    )
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
        EnvelopeTurnAdapter::new("work.result/v1"),
    )
    .expect("valid worker");
    let cancellation = CancellationToken::new();
    let trigger = cancellation.clone();
    let canceller = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
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
    fn prepare(&self, invocation: &fleetd::Invocation) -> Result<PreparedTurn, String> {
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
            delegate: EnvelopeTurnAdapter::new("work.result/v1"),
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
        EnvelopeTurnAdapter::new("work.result/v1"),
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

struct RejectingAdapter;

impl TurnAdapter for RejectingAdapter {
    fn prepare(&self, _invocation: &fleetd::Invocation) -> Result<PreparedTurn, String> {
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
        RejectingAdapter,
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
