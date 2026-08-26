use fleetd::execution::invocation;
use fleetd::execution::operations;
use fleetd::execution::session_binding;
use fleetd::execution::settlement;
use fleetd::{
    error::FleetError,
    execution::operations::NewPluginGeneration,
    execution::session_binding::{
        AcquireSessionBinding, SessionAcquisitionMode, SessionBinding, SessionBindingState,
    },
    model::{
        ArmInvocation, BlockDelivery, BlockResolution, BlockedDelivery, ClaimDeliveries,
        CompleteInvocation, CreateAgent, CreateChannel, CreateMessage, Invocation, InvocationState,
        ResolveDeliveryBlock,
    },
    plugin::{
        DescribeResult, DriverIdentity, HarnessLimits, PluginIdentity, RuntimeIdentity,
        SessionPersistence, harness_acp_interface,
    },
    store::Store,
};
use semver::Version;
use serde_json::json;

mod common;

struct Fixture {
    directory: tempfile::TempDir,
    store: Store,
    receiver: fleetd::model::Agent,
    invocation: Invocation,
    generation_id: String,
}

async fn fixture() -> Fixture {
    fixture_with_lease(30_000).await
}

async fn fixture_with_lease(lease_duration_ms: u64) -> Fixture {
    let common::TempStore {
        directory, store, ..
    } = common::temp_store().await;
    let sender = agent(&store, "binding-sender").await;
    let receiver = agent(&store, "binding-receiver").await;
    let generation_id = generation(&store, &receiver.id).await;
    let channel = store
        .create_channel(CreateChannel {
            name: "binding-work".to_owned(),
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
                sender_id: sender.id,
                idempotency_key: None,
                recipient_id: Some(receiver.id.clone()),
                kind: "work.request/v1".to_owned(),
                payload: json!({"task": "exercise durable session fencing"}),
                correlation_id: Some("binding-work".to_owned()),
                causation_id: None,
            },
        )
        .await
        .expect("append work");
    let invocation = invocation::reserve_invocations(
        &store,
        &receiver.id,
        ClaimDeliveries {
            limit: 1,
            lease_duration_ms,
        },
    )
    .await
    .expect("reserve invocation")
    .invocations
    .pop()
    .expect("one invocation");
    Fixture {
        directory,
        store,
        receiver,
        invocation,
        generation_id,
    }
}

/// Runs the daemon's reaper until it parks the orphaned attempt, and returns
/// the block it created.
///
/// The point of the fix is that this needs no worker for the agent: a worker
/// only recovers the agent it is running, so an interrupted seat with no worker
/// used to stay stuck.
async fn reap_expired_invocation(store: &Store, agent_id: &str) -> BlockedDelivery {
    let cancellation = tokio_util::sync::CancellationToken::new();
    let reaper = tokio::spawn(invocation::run_expired_invocation_reaper(
        store.clone(),
        cancellation.clone(),
        std::time::Duration::from_millis(5),
    ));
    let block = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if let Some(block) = store
                .list_blocked_deliveries(Some(agent_id))
                .await
                .expect("list recovered block")
                .pop()
            {
                return block;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("daemon recovery sweep did not park the orphaned invocation");
    cancellation.cancel();
    reaper.await.expect("stop daemon recovery sweep");
    block
}

async fn generation(store: &Store, agent_id: &str) -> String {
    let id = "test-plugin-generation".to_owned();
    operations::record_plugin_generation(
        store,
        NewPluginGeneration {
            id: id.clone(),
            agent_id: agent_id.to_owned(),
            plugin: PluginIdentity {
                id: "test.harness".to_owned(),
                name: "Test harness".to_owned(),
                version: Version::new(0, 1, 0),
            },
            interfaces: vec![harness_acp_interface()],
            process_id: Some(42),
            description: DescribeResult {
                driver: DriverIdentity {
                    version: "0.1.0".to_owned(),
                    acp_sdk_version: "2.0.0".to_owned(),
                    acp_protocol_version: 1,
                },
                runtime: RuntimeIdentity {
                    name: "test-runtime".to_owned(),
                    version: "1.0.0".to_owned(),
                    executable_digest: "sha256:test-runtime".to_owned(),
                },
                agent_capabilities: json!({}),
                limits: HarnessLimits {
                    max_concurrent_turns: 1,
                    max_frame_bytes: 1_048_576,
                },
                profile_digest: "sha256:test-profile".to_owned(),
                raw_initialize_result: json!({}),
            },
            compatibility_digest: "sha256:test-compatibility".to_owned(),
            heartbeat_interval_ms: 5_000,
        },
    )
    .await
    .expect("record plugin generation");
    id
}

async fn agent(store: &Store, name: &str) -> fleetd::model::Agent {
    store
        .create_agent(CreateAgent {
            name: name.to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create agent")
}

fn acquisition(owner: &str, profile: &str) -> AcquireSessionBinding {
    AcquireSessionBinding {
        lane_policy: "per-agent".to_owned(),
        lane_key: "primary".to_owned(),
        owner_instance_id: owner.to_owned(),
        profile_digest: profile.to_owned(),
        compatibility_digest: "sha256:acp-driver-v1".to_owned(),
        working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
        additional_directories: Vec::new(),
    }
}

async fn opened_binding(
    store: &Store,
    agent_id: &str,
    owner: &str,
    profile: &str,
) -> SessionBinding {
    let acquired =
        session_binding::acquire_session_binding(store, agent_id, acquisition(owner, profile))
            .await
            .expect("acquire binding");
    assert_eq!(acquired.mode, SessionAcquisitionMode::Create);
    session_binding::record_session_opened(
        store,
        agent_id,
        &acquired.session.binding,
        "native-session-1",
    )
    .await
    .expect("record opened session")
}

fn arm(invocation: &Invocation) -> ArmInvocation {
    ArmInvocation {
        lease_token: invocation.lease_token.clone(),
        fence_token: invocation.fence_token.clone(),
    }
}

fn completion(invocation: &Invocation) -> CompleteInvocation {
    CompleteInvocation {
        lease_token: invocation.lease_token.clone(),
        fence_token: invocation.fence_token.clone(),
        kind: "work.result/v1".to_owned(),
        payload: json!({"status": "done"}),
    }
}

async fn assert_bounded_event_folding(fixture: &Fixture) {
    let observed = json!({"sessionUpdate": "agent_message_chunk", "text": "bounded"});
    operations::record_invocation_event(
        &fixture.store,
        &fixture.generation_id,
        &fixture.invocation.id,
        1,
        123,
        "agent_message_content",
        &observed,
    )
    .await
    .expect("record first event");
    operations::record_invocation_event(
        &fixture.store,
        &fixture.generation_id,
        &fixture.invocation.id,
        1,
        123,
        "agent_message_content",
        &observed,
    )
    .await
    .expect("exact event replay is idempotent");
    let changed_event = operations::record_invocation_event(
        &fixture.store,
        &fixture.generation_id,
        &fixture.invocation.id,
        1,
        123,
        "agent_message_content",
        &json!({"sessionUpdate": "agent_message_chunk", "text": "changed"}),
    )
    .await
    .expect_err("changed event replay must conflict");
    assert!(matches!(changed_event, FleetError::Conflict(_)));
    let observations =
        operations::list_invocation_observations(&fixture.store, Some(&fixture.receiver.id))
            .await
            .expect("list invocation observations");
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].event_count, 1);
    assert_eq!(observations[0].counts.assistant, 1);
    assert!(observations[0].event_chain_digest.is_some());
}

#[tokio::test]
async fn opening_and_native_reference_survive_restart_with_idempotent_replay() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("fleetd.db");
    let store = Store::open(&path).await.expect("open store");
    let receiver = agent(&store, "restart-binding-agent").await;
    let input = acquisition("controller-1", "sha256:profile-a");
    let first = session_binding::acquire_session_binding(&store, &receiver.id, input.clone())
        .await
        .expect("first acquisition");
    assert_eq!(first.mode, SessionAcquisitionMode::Create);
    assert_eq!(first.session.state, SessionBindingState::Opening);
    drop(store);

    let reopened = Store::open(&path).await.expect("reopen store");
    let replay = session_binding::acquire_session_binding(&reopened, &receiver.id, input)
        .await
        .expect("acquisition replay");
    assert_eq!(replay, first);
    let ready = session_binding::record_session_opened(
        &reopened,
        &receiver.id,
        &first.session.binding,
        "native-session-1",
    )
    .await
    .expect("record native reference");
    assert_eq!(ready.state, SessionBindingState::Ready);
    assert_eq!(ready.session_ref.as_deref(), Some("native-session-1"));
    assert_eq!(
        session_binding::record_session_opened(
            &reopened,
            &receiver.id,
            &first.session.binding,
            "native-session-1"
        )
        .await
        .expect("native reference replay"),
        ready
    );
    let changed = session_binding::record_session_opened(
        &reopened,
        &receiver.id,
        &first.session.binding,
        "different-session",
    )
    .await
    .expect_err("changed native reference must conflict");
    assert!(matches!(changed, FleetError::Conflict(_)));
}

#[tokio::test]
async fn compatible_adoption_increments_epoch_and_fences_the_stale_owner() {
    let fixture = fixture().await;
    let first = opened_binding(
        &fixture.store,
        &fixture.receiver.id,
        "controller-1",
        "sha256:profile-a",
    )
    .await;
    let adopted = session_binding::acquire_session_binding(
        &fixture.store,
        &fixture.receiver.id,
        acquisition("controller-2", "sha256:profile-a"),
    )
    .await
    .expect("adopt compatible session");
    assert_eq!(adopted.session.binding.binding_id, first.binding.binding_id);
    assert_eq!(adopted.session.binding.binding_generation, 1);
    assert_eq!(adopted.session.binding.owner_epoch, 2);
    assert_eq!(
        adopted.mode,
        SessionAcquisitionMode::Resume {
            session_ref: "native-session-1".to_owned()
        }
    );

    let stale = session_binding::arm_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &first.binding,
        "native-session-1",
        &fixture.generation_id,
        arm(&fixture.invocation),
    )
    .await
    .expect_err("stale owner must not arm");
    assert!(matches!(stale, FleetError::LeaseConflict(_)));
    let armed = session_binding::arm_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &adopted.session.binding,
        "native-session-1",
        &fixture.generation_id,
        arm(&fixture.invocation),
    )
    .await
    .expect("current owner arms");
    assert_eq!(armed.session.state, SessionBindingState::Active);
    assert_eq!(
        armed.session.active_invocation_id.as_deref(),
        Some(fixture.invocation.id.as_str())
    );
    let direct_ack = settlement::acknowledge_delivery(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.message.id,
        &fixture.invocation.lease_token,
    )
    .await
    .expect_err("generic acknowledgement must not bypass a bound turn");
    assert!(matches!(direct_ack, FleetError::Conflict(_)));
    let direct_block = settlement::block_delivery(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.message.id,
        BlockDelivery {
            lease_token: fixture.invocation.lease_token.clone(),
            reason: "generic block attempted before binding fence".to_owned(),
        },
    )
    .await
    .expect_err("generic block must not bypass a bound turn");
    assert!(matches!(direct_block, FleetError::Conflict(_)));
    assert!(
        fixture
            .store
            .list_blocked_deliveries(Some(&fixture.receiver.id))
            .await
            .expect("list blocks")
            .is_empty()
    );
    let active_adoption = session_binding::acquire_session_binding(
        &fixture.store,
        &fixture.receiver.id,
        acquisition("controller-3", "sha256:profile-a"),
    )
    .await
    .expect_err("active session cannot be adopted");
    assert!(matches!(active_adoption, FleetError::Conflict(_)));
}

#[tokio::test]
async fn incompatible_or_abandoned_session_rotates_the_generation() {
    let fixture = fixture().await;
    let first = opened_binding(
        &fixture.store,
        &fixture.receiver.id,
        "controller-1",
        "sha256:profile-a",
    )
    .await;
    let second = session_binding::acquire_session_binding(
        &fixture.store,
        &fixture.receiver.id,
        acquisition("controller-2", "sha256:profile-b"),
    )
    .await
    .expect("rotate incompatible ready session");
    assert_eq!(second.mode, SessionAcquisitionMode::Create);
    assert_eq!(second.session.binding.binding_id, first.binding.binding_id);
    assert_eq!(second.session.binding.binding_generation, 2);
    assert_eq!(second.session.binding.owner_epoch, 1);
    assert_eq!(second.session.state, SessionBindingState::Opening);

    let third = session_binding::acquire_session_binding(
        &fixture.store,
        &fixture.receiver.id,
        acquisition("controller-3", "sha256:profile-b"),
    )
    .await
    .expect("rotate abandoned opening");
    assert_eq!(third.session.binding.binding_id, first.binding.binding_id);
    assert_eq!(third.session.binding.binding_generation, 3);
    assert_eq!(third.session.binding.owner_epoch, 1);
    let generations =
        session_binding::list_session_bindings(&fixture.store, Some(&fixture.receiver.id))
            .await
            .expect("list generations");
    assert_eq!(generations.len(), 3);
    assert_eq!(
        generations
            .iter()
            .filter(|binding| binding.state == SessionBindingState::Retired)
            .count(),
        2
    );
}

#[tokio::test]
async fn completion_atomically_quiesces_the_session_and_can_resume_after_restart() {
    let fixture = fixture().await;
    let path = fixture.directory.path().join("fleetd.db");
    let ready = opened_binding(
        &fixture.store,
        &fixture.receiver.id,
        "controller-1",
        "sha256:profile-a",
    )
    .await;
    session_binding::arm_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &ready.binding,
        "native-session-1",
        &fixture.generation_id,
        arm(&fixture.invocation),
    )
    .await
    .expect("arm bound invocation");
    assert_bounded_event_folding(&fixture).await;
    let direct_completion = invocation::complete_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        completion(&fixture.invocation),
    )
    .await
    .expect_err("generic completion must not bypass a bound turn");
    assert!(matches!(direct_completion, FleetError::Conflict(_)));
    let completed = session_binding::complete_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &ready.binding,
        SessionPersistence::Confirmed,
        completion(&fixture.invocation),
    )
    .await
    .expect("complete bound invocation");
    assert!(completed.1);
    let settled =
        session_binding::list_session_bindings(&fixture.store, Some(&fixture.receiver.id))
            .await
            .expect("list settled binding")
            .pop()
            .expect("one binding");
    assert_eq!(settled.state, SessionBindingState::Ready);
    assert_eq!(settled.active_invocation_id, None);
    assert_eq!(
        settled.last_quiescent_invocation_id.as_deref(),
        Some(fixture.invocation.id.as_str())
    );
    assert_eq!(
        settled.session_persistence,
        Some(SessionPersistence::Confirmed)
    );
    let replay = session_binding::complete_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &ready.binding,
        SessionPersistence::Confirmed,
        completion(&fixture.invocation),
    )
    .await
    .expect("completion replay");
    assert!(!replay.1);
    assert_eq!(replay.0, completed.0);

    drop(fixture.store);
    let reopened = Store::open(path).await.expect("reopen completed store");
    let adopted = session_binding::acquire_session_binding(
        &reopened,
        &fixture.receiver.id,
        acquisition("controller-2", "sha256:profile-a"),
    )
    .await
    .expect("resume after restart");
    assert_eq!(adopted.session.binding.owner_epoch, 2);
    assert_eq!(
        adopted.mode,
        SessionAcquisitionMode::Resume {
            session_ref: "native-session-1".to_owned()
        }
    );
}

#[tokio::test]
async fn generation_evidence_and_dispatch_arm_are_one_transaction() {
    let fixture = fixture().await;
    let ready = opened_binding(
        &fixture.store,
        &fixture.receiver.id,
        "controller-1",
        "sha256:profile-a",
    )
    .await;
    let error = session_binding::arm_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &ready.binding,
        "native-session-1",
        "missing-generation",
        arm(&fixture.invocation),
    )
    .await
    .expect_err("missing generation must roll back dispatch arm");
    assert!(matches!(error, FleetError::Conflict(_)));

    let invocation = invocation::list_invocations(&fixture.store, Some(&fixture.receiver.id))
        .await
        .expect("list invocation after rollback")
        .pop()
        .expect("one invocation");
    assert_eq!(invocation.state, InvocationState::Reserved);
    let binding =
        session_binding::list_session_bindings(&fixture.store, Some(&fixture.receiver.id))
            .await
            .expect("list binding after rollback")
            .pop()
            .expect("one binding");
    assert_eq!(binding.state, SessionBindingState::Ready);
    assert_eq!(binding.active_invocation_id, None);
    assert!(
        operations::list_invocation_observations(&fixture.store, Some(&fixture.receiver.id))
            .await
            .expect("list observations after rollback")
            .is_empty()
    );
}

#[tokio::test]
async fn daemon_recovery_parks_an_orphaned_turn_and_resolution_restores_the_seat() {
    let fixture = fixture_with_lease(25).await;
    let path = fixture.directory.path().join("fleetd.db");
    let agent_id = fixture.receiver.id.clone();
    let invocation_id = fixture.invocation.id.clone();
    let ready = opened_binding(
        &fixture.store,
        &agent_id,
        "controller-1",
        "sha256:profile-a",
    )
    .await;
    session_binding::arm_session_invocation(
        &fixture.store,
        &agent_id,
        &invocation_id,
        &ready.binding,
        "native-session-1",
        &fixture.generation_id,
        arm(&fixture.invocation),
    )
    .await
    .expect("arm bound invocation");
    drop(fixture.store);
    tokio::time::sleep(std::time::Duration::from_millis(75)).await;

    let reopened = Store::open(path).await.expect("reopen expired store");
    // No worker is running for this agent. The daemon's sweep is what recovers it.
    let block = reap_expired_invocation(&reopened, &agent_id).await;
    let session = session_binding::list_session_bindings(&reopened, Some(&agent_id))
        .await
        .expect("list recovered session")
        .pop()
        .expect("one session");
    assert_eq!(session.state, SessionBindingState::Uncertain);
    assert_eq!(
        session.active_invocation_id.as_deref(),
        Some(invocation_id.as_str())
    );
    assert!(
        session
            .uncertain_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("lease expired after dispatch was armed"))
    );
    assert_eq!(
        reopened
            .list_blocked_deliveries(Some(&agent_id))
            .await
            .expect("list recovered block")
            .len(),
        1
    );
    let adoption = session_binding::acquire_session_binding(
        &reopened,
        &agent_id,
        acquisition("controller-2", "sha256:profile-a"),
    )
    .await
    .expect_err("recovered uncertain session cannot be adopted");
    assert!(matches!(adoption, FleetError::Conflict(_)));

    // Resolving the block is what frees the seat: the uncertain generation is
    // retired in the same transaction, so the next acquisition is a clean one.
    let resolution = ResolveDeliveryBlock {
        resolution: BlockResolution::Requeue,
        retry_after_ms: 0,
        note: Some("operator verified the interrupted attempt is safe to repeat".to_owned()),
    };
    settlement::resolve_delivery_block(&reopened, block.block_id, resolution.clone())
        .await
        .expect("resolve block and retire uncertain session");
    settlement::resolve_delivery_block(&reopened, block.block_id, resolution)
        .await
        .expect("recovery replay is idempotent");
    let retired = session_binding::list_session_bindings(&reopened, Some(&agent_id))
        .await
        .expect("list retired session")
        .pop()
        .expect("one retired session");
    assert_eq!(retired.state, SessionBindingState::Retired);
    assert_eq!(retired.active_invocation_id, None);
    let expected_retirement = format!(
        "operator_resolved_delivery_block_{}_requeued",
        block.block_id
    );
    assert_eq!(
        retired.retired_reason.as_deref(),
        Some(expected_retirement.as_str())
    );
    let replacement = session_binding::acquire_session_binding(
        &reopened,
        &agent_id,
        acquisition("controller-2", "sha256:profile-a"),
    )
    .await
    .expect("acquire a fresh seat after resolution");
    assert_eq!(replacement.session.binding.binding_generation, 2);
    assert_eq!(replacement.mode, SessionAcquisitionMode::Create);
}

#[tokio::test]
async fn uncertain_turn_blocks_adoption_until_explicit_retirement() {
    let fixture = fixture().await;
    let ready = opened_binding(
        &fixture.store,
        &fixture.receiver.id,
        "controller-1",
        "sha256:profile-a",
    )
    .await;
    session_binding::arm_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &ready.binding,
        "native-session-1",
        &fixture.generation_id,
        arm(&fixture.invocation),
    )
    .await
    .expect("arm bound invocation");
    let uncertain = session_binding::mark_session_invocation_uncertain(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &ready.binding,
        "transport disappeared after dispatch",
    )
    .await
    .expect("mark uncertainty");
    assert_eq!(uncertain.state, SessionBindingState::Uncertain);
    assert_eq!(
        session_binding::mark_session_invocation_uncertain(
            &fixture.store,
            &fixture.receiver.id,
            &fixture.invocation.id,
            &ready.binding,
            "transport disappeared after dispatch",
        )
        .await
        .expect("uncertainty replay"),
        uncertain
    );
    let changed = session_binding::mark_session_invocation_uncertain(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        &ready.binding,
        "different evidence",
    )
    .await
    .expect_err("changed uncertainty must conflict");
    assert!(matches!(changed, FleetError::Conflict(_)));
    let adoption = session_binding::acquire_session_binding(
        &fixture.store,
        &fixture.receiver.id,
        acquisition("controller-2", "sha256:profile-a"),
    )
    .await
    .expect_err("uncertain session cannot be adopted");
    assert!(matches!(adoption, FleetError::Conflict(_)));

    session_binding::retire_session_binding(
        &fixture.store,
        &fixture.receiver.id,
        &ready.binding,
        "operator reconciled uncertain native state",
    )
    .await
    .expect("retire uncertain generation");
    let replacement = session_binding::acquire_session_binding(
        &fixture.store,
        &fixture.receiver.id,
        acquisition("controller-2", "sha256:profile-a"),
    )
    .await
    .expect("acquire replacement generation");
    assert_eq!(replacement.session.binding.binding_generation, 2);
    assert_eq!(replacement.session.binding.owner_epoch, 1);
    assert_eq!(replacement.mode, SessionAcquisitionMode::Create);
}

#[tokio::test]
async fn racing_adoptions_leave_only_the_latest_epoch_able_to_arm() {
    let fixture = fixture().await;
    opened_binding(
        &fixture.store,
        &fixture.receiver.id,
        "controller-1",
        "sha256:profile-a",
    )
    .await;
    let first_store = fixture.store.clone();
    let second_store = fixture.store.clone();
    let first_agent = fixture.receiver.id.clone();
    let second_agent = fixture.receiver.id.clone();
    let (first, second) = tokio::join!(
        session_binding::acquire_session_binding(
            &first_store,
            &first_agent,
            acquisition("racing-controller-a", "sha256:profile-a")
        ),
        session_binding::acquire_session_binding(
            &second_store,
            &second_agent,
            acquisition("racing-controller-b", "sha256:profile-a")
        ),
    );
    let first = first.expect("first racing acquisition");
    let second = second.expect("second racing acquisition");
    assert_ne!(
        first.session.binding.owner_epoch,
        second.session.binding.owner_epoch
    );
    let current =
        session_binding::list_session_bindings(&fixture.store, Some(&fixture.receiver.id))
            .await
            .expect("read current owner")
            .pop()
            .expect("one binding");
    let (latest, stale) = if current.binding.owner_epoch == first.session.binding.owner_epoch {
        (&first.session.binding, &second.session.binding)
    } else {
        (&second.session.binding, &first.session.binding)
    };
    let stale_result = session_binding::arm_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        stale,
        "native-session-1",
        &fixture.generation_id,
        arm(&fixture.invocation),
    )
    .await;
    assert!(matches!(stale_result, Err(FleetError::LeaseConflict(_))));
    session_binding::arm_session_invocation(
        &fixture.store,
        &fixture.receiver.id,
        &fixture.invocation.id,
        latest,
        "native-session-1",
        &fixture.generation_id,
        arm(&fixture.invocation),
    )
    .await
    .expect("latest owner arms");
}
