use fleetd::{
    error::FleetError,
    model::{
        ArmInvocation, BlockResolution, ClaimDeliveries, CompleteInvocation, CreateAgent,
        CreateChannel, CreateMessage, ExecutionCertainty, Invocation, InvocationState,
        ResolveDeliveryBlock, RetryDelivery,
    },
    store::Store,
};
use serde_json::json;

async fn fixture() -> (
    tempfile::TempDir,
    Store,
    fleetd::model::Agent,
    fleetd::model::Agent,
    fleetd::model::Message,
) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let sender = agent(&store, "invocation-sender").await;
    let receiver = agent(&store, "invocation-receiver").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "managed-work".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), receiver.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");
    let message = store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: sender.id.clone(),
                idempotency_key: None,
                recipient_id: Some(receiver.id.clone()),
                kind: "work.requested/v1".to_owned(),
                payload: json!({ "task": "exercise crash fence" }),
                correlation_id: Some("managed-work".to_owned()),
                causation_id: None,
            },
        )
        .await
        .expect("append work");
    (directory, store, sender, receiver, message)
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

fn reservation(limit: u32, lease_duration_ms: u64) -> ClaimDeliveries {
    ClaimDeliveries {
        limit,
        lease_duration_ms,
    }
}

async fn reserve_one(store: &Store, agent_id: &str, lease_duration_ms: u64) -> Invocation {
    let batch = store
        .reserve_invocations(agent_id, reservation(1, lease_duration_ms))
        .await
        .expect("reserve invocation");
    assert_eq!(batch.invocations.len(), 1);
    batch
        .invocations
        .into_iter()
        .next()
        .expect("one invocation")
}

fn arm_input(invocation: &Invocation) -> ArmInvocation {
    ArmInvocation {
        lease_token: invocation.lease_token.clone(),
        fence_token: invocation.fence_token.clone(),
    }
}

#[tokio::test]
async fn concurrent_reservers_create_one_lease_and_one_invocation() {
    let (_directory, store, _sender, receiver, message) = fixture().await;
    let first_store = store.clone();
    let second_store = store.clone();
    let first_agent = receiver.id.clone();
    let second_agent = receiver.id.clone();
    let (first, second) = tokio::join!(
        first_store.reserve_invocations(&first_agent, reservation(1, 10_000)),
        second_store.reserve_invocations(&second_agent, reservation(1, 10_000)),
    );
    let invocations: Vec<_> = first
        .expect("first reserve")
        .invocations
        .into_iter()
        .chain(second.expect("second reserve").invocations)
        .collect();
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].message, message);
    assert_eq!(invocations[0].delivery_attempt, 1);

    let raw_claim = store
        .claim_deliveries(&receiver.id, reservation(1, 10_000))
        .await
        .expect("raw claim while reservation is live");
    assert!(raw_claim.deliveries.is_empty());
    assert_eq!(
        store
            .list_invocations(Some(&receiver.id))
            .await
            .expect("list invocations"),
        invocations
    );
}

#[tokio::test]
async fn a_crash_before_dispatch_is_proven_not_started_and_safely_reclaimed() {
    let (directory, store, _sender, receiver, message) = fixture().await;
    let first = reserve_one(&store, &receiver.id, 25).await;
    drop(store);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let reopened = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("reopen store");
    let second = reserve_one(&reopened, &receiver.id, 10_000).await;
    assert_eq!(second.message, message);
    assert_eq!(second.delivery_attempt, 2);
    assert_ne!(second.id, first.id);

    let invocations = reopened
        .list_invocations(Some(&receiver.id))
        .await
        .expect("list after recovery");
    let recovered = find_invocation(&invocations, &first.id);
    assert_eq!(recovered.state, InvocationState::Terminal);
    assert_eq!(
        recovered.execution_certainty,
        Some(ExecutionCertainty::NotStarted)
    );
    assert_eq!(
        recovered.terminal_reason.as_deref(),
        Some("reservation_expired_before_dispatch")
    );
    assert!(
        reopened
            .list_blocked_deliveries(None)
            .await
            .expect("no block before dispatch")
            .is_empty()
    );
}

#[tokio::test]
async fn a_crash_after_dispatch_is_parked_instead_of_reexecuted() {
    let (directory, store, _sender, receiver, message) = fixture().await;
    let reserved = reserve_one(&store, &receiver.id, 50).await;
    let armed = store
        .arm_invocation(&receiver.id, &reserved.id, arm_input(&reserved))
        .await
        .expect("arm invocation");
    assert_eq!(armed.state, InvocationState::DispatchArmed);
    let replay = store
        .arm_invocation(&receiver.id, &reserved.id, arm_input(&reserved))
        .await
        .expect("arm replay");
    assert_eq!(replay, armed);
    assert_armed_invocation_cannot_retry(&store, &receiver.id, &message.id, &reserved).await;
    drop(store);
    tokio::time::sleep(std::time::Duration::from_millis(75)).await;

    let reopened = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("reopen store");
    let raw_claim = reopened
        .claim_deliveries(&receiver.id, reservation(1, 10_000))
        .await
        .expect("claim runs managed recovery");
    assert!(raw_claim.deliveries.is_empty());
    let invocations = reopened
        .list_invocations(Some(&receiver.id))
        .await
        .expect("list recovered invocation");
    let recovered = find_invocation(&invocations, &reserved.id);
    assert_eq!(recovered.state, InvocationState::Terminal);
    assert_eq!(
        recovered.execution_certainty,
        Some(ExecutionCertainty::OutcomeUnknown)
    );
    assert_eq!(
        recovered.terminal_reason.as_deref(),
        Some("lease_expired_after_dispatch_armed")
    );

    let blocks = reopened
        .list_blocked_deliveries(Some(&receiver.id))
        .await
        .expect("list recovery block");
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].message, message);
    assert!(blocks[0].reason.contains(&reserved.id));
    reopened
        .resolve_delivery_block(
            blocks[0].block_id,
            ResolveDeliveryBlock {
                resolution: BlockResolution::Requeue,
                retry_after_ms: 0,
                note: Some("operator reconciled external state".to_owned()),
            },
        )
        .await
        .expect("operator requeues");
    let next = reserve_one(&reopened, &receiver.id, 10_000).await;
    assert_eq!(next.delivery_attempt, 2);
}

async fn assert_armed_invocation_cannot_retry(
    store: &Store,
    agent_id: &str,
    message_id: &str,
    invocation: &Invocation,
) {
    let bad_fence = store
        .arm_invocation(
            agent_id,
            &invocation.id,
            ArmInvocation {
                lease_token: invocation.lease_token.clone(),
                fence_token: "wrong-fence".to_owned(),
            },
        )
        .await
        .expect_err("wrong fence must fail");
    assert!(matches!(bad_fence, FleetError::LeaseConflict(_)));
    let unsafe_retry = store
        .retry_delivery(
            agent_id,
            message_id,
            RetryDelivery {
                lease_token: invocation.lease_token.clone(),
                retry_after_ms: 0,
                error: Some("transport disappeared".to_owned()),
            },
        )
        .await
        .expect_err("armed invocation cannot use ordinary retry");
    assert!(matches!(unsafe_retry, FleetError::Conflict(_)));
}

#[tokio::test]
async fn delivery_settlement_terminalizes_the_matching_invocation() {
    let (_directory, store, _sender, receiver, message) = fixture().await;
    let unarmed = reserve_one(&store, &receiver.id, 10_000).await;
    store
        .retry_delivery(
            &receiver.id,
            &message.id,
            RetryDelivery {
                lease_token: unarmed.lease_token.clone(),
                retry_after_ms: 0,
                error: Some("failed before dispatch".to_owned()),
            },
        )
        .await
        .expect("retry unarmed invocation");
    let armed = reserve_one(&store, &receiver.id, 10_000).await;
    store
        .arm_invocation(&receiver.id, &armed.id, arm_input(&armed))
        .await
        .expect("arm second invocation");
    store
        .acknowledge_delivery(&receiver.id, &message.id, &armed.lease_token)
        .await
        .expect("acknowledge known result");

    let invocations = store
        .list_invocations(Some(&receiver.id))
        .await
        .expect("list settled invocations");
    let first = find_invocation(&invocations, &unarmed.id);
    assert_eq!(
        first.execution_certainty,
        Some(ExecutionCertainty::NotStarted)
    );
    assert_eq!(first.terminal_reason.as_deref(), Some("retry"));
    let second = find_invocation(&invocations, &armed.id);
    assert_eq!(
        second.execution_certainty,
        Some(ExecutionCertainty::OutcomeKnown)
    );
    assert_eq!(second.terminal_reason.as_deref(), Some("acknowledged"));
}

#[tokio::test]
async fn completion_atomically_publishes_one_result_and_acknowledges_the_input() {
    let (directory, store, sender, receiver, input_message) = fixture().await;
    let reserved = reserve_one(&store, &receiver.id, 100).await;
    store
        .arm_invocation(&receiver.id, &reserved.id, arm_input(&reserved))
        .await
        .expect("arm invocation");
    let completion = CompleteInvocation {
        lease_token: reserved.lease_token.clone(),
        fence_token: reserved.fence_token.clone(),
        kind: "work.result/v1".to_owned(),
        payload: json!({ "status": "done" }),
    };
    let first_store = store.clone();
    let second_store = store.clone();
    let first_agent = receiver.id.clone();
    let second_agent = receiver.id.clone();
    let first_invocation = reserved.id.clone();
    let second_invocation = reserved.id.clone();
    let second_completion = completion.clone();
    let (first, second) = tokio::join!(
        first_store.complete_invocation(&first_agent, &first_invocation, completion.clone()),
        second_store.complete_invocation(&second_agent, &second_invocation, second_completion),
    );
    let first = first.expect("first completion");
    let second = second.expect("second completion");
    assert_eq!(u8::from(first.1) + u8::from(second.1), 1);
    assert_eq!(first.0, second.0);
    let completed = first.0;
    assert_eq!(completed.result.channel_id, input_message.channel_id);
    assert_eq!(completed.result.sender_id, receiver.id);
    assert_eq!(
        completed.result.recipient_id.as_deref(),
        Some(sender.id.as_str())
    );
    assert_eq!(
        completed.result.correlation_id,
        input_message.correlation_id
    );
    assert_eq!(
        completed.result.causation_id.as_deref(),
        Some(input_message.id.as_str())
    );
    assert_eq!(completed.invocation.state, InvocationState::Terminal);
    assert_eq!(
        completed.invocation.execution_certainty,
        Some(ExecutionCertainty::OutcomeKnown)
    );
    assert_eq!(
        completed.invocation.terminal_reason.as_deref(),
        Some("completed")
    );
    assert_eq!(
        completed.invocation.result_message_id.as_deref(),
        Some(completed.result.id.as_str())
    );

    let input_is_settled = store
        .claim_deliveries(&receiver.id, reservation(1, 10_000))
        .await
        .expect("claim completed input");
    assert!(input_is_settled.deliveries.is_empty());
    let result_delivery = store
        .claim_deliveries(&sender.id, reservation(1, 10_000))
        .await
        .expect("claim result delivery");
    assert_eq!(result_delivery.deliveries.len(), 1);
    assert_eq!(result_delivery.deliveries[0].message, completed.result);

    drop(store);
    tokio::time::sleep(std::time::Duration::from_millis(125)).await;
    let reopened = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("reopen completed store");
    let replay = reopened
        .complete_invocation(&receiver.id, &reserved.id, completion.clone())
        .await
        .expect("completion replay after lease expiry");
    assert!(!replay.1);
    assert_eq!(replay.0, completed);
    let mut changed = completion;
    changed.payload = json!({ "status": "different" });
    let conflict = reopened
        .complete_invocation(&receiver.id, &reserved.id, changed)
        .await
        .expect_err("changed completion must conflict");
    assert!(matches!(conflict, FleetError::Conflict(_)));
}

fn find_invocation<'a>(invocations: &'a [Invocation], id: &str) -> &'a Invocation {
    invocations
        .iter()
        .find(|invocation| invocation.id == id)
        .unwrap_or_else(|| panic!("missing invocation {id}"))
}
