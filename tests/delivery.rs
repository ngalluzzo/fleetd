use fleetd::execution::settlement;
use fleetd::{
    error::FleetError,
    model::{
        BlockDelivery, BlockResolution, BlockedDelivery, ClaimDeliveries, CreateAgent,
        CreateChannel, CreateMessage, ResolveDeliveryBlock, RetryDelivery,
    },
    store::Store,
};
use serde_json::json;

async fn agent(store: &Store, name: &str) -> fleetd::model::Agent {
    store
        .create_agent(CreateAgent {
            name: name.to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create agent")
}

async fn fixture() -> (
    tempfile::TempDir,
    Store,
    fleetd::model::Agent,
    fleetd::model::Agent,
    fleetd::model::Channel,
) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let sender = agent(&store, "sender").await;
    let receiver = agent(&store, "receiver").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "work".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), receiver.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");
    (directory, store, sender, receiver, channel)
}

fn claim(limit: u32, lease_duration_ms: u64) -> ClaimDeliveries {
    ClaimDeliveries {
        limit,
        lease_duration_ms,
    }
}

async fn send(
    store: &Store,
    channel_id: &str,
    sender_id: &str,
    recipient_id: Option<String>,
    text: &str,
) -> fleetd::model::Message {
    store
        .append_message(
            channel_id,
            CreateMessage {
                sender_id: sender_id.to_owned(),
                idempotency_key: None,
                recipient_id,
                kind: "text".to_owned(),
                payload: json!({ "text": text }),
                correlation_id: None,
                causation_id: None,
            },
        )
        .await
        .expect("append message")
}

#[tokio::test]
async fn broadcast_recipients_are_snapshotted_when_the_message_is_appended() {
    let (_directory, store, sender, receiver, channel) = fixture().await;
    let broadcast = send(&store, &channel.id, &sender.id, None, "before join").await;
    let late_member = agent(&store, "late-member").await;
    store
        .add_member(&channel.id, &late_member.id)
        .await
        .expect("add late member");

    let receiver_batch = settlement::claim_deliveries(&store, &receiver.id, claim(10, 10_000))
        .await
        .expect("receiver claim");
    assert_eq!(receiver_batch.deliveries.len(), 1);
    assert_eq!(receiver_batch.deliveries[0].message, broadcast);

    let late_batch = settlement::claim_deliveries(&store, &late_member.id, claim(10, 10_000))
        .await
        .expect("late member claim");
    assert!(late_batch.deliveries.is_empty());
}

#[tokio::test]
async fn concurrent_workers_cannot_claim_the_same_delivery() {
    let (_directory, store, sender, receiver, channel) = fixture().await;
    let message = send(
        &store,
        &channel.id,
        &sender.id,
        Some(receiver.id.clone()),
        "one owner",
    )
    .await;
    let first_store = store.clone();
    let second_store = store.clone();
    let first_agent = receiver.id.clone();
    let second_agent = receiver.id.clone();
    let (first, second) = tokio::join!(
        settlement::claim_deliveries(&first_store, &first_agent, claim(1, 10_000)),
        settlement::claim_deliveries(&second_store, &second_agent, claim(1, 10_000)),
    );
    let first = first.expect("first claim");
    let second = second.expect("second claim");
    let deliveries: Vec<_> = first
        .deliveries
        .into_iter()
        .chain(second.deliveries)
        .collect();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].message, message);
}

#[tokio::test]
async fn an_expired_lease_is_reclaimed_and_old_owners_cannot_acknowledge_it() {
    let (_directory, store, sender, receiver, channel) = fixture().await;
    let message = send(
        &store,
        &channel.id,
        &sender.id,
        Some(receiver.id.clone()),
        "survive a crash",
    )
    .await;
    let first = settlement::claim_deliveries(&store, &receiver.id, claim(1, 25))
        .await
        .expect("first claim");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let second = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("reclaim");
    assert_eq!(second.deliveries.len(), 1);
    assert_eq!(second.deliveries[0].attempt, 2);

    let stale =
        settlement::acknowledge_delivery(&store, &receiver.id, &message.id, &first.lease_token)
            .await
            .expect_err("stale owner must be rejected");
    assert!(matches!(stale, FleetError::LeaseConflict(_)));
    settlement::acknowledge_delivery(&store, &receiver.id, &message.id, &second.lease_token)
        .await
        .expect("active owner acknowledges");
    settlement::acknowledge_delivery(&store, &receiver.id, &message.id, &second.lease_token)
        .await
        .expect("acknowledgement retry is idempotent");
    let empty = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("claim after acknowledgement");
    assert!(empty.deliveries.is_empty());
}

#[tokio::test]
async fn a_retry_is_delayed_and_preserves_failure_evidence() {
    let (_directory, store, sender, receiver, channel) = fixture().await;
    let message = send(
        &store,
        &channel.id,
        &sender.id,
        Some(receiver.id.clone()),
        "retry me",
    )
    .await;
    let first = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("first claim");
    let retry = RetryDelivery {
        lease_token: first.lease_token,
        retry_after_ms: 25,
        error: Some("model server unavailable".to_owned()),
    };
    settlement::retry_delivery(&store, &receiver.id, &message.id, retry.clone())
        .await
        .expect("release delivery");
    settlement::retry_delivery(&store, &receiver.id, &message.id, retry)
        .await
        .expect("retry release is idempotent");
    let early = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("early claim");
    assert!(early.deliveries.is_empty());

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let second = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("delayed retry");
    assert_eq!(second.deliveries.len(), 1);
    assert_eq!(second.deliveries[0].attempt, 2);
    assert_eq!(
        second.deliveries[0].last_error.as_deref(),
        Some("model server unavailable")
    );
}

#[tokio::test]
async fn a_blocked_delivery_stays_parked_until_an_operator_resolves_it() {
    let (_directory, store, sender, receiver, channel) = fixture().await;
    let message = send(
        &store,
        &channel.id,
        &sender.id,
        Some(receiver.id.clone()),
        "do not execute twice",
    )
    .await;
    let first = settlement::claim_deliveries(&store, &receiver.id, claim(1, 100))
        .await
        .expect("first claim");
    let created = assert_block_replay(&store, &receiver.id, &message, first.lease_token).await;

    tokio::time::sleep(std::time::Duration::from_millis(125)).await;
    let still_parked = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("claim after former lease expired");
    assert!(still_parked.deliveries.is_empty());
    assert_eq!(
        store
            .list_blocked_deliveries(Some(&receiver.id))
            .await
            .expect("list blocked deliveries"),
        vec![created.clone()]
    );

    let resolution = ResolveDeliveryBlock {
        resolution: BlockResolution::Requeue,
        retry_after_ms: 25,
        note: Some("operator verified no side effect".to_owned()),
    };
    store
        .resolve_delivery_block(created.block_id, resolution.clone())
        .await
        .expect("requeue block");
    store
        .resolve_delivery_block(created.block_id, resolution)
        .await
        .expect("resolution replay is idempotent");
    let conflicting_resolution = store
        .resolve_delivery_block(
            created.block_id,
            ResolveDeliveryBlock {
                resolution: BlockResolution::Abandon,
                retry_after_ms: 0,
                note: Some("changed decision".to_owned()),
            },
        )
        .await
        .expect_err("changed resolution must conflict");
    assert!(matches!(conflicting_resolution, FleetError::Conflict(_)));
    assert!(
        store
            .list_blocked_deliveries(None)
            .await
            .expect("resolved block is absent")
            .is_empty()
    );

    let too_early = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("claim before operator delay");
    assert!(too_early.deliveries.is_empty());
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let second = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("claim requeued delivery");
    assert_eq!(second.deliveries.len(), 1);
    assert_eq!(second.deliveries[0].attempt, 2);
    assert_eq!(
        second.deliveries[0].last_error.as_deref(),
        Some("operator verified no side effect")
    );

    assert_abandon(
        &store,
        &receiver.id,
        &message.id,
        second.lease_token,
        created.block_id,
    )
    .await;
}

async fn assert_abandon(
    store: &Store,
    agent_id: &str,
    message_id: &str,
    lease_token: String,
    previous_block_id: i64,
) {
    let (second_block, was_created) = settlement::block_delivery(
        store,
        agent_id,
        message_id,
        BlockDelivery {
            lease_token,
            reason: "second ambiguous attempt".to_owned(),
        },
    )
    .await
    .expect("block second attempt");
    assert!(was_created);
    assert_ne!(second_block.block_id, previous_block_id);
    store
        .resolve_delivery_block(
            second_block.block_id,
            ResolveDeliveryBlock {
                resolution: BlockResolution::Abandon,
                retry_after_ms: 0,
                note: Some("side effect confirmed".to_owned()),
            },
        )
        .await
        .expect("abandon block");
    let never_again = settlement::claim_deliveries(store, agent_id, claim(1, 10_000))
        .await
        .expect("claim abandoned delivery");
    assert!(never_again.deliveries.is_empty());
}

async fn assert_block_replay(
    store: &Store,
    agent_id: &str,
    message: &fleetd::model::Message,
    lease_token: String,
) -> BlockedDelivery {
    let block = BlockDelivery {
        lease_token,
        reason: "tool returned after its connection closed".to_owned(),
    };
    let (created, was_created) =
        settlement::block_delivery(store, agent_id, &message.id, block.clone())
            .await
            .expect("block delivery");
    assert!(was_created);
    assert_eq!(created.attempt, 1);
    assert_eq!(&created.message, message);

    let (replayed, was_created) =
        settlement::block_delivery(store, agent_id, &message.id, block.clone())
            .await
            .expect("replay block after a lost response");
    assert!(!was_created);
    assert_eq!(replayed, created);
    let conflict = settlement::block_delivery(
        store,
        agent_id,
        &message.id,
        BlockDelivery {
            lease_token: block.lease_token,
            reason: "different evidence".to_owned(),
        },
    )
    .await
    .expect_err("changed replay must conflict");
    assert!(matches!(conflict, FleetError::Conflict(_)));
    created
}

#[tokio::test]
async fn blocking_requires_the_current_unexpired_lease() {
    let (_directory, store, sender, receiver, channel) = fixture().await;
    let message = send(
        &store,
        &channel.id,
        &sender.id,
        Some(receiver.id.clone()),
        "lease fenced",
    )
    .await;
    let batch = settlement::claim_deliveries(&store, &receiver.id, claim(1, 25))
        .await
        .expect("claim delivery");
    let foreign = settlement::block_delivery(
        &store,
        &receiver.id,
        &message.id,
        BlockDelivery {
            lease_token: "foreign-lease".to_owned(),
            reason: "not the owner".to_owned(),
        },
    )
    .await
    .expect_err("foreign lease must fail");
    assert!(matches!(foreign, FleetError::LeaseConflict(_)));

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let expired = settlement::block_delivery(
        &store,
        &receiver.id,
        &message.id,
        BlockDelivery {
            lease_token: batch.lease_token,
            reason: "too late".to_owned(),
        },
    )
    .await
    .expect_err("expired lease must fail");
    assert!(matches!(expired, FleetError::LeaseConflict(_)));
}

#[tokio::test]
async fn concurrent_block_retries_create_one_evidence_record() {
    let (_directory, store, sender, receiver, channel) = fixture().await;
    let message = send(
        &store,
        &channel.id,
        &sender.id,
        Some(receiver.id.clone()),
        "concurrent block",
    )
    .await;
    let batch = settlement::claim_deliveries(&store, &receiver.id, claim(1, 10_000))
        .await
        .expect("claim delivery");
    let input = BlockDelivery {
        lease_token: batch.lease_token,
        reason: "worker lost the response".to_owned(),
    };
    let first_store = store.clone();
    let second_store = store.clone();
    let first_agent = receiver.id.clone();
    let second_agent = receiver.id.clone();
    let first_message = message.id.clone();
    let second_message = message.id.clone();
    let second_input = input.clone();
    let (first, second) = tokio::join!(
        settlement::block_delivery(&first_store, &first_agent, &first_message, input),
        settlement::block_delivery(&second_store, &second_agent, &second_message, second_input),
    );
    let first = first.expect("first block");
    let second = second.expect("second block");
    assert_eq!(u8::from(first.1) + u8::from(second.1), 1);
    assert_eq!(first.0, second.0);
    assert_eq!(
        store
            .list_blocked_deliveries(None)
            .await
            .expect("list blocks"),
        vec![first.0]
    );
}
