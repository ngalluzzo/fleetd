use fleetd::{
    ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage, FleetError, RetryDelivery, Store,
};
use serde_json::json;

async fn agent(store: &Store, name: &str) -> fleetd::Agent {
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
    fleetd::Agent,
    fleetd::Agent,
    fleetd::Channel,
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
) -> fleetd::Message {
    store
        .append_message(
            channel_id,
            CreateMessage {
                sender_id: sender_id.to_owned(),
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

    let receiver_batch = store
        .claim_deliveries(&receiver.id, claim(10, 10_000))
        .await
        .expect("receiver claim");
    assert_eq!(receiver_batch.deliveries.len(), 1);
    assert_eq!(receiver_batch.deliveries[0].message, broadcast);

    let late_batch = store
        .claim_deliveries(&late_member.id, claim(10, 10_000))
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
        first_store.claim_deliveries(&first_agent, claim(1, 10_000)),
        second_store.claim_deliveries(&second_agent, claim(1, 10_000)),
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
    let first = store
        .claim_deliveries(&receiver.id, claim(1, 25))
        .await
        .expect("first claim");
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let second = store
        .claim_deliveries(&receiver.id, claim(1, 10_000))
        .await
        .expect("reclaim");
    assert_eq!(second.deliveries.len(), 1);
    assert_eq!(second.deliveries[0].attempt, 2);

    let stale = store
        .acknowledge_delivery(&receiver.id, &message.id, &first.lease_token)
        .await
        .expect_err("stale owner must be rejected");
    assert!(matches!(stale, FleetError::LeaseConflict(_)));
    store
        .acknowledge_delivery(&receiver.id, &message.id, &second.lease_token)
        .await
        .expect("active owner acknowledges");
    store
        .acknowledge_delivery(&receiver.id, &message.id, &second.lease_token)
        .await
        .expect("acknowledgement retry is idempotent");
    let empty = store
        .claim_deliveries(&receiver.id, claim(1, 10_000))
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
    let first = store
        .claim_deliveries(&receiver.id, claim(1, 10_000))
        .await
        .expect("first claim");
    let retry = RetryDelivery {
        lease_token: first.lease_token,
        retry_after_ms: 25,
        error: Some("model server unavailable".to_owned()),
    };
    store
        .retry_delivery(&receiver.id, &message.id, retry.clone())
        .await
        .expect("release delivery");
    store
        .retry_delivery(&receiver.id, &message.id, retry)
        .await
        .expect("retry release is idempotent");
    let early = store
        .claim_deliveries(&receiver.id, claim(1, 10_000))
        .await
        .expect("early claim");
    assert!(early.deliveries.is_empty());

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let second = store
        .claim_deliveries(&receiver.id, claim(1, 10_000))
        .await
        .expect("delayed retry");
    assert_eq!(second.deliveries.len(), 1);
    assert_eq!(second.deliveries[0].attempt, 2);
    assert_eq!(
        second.deliveries[0].last_error.as_deref(),
        Some("model server unavailable")
    );
}
