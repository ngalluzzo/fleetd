use fleetd::{CreateAgent, CreateChannel, CreateMessage, FleetError, Store};
use serde_json::json;

async fn test_store() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    (directory, store)
}

#[tokio::test]
async fn messages_are_durable_ordered_and_cursor_addressable() {
    let (directory, store) = test_store().await;
    let piler = store
        .create_agent(CreateAgent {
            name: "piler".to_owned(),
            metadata: json!({ "harness": "dsh" }),
        })
        .await
        .expect("create piler");
    let weaver = store
        .create_agent(CreateAgent {
            name: "weaver".to_owned(),
            metadata: json!({ "harness": "codex" }),
        })
        .await
        .expect("create weaver");
    let channel = store
        .create_channel(CreateChannel {
            name: "gooir-001".to_owned(),
            metadata: json!({}),
            member_ids: vec![piler.id.clone(), weaver.id.clone()],
        })
        .await
        .expect("create channel");

    let first = store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: piler.id.clone(),
                recipient_id: Some(weaver.id.clone()),
                kind: "review.requested/v1".to_owned(),
                payload: json!({ "commit": "5fe343f" }),
                correlation_id: Some("gooir-001".to_owned()),
                causation_id: None,
            },
        )
        .await
        .expect("append first message");
    let second = store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: weaver.id,
                recipient_id: Some(piler.id),
                kind: "review.completed/v1".to_owned(),
                payload: json!({ "verdict": "approve" }),
                correlation_id: Some("gooir-001".to_owned()),
                causation_id: Some(first.id.clone()),
            },
        )
        .await
        .expect("append second message");

    assert!(second.seq > first.seq);
    let page = store
        .list_messages(&channel.id, first.seq, 100)
        .await
        .expect("list after cursor");
    assert_eq!(page.messages, vec![second.clone()]);
    assert_eq!(page.next_cursor, second.seq);

    drop(store);
    let reopened = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("reopen store");
    let durable = reopened
        .list_messages(&channel.id, 0, 100)
        .await
        .expect("read durable messages");
    assert_eq!(durable.messages, vec![first, second]);
}

#[tokio::test]
async fn non_members_cannot_send_or_receive_in_a_channel() {
    let (_directory, store) = test_store().await;
    let member = store
        .create_agent(CreateAgent {
            name: "member".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create member");
    let outsider = store
        .create_agent(CreateAgent {
            name: "outsider".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create outsider");
    let channel = store
        .create_channel(CreateChannel {
            name: "bounded".to_owned(),
            metadata: json!({}),
            member_ids: vec![member.id.clone()],
        })
        .await
        .expect("create channel");
    let message = CreateMessage {
        sender_id: outsider.id.clone(),
        recipient_id: Some(member.id),
        kind: "text".to_owned(),
        payload: json!({ "text": "hello" }),
        correlation_id: None,
        causation_id: None,
    };

    let error = store
        .append_message(&channel.id, message)
        .await
        .expect_err("outsider must be rejected");
    assert!(matches!(
        error,
        FleetError::NotMember { agent_id, .. } if agent_id == outsider.id
    ));
}
