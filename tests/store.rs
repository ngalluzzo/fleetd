use fleetd::execution::settlement;
use fleetd::{
    error::FleetError,
    model::{
        ClaimDeliveries, ConversationAttention, CreateAgent, CreateChannel, CreateChannelMember,
        CreateMessage, MembershipDeliveryMode,
    },
    store::Store,
};
use serde_json::json;

mod common;

use fleetd_conversation as conversation;

#[tokio::test]
async fn participant_attention_is_exact_monotonic_and_durable() {
    let temporary = common::temp_store().await;
    let database_path = temporary.database_path.clone();
    let store = temporary.store;
    let sender = agent(&store, "attention-sender").await;
    let reader = agent(&store, "attention-reader").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "attention-channel".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), reader.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create attention channel");
    let addressed = append_text(
        &store,
        &channel.id,
        &sender.id,
        Some(&reader.id),
        "please look",
    )
    .await;
    let other_channel = store
        .create_channel(CreateChannel {
            name: "attention-gap-channel".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create interleaved channel");
    let interleaved = append_text(
        &store,
        &other_channel.id,
        &sender.id,
        None,
        "unrelated global sequence",
    )
    .await;
    let broadcast = append_text(&store, &channel.id, &sender.id, None, "shared update").await;

    assert_initial_attention(
        &store,
        &channel.id,
        &sender.id,
        &reader.id,
        addressed.seq,
        broadcast.seq,
    )
    .await;

    assert_eq!(
        store
            .advance_member_read_cursor(&channel.id, &reader.id, addressed.seq)
            .await
            .expect("advance cursor"),
        addressed.seq
    );
    assert_eq!(
        store
            .advance_member_read_cursor(&channel.id, &reader.id, 0)
            .await
            .expect("stale cursor replay"),
        addressed.seq,
        "a stale client cannot rewind another client's progress"
    );
    let future = store
        .advance_member_read_cursor(&channel.id, &reader.id, broadcast.seq + 1)
        .await
        .expect_err("future cursor must fail");
    assert!(matches!(future, FleetError::Invalid(_)));
    let other_channel_cursor = store
        .advance_member_read_cursor(&channel.id, &reader.id, interleaved.seq)
        .await
        .expect_err("another channel's global cursor must fail");
    assert!(matches!(other_channel_cursor, FleetError::Invalid(_)));

    drop(store);
    let reopened = Store::open(&database_path).await.expect("reopen store");
    assert_eq!(
        conversation::attention_for(&reopened, &reader.id, &channel.id)
            .await
            .expect("durable attention"),
        ConversationAttention {
            channel_id: channel.id,
            read_through_seq: addressed.seq,
            latest_message_seq: Some(broadcast.seq),
            unread_count: 1,
            addressed_unread_count: 0,
            first_unread_seq: Some(broadcast.seq),
            first_addressed_unread_seq: None,
        }
    );
}

async fn assert_initial_attention(
    store: &Store,
    channel_id: &str,
    sender_id: &str,
    reader_id: &str,
    addressed_seq: i64,
    latest_seq: i64,
) {
    assert_eq!(
        conversation::attention_for(store, reader_id, channel_id)
            .await
            .expect("initial attention"),
        ConversationAttention {
            channel_id: channel_id.to_owned(),
            read_through_seq: 0,
            latest_message_seq: Some(latest_seq),
            unread_count: 2,
            addressed_unread_count: 1,
            first_unread_seq: Some(addressed_seq),
            first_addressed_unread_seq: Some(addressed_seq),
        }
    );
    assert_eq!(
        conversation::attention_for(store, sender_id, channel_id)
            .await
            .expect("sender attention")
            .unread_count,
        0,
        "a participant's own messages are not unread work"
    );
    let late_member = agent(store, "attention-late-member").await;
    store
        .add_member_with_mode(
            channel_id,
            &late_member.id,
            MembershipDeliveryMode::StreamOnly,
        )
        .await
        .expect("add late member");
    let late_attention = conversation::attention_for(store, &late_member.id, channel_id)
        .await
        .expect("late member attention");
    assert_eq!(late_attention.read_through_seq, latest_seq);
    assert_eq!(late_attention.latest_message_seq, Some(latest_seq));
    assert_eq!(late_attention.unread_count, 0);
}

async fn test_store() -> (tempfile::TempDir, Store) {
    let common::TempStore {
        directory, store, ..
    } = common::temp_store().await;
    (directory, store)
}

#[tokio::test]
async fn membership_delivery_modes_control_only_the_delivery_snapshot() {
    let (_directory, store) = test_store().await;
    let sender = agent(&store, "mode-sender").await;
    let worker = agent(&store, "mode-worker").await;
    let human = agent(&store, "mode-human").await;
    let channel = store
        .create_channel_with_members(
            "mixed-membership".to_owned(),
            json!({}),
            vec![
                exact_member(&sender.id, MembershipDeliveryMode::Inbox),
                exact_member(&worker.id, MembershipDeliveryMode::Inbox),
                exact_member(&human.id, MembershipDeliveryMode::StreamOnly),
            ],
        )
        .await
        .expect("create mixed channel");

    let memberships = store
        .list_channel_members(&channel.id)
        .await
        .expect("list exact memberships");
    assert_eq!(memberships.len(), 3);
    assert_eq!(
        memberships
            .iter()
            .find(|membership| membership.agent_id == human.id)
            .expect("human membership")
            .delivery_mode,
        MembershipDeliveryMode::StreamOnly
    );

    let direct_to_human = store
        .append_message_idempotent(
            &channel.id,
            CreateMessage {
                sender_id: sender.id.clone(),
                idempotency_key: Some("direct/human".to_owned()),
                recipient_id: Some(human.id.clone()),
                kind: "unknown.result/v7".to_owned(),
                payload: json!({ "nested": { "preserved": true }, "extension": [1, 2, 3] }),
                correlation_id: None,
                causation_id: None,
            },
        )
        .await
        .expect("send direct to stream member");
    let replay = store
        .append_message_idempotent(
            &channel.id,
            CreateMessage {
                sender_id: sender.id.clone(),
                idempotency_key: Some("direct/human".to_owned()),
                recipient_id: Some(human.id.clone()),
                kind: "unknown.result/v7".to_owned(),
                payload: json!({ "nested": { "preserved": true }, "extension": [1, 2, 3] }),
                correlation_id: None,
                causation_id: None,
            },
        )
        .await
        .expect("replay direct to stream member");
    assert!(!replay.created);
    assert_eq!(replay.message, direct_to_human.message);
    assert!(claim_all(&store, &human.id).await.deliveries.is_empty());
    let human_history = store
        .list_messages(&channel.id, 0, 100)
        .await
        .expect("stream member history");
    assert_eq!(human_history.messages, vec![direct_to_human.message]);

    let direct_to_worker = append_text(
        &store,
        &channel.id,
        &sender.id,
        Some(&worker.id),
        "worker direct",
    )
    .await;
    let worker_direct = claim_all(&store, &worker.id).await;
    assert_eq!(worker_direct.deliveries.len(), 1);
    assert_eq!(worker_direct.deliveries[0].message, direct_to_worker);
    settlement::acknowledge_delivery(
        &store,
        &worker.id,
        &direct_to_worker.id,
        &worker_direct.lease_token,
    )
    .await
    .expect("ack direct worker delivery");

    let broadcast = append_text(&store, &channel.id, &sender.id, None, "mixed broadcast").await;
    assert!(claim_all(&store, &human.id).await.deliveries.is_empty());
    let worker_broadcast = claim_all(&store, &worker.id).await;
    assert_eq!(worker_broadcast.deliveries.len(), 1);
    assert_eq!(worker_broadcast.deliveries[0].message, broadcast);

    store
        .add_member_with_mode(&channel.id, &human.id, MembershipDeliveryMode::StreamOnly)
        .await
        .expect("exact membership replay");
    let conflict = store
        .add_member_with_mode(&channel.id, &human.id, MembershipDeliveryMode::Inbox)
        .await
        .expect_err("delivery mode mutation must conflict");
    assert!(matches!(conflict, FleetError::Conflict(_)));
}

#[tokio::test]
async fn concurrent_member_add_and_append_use_one_committed_snapshot() {
    let (_directory, store) = test_store().await;
    let sender = agent(&store, "concurrent-sender").await;
    let late = agent(&store, "concurrent-late").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "concurrent-membership-snapshot".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create initial channel");
    let input = CreateMessage {
        sender_id: sender.id.clone(),
        idempotency_key: Some("concurrent/broadcast".to_owned()),
        recipient_id: None,
        kind: "unknown.concurrent/v1".to_owned(),
        payload: json!({ "opaque": { "value": 42 } }),
        correlation_id: None,
        causation_id: None,
    };

    let (added, appended) = tokio::join!(
        store.add_member_with_mode(&channel.id, &late.id, MembershipDeliveryMode::Inbox),
        store.append_message_idempotent(&channel.id, input.clone())
    );
    added.expect("concurrent membership commits");
    let appended = appended.expect("concurrent message commits");
    let first_claim = claim_all(&store, &late.id).await;
    assert!(first_claim.deliveries.len() <= 1);
    if let Some(delivery) = first_claim.deliveries.first() {
        assert_eq!(delivery.message, appended.message);
    }

    let replay = store
        .append_message_idempotent(&channel.id, input)
        .await
        .expect("idempotent replay after membership commit");
    assert!(!replay.created);
    assert_eq!(replay.message, appended.message);
    let second_claim = claim_all(&store, &late.id).await;
    assert!(
        second_claim.deliveries.is_empty(),
        "replay must not recompute the delivery snapshot"
    );
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
            name: "project-001".to_owned(),
            metadata: json!({}),
            member_ids: vec![piler.id.clone(), weaver.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");

    let first = store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: piler.id.clone(),
                idempotency_key: None,
                recipient_id: Some(weaver.id.clone()),
                kind: "review.requested/v1".to_owned(),
                payload: json!({ "commit": "5fe343f" }),
                correlation_id: Some("project-001".to_owned()),
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
                idempotency_key: None,
                recipient_id: Some(piler.id),
                kind: "review.completed/v1".to_owned(),
                payload: json!({ "verdict": "approve" }),
                correlation_id: Some("project-001".to_owned()),
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
async fn addressed_messages_stay_visible_to_the_whole_channel() {
    let (_directory, store) = test_store().await;
    let piler = agent(&store, "piler").await;
    let weaver = agent(&store, "weaver").await;
    let bystander = agent(&store, "bystander").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "scoped".to_owned(),
            metadata: json!({}),
            member_ids: vec![piler.id.clone(), weaver.id.clone(), bystander.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");

    let broadcast = append_text(&store, &channel.id, &piler.id, None, "standup").await;
    let inbound = append_text(
        &store,
        &channel.id,
        &weaver.id,
        Some(&piler.id),
        "for piler",
    )
    .await;
    let outbound = append_text(
        &store,
        &channel.id,
        &piler.id,
        Some(&weaver.id),
        "for weaver",
    )
    .await;
    let later_broadcast = append_text(&store, &channel.id, &weaver.id, None, "standup again").await;

    let whole_log = vec![
        broadcast.clone(),
        inbound.clone(),
        outbound.clone(),
        later_broadcast.clone(),
    ];

    // The reader's identity does not narrow the log. A bystander who never sent
    // or received either addressed message reads exactly what the operator and
    // the addressed participant read.
    let page = store
        .list_messages(&channel.id, 0, 100)
        .await
        .expect("read channel history");
    assert_eq!(page.messages, whole_log);
    assert_eq!(page.next_cursor, later_broadcast.seq);

    // Addressing survives on the envelope; it just stopped gating reads.
    assert_eq!(inbound.recipient_id.as_deref(), Some(piler.id.as_str()));
    assert_eq!(outbound.recipient_id.as_deref(), Some(weaver.id.as_str()));
    assert_eq!(broadcast.recipient_id, None);

    // Paging walks the addressed messages too, rather than skipping past them
    // to the next broadcast.
    let mut walked = Vec::new();
    let mut cursor = 0;
    loop {
        let step = store
            .list_messages(&channel.id, cursor, 1)
            .await
            .expect("walk one message at a time");
        if step.messages.is_empty() {
            break;
        }
        assert_eq!(step.messages.len(), 1);
        cursor = step.next_cursor;
        walked.extend(step.messages);
    }
    assert_eq!(walked, whole_log);

    // Execution context can ask for the newest bounded prefix without
    // replaying the channel from sequence zero. The boundary is exclusive and
    // the returned slice is restored to durable ascending order.
    let recent = store
        .list_recent_messages_before(&channel.id, later_broadcast.seq, 2)
        .await
        .expect("read recent shared context");
    assert_eq!(recent, vec![inbound, outbound.clone()]);
    let through_end = store
        .list_recent_messages_before(&channel.id, later_broadcast.seq + 1, 2)
        .await
        .expect("read history through the newest message");
    assert_eq!(through_end, vec![outbound, later_broadcast]);
    assert!(
        store
            .list_recent_messages_before(&channel.id, 0, 2)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn message_idempotency_is_concurrent_durable_and_conflict_detecting() {
    let (directory, store) = test_store().await;
    let piler = agent(&store, "piler-idempotency").await;
    let weaver = agent(&store, "weaver-idempotency").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "idempotent-results".to_owned(),
            metadata: json!({}),
            member_ids: vec![piler.id.clone(), weaver.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");
    let input = CreateMessage {
        sender_id: piler.id.clone(),
        idempotency_key: Some("invocation/123/result".to_owned()),
        recipient_id: Some(weaver.id.clone()),
        kind: "agent.output/v1".to_owned(),
        payload: json!({ "text": "one durable result" }),
        correlation_id: Some("work-123".to_owned()),
        causation_id: Some("request-123".to_owned()),
    };

    let (first, second) = tokio::join!(
        store.append_message_idempotent(&channel.id, input.clone()),
        store.append_message_idempotent(&channel.id, input.clone()),
    );
    let first = first.expect("first append");
    let second = second.expect("second append");
    assert_eq!(first.message, second.message);
    assert_ne!(first.created, second.created);

    let delivery = settlement::claim_deliveries(
        &store,
        &weaver.id,
        fleetd::model::ClaimDeliveries {
            limit: 10,
            lease_duration_ms: 10_000,
        },
    )
    .await
    .expect("claim recipient delivery");
    assert_eq!(delivery.deliveries.len(), 1);
    assert_eq!(delivery.deliveries[0].message, first.message);

    let mut conflicting = input.clone();
    conflicting.payload = json!({ "text": "different result" });
    let error = store
        .append_message_idempotent(&channel.id, conflicting)
        .await
        .expect_err("different content must conflict");
    assert!(matches!(error, FleetError::Conflict(_)));

    let same_key_other_agent = store
        .append_message_idempotent(
            &channel.id,
            CreateMessage {
                sender_id: weaver.id,
                idempotency_key: input.idempotency_key.clone(),
                recipient_id: Some(piler.id),
                kind: input.kind.clone(),
                payload: input.payload.clone(),
                correlation_id: input.correlation_id.clone(),
                causation_id: input.causation_id.clone(),
            },
        )
        .await
        .expect("same key in another agent scope");
    assert!(same_key_other_agent.created);
    assert_ne!(same_key_other_agent.message.id, first.message.id);

    drop(store);
    let reopened = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("reopen store");
    let replay = reopened
        .append_message_idempotent(&channel.id, input)
        .await
        .expect("replay after restart");
    assert!(!replay.created);
    assert_eq!(replay.message, first.message);
}

#[tokio::test]
async fn idempotency_keys_are_bounded() {
    let (_directory, store) = test_store().await;
    let sender = agent(&store, "bounded-key-sender").await;
    let receiver = agent(&store, "bounded-key-receiver").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "bounded-keys".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), receiver.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");

    for key in ["   ".to_owned(), "x".repeat(257)] {
        let error = store
            .append_message_idempotent(
                &channel.id,
                CreateMessage {
                    sender_id: sender.id.clone(),
                    idempotency_key: Some(key),
                    recipient_id: Some(receiver.id.clone()),
                    kind: "text".to_owned(),
                    payload: json!({ "text": "invalid key" }),
                    correlation_id: None,
                    causation_id: None,
                },
            )
            .await
            .expect_err("invalid key must fail");
        assert!(matches!(error, FleetError::Invalid(_)));
    }
}

#[tokio::test]
async fn listing_conversations_reports_the_same_membership_as_reading_one() {
    let (_directory, store) = test_store().await;
    let first = agent(&store, "listing-first").await;
    let second = agent(&store, "listing-second").await;
    let third = agent(&store, "listing-third").await;

    let populated = store
        .create_channel_with_members(
            "listing-populated".to_owned(),
            json!({}),
            vec![
                exact_member(&first.id, MembershipDeliveryMode::Inbox),
                exact_member(&second.id, MembershipDeliveryMode::StreamOnly),
                exact_member(&third.id, MembershipDeliveryMode::Inbox),
            ],
        )
        .await
        .expect("create populated channel");
    let empty = store
        .create_channel(CreateChannel {
            name: "listing-empty".to_owned(),
            metadata: json!({}),
            member_ids: Vec::new(),
            members: Vec::new(),
        })
        .await
        .expect("create memberless channel");
    let (direct, _) = store
        .open_direct_pair(fleetd::model::OpenDirectConversation {
            members: vec![
                exact_member(&first.id, MembershipDeliveryMode::Inbox),
                exact_member(&second.id, MembershipDeliveryMode::Inbox),
            ],
        })
        .await
        .expect("open direct pair");
    let archived = store
        .create_channel_with_members(
            "listing-archived".to_owned(),
            json!({}),
            vec![exact_member(&third.id, MembershipDeliveryMode::Inbox)],
        )
        .await
        .expect("create archived channel");
    store
        .archive_channel(&archived.id)
        .await
        .expect("archive channel");

    // One listing must agree with reading each conversation on its own, member
    // for member and in the same order. The listing groups one membership query
    // across every row, so nothing but this pins the two together.
    let listed = conversation::list(&store, true)
        .await
        .expect("list every conversation");
    assert_eq!(
        listed
            .iter()
            .map(|summary| summary.id.as_str())
            .collect::<Vec<_>>(),
        vec![
            populated.id.as_str(),
            empty.id.as_str(),
            direct.id.as_str(),
            archived.id.as_str()
        ]
    );
    for summary in &listed {
        let directly = store
            .list_channel_members(&summary.id)
            .await
            .expect("read one conversation's membership");
        assert_eq!(summary.members, directly, "membership of {}", summary.id);
    }

    let memberless = listed
        .iter()
        .find(|summary| summary.id == empty.id)
        .expect("memberless conversation is listed");
    assert!(memberless.members.is_empty());

    let active = conversation::list(&store, false)
        .await
        .expect("list active conversations");
    assert!(active.iter().all(|summary| summary.id != archived.id));
    for summary in &active {
        let directly = store
            .list_channel_members(&summary.id)
            .await
            .expect("read one active conversation's membership");
        assert_eq!(summary.members, directly, "membership of {}", summary.id);
    }
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

async fn append_text(
    store: &Store,
    channel_id: &str,
    sender_id: &str,
    recipient_id: Option<&str>,
    text: &str,
) -> fleetd::model::Message {
    store
        .append_message(
            channel_id,
            CreateMessage {
                sender_id: sender_id.to_owned(),
                idempotency_key: None,
                recipient_id: recipient_id.map(str::to_owned),
                kind: "text".to_owned(),
                payload: json!({ "text": text }),
                correlation_id: None,
                causation_id: None,
            },
        )
        .await
        .expect("append message")
}

fn exact_member(agent_id: &str, delivery_mode: MembershipDeliveryMode) -> CreateChannelMember {
    CreateChannelMember {
        agent_id: agent_id.to_owned(),
        delivery_mode,
    }
}

async fn claim_all(store: &Store, agent_id: &str) -> fleetd::model::ClaimBatch {
    settlement::claim_deliveries(
        store,
        agent_id,
        ClaimDeliveries {
            limit: 100,
            lease_duration_ms: 10_000,
        },
    )
    .await
    .expect("claim deliveries")
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
            members: Vec::new(),
        })
        .await
        .expect("create channel");
    let message = CreateMessage {
        sender_id: outsider.id.clone(),
        idempotency_key: None,
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
