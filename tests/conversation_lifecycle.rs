use fleetd::{
    model::{
        ConversationKind, CreateAgent, CreateChannel, CreateChannelMember, CreateMessage,
        MembershipDeliveryMode, OpenDirectConversation, RenameChannel, SendMessage,
    },
    store::Store,
};
use serde_json::json;

mod common;

use fleetd_conversation as conversation;

use common::api::Daemon;

async fn test_store() -> (tempfile::TempDir, Store) {
    let common::TempStore {
        directory, store, ..
    } = common::temp_store().await;
    (directory, store)
}

async fn create_agent(store: &Store, name: &str) -> fleetd::model::Agent {
    store
        .create_agent(CreateAgent {
            name: name.to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create agent")
}

fn participant(agent_id: &str, delivery_mode: MembershipDeliveryMode) -> CreateChannelMember {
    CreateChannelMember {
        agent_id: agent_id.to_owned(),
        delivery_mode,
    }
}

#[tokio::test]
async fn direct_conversation_open_is_exact_pair_idempotent_and_concurrency_safe() {
    let (_directory, store) = test_store().await;
    let human = create_agent(&store, "direct-human").await;
    let worker = create_agent(&store, "direct-worker").await;
    let input = OpenDirectConversation {
        members: vec![
            participant(&human.id, MembershipDeliveryMode::StreamOnly),
            participant(&worker.id, MembershipDeliveryMode::Inbox),
        ],
    };

    let (first, second) = tokio::join!(
        store.open_direct_pair(input.clone()),
        store.open_direct_pair(OpenDirectConversation {
            members: input.members.iter().cloned().rev().collect(),
        })
    );
    let (first_channel, first_created) = first.expect("first concurrent open");
    let (second_channel, second_created) = second.expect("second concurrent open");
    assert_eq!(first_channel.id, second_channel.id);
    assert_ne!(first_created, second_created);
    assert_eq!(first_channel.kind, ConversationKind::Direct);

    // The pair is substrate; its membership is the projection over it.
    let opened = conversation::summary(&store, &first_channel.id)
        .await
        .expect("present the opened pair");
    assert_eq!(opened.members.len(), 2);

    let listed = conversation::list(&store, false)
        .await
        .expect("list conversations");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, first_channel.id);

    let incompatible = store
        .open_direct_pair(OpenDirectConversation {
            members: vec![
                participant(&human.id, MembershipDeliveryMode::Inbox),
                participant(&worker.id, MembershipDeliveryMode::Inbox),
            ],
        })
        .await
        .expect_err("immutable mode mismatch");
    assert!(matches!(
        incompatible,
        fleetd::error::FleetError::Conflict(_)
    ));
    let fixed_membership = store
        .add_member_with_mode(
            &first_channel.id,
            &human.id,
            MembershipDeliveryMode::StreamOnly,
        )
        .await
        .expect_err("direct membership cannot be mutated through channel API");
    assert!(matches!(
        fixed_membership,
        fleetd::error::FleetError::Conflict(_)
    ));
    let renamed = store
        .rename_channel(&first_channel.id, "not-a-direct-name".to_owned())
        .await
        .expect_err("direct conversation name is fixed");
    assert!(matches!(renamed, fleetd::error::FleetError::Conflict(_)));
    let archived = store
        .archive_channel(&first_channel.id)
        .await
        .expect_err("direct conversation lifecycle is fixed");
    assert!(matches!(archived, fleetd::error::FleetError::Conflict(_)));

    for invalid_members in [
        vec![participant(&human.id, MembershipDeliveryMode::StreamOnly)],
        vec![
            participant(&human.id, MembershipDeliveryMode::StreamOnly),
            participant(&human.id, MembershipDeliveryMode::Inbox),
        ],
    ] {
        let invalid = store
            .open_direct_pair(OpenDirectConversation {
                members: invalid_members,
            })
            .await
            .expect_err("invalid participant set");
        assert!(matches!(invalid, fleetd::error::FleetError::Invalid(_)));
    }
}

#[tokio::test]
async fn shared_channel_rename_and_archive_preserve_history_and_close_writes() {
    let (_directory, store) = test_store().await;
    let sender = create_agent(&store, "archive-sender").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "before-archive".to_owned(),
            metadata: json!({ "opaque": true }),
            member_ids: vec![sender.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create shared channel");
    let renamed = store
        .rename_channel(&channel.id, "after-rename".to_owned())
        .await
        .expect("rename shared channel");
    assert_eq!(renamed.name, "after-rename");
    assert_eq!(renamed.kind, ConversationKind::Shared);

    let message = store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: sender.id.clone(),
                idempotency_key: None,
                recipient_id: None,
                kind: "unknown.product/v1".to_owned(),
                payload: json!({ "kept": [1, true, null] }),
                correlation_id: None,
                causation_id: None,
            },
        )
        .await
        .expect("append before archive");
    let archived = store
        .archive_channel(&channel.id)
        .await
        .expect("archive channel");
    let replay = store
        .archive_channel(&channel.id)
        .await
        .expect("idempotent archive");
    assert_eq!(replay.archived_at_ms, archived.archived_at_ms);
    assert!(archived.archived_at_ms.is_some());
    assert!(
        conversation::list(&store, false)
            .await
            .expect("active conversations")
            .is_empty()
    );
    let all = conversation::list(&store, true)
        .await
        .expect("all conversations");
    assert_eq!(all[0].latest_message_seq, Some(message.seq));
    assert_eq!(all[0].latest_message_at_ms, Some(message.created_at_ms));

    let append = store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: sender.id,
                idempotency_key: None,
                recipient_id: None,
                kind: "text".to_owned(),
                payload: json!({ "text": "too late" }),
                correlation_id: None,
                causation_id: None,
            },
        )
        .await
        .expect_err("archived channel rejects append");
    assert!(matches!(append, fleetd::error::FleetError::Conflict(_)));
    let history = store
        .list_messages(&channel.id, 0, 100)
        .await
        .expect("archived history remains readable");
    assert_eq!(history.messages, vec![message]);
}

/// Opening a direct conversation is this suite's own subject, so it stays here.
async fn open_direct(server: &Daemon, members: Vec<CreateChannelMember>) -> reqwest::Response {
    server
        .post("/v1/direct-conversations", Some(&server.operator_token))
        .json(&OpenDirectConversation { members })
        .send()
        .await
        .expect("open direct request")
}

#[tokio::test]
async fn direct_conversation_http_open_is_idempotent_and_discoverable() {
    let server = Daemon::start().await;
    let human = server.register("api-human").await;
    let worker = server.register("api-worker").await;
    let opened = open_direct(
        &server,
        vec![
            participant(&human.agent.id, MembershipDeliveryMode::StreamOnly),
            participant(&worker.agent.id, MembershipDeliveryMode::Inbox),
        ],
    )
    .await;
    assert_eq!(opened.status(), reqwest::StatusCode::CREATED);
    let direct: fleetd::model::ConversationSummary = opened.json().await.expect("direct body");
    assert_eq!(direct.kind, ConversationKind::Direct);
    let replay = open_direct(
        &server,
        vec![
            participant(&worker.agent.id, MembershipDeliveryMode::Inbox),
            participant(&human.agent.id, MembershipDeliveryMode::StreamOnly),
        ],
    )
    .await;
    assert_eq!(replay.status(), reqwest::StatusCode::OK);

    let active: Vec<fleetd::model::ConversationSummary> = server
        .get("/v1/conversations", Some(&server.operator_token))
        .send()
        .await
        .expect("active list")
        .error_for_status()
        .expect("active list status")
        .json()
        .await
        .expect("active list body");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].id, direct.id);
}

#[tokio::test]
async fn shared_channel_http_lifecycle_is_closed_after_archive() {
    let server = Daemon::start().await;
    let human = server.register("api-shared-human").await;
    let channel: fleetd::model::Channel = server
        .post("/v1/channels", Some(&server.operator_token))
        .json(&CreateChannel {
            name: "api-shared".to_owned(),
            metadata: json!({}),
            member_ids: vec![human.agent.id.clone()],
            members: Vec::new(),
        })
        .send()
        .await
        .expect("create channel")
        .error_for_status()
        .expect("create channel status")
        .json()
        .await
        .expect("channel body");
    let renamed: fleetd::model::Channel = server
        .request(
            reqwest::Method::PATCH,
            &format!("/v1/channels/{}", channel.id),
            Some(&server.operator_token),
        )
        .json(&RenameChannel {
            name: "api-renamed".to_owned(),
        })
        .send()
        .await
        .expect("rename request")
        .error_for_status()
        .expect("rename status")
        .json()
        .await
        .expect("renamed body");
    assert_eq!(renamed.name, "api-renamed");
    let archived: fleetd::model::Channel = server
        .post(
            &format!("/v1/channels/{}/archive", channel.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("archive request")
        .error_for_status()
        .expect("archive status")
        .json()
        .await
        .expect("archived body");
    assert!(archived.archived_at_ms.is_some());

    let active: Vec<fleetd::model::ConversationSummary> = server
        .get("/v1/conversations", Some(&server.operator_token))
        .send()
        .await
        .expect("active list")
        .error_for_status()
        .expect("active list status")
        .json()
        .await
        .expect("active list body");
    assert!(active.is_empty());
    let all: Vec<fleetd::model::ConversationSummary> = server
        .get(
            "/v1/conversations?include_archived=true",
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("all list")
        .error_for_status()
        .expect("all list status")
        .json()
        .await
        .expect("all list body");
    assert_eq!(all.len(), 1);

    let send_to_archived = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/channels/{}/messages",
            server.address, channel.id
        ))
        .bearer_auth(&human.credential.token)
        .json(&SendMessage {
            idempotency_key: None,
            recipient_id: None,
            kind: "text".to_owned(),
            payload: json!({ "text": "closed" }),
            correlation_id: None,
            causation_id: None,
        })
        .send()
        .await
        .expect("archived append response");
    assert_eq!(send_to_archived.status(), reqwest::StatusCode::CONFLICT);
}
