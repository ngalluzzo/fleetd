use fleetd::{
    auth::AuthService,
    http::{AppState, router},
    model::{
        ConversationKind, CreateAgent, CreateChannel, CreateChannelMember, CreateMessage,
        MembershipDeliveryMode, OpenDirectConversation, RenameChannel, SendMessage,
    },
    store::Store,
};
use serde_json::json;

async fn test_store() -> (tempfile::TempDir, Store) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
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
        store.open_direct_conversation(input.clone()),
        store.open_direct_conversation(OpenDirectConversation {
            members: input.members.iter().cloned().rev().collect(),
        })
    );
    let first = first.expect("first concurrent open");
    let second = second.expect("second concurrent open");
    assert_eq!(first.conversation.id, second.conversation.id);
    assert_ne!(first.created, second.created);
    assert_eq!(first.conversation.kind, ConversationKind::Direct);
    assert_eq!(first.conversation.members.len(), 2);

    let listed = store
        .list_conversations(false)
        .await
        .expect("list conversations");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, first.conversation.id);

    let incompatible = store
        .open_direct_conversation(OpenDirectConversation {
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
            &first.conversation.id,
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
        .rename_channel(&first.conversation.id, "not-a-direct-name".to_owned())
        .await
        .expect_err("direct conversation name is fixed");
    assert!(matches!(renamed, fleetd::error::FleetError::Conflict(_)));
    let archived = store
        .archive_channel(&first.conversation.id)
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
            .open_direct_conversation(OpenDirectConversation {
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
        store
            .list_conversations(false)
            .await
            .expect("active conversations")
            .is_empty()
    );
    let all = store
        .list_conversations(true)
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
        .list_messages(&channel.id, None, 0, 100)
        .await
        .expect("archived history remains readable");
    assert_eq!(history.messages, vec![message]);
}

struct TestServer {
    _directory: tempfile::TempDir,
    address: std::net::SocketAddr,
    operator_token: String,
    process: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = Store::open(directory.path().join("fleetd.db"))
            .await
            .expect("open store");
        let token_path = directory.path().join("operator.token");
        AuthService::new(store.clone())
            .ensure_operator_credential(&token_path)
            .await
            .expect("operator credential");
        let operator_token = std::fs::read_to_string(token_path)
            .expect("read operator token")
            .trim()
            .to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind server");
        let address = listener.local_addr().expect("server address");
        let process = tokio::spawn(async move {
            axum::serve(listener, router(AppState::new(store)))
                .await
                .expect("serve API");
        });
        Self {
            _directory: directory,
            address,
            operator_token,
            process,
        }
    }

    fn request(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .request(method, format!("http://{}{path}", self.address))
            .bearer_auth(&self.operator_token)
    }

    async fn register(&self, name: &str) -> fleetd::model::RegisteredAgent {
        self.request(reqwest::Method::POST, "/v1/agents")
            .json(&CreateAgent {
                name: name.to_owned(),
                metadata: json!({}),
            })
            .send()
            .await
            .expect("register request")
            .error_for_status()
            .expect("register response")
            .json()
            .await
            .expect("registration body")
    }

    async fn open_direct(&self, members: Vec<CreateChannelMember>) -> reqwest::Response {
        self.request(reqwest::Method::POST, "/v1/direct-conversations")
            .json(&OpenDirectConversation { members })
            .send()
            .await
            .expect("open direct request")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.process.abort();
    }
}

#[tokio::test]
async fn direct_conversation_http_open_is_idempotent_and_discoverable() {
    let server = TestServer::start().await;
    let human = server.register("api-human").await;
    let worker = server.register("api-worker").await;
    let opened = server
        .open_direct(vec![
            participant(&human.agent.id, MembershipDeliveryMode::StreamOnly),
            participant(&worker.agent.id, MembershipDeliveryMode::Inbox),
        ])
        .await;
    assert_eq!(opened.status(), reqwest::StatusCode::CREATED);
    let direct: fleetd::model::ConversationSummary = opened.json().await.expect("direct body");
    assert_eq!(direct.kind, ConversationKind::Direct);
    let replay = server
        .open_direct(vec![
            participant(&worker.agent.id, MembershipDeliveryMode::Inbox),
            participant(&human.agent.id, MembershipDeliveryMode::StreamOnly),
        ])
        .await;
    assert_eq!(replay.status(), reqwest::StatusCode::OK);

    let active: Vec<fleetd::model::ConversationSummary> = server
        .request(reqwest::Method::GET, "/v1/conversations")
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
    let server = TestServer::start().await;
    let human = server.register("api-shared-human").await;
    let channel: fleetd::model::Channel = server
        .request(reqwest::Method::POST, "/v1/channels")
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
        .request(
            reqwest::Method::POST,
            &format!("/v1/channels/{}/archive", channel.id),
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
        .request(reqwest::Method::GET, "/v1/conversations")
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
        .request(
            reqwest::Method::GET,
            "/v1/conversations?include_archived=true",
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
