use fleetd::{
    AppState, AuthService, ClaimBatch, ClaimDeliveries, CreateAgent, CreateChannel,
    IssuedCredential, Message, MessagePage, RegisteredAgent, SendMessage, Store, router,
};
use serde_json::json;

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
            .expect("bootstrap operator");
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

    fn get(&self, path: &str, token: Option<&str>) -> reqwest::RequestBuilder {
        authorize(
            reqwest::Client::new().get(format!("http://{}{path}", self.address)),
            token,
        )
    }

    fn post(&self, path: &str, token: Option<&str>) -> reqwest::RequestBuilder {
        authorize(
            reqwest::Client::new().post(format!("http://{}{path}", self.address)),
            token,
        )
    }

    async fn register(&self, name: &str) -> RegisteredAgent {
        self.post("/v1/agents", Some(&self.operator_token))
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

    async fn channel(&self, members: &[&str]) -> fleetd::Channel {
        self.post("/v1/channels", Some(&self.operator_token))
            .json(&CreateChannel {
                name: "auth-test".to_owned(),
                metadata: json!({}),
                member_ids: members.iter().map(|member| (*member).to_owned()).collect(),
            })
            .send()
            .await
            .expect("channel request")
            .error_for_status()
            .expect("channel response")
            .json()
            .await
            .expect("channel body")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.process.abort();
    }
}

#[tokio::test]
async fn administration_requires_an_operator_credential() {
    let server = TestServer::start().await;
    let missing = server
        .get("/v1/agents", None)
        .send()
        .await
        .expect("missing credential response");
    assert_eq!(missing.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert_eq!(
        missing.headers().get(reqwest::header::WWW_AUTHENTICATE),
        Some(&reqwest::header::HeaderValue::from_static("Bearer"))
    );
    let lowercase_scheme = reqwest::Client::new()
        .get(format!("http://{}/v1/agents", server.address))
        .header(
            reqwest::header::AUTHORIZATION,
            format!("bearer {}", server.operator_token),
        )
        .send()
        .await
        .expect("lowercase bearer response");
    assert_eq!(lowercase_scheme.status(), reqwest::StatusCode::OK);

    let agent = server.register("agent").await;
    let forbidden = server
        .get("/v1/agents", Some(&agent.credential.token))
        .send()
        .await
        .expect("agent administration response");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn sender_attribution_and_inbox_access_are_bound_to_the_agent_credential() {
    let server = TestServer::start().await;
    let alice = server.register("alice").await;
    let bob = server.register("bob").await;
    let outsider = server.register("outsider").await;
    let channel = server.channel(&[&alice.agent.id, &bob.agent.id]).await;

    let spoof = server
        .post(
            &format!("/v1/channels/{}/messages", channel.id),
            Some(&alice.credential.token),
        )
        .json(&json!({
            "sender_id": bob.agent.id,
            "recipient_id": bob.agent.id,
            "kind": "text",
            "payload": { "text": "spoof" },
            "correlation_id": null,
            "causation_id": null
        }))
        .send()
        .await
        .expect("spoof response");
    assert_eq!(spoof.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);

    let sent = send_message(&server, &channel.id, &alice, &bob.agent.id).await;
    assert_eq!(sent.sender_id, alice.agent.id);
    let operator_send = server
        .post(
            &format!("/v1/channels/{}/messages", channel.id),
            Some(&server.operator_token),
        )
        .json(&SendMessage {
            idempotency_key: None,
            recipient_id: Some(bob.agent.id.clone()),
            kind: "text".to_owned(),
            payload: json!({ "text": "operator impersonation" }),
            correlation_id: None,
            causation_id: None,
        })
        .send()
        .await
        .expect("operator send response");
    assert_eq!(operator_send.status(), reqwest::StatusCode::FORBIDDEN);
    let cross_agent = claim(&server, &bob.agent.id, &alice.credential.token).await;
    assert_eq!(cross_agent.status(), reqwest::StatusCode::FORBIDDEN);
    let operator_claim = claim(&server, &bob.agent.id, &server.operator_token).await;
    assert_eq!(operator_claim.status(), reqwest::StatusCode::FORBIDDEN);
    let outsider_history = server
        .get(
            &format!("/v1/channels/{}/messages", channel.id),
            Some(&outsider.credential.token),
        )
        .send()
        .await
        .expect("outsider history response");
    assert_eq!(outsider_history.status(), reqwest::StatusCode::FORBIDDEN);

    let batch: ClaimBatch = claim(&server, &bob.agent.id, &bob.credential.token)
        .await
        .error_for_status()
        .expect("bound inbox claim")
        .json()
        .await
        .expect("claim body");
    assert_eq!(batch.deliveries.len(), 1);
    assert_eq!(batch.deliveries[0].message, sent);
}

#[tokio::test]
async fn credential_rotation_revokes_the_old_token_on_the_next_request() {
    let server = TestServer::start().await;
    let agent = server.register("rotating-agent").await;
    let replacement: IssuedCredential = server
        .post(
            &format!("/v1/agents/{}/credentials/rotate", agent.agent.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("rotation request")
        .error_for_status()
        .expect("rotation response")
        .json()
        .await
        .expect("rotation body");
    let old = claim(&server, &agent.agent.id, &agent.credential.token).await;
    assert_eq!(old.status(), reqwest::StatusCode::UNAUTHORIZED);
    let current = claim(&server, &agent.agent.id, &replacement.token).await;
    assert_eq!(current.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn channel_history_scopes_direct_messages_to_their_participants() {
    let server = TestServer::start().await;
    let alice = server.register("alice").await;
    let bob = server.register("bob").await;
    let channel = server.channel(&[&alice.agent.id, &bob.agent.id]).await;

    let direct = send_message(&server, &channel.id, &alice, &bob.agent.id).await;
    let broadcast: Message = server
        .post(
            &format!("/v1/channels/{}/messages", channel.id),
            Some(&alice.credential.token),
        )
        .json(&SendMessage {
            idempotency_key: None,
            recipient_id: None,
            kind: "text".to_owned(),
            payload: json!({ "text": "standup" }),
            correlation_id: None,
            causation_id: None,
        })
        .send()
        .await
        .expect("broadcast request")
        .error_for_status()
        .expect("broadcast response")
        .json()
        .await
        .expect("broadcast body");

    let expected = vec![direct.clone(), broadcast.clone()];
    for (role, token) in [
        ("sender", &alice.credential.token),
        ("recipient", &bob.credential.token),
    ] {
        let page: MessagePage = server
            .get(
                &format!("/v1/channels/{}/messages", channel.id),
                Some(token),
            )
            .send()
            .await
            .expect("member history response")
            .error_for_status()
            .expect("member history status")
            .json()
            .await
            .expect("member history body");
        assert_eq!(page.messages, expected, "{role} history");
    }

    let operator_page: MessagePage = server
        .get(
            &format!("/v1/channels/{}/messages", channel.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("operator history response")
        .error_for_status()
        .expect("operator history status")
        .json()
        .await
        .expect("operator history body");
    assert_eq!(operator_page.messages, vec![direct, broadcast]);
}

#[tokio::test]
async fn message_idempotency_is_agent_scoped_and_survives_credential_rotation() {
    let server = TestServer::start().await;
    let alice = server.register("idempotent-alice").await;
    let bob = server.register("idempotent-bob").await;
    let channel = server.channel(&[&alice.agent.id, &bob.agent.id]).await;
    let input = SendMessage {
        idempotency_key: Some("invocation/abc/result".to_owned()),
        recipient_id: Some(bob.agent.id.clone()),
        kind: "agent.output/v1".to_owned(),
        payload: json!({ "text": "stable output" }),
        correlation_id: Some("work-abc".to_owned()),
        causation_id: Some("request-abc".to_owned()),
    };

    let first_response = post_message(&server, &channel.id, &alice.credential.token, &input).await;
    assert_eq!(first_response.status(), reqwest::StatusCode::CREATED);
    let first: Message = first_response.json().await.expect("first message");

    let duplicate_response =
        post_message(&server, &channel.id, &alice.credential.token, &input).await;
    assert_eq!(duplicate_response.status(), reqwest::StatusCode::OK);
    let duplicate: Message = duplicate_response.json().await.expect("duplicate message");
    assert_eq!(duplicate, first);

    let replacement: IssuedCredential = server
        .post(
            &format!("/v1/agents/{}/credentials/rotate", alice.agent.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("rotate credential")
        .error_for_status()
        .expect("rotation response")
        .json()
        .await
        .expect("replacement credential");
    let after_rotation = post_message(&server, &channel.id, &replacement.token, &input).await;
    assert_eq!(after_rotation.status(), reqwest::StatusCode::OK);
    assert_eq!(
        after_rotation
            .json::<Message>()
            .await
            .expect("message after rotation"),
        first
    );

    let mut conflicting = input.clone();
    conflicting.payload = json!({ "text": "changed output" });
    let conflict = post_message(&server, &channel.id, &replacement.token, &conflicting).await;
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);

    let other_scope = post_message(
        &server,
        &channel.id,
        &bob.credential.token,
        &SendMessage {
            idempotency_key: input.idempotency_key,
            recipient_id: Some(alice.agent.id),
            kind: input.kind,
            payload: input.payload,
            correlation_id: input.correlation_id,
            causation_id: input.causation_id,
        },
    )
    .await;
    assert_eq!(other_scope.status(), reqwest::StatusCode::CREATED);

    let batch: ClaimBatch = claim(&server, &bob.agent.id, &bob.credential.token)
        .await
        .error_for_status()
        .expect("claim result delivery")
        .json()
        .await
        .expect("claim body");
    assert_eq!(batch.deliveries.len(), 1);
    assert_eq!(batch.deliveries[0].message, first);
}

async fn send_message(
    server: &TestServer,
    channel_id: &str,
    sender: &RegisteredAgent,
    recipient_id: &str,
) -> Message {
    server
        .post(
            &format!("/v1/channels/{channel_id}/messages"),
            Some(&sender.credential.token),
        )
        .json(&SendMessage {
            idempotency_key: None,
            recipient_id: Some(recipient_id.to_owned()),
            kind: "review.requested/v1".to_owned(),
            payload: json!({ "commit": "4aa4cd1" }),
            correlation_id: Some("auth-test".to_owned()),
            causation_id: None,
        })
        .send()
        .await
        .expect("send request")
        .error_for_status()
        .expect("send response")
        .json()
        .await
        .expect("message body")
}

async fn post_message(
    server: &TestServer,
    channel_id: &str,
    token: &str,
    input: &SendMessage,
) -> reqwest::Response {
    server
        .post(&format!("/v1/channels/{channel_id}/messages"), Some(token))
        .json(input)
        .send()
        .await
        .expect("message response")
}

async fn claim(server: &TestServer, agent_id: &str, token: &str) -> reqwest::Response {
    server
        .post(
            &format!("/v1/agents/{agent_id}/deliveries/claim"),
            Some(token),
        )
        .json(&ClaimDeliveries {
            limit: 1,
            lease_duration_ms: 10_000,
        })
        .send()
        .await
        .expect("claim response")
}

fn authorize(request: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}
