use fleetd::{
    auth::AuthService,
    http::{AppState, router},
    model::{
        ArmInvocation, BlockDelivery, BlockResolution, BlockedDelivery, ClaimBatch,
        ClaimDeliveries, CompleteInvocation, CreateAgent, CreateChannel, Invocation,
        InvocationBatch, InvocationCompletion, InvocationState, IssuedCredential, Message,
        MessagePage, RegisteredAgent, ResolveDeliveryBlock, SendMessage,
    },
    store::Store,
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

    async fn channel(&self, members: &[&str]) -> fleetd::model::Channel {
        self.post("/v1/channels", Some(&self.operator_token))
            .json(&CreateChannel {
                name: "auth-test".to_owned(),
                metadata: json!({}),
                member_ids: members.iter().map(|member| (*member).to_owned()).collect(),
                members: Vec::new(),
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
async fn operational_read_models_require_the_operator() {
    let server = TestServer::start().await;
    let agent = server.register("operations-reader").await;
    for path in [
        "/v1/plugin-generations",
        "/v1/invocation-observations",
        "/v1/session-bindings",
    ] {
        let operator = server
            .get(path, Some(&server.operator_token))
            .send()
            .await
            .expect("operator read model response");
        assert_eq!(operator.status(), reqwest::StatusCode::OK);
        assert_eq!(
            operator
                .json::<serde_json::Value>()
                .await
                .expect("read model body"),
            json!([])
        );

        let forbidden = server
            .get(path, Some(&agent.credential.token))
            .send()
            .await
            .expect("agent read model response");
        assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn channel_membership_listing_is_exact_bounded_and_authorized() {
    let server = TestServer::start().await;
    let worker = server.register("membership-worker").await;
    let human = server.register("membership-human").await;
    let outsider = server.register("membership-outsider").await;

    let created = server
        .post("/v1/channels", Some(&server.operator_token))
        .json(&json!({
            "name": "mixed-membership-api",
            "metadata": { "opaque": "channel metadata" },
            "member_ids": [worker.agent.id],
            "members": [{
                "agent_id": human.agent.id,
                "delivery_mode": "stream_only"
            }]
        }))
        .send()
        .await
        .expect("create mixed membership channel");
    assert_eq!(created.status(), reqwest::StatusCode::CREATED);
    let channel: fleetd::model::Channel = created.json().await.expect("channel body");

    let operator_members = server
        .get(
            &format!("/v1/channels/{}/members", channel.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("operator member list");
    assert_eq!(operator_members.status(), reqwest::StatusCode::OK);
    let memberships: Vec<serde_json::Value> =
        operator_members.json().await.expect("membership list body");
    assert_eq!(memberships.len(), 2);
    for membership in &memberships {
        let fields = membership.as_object().expect("membership object");
        assert_eq!(
            fields.len(),
            5,
            "bounded read model must expose five fields"
        );
        for field in [
            "channel_id",
            "agent_id",
            "agent_name",
            "joined_at_ms",
            "delivery_mode",
        ] {
            assert!(fields.contains_key(field), "missing bounded field {field}");
        }
        assert!(!fields.contains_key("metadata"));
    }
    assert_eq!(
        memberships
            .iter()
            .find(|membership| membership["agent_id"] == human.agent.id)
            .expect("human membership")["delivery_mode"],
        "stream_only"
    );
    assert_eq!(
        memberships
            .iter()
            .find(|membership| membership["agent_id"] == worker.agent.id)
            .expect("worker membership")["delivery_mode"],
        "inbox"
    );

    let member_view = server
        .get(
            &format!("/v1/channels/{}/members", channel.id),
            Some(&human.credential.token),
        )
        .send()
        .await
        .expect("exact member list");
    assert_eq!(member_view.status(), reqwest::StatusCode::OK);
    server.channel(&[&outsider.agent.id]).await;
    let outsider_view = server
        .get(
            &format!("/v1/channels/{}/members", channel.id),
            Some(&outsider.credential.token),
        )
        .send()
        .await
        .expect("outsider member list");
    assert_eq!(outsider_view.status(), reqwest::StatusCode::FORBIDDEN);
    let unknown = server
        .get(
            "/v1/channels/unknown-channel/members",
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("unknown channel list");
    assert_eq!(unknown.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn channel_membership_writes_are_atomic_immutable_and_strict() {
    let server = TestServer::start().await;
    let worker = server.register("membership-write-worker").await;
    let legacy = server.register("membership-write-legacy").await;
    let passive = server.register("membership-write-passive").await;
    let channel = server.channel(&[&worker.agent.id]).await;

    let legacy_added = server
        .post(
            &format!("/v1/channels/{}/members", channel.id),
            Some(&server.operator_token),
        )
        .json(&json!({ "agent_id": legacy.agent.id }))
        .send()
        .await
        .expect("add member with omitted mode");
    assert_eq!(legacy_added.status(), reqwest::StatusCode::NO_CONTENT);
    let legacy_replay = server
        .post(
            &format!("/v1/channels/{}/members", channel.id),
            Some(&server.operator_token),
        )
        .json(&json!({
            "agent_id": legacy.agent.id,
            "delivery_mode": "inbox"
        }))
        .send()
        .await
        .expect("replay exact membership");
    assert_eq!(legacy_replay.status(), reqwest::StatusCode::NO_CONTENT);

    let stream_added = server
        .post(
            &format!("/v1/channels/{}/members", channel.id),
            Some(&server.operator_token),
        )
        .json(&json!({
            "agent_id": passive.agent.id,
            "delivery_mode": "stream_only"
        }))
        .send()
        .await
        .expect("add explicit stream-only member");
    assert_eq!(stream_added.status(), reqwest::StatusCode::NO_CONTENT);
    let mismatch = server
        .post(
            &format!("/v1/channels/{}/members", channel.id),
            Some(&server.operator_token),
        )
        .json(&json!({
            "agent_id": legacy.agent.id,
            "delivery_mode": "stream_only"
        }))
        .send()
        .await
        .expect("attempt membership mode mutation");
    assert_eq!(mismatch.status(), reqwest::StatusCode::CONFLICT);

    let duplicate = server
        .post("/v1/channels", Some(&server.operator_token))
        .json(&json!({
            "name": "duplicate-membership-must-rollback",
            "member_ids": [worker.agent.id],
            "members": [{
                "agent_id": worker.agent.id,
                "delivery_mode": "stream_only"
            }]
        }))
        .send()
        .await
        .expect("duplicate initial membership response");
    assert_eq!(duplicate.status(), reqwest::StatusCode::BAD_REQUEST);
    let channels: Vec<fleetd::model::Channel> = server
        .get("/v1/channels", Some(&server.operator_token))
        .send()
        .await
        .expect("list channels")
        .error_for_status()
        .expect("channel list status")
        .json()
        .await
        .expect("channel list body");
    assert!(
        channels
            .iter()
            .all(|candidate| candidate.name != "duplicate-membership-must-rollback")
    );

    for invalid in [
        json!({ "agent_id": worker.agent.id, "delivery_mode": "unknown" }),
        json!({ "agent_id": worker.agent.id, "unknown": true }),
    ] {
        let response = server
            .post(
                &format!("/v1/channels/{}/members", channel.id),
                Some(&server.operator_token),
            )
            .json(&invalid)
            .send()
            .await
            .expect("invalid member input");
        assert_eq!(response.status(), reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    }
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

    let envelope_extension = server
        .post(
            &format!("/v1/channels/{}/messages", channel.id),
            Some(&alice.credential.token),
        )
        .json(&json!({
            "recipient_id": bob.agent.id,
            "kind": "future-contract/v7",
            "payload": { "opaque_extension": [1, true, null] },
            "correlation_id": null,
            "causation_id": null,
            "future_envelope_extension": { "must_not_be_discarded": true }
        }))
        .send()
        .await
        .expect("unknown envelope field response");
    assert_eq!(
        envelope_extension.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

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

#[tokio::test]
async fn blocked_delivery_authority_is_split_between_worker_and_operator() {
    let server = TestServer::start().await;
    let alice = server.register("blocking-alice").await;
    let bob = server.register("blocking-bob").await;
    let channel = server.channel(&[&alice.agent.id, &bob.agent.id]).await;
    let sent = send_message(&server, &channel.id, &alice, &bob.agent.id).await;
    let batch: ClaimBatch = claim(&server, &bob.agent.id, &bob.credential.token)
        .await
        .error_for_status()
        .expect("bound inbox claim")
        .json()
        .await
        .expect("claim body");
    let input = BlockDelivery {
        lease_token: batch.lease_token,
        reason: "remote tool outcome is unknown".to_owned(),
    };
    let block_path = format!("/v1/agents/{}/deliveries/{}/block", bob.agent.id, sent.id);

    let operator_block = server
        .post(&block_path, Some(&server.operator_token))
        .json(&input)
        .send()
        .await
        .expect("operator block response");
    assert_eq!(operator_block.status(), reqwest::StatusCode::FORBIDDEN);
    let cross_agent_block = server
        .post(&block_path, Some(&alice.credential.token))
        .json(&input)
        .send()
        .await
        .expect("cross-agent block response");
    assert_eq!(cross_agent_block.status(), reqwest::StatusCode::FORBIDDEN);

    let first_response = server
        .post(&block_path, Some(&bob.credential.token))
        .json(&input)
        .send()
        .await
        .expect("block response");
    assert_eq!(first_response.status(), reqwest::StatusCode::CREATED);
    let blocked: BlockedDelivery = first_response.json().await.expect("block body");
    let replay_response = server
        .post(&block_path, Some(&bob.credential.token))
        .json(&input)
        .send()
        .await
        .expect("block replay response");
    assert_eq!(replay_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        replay_response
            .json::<BlockedDelivery>()
            .await
            .expect("block replay body"),
        blocked
    );

    let agent_list = server
        .get("/v1/delivery-blocks", Some(&bob.credential.token))
        .send()
        .await
        .expect("agent block list response");
    assert_eq!(agent_list.status(), reqwest::StatusCode::FORBIDDEN);
    let listed: Vec<BlockedDelivery> = server
        .get(
            &format!("/v1/delivery-blocks?agent={}", bob.agent.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("operator block list response")
        .error_for_status()
        .expect("operator block list status")
        .json()
        .await
        .expect("operator block list body");
    assert_eq!(listed, vec![blocked.clone()]);

    assert_resolution_authority(&server, &bob, blocked.block_id).await;
}

#[tokio::test]
async fn managed_invocations_are_agent_bound_and_operator_observable() {
    let server = TestServer::start().await;
    let alice = server.register("invocation-alice").await;
    let bob = server.register("invocation-bob").await;
    let channel = server.channel(&[&alice.agent.id, &bob.agent.id]).await;
    send_message(&server, &channel.id, &alice, &bob.agent.id).await;
    let reserve_path = format!("/v1/agents/{}/invocations/reserve", bob.agent.id);
    let request = ClaimDeliveries {
        limit: 1,
        lease_duration_ms: 10_000,
    };

    for (label, token) in [
        ("operator", server.operator_token.as_str()),
        ("other agent", alice.credential.token.as_str()),
    ] {
        let response = server
            .post(&reserve_path, Some(token))
            .json(&request)
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} reserve response: {error}"));
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN, "{label}");
    }
    let batch: InvocationBatch = server
        .post(&reserve_path, Some(&bob.credential.token))
        .json(&request)
        .send()
        .await
        .expect("bound reserve response")
        .error_for_status()
        .expect("bound reserve status")
        .json()
        .await
        .expect("bound reserve body");
    assert_eq!(batch.invocations.len(), 1);
    let invocation = &batch.invocations[0];
    let arm_path = format!(
        "/v1/agents/{}/invocations/{}/arm",
        bob.agent.id, invocation.id
    );
    let arm = ArmInvocation {
        lease_token: invocation.lease_token.clone(),
        fence_token: invocation.fence_token.clone(),
    };
    let cross_agent = server
        .post(&arm_path, Some(&alice.credential.token))
        .json(&arm)
        .send()
        .await
        .expect("cross-agent arm response");
    assert_eq!(cross_agent.status(), reqwest::StatusCode::FORBIDDEN);
    let operator = server
        .post(&arm_path, Some(&server.operator_token))
        .json(&arm)
        .send()
        .await
        .expect("operator arm response");
    assert_eq!(operator.status(), reqwest::StatusCode::FORBIDDEN);
    let armed: Invocation = server
        .post(&arm_path, Some(&bob.credential.token))
        .json(&arm)
        .send()
        .await
        .expect("bound arm response")
        .error_for_status()
        .expect("bound arm status")
        .json()
        .await
        .expect("bound arm body");
    assert_eq!(armed.state, InvocationState::DispatchArmed);

    let agent_list = server
        .get("/v1/invocations", Some(&bob.credential.token))
        .send()
        .await
        .expect("agent invocation list response");
    assert_eq!(agent_list.status(), reqwest::StatusCode::FORBIDDEN);
    let listed: Vec<Invocation> = server
        .get(
            &format!("/v1/invocations?agent={}", bob.agent.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("operator invocation list response")
        .error_for_status()
        .expect("operator invocation list status")
        .json()
        .await
        .expect("operator invocation list body");
    assert_eq!(listed, vec![armed.clone()]);
    assert_completion_authority(&server, &alice, &bob, &armed).await;
}

async fn assert_completion_authority(
    server: &TestServer,
    other_agent: &RegisteredAgent,
    owner: &RegisteredAgent,
    invocation: &Invocation,
) {
    let path = format!(
        "/v1/agents/{}/invocations/{}/complete",
        owner.agent.id, invocation.id
    );
    let input = CompleteInvocation {
        lease_token: invocation.lease_token.clone(),
        fence_token: invocation.fence_token.clone(),
        kind: "work.result/v1".to_owned(),
        payload: json!({ "status": "done" }),
    };
    for (label, token) in [
        ("operator", server.operator_token.as_str()),
        ("other agent", other_agent.credential.token.as_str()),
    ] {
        let response = server
            .post(&path, Some(token))
            .json(&input)
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} completion response: {error}"));
        assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN, "{label}");
    }
    let first_response = server
        .post(&path, Some(&owner.credential.token))
        .json(&input)
        .send()
        .await
        .expect("bound completion response");
    assert_eq!(first_response.status(), reqwest::StatusCode::CREATED);
    let first: InvocationCompletion = first_response.json().await.expect("completion body");
    assert_eq!(first.invocation.state, InvocationState::Terminal);
    assert_eq!(first.result.sender_id, owner.agent.id);

    let replay_response = server
        .post(&path, Some(&owner.credential.token))
        .json(&input)
        .send()
        .await
        .expect("completion replay response");
    assert_eq!(replay_response.status(), reqwest::StatusCode::OK);
    assert_eq!(
        replay_response
            .json::<InvocationCompletion>()
            .await
            .expect("completion replay body"),
        first
    );
}

async fn assert_resolution_authority(server: &TestServer, agent: &RegisteredAgent, block_id: i64) {
    let resolution = ResolveDeliveryBlock {
        resolution: BlockResolution::Requeue,
        retry_after_ms: 0,
        note: Some("verified safe to retry".to_owned()),
    };
    let resolution_path = format!("/v1/delivery-blocks/{block_id}/resolve");
    let agent_resolution = server
        .post(&resolution_path, Some(&agent.credential.token))
        .json(&resolution)
        .send()
        .await
        .expect("agent resolution response");
    assert_eq!(agent_resolution.status(), reqwest::StatusCode::FORBIDDEN);
    for label in ["first resolution", "resolution replay"] {
        let response = server
            .post(&resolution_path, Some(&server.operator_token))
            .json(&resolution)
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} response: {error}"));
        assert_eq!(
            response.status(),
            reqwest::StatusCode::NO_CONTENT,
            "{label}"
        );
    }
    let conflicting_resolution = server
        .post(&resolution_path, Some(&server.operator_token))
        .json(&ResolveDeliveryBlock {
            resolution: BlockResolution::Abandon,
            retry_after_ms: 0,
            note: Some("changed decision".to_owned()),
        })
        .send()
        .await
        .expect("conflicting resolution response");
    assert_eq!(
        conflicting_resolution.status(),
        reqwest::StatusCode::CONFLICT
    );

    let reclaimed: ClaimBatch = claim(server, &agent.agent.id, &agent.credential.token)
        .await
        .error_for_status()
        .expect("claim after resolution")
        .json()
        .await
        .expect("claim body after resolution");
    assert_eq!(reclaimed.deliveries.len(), 1);
    assert_eq!(reclaimed.deliveries[0].attempt, 2);
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
