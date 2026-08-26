//! Authorization over channels, membership, and durable messages.

use fleetd::model::{ClaimBatch, IssuedCredential, Message, MessagePage, SendMessage};
use serde_json::json;

mod common;

use common::api::{Daemon, claim, post_message, send_message};

#[tokio::test]
async fn channel_membership_listing_is_exact_bounded_and_authorized() {
    let server = Daemon::start().await;
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
    let server = Daemon::start().await;
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
    let server = Daemon::start().await;
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
async fn channel_history_scopes_direct_messages_to_their_participants() {
    let server = Daemon::start().await;
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
    let server = Daemon::start().await;
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
