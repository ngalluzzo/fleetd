//! Authorization over the leased inbox and its blocks.

use fleetd::model::{BlockDelivery, BlockedDelivery, ClaimBatch};

mod common;

use common::api::{Daemon, assert_resolution_authority, claim, send_message};

#[tokio::test]
async fn blocked_delivery_authority_is_split_between_worker_and_operator() {
    let server = Daemon::start().await;
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
async fn the_delivery_read_model_is_operator_only_and_hides_the_lease_token() {
    let server = Daemon::start().await;
    let sender = server.register("record-sender").await;
    let worker = server.register("record-worker").await;
    let channel = server.channel(&[&sender.agent.id, &worker.agent.id]).await;
    let message = send_message(&server, &channel.id, &sender, &worker.agent.id).await;

    let forbidden = server
        .get("/v1/deliveries", Some(&worker.credential.token))
        .send()
        .await
        .expect("agent delivery list response");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    let leased = claim(&server, &worker.agent.id, &worker.credential.token)
        .await
        .error_for_status()
        .expect("claim status")
        .json::<ClaimBatch>()
        .await
        .expect("claim body");

    let records = server
        .get("/v1/deliveries", Some(&server.operator_token))
        .send()
        .await
        .expect("operator delivery list response")
        .error_for_status()
        .expect("operator delivery list status")
        .json::<serde_json::Value>()
        .await
        .expect("delivery list body");
    let records = records.as_array().expect("a list of records");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["message"]["id"], serde_json::json!(message.id));
    assert_eq!(records[0]["state"], serde_json::json!("leased"));
    assert_eq!(records[0]["lease_expired"], serde_json::json!(false));

    // The record reports that a lease exists without handing over the token
    // that owns it, so reading operator state can never settle work.
    let serialized = serde_json::to_string(&records[0]).expect("serialize record");
    assert!(
        !serialized.contains(&leased.lease_token),
        "the read model must not carry the lease token"
    );
    assert!(
        !serialized.contains("lease_token"),
        "the read model must not carry a lease token field"
    );

    let filtered = server
        .get("/v1/deliveries?state=pending", Some(&server.operator_token))
        .send()
        .await
        .expect("filtered response")
        .error_for_status()
        .expect("filtered status")
        .json::<serde_json::Value>()
        .await
        .expect("filtered body");
    assert_eq!(filtered, serde_json::json!([]));

    for bad in ["0", "501"] {
        let rejected = server
            .get(
                &format!("/v1/deliveries?limit={bad}"),
                Some(&server.operator_token),
            )
            .send()
            .await
            .expect("bounded response");
        assert_eq!(
            rejected.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "limit={bad} must be refused"
        );
    }
}
