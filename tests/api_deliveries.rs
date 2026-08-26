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
