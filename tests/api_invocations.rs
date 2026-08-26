//! Authorization over managed invocations.

use fleetd::model::{
    ArmInvocation, ClaimDeliveries, CompleteInvocation, Invocation, InvocationBatch,
    InvocationCompletion, InvocationState, RegisteredAgent,
};
use serde_json::json;

mod common;

use common::api::{Daemon, send_message};

#[tokio::test]
async fn managed_invocations_are_agent_bound_and_operator_observable() {
    let server = Daemon::start().await;
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
    server: &Daemon,
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
