//! Authorization over agent identity and credentials.

use fleetd::model::{Agent, IssuedCredential};
use sha2::{Digest, Sha256};

mod common;

use common::api::{Daemon, claim};

#[test]
fn generated_list_agents_adapter_matches_the_admitted_candidate() {
    let source = include_bytes!("../crates/http/src/agents/generated_list_agents.rs");
    assert_eq!(
        format!("sha256:{:x}", Sha256::digest(source)),
        "sha256:3c4e6292640ff8a52d3b0400aabf53b7e1774dee4da4a212fad0fcd3784ee5be"
    );
    assert!(!source.windows(5).any(|window| window == b"gooir"));
}

#[tokio::test]
async fn administration_requires_an_operator_credential() {
    let server = Daemon::start().await;
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
async fn list_agents_returns_the_exact_durable_projection_without_credentials() {
    let server = Daemon::start().await;
    let first = server.register("first-agent").await;
    let second = server.register("second-agent").await;

    let response = server
        .get("/v1/agents", Some(&server.operator_token))
        .send()
        .await
        .expect("list agents response")
        .error_for_status()
        .expect("list agents success");
    let body = response.bytes().await.expect("list agents body");
    let listed: Vec<Agent> = serde_json::from_slice(&body).expect("list agents JSON");

    let mut expected = vec![first.agent, second.agent];
    expected.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then_with(|| left.id.cmp(&right.id))
    });
    assert_eq!(listed, expected);

    let json: serde_json::Value = serde_json::from_slice(&body).expect("list agents JSON value");
    for agent in json.as_array().expect("agent array") {
        let object = agent.as_object().expect("agent object");
        assert_eq!(object.len(), 4);
        assert!(!object.contains_key("credential"));
        assert!(!object.contains_key("token"));
    }
}

#[tokio::test]
async fn credential_rotation_revokes_the_old_token_on_the_next_request() {
    let server = Daemon::start().await;
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
