//! Authorization over agent identity and credentials.

use fleetd::model::IssuedCredential;

mod common;

use common::api::{Daemon, claim};

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
