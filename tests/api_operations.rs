//! Authorization over the operator read models.

use serde_json::json;

mod common;

use common::api::Daemon;

#[tokio::test]
async fn operational_read_models_require_the_operator() {
    let server = Daemon::start().await;
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
async fn an_agent_with_no_worker_projects_as_an_unmanaged_seat() {
    let server = Daemon::start().await;
    let agent = server.register("seat-without-worker").await;

    let seats = server
        .get("/v1/agent-seats", Some(&server.operator_token))
        .send()
        .await
        .expect("seat request")
        .json::<serde_json::Value>()
        .await
        .expect("seat body");

    // Unlike the other operator read models this is never empty: it projects a
    // row per agent, which is why it cannot share their empty-body assertion.
    // Every field is asserted because the point of the projection is that it
    // returns evidence and never a lease or fence token.
    assert_eq!(
        seats,
        json!([{
            "agent_id": agent.agent.id,
            "binding_generation": null,
            "binding_id": null,
            "delivery_state": null,
            "generation_health": null,
            "generation_id": null,
            "invocation_id": null,
            "invocation_state": null,
            "last_progress_at_ms": null,
            "lease_expired": false,
            "lease_expires_at_ms": null,
            "owner_epoch": null,
            "reason": "no_worker_observed",
            "session_state": null,
            "source_message_id": null,
            "state": "unmanaged",
            "unresolved_block_id": null
        }])
    );

    let forbidden = server
        .get("/v1/agent-seats", Some(&agent.credential.token))
        .send()
        .await
        .expect("agent seat request");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);
}
