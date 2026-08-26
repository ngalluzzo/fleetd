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
async fn an_evidence_cursor_is_refused_unless_both_halves_arrive() {
    let server = Daemon::start().await;
    for path in ["/v1/plugin-generations", "/v1/invocation-observations"] {
        // A cursor addresses a position, and a position is two halves. Reading
        // a half cursor as "start from the beginning" would silently rewind a
        // collector that dropped one parameter, so the surface refuses it
        // rather than serving evidence the caller has already archived.
        for query in ["after_ms=5", "after_id=abc"] {
            let response = server
                .get(&format!("{path}?{query}"), Some(&server.operator_token))
                .send()
                .await
                .expect("half cursor response");
            assert_eq!(
                response.status(),
                reqwest::StatusCode::BAD_REQUEST,
                "{path}?{query} must not be read as an absent cursor"
            );
        }

        let negative = server
            .get(
                &format!("{path}?after_ms=-1&after_id=abc"),
                Some(&server.operator_token),
            )
            .send()
            .await
            .expect("negative cursor response");
        assert_eq!(negative.status(), reqwest::StatusCode::BAD_REQUEST);

        let accepted = server
            .get(
                &format!("{path}?after_ms=0&after_id=&limit=1&settled=true&order=oldest"),
                Some(&server.operator_token),
            )
            .send()
            .await
            .expect("settled page response");
        assert_eq!(accepted.status(), reqwest::StatusCode::OK);
        assert_eq!(
            accepted
                .json::<serde_json::Value>()
                .await
                .expect("settled page body"),
            json!([]),
            "a cursor-addressed listing stays a plain array"
        );
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

#[tokio::test]
async fn tracing_an_unknown_invocation_is_a_not_found_for_the_operator_alone() {
    let server = Daemon::start().await;
    let agent = server.register("trace-reader").await;
    let path = "/v1/invocations/missing-invocation/trace";

    let forbidden = server
        .get(path, Some(&agent.credential.token))
        .send()
        .await
        .expect("agent trace response");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    let missing = server
        .get(path, Some(&server.operator_token))
        .send()
        .await
        .expect("operator trace response");
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fleet_health_is_operator_only_and_bounds_its_census() {
    let server = Daemon::start().await;
    let agent = server.register("health-reader").await;

    let forbidden = server
        .get("/v1/fleet-health", Some(&agent.credential.token))
        .send()
        .await
        .expect("agent health response");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    // An idle fleet reports an empty census rather than an absent one, so an
    // operator can tell "nothing running" from "nothing known".
    let report = server
        .get("/v1/fleet-health", Some(&server.operator_token))
        .send()
        .await
        .expect("operator health response")
        .error_for_status()
        .expect("operator health status")
        .json::<serde_json::Value>()
        .await
        .expect("health body");
    assert_eq!(
        report,
        json!({
            "agent_id": null,
            "current_plugin_generations": [],
            "current_session_bindings": [],
            "active_invocations": [],
            "delivery_records": [],
            "deliveries": {
                "inspected": 0,
                "pending": 0,
                "leased": 0,
                "expired_leases": 0,
                "blocked": 0,
                "acknowledged": 0,
                "dead": 0
            }
        })
    );

    let scoped = server
        .get(
            &format!("/v1/fleet-health?agent={}", agent.agent.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("scoped health response")
        .error_for_status()
        .expect("scoped health status")
        .json::<serde_json::Value>()
        .await
        .expect("scoped health body");
    assert_eq!(scoped["agent_id"], json!(agent.agent.id));

    for bad in ["0", "501"] {
        let rejected = server
            .get(
                &format!("/v1/fleet-health?delivery_limit={bad}"),
                Some(&server.operator_token),
            )
            .send()
            .await
            .expect("bounded health response");
        assert_eq!(
            rejected.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "delivery_limit={bad} must be refused"
        );
    }
}
