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
