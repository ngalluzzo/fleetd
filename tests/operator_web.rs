use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use fleetd::{AppState, Store, router};
use serde_json::Value;
use tower::ServiceExt;

#[tokio::test]
async fn operator_assets_are_public_exact_and_browser_hardened() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let app = router(AppState::new(store));

    for (path, content_type) in [
        ("/operator/", "text/html; charset=utf-8"),
        ("/operator/operator.css", "text/css; charset=utf-8"),
        ("/operator/operator.js", "text/javascript; charset=utf-8"),
        ("/operator/contract.json", "application/json"),
    ] {
        let response = app
            .clone()
            .oneshot(request(path))
            .await
            .expect("asset response");
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some(content_type),
            "{path}"
        );
        let policy = response
            .headers()
            .get(header::CONTENT_SECURITY_POLICY)
            .and_then(|value| value.to_str().ok())
            .expect("content security policy");
        assert!(policy.contains("default-src 'none'"));
        assert!(policy.contains("script-src 'self'"));
        assert!(policy.contains("connect-src 'self'"));
        assert_eq!(
            response.headers().get(header::X_CONTENT_TYPE_OPTIONS),
            Some(&header::HeaderValue::from_static("nosniff"))
        );
    }

    let contract = body(app.clone(), "/operator/contract.json").await;
    let observed: Value = serde_json::from_slice(&contract).expect("served contract JSON");
    let request: Value =
        serde_json::from_str(include_str!("fixtures/gooir_runnable_web_request.json"))
            .expect("GOOIR request fixture");
    assert_eq!(observed, request["inputs"][0]["payload"]);

    let html = String::from_utf8(body(app.clone(), "/operator/").await).expect("UTF-8 HTML");
    for marker in [
        "id=\"operator-auth\"",
        "id=\"operator-token\"",
        "id=\"surface-status\"",
        "id=\"delivery-blocks\"",
    ] {
        assert!(html.contains(marker), "missing {marker}");
    }
    assert!(!html.contains("<script>"));
    assert!(!html.contains("<style>"));

    let script = String::from_utf8(body(app, "/operator/operator.js").await).expect("UTF-8 JS");
    assert!(script.contains("/operator/contract.json"));
    assert!(!script.contains("localStorage"));
    assert!(!script.contains("innerHTML"));
}

fn request(path: &str) -> Request<Body> {
    Request::builder()
        .uri(path)
        .body(Body::empty())
        .expect("request")
}

async fn body(app: axum::Router, path: &str) -> Vec<u8> {
    let response = app.oneshot(request(path)).await.expect("asset response");
    to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("asset body")
        .to_vec()
}
