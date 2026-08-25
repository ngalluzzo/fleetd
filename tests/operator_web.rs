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
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL),
            Some(&header::HeaderValue::from_static("no-store")),
            "{path}"
        );
        assert_eq!(
            response.headers().get(header::REFERRER_POLICY),
            Some(&header::HeaderValue::from_static("no-referrer")),
            "{path}"
        );
    }

    let contract = body(app.clone(), "/operator/contract.json").await;
    let observed: Value = serde_json::from_slice(&contract).expect("served contract JSON");
    assert!(observed.is_object());
    assert_eq!(observed["record_type"], "BlockedDelivery");
    assert_eq!(observed["binding"]["list_path"], "/v1/delivery-blocks");

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

#[tokio::test]
async fn conversation_assets_are_public_exact_and_browser_hardened() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let app = router(AppState::new(store));

    for (path, content_type) in [
        ("/conversation/", "text/html; charset=utf-8"),
        ("/conversation/conversation.css", "text/css; charset=utf-8"),
        (
            "/conversation/conversation.js",
            "text/javascript; charset=utf-8",
        ),
        ("/conversation/contract.json", "application/json"),
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
            response.headers().get(header::CACHE_CONTROL),
            Some(&header::HeaderValue::from_static("no-store")),
            "{path}"
        );
        assert_eq!(
            response.headers().get(header::REFERRER_POLICY),
            Some(&header::HeaderValue::from_static("no-referrer")),
            "{path}"
        );
    }

    let contract = body(app.clone(), "/conversation/contract.json").await;
    let observed: Value = serde_json::from_slice(&contract).expect("served contract JSON");
    assert_eq!(observed["schema_version"], 1);
    assert_eq!(observed["authority"]["channel_discovery"], "operator");
    assert_eq!(observed["authority"]["send"], "human_participant");
    assert_eq!(observed["unknown_message_fallback"], "exact_json_envelope");

    let html = String::from_utf8(body(app.clone(), "/conversation/").await).expect("UTF-8 HTML");
    for marker in [
        "id=\"connect-form\"",
        "id=\"operator-credential\"",
        "id=\"participant-credential\"",
        "id=\"channel-list\"",
        "id=\"message-list\"",
        "id=\"composer\"",
    ] {
        assert!(html.contains(marker), "missing {marker}");
    }
    assert!(!html.contains("<script>"));
    assert!(!html.contains("<style>"));

    let script = String::from_utf8(body(app, "/conversation/conversation.js").await)
        .expect("UTF-8 JavaScript");
    assert!(script.contains("openBrowserChannelStream"));
    assert!(script.contains("fleetdConversationReady"));
    for forbidden in [
        "localStorage",
        "sessionStorage",
        "indexedDB",
        "document.cookie",
        "innerHTML",
        "setInterval",
    ] {
        assert!(
            !script.contains(forbidden),
            "forbidden browser API: {forbidden}"
        );
    }
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
