use std::collections::BTreeSet;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use fleetd::{AppState, Store, openapi_document, router};
use serde_json::Value;
use tower::ServiceExt;

const COMMITTED_CONTRACT: &str = include_str!("../openapi/fleetd-v1.json");
const HTTP_METHODS: [&str; 8] = [
    "get", "put", "post", "delete", "options", "head", "patch", "trace",
];

fn generated_contract() -> Value {
    serde_json::to_value(openapi_document()).expect("serialize generated OpenAPI")
}

#[test]
fn committed_contract_matches_registered_handlers() {
    let committed: Value =
        serde_json::from_str(COMMITTED_CONTRACT).expect("parse committed OpenAPI");
    assert_eq!(committed, generated_contract());
}

#[test]
fn contract_has_stable_unique_operations_and_explicit_security() {
    let contract = generated_contract();
    assert_eq!(contract["openapi"], "3.1.0");
    assert_eq!(contract["info"]["version"], "1.3.0");
    assert_eq!(
        contract["components"]["securitySchemes"]["bearerAuth"]["scheme"],
        "bearer"
    );

    let paths = contract["paths"].as_object().expect("paths object");
    let mut operation_ids = BTreeSet::new();
    let mut operation_count = 0;
    for (path, path_item) in paths {
        let path_item = path_item.as_object().expect("path item object");
        for method in HTTP_METHODS {
            let Some(operation) = path_item.get(method) else {
                continue;
            };
            operation_count += 1;
            let operation_id = operation["operationId"]
                .as_str()
                .expect("operationId string");
            assert!(
                operation_ids.insert(operation_id.to_owned()),
                "duplicate operationId {operation_id}"
            );
            if path.starts_with("/v1/") && path != "/v1/browser/channel-stream" {
                assert_eq!(
                    operation["security"],
                    serde_json::json!([{ "bearerAuth": [] }]),
                    "{method} {path} must declare bearer authentication"
                );
            } else {
                assert!(
                    operation.get("security").is_none(),
                    "public operation {method} {path} must not require a credential"
                );
            }
        }
    }

    assert_eq!(operation_count, 31);
    assert_eq!(operation_ids.len(), operation_count);
}

#[test]
fn websocket_upgrade_declares_its_frame_contract() {
    let contract = generated_contract();
    let stream = &contract["paths"]["/v1/channels/{channel_id}/stream"]["get"];
    assert_eq!(
        stream["x-fleetd-websocket"]["messageSchema"]["$ref"],
        "#/components/schemas/Message"
    );
    assert_eq!(
        stream["responses"]["101"]["description"],
        "WebSocket protocol switched"
    );

    let browser = &contract["paths"]["/v1/browser/channel-stream"]["get"];
    assert!(browser.get("security").is_none());
    assert_eq!(
        browser["x-fleetd-websocket"]["firstClientMessageSchema"]["$ref"],
        "#/components/schemas/BrowserStreamRedemptionRequest"
    );
    assert_eq!(
        browser["x-fleetd-websocket"]["serverMessageSchema"]["$ref"],
        "#/components/schemas/BrowserStreamServerFrame"
    );
    assert_eq!(
        browser["responses"]["101"]["headers"]["Sec-WebSocket-Protocol"]["description"],
        "fleetd.channel-stream.browser.v1"
    );
    let issuance = &contract["paths"]["/v1/channels/{channel_id}/stream-grants"]["post"];
    assert_eq!(
        issuance["security"],
        serde_json::json!([{ "bearerAuth": [] }])
    );
    assert_eq!(
        issuance["responses"]["201"]["headers"]["Cache-Control"]["description"],
        "Always no-store"
    );
}

#[test]
fn browser_contract_has_no_raw_secret_fixture_or_secret_bearing_upgrade_field() {
    let generated = generated_contract();
    let committed: Value =
        serde_json::from_str(COMMITTED_CONTRACT).expect("parse committed OpenAPI");
    let embedded_fixture = format!(
        "Bearer fl_ag_{} trailing-metadata",
        URL_SAFE_NO_PAD.encode([0_u8; 32])
    );
    assert!(
        contains_raw_secret(&embedded_fixture),
        "the contract scanner must detect an embedded token-shaped fixture"
    );
    assert!(
        !contains_raw_secret("fl_ag_<redacted>"),
        "the contract scanner must permit an explicit redaction marker"
    );
    assert_no_raw_secret_fixture(&generated, "generated OpenAPI");
    assert_no_raw_secret_fixture(&committed, "committed OpenAPI");

    let browser = &generated["paths"]["/v1/browser/channel-stream"]["get"];
    assert!(browser.get("security").is_none());
    assert!(browser.get("parameters").is_none());
    assert_eq!(
        browser["x-fleetd-websocket"]["subprotocol"],
        "fleetd.channel-stream.browser.v1"
    );
    let response_headers = browser["responses"]["101"]["headers"]
        .as_object()
        .expect("upgrade response headers");
    assert_eq!(response_headers.len(), 1);
    assert!(response_headers.contains_key("Sec-WebSocket-Protocol"));

    let grant_schema = &generated["components"]["schemas"]["BrowserStreamGrant"];
    assert!(grant_schema.get("example").is_none());
    assert!(grant_schema.get("default").is_none());
    assert!(grant_schema.get("enum").is_none());
}

#[tokio::test]
async fn daemon_serves_the_generated_contract() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let response = router(AppState::new(store))
        .oneshot(
            Request::builder()
                .uri("/openapi.json")
                .body(Body::empty())
                .expect("contract request"),
        )
        .await
        .expect("contract response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read contract response");
    let served: Value = serde_json::from_slice(&body).expect("parse served OpenAPI");
    assert_eq!(served, generated_contract());
}

fn assert_no_raw_secret_fixture(value: &Value, surface: &str) {
    match value {
        Value::Array(values) => {
            for value in values {
                assert_no_raw_secret_fixture(value, surface);
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                assert!(
                    !contains_raw_secret(key),
                    "{surface} contains raw secret key"
                );
                assert_no_raw_secret_fixture(value, surface);
            }
        }
        Value::String(value) => {
            assert!(
                !contains_raw_secret(value),
                "{surface} contains a raw credential or stream-grant fixture"
            );
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn contains_raw_secret(value: &str) -> bool {
    ["fl_ag_", "fl_op_", "fl_sg_"].into_iter().any(|prefix| {
        value.match_indices(prefix).any(|(index, _)| {
            let encoded_start = index + prefix.len();
            let Some(encoded) = value.get(encoded_start..encoded_start + 43) else {
                return false;
            };
            URL_SAFE_NO_PAD
                .decode(encoded)
                .is_ok_and(|decoded| decoded.len() == 32)
        })
    })
}
