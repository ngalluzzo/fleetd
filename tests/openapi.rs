use std::collections::BTreeSet;

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
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
    assert_eq!(contract["info"]["version"], "1.0.0");
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
            if path.starts_with("/v1/") {
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

    assert_eq!(operation_count, 24);
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
