use std::{path::Path, sync::Arc};

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use fleetd::SendMessage;
use fleetd_soak::{
    CompletionSpec, FleetdEndpoint, ObserverSpec, RunStatus, SeedSpec, SoakPlan, WorkloadSpec,
    WorkloadStatus, execute_plan,
};
use serde::Deserialize;
use serde_json::{Value, json};

const OPERATOR_TOKEN: &str = "fleetd_operator_test";
const SENDER_TOKEN: &str = "fleetd_agent_test";
const SEED_ID: &str = "seed-message";

#[derive(Clone)]
struct MockState {
    fail_history: bool,
}

#[derive(Deserialize)]
struct HistoryQuery {
    #[allow(dead_code)]
    after: Option<i64>,
}

#[tokio::test]
async fn preserves_exact_causal_and_opaque_observer_evidence() {
    let (server, task) = spawn_mock(false).await;
    let directory = tempfile::tempdir().expect("temporary directory");
    let operator_path = private_token(directory.path(), "operator.token", OPERATOR_TOKEN);
    let sender_path = private_token(directory.path(), "sender.token", SENDER_TOKEN);
    let plan = plan(&server, operator_path, sender_path);

    let report = execute_plan(&plan, "sha256-of-plan".to_owned())
        .await
        .expect("execute plan");

    assert_eq!(report.status, RunStatus::Passed);
    assert_eq!(report.workloads.len(), 1);
    let workload = &report.workloads[0];
    assert_eq!(workload.status, WorkloadStatus::Passed);
    assert_eq!(
        workload.seed.as_ref().map(|message| message.id.as_str()),
        Some(SEED_ID)
    );
    assert_eq!(
        workload
            .completion
            .as_ref()
            .map(|message| message.id.as_str()),
        Some("completion-message")
    );
    assert_eq!(workload.invocation_observations.len(), 1);
    assert_eq!(
        workload.invocation_observations[0].source_message_id,
        SEED_ID
    );
    assert_eq!(
        workload.before.observers[0].document,
        Some(json!({"backend_private_shape": {"decode_tok_s": 31.25}}))
    );
    let encoded = serde_json::to_string(&report).expect("serialize report");
    assert!(!encoded.contains(OPERATOR_TOKEN));
    assert!(!encoded.contains(SENDER_TOKEN));
    task.abort();
}

#[tokio::test]
async fn records_poll_failure_after_dispatch_instead_of_losing_the_run() {
    let (server, task) = spawn_mock(true).await;
    let directory = tempfile::tempdir().expect("temporary directory");
    let operator_path = private_token(directory.path(), "operator.token", OPERATOR_TOKEN);
    let sender_path = private_token(directory.path(), "sender.token", SENDER_TOKEN);
    let plan = plan(&server, operator_path, sender_path);

    let report = execute_plan(&plan, "sha256-of-plan".to_owned())
        .await
        .expect("execute plan");

    assert_eq!(report.status, RunStatus::Failed);
    let workload = &report.workloads[0];
    assert_eq!(workload.status, WorkloadStatus::Failed);
    assert!(workload.seed.is_some());
    assert!(
        workload
            .error
            .as_deref()
            .is_some_and(|error| error.contains("evidence poll failed"))
    );
    assert!(workload.after.fleetd.is_some());
    task.abort();
}

#[cfg(unix)]
#[tokio::test]
async fn rejects_group_readable_credentials_before_network_access() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("temporary directory");
    let operator_path = private_token(directory.path(), "operator.token", OPERATOR_TOKEN);
    let sender_path = private_token(directory.path(), "sender.token", SENDER_TOKEN);
    std::fs::set_permissions(&sender_path, std::fs::Permissions::from_mode(0o640))
        .expect("weaken token permissions");
    let plan = plan("http://127.0.0.1:9", operator_path, sender_path);

    let error = execute_plan(&plan, "sha256-of-plan".to_owned())
        .await
        .expect_err("public credential must be rejected");
    assert!(error.to_string().contains("private regular file"));
}

async fn spawn_mock(fail_history: bool) -> (String, tokio::task::JoinHandle<()>) {
    let state = Arc::new(MockState { fail_history });
    let app = Router::new()
        .route("/v1/plugin-generations", get(empty_operator_list))
        .route("/v1/session-bindings", get(empty_operator_list))
        .route("/v1/delivery-blocks", get(empty_operator_list))
        .route("/v1/invocation-observations", get(invocation_observations))
        .route(
            "/v1/channels/channel/messages",
            post(append_seed).get(channel_history),
        )
        .route("/metrics", get(metrics))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock server");
    let address = listener.local_addr().expect("mock server address");
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve mock API");
    });
    (format!("http://{address}"), task)
}

async fn empty_operator_list(headers: HeaderMap) -> Response {
    if !authorized(&headers, OPERATOR_TOKEN) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(json!([])).into_response()
}

async fn invocation_observations(headers: HeaderMap) -> Response {
    if !authorized(&headers, OPERATOR_TOKEN) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(json!([{
        "invocation_id": "invocation-1",
        "agent_id": "seat-a",
        "source_message_id": SEED_ID,
        "result_message_id": "result-message",
        "generation_id": "generation-1",
        "binding_id": "binding-1",
        "binding_generation": 1,
        "owner_epoch": 1,
        "started_at_ms": 10,
        "updated_at_ms": 20,
        "first_event_at_ms": 11,
        "last_event_at_ms": 19,
        "event_count": 1,
        "observed_payload_bytes": 10,
        "last_event_seq": 1,
        "event_chain_digest": "sha256:event",
        "counts": {
            "assistant": 1,
            "reasoning": 0,
            "tool": 0,
            "plan": 0,
            "usage": 0,
            "metadata": 0,
            "permission": 0,
            "unknown": 0
        },
        "terminal_at_ms": 20,
        "stop_reason": "completed",
        "runtime_stop_reason": "end_turn",
        "execution_certainty": "outcome_known",
        "session_quiescent": true,
        "session_persistence": "confirmed",
        "usage": {}
    }]))
    .into_response()
}

async fn append_seed(headers: HeaderMap, Json(input): Json<SendMessage>) -> Response {
    if !authorized(&headers, SENDER_TOKEN) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let message = json!({
        "seq": 1,
        "id": SEED_ID,
        "channel_id": "channel",
        "sender_id": "seed-sender",
        "recipient_id": input.recipient_id,
        "kind": input.kind,
        "payload": input.payload,
        "correlation_id": null,
        "causation_id": null,
        "created_at_ms": 1
    });
    (StatusCode::CREATED, Json(message)).into_response()
}

async fn channel_history(
    State(state): State<Arc<MockState>>,
    headers: HeaderMap,
    Query(_query): Query<HistoryQuery>,
) -> Response {
    if !authorized(&headers, OPERATOR_TOKEN) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if state.fail_history {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "simulated history failure",
        )
            .into_response();
    }
    Json(json!({
        "messages": [
            {
                "seq": 1,
                "id": SEED_ID,
                "channel_id": "channel",
                "sender_id": "seed-sender",
                "recipient_id": "seat-a",
                "kind": "work.seed/v1",
                "payload": {"opaque": true},
                "correlation_id": null,
                "causation_id": null,
                "created_at_ms": 1
            },
            {
                "seq": 2,
                "id": "completion-message",
                "channel_id": "channel",
                "sender_id": "seat-a",
                "recipient_id": "seed-sender",
                "kind": "work.final/v1",
                "payload": {"arbitrary": [1, 2, 3]},
                "correlation_id": SEED_ID,
                "causation_id": SEED_ID,
                "created_at_ms": 2
            }
        ],
        "next_cursor": 2
    }))
    .into_response()
}

async fn metrics() -> Json<Value> {
    Json(json!({
        "backend_private_shape": {"decode_tok_s": 31.25}
    }))
}

fn authorized(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value == format!("Bearer {token}"))
}

fn plan(
    server: &str,
    operator_token_file: std::path::PathBuf,
    sender_token_file: std::path::PathBuf,
) -> SoakPlan {
    SoakPlan {
        schema_version: 1,
        run_id: "run-1".to_owned(),
        fleetd: FleetdEndpoint {
            server: server.to_owned(),
            operator_token_file,
            sender_token_file,
        },
        poll_interval_ms: 50,
        observers: vec![ObserverSpec {
            id: "model-server".to_owned(),
            url: format!("{server}/metrics"),
            required: true,
            timeout_ms: 1_000,
            max_bytes: 1_048_576,
        }],
        workloads: vec![WorkloadSpec {
            id: "workload-1".to_owned(),
            seed: SeedSpec {
                channel_id: "channel".to_owned(),
                recipient_id: "seat-a".to_owned(),
                kind: "work.seed/v1".to_owned(),
                payload: json!({"opaque": true}),
                idempotency_key: "soak/run-1/workload-1".to_owned(),
            },
            completion: CompletionSpec {
                kind: "work.final/v1".to_owned(),
                timeout_ms: 1_000,
                invocation_agents: vec!["seat-a".to_owned()],
            },
        }],
    }
}

fn private_token(directory: &Path, name: &str, token: &str) -> std::path::PathBuf {
    let path = directory.join(name);
    std::fs::write(&path, token).expect("write token");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("secure token");
    }
    path
}
