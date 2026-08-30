#![cfg(unix)]

use std::{net::TcpListener, path::PathBuf, time::Duration};

use fleetd_plugin_host::{PluginProcess, PluginSpec, inference_openai_interface};
use serde_json::json;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local address")
        .port()
}

#[tokio::test]
async fn mlx_vlm_plugin_owns_a_ready_typed_route() {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_backend.py");
    let model = tempfile::tempdir().expect("model directory");
    let home = tempfile::tempdir().expect("home directory");
    let model_id = std::fs::canonicalize(model.path()).expect("canonical model directory");
    let port = free_port();
    let process = PluginProcess::start(
        PluginSpec::new(
            "fleetd.inference.mlx-vlm",
            env!("CARGO_BIN_EXE_fleetd-inference-mlx-vlm"),
        )
        .with_config(json!({
            "python_executable": fixture,
            "expected_version": "0.6.15",
            "home": home.path(),
            "model": model.path(),
            "model_id": model_id,
            "model_name": "Qwen local",
            "port": port,
            "apc_num_blocks": 64,
            "startup_timeout_ms": 5000
        }))
        .require_interface(inference_openai_interface())
        .with_initialize_timeout(Duration::from_secs(10))
        .with_request_timeout(Duration::from_secs(5)),
    )
    .await
    .expect("start MLX-VLM backend plugin");
    let mut inference = process.into_inference_openai().expect("typed inference");
    let description = inference.describe().await.expect("describe route");
    assert_eq!(description.backend.name, "MLX-VLM");
    assert_eq!(description.endpoint.model.id, model_id.to_string_lossy());
    assert_eq!(
        description.endpoint.base_url,
        format!("http://127.0.0.1:{port}/v1")
    );
    inference.health().await.expect("backend remains ready");
    inference.shutdown().await.expect("shutdown backend");
}
