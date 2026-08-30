#![cfg(unix)]

use std::{path::PathBuf, time::Duration};

use fleetd_plugin_host::{
    Binding, ExecutionFence, HarnessAcpNotification, OpenSession, OpenSessionMode, PluginProcess,
    PluginSpec, PromptBlock, StartTurn, ToolBudget, TurnPolicy, TurnSource, harness_acp_interface,
};
use serde_json::json;

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_deepseek.py")
}

fn local_plugin_spec(dsh_home: &std::path::Path) -> PluginSpec {
    PluginSpec::new(
        "fleetd.harness.deepseek",
        env!("CARGO_BIN_EXE_fleetd-harness-deepseek"),
    )
    .with_config(json!({
        "executable": fixture_path(),
        "expected_version": "0.0.1",
        "home": dsh_home,
        "dsh_home": dsh_home,
        "path": "/usr/bin:/bin",
        "term": "xterm-256color",
        "tmpdir": dsh_home,
        "tools_mode": "ptc",
        "reasoning_effort": "none",
        "max_output_tokens": 8192,
        "context_window": 262_144,
        "stream_idle_timeout_ms": 300_000,
        "inference": {
            "backend": {
                "name": "MLX-VLM",
                "version": "0.6.15",
                "executable_digest": format!("sha256:{}", "a".repeat(64))
            },
            "endpoint": {
                "base_url": "http://127.0.0.1:18082/v1",
                "model": {
                    "id": "/models/qwen",
                    "name": "Qwen",
                    "revision": null
                }
            },
            "profile_digest": format!("sha256:{}", "b".repeat(64)),
            "observer": null
        }
    }))
    .require_interface(harness_acp_interface())
    .with_initialize_timeout(Duration::from_secs(5))
    .with_request_timeout(Duration::from_secs(5))
    .with_shutdown_timeout(Duration::from_secs(5))
}

fn provider_plugin_spec(dsh_home: &std::path::Path) -> PluginSpec {
    PluginSpec::new(
        "fleetd.harness.deepseek",
        env!("CARGO_BIN_EXE_fleetd-harness-deepseek"),
    )
    .with_config(json!({
        "executable": fixture_path(),
        "expected_version": "0.0.1",
        "home": dsh_home,
        "dsh_home": dsh_home,
        "path": "/usr/bin:/bin",
        "term": "xterm-256color",
        "tmpdir": dsh_home,
        "tools_mode": "ptc",
        "provider": "zai",
        "model": "glm-5.3"
    }))
    .require_interface(harness_acp_interface())
    .with_initialize_timeout(Duration::from_secs(5))
    .with_request_timeout(Duration::from_secs(5))
    .with_shutdown_timeout(Duration::from_secs(5))
}

async fn exercise_plugin(spec: PluginSpec) {
    let process = PluginProcess::start(spec)
        .await
        .expect("start DeepSeek Harness plugin");
    assert_eq!(process.manifest().plugin.id, "fleetd.harness.deepseek");
    assert_eq!(
        process.manifest().interfaces,
        vec![harness_acp_interface()],
        "DSH omits session/load and must not advertise transcript retrieval"
    );
    let mut harness = process.into_harness_acp().expect("typed harness");
    let description = harness.describe().await.expect("describe");
    assert_eq!(description.runtime.name, "deepseek-harness-acp");
    assert_eq!(description.runtime.version, "0.0.1");
    assert_eq!(
        description.agent_capabilities["sessionCapabilities"]["resume"],
        json!({})
    );
    assert!(description.profile_digest.starts_with("sha256:"));

    let session = harness
        .open_session(&OpenSession {
            binding: Binding {
                binding_id: "binding-1".to_owned(),
                binding_generation: 1,
                owner_epoch: 1,
            },
            mode: OpenSessionMode::Create,
            working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            resolved_mcp_grants: Vec::new(),
            profile_digest: description.profile_digest,
        })
        .await
        .expect("open DeepSeek session");
    harness
        .start_turn(&StartTurn {
            fence: ExecutionFence {
                binding_id: "binding-1".to_owned(),
                binding_generation: 1,
                owner_epoch: 1,
                invocation_id: "invocation-1".to_owned(),
                fence_token: "fence-1".to_owned(),
            },
            session_ref: session.session_ref,
            source: TurnSource {
                agent_id: "agent-1".to_owned(),
                message_id: "message-1".to_owned(),
                channel_id: "channel-1".to_owned(),
                sender_id: "sender-1".to_owned(),
                correlation_id: None,
                causation_id: None,
            },
            prompt: vec![PromptBlock::Text {
                text: "test prompt".to_owned(),
            }],
            policy: TurnPolicy {
                idle_timeout_ms: 2_000,
                wall_timeout_ms: 5_000,
                cancel_drain_timeout_ms: 500,
                max_captured_output_bytes: 4_096,
                permission_policy: "controller".to_owned(),
                tool_budget: ToolBudget {
                    limit: 8,
                    required_enforcement: "observe_then_cancel".to_owned(),
                },
                token_budget: None,
            },
        })
        .await
        .expect("start turn");

    let thought = harness.next_notification().await.expect("thought event");
    assert!(matches!(thought, HarnessAcpNotification::TurnEvent(_)));
    let message = harness.next_notification().await.expect("message event");
    assert!(matches!(message, HarnessAcpNotification::TurnEvent(_)));
    let terminal = harness.next_notification().await.expect("terminal");
    let HarnessAcpNotification::TurnTerminal(terminal) = terminal else {
        panic!("expected terminal");
    };
    assert_eq!(
        terminal.assistant_messages[0].content[0]["text"],
        "DeepSeek answer"
    );
    harness
        .shutdown()
        .await
        .expect("shutdown DeepSeek Harness plugin");
}

#[tokio::test]
async fn deepseek_local_inference_is_a_distinct_typed_harness_plugin() {
    let dsh_home = tempfile::TempDir::new().expect("temporary DSH home");
    exercise_plugin(local_plugin_spec(dsh_home.path())).await;
}

#[tokio::test]
async fn deepseek_provider_route_uses_harness_owned_state() {
    let dsh_home = tempfile::TempDir::new().expect("temporary DSH home");
    let settings = "llm-pi-ai:\n  providers:\n    zai:\n      apiKeyEnv: ZAI_API_KEY\n";
    let credentials = "managed-by: dsh\n";
    std::fs::write(dsh_home.path().join("settings.yaml"), settings).expect("write DSH settings");
    std::fs::write(dsh_home.path().join(".credentials.yaml"), credentials)
        .expect("write DSH credentials");

    exercise_plugin(provider_plugin_spec(dsh_home.path())).await;

    assert_eq!(
        std::fs::read_to_string(dsh_home.path().join("settings.yaml"))
            .expect("preserved DSH settings"),
        settings
    );
    assert_eq!(
        std::fs::read_to_string(dsh_home.path().join(".credentials.yaml"))
            .expect("preserved DSH credentials"),
        credentials
    );
}
