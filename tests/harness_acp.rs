#![cfg(unix)]

use std::{path::PathBuf, time::Duration};

use fleetd::plugin::{
    Binding, CloseSession, ExecutionFence, HarnessAcpNotification, OpenSession, OpenSessionMode,
    PluginError, PluginProcess, PluginSpec, PromptBlock, StartTranscript, StartTurn, ToolBudget,
    TurnPolicy, TurnSource, harness_acp_interface,
};

fn fixture_spec(mode: &str) -> PluginSpec {
    PluginSpec::new("mock.harness", "/usr/bin/python3")
        .with_arg(fixture_path())
        .with_arg(mode)
        .require_interface(harness_acp_interface())
        .with_request_timeout(Duration::from_secs(1))
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_harness_plugin.py")
}

fn binding() -> Binding {
    Binding {
        binding_id: "binding-1".to_owned(),
        binding_generation: 1,
        owner_epoch: 1,
    }
}

fn fence() -> ExecutionFence {
    ExecutionFence {
        binding_id: "binding-1".to_owned(),
        binding_generation: 1,
        owner_epoch: 1,
        invocation_id: "invocation-1".to_owned(),
        fence_token: "fence-1".to_owned(),
    }
}

fn turn() -> StartTurn {
    StartTurn {
        fence: fence(),
        session_ref: "mock-session".to_owned(),
        source: TurnSource {
            agent_id: "agent-1".to_owned(),
            message_id: "message-1".to_owned(),
            channel_id: "channel-1".to_owned(),
            sender_id: "sender-1".to_owned(),
            correlation_id: Some("correlation-1".to_owned()),
            causation_id: None,
        },
        prompt: vec![PromptBlock::Text {
            text: "do the bounded work".to_owned(),
        }],
        policy: TurnPolicy {
            idle_timeout_ms: 1_000,
            wall_timeout_ms: 5_000,
            cancel_drain_timeout_ms: 500,
            max_captured_output_bytes: 1_024,
            permission_policy: "controller".to_owned(),
            tool_budget: ToolBudget {
                limit: 8,
                required_enforcement: "observe_then_cancel".to_owned(),
            },
            token_budget: None,
        },
    }
}

#[tokio::test]
async fn typed_harness_client_runs_one_fenced_turn() {
    let process = PluginProcess::start(fixture_spec("healthy"))
        .await
        .expect("start mock harness");
    let mut harness = process.into_harness_acp().expect("typed harness client");

    let description = harness.describe().await.expect("describe harness");
    assert_eq!(description.runtime.name, "mock-acp");
    assert_eq!(description.raw_initialize_result["extension"], "preserved");

    let session = harness
        .open_session(&OpenSession {
            binding: binding(),
            mode: OpenSessionMode::Create,
            working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            resolved_mcp_grants: Vec::new(),
            profile_digest: "sha256:profile".to_owned(),
        })
        .await
        .expect("open session");
    assert_eq!(session.session_ref, "mock-session");

    let accepted = harness.start_turn(&turn()).await.expect("start turn");
    assert!(accepted.accepted);

    let event = harness.next_notification().await.expect("turn event");
    let HarnessAcpNotification::TurnEvent(event) = event else {
        panic!("expected turn event");
    };
    assert_eq!(event.event_seq, 1);
    assert_eq!(event.raw_update["unknownExtension"]["preserved"], true);

    let terminal = harness.next_notification().await.expect("terminal event");
    let HarnessAcpNotification::TurnTerminal(terminal) = terminal else {
        panic!("expected terminal event");
    };
    assert_eq!(terminal.stop_reason, "end_turn");
    assert_eq!(terminal.assistant_messages[0].content[0]["text"], "done");

    let closed = harness
        .close_session(&CloseSession {
            binding_id: "binding-1".to_owned(),
            binding_generation: 1,
            owner_epoch: 1,
            session_ref: session.session_ref,
            reason: "test".to_owned(),
        })
        .await
        .expect("close session");
    assert!(closed.ownership_retired);
    harness.shutdown().await.expect("shutdown harness");
}

#[tokio::test]
async fn typed_harness_client_rejects_stale_event_fences() {
    let process = PluginProcess::start(fixture_spec("wrong-fence"))
        .await
        .expect("start mock harness");
    let mut harness = process.into_harness_acp().expect("typed harness client");
    harness.start_turn(&turn()).await.expect("start turn");
    let error = harness
        .next_notification()
        .await
        .expect_err("stale fence must fail");
    assert!(matches!(error, PluginError::Protocol(_)));
}

#[tokio::test]
async fn notification_overflow_fails_calls_instead_of_deadlocking() {
    let result = PluginProcess::start(
        fixture_spec("overflow").with_initialize_timeout(Duration::from_secs(1)),
    )
    .await;
    let error = match result {
        Ok(plugin) => {
            let _shutdown = plugin.shutdown().await;
            panic!("notification overflow must fail startup")
        }
        Err(error) => error,
    };
    assert!(matches!(error, PluginError::Protocol(_)));
}

#[tokio::test]
async fn typed_harness_client_validates_effect_boundaries_locally() {
    let process = PluginProcess::start(fixture_spec("healthy"))
        .await
        .expect("start mock harness");
    let mut harness = process.into_harness_acp().expect("typed harness client");
    let mut invalid = turn();
    invalid.session_ref = String::new();
    let error = harness
        .start_turn(&invalid)
        .await
        .expect_err("empty session ref must fail");
    assert!(matches!(error, PluginError::Protocol(_)));
    harness.shutdown().await.expect("shutdown harness");
}

#[tokio::test]
async fn typed_harness_client_rejects_weaker_effective_enforcement() {
    let process = PluginProcess::start(fixture_spec("weak-enforcement"))
        .await
        .expect("start mock harness");
    let mut harness = process.into_harness_acp().expect("typed harness client");
    let error = harness
        .start_turn(&turn())
        .await
        .expect_err("weaker enforcement must fail closed");
    assert!(matches!(error, PluginError::Protocol(_)));
    harness.shutdown().await.expect("shutdown harness");
}

/// A replay reaches the caller as ordered entries closed by exactly one
/// completion, and carries no fence because it starts no work.
#[tokio::test]
async fn a_transcript_replay_streams_entries_and_one_completion() {
    let process = PluginProcess::start(fixture_spec("healthy"))
        .await
        .expect("start mock harness");
    let mut harness = process.into_harness_acp().expect("typed harness client");
    harness
        .open_session(&OpenSession {
            binding: binding(),
            mode: OpenSessionMode::Create,
            working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            resolved_mcp_grants: Vec::new(),
            profile_digest: "mock-profile".to_owned(),
        })
        .await
        .expect("open a session");

    let accepted = harness
        .start_transcript(&StartTranscript {
            binding_id: binding().binding_id,
            binding_generation: binding().binding_generation,
            owner_epoch: binding().owner_epoch,
            session_ref: "mock-session".to_owned(),
        })
        .await
        .expect("start a replay");
    assert!(accepted.accepted);

    let mut classifications = Vec::new();
    let complete = loop {
        match harness
            .next_notification()
            .await
            .expect("a transcript notification")
        {
            HarnessAcpNotification::TranscriptEntry(entry) => {
                assert_eq!(
                    entry.entry_seq,
                    u64::try_from(classifications.len()).expect("fits") + 1,
                    "entries must arrive in order"
                );
                classifications.push(entry.classification);
            }
            HarnessAcpNotification::TranscriptComplete(complete) => break complete,
            other => panic!("unexpected notification during a replay: {other:?}"),
        }
    };

    assert_eq!(
        classifications,
        vec![
            "reasoning_content".to_owned(),
            "tool_call".to_owned(),
            "agent_message_content".to_owned(),
        ],
        "a replay carries reasoning and tool calls, not only messages"
    );
    assert_eq!(complete.entry_count, 3);
    assert!(!complete.truncated);
    assert!(complete.failure.is_none());
}

/// A runtime that cannot replay must say so, rather than reporting an empty
/// transcript that reads like an agent which did nothing.
#[tokio::test]
async fn a_runtime_that_cannot_replay_reports_a_failure() {
    let process = PluginProcess::start(fixture_spec("no-transcript"))
        .await
        .expect("start mock harness");
    let harness = process.into_harness_acp().expect("typed harness client");
    let error = harness
        .start_transcript(&StartTranscript {
            binding_id: binding().binding_id,
            binding_generation: binding().binding_generation,
            owner_epoch: binding().owner_epoch,
            session_ref: "mock-session".to_owned(),
        })
        .await
        .expect_err("a refusal reaches the caller");
    assert!(
        format!("{error}").contains("cannot replay"),
        "unexpected error: {error}"
    );
}
