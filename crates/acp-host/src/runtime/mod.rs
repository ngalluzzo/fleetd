//! The ACP driver runtime: what a driver is, and the command surface a
//! plugin drives it through.
//!
//! Behaviour lives in the child modules beside this one, one per concept.
//! The types and shared state stay here because a child may reach an
//! ancestor's private items, so the split costs the call sites nothing.

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agent_client_protocol::{
    Agent, ConnectionTo, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
};
use fleetd_proto::harness_acp::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, CloseSession, CloseSessionResult,
    DescribeResult, ExecutionFence, OpenSession, OpenSessionResult, PermissionOutcome,
    PermissionResolution, StartTranscript, StartTranscriptResult, StartTurn, StartTurnResult,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot, watch};

mod initialize;
mod launch;
mod permission;
mod session;
mod transcript;
mod turn;
mod update;

use initialize::{AdoptionMethods, initialize_runtime};
use launch::run_acp;
use permission::{
    cancel_all_permissions, cancel_permissions_for_fence, handle_permission_request,
    resolve_permission,
};
use session::{close_session, open_session};
use transcript::{capture_transcript_entry, forward_transcript_entry, start_transcript};
use turn::{cancel_turn, start_turn};
use update::{bound_json, classify_update, handle_session_update};

const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DriverConfig {
    pub profile_digest: String,
    pub runtime: RuntimeConfig,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    pub expected_name: String,
    pub expected_version: String,
    pub executable: PathBuf,
    pub identity_path: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
}

#[derive(Debug, Error)]
pub enum DriverError {
    #[error("invalid driver configuration: {0}")]
    InvalidConfig(String),
    #[error("driver protocol error: {0}")]
    Protocol(String),
    #[error("inner ACP runtime error: {0}")]
    Runtime(String),
    #[error("driver JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("driver I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl DriverError {
    #[must_use]
    pub const fn code(&self) -> i64 {
        match self {
            Self::InvalidConfig(_) | Self::Protocol(_) | Self::Json(_) => -32602,
            Self::Runtime(_) | Self::Io(_) => -32000,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DriverNotification {
    pub method: String,
    pub params: Value,
}

pub struct DriverRuntime {
    profile_digest: String,
    description: DescribeResult,
    commands: mpsc::Sender<Command>,
    shared: Arc<Mutex<SharedState>>,
    task: tokio::task::JoinHandle<Result<(), DriverError>>,
}

impl DriverRuntime {
    pub async fn start(
        config: DriverConfig,
        allowed_environment: &[&str],
    ) -> Result<(Self, mpsc::Receiver<DriverNotification>), DriverError> {
        config.validate(allowed_environment)?;
        let executable_digest = crate::config::executable_digest(&config.runtime.identity_path)?;
        let profile_digest = config.profile_digest.clone();
        let (commands, command_rx) = mpsc::channel(32);
        let (notifications_tx, notifications) = mpsc::channel(256);
        let (ready_tx, ready_rx) = oneshot::channel();
        let shared = Arc::new(Mutex::new(SharedState::default()));
        let task_shared = Arc::clone(&shared);
        let runtime_config = config.runtime.clone();
        let task_profile_digest = profile_digest.clone();
        let task = tokio::spawn(async move {
            run_acp(
                runtime_config,
                executable_digest,
                task_profile_digest,
                task_shared,
                command_rx,
                notifications_tx,
                ready_tx,
            )
            .await
        });
        let description = ready_rx.await.map_err(|_| {
            DriverError::Runtime("ACP runtime exited during initialize".to_owned())
        })??;
        Ok((
            Self {
                profile_digest,
                description,
                commands,
                shared,
                task,
            },
            notifications,
        ))
    }

    pub async fn handle(&mut self, method: &str, params: Value) -> Result<Value, DriverError> {
        if self.task.is_finished() {
            return Err(DriverError::Runtime(
                "inner ACP runtime is no longer running".to_owned(),
            ));
        }
        match method {
            "fleetd.health" => Ok(json!({"status": "ok"})),
            "harness.acp.describe" => Ok(serde_json::to_value(&self.description)?),
            "harness.acp.session.open" => {
                let request: OpenSession = serde_json::from_value(params)?;
                if request.profile_digest != self.profile_digest {
                    return Err(DriverError::Protocol(
                        "session profile digest does not match active profile".to_owned(),
                    ));
                }
                let result = self
                    .command(|reply| Command::Open { request, reply })
                    .await?;
                Ok(serde_json::to_value(result)?)
            }
            "harness.acp.session.transcript.start" => {
                let request: StartTranscript = serde_json::from_value(params)?;
                let result = self
                    .command(|reply| Command::Transcript { request, reply })
                    .await?;
                Ok(serde_json::to_value(result)?)
            }
            "harness.acp.turn.start" => {
                let request: StartTurn = serde_json::from_value(params)?;
                let result = self
                    .command(|reply| Command::Start {
                        request: Box::new(request),
                        reply,
                    })
                    .await?;
                Ok(serde_json::to_value(result)?)
            }
            "harness.acp.turn.cancel" => {
                let request: CancelTurn = serde_json::from_value(params)?;
                let result = self
                    .command(|reply| Command::Cancel { request, reply })
                    .await?;
                Ok(serde_json::to_value(result)?)
            }
            "harness.acp.permission.resolve" => {
                let request: PermissionResolution = serde_json::from_value(params)?;
                resolve_permission(&self.shared, request).await?;
                Ok(json!({"accepted": true}))
            }
            "harness.acp.session.close" => {
                let request: CloseSession = serde_json::from_value(params)?;
                let result = self
                    .command(|reply| Command::Close { request, reply })
                    .await?;
                Ok(serde_json::to_value(result)?)
            }
            _ => Err(DriverError::Protocol(format!(
                "unsupported driver method: {method}"
            ))),
        }
    }

    /// Operational generations this initialized runtime can actually serve.
    ///
    /// ACP reserves transcript replay for `session/load`. A resumable runtime
    /// that intentionally omits load can still provide the complete managed
    /// turn interface, but it must not promise Fleetd's transcript generation.
    pub(crate) fn declared_interfaces(&self) -> Vec<fleetd_proto::plugin::PluginInterface> {
        let supports_transcript_replay = self
            .description
            .agent_capabilities
            .get("loadSession")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        fleetd_proto::harness_acp::declared_interfaces(supports_transcript_replay)
    }

    pub async fn stop(&mut self) {
        let (reply_tx, reply_rx) = oneshot::channel();
        let _unused = self.commands.send(Command::Stop { reply: reply_tx }).await;
        let _unused = tokio::time::timeout(Duration::from_secs(2), reply_rx).await;
        if !self.task.is_finished() {
            self.task.abort();
        }
    }

    async fn command<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, DriverError>>) -> Command,
    ) -> Result<T, DriverError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(build(reply_tx))
            .await
            .map_err(|_| DriverError::Runtime("ACP command channel closed".to_owned()))?;
        reply_rx
            .await
            .map_err(|_| DriverError::Runtime("ACP command was abandoned".to_owned()))?
    }
}

impl DriverConfig {
    fn validate(&self, allowed_environment: &[&str]) -> Result<(), DriverError> {
        if self.profile_digest.trim().is_empty() {
            return Err(DriverError::InvalidConfig(
                "profile_digest must not be empty".to_owned(),
            ));
        }
        validate_file("runtime executable", &self.runtime.executable)?;
        validate_file("runtime identity path", &self.runtime.identity_path)?;
        if self.runtime.expected_name.trim().is_empty()
            || self.runtime.expected_version.trim().is_empty()
        {
            return Err(DriverError::InvalidConfig(
                "expected runtime identity must not be empty".to_owned(),
            ));
        }
        for name in self.runtime.environment.keys() {
            if !allowed_environment.contains(&name.as_str()) {
                return Err(DriverError::InvalidConfig(format!(
                    "environment variable is not an approved non-secret setting: {name}"
                )));
            }
        }
        Ok(())
    }
}

fn validate_file(kind: &str, path: &Path) -> Result<(), DriverError> {
    if !path.is_absolute() || !path.is_file() {
        return Err(DriverError::InvalidConfig(format!(
            "{kind} must be an existing absolute file: {}",
            path.display()
        )));
    }
    Ok(())
}

enum Command {
    Transcript {
        request: StartTranscript,
        reply: oneshot::Sender<Result<StartTranscriptResult, DriverError>>,
    },
    Open {
        request: OpenSession,
        reply: oneshot::Sender<Result<OpenSessionResult, DriverError>>,
    },
    Start {
        request: Box<StartTurn>,
        reply: oneshot::Sender<Result<StartTurnResult, DriverError>>,
    },
    Cancel {
        request: CancelTurn,
        reply: oneshot::Sender<Result<AcceptedResult, DriverError>>,
    },
    Close {
        request: CloseSession,
        reply: oneshot::Sender<Result<CloseSessionResult, DriverError>>,
    },
    Stop {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Default)]
struct SharedState {
    sessions: HashMap<String, SessionState>,
    permissions: HashMap<String, PendingPermission>,
}

struct SessionState {
    binding: Binding,
    cwd: String,
    additional_directories: Vec<String>,
    mcp_grants: Vec<String>,
    active: Option<ActiveTurn>,
    /// Set only while a transcript replay is in flight. A replayed entry has no
    /// invocation, so it must never be folded into one; this is what tells
    /// `handle_session_update` to forward it as an entry instead of ignoring it.
    capturing: Option<TranscriptCapture>,
}

/// Bounds and running totals for one transcript replay.
struct TranscriptCapture {
    next_entry_seq: u64,
    entry_count: u64,
    observed_payload_bytes: u64,
    truncated: bool,
}

struct ActiveTurn {
    fence: ExecutionFence,
    next_event_seq: u64,
    policy: fleetd_proto::harness_acp::TurnPolicy,
    captured_bytes: usize,
    assistant_messages: Vec<AssistantMessage>,
    tool_calls: u64,
    usage: Value,
    activity: watch::Sender<u64>,
    cancellation: watch::Sender<Option<String>>,
}

struct PendingPermission {
    fence: ExecutionFence,
    response: oneshot::Sender<PermissionOutcome>,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonRpcNotification)]
#[notification(method = "session/update")]
#[serde(transparent)]
struct RawSessionNotification(Value);

#[derive(Clone, Debug, Deserialize, Serialize, JsonRpcRequest)]
#[request(method = "session/request_permission", response = RawResponse)]
#[serde(transparent)]
struct RawPermissionRequest(Value);

#[derive(Clone, Debug, Deserialize, Serialize, JsonRpcResponse)]
#[serde(transparent)]
struct RawResponse(Value);

#[derive(Clone, Debug, Deserialize, Serialize, JsonRpcRequest)]
#[request(method = "initialize", response = RawResponse)]
#[serde(transparent)]
struct RawInitializeRequest(Value);

#[derive(Clone, Debug, Deserialize, Serialize, JsonRpcRequest)]
#[request(method = "session/new", response = RawResponse)]
#[serde(transparent)]
struct RawNewSessionRequest(Value);

#[derive(Clone, Debug, Deserialize, Serialize, JsonRpcRequest)]
#[request(method = "session/load", response = RawResponse)]
#[serde(transparent)]
struct RawLoadSessionRequest(Value);

#[derive(Clone, Debug, Deserialize, Serialize, JsonRpcRequest)]
#[request(method = "session/resume", response = RawResponse)]
#[serde(transparent)]
struct RawResumeSessionRequest(Value);

#[derive(Clone, Debug, Deserialize, Serialize, JsonRpcRequest)]
#[request(method = "session/prompt", response = RawResponse)]
#[serde(transparent)]
struct RawPromptRequest(Value);

async fn serve_commands(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    commands: &mut mpsc::Receiver<Command>,
    adoption: AdoptionMethods,
) -> Result<(), agent_client_protocol::Error> {
    while let Some(command) = commands.recv().await {
        match command {
            Command::Open { request, reply } => {
                let result = open_session(connection, shared, request, adoption).await;
                let _unused = reply.send(result);
            }
            Command::Transcript { request, reply } => {
                let result =
                    start_transcript(connection, shared, notifications, request, adoption).await;
                let _unused = reply.send(result);
            }
            Command::Start { request, reply } => {
                let result = start_turn(connection, shared, notifications, *request).await;
                let _unused = reply.send(result);
            }
            Command::Cancel { request, reply } => {
                let result = cancel_turn(connection, shared, request).await;
                let _unused = reply.send(result);
            }
            Command::Close { request, reply } => {
                let result = close_session(shared, request).await;
                let _unused = reply.send(result);
            }
            Command::Stop { reply } => {
                cancel_all_permissions(shared).await;
                let _unused = reply.send(());
                return Ok(());
            }
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    u64::try_from(now_ms_i64()).unwrap_or_default()
}

fn now_ms_i64() -> i64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn acp_error(error: impl std::fmt::Display) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(error.to_string())
}
