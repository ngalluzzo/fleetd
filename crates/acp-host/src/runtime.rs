use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agent_client_protocol::{
    AcpAgent, AcpAgentConfig, Agent, ConnectionTo, JsonRpcNotification, JsonRpcRequest,
    JsonRpcResponse,
    schema::{
        ProtocolVersion,
        v1::{
            AgentCapabilities, CancelNotification, Implementation, InitializeRequest,
            InitializeResponse,
        },
    },
};
use fleetd_proto::harness_acp::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, CloseSession, CloseSessionResult,
    DescribeResult, DriverIdentity, EffectiveEnforcement, ExecutionFence,
    HarnessExecutionCertainty, HarnessLimits, OpenSession, OpenSessionMode, OpenSessionResult,
    PermissionOutcome, PermissionResolution, ResolvedMcpEndpoint, RuntimeIdentity,
    SessionPersistence, StartTranscript, StartTranscriptResult, StartTurn, StartTurnResult,
    TranscriptComplete, TranscriptEntry, TurnEvent, TurnTerminal,
};
use http::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use url::Url;
use uuid::Uuid;

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_CAPTURE_BYTES: usize = 512 * 1024;
/// The most entries one replay forwards before it reports truncation.
const MAX_TRANSCRIPT_ENTRIES: u64 = 10_000;

/// The most encoded bytes one replay forwards before it reports truncation.
const MAX_TRANSCRIPT_BYTES: u64 = 8 * 1024 * 1024;

const ACP_SDK_VERSION: &str = "2.0.0";

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

async fn run_acp(
    runtime: RuntimeConfig,
    executable_digest: String,
    profile_digest: String,
    shared: Arc<Mutex<SharedState>>,
    mut commands: mpsc::Receiver<Command>,
    notifications: mpsc::Sender<DriverNotification>,
    ready: oneshot::Sender<Result<DescribeResult, DriverError>>,
) -> Result<(), DriverError> {
    let agent_config = build_agent_config(&runtime)?;
    let agent = AcpAgent::new(agent_config);
    let update_shared = Arc::clone(&shared);
    let update_notifications = notifications.clone();
    let permission_shared = Arc::clone(&shared);
    let permission_notifications = notifications.clone();

    let connection = agent_client_protocol::Client
        .builder()
        .name("fleetd-acp-host")
        .on_receive_notification(
            async move |notification: RawSessionNotification, _connection| {
                handle_session_update(&update_shared, &update_notifications, notification.0)
                    .await
                    .map_err(acp_error)
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RawPermissionRequest, responder, _connection| {
                let response = handle_permission_request(
                    &permission_shared,
                    &permission_notifications,
                    request.0,
                )
                .await
                .unwrap_or_else(|_| json!({"outcome": {"outcome": "cancelled"}}));
                responder.respond(RawResponse(response))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(agent, |connection: ConnectionTo<Agent>| async move {
            let initialized =
                initialize_runtime(&connection, &runtime, executable_digest, profile_digest).await;
            let (description, adoption) = match initialized {
                Ok(initialized) => initialized,
                Err(error) => {
                    let _unused = ready.send(Err(error));
                    return Ok(());
                }
            };
            let _unused = ready.send(Ok(description));
            serve_commands(
                &connection,
                &shared,
                &notifications,
                &mut commands,
                adoption,
            )
            .await
        })
        .await;

    connection.map_err(|error| DriverError::Runtime(error.to_string()))
}

fn build_agent_config(runtime: &RuntimeConfig) -> Result<AcpAgentConfig, DriverError> {
    let launcher = std::env::current_exe()?;
    let mut launcher_args = vec![
        "--inner-launch".to_owned(),
        parent_process_group()?,
        runtime.executable.to_string_lossy().into_owned(),
    ];
    launcher_args.extend(runtime.args.clone());
    Ok(AcpAgentConfig::new(launcher)
        .args(launcher_args)
        .envs(runtime.environment.clone()))
}

#[cfg(unix)]
fn parent_process_group() -> Result<String, DriverError> {
    let process_group = nix::unistd::getpgrp().as_raw();
    if process_group <= 0 {
        return Err(DriverError::Runtime(
            "driver does not have a valid parent process group".to_owned(),
        ));
    }
    Ok(process_group.to_string())
}

#[cfg(not(unix))]
fn parent_process_group() -> Result<String, DriverError> {
    Err(DriverError::InvalidConfig(
        "the ACP driver requires Unix process-group ownership".to_owned(),
    ))
}

/// Which session-adoption methods the inner runtime advertised.
///
/// ACP requires `session/load` to replay the entire conversation as
/// `session/update` notifications before it answers, and requires
/// `session/resume` not to. Adoption wants the session back rather than its
/// transcript, so it prefers `resume`; `load` remains the fallback for a
/// runtime that predates it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdoptionMethods {
    load: bool,
    resume: bool,
}

impl AdoptionMethods {
    fn from_capabilities(capabilities: &AgentCapabilities) -> Self {
        Self {
            load: capabilities.load_session,
            resume: capabilities.session_capabilities.resume.is_some(),
        }
    }

    /// Which ACP method adopts an existing session, or `None` when the runtime
    /// advertises neither.
    ///
    /// `resume` wins wherever it exists: both restore the session, and only
    /// `load` is obliged to replay the entire conversation first.
    const fn method(self) -> Option<&'static str> {
        if self.resume {
            Some("session/resume")
        } else if self.load {
            Some("session/load")
        } else {
            None
        }
    }
}

async fn initialize_runtime(
    connection: &ConnectionTo<Agent>,
    runtime: &RuntimeConfig,
    executable_digest: String,
    profile_digest: String,
) -> Result<(DescribeResult, AdoptionMethods), DriverError> {
    let request = InitializeRequest::new(ProtocolVersion::V1).client_info(Implementation::new(
        "fleetd-acp-host",
        env!("CARGO_PKG_VERSION"),
    ));
    let raw_initialize = connection
        .send_request(RawInitializeRequest(serde_json::to_value(request)?))
        .block_task()
        .await
        .map_err(|error| DriverError::Runtime(error.to_string()))?;
    let parsed: InitializeResponse = serde_json::from_value(raw_initialize.0.clone())?;
    let agent_info = parsed.agent_info.clone().ok_or_else(|| {
        DriverError::Runtime("inner ACP runtime did not report agentInfo".to_owned())
    })?;
    if agent_info.name != runtime.expected_name || agent_info.version != runtime.expected_version {
        return Err(DriverError::Runtime(format!(
            "runtime identity mismatch: expected {} {}, received {} {}",
            runtime.expected_name, runtime.expected_version, agent_info.name, agent_info.version
        )));
    }
    let adoption = AdoptionMethods::from_capabilities(&parsed.agent_capabilities);
    let description = DescribeResult {
        driver: DriverIdentity {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            acp_sdk_version: ACP_SDK_VERSION.to_owned(),
            acp_protocol_version: 1,
        },
        runtime: RuntimeIdentity {
            name: agent_info.name,
            version: agent_info.version,
            executable_digest,
        },
        agent_capabilities: serde_json::to_value(&parsed.agent_capabilities)?,
        limits: HarnessLimits {
            max_concurrent_turns: 1,
            max_frame_bytes: MAX_FRAME_BYTES,
        },
        profile_digest,
        raw_initialize_result: bound_json(raw_initialize.0, MAX_FRAME_BYTES / 2),
    };
    Ok((description, adoption))
}

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

/// Starts one transcript replay and answers immediately.
///
/// `session/load` is the only ACP method obliged to replay a conversation, so
/// retrieval uses it even though adoption no longer does. The request returns as
/// soon as the replay is under way: entries arrive as notifications and a
/// terminal notification closes it, because a plugin drains notifications only
/// between requests and awaiting the whole replay here would deadlock once it
/// outgrew the channel.
async fn start_transcript(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    request: StartTranscript,
    adoption: AdoptionMethods,
) -> Result<StartTranscriptResult, DriverError> {
    if !adoption.load {
        return Err(DriverError::Protocol(
            "inner ACP runtime does not support session/load, so it cannot replay a transcript"
                .to_owned(),
        ));
    }
    let (cwd, directories) = {
        let mut state = shared.lock().await;
        let session = state
            .sessions
            .get_mut(&request.session_ref)
            .ok_or_else(|| {
                DriverError::Protocol("transcript requested for an unopened session".to_owned())
            })?;
        // Retrieval must not be able to read a lane the caller does not own.
        if session.binding.binding_id != request.binding_id
            || session.binding.binding_generation != request.binding_generation
            || session.binding.owner_epoch != request.owner_epoch
        {
            return Err(DriverError::Protocol(
                "transcript requested under a binding that does not own this session".to_owned(),
            ));
        }
        // A replay while a turn is draining would interleave entries with that
        // turn's events, and both would be wrong.
        if session.active.is_some() {
            return Err(DriverError::Protocol(
                "transcript cannot be replayed while a turn is active on this session".to_owned(),
            ));
        }
        if session.capturing.is_some() {
            return Err(DriverError::Protocol(
                "a transcript replay is already in flight for this session".to_owned(),
            ));
        }
        session.capturing = Some(TranscriptCapture {
            next_entry_seq: 1,
            entry_count: 0,
            observed_payload_bytes: 0,
            truncated: false,
        });
        (session.cwd.clone(), session.additional_directories.clone())
    };

    let sent = connection.send_request(RawLoadSessionRequest(json!({
        "sessionId": request.session_ref,
        "cwd": cwd,
        "additionalDirectories": directories,
        "mcpServers": []
    })));
    let capture_shared = Arc::clone(shared);
    let capture_notifications = notifications.clone();
    let session_ref = request.session_ref.clone();
    tokio::spawn(async move {
        let failure = sent.block_task().await.err().map(|error| error.to_string());
        let (entry_count, observed_payload_bytes, truncated) = {
            let mut state = capture_shared.lock().await;
            state
                .sessions
                .get_mut(&session_ref)
                .and_then(|session| session.capturing.take())
                .map_or((0, 0, false), |capture| {
                    (
                        capture.entry_count,
                        capture.observed_payload_bytes,
                        capture.truncated,
                    )
                })
        };
        let complete = TranscriptComplete {
            session_ref,
            entry_count,
            observed_payload_bytes,
            truncated,
            failure: failure.map(|reason| bounded_reason(&reason)),
        };
        if let Ok(params) = serde_json::to_value(&complete) {
            let _closed = capture_notifications
                .send(DriverNotification {
                    method: "harness.acp.session.transcript.complete".to_owned(),
                    params,
                })
                .await;
        }
    });

    Ok(StartTranscriptResult { accepted: true })
}

/// Bounds a runtime diagnostic before it is forwarded as evidence.
fn bounded_reason(reason: &str) -> String {
    const LIMIT: usize = 1_024;
    if reason.len() <= LIMIT {
        return reason.to_owned();
    }
    let mut end = LIMIT;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_owned()
}

async fn open_session(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    request: OpenSession,
    adoption: AdoptionMethods,
) -> Result<OpenSessionResult, DriverError> {
    let mcp_servers = resolve_mcp_servers(&request)?;
    let directories = request
        .additional_directories
        .iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let (session_ref, resumed, raw_result) = match &request.mode {
        OpenSessionMode::Create => {
            let raw_request = json!({
                "cwd": request.working_directory,
                "additionalDirectories": directories,
                "mcpServers": mcp_servers
            });
            let response = connection
                .send_request(RawNewSessionRequest(raw_request))
                .block_task()
                .await
                .map_err(|error| DriverError::Runtime(error.to_string()))?;
            let session_ref = response
                .0
                .get("sessionId")
                .and_then(Value::as_str)
                .ok_or_else(|| DriverError::Protocol("session/new omitted sessionId".to_owned()))?
                .to_owned();
            (session_ref, false, response.0)
        }
        OpenSessionMode::Resume { session_ref } => {
            let Some(method) = adoption.method() else {
                return Err(DriverError::Protocol(
                    "inner ACP runtime supports neither session/resume nor session/load".to_owned(),
                ));
            };
            let raw_request = json!({
                "sessionId": session_ref,
                "cwd": request.working_directory,
                "additionalDirectories": directories,
                "mcpServers": mcp_servers
            });
            let response = if method == "session/resume" {
                connection
                    .send_request(RawResumeSessionRequest(raw_request))
                    .block_task()
                    .await
            } else {
                connection
                    .send_request(RawLoadSessionRequest(raw_request))
                    .block_task()
                    .await
            }
            .map_err(|error| DriverError::Runtime(error.to_string()))?;
            (session_ref.clone(), true, response.0)
        }
    };
    let mut state = shared.lock().await;
    if let Some(existing) = state.sessions.get(&session_ref)
        && (existing.binding != request.binding
            || existing.cwd != request.working_directory
            || existing.additional_directories != request.additional_directories
            || existing.mcp_grants != request.mcp_grants)
    {
        return Err(DriverError::Protocol(
            "session reference already belongs to incompatible binding state".to_owned(),
        ));
    }
    let effective_working_directory = request.working_directory.clone();
    let effective_additional_directories = request.additional_directories.clone();
    let effective_mcp_grants = request.mcp_grants.clone();
    state.sessions.insert(
        session_ref.clone(),
        SessionState {
            binding: request.binding,
            cwd: request.working_directory,
            additional_directories: request.additional_directories,
            mcp_grants: request.mcp_grants,
            active: None,
            capturing: None,
        },
    );
    Ok(OpenSessionResult {
        session_ref,
        profile_digest: request.profile_digest,
        resumed,
        effective_config: json!({
            "working_directory": effective_working_directory,
            "additional_directories": effective_additional_directories,
            "mcp_grants": effective_mcp_grants,
        }),
        raw_session_result: bound_json(raw_result, MAX_FRAME_BYTES / 2),
    })
}

fn resolve_mcp_servers(request: &OpenSession) -> Result<Vec<Value>, DriverError> {
    let mut requested = BTreeSet::new();
    for name in &request.mcp_grants {
        if name.trim().is_empty() || name.len() > 256 {
            return Err(DriverError::Protocol(
                "MCP grant names must contain between 1 and 256 bytes".to_owned(),
            ));
        }
        if !requested.insert(name.as_str()) {
            return Err(DriverError::Protocol(format!(
                "duplicate MCP grant name: {name}"
            )));
        }
    }
    let mut resolved = BTreeMap::new();
    for grant in &request.resolved_mcp_grants {
        if !requested.contains(grant.name.as_str()) {
            return Err(DriverError::Protocol(format!(
                "resolved MCP endpoint was not requested: {}",
                grant.name
            )));
        }
        if resolved.insert(grant.name.as_str(), grant).is_some() {
            return Err(DriverError::Protocol(format!(
                "duplicate resolved MCP grant: {}",
                grant.name
            )));
        }
    }
    if requested.len() != resolved.len() {
        return Err(DriverError::Protocol(
            "every requested MCP grant must have one resolved endpoint".to_owned(),
        ));
    }

    request
        .mcp_grants
        .iter()
        .map(|name| {
            let grant = resolved
                .get(name.as_str())
                .expect("resolved and requested MCP grant sets match");
            match &grant.endpoint {
                ResolvedMcpEndpoint::Http { url, headers } => {
                    validate_loopback_mcp_url(url)?;
                    if headers.len() > 16 {
                        return Err(DriverError::Protocol(
                            "resolved MCP HTTP endpoints may have at most 16 headers".to_owned(),
                        ));
                    }
                    let mut header_names = BTreeSet::new();
                    for header in headers {
                        let parsed_name =
                            HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| {
                                DriverError::Protocol("invalid MCP HTTP header name".to_owned())
                            })?;
                        HeaderValue::from_str(&header.value).map_err(|_| {
                            DriverError::Protocol("invalid MCP HTTP header value".to_owned())
                        })?;
                        if header.value.len() > 4_096 {
                            return Err(DriverError::Protocol(
                                "MCP HTTP header values must not exceed 4,096 bytes".to_owned(),
                            ));
                        }
                        if !header_names.insert(parsed_name.as_str().to_owned()) {
                            return Err(DriverError::Protocol(
                                "duplicate MCP HTTP header name".to_owned(),
                            ));
                        }
                    }
                    Ok(json!({
                        "type": "http",
                        "name": name,
                        "url": url,
                        "headers": headers,
                    }))
                }
            }
        })
        .collect()
}

fn validate_loopback_mcp_url(raw: &str) -> Result<(), DriverError> {
    let url = Url::parse(raw)
        .map_err(|_| DriverError::Protocol("resolved MCP URL is invalid".to_owned()))?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DriverError::Protocol(
            "resolved MCP URL must be an explicit 127.0.0.1 HTTP endpoint without credentials, query, or fragment"
                .to_owned(),
        ));
    }
    Ok(())
}

async fn start_turn(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    request: StartTurn,
) -> Result<StartTurnResult, DriverError> {
    if request.policy.max_captured_output_bytes == 0
        || request.policy.max_captured_output_bytes > MAX_CAPTURE_BYTES
    {
        return Err(DriverError::Protocol(format!(
            "max_captured_output_bytes must be between 1 and {MAX_CAPTURE_BYTES}"
        )));
    }
    if request.policy.permission_policy != "controller"
        || request.policy.tool_budget.required_enforcement != "observe_then_cancel"
        || request.policy.token_budget.is_some()
    {
        return Err(DriverError::Protocol(
            "requested turn enforcement is not supported".to_owned(),
        ));
    }
    let (activity_tx, activity_rx) = watch::channel(now_ms());
    let (cancellation_tx, cancellation_rx) = watch::channel(None);
    {
        let mut state = shared.lock().await;
        let session = state
            .sessions
            .get_mut(&request.session_ref)
            .ok_or_else(|| {
                DriverError::Protocol("turn references an unopened session".to_owned())
            })?;
        if session.active.is_some() {
            return Err(DriverError::Protocol(
                "session already has an active turn".to_owned(),
            ));
        }
        if session.binding.binding_id != request.fence.binding_id
            || session.binding.binding_generation != request.fence.binding_generation
            || session.binding.owner_epoch != request.fence.owner_epoch
        {
            return Err(DriverError::Protocol(
                "turn fence does not match session binding".to_owned(),
            ));
        }
        session.active = Some(ActiveTurn {
            fence: request.fence.clone(),
            next_event_seq: 1,
            policy: request.policy.clone(),
            captured_bytes: 0,
            assistant_messages: Vec::new(),
            tool_calls: 0,
            usage: Value::Null,
            activity: activity_tx,
            cancellation: cancellation_tx,
        });
    }
    let prompt = serde_json::to_value(&request.prompt)?;
    let raw_request = json!({
        "sessionId": request.session_ref,
        "prompt": prompt,
        "_meta": {
            "fleetd": {
                "source": request.source,
                "fence": request.fence,
            }
        }
    });
    let sent = connection.send_request(RawPromptRequest(raw_request));
    let task_connection = connection.clone();
    let task_shared = Arc::clone(shared);
    let task_notifications = notifications.clone();
    let task_session_ref = request.session_ref.clone();
    let wall_timeout = Duration::from_millis(request.policy.wall_timeout_ms);
    let idle_timeout = Duration::from_millis(request.policy.idle_timeout_ms);
    let cancel_drain_timeout = Duration::from_millis(request.policy.cancel_drain_timeout_ms);
    tokio::spawn(async move {
        monitor_prompt(
            task_connection,
            task_shared,
            task_notifications,
            task_session_ref,
            sent,
            activity_rx,
            cancellation_rx,
            wall_timeout,
            idle_timeout,
            cancel_drain_timeout,
        )
        .await;
    });

    Ok(StartTurnResult {
        accepted: true,
        effective_enforcement: EffectiveEnforcement {
            wall_timeout: "hard".to_owned(),
            idle_timeout: "hard".to_owned(),
            cancel_drain_timeout: "hard".to_owned(),
            captured_output_bytes: "hard".to_owned(),
            tool_budget: "observe_then_cancel".to_owned(),
            token_budget: "unavailable".to_owned(),
        },
    })
}

#[allow(clippy::too_many_arguments)]
async fn monitor_prompt(
    connection: ConnectionTo<Agent>,
    shared: Arc<Mutex<SharedState>>,
    notifications: mpsc::Sender<DriverNotification>,
    session_ref: String,
    sent: agent_client_protocol::SentRequest<RawResponse>,
    mut activity: watch::Receiver<u64>,
    mut cancellation: watch::Receiver<Option<String>>,
    wall_timeout: Duration,
    idle_timeout: Duration,
    cancel_drain_timeout: Duration,
) {
    let response_task = tokio::spawn(sent.block_task());
    let wall_deadline = tokio::time::Instant::now() + wall_timeout;
    let mut idle_deadline = tokio::time::Instant::now() + idle_timeout;
    tokio::pin!(response_task);
    let outcome = loop {
        tokio::select! {
            response = &mut response_task => {
                break match response {
                    Ok(Ok(response)) => PromptOutcome::Known {
                        response: response.0,
                        host_stop_reason: None,
                    },
                    Ok(Err(error)) => PromptOutcome::Unknown(json!({"error": error.to_string()})),
                    Err(error) => PromptOutcome::Unknown(json!({"join_error": error.to_string()})),
                };
            }
            () = tokio::time::sleep_until(wall_deadline) => {
                break cancel_and_drain(&connection, &session_ref, &mut response_task, cancel_drain_timeout, "wall_deadline").await;
            }
            () = tokio::time::sleep_until(idle_deadline) => {
                break cancel_and_drain(&connection, &session_ref, &mut response_task, cancel_drain_timeout, "idle_deadline").await;
            }
            changed = activity.changed() => {
                if changed.is_err() {
                    break PromptOutcome::Unknown(json!({"error": "activity monitor closed"}));
                }
                idle_deadline = tokio::time::Instant::now() + idle_timeout;
            }
            changed = cancellation.changed() => {
                if changed.is_err() {
                    break PromptOutcome::Unknown(json!({"error": "cancellation monitor closed"}));
                }
                let reason = cancellation.borrow().clone();
                if let Some(reason) = reason {
                    break cancel_and_drain(&connection, &session_ref, &mut response_task, cancel_drain_timeout, &reason).await;
                }
            }
        }
    };
    let _unused = emit_terminal(&shared, &notifications, &session_ref, outcome).await;
}

enum PromptOutcome {
    Known {
        response: Value,
        host_stop_reason: Option<String>,
    },
    Unknown(Value),
}

async fn cancel_and_drain(
    connection: &ConnectionTo<Agent>,
    session_ref: &str,
    response: &mut tokio::task::JoinHandle<Result<RawResponse, agent_client_protocol::Error>>,
    drain_timeout: Duration,
    reason: &str,
) -> PromptOutcome {
    let _unused = connection.send_notification(CancelNotification::new(session_ref.to_owned()));
    match tokio::time::timeout(drain_timeout, &mut *response).await {
        Ok(Ok(Ok(response))) => PromptOutcome::Known {
            response: response.0,
            host_stop_reason: Some(reason.to_owned()),
        },
        Ok(Ok(Err(error))) => PromptOutcome::Unknown(json!({
            "cancel_reason": reason,
            "error": error.to_string(),
        })),
        Ok(Err(error)) => PromptOutcome::Unknown(json!({
            "cancel_reason": reason,
            "join_error": error.to_string(),
        })),
        Err(_) => {
            response.abort();
            PromptOutcome::Unknown(json!({
                "cancel_reason": reason,
                "error": "cancel drain deadline exceeded",
            }))
        }
    }
}

async fn emit_terminal(
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    session_ref: &str,
    outcome: PromptOutcome,
) -> Result<(), DriverError> {
    let (terminal, permission_ids) = {
        let mut state = shared.lock().await;
        let session = state
            .sessions
            .get_mut(session_ref)
            .ok_or_else(|| DriverError::Protocol("terminal turn session disappeared".to_owned()))?;
        let active = session
            .active
            .take()
            .ok_or_else(|| DriverError::Protocol("terminal turn was not active".to_owned()))?;
        let permission_ids = state
            .permissions
            .iter()
            .filter(|(_, pending)| pending.fence == active.fence)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let (raw_response, certainty, quiescent, stop_reason, runtime_stop_reason) = match outcome {
            PromptOutcome::Known {
                response,
                host_stop_reason,
            } => {
                let runtime_stop = response
                    .get("stopReason")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                let (stop_reason, runtime_stop_reason) = if let Some(host_stop) = host_stop_reason {
                    (host_stop, Some(runtime_stop))
                } else {
                    (runtime_stop, None)
                };
                (
                    response,
                    HarnessExecutionCertainty::OutcomeKnown,
                    true,
                    stop_reason,
                    runtime_stop_reason,
                )
            }
            PromptOutcome::Unknown(evidence) => (
                evidence,
                HarnessExecutionCertainty::OutcomeUnknown,
                false,
                "outcome_unknown".to_owned(),
                None,
            ),
        };
        let last_event_seq = active.next_event_seq.saturating_sub(1);
        let assistant_messages = active.assistant_messages;
        (
            TurnTerminal {
                fence: active.fence,
                last_event_seq,
                stop_reason,
                runtime_stop_reason,
                execution_certainty: certainty,
                session_quiescent: quiescent,
                session_persistence: if quiescent {
                    SessionPersistence::RuntimeClaimed
                } else {
                    SessionPersistence::Unknown
                },
                assistant_messages,
                usage: active.usage,
                raw_prompt_response: bound_json(raw_response, MAX_FRAME_BYTES / 2),
            },
            permission_ids,
        )
    };
    for permission_id in permission_ids {
        if let Some(pending) = shared.lock().await.permissions.remove(&permission_id) {
            let _unused = pending.response.send(PermissionOutcome::Cancelled);
        }
    }
    notifications
        .send(DriverNotification {
            method: "harness.acp.turn.terminal".to_owned(),
            params: serde_json::to_value(terminal)?,
        })
        .await
        .map_err(|_| DriverError::Runtime("host notification channel closed".to_owned()))
}

/// Folds one replayed update into the capture, or refuses it past a bound.
///
/// Returning `None` marks the capture truncated and stops forwarding, so a
/// consumer learns a bound was reached instead of inferring completeness from a
/// replay that simply stopped.
fn capture_transcript_entry(
    capture: &mut TranscriptCapture,
    session_ref: &str,
    update: &Value,
) -> Option<TranscriptEntry> {
    if capture.truncated {
        return None;
    }
    let encoded = u64::try_from(update.to_string().len()).unwrap_or(u64::MAX);
    if capture.entry_count >= MAX_TRANSCRIPT_ENTRIES
        || capture.observed_payload_bytes.saturating_add(encoded) > MAX_TRANSCRIPT_BYTES
    {
        capture.truncated = true;
        return None;
    }
    let entry_seq = capture.next_entry_seq;
    capture.next_entry_seq = capture.next_entry_seq.saturating_add(1);
    capture.entry_count = capture.entry_count.saturating_add(1);
    capture.observed_payload_bytes = capture.observed_payload_bytes.saturating_add(encoded);
    Some(TranscriptEntry {
        session_ref: session_ref.to_owned(),
        entry_seq,
        observed_at_ms: now_ms_i64(),
        classification: classify_update(update).to_owned(),
        raw_update: update.clone(),
    })
}

async fn forward_transcript_entry(
    notifications: &mpsc::Sender<DriverNotification>,
    entry: TranscriptEntry,
) -> Result<(), DriverError> {
    notifications
        .send(DriverNotification {
            method: "harness.acp.session.transcript.entry".to_owned(),
            params: serde_json::to_value(entry)?,
        })
        .await
        .map_err(|_| DriverError::Runtime("host notification channel closed".to_owned()))
}

async fn handle_session_update(
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    raw: Value,
) -> Result<(), DriverError> {
    let session_ref = raw
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| DriverError::Protocol("session/update omitted sessionId".to_owned()))?;
    let update = raw
        .get("update")
        .cloned()
        .ok_or_else(|| DriverError::Protocol("session/update omitted update".to_owned()))?;
    let event = {
        let mut state = shared.lock().await;
        let Some(session) = state.sessions.get_mut(session_ref) else {
            return Ok(());
        };
        // A replay in flight claims these updates as transcript entries. It is
        // the one case where an update outside a turn has an honest home,
        // because it is answering a question a caller asked rather than
        // reporting work.
        if let Some(capture) = session.capturing.as_mut() {
            let entry = capture_transcript_entry(capture, session_ref, &update);
            drop(state);
            return match entry {
                Some(entry) => forward_transcript_entry(notifications, entry).await,
                None => Ok(()),
            };
        }
        // Otherwise an update outside any active turn belongs to no invocation,
        // and attributing it to the next one would corrupt that invocation's
        // event count and chain digest. Adoption no longer produces these: it
        // sends `session/resume`, which must not replay. What reaches here now
        // is a runtime volunteering activity Fleetd never fenced, which is
        // exactly what there is no honest place to put.
        let Some(active) = session.active.as_mut() else {
            return Ok(());
        };
        let event_seq = active.next_event_seq;
        active.next_event_seq = active
            .next_event_seq
            .checked_add(1)
            .ok_or_else(|| DriverError::Protocol("event sequence overflowed".to_owned()))?;
        let classification = classify_update(&update);
        let recognized_activity = matches!(
            classification,
            "agent_message_content"
                | "reasoning_content"
                | "tool_call"
                | "tool_call_update"
                | "plan_update"
        );
        if recognized_activity {
            active.activity.send_replace(now_ms());
        }
        capture_update(active, event_seq, &update)?;
        if classification == "tool_call" {
            active.tool_calls = active.tool_calls.saturating_add(1);
        }
        if active.tool_calls > active.policy.tool_budget.limit {
            active
                .cancellation
                .send_replace(Some("tool_budget".to_owned()));
        }
        TurnEvent {
            fence: active.fence.clone(),
            event_seq,
            observed_at_ms: now_ms_i64(),
            classification: classification.to_owned(),
            raw_update: bound_json(update, active.policy.max_captured_output_bytes),
        }
    };
    notifications
        .send(DriverNotification {
            method: "harness.acp.turn.event".to_owned(),
            params: serde_json::to_value(event)?,
        })
        .await
        .map_err(|_| DriverError::Runtime("host notification channel closed".to_owned()))
}

fn capture_update(
    active: &mut ActiveTurn,
    event_seq: u64,
    update: &Value,
) -> Result<(), DriverError> {
    let kind = update.get("sessionUpdate").and_then(Value::as_str);
    if kind == Some("usage_update") {
        active.usage = update.clone();
    }
    if kind != Some("agent_message_chunk") {
        return Ok(());
    }
    let message_id = match update.get("messageId") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_str().map(str::to_owned).ok_or_else(|| {
            DriverError::Protocol("agent messageId must be a string or null".to_owned())
        })?),
    };
    let content = update
        .get("content")
        .ok_or_else(|| DriverError::Protocol("agent message chunk omitted content".to_owned()))?;

    let starts_new_message = active
        .assistant_messages
        .last()
        .is_none_or(|message| message.message_id != message_id);
    if starts_new_message {
        if let Some(id) = &message_id
            && active
                .assistant_messages
                .iter()
                .any(|message| message.message_id.as_ref() == Some(id))
        {
            return Err(DriverError::Protocol(format!(
                "agent messageId {id} reappeared after a different message"
            )));
        }
        active.assistant_messages.push(AssistantMessage {
            message_id,
            content: Vec::new(),
            complete: true,
            first_event_seq: event_seq,
            last_event_seq: event_seq,
        });
    }
    let message = active
        .assistant_messages
        .last_mut()
        .expect("assistant message was created before capture");
    message.last_event_seq = event_seq;
    let remaining = active
        .policy
        .max_captured_output_bytes
        .saturating_sub(active.captured_bytes);
    if content.get("type").and_then(Value::as_str) == Some("text") {
        let text = content
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| DriverError::Protocol("agent text chunk omitted text".to_owned()))?;
        if text.len() <= remaining {
            message.content.push(content.clone());
            active.captured_bytes += text.len();
        } else {
            let mut end = remaining.min(text.len());
            while !text.is_char_boundary(end) {
                end = end.saturating_sub(1);
            }
            if end > 0 {
                let mut captured = content.clone();
                captured["text"] = Value::String(text[..end].to_owned());
                message.content.push(captured);
            }
            active.captured_bytes = active.policy.max_captured_output_bytes;
            message.complete = false;
        }
    } else {
        let encoded = serde_json::to_vec(content)?;
        if encoded.len() <= remaining {
            message.content.push(content.clone());
            active.captured_bytes += encoded.len();
        } else {
            active.captured_bytes = active.policy.max_captured_output_bytes;
            message.complete = false;
        }
    }
    Ok(())
}

fn classify_update(update: &Value) -> &'static str {
    match update.get("sessionUpdate").and_then(Value::as_str) {
        Some("user_message_chunk") => "user_message_content",
        Some("agent_message_chunk") => "agent_message_content",
        Some("agent_thought_chunk") => "reasoning_content",
        Some("tool_call") => "tool_call",
        Some("tool_call_update") => "tool_call_update",
        Some("plan") => "plan_update",
        Some("usage_update") => "usage",
        Some(
            "session_info_update"
            | "available_commands_update"
            | "current_mode_update"
            | "config_option_update",
        ) => "metadata",
        _ => "unknown",
    }
}

async fn handle_permission_request(
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    raw: Value,
) -> Result<Value, DriverError> {
    let session_ref = raw
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| DriverError::Protocol("permission request omitted sessionId".to_owned()))?;
    let permission_id = Uuid::new_v4().to_string();
    let (response_tx, response_rx) = oneshot::channel();
    let (event, expiry) = {
        let mut state = shared.lock().await;
        let session = state.sessions.get_mut(session_ref).ok_or_else(|| {
            DriverError::Protocol("permission request references unknown session".to_owned())
        })?;
        let active = session.active.as_mut().ok_or_else(|| {
            DriverError::Protocol("permission request arrived outside an active turn".to_owned())
        })?;
        let event_seq = active.next_event_seq;
        active.next_event_seq += 1;
        active.activity.send_replace(now_ms());
        let expiry = active
            .policy
            .idle_timeout_ms
            .min(active.policy.wall_timeout_ms);
        let fence = active.fence.clone();
        let event = fleetd_proto::harness_acp::PermissionRequested {
            fence: fence.clone(),
            permission_id: permission_id.clone(),
            event_seq,
            tool_call: raw.get("toolCall").cloned().unwrap_or(Value::Null),
            options: raw
                .get("options")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            expires_at_ms: now_ms_i64().saturating_add(i64::try_from(expiry).unwrap_or(i64::MAX)),
        };
        state.permissions.insert(
            permission_id.clone(),
            PendingPermission {
                fence,
                response: response_tx,
            },
        );
        (event, expiry)
    };
    notifications
        .send(DriverNotification {
            method: "harness.acp.permission.requested".to_owned(),
            params: serde_json::to_value(event)?,
        })
        .await
        .map_err(|_| DriverError::Runtime("host notification channel closed".to_owned()))?;
    let outcome = tokio::time::timeout(Duration::from_millis(expiry), response_rx)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(PermissionOutcome::Cancelled);
    shared.lock().await.permissions.remove(&permission_id);
    Ok(match outcome {
        PermissionOutcome::Selected { option_id } => {
            json!({"outcome": {"outcome": "selected", "optionId": option_id}})
        }
        PermissionOutcome::Cancelled => json!({"outcome": {"outcome": "cancelled"}}),
    })
}

async fn resolve_permission(
    shared: &Arc<Mutex<SharedState>>,
    request: PermissionResolution,
) -> Result<(), DriverError> {
    let pending = shared
        .lock()
        .await
        .permissions
        .remove(&request.permission_id)
        .ok_or_else(|| DriverError::Protocol("permission request is not pending".to_owned()))?;
    if pending.fence != request.fence {
        return Err(DriverError::Protocol(
            "permission resolution fence does not match request".to_owned(),
        ));
    }
    pending
        .response
        .send(request.outcome)
        .map_err(|_| DriverError::Protocol("permission request already expired".to_owned()))
}

async fn cancel_turn(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    request: CancelTurn,
) -> Result<AcceptedResult, DriverError> {
    let session_ref = {
        let state = shared.lock().await;
        state
            .sessions
            .iter()
            .find(|(_, session)| {
                session
                    .active
                    .as_ref()
                    .is_some_and(|active| active.fence == request.fence)
            })
            .map(|(session_ref, _)| session_ref.clone())
            .ok_or_else(|| DriverError::Protocol("turn fence is not active".to_owned()))?
    };
    cancel_permissions_for_fence(shared, &request.fence).await;
    {
        let mut state = shared.lock().await;
        let active = state
            .sessions
            .get_mut(&session_ref)
            .and_then(|session| session.active.as_mut())
            .ok_or_else(|| DriverError::Protocol("turn stopped during cancellation".to_owned()))?;
        active.cancellation.send_replace(Some(request.reason));
    }
    connection
        .send_notification(CancelNotification::new(session_ref))
        .map_err(|error| DriverError::Runtime(error.to_string()))?;
    Ok(AcceptedResult { accepted: true })
}

async fn close_session(
    shared: &Arc<Mutex<SharedState>>,
    request: CloseSession,
) -> Result<CloseSessionResult, DriverError> {
    let mut state = shared.lock().await;
    let session = state.sessions.get(&request.session_ref).ok_or_else(|| {
        DriverError::Protocol("session reference is not owned by driver".to_owned())
    })?;
    if session.active.is_some() {
        return Err(DriverError::Protocol(
            "cannot close a session with an active turn".to_owned(),
        ));
    }
    if session.binding.binding_id != request.binding_id
        || session.binding.binding_generation != request.binding_generation
        || session.binding.owner_epoch != request.owner_epoch
    {
        return Err(DriverError::Protocol(
            "session close fence does not match binding".to_owned(),
        ));
    }
    state.sessions.remove(&request.session_ref);
    Ok(CloseSessionResult {
        ownership_retired: true,
        native_resources_released: false,
    })
}

async fn cancel_permissions_for_fence(shared: &Arc<Mutex<SharedState>>, fence: &ExecutionFence) {
    let pending = {
        let mut state = shared.lock().await;
        let ids = state
            .permissions
            .iter()
            .filter(|(_, pending)| pending.fence == *fence)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| state.permissions.remove(&id))
            .collect::<Vec<_>>()
    };
    for pending in pending {
        let _unused = pending.response.send(PermissionOutcome::Cancelled);
    }
}

async fn cancel_all_permissions(shared: &Arc<Mutex<SharedState>>) {
    let pending = shared
        .lock()
        .await
        .permissions
        .drain()
        .map(|(_, pending)| pending)
        .collect::<Vec<_>>();
    for pending in pending {
        let _unused = pending.response.send(PermissionOutcome::Cancelled);
    }
}

fn bound_json(value: Value, limit: usize) -> Value {
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    if bytes.len() <= limit {
        return value;
    }
    json!({
        "truncated": true,
        "observed_bytes": bytes.len(),
        "sha256": format!("sha256:{:x}", Sha256::digest(&bytes)),
    })
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

#[cfg(test)]
mod tests {
    use fleetd_proto::harness_acp::{ResolvedMcpGrant, ResolvedMcpHttpHeader};

    use super::*;

    /// A prompt echoed back by the runtime is a recognised ACP kind, so it must
    /// not land in `unknown`. That counter exists to mean "an update this build
    /// has never seen", and a real harness emits one prompt per turn, so leaving
    /// it unnamed put a constant offset on the one signal worth watching.
    #[test]
    fn every_acp_update_this_build_understands_is_named() {
        for (update, expected) in [
            ("user_message_chunk", "user_message_content"),
            ("agent_message_chunk", "agent_message_content"),
            ("agent_thought_chunk", "reasoning_content"),
            ("tool_call", "tool_call"),
            ("tool_call_update", "tool_call_update"),
            ("plan", "plan_update"),
            ("usage_update", "usage"),
            ("session_info_update", "metadata"),
        ] {
            let classification = classify_update(&json!({"sessionUpdate": update}));
            assert_eq!(classification, expected, "{update} was misclassified");
            assert_ne!(
                fleetd_proto::operations::EventClass::parse(classification),
                fleetd_proto::operations::EventClass::Unknown,
                "{update} is a kind this build understands and must not count as unknown"
            );
        }
        assert_eq!(
            classify_update(&json!({"sessionUpdate": "a_kind_acp_adds_later"})),
            "unknown"
        );
    }

    /// Adoption wants the session back, not its transcript. ACP obliges
    /// `session/load` to replay the entire conversation before it answers and
    /// obliges `session/resume` not to, so resume wins wherever it exists.
    #[test]
    fn adoption_prefers_resume_and_falls_back_to_load() {
        assert_eq!(
            AdoptionMethods {
                load: true,
                resume: true
            }
            .method(),
            Some("session/resume")
        );
        assert_eq!(
            AdoptionMethods {
                load: true,
                resume: false
            }
            .method(),
            Some("session/load")
        );
        assert_eq!(
            AdoptionMethods {
                load: false,
                resume: true
            }
            .method(),
            Some("session/resume")
        );
        assert_eq!(
            AdoptionMethods {
                load: false,
                resume: false
            }
            .method(),
            None,
            "a runtime advertising neither cannot be adopted, and must not \
             silently open a fresh session instead"
        );
    }

    /// The capability shape is read from the runtime's own initialize response,
    /// so a runtime that omits `sessionCapabilities` entirely is load-only
    /// rather than unadoptable.
    #[test]
    fn adoption_methods_are_read_from_advertised_capabilities() {
        let load_only: AgentCapabilities = serde_json::from_value(json!({"loadSession": true}))
            .expect("capabilities without sessionCapabilities parse");
        assert_eq!(
            AdoptionMethods::from_capabilities(&load_only).method(),
            Some("session/load")
        );

        let resumable: AgentCapabilities = serde_json::from_value(
            json!({"loadSession": true, "sessionCapabilities": {"resume": {}}}),
        )
        .expect("capabilities with resume parse");
        assert_eq!(
            AdoptionMethods::from_capabilities(&resumable).method(),
            Some("session/resume")
        );

        let fresh_only: AgentCapabilities =
            serde_json::from_value(json!({})).expect("empty capabilities parse");
        assert_eq!(
            AdoptionMethods::from_capabilities(&fresh_only).method(),
            None
        );
    }

    #[test]
    fn mcp_resolution_requires_an_exact_requested_loopback_endpoint() {
        let mut request = open_request();
        request.mcp_grants = vec!["fleet.messaging.send".to_owned()];
        assert!(resolve_mcp_servers(&request).is_err());

        request.resolved_mcp_grants = vec![ResolvedMcpGrant {
            name: "fleet.messaging.send".to_owned(),
            endpoint: ResolvedMcpEndpoint::Http {
                url: "https://example.com/mcp".to_owned(),
                headers: Vec::new(),
            },
        }];
        assert!(resolve_mcp_servers(&request).is_err());

        request.resolved_mcp_grants[0].endpoint = ResolvedMcpEndpoint::Http {
            url: "http://127.0.0.1:49152/mcp".to_owned(),
            headers: vec![ResolvedMcpHttpHeader {
                name: "x-fleetd-grant-token".to_owned(),
                value: "narrow-token".to_owned(),
            }],
        };
        let servers = resolve_mcp_servers(&request).expect("valid resolution");
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0]["type"], "http");
        assert_eq!(servers[0]["name"], "fleet.messaging.send");
    }

    #[test]
    fn mcp_resolution_rejects_duplicate_or_unrequested_grants() {
        let mut request = open_request();
        request.mcp_grants = vec![
            "fleet.messaging.send".to_owned(),
            "fleet.messaging.send".to_owned(),
        ];
        assert!(resolve_mcp_servers(&request).is_err());

        request.mcp_grants = vec!["fleet.messaging.send".to_owned()];
        request.resolved_mcp_grants = vec![ResolvedMcpGrant {
            name: "fleet.messaging.read".to_owned(),
            endpoint: ResolvedMcpEndpoint::Http {
                url: "http://127.0.0.1:49152/mcp".to_owned(),
                headers: Vec::new(),
            },
        }];
        assert!(resolve_mcp_servers(&request).is_err());
    }

    fn open_request() -> OpenSession {
        OpenSession {
            binding: Binding {
                binding_id: "binding".to_owned(),
                binding_generation: 1,
                owner_epoch: 1,
            },
            mode: OpenSessionMode::Create,
            working_directory: "/tmp".to_owned(),
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            resolved_mcp_grants: Vec::new(),
            profile_digest: "sha256:profile".to_owned(),
        }
    }
}
