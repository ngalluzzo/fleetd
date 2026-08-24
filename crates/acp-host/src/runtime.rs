use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agent_client_protocol::{
    AcpAgent, AcpAgentConfig, Agent, ConnectionTo, JsonRpcNotification, JsonRpcRequest,
    JsonRpcResponse,
    schema::{
        ProtocolVersion,
        v1::{CancelNotification, Implementation, InitializeRequest, InitializeResponse},
    },
};
use fleetd::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, CloseSession, CloseSessionResult,
    DescribeResult, DriverIdentity, EffectiveEnforcement, ExecutionFence,
    HarnessExecutionCertainty, HarnessLimits, OpenSession, OpenSessionMode, OpenSessionResult,
    PermissionOutcome, PermissionResolution, RuntimeIdentity, SessionPersistence, StartTurn,
    StartTurnResult, TurnEvent, TurnTerminal,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use uuid::Uuid;

const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_CAPTURE_BYTES: usize = 512 * 1024;
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
        let executable_digest = digest_file(&config.runtime.identity_path)?;
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

fn digest_file(path: &Path) -> Result<String, DriverError> {
    let bytes = std::fs::read(path)?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

enum Command {
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
    active: Option<ActiveTurn>,
}

struct ActiveTurn {
    fence: ExecutionFence,
    next_event_seq: u64,
    policy: fleetd::TurnPolicy,
    captured_bytes: usize,
    assistant_text: String,
    assistant_first_event: Option<u64>,
    assistant_last_event: Option<u64>,
    assistant_complete: bool,
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
            let (description, load_session) = match initialized {
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
                load_session,
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

async fn initialize_runtime(
    connection: &ConnectionTo<Agent>,
    runtime: &RuntimeConfig,
    executable_digest: String,
    profile_digest: String,
) -> Result<(DescribeResult, bool), DriverError> {
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
    let load_session = parsed.agent_capabilities.load_session;
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
    Ok((description, load_session))
}

async fn serve_commands(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    commands: &mut mpsc::Receiver<Command>,
    load_session: bool,
) -> Result<(), agent_client_protocol::Error> {
    while let Some(command) = commands.recv().await {
        match command {
            Command::Open { request, reply } => {
                let result = open_session(connection, shared, request, load_session).await;
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

async fn open_session(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    request: OpenSession,
    load_session: bool,
) -> Result<OpenSessionResult, DriverError> {
    if !request.mcp_grants.is_empty() {
        return Err(DriverError::Protocol(
            "MCP grants are not implemented by this driver profile".to_owned(),
        ));
    }
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
                "mcpServers": []
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
            if !load_session {
                return Err(DriverError::Protocol(
                    "inner ACP runtime does not support session/load".to_owned(),
                ));
            }
            let raw_request = json!({
                "sessionId": session_ref,
                "cwd": request.working_directory,
                "additionalDirectories": directories,
                "mcpServers": []
            });
            let response = connection
                .send_request(RawLoadSessionRequest(raw_request))
                .block_task()
                .await
                .map_err(|error| DriverError::Runtime(error.to_string()))?;
            (session_ref.clone(), true, response.0)
        }
    };
    let mut state = shared.lock().await;
    if let Some(existing) = state.sessions.get(&session_ref)
        && (existing.binding != request.binding
            || existing.cwd != request.working_directory
            || existing.additional_directories != request.additional_directories)
    {
        return Err(DriverError::Protocol(
            "session reference already belongs to incompatible binding state".to_owned(),
        ));
    }
    let effective_working_directory = request.working_directory.clone();
    let effective_additional_directories = request.additional_directories.clone();
    state.sessions.insert(
        session_ref.clone(),
        SessionState {
            binding: request.binding,
            cwd: request.working_directory,
            additional_directories: request.additional_directories,
            active: None,
        },
    );
    Ok(OpenSessionResult {
        session_ref,
        profile_digest: request.profile_digest,
        resumed,
        effective_config: json!({
            "working_directory": effective_working_directory,
            "additional_directories": effective_additional_directories,
        }),
        raw_session_result: bound_json(raw_result, MAX_FRAME_BYTES / 2),
    })
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
            assistant_text: String::new(),
            assistant_first_event: None,
            assistant_last_event: None,
            assistant_complete: true,
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
                    Ok(Ok(response)) => PromptOutcome::Known(response.0),
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
    Known(Value),
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
        Ok(Ok(Ok(response))) => PromptOutcome::Known(response.0),
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
        let (raw_response, certainty, quiescent, stop_reason) = match outcome {
            PromptOutcome::Known(response) => {
                let stop = response
                    .get("stopReason")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                (
                    response,
                    HarnessExecutionCertainty::OutcomeKnown,
                    true,
                    stop,
                )
            }
            PromptOutcome::Unknown(evidence) => (
                evidence,
                HarnessExecutionCertainty::OutcomeUnknown,
                false,
                "outcome_unknown".to_owned(),
            ),
        };
        let last_event_seq = active.next_event_seq.saturating_sub(1);
        let assistant_messages = if active.assistant_text.is_empty() && active.assistant_complete {
            Vec::new()
        } else {
            vec![AssistantMessage {
                message_id: None,
                content: vec![json!({"type": "text", "text": active.assistant_text})],
                complete: active.assistant_complete,
                first_event_seq: active.assistant_first_event.unwrap_or(last_event_seq),
                last_event_seq: active.assistant_last_event.unwrap_or(last_event_seq),
            }]
        };
        (
            TurnTerminal {
                fence: active.fence,
                last_event_seq,
                stop_reason,
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
        capture_update(active, event_seq, &update);
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

fn capture_update(active: &mut ActiveTurn, event_seq: u64, update: &Value) {
    let kind = update.get("sessionUpdate").and_then(Value::as_str);
    if kind == Some("usage_update") {
        active.usage = update.clone();
    }
    if kind != Some("agent_message_chunk") {
        return;
    }
    let Some(text) = update
        .get("content")
        .filter(|content| content.get("type").and_then(Value::as_str) == Some("text"))
        .and_then(|content| content.get("text"))
        .and_then(Value::as_str)
    else {
        return;
    };
    active.assistant_first_event.get_or_insert(event_seq);
    active.assistant_last_event = Some(event_seq);
    let remaining = active
        .policy
        .max_captured_output_bytes
        .saturating_sub(active.captured_bytes);
    if text.len() <= remaining {
        active.assistant_text.push_str(text);
        active.captured_bytes += text.len();
    } else {
        let mut end = remaining.min(text.len());
        while !text.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        active.assistant_text.push_str(&text[..end]);
        active.captured_bytes = active.policy.max_captured_output_bytes;
        active.assistant_complete = false;
    }
}

fn classify_update(update: &Value) -> &'static str {
    match update.get("sessionUpdate").and_then(Value::as_str) {
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
        let event = fleetd::PermissionRequested {
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
