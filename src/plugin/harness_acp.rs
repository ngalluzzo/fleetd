use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{Capability, PluginError, PluginManifest, PluginProcess};

const CAPABILITY_NAME: &str = "harness.acp";
const CAPABILITY_VERSION: u32 = 1;
const MAX_FLEET_ID_BYTES: usize = 256;
const MAX_SESSION_REF_BYTES: usize = 4_096;
const MAX_FRAME_BYTES: usize = 1024 * 1024;
const MAX_CAPTURE_BYTES: usize = 512 * 1024;

/// The exact experimental ACP harness capability implemented by this client.
#[must_use]
pub fn capability() -> Capability {
    Capability {
        name: CAPABILITY_NAME.to_owned(),
        version: CAPABILITY_VERSION,
    }
}

/// Fleet-owned identity for one logical session lane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Binding {
    pub binding_id: String,
    pub binding_generation: u64,
    pub owner_epoch: u64,
}

/// Complete write-ahead fence for one effectful harness turn.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExecutionFence {
    pub binding_id: String,
    pub binding_generation: u64,
    pub owner_epoch: u64,
    pub invocation_id: String,
    pub fence_token: String,
}

/// Driver and observed inner-runtime identity.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DescribeResult {
    pub driver: DriverIdentity,
    pub runtime: RuntimeIdentity,
    pub agent_capabilities: Value,
    pub limits: HarnessLimits,
    pub profile_digest: String,
    pub raw_initialize_result: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DriverIdentity {
    pub version: String,
    pub acp_sdk_version: String,
    pub acp_protocol_version: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RuntimeIdentity {
    pub name: String,
    pub version: String,
    pub executable_digest: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HarnessLimits {
    pub max_concurrent_turns: u32,
    pub max_frame_bytes: usize,
}

/// Creates or resumes a native ACP session.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OpenSession {
    pub binding: Binding,
    pub mode: OpenSessionMode,
    pub working_directory: String,
    #[serde(default)]
    pub additional_directories: Vec<String>,
    #[serde(default)]
    pub mcp_grants: Vec<String>,
    /// Controller-resolved, capability-scoped endpoints for the requested
    /// grant names. These are trusted controller-to-driver data, never worker
    /// configuration supplied as arbitrary child commands.
    #[serde(default)]
    pub resolved_mcp_grants: Vec<ResolvedMcpGrant>,
    pub profile_digest: String,
}

/// One controller-approved MCP endpoint resolving an exact semantic grant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolvedMcpGrant {
    pub name: String,
    pub endpoint: ResolvedMcpEndpoint,
}

/// Transport for one controller-approved MCP grant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResolvedMcpEndpoint {
    Http {
        url: String,
        #[serde(default)]
        headers: Vec<ResolvedMcpHttpHeader>,
    },
}

/// An HTTP header whose value is redacted from Rust debug output.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
pub struct ResolvedMcpHttpHeader {
    pub name: String,
    pub value: String,
}

impl fmt::Debug for ResolvedMcpHttpHeader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedMcpHttpHeader")
            .field("name", &self.name)
            .field("value", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OpenSessionMode {
    Create,
    Resume { session_ref: String },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OpenSessionResult {
    pub session_ref: String,
    pub profile_digest: String,
    pub resumed: bool,
    pub effective_config: Value,
    pub raw_session_result: Value,
}

/// Immutable fleet attribution carried as evidence, never authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TurnSource {
    pub agent_id: String,
    pub message_id: String,
    pub channel_id: String,
    pub sender_id: String,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PromptBlock {
    Text { text: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ToolBudget {
    pub limit: u64,
    pub required_enforcement: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TurnPolicy {
    pub idle_timeout_ms: u64,
    pub wall_timeout_ms: u64,
    pub cancel_drain_timeout_ms: u64,
    pub max_captured_output_bytes: usize,
    pub permission_policy: String,
    pub tool_budget: ToolBudget,
    pub token_budget: Option<u64>,
}

/// Starts one prompt under an already-durable invocation fence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StartTurn {
    pub fence: ExecutionFence,
    pub session_ref: String,
    pub source: TurnSource,
    pub prompt: Vec<PromptBlock>,
    pub policy: TurnPolicy,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StartTurnResult {
    pub accepted: bool,
    pub effective_enforcement: EffectiveEnforcement,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EffectiveEnforcement {
    pub wall_timeout: String,
    pub idle_timeout: String,
    pub cancel_drain_timeout: String,
    pub captured_output_bytes: String,
    pub tool_budget: String,
    pub token_budget: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PermissionResolution {
    pub fence: ExecutionFence,
    pub permission_id: String,
    pub outcome: PermissionOutcome,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PermissionOutcome {
    Selected { option_id: String },
    Cancelled,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CancelTurn {
    pub fence: ExecutionFence,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AcceptedResult {
    pub accepted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CloseSession {
    pub binding_id: String,
    pub binding_generation: u64,
    pub owner_epoch: u64,
    pub session_ref: String,
    pub reason: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CloseSessionResult {
    pub ownership_retired: bool,
    pub native_resources_released: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TurnEvent {
    pub fence: ExecutionFence,
    pub event_seq: u64,
    pub observed_at_ms: i64,
    pub classification: String,
    pub raw_update: Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PermissionRequested {
    pub fence: ExecutionFence,
    pub permission_id: String,
    pub event_seq: u64,
    pub tool_call: Value,
    pub options: Vec<Value>,
    pub expires_at_ms: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AssistantMessage {
    pub message_id: Option<String>,
    pub content: Vec<Value>,
    pub complete: bool,
    pub first_event_seq: u64,
    pub last_event_seq: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessExecutionCertainty {
    NotStarted,
    OutcomeKnown,
    OutcomeUnknown,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionPersistence {
    Confirmed,
    RuntimeClaimed,
    Unknown,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TurnTerminal {
    pub fence: ExecutionFence,
    pub last_event_seq: u64,
    pub stop_reason: String,
    pub execution_certainty: HarnessExecutionCertainty,
    pub session_quiescent: bool,
    pub session_persistence: SessionPersistence,
    pub assistant_messages: Vec<AssistantMessage>,
    #[serde(default)]
    pub usage: Value,
    pub raw_prompt_response: Value,
}

/// The closed set of notifications emitted by `harness.acp` v1.
#[derive(Clone, Debug, PartialEq)]
pub enum HarnessAcpNotification {
    TurnEvent(TurnEvent),
    PermissionRequested(PermissionRequested),
    TurnTerminal(TurnTerminal),
}

#[derive(Clone, Debug)]
struct ActiveTurn {
    fence: ExecutionFence,
    last_event_seq: u64,
    seen: BTreeMap<u64, [u8; 32]>,
}

/// Typed host-side client for `harness.acp` v1.
///
/// This is deliberately the only capability call surface exported by fleetd;
/// callers cannot issue arbitrary JSON-RPC methods through a `PluginProcess`.
pub struct HarnessAcpClient {
    process: PluginProcess,
    active_turn: Option<ActiveTurn>,
}

impl HarnessAcpClient {
    pub(crate) fn new(process: PluginProcess) -> Result<Self, PluginError> {
        let required = capability();
        if !process.manifest().capabilities.contains(&required) {
            return Err(PluginError::MissingCapability {
                name: required.name,
                version: required.version,
            });
        }
        Ok(Self {
            process,
            active_turn: None,
        })
    }

    #[must_use]
    pub const fn manifest(&self) -> &PluginManifest {
        self.process.manifest()
    }

    #[must_use]
    pub fn process_id(&self) -> Option<u32> {
        self.process.process_id()
    }

    /// Reports observed driver and inner-runtime identity.
    ///
    /// # Errors
    ///
    /// Returns an error for transport, timeout, malformed identity, or unusable
    /// negotiated limits.
    pub async fn describe(&self) -> Result<DescribeResult, PluginError> {
        let result: DescribeResult = self
            .process
            .capability_call("harness.acp.describe", &serde_json::json!({}))
            .await?;
        validate_id("driver version", &result.driver.version)?;
        validate_id("ACP SDK version", &result.driver.acp_sdk_version)?;
        validate_id("runtime name", &result.runtime.name)?;
        validate_id("runtime version", &result.runtime.version)?;
        validate_id(
            "runtime executable digest",
            &result.runtime.executable_digest,
        )?;
        validate_id("profile digest", &result.profile_digest)?;
        if result.driver.acp_protocol_version != 1
            || result.limits.max_concurrent_turns == 0
            || result.limits.max_frame_bytes == 0
            || result.limits.max_frame_bytes > MAX_FRAME_BYTES
        {
            return Err(protocol("driver reported unusable ACP limits"));
        }
        Ok(result)
    }

    /// Creates or resumes a native session.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, transport failure, or a mismatched
    /// effective profile.
    pub async fn open_session(
        &self,
        request: &OpenSession,
    ) -> Result<OpenSessionResult, PluginError> {
        validate_binding(&request.binding)?;
        validate_id("profile_digest", &request.profile_digest)?;
        validate_absolute_path("working_directory", &request.working_directory)?;
        for directory in &request.additional_directories {
            validate_absolute_path("additional_directory", directory)?;
        }
        if let OpenSessionMode::Resume { session_ref } = &request.mode {
            validate_session_ref(session_ref)?;
        }
        let result: OpenSessionResult = self
            .process
            .capability_call("harness.acp.session.open", request)
            .await?;
        validate_session_ref(&result.session_ref)?;
        if result.profile_digest != request.profile_digest {
            return Err(protocol("session profile digest does not match request"));
        }
        match &request.mode {
            OpenSessionMode::Create if result.resumed => {
                return Err(protocol("created session was reported as resumed"));
            }
            OpenSessionMode::Resume { session_ref }
                if !result.resumed || result.session_ref != *session_ref =>
            {
                return Err(protocol(
                    "resumed session did not preserve its native reference",
                ));
            }
            OpenSessionMode::Create | OpenSessionMode::Resume { .. } => {}
        }
        Ok(result)
    }

    /// Sends one already-armed prompt. Completion arrives through
    /// [`Self::next_notification`].
    ///
    /// # Errors
    ///
    /// Returns an error for invalid or conflicting turn state, unsupported
    /// enforcement, or transport failure.
    pub async fn start_turn(
        &mut self,
        request: &StartTurn,
    ) -> Result<StartTurnResult, PluginError> {
        if self.active_turn.is_some() {
            return Err(protocol("a harness turn is already active"));
        }
        validate_fence(&request.fence)?;
        validate_session_ref(&request.session_ref)?;
        validate_turn_source(&request.source)?;
        if request.prompt.is_empty() {
            return Err(protocol("turn prompt must contain at least one block"));
        }
        if request.policy.idle_timeout_ms == 0
            || request.policy.wall_timeout_ms == 0
            || request.policy.cancel_drain_timeout_ms == 0
            || request.policy.max_captured_output_bytes == 0
            || request.policy.max_captured_output_bytes > MAX_CAPTURE_BYTES
        {
            return Err(protocol("turn policy bounds are invalid"));
        }
        if request.policy.permission_policy != "controller"
            || request.policy.tool_budget.required_enforcement != "observe_then_cancel"
            || request.policy.token_budget.is_some()
        {
            return Err(protocol("turn requests unsupported enforcement"));
        }
        let result: StartTurnResult = self
            .process
            .capability_call("harness.acp.turn.start", request)
            .await?;
        if !result.accepted {
            return Err(protocol("driver returned an unaccepted turn as success"));
        }
        validate_effective_enforcement(&result.effective_enforcement)?;
        self.active_turn = Some(ActiveTurn {
            fence: request.fence.clone(),
            last_event_seq: 0,
            seen: BTreeMap::new(),
        });
        Ok(result)
    }

    /// Resolves one bridged ACP permission request.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale fence, invalid permission ID, or driver
    /// rejection.
    pub async fn resolve_permission(
        &self,
        request: &PermissionResolution,
    ) -> Result<(), PluginError> {
        self.validate_active_fence(&request.fence)?;
        validate_id("permission_id", &request.permission_id)?;
        let result: AcceptedResult = self
            .process
            .capability_call("harness.acp.permission.resolve", request)
            .await?;
        if !result.accepted {
            return Err(protocol("driver rejected permission resolution"));
        }
        Ok(())
    }

    /// Requests cancellation while retaining the turn until terminal evidence.
    ///
    /// # Errors
    ///
    /// Returns an error when no turn is active or the driver rejects or cannot
    /// receive cancellation.
    pub async fn cancel_turn(&self, reason: impl Into<String>) -> Result<(), PluginError> {
        let active = self
            .active_turn
            .as_ref()
            .ok_or_else(|| protocol("no harness turn is active"))?;
        let reason = reason.into();
        validate_id("cancellation reason", &reason)?;
        let request = CancelTurn {
            fence: active.fence.clone(),
            reason,
        };
        let result: AcceptedResult = self
            .process
            .capability_call("harness.acp.turn.cancel", &request)
            .await?;
        if !result.accepted {
            return Err(protocol("driver rejected turn cancellation"));
        }
        Ok(())
    }

    /// Waits for and validates the next fenced harness notification.
    ///
    /// # Errors
    ///
    /// Returns an error for transport loss, an unexpected notification, stale
    /// fencing, or non-contiguous evidence.
    pub async fn next_notification(&mut self) -> Result<HarnessAcpNotification, PluginError> {
        let notification = self.process.next_notification().await?;
        match notification.method.as_str() {
            "harness.acp.turn.event" => {
                let event: TurnEvent = serde_json::from_value(notification.params)?;
                validate_id("event classification", &event.classification)?;
                let raw = serde_json::to_value(&event)?;
                self.admit_event(&event.fence, event.event_seq, &raw)?;
                Ok(HarnessAcpNotification::TurnEvent(event))
            }
            "harness.acp.permission.requested" => {
                let request: PermissionRequested = serde_json::from_value(notification.params)?;
                validate_id("permission_id", &request.permission_id)?;
                let raw = serde_json::to_value(&request)?;
                self.admit_event(&request.fence, request.event_seq, &raw)?;
                Ok(HarnessAcpNotification::PermissionRequested(request))
            }
            "harness.acp.turn.terminal" => {
                let terminal: TurnTerminal = serde_json::from_value(notification.params)?;
                self.validate_terminal(&terminal)?;
                self.active_turn = None;
                Ok(HarnessAcpNotification::TurnTerminal(terminal))
            }
            method => Err(protocol(format!(
                "unexpected notification for harness client: {method}"
            ))),
        }
    }

    /// Retires a quiescent native session binding.
    ///
    /// # Errors
    ///
    /// Returns an error when a turn is active, the close fence is invalid, or
    /// the driver rejects the request.
    pub async fn close_session(
        &self,
        request: &CloseSession,
    ) -> Result<CloseSessionResult, PluginError> {
        if self.active_turn.is_some() {
            return Err(protocol("cannot close a session with an active turn"));
        }
        validate_id("binding_id", &request.binding_id)?;
        validate_positive("binding_generation", request.binding_generation)?;
        validate_positive("owner_epoch", request.owner_epoch)?;
        validate_session_ref(&request.session_ref)?;
        validate_id("close reason", &request.reason)?;
        self.process
            .capability_call("harness.acp.session.close", request)
            .await
    }

    /// Gracefully stops the driver and its inner runtime.
    ///
    /// # Errors
    ///
    /// Returns an error when graceful shutdown is rejected or transport fails.
    pub async fn shutdown(self) -> Result<super::ShutdownOutcome, PluginError> {
        self.process.shutdown().await
    }

    fn validate_active_fence(&self, fence: &ExecutionFence) -> Result<(), PluginError> {
        validate_fence(fence)?;
        let active = self
            .active_turn
            .as_ref()
            .ok_or_else(|| protocol("no harness turn is active"))?;
        if active.fence != *fence {
            return Err(protocol(
                "notification or request fence does not match active turn",
            ));
        }
        Ok(())
    }

    fn admit_event(
        &mut self,
        fence: &ExecutionFence,
        event_seq: u64,
        raw: &Value,
    ) -> Result<(), PluginError> {
        self.validate_active_fence(fence)?;
        validate_positive("event_seq", event_seq)?;
        let active = self.active_turn.as_mut().expect("validated active turn");
        let digest: [u8; 32] = Sha256::digest(serde_json::to_vec(&raw)?).into();
        if let Some(previous) = active.seen.get(&event_seq) {
            return if *previous == digest {
                Ok(())
            } else {
                Err(protocol("event sequence was reused with different content"))
            };
        }
        let expected = active
            .last_event_seq
            .checked_add(1)
            .ok_or_else(|| protocol("event sequence overflowed"))?;
        if event_seq != expected {
            return Err(protocol(format!(
                "event sequence is not contiguous: expected {expected}, received {event_seq}"
            )));
        }
        active.seen.insert(event_seq, digest);
        active.last_event_seq = event_seq;
        Ok(())
    }

    fn validate_terminal(&self, terminal: &TurnTerminal) -> Result<(), PluginError> {
        self.validate_active_fence(&terminal.fence)?;
        let active = self.active_turn.as_ref().expect("validated active turn");
        if terminal.last_event_seq != active.last_event_seq {
            return Err(protocol(format!(
                "terminal event sequence does not match admitted evidence: expected {}, received {}",
                active.last_event_seq, terminal.last_event_seq
            )));
        }
        validate_id("terminal stop reason", &terminal.stop_reason)?;
        if terminal.execution_certainty == HarnessExecutionCertainty::NotStarted {
            return Err(protocol(
                "an accepted turn cannot terminate with not-started certainty",
            ));
        }
        if terminal.session_quiescent
            && terminal.execution_certainty == HarnessExecutionCertainty::OutcomeUnknown
        {
            return Err(protocol(
                "outcome-unknown terminal evidence cannot claim a quiescent session",
            ));
        }
        for message in &terminal.assistant_messages {
            if message.first_event_seq == 0
                || message.first_event_seq > message.last_event_seq
                || message.last_event_seq > terminal.last_event_seq
            {
                return Err(protocol(
                    "assistant message references invalid turn event bounds",
                ));
            }
        }
        Ok(())
    }
}

fn validate_binding(binding: &Binding) -> Result<(), PluginError> {
    validate_id("binding_id", &binding.binding_id)?;
    validate_positive("binding_generation", binding.binding_generation)?;
    validate_positive("owner_epoch", binding.owner_epoch)
}

fn validate_fence(fence: &ExecutionFence) -> Result<(), PluginError> {
    validate_id("binding_id", &fence.binding_id)?;
    validate_positive("binding_generation", fence.binding_generation)?;
    validate_positive("owner_epoch", fence.owner_epoch)?;
    validate_id("invocation_id", &fence.invocation_id)?;
    validate_id("fence_token", &fence.fence_token)
}

fn validate_turn_source(source: &TurnSource) -> Result<(), PluginError> {
    validate_id("source agent_id", &source.agent_id)?;
    validate_id("source message_id", &source.message_id)?;
    validate_id("source channel_id", &source.channel_id)?;
    validate_id("source sender_id", &source.sender_id)?;
    if let Some(correlation_id) = &source.correlation_id {
        validate_id("source correlation_id", correlation_id)?;
    }
    if let Some(causation_id) = &source.causation_id {
        validate_id("source causation_id", causation_id)?;
    }
    Ok(())
}

fn validate_effective_enforcement(enforcement: &EffectiveEnforcement) -> Result<(), PluginError> {
    if enforcement.wall_timeout != "hard"
        || enforcement.idle_timeout != "hard"
        || enforcement.cancel_drain_timeout != "hard"
        || enforcement.captured_output_bytes != "hard"
        || enforcement.tool_budget != "observe_then_cancel"
        || enforcement.token_budget != "unavailable"
    {
        return Err(protocol(
            "driver did not provide the required effective enforcement",
        ));
    }
    Ok(())
}

fn validate_id(kind: &str, value: &str) -> Result<(), PluginError> {
    if value.trim().is_empty() || value.len() > MAX_FLEET_ID_BYTES {
        return Err(protocol(format!(
            "{kind} must contain between 1 and {MAX_FLEET_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_positive(kind: &str, value: u64) -> Result<(), PluginError> {
    if value == 0 {
        return Err(protocol(format!("{kind} must be greater than zero")));
    }
    Ok(())
}

fn validate_session_ref(session_ref: &str) -> Result<(), PluginError> {
    if session_ref.is_empty() || session_ref.len() > MAX_SESSION_REF_BYTES {
        return Err(protocol(format!(
            "session_ref must contain between 1 and {MAX_SESSION_REF_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_absolute_path(kind: &str, value: &str) -> Result<(), PluginError> {
    if !std::path::Path::new(value).is_absolute() {
        return Err(protocol(format!("{kind} must be an absolute path")));
    }
    Ok(())
}

fn protocol(message: impl Into<String>) -> PluginError {
    PluginError::Protocol(message.into())
}
