//! Wire types for the `fleetd.harness-acp` operational interface.
//!
//! ACP is an inner harness interoperability protocol. These types describe the
//! exact frames exchanged with a harness plugin; the typed host client that
//! drives them lives in the daemon.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::plugin::PluginInterface;

pub const HARNESS_ACP_INTERFACE_ID: &str = "fleetd.harness-acp";

/// The exact operational interface implemented by an ACP harness plugin.
#[must_use]
pub fn interface() -> PluginInterface {
    PluginInterface::new(HARNESS_ACP_INTERFACE_ID, semver::Version::new(0, 1, 0))
}

/// Fleet-owned identity for one logical session lane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
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
    /// Controller-resolved, invocation-scoped endpoints for the requested
    /// grant names. These are trusted controller-to-driver data, never worker
    /// configuration supplied as arbitrary child commands.
    #[serde(default)]
    pub resolved_mcp_grants: Vec<ResolvedMcpGrant>,
    pub profile_digest: String,
}

/// One controller-approved MCP endpoint resolving an exact runtime grant.
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_stop_reason: Option<String>,
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
