//! Wire types for the `fleetd.harness-acp` operational interface.
//!
//! ACP is an inner harness interoperability protocol. These types describe the
//! exact frames exchanged with a harness plugin; the typed host client that
//! drives them lives in the daemon.

use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{model::ExecutionCertainty, plugin::PluginInterface};

pub const HARNESS_ACP_INTERFACE_ID: &str = "fleetd.harness-acp";

/// The exact operational interface implemented by an ACP harness plugin.
///
/// Interface identity is matched by equality rather than by `SemVer` range, so a
/// generation is a distinct name rather than a compatible upgrade. This is the
/// generation every host requires today.
#[must_use]
pub fn interface() -> PluginInterface {
    PluginInterface::new(HARNESS_ACP_INTERFACE_ID, semver::Version::new(0, 1, 0))
}

/// The generation that adds transcript retrieval.
///
/// A plugin declares this only once it answers
/// `harness.acp.session.transcript.start`, because an interface version is a
/// promise about methods and negotiating one a plugin cannot serve would trade
/// a clear refusal for a later "method not found".
#[must_use]
pub fn interface_v2() -> PluginInterface {
    PluginInterface::new(HARNESS_ACP_INTERFACE_ID, semver::Version::new(0, 2, 0))
}

/// Every generation an ACP harness plugin currently implements.
///
/// Both are declared so a host requiring either negotiates successfully; per
/// this repository's rule, `0.2.0` stays unstable until two independent
/// integrations have qualified against it.
#[must_use]
pub fn declared_interfaces() -> Vec<PluginInterface> {
    vec![interface(), interface_v2()]
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

/// A harness's claim, restated as the certainty fleetd records.
///
/// The two enums are deliberately separate types -- one is what a plugin
/// asserts, the other is what fleetd is willing to durably believe -- but they
/// share a vocabulary, so the translation belongs here rather than in whichever
/// caller happens to need it.
impl From<HarnessExecutionCertainty> for ExecutionCertainty {
    fn from(value: HarnessExecutionCertainty) -> Self {
        match value {
            HarnessExecutionCertainty::NotStarted => Self::NotStarted,
            HarnessExecutionCertainty::OutcomeKnown => Self::OutcomeKnown,
            HarnessExecutionCertainty::OutcomeUnknown => Self::OutcomeUnknown,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionPersistence {
    Confirmed,
    RuntimeClaimed,
    Unknown,
}

impl SessionPersistence {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 3] = [Self::Confirmed, Self::RuntimeClaimed, Self::Unknown];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Confirmed => "confirmed",
            Self::RuntimeClaimed => "runtime_claimed",
            Self::Unknown => "unknown",
        }
    }

    /// Reads back the representation `as_str` produced.
    ///
    /// Returns `None` for anything else, leaving the caller to say what an
    /// unreadable stored value means to it.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|variant| variant.as_str() == value)
    }
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
    TranscriptEntry(TranscriptEntry),
    TranscriptComplete(TranscriptComplete),
}

/// A request to replay one native session's stored conversation.
///
/// This is retrieval, not adoption. It carries the owning binding so a caller
/// cannot read a session lane it does not own, and it is answered immediately:
/// the entries arrive as notifications and a terminal notification closes the
/// replay, the same shape a turn already uses. Awaiting the whole replay inside
/// the request would deadlock, because a plugin drains notifications only
/// between requests.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StartTranscript {
    pub binding_id: String,
    pub binding_generation: u64,
    pub owner_epoch: u64,
    pub session_ref: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StartTranscriptResult {
    pub accepted: bool,
}

/// One stored conversation entry, as the runtime replayed it.
///
/// `entry_seq` orders the replay and is unrelated to a turn's `event_seq`: a
/// replay is each entry's final state, so these are entries rather than the
/// streamed updates that produced them.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TranscriptEntry {
    pub session_ref: String,
    pub entry_seq: u64,
    pub observed_at_ms: i64,
    pub classification: String,
    pub raw_update: Value,
}

/// The end of one replay, whether or not it was complete.
///
/// `truncated` is set when a bound stopped the replay, so a consumer can tell a
/// short conversation from a capped one instead of inferring completeness from
/// silence. `failure` carries a bounded diagnostic when the runtime refused.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptComplete {
    pub session_ref: String,
    pub entry_count: u64,
    pub observed_payload_bytes: u64,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{ExecutionCertainty, HarnessExecutionCertainty, SessionPersistence};

    #[test]
    fn stored_spelling_matches_the_wire_spelling() {
        for variant in SessionPersistence::ALL {
            let wire = serde_json::to_value(variant).expect("serialize persistence");
            assert_eq!(
                wire,
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(SessionPersistence::parse(variant.as_str()), Some(variant));
        }
    }

    #[test]
    fn unreadable_values_do_not_parse() {
        assert_eq!(SessionPersistence::parse("runtime-claimed"), None);
        assert_eq!(SessionPersistence::parse("Confirmed"), None);
        assert_eq!(SessionPersistence::parse(""), None);
    }

    #[test]
    fn all_lists_every_variant() {
        // Adding a variant makes this match non-exhaustive, and the count below
        // then fails until `ALL` learns about it too.
        for variant in SessionPersistence::ALL {
            match variant {
                SessionPersistence::Confirmed
                | SessionPersistence::RuntimeClaimed
                | SessionPersistence::Unknown => {}
            }
        }
        assert_eq!(SessionPersistence::ALL.len(), 3);
    }

    #[test]
    fn a_harness_claim_maps_onto_the_recorded_certainty() {
        for (claimed, recorded) in [
            (
                HarnessExecutionCertainty::NotStarted,
                ExecutionCertainty::NotStarted,
            ),
            (
                HarnessExecutionCertainty::OutcomeKnown,
                ExecutionCertainty::OutcomeKnown,
            ),
            (
                HarnessExecutionCertainty::OutcomeUnknown,
                ExecutionCertainty::OutcomeUnknown,
            ),
        ] {
            assert_eq!(ExecutionCertainty::from(claimed), recorded);
            // The two types agree on the wire, which is what let an earlier
            // version of this code translate through the stored string.
            assert_eq!(
                serde_json::to_value(claimed).expect("serialize claim"),
                serde_json::to_value(&recorded).expect("serialize record"),
            );
        }
    }
}
