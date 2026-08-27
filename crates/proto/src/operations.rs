//! Bounded operator read models for plugin generations and managed turns.
//!
//! Raw harness update streams are represented by exact byte counts and a chain
//! digest; the bounded result message remains the transcript authority.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{
    harness_acp::SessionPersistence,
    model::{
        DeliveryRecord, DeliveryState, ExecutionCertainty, Invocation, InvocationState, Message,
    },
    session::{SessionBinding, SessionBindingState},
};

/// One exact operational interface observed on a plugin generation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct ObservedPluginInterface {
    pub id: String,
    pub version: String,
}

/// Persisted lifecycle state of one ready plugin generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginGenerationState {
    Active,
    Stopped,
}

/// Operator-facing liveness derived from persisted state and heartbeat age.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginGenerationHealth {
    Active,
    Stale,
    Stopped,
}

/// Why a worker stopped routing work through one plugin generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginGenerationDisposition {
    Stopped,
    Restart,
    Fatal,
}

impl PluginGenerationDisposition {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 3] = [Self::Stopped, Self::Restart, Self::Fatal];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Restart => "restart",
            Self::Fatal => "fatal",
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

/// What Fleetd observed while terminating a plugin process group.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginShutdownOutcome {
    Graceful,
    Forced,
    Failed,
}

impl PluginShutdownOutcome {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 3] = [Self::Graceful, Self::Forced, Self::Failed];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Graceful => "graceful",
            Self::Forced => "forced",
            Self::Failed => "failed",
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

/// Durable operator read model for one ready plugin generation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct PluginGeneration {
    pub id: String,
    pub agent_id: String,
    pub plugin_id: String,
    pub plugin_name: String,
    pub plugin_version: String,
    pub interfaces: Vec<ObservedPluginInterface>,
    pub process_id: Option<u32>,
    pub driver_version: String,
    pub acp_sdk_version: String,
    pub acp_protocol_version: u32,
    pub runtime_name: String,
    pub runtime_version: String,
    pub runtime_executable_digest: String,
    pub agent_capabilities: Value,
    pub max_concurrent_turns: u32,
    pub max_frame_bytes: usize,
    pub profile_digest: String,
    pub compatibility_digest: String,
    pub raw_initialize_result: Value,
    pub heartbeat_interval_ms: u64,
    pub state: PluginGenerationState,
    pub health: PluginGenerationHealth,
    pub started_at_ms: i64,
    pub last_heartbeat_at_ms: i64,
    pub stopped_at_ms: Option<i64>,
    pub stop_disposition: Option<PluginGenerationDisposition>,
    pub stop_reason: Option<String>,
    pub shutdown_outcome: Option<PluginShutdownOutcome>,
    pub shutdown_exit_code: Option<i32>,
}

/// Current operator-facing state of one durable agent seat.
///
/// Derived from evidence rather than stored, so unlike the enums the substrate
/// persists this needs no string codec: it is only ever serialised outward.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentSeatState {
    Unmanaged,
    Idle,
    Working,
    Interrupted,
    RecoveryRequired,
    Offline,
}

/// The exact durable evidence responsible for a projected seat state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentSeatReason {
    NoWorkerObserved,
    Ready,
    ReadySession,
    OpeningSession,
    ReservedInvocation,
    ActiveInvocation,
    ReservedGenerationUnavailable,
    ArmedGenerationUnavailable,
    ArmedLeaseExpired,
    UncertainSession,
    SessionWithoutWorker,
    GenerationStale,
    GenerationStopped,
}

/// Credential-free operational projection of one stable agent identity.
///
/// Joins the current worker generation, the native-session lane, the managed
/// invocation, and the delivery it came from, so an operator can see a seat that
/// needs recovery without correlating four endpoints by hand.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct AgentSeat {
    pub agent_id: String,
    pub state: AgentSeatState,
    pub reason: AgentSeatReason,
    pub generation_id: Option<String>,
    pub generation_health: Option<PluginGenerationHealth>,
    pub binding_id: Option<String>,
    pub binding_generation: Option<u64>,
    pub owner_epoch: Option<u64>,
    pub session_state: Option<SessionBindingState>,
    pub invocation_id: Option<String>,
    pub invocation_state: Option<InvocationState>,
    pub source_message_id: Option<String>,
    pub delivery_state: Option<DeliveryState>,
    pub lease_expires_at_ms: Option<i64>,
    pub lease_expired: bool,
    pub last_progress_at_ms: Option<i64>,
    pub unresolved_block_id: Option<i64>,
}

/// Which counter one observed harness update advances.
///
/// The wire carries finer classifications than the counters keep -- `tool_call`
/// and `tool_call_update` are two updates and one counter -- so the reduction
/// has to live somewhere. It lives here, beside the counters it names, because
/// two readers need it: the durable fold that increments a row, and a
/// trajectory sink that groups spans by the same vocabulary an operator already
/// reads in [`InvocationEventCounts`].
///
/// An unrecognized classification is [`EventClass::Unknown`] rather than an
/// error. A harness may emit an update this build has never seen, and losing
/// the count would be worse than counting it as unknown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventClass {
    Prompt,
    Assistant,
    Reasoning,
    Tool,
    Plan,
    Usage,
    Metadata,
    Permission,
    Unknown,
}

impl EventClass {
    /// Every variant, so a caller can enumerate the vocabulary without
    /// repeating it.
    pub const ALL: [Self; 9] = [
        Self::Prompt,
        Self::Assistant,
        Self::Reasoning,
        Self::Tool,
        Self::Plan,
        Self::Usage,
        Self::Metadata,
        Self::Permission,
        Self::Unknown,
    ];

    /// Reduces one wire classification to the counter it advances.
    #[must_use]
    pub fn parse(classification: &str) -> Self {
        match classification {
            "user_message_content" => Self::Prompt,
            "agent_message_content" => Self::Assistant,
            "reasoning_content" => Self::Reasoning,
            "tool_call" | "tool_call_update" => Self::Tool,
            "plan_update" => Self::Plan,
            "usage" => Self::Usage,
            "metadata" => Self::Metadata,
            "permission_request" => Self::Permission,
            _ => Self::Unknown,
        }
    }

    /// The counter's own name, which is also how an operator selects it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Assistant => "assistant",
            Self::Reasoning => "reasoning",
            Self::Tool => "tool",
            Self::Plan => "plan",
            Self::Usage => "usage",
            Self::Metadata => "metadata",
            Self::Permission => "permission",
            Self::Unknown => "unknown",
        }
    }
}

/// Fixed-size event counters retained for one managed invocation.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct InvocationEventCounts {
    /// The prompt Fleetd sent, echoed back by the harness. It is also where the
    /// envelope adapter's invocation id appears in a replayed transcript.
    pub prompt: u64,
    pub assistant: u64,
    pub reasoning: u64,
    pub tool: u64,
    pub plan: u64,
    pub usage: u64,
    pub metadata: u64,
    pub permission: u64,
    pub unknown: u64,
}

/// Bounded operational evidence for one managed invocation and its exact
/// source/result message relationship.
///
/// Raw update streams are represented by exact byte counts and a chain digest;
/// the bounded result message remains the transcript authority.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct InvocationObservation {
    pub invocation_id: String,
    pub agent_id: String,
    pub source_message_id: String,
    pub result_message_id: Option<String>,
    pub generation_id: String,
    pub binding_id: String,
    pub binding_generation: u64,
    pub owner_epoch: u64,
    pub started_at_ms: i64,
    pub updated_at_ms: i64,
    pub first_event_at_ms: Option<i64>,
    pub last_event_at_ms: Option<i64>,
    pub event_count: u64,
    pub observed_payload_bytes: u64,
    pub last_event_seq: u64,
    pub event_chain_digest: Option<String>,
    pub counts: InvocationEventCounts,
    pub terminal_at_ms: Option<i64>,
    pub stop_reason: Option<String>,
    pub runtime_stop_reason: Option<String>,
    pub execution_certainty: Option<ExecutionCertainty>,
    pub session_quiescent: Option<bool>,
    pub session_persistence: Option<SessionPersistence>,
    pub usage: Option<Value>,
}

/// Exclusive keyset position in a cursor-addressed evidence listing.
///
/// `changed_at_ms` is the last time durable evidence for a row changed, and
/// `id` breaks ties between rows that changed in the same millisecond. Both
/// halves are required: a millisecond alone cannot address a page boundary
/// that falls between two rows sharing it.
///
/// This is the paired form of two query parameters rather than a wire type of
/// its own. A listing stays a plain array, so a caller reads its next position
/// off the last row it received: `PluginGeneration::last_heartbeat_at_ms` with
/// `PluginGeneration::id`, or `InvocationObservation::updated_at_ms` with
/// `InvocationObservation::invocation_id`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EvidenceCursor {
    pub changed_at_ms: i64,
    pub id: String,
}

/// Which way a cursor-addressed evidence listing walks the change clock.
///
/// An operator reads `Newest` first. A collector archiving every row walks
/// `Oldest` from its last position, so rows appended or changed while it was
/// away arrive ahead of it rather than behind it.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceOrder {
    #[default]
    Newest,
    Oldest,
}

/// A census of durable delivery rows by state, plus the leases among them
/// whose window has already closed.
///
/// `inspected` is how many rows the census actually read, which a bounded
/// read model may cap below the true total.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct DeliveryCensus {
    pub inspected: usize,
    pub pending: usize,
    pub leased: usize,
    pub expired_leases: usize,
    pub blocked: usize,
    pub acknowledged: usize,
    pub dead: usize,
}

/// What a fleet is doing right now, as one durable read.
///
/// "Current" means the newest generation for each agent and the newest
/// generation of each session binding; older rows for the same key are
/// history, not health. Invocations are the ones still owed an outcome.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct FleetHealth {
    pub agent_id: Option<String>,
    pub current_plugin_generations: Vec<PluginGeneration>,
    pub current_session_bindings: Vec<SessionBinding>,
    pub active_invocations: Vec<Invocation>,
    pub deliveries: DeliveryCensus,
    pub delivery_records: Vec<DeliveryRecord>,
}

/// Exact operator trace joining one invocation to its bounded harness,
/// plugin-generation, native-session, and result evidence.
///
/// A reserved invocation carries none of it yet. Once an observation exists,
/// the session and plugin generation it names must still be readable, so a
/// present observation with an absent session is an integrity error rather
/// than an empty field.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct InvocationTrace {
    pub invocation: Invocation,
    pub observation: Option<InvocationObservation>,
    pub session: Option<SessionBinding>,
    pub plugin_generation: Option<PluginGeneration>,
    pub result: Option<Message>,
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        EventClass, InvocationEventCounts, PluginGenerationDisposition, PluginShutdownOutcome,
    };

    #[test]
    fn every_event_class_names_a_counter_field() {
        let counts =
            serde_json::to_value(InvocationEventCounts::default()).expect("serialize counters");
        let fields = counts.as_object().expect("counters are an object");
        assert_eq!(fields.len(), EventClass::ALL.len());
        for class in EventClass::ALL {
            assert!(
                fields.contains_key(class.as_str()),
                "EventClass::{class:?} spells `{}`, which is not a counter field. The \
                 vocabulary an operator selects by is the one the read model reports.",
                class.as_str()
            );
        }
    }

    #[test]
    fn an_unrecognized_classification_counts_as_unknown() {
        assert_eq!(
            EventClass::parse("user_message_content"),
            EventClass::Prompt
        );
        assert_eq!(
            EventClass::parse("reasoning_content"),
            EventClass::Reasoning
        );
        assert_eq!(EventClass::parse("tool_call"), EventClass::Tool);
        assert_eq!(EventClass::parse("tool_call_update"), EventClass::Tool);
        assert_eq!(
            EventClass::parse("a_kind_this_build_has_never_seen"),
            EventClass::Unknown
        );
    }

    #[test]
    fn stored_spelling_matches_the_wire_spelling() {
        for variant in PluginGenerationDisposition::ALL {
            assert_eq!(
                serde_json::to_value(variant).expect("serialize disposition"),
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(
                PluginGenerationDisposition::parse(variant.as_str()),
                Some(variant)
            );
        }
        for variant in PluginShutdownOutcome::ALL {
            assert_eq!(
                serde_json::to_value(variant).expect("serialize outcome"),
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(
                PluginShutdownOutcome::parse(variant.as_str()),
                Some(variant)
            );
        }
    }

    #[test]
    fn unreadable_values_do_not_parse() {
        assert_eq!(PluginGenerationDisposition::parse("Restart"), None);
        assert_eq!(PluginGenerationDisposition::parse("graceful"), None);
        assert_eq!(PluginShutdownOutcome::parse("Forced"), None);
        assert_eq!(PluginShutdownOutcome::parse(""), None);
    }

    #[test]
    fn all_lists_every_variant() {
        // Adding a variant makes these matches non-exhaustive, and the counts
        // below then fail until `ALL` learns about it too.
        for variant in PluginGenerationDisposition::ALL {
            match variant {
                PluginGenerationDisposition::Stopped
                | PluginGenerationDisposition::Restart
                | PluginGenerationDisposition::Fatal => {}
            }
        }
        for variant in PluginShutdownOutcome::ALL {
            match variant {
                PluginShutdownOutcome::Graceful
                | PluginShutdownOutcome::Forced
                | PluginShutdownOutcome::Failed => {}
            }
        }
        assert_eq!(PluginGenerationDisposition::ALL.len(), 3);
        assert_eq!(PluginShutdownOutcome::ALL.len(), 3);
    }
}
