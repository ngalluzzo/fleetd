//! Bounded operator read models for plugin generations and managed turns.
//!
//! Raw harness update streams are represented by exact byte counts and a chain
//! digest; the bounded result message remains the transcript authority.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::{harness_acp::SessionPersistence, model::ExecutionCertainty};

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

/// What Fleetd observed while terminating a plugin process group.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginShutdownOutcome {
    Graceful,
    Forced,
    Failed,
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

/// Fixed-size event counters retained for one managed invocation.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct InvocationEventCounts {
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
