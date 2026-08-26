//! Durable harness session lanes and their ownership fences.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::harness_acp::{Binding, SessionPersistence};

/// Durable lifecycle state for one native harness session generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionBindingState {
    Opening,
    Ready,
    Active,
    Uncertain,
    Retired,
}

/// Exact desired lane and runtime compatibility used to acquire ownership.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AcquireSessionBinding {
    pub lane_policy: String,
    pub lane_key: String,
    pub owner_instance_id: String,
    pub profile_digest: String,
    pub compatibility_digest: String,
    pub working_directory: String,
    #[serde(default)]
    pub additional_directories: Vec<String>,
}

/// Harness operation required after durable lane acquisition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionAcquisitionMode {
    Create,
    Resume { session_ref: String },
}

/// One durable native-session generation and its current owner fence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct SessionBinding {
    pub binding: Binding,
    pub agent_id: String,
    pub lane_policy: String,
    pub lane_key: String,
    pub owner_instance_id: String,
    pub profile_digest: String,
    pub compatibility_digest: String,
    pub working_directory: String,
    pub additional_directories: Vec<String>,
    pub session_ref: Option<String>,
    pub state: SessionBindingState,
    pub active_invocation_id: Option<String>,
    pub last_quiescent_invocation_id: Option<String>,
    pub session_persistence: Option<SessionPersistence>,
    pub uncertain_reason: Option<String>,
    pub retired_reason: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub opened_at_ms: Option<i64>,
    pub retired_at_ms: Option<i64>,
}

/// Result of acquiring one logical session lane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionAcquisition {
    pub session: SessionBinding,
    pub mode: SessionAcquisitionMode,
}
