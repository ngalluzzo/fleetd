//! Credential-free draft wire between the workflow runner and author-review plugin.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const INTERFACE_ID: &str = "fleetd.workflow-draft";
pub const INTERFACE_VERSION: &str = "0.0.1";
pub const PLUGIN_ID: &str = "fleetd.workflow.author-review";
pub const PLUGIN_VERSION: &str = "0.0.1";
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_HISTORY_MESSAGES: usize = 10_000;
pub const MAX_MEMBERS: usize = 256;
pub const MAX_PROPOSALS: usize = 32;
pub const MIN_REVISION_ROUNDS: u32 = 0;
pub const MAX_REVISION_ROUNDS: u32 = 8;

pub const WORK_REQUESTED: &str = "work.requested";
pub const ARTIFACT_PROPOSED: &str = "artifact.proposed";
pub const REVIEW_REQUESTED: &str = "review.requested";
pub const REVIEW_COMPLETED: &str = "review.completed";
pub const REVISION_REQUESTED: &str = "revision.requested";
pub const WORK_COMPLETED: &str = "work.completed";
pub const WORK_BLOCKED: &str = "work.blocked";

pub const EVENT_KINDS: [&str; 7] = [
    WORK_REQUESTED,
    ARTIFACT_PROPOSED,
    REVIEW_REQUESTED,
    REVIEW_COMPLETED,
    REVISION_REQUESTED,
    WORK_COMPLETED,
    WORK_BLOCKED,
];

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub id: u64,
    pub method: String,
    pub params: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeResult {
    pub interface_id: String,
    pub interface_version: String,
    pub plugin_id: String,
    pub plugin_version: String,
    pub roles: Vec<String>,
    pub event_schemas: Vec<EventSchema>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EventSchema {
    pub kind: String,
    pub schema: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluateParams {
    pub configuration: Value,
    pub runner_agent_id: String,
    pub workflow_id: String,
    pub input: WorkflowMessage,
    pub history: Vec<WorkflowMessage>,
    pub members: Vec<WorkflowMember>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluateResult {
    pub projection: Value,
    pub proposals: Vec<ProposedMessage>,
}

/// A bounded effect proposal. The runner derives sender, channel, correlation,
/// causation, and durable idempotency from the leased input.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProposedMessage {
    pub operation_id: String,
    pub recipient_id: String,
    pub kind: String,
    pub payload: Value,
}

/// Credential-free copy of Fleetd's immutable public envelope.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowMessage {
    pub seq: i64,
    pub id: String,
    pub channel_id: String,
    pub sender_id: String,
    pub recipient_id: Option<String>,
    pub kind: String,
    pub payload: Value,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub created_at_ms: i64,
}

/// Credential-free public membership projection supplied by the runner.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowMember {
    pub agent_id: String,
    pub agent_name: String,
    pub delivery_mode: String,
    pub joined_at_ms: i64,
}

impl RpcResponse {
    #[must_use]
    pub fn success(id: u64, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id,
            result: Some(result),
            error: None,
        }
    }

    #[must_use]
    pub fn failure(id: u64, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id,
            result: None,
            error: Some(RpcError {
                code,
                message: message.into(),
            }),
        }
    }
}
