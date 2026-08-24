use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// An addressable participant in the fleet.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub metadata: Value,
    pub created_at_ms: i64,
}

/// Input for registering an agent.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateAgent {
    pub name: String,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

/// A durable conversation shared by a bounded set of agents.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Channel {
    pub id: String,
    pub name: String,
    pub metadata: Value,
    pub created_at_ms: i64,
}

/// Input for creating a channel and its initial membership.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateChannel {
    pub name: String,
    #[serde(default = "empty_object")]
    pub metadata: Value,
    #[serde(default)]
    pub member_ids: Vec<String>,
}

/// Input for adding one agent to a channel.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AddMember {
    pub agent_id: String,
}

/// An immutable message envelope in the global event sequence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Message {
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

/// Input for appending a message to a channel.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CreateMessage {
    pub sender_id: String,
    pub recipient_id: Option<String>,
    #[serde(default = "default_message_kind")]
    pub kind: String,
    pub payload: Value,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

/// A cursor-addressed page from a channel's message history.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessagePage {
    pub messages: Vec<Message>,
    pub next_cursor: i64,
}

fn empty_object() -> Value {
    json!({})
}

fn default_message_kind() -> String {
    "text".to_owned()
}
