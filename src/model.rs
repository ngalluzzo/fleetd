use std::fmt;

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

/// A newly issued credential. Its token is returned once and never persisted
/// in plaintext by fleetd.
#[derive(Deserialize, Serialize)]
pub struct IssuedCredential {
    pub id: String,
    pub token: String,
    pub created_at_ms: i64,
}

impl fmt::Debug for IssuedCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedCredential")
            .field("id", &self.id)
            .field("token", &"[REDACTED]")
            .field("created_at_ms", &self.created_at_ms)
            .finish()
    }
}

/// An agent registration and its one-time credential response.
#[derive(Debug, Deserialize, Serialize)]
pub struct RegisteredAgent {
    pub agent: Agent,
    pub credential: IssuedCredential,
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

/// Authenticated input for sending a message. The server supplies `sender_id`
/// from the bound agent credential.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SendMessage {
    pub recipient_id: Option<String>,
    #[serde(default = "default_message_kind")]
    pub kind: String,
    pub payload: Value,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl SendMessage {
    /// Attributes this wire input to an authenticated agent.
    #[must_use]
    pub fn attributed_to(self, sender_id: impl Into<String>) -> CreateMessage {
        CreateMessage {
            sender_id: sender_id.into(),
            recipient_id: self.recipient_id,
            kind: self.kind,
            payload: self.payload,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
        }
    }
}

/// A cursor-addressed page from a channel's message history.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MessagePage {
    pub messages: Vec<Message>,
    pub next_cursor: i64,
}

/// Input for atomically leasing work from an agent inbox.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClaimDeliveries {
    #[serde(default = "default_claim_limit")]
    pub limit: u32,
    #[serde(default = "default_lease_duration_ms")]
    pub lease_duration_ms: u64,
}

/// One leased inbox entry and the immutable message it carries.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Delivery {
    pub message: Message,
    pub attempt: i64,
    pub lease_expires_at_ms: i64,
    pub last_error: Option<String>,
}

/// A set of deliveries owned by one expiring lease token.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ClaimBatch {
    pub lease_token: String,
    pub lease_expires_at_ms: i64,
    pub deliveries: Vec<Delivery>,
}

/// Input for acknowledging a successfully processed delivery.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AckDelivery {
    pub lease_token: String,
}

/// Input for releasing a failed delivery for a later attempt.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RetryDelivery {
    pub lease_token: String,
    #[serde(default)]
    pub retry_after_ms: u64,
    pub error: Option<String>,
}

fn empty_object() -> Value {
    json!({})
}

fn default_message_kind() -> String {
    "text".to_owned()
}

const fn default_claim_limit() -> u32 {
    1
}

const fn default_lease_duration_ms() -> u64 {
    300_000
}
