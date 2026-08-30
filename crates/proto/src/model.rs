use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;

/// An addressable participant in the fleet.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct Agent {
    pub id: String,
    pub name: String,
    pub metadata: Value,
    pub created_at_ms: i64,
}

/// Input for registering an agent.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CreateAgent {
    pub name: String,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

/// A newly issued credential. Its token is returned once and never persisted
/// in plaintext by fleetd.
#[derive(Deserialize, Serialize, ToSchema)]
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
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct RegisteredAgent {
    pub agent: Agent,
    pub credential: IssuedCredential,
}

/// The durable conversation lifecycle selected by fleetd.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConversationKind {
    /// A named conversation whose permanent membership may grow.
    #[default]
    Shared,
    /// A one-to-one conversation with one immutable exact participant pair.
    Direct,
}

impl ConversationKind {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 2] = [Self::Shared, Self::Direct];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Direct => "direct",
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

/// A durable conversation shared by a bounded set of agents.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct Channel {
    pub id: String,
    pub name: String,
    pub kind: ConversationKind,
    pub metadata: Value,
    pub created_at_ms: i64,
    pub archived_at_ms: Option<i64>,
}

/// Input for creating a shared channel and its initial membership.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateChannel {
    pub name: String,
    #[serde(default = "empty_object")]
    pub metadata: Value,
    #[serde(default)]
    pub member_ids: Vec<String>,
    #[serde(default)]
    pub members: Vec<CreateChannelMember>,
}

/// Whether one channel membership receives leased inbox work.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MembershipDeliveryMode {
    /// Snapshot addressed and broadcast messages into the durable inbox.
    #[default]
    Inbox,
    /// Retain history and live visibility without creating inbox work.
    StreamOnly,
}

impl MembershipDeliveryMode {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 2] = [Self::Inbox, Self::StreamOnly];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::StreamOnly => "stream_only",
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

/// Exact initial membership used by the durable store.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CreateChannelMember {
    pub agent_id: String,
    pub delivery_mode: MembershipDeliveryMode,
}

/// Bounded public membership projection without opaque agent metadata.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct ChannelMember {
    pub channel_id: String,
    pub agent_id: String,
    pub agent_name: String,
    pub joined_at_ms: i64,
    pub delivery_mode: MembershipDeliveryMode,
}

/// Input for adding one agent to a channel.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AddMember {
    pub agent_id: String,
    #[serde(default)]
    pub delivery_mode: MembershipDeliveryMode,
}

/// Input for idempotently opening a one-to-one direct conversation.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct OpenDirectConversation {
    /// Exactly two distinct participants. Their delivery modes become
    /// immutable with the direct conversation.
    pub members: Vec<CreateChannelMember>,
}

/// Input for renaming a shared channel.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RenameChannel {
    pub name: String,
}

/// One conversation with bounded participant and recency metadata for clients.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct ConversationSummary {
    pub id: String,
    pub name: String,
    pub kind: ConversationKind,
    pub metadata: Value,
    pub created_at_ms: i64,
    pub archived_at_ms: Option<i64>,
    pub members: Vec<ChannelMember>,
    pub latest_message_seq: Option<i64>,
    pub latest_message_at_ms: Option<i64>,
}

/// Exact unread state for one authenticated participant in one conversation.
///
/// The projection is derived only from the participant's durable membership
/// cursor and immutable message envelopes. The participant's own messages are
/// not unread. `addressed_unread_count` counts messages from another
/// participant whose exact `recipient_id` is the reader; it does not infer
/// urgency from message content.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct ConversationAttention {
    pub channel_id: String,
    pub read_through_seq: i64,
    pub latest_message_seq: Option<i64>,
    pub unread_count: i64,
    pub addressed_unread_count: i64,
    pub first_unread_seq: Option<i64>,
    pub first_addressed_unread_seq: Option<i64>,
}

/// Advances the authenticated participant's durable read cursor.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct AdvanceConversationRead {
    /// Highest channel message sequence the participant has observed.
    #[schema(minimum = 0)]
    pub through_seq: i64,
}

/// An immutable message envelope in the global event sequence.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
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
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CreateMessage {
    pub sender_id: String,
    pub idempotency_key: Option<String>,
    pub recipient_id: Option<String>,
    #[serde(default = "default_message_kind")]
    pub kind: String,
    pub payload: Value,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

/// Authenticated input for sending a message. The server supplies `sender_id`
/// from the bound agent credential.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SendMessage {
    pub idempotency_key: Option<String>,
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
            idempotency_key: self.idempotency_key,
            recipient_id: self.recipient_id,
            kind: self.kind,
            payload: self.payload,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
        }
    }
}

/// A cursor-addressed page from a channel's message history.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct MessagePage {
    pub messages: Vec<Message>,
    pub next_cursor: i64,
}

/// Input for atomically leasing work from an agent inbox.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ClaimDeliveries {
    #[serde(default = "default_claim_limit")]
    #[schema(default = 1, minimum = 1, maximum = 100)]
    pub limit: u32,
    #[serde(default = "default_lease_duration_ms")]
    #[schema(default = 300_000, minimum = 1, maximum = 3_600_000)]
    pub lease_duration_ms: u64,
}

/// One leased inbox entry and the immutable message it carries.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct Delivery {
    pub message: Message,
    pub attempt: i64,
    pub lease_expires_at_ms: i64,
    pub last_error: Option<String>,
}

/// A set of deliveries owned by one expiring lease token.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ClaimBatch {
    pub lease_token: String,
    pub lease_expires_at_ms: i64,
    pub deliveries: Vec<Delivery>,
}

/// Input for acknowledging a successfully processed delivery.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct AckDelivery {
    pub lease_token: String,
}

/// Input for releasing a failed delivery for a later attempt.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct RetryDelivery {
    pub lease_token: String,
    #[serde(default)]
    #[schema(default = 0, maximum = 86_400_000)]
    pub retry_after_ms: u64,
    pub error: Option<String>,
}

/// Input for parking an ambiguously executed delivery under its active lease.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct BlockDelivery {
    pub lease_token: String,
    pub reason: String,
}

/// One unresolved blocked delivery and its immutable source message.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct BlockedDelivery {
    pub block_id: i64,
    pub agent_id: String,
    pub message: Message,
    pub attempt: i64,
    pub reason: String,
    pub blocked_at_ms: i64,
}

/// Operator decision for a blocked delivery.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BlockResolution {
    Requeue,
    Abandon,
}

/// Input for resolving one exact blocked-delivery record.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct ResolveDeliveryBlock {
    pub resolution: BlockResolution,
    #[serde(default)]
    #[schema(default = 0, maximum = 86_400_000)]
    pub retry_after_ms: u64,
    pub note: Option<String>,
}

/// Durable lifecycle state for one managed delivery attempt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum InvocationState {
    Reserved,
    DispatchArmed,
    Terminal,
}

impl InvocationState {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 3] = [Self::Reserved, Self::DispatchArmed, Self::Terminal];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::DispatchArmed => "dispatch_armed",
            Self::Terminal => "terminal",
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
            .find(|variant| variant.as_str() == value)
            .cloned()
    }
}

/// Durable inbox state exposed to the operator without lease credentials.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryState {
    Pending,
    Leased,
    Blocked,
    Acknowledged,
    Dead,
}

impl DeliveryState {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 5] = [
        Self::Pending,
        Self::Leased,
        Self::Blocked,
        Self::Acknowledged,
        Self::Dead,
    ];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Leased => "leased",
            Self::Blocked => "blocked",
            Self::Acknowledged => "acknowledged",
            Self::Dead => "dead",
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

/// Read-only operator projection of one durable delivery.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct DeliveryRecord {
    pub agent_id: String,
    pub message: Message,
    pub state: DeliveryState,
    pub attempt: i64,
    pub available_at_ms: i64,
    pub lease_expires_at_ms: Option<i64>,
    pub lease_expired: bool,
    pub last_error: Option<String>,
    pub created_at_ms: i64,
    pub acknowledged_at_ms: Option<i64>,
    pub unresolved_block_id: Option<i64>,
}

/// What fleetd can prove about an invocation's external execution.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionCertainty {
    NotStarted,
    OutcomeKnown,
    OutcomeUnknown,
}

impl ExecutionCertainty {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 3] = [Self::NotStarted, Self::OutcomeKnown, Self::OutcomeUnknown];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::OutcomeKnown => "outcome_known",
            Self::OutcomeUnknown => "outcome_unknown",
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
            .find(|variant| variant.as_str() == value)
            .cloned()
    }
}

/// One durable managed attempt reserved together with its inbox lease.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct Invocation {
    pub id: String,
    pub agent_id: String,
    pub message: Message,
    pub delivery_attempt: i64,
    pub lease_token: String,
    pub lease_expires_at_ms: i64,
    pub fence_token: String,
    pub state: InvocationState,
    pub reserved_at_ms: i64,
    pub dispatch_armed_at_ms: Option<i64>,
    pub terminal_at_ms: Option<i64>,
    pub execution_certainty: Option<ExecutionCertainty>,
    pub terminal_reason: Option<String>,
    pub result_message_id: Option<String>,
}

/// A batch of delivery attempts atomically leased and durably reserved.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct InvocationBatch {
    pub invocations: Vec<Invocation>,
}

/// Input for the write-ahead fence immediately before effectful dispatch.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ArmInvocation {
    pub lease_token: String,
    pub fence_token: String,
}

/// Input for atomically publishing one result and acknowledging its invocation.
#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CompleteInvocation {
    pub lease_token: String,
    pub fence_token: String,
    #[serde(default = "default_message_kind")]
    pub kind: String,
    pub payload: Value,
}

/// Durable result of completing one managed invocation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct InvocationCompletion {
    pub invocation: Invocation,
    pub result: Message,
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

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{
        ConversationKind, DeliveryState, ExecutionCertainty, InvocationState,
        MembershipDeliveryMode,
    };

    #[test]
    fn stored_spelling_matches_the_wire_spelling() {
        for variant in ExecutionCertainty::ALL {
            let wire = serde_json::to_value(&variant).expect("serialize certainty");
            assert_eq!(
                wire,
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(ExecutionCertainty::parse(variant.as_str()), Some(variant));
        }
        for variant in ConversationKind::ALL {
            assert_eq!(
                serde_json::to_value(variant).expect("serialize kind"),
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(ConversationKind::parse(variant.as_str()), Some(variant));
        }
        for variant in InvocationState::ALL {
            assert_eq!(
                serde_json::to_value(&variant).expect("serialize invocation state"),
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(InvocationState::parse(variant.as_str()), Some(variant));
        }
        for variant in DeliveryState::ALL {
            assert_eq!(
                serde_json::to_value(variant).expect("serialize delivery state"),
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(DeliveryState::parse(variant.as_str()), Some(variant));
        }
        for variant in MembershipDeliveryMode::ALL {
            assert_eq!(
                serde_json::to_value(variant).expect("serialize delivery mode"),
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(
                MembershipDeliveryMode::parse(variant.as_str()),
                Some(variant)
            );
        }
    }

    #[test]
    fn unreadable_values_do_not_parse() {
        assert_eq!(ExecutionCertainty::parse("outcome_maybe"), None);
        assert_eq!(ExecutionCertainty::parse("NotStarted"), None);
        assert_eq!(ExecutionCertainty::parse(""), None);
        assert_eq!(ConversationKind::parse("Shared"), None);
        assert_eq!(ConversationKind::parse("group"), None);
        assert_eq!(MembershipDeliveryMode::parse("streamonly"), None);
        assert_eq!(MembershipDeliveryMode::parse("Inbox"), None);
        assert_eq!(DeliveryState::parse("Leased"), None);
        assert_eq!(DeliveryState::parse("expired"), None);
        assert_eq!(InvocationState::parse("armed"), None);
    }

    #[test]
    fn all_lists_every_variant() {
        // Adding a variant makes this match non-exhaustive, and the count below
        // then fails until `ALL` learns about it too.
        for variant in ExecutionCertainty::ALL {
            match variant {
                ExecutionCertainty::NotStarted
                | ExecutionCertainty::OutcomeKnown
                | ExecutionCertainty::OutcomeUnknown => {}
            }
        }
        assert_eq!(ExecutionCertainty::ALL.len(), 3);
        for variant in ConversationKind::ALL {
            match variant {
                ConversationKind::Shared | ConversationKind::Direct => {}
            }
        }
        assert_eq!(ConversationKind::ALL.len(), 2);
        for variant in MembershipDeliveryMode::ALL {
            match variant {
                MembershipDeliveryMode::Inbox | MembershipDeliveryMode::StreamOnly => {}
            }
        }
        assert_eq!(MembershipDeliveryMode::ALL.len(), 2);
        for variant in DeliveryState::ALL {
            match variant {
                DeliveryState::Pending
                | DeliveryState::Leased
                | DeliveryState::Blocked
                | DeliveryState::Acknowledged
                | DeliveryState::Dead => {}
            }
        }
        assert_eq!(DeliveryState::ALL.len(), 5);
        for variant in InvocationState::ALL {
            match variant {
                InvocationState::Reserved
                | InvocationState::DispatchArmed
                | InvocationState::Terminal => {}
            }
        }
        assert_eq!(InvocationState::ALL.len(), 3);
    }
}
