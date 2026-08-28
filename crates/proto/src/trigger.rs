//! Wire types for inbound triggers.
//!
//! A trigger is a thing that creates work with no human present and no
//! invocation to scope to: a recurring job, a webhook receiver, a file watcher.
//! [ADR 0031](../../../docs/adr/0031-inbound-triggers.md) gives it standing but
//! narrow authority, in the shape ADR 0016 established for peer messaging.
//!
//! The division these types encode is the whole decision. A registration is
//! what an operator declares and Fleetd remembers; an occurrence is the little
//! a trigger may choose when it fires. Everything that establishes identity is
//! on the registration side, so a trigger cannot name a sender, reach another
//! channel, or create a kind it never declared.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

use crate::model::IssuedCredential;

/// Whether a trigger may still create work.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum TriggerState {
    Active,
    Retired,
}

impl TriggerState {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 2] = [Self::Active, Self::Retired];

    /// Returns the exact stored representation of this variant.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Retired => "retired",
        }
    }

    /// Reads back the representation `as_str` produced.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|variant| variant.as_str() == value)
    }
}

/// What an operator declares when registering a trigger.
///
/// `sender_id` names an existing agent rather than introducing a principal of
/// its own. A trigger's messages are attributable to a durable identity that
/// already exists, and the kernel's six concepts are unchanged; what is new is
/// the credential, which may do this and nothing else.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct RegisterTrigger {
    pub name: String,
    pub channel_id: String,
    pub sender_id: String,
    /// The exact kinds this trigger may create. Non-empty, deduplicated, and
    /// part of the trigger's identity: changing it changes what the trigger is.
    pub accepted_kinds: Vec<String>,
}

/// One registered trigger, as an operator reads it.
///
/// The firing history is the reason to register a trigger at all. A crontab
/// entry that stopped firing on Tuesday leaves no trace, and an idle fleet looks
/// exactly like a healthy quiet one; `last_fired_at_ms` is what turns that
/// absence into a fact.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct Trigger {
    pub id: String,
    pub name: String,
    pub channel_id: String,
    pub sender_id: String,
    pub accepted_kinds: Vec<String>,
    pub state: TriggerState,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// The last occurrence Fleetd accepted from this trigger, and when.
    pub last_occurrence_id: Option<String>,
    pub last_fired_at_ms: Option<i64>,
    pub accepted_occurrences: u64,
    pub retired_at_ms: Option<i64>,
    pub retired_reason: Option<String>,
}

/// Why an operator is ending a trigger's standing grant.
///
/// Required rather than optional. A retired trigger with no reason recorded
/// leaves the next operator guessing whether it was decommissioned or switched
/// off in an incident, which is exactly the question the record exists to
/// answer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct RetireTrigger {
    pub reason: String,
}

/// A trigger registration and its one-time credential response.
///
/// Registering and being able to fire are one act. A trigger holding no
/// credential is inert, and a credential naming a trigger that does not exist is
/// authority over nothing, so neither is a state fleetd will hand back.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct RegisteredTrigger {
    pub trigger: Trigger,
    pub credential: IssuedCredential,
}

/// What a trigger supplies when it fires.
///
/// Deliberately small. Sender, channel, correlation, causation, and the durable
/// idempotency key are all derived from the registration, so this carries only
/// what the trigger actually knows: which firing this is, who should act, and
/// what to say.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct TriggerOccurrence {
    /// The trigger's own name for this firing. Fleetd derives the durable
    /// idempotency key from the trigger and this together, so a repeat is
    /// absorbed exactly and two triggers cannot collide on one key.
    pub occurrence_id: String,
    pub recipient_id: String,
    pub kind: String,
    pub payload: Value,
}

/// What Fleetd answers when a trigger fires.
///
/// `created` distinguishes a firing that produced work from one Fleetd
/// recognised as a repeat. An unattended scheduler cannot tell the difference on
/// its own, and the difference is the whole reason idempotency lives here.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct TriggerFired {
    pub trigger_id: String,
    pub occurrence_id: String,
    pub message_id: String,
    pub created: bool,
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::TriggerState;

    #[test]
    fn stored_spelling_matches_the_wire_spelling() {
        for variant in TriggerState::ALL {
            assert_eq!(
                serde_json::to_value(variant).expect("serialize trigger state"),
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(TriggerState::parse(variant.as_str()), Some(variant));
        }
    }

    #[test]
    fn unreadable_values_do_not_parse() {
        assert_eq!(TriggerState::parse("Active"), None);
        assert_eq!(TriggerState::parse("paused"), None);
        assert_eq!(TriggerState::parse(""), None);
    }
}
