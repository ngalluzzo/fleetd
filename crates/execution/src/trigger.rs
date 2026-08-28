//! What an inbound trigger's standing authority permits.
//!
//! The shape mirrors `message_grant`, and the difference is where the authority
//! lives. An invocation grant is armed in memory for one turn; a trigger's grant
//! is a durable registration, so every firing reads it. What that buys is the
//! thing [ADR 0031](../../../docs/adr/0031-inbound-triggers.md) exists for: the
//! authority is a row an operator can read, retire, and find in the record
//! afterwards.
//!
//! Fleetd derives sender, channel, correlation, causation, and the durable
//! idempotency key from the registration. A trigger chooses only the recipient,
//! a kind from its declared set, an opaque payload, and a name for this firing.
//!
//! Nothing here interprets what made the trigger fire. A cron expression, a
//! webhook body, a filesystem event: all of them arrive as an occurrence that
//! already happened.

use sha2::{Digest, Sha256};

use fleetd_kernel::{
    error::FleetError,
    store::{
        Store,
        message::append_message_in_transaction,
        now_ms,
        trigger::{record_trigger_occurrence, trigger_in_transaction},
    },
};
use fleetd_proto::{
    model::CreateMessage,
    trigger::{TriggerFired, TriggerOccurrence, TriggerState},
};

const MAX_OCCURRENCE_ID_BYTES: usize = 128;
const MAX_AGENT_ID_BYTES: usize = 256;
const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

/// Creates the work one occurrence asks for, under the trigger's registration.
///
/// The registration is read, checked, and acted on inside one immediate
/// transaction, so a retirement racing a firing either wins outright or loses
/// outright, and the message and the record of it commit together.
///
/// Repeating an occurrence is absorbed exactly: the durable key is derived from
/// the trigger and the occurrence together, so a scheduler that double-fires
/// creates one piece of work and is told which of the two calls made it. That is
/// the one thing a trigger inside fleetd does better than a crontab line, which
/// has to construct a distinct key itself and silently creates nothing when it
/// gets that wrong.
///
/// # Errors
///
/// Returns not found for an unknown trigger, a conflict for a retired one,
/// forbidden for a kind the trigger never declared, invalid for out-of-bounds
/// input, or a persistence failure.
pub async fn fire(
    store: &Store,
    trigger_id: &str,
    occurrence: TriggerOccurrence,
) -> Result<TriggerFired, FleetError> {
    validate_occurrence(&occurrence)?;
    let idempotency_key = durable_key(trigger_id, &occurrence.occurrence_id);
    let mut transaction = store.begin_immediate().await?;
    let trigger = trigger_in_transaction(&mut transaction, trigger_id).await?;
    if trigger.state != TriggerState::Active {
        return Err(FleetError::Conflict(format!(
            "trigger {trigger_id} is retired and may not create work"
        )));
    }
    if !trigger.accepted_kinds.contains(&occurrence.kind) {
        return Err(FleetError::Forbidden(format!(
            "trigger {} did not declare the message kind {}",
            trigger.name, occurrence.kind
        )));
    }
    if occurrence.recipient_id == trigger.sender_id {
        return Err(FleetError::Invalid(
            "recipient_id must identify a peer, not the trigger's own sender".to_owned(),
        ));
    }

    let appended = append_message_in_transaction(
        &mut transaction,
        &trigger.channel_id,
        CreateMessage {
            sender_id: trigger.sender_id.clone(),
            idempotency_key: Some(idempotency_key),
            recipient_id: Some(occurrence.recipient_id),
            kind: occurrence.kind,
            payload: occurrence.payload,
            // A firing has no message behind it, so the work it creates is the
            // root of its own trace rather than a continuation of someone
            // else's. Deriving that is still deriving it: a trigger cannot
            // attach its work to a conversation it did not start.
            correlation_id: None,
            causation_id: None,
        },
    )
    .await?;

    if appended.created
        && !record_trigger_occurrence(
            &mut transaction,
            trigger_id,
            &occurrence.occurrence_id,
            now_ms(),
        )
        .await?
    {
        return Err(FleetError::Conflict(format!(
            "trigger {trigger_id} was retired while it was firing"
        )));
    }
    transaction.commit().await?;
    store.notify_message_commit(appended.created);

    Ok(TriggerFired {
        trigger_id: trigger_id.to_owned(),
        occurrence_id: occurrence.occurrence_id,
        message_id: appended.message.id,
        created: appended.created,
    })
}

/// Derives the durable key from the trigger and the occurrence together.
///
/// Both halves are load-bearing. Without the trigger, two triggers sharing a
/// sender collide on an occurrence name neither of them chose to coordinate;
/// without the occurrence, every firing is the same firing.
fn durable_key(trigger_id: &str, occurrence_id: &str) -> String {
    let occurrence_digest = Sha256::digest(occurrence_id.as_bytes());
    format!("trigger:{trigger_id}:occurrence:{occurrence_digest:x}")
}

fn validate_occurrence(occurrence: &TriggerOccurrence) -> Result<(), FleetError> {
    if occurrence.occurrence_id.trim().is_empty()
        || occurrence.occurrence_id.len() > MAX_OCCURRENCE_ID_BYTES
    {
        return Err(FleetError::Invalid(format!(
            "occurrence_id must contain between 1 and {MAX_OCCURRENCE_ID_BYTES} bytes"
        )));
    }
    if occurrence.recipient_id.trim().is_empty()
        || occurrence.recipient_id.len() > MAX_AGENT_ID_BYTES
    {
        return Err(FleetError::Invalid(format!(
            "recipient_id must contain between 1 and {MAX_AGENT_ID_BYTES} bytes"
        )));
    }
    let payload_bytes = serde_json::to_vec(&occurrence.payload)?;
    if payload_bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(FleetError::Invalid(format!(
            "payload must not exceed {MAX_PAYLOAD_BYTES} encoded bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::durable_key;

    /// Two triggers may share a sender, and an occurrence name is a trigger's
    /// own vocabulary. "nightly-2026-08-27" from two schedulers is two firings.
    #[test]
    fn the_durable_key_separates_triggers_sharing_an_occurrence_name() {
        assert_ne!(
            durable_key("trigger-a", "nightly-2026-08-27"),
            durable_key("trigger-b", "nightly-2026-08-27")
        );
        assert_ne!(
            durable_key("trigger-a", "nightly-2026-08-27"),
            durable_key("trigger-a", "nightly-2026-08-28")
        );
        assert_eq!(
            durable_key("trigger-a", "nightly-2026-08-27"),
            durable_key("trigger-a", "nightly-2026-08-27")
        );
    }

    /// The occurrence is hashed, so a trigger cannot steer the key's shape with a
    /// name full of separators, and cannot reach a key belonging to another
    /// trigger by spelling one out.
    #[test]
    fn an_occurrence_name_cannot_shape_the_key() {
        let plain = durable_key("trigger-a", "nightly");
        let hostile = durable_key("trigger-a", "trigger-b:occurrence:nightly");
        assert_eq!(plain.len(), hostile.len());
        assert_eq!(plain.matches(':').count(), hostile.matches(':').count());
        for key in [&plain, &hostile] {
            assert!(key.starts_with("trigger:trigger-a:occurrence:"));
        }
    }
}
