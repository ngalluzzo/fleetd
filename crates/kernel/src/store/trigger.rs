//! Durable registration for a thing that creates work on its own.
//!
//! What a trigger may do is stored here rather than supplied when it fires, so
//! "narrow" is a property of the row instead of a promise the caller keeps.
//!
//! Reads are `Store` methods. Writes are transaction-scoped functions instead,
//! because each one is half of something: registering a trigger and issuing the
//! credential that lets it fire are one act, and so are creating work and
//! recording that the trigger created it. The callers that compose those halves
//! live in `auth::trigger` and in the execution layer.

use sqlx::Row;
use uuid::Uuid;

use crate::error::FleetError;
use fleetd_proto::trigger::{RegisterTrigger, Trigger, TriggerState};

use super::{Store, map_unique_conflict, validate_name};

/// A trigger declaring nothing could create nothing, and one declaring the world
/// is not narrow. Both are configuration mistakes worth refusing at the door.
const MAX_ACCEPTED_KINDS: usize = 32;
const MAX_KIND_BYTES: usize = 256;

impl Store {
    /// Reads one registration by id.
    ///
    /// # Errors
    ///
    /// Returns not found for an unknown trigger, or a decoding error for an
    /// unreadable stored row.
    pub async fn get_trigger(&self, trigger_id: &str) -> Result<Trigger, FleetError> {
        let row = sqlx::query(&format!("{} WHERE id = ?", trigger_select()))
            .bind(trigger_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| FleetError::NotFound {
                entity: "trigger",
                id: trigger_id.to_owned(),
            })?;
        trigger_from_row(&row)
    }

    /// Lists registrations in creation order, optionally scoped to one channel.
    ///
    /// Creation order rather than recent activity, matching every other listing
    /// the kernel exposes: each row already carries `last_fired_at_ms`, so a
    /// surface that wants to rank by activity can, while the durable order stays
    /// stable across reads.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored rows cannot be read or decoded.
    pub async fn list_triggers(
        &self,
        channel_id: Option<&str>,
    ) -> Result<Vec<Trigger>, FleetError> {
        let rows = match channel_id {
            Some(channel_id) => {
                sqlx::query(&format!(
                    "{} WHERE channel_id = ? ORDER BY created_at_ms, id",
                    trigger_select()
                ))
                .bind(channel_id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query(&format!("{} ORDER BY created_at_ms, id", trigger_select()))
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.iter().map(trigger_from_row).collect()
    }
}

/// Builds the registration a caller asked for, refusing the shapes that would
/// make "narrow" meaningless.
///
/// Separate from persisting it so the whole declaration is checked before a
/// transaction is opened around it.
pub(crate) fn new_trigger(input: RegisterTrigger, now_ms: i64) -> Result<Trigger, FleetError> {
    validate_name("trigger", &input.name)?;
    Ok(Trigger {
        id: Uuid::new_v4().to_string(),
        name: input.name,
        channel_id: input.channel_id,
        sender_id: input.sender_id,
        accepted_kinds: normalize_kinds(input.accepted_kinds)?,
        state: TriggerState::Active,
        created_at_ms: now_ms,
        updated_at_ms: now_ms,
        last_occurrence_id: None,
        last_fired_at_ms: None,
        accepted_occurrences: 0,
        retired_at_ms: None,
        retired_reason: None,
    })
}

pub(crate) async fn insert_trigger(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    trigger: &Trigger,
) -> Result<(), FleetError> {
    let kinds_json = serde_json::to_string(&trigger.accepted_kinds)?;
    let result = sqlx::query(
        "INSERT INTO triggers (id, name, channel_id, sender_id, accepted_kinds_json, \
         state, created_at_ms, updated_at_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&trigger.id)
    .bind(&trigger.name)
    .bind(&trigger.channel_id)
    .bind(&trigger.sender_id)
    .bind(kinds_json)
    .bind(trigger.state.as_str())
    .bind(trigger.created_at_ms)
    .bind(trigger.updated_at_ms)
    .execute(&mut **transaction)
    .await;
    map_unique_conflict(result, "trigger name")
}

/// Moves an active registration to retired, and reports whether it moved.
///
/// A registration already at rest is left exactly as it was, reason included:
/// the recorded reason is the one that describes why it stopped, and a second
/// operator stopping it again has not made a mistake.
pub(crate) async fn retire_trigger_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    trigger_id: &str,
    reason: &str,
    now_ms: i64,
) -> Result<bool, FleetError> {
    let result = sqlx::query(
        "UPDATE triggers SET state = ?, updated_at_ms = ?, retired_at_ms = ?, \
         retired_reason = ? WHERE id = ? AND state = ?",
    )
    .bind(TriggerState::Retired.as_str())
    .bind(now_ms)
    .bind(now_ms)
    .bind(reason)
    .bind(trigger_id)
    .bind(TriggerState::Active.as_str())
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Reads a registration inside a caller's transaction.
///
/// A firing has to decide what a trigger may do and then do it without the
/// registration moving underneath it, so the authority is read in the same
/// transaction that acts on it.
///
/// # Errors
///
/// Returns not found for an unknown trigger, or a decoding error for an
/// unreadable stored row.
pub async fn trigger_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    trigger_id: &str,
) -> Result<Trigger, FleetError> {
    let row = sqlx::query(&format!("{} WHERE id = ?", trigger_select()))
        .bind(trigger_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or_else(|| FleetError::NotFound {
            entity: "trigger",
            id: trigger_id.to_owned(),
        })?;
    trigger_from_row(&row)
}

/// Records that an active trigger created work, and reports whether it did.
///
/// The `state = 'active'` predicate is the authority check, not a courtesy: it
/// runs in the caller's transaction, so a retirement racing a firing either
/// wins outright or loses outright.
///
/// Only accepted occurrences are recorded. A repeat that the message layer
/// absorbed created nothing, and a trigger whose scheduler has been re-firing
/// Tuesday's occurrence all week has genuinely produced no work since Tuesday.
///
/// # Errors
///
/// Returns an error when the row cannot be written.
pub async fn record_trigger_occurrence(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    trigger_id: &str,
    occurrence_id: &str,
    now_ms: i64,
) -> Result<bool, FleetError> {
    let result = sqlx::query(
        "UPDATE triggers SET last_occurrence_id = ?, last_fired_at_ms = ?, \
         updated_at_ms = ?, accepted_occurrences = accepted_occurrences + 1 \
         WHERE id = ? AND state = ?",
    )
    .bind(occurrence_id)
    .bind(now_ms)
    .bind(now_ms)
    .bind(trigger_id)
    .bind(TriggerState::Active.as_str())
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Sorts and deduplicates the declared set.
fn normalize_kinds(kinds: Vec<String>) -> Result<Vec<String>, FleetError> {
    if kinds.is_empty() {
        return Err(FleetError::Invalid(
            "a trigger must declare at least one message kind".to_owned(),
        ));
    }
    if kinds.len() > MAX_ACCEPTED_KINDS {
        return Err(FleetError::Invalid(format!(
            "a trigger may declare at most {MAX_ACCEPTED_KINDS} message kinds"
        )));
    }
    let mut sorted = Vec::with_capacity(kinds.len());
    for kind in kinds {
        if kind.trim().is_empty() || kind.len() > MAX_KIND_BYTES {
            return Err(FleetError::Invalid(format!(
                "a declared message kind must contain between 1 and {MAX_KIND_BYTES} bytes"
            )));
        }
        if sorted.contains(&kind) {
            return Err(FleetError::Invalid(format!(
                "duplicate declared message kind {kind}"
            )));
        }
        sorted.push(kind);
    }
    sorted.sort();
    Ok(sorted)
}

fn trigger_select() -> &'static str {
    "SELECT id, name, channel_id, sender_id, accepted_kinds_json, state, created_at_ms, \
     updated_at_ms, last_occurrence_id, last_fired_at_ms, accepted_occurrences, \
     retired_at_ms, retired_reason FROM triggers"
}

fn trigger_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Trigger, FleetError> {
    let state: String = row.try_get("state")?;
    let kinds_json: String = row.try_get("accepted_kinds_json")?;
    let occurrences: i64 = row.try_get("accepted_occurrences")?;
    Ok(Trigger {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        channel_id: row.try_get("channel_id")?,
        sender_id: row.try_get("sender_id")?,
        accepted_kinds: serde_json::from_str(&kinds_json)?,
        state: TriggerState::parse(&state).ok_or_else(|| {
            FleetError::Invalid(format!("stored trigger state {state} is unreadable"))
        })?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
        last_occurrence_id: row.try_get("last_occurrence_id")?,
        last_fired_at_ms: row.try_get("last_fired_at_ms")?,
        accepted_occurrences: u64::try_from(occurrences).map_err(|_| {
            FleetError::Invalid("stored trigger occurrence count is negative".to_owned())
        })?,
        retired_at_ms: row.try_get("retired_at_ms")?,
        retired_reason: row.try_get("retired_reason")?,
    })
}
