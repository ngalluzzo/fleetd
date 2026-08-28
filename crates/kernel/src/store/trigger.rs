//! Durable registration for a thing that creates work on its own.
//!
//! What a trigger may do is stored here rather than supplied when it fires, so
//! "narrow" is a property of the row instead of a promise the caller keeps.
//! Composition that turns a firing into a message lives above the kernel: this
//! module owns the row and its transitions, and nothing else.

use sqlx::Row;
use uuid::Uuid;

use crate::error::FleetError;
use fleetd_proto::trigger::{RegisterTrigger, Trigger, TriggerState};

use super::{Store, map_unique_conflict, now_ms, validate_name};

/// A trigger declaring nothing could create nothing, and one declaring the world
/// is not narrow. Both are configuration mistakes worth refusing at the door.
const MAX_ACCEPTED_KINDS: usize = 32;
const MAX_KIND_BYTES: usize = 256;

impl Store {
    /// Registers a trigger against an existing channel and sender.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or duplicate name, an empty, oversized, or
    /// duplicated kind set, an unknown channel or sender, or a persistence
    /// failure.
    pub async fn register_trigger(&self, input: RegisterTrigger) -> Result<Trigger, FleetError> {
        validate_name("trigger", &input.name)?;
        let accepted_kinds = normalize_kinds(input.accepted_kinds)?;
        let now = now_ms();
        let trigger = Trigger {
            id: Uuid::new_v4().to_string(),
            name: input.name,
            channel_id: input.channel_id,
            sender_id: input.sender_id,
            accepted_kinds,
            state: TriggerState::Active,
            created_at_ms: now,
            updated_at_ms: now,
            last_occurrence_id: None,
            last_fired_at_ms: None,
            accepted_occurrences: 0,
            retired_at_ms: None,
            retired_reason: None,
        };
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
        .execute(&self.pool)
        .await;
        map_unique_conflict(result, "trigger name")?;
        Ok(trigger)
    }

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

    /// Retires a trigger so it may no longer create work.
    ///
    /// Retiring is idempotent: a second retirement of the same trigger reports
    /// the row already at rest rather than failing, because an operator
    /// stopping something twice has not made a mistake.
    ///
    /// # Errors
    ///
    /// Returns not found for an unknown trigger, or a persistence failure.
    pub async fn retire_trigger(
        &self,
        trigger_id: &str,
        reason: &str,
    ) -> Result<Trigger, FleetError> {
        let now = now_ms();
        sqlx::query(
            "UPDATE triggers SET state = ?, updated_at_ms = ?, retired_at_ms = ?, \
             retired_reason = ? WHERE id = ? AND state = ?",
        )
        .bind(TriggerState::Retired.as_str())
        .bind(now)
        .bind(now)
        .bind(reason)
        .bind(trigger_id)
        .bind(TriggerState::Active.as_str())
        .execute(&self.pool)
        .await?;
        self.get_trigger(trigger_id).await
    }
}

/// Sorts and deduplicates the declared set, and refuses the shapes that would
/// make "narrow" meaningless.
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
