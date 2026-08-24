//! Durable controller-owned harness session lanes and ownership fencing.

use std::path::Path;

use serde::{Deserialize, Serialize};
use sqlx::Row;
use uuid::Uuid;

use crate::{
    ArmInvocation, CompleteInvocation, FleetError, Invocation, InvocationCompletion,
    SessionPersistence, Store,
    invocation::{arm_invocation_transaction, complete_invocation_transaction},
    plugin::Binding,
    store::now_ms,
};

const MAX_ID_BYTES: usize = 256;
const MAX_LANE_KEY_BYTES: usize = 4_096;
const MAX_PATH_BYTES: usize = 4_096;
const MAX_SESSION_REF_BYTES: usize = 4_096;
const MAX_REASON_BYTES: usize = 4_096;
const MAX_ADDITIONAL_DIRECTORIES: usize = 64;

/// Durable lifecycle state for one native harness session generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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

/// Invocation armed atomically with exact session ownership.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundInvocation {
    pub invocation: Invocation,
    pub session: SessionBinding,
}

impl Store {
    /// Acquires one session lane for a fresh controller process instance.
    ///
    /// A compatible ready session is adopted by incrementing `owner_epoch`.
    /// An incompatible ready session or an abandoned opening rotates to the
    /// next binding generation. Active or uncertain sessions fail closed.
    /// Repeating the call with the same owner instance is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, an unknown agent, active or
    /// uncertain prior ownership, epoch overflow, or persistence failure.
    pub async fn acquire_session_binding(
        &self,
        agent_id: &str,
        input: AcquireSessionBinding,
    ) -> Result<SessionAcquisition, FleetError> {
        validate_acquisition(agent_id, &input)?;
        crate::delivery::ensure_agent(&self.pool, agent_id).await?;
        let directories_json = serde_json::to_string(&input.additional_directories)?;
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current = current_binding_row(
            &mut transaction,
            agent_id,
            &input.lane_policy,
            &input.lane_key,
        )
        .await?;
        let acquisition = match current {
            None => {
                insert_after_latest_retired(
                    &mut transaction,
                    agent_id,
                    &input,
                    &directories_json,
                    now,
                )
                .await?
            }
            Some(row) => {
                acquire_existing(
                    &mut transaction,
                    row,
                    agent_id,
                    &input,
                    &directories_json,
                    now,
                )
                .await?
            }
        };
        transaction.commit().await?;
        Ok(acquisition)
    }

    /// Persists the opaque native session reference before any prompt is armed.
    /// An identical retry by the same owner epoch is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid reference, stale owner fence,
    /// conflicting replay, unknown binding, or persistence failure.
    pub async fn record_session_opened(
        &self,
        agent_id: &str,
        binding: &Binding,
        session_ref: &str,
    ) -> Result<SessionBinding, FleetError> {
        validate_binding(binding)?;
        validate_bounded("session reference", session_ref, MAX_SESSION_REF_BYTES)?;
        let generation = as_i64("binding generation", binding.binding_generation)?;
        let owner_epoch = as_i64("owner epoch", binding.owner_epoch)?;
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = exact_binding_row(&mut transaction, agent_id, binding).await?;
        validate_owner(&row, binding)?;
        let state: String = row.try_get("state")?;
        match state.as_str() {
            "opening" => {
                let updated = sqlx::query(
                    r"
                    UPDATE session_bindings
                    SET state = 'ready', session_ref = ?, opened_at_ms = ?, updated_at_ms = ?
                    WHERE binding_id = ? AND binding_generation = ? AND agent_id = ?
                      AND owner_epoch = ? AND state = 'opening'
                    ",
                )
                .bind(session_ref)
                .bind(now)
                .bind(now)
                .bind(&binding.binding_id)
                .bind(generation)
                .bind(agent_id)
                .bind(owner_epoch)
                .execute(&mut *transaction)
                .await?;
                require_one(
                    updated.rows_affected(),
                    "session binding changed while opening",
                )?;
            }
            "ready"
                if row.try_get::<Option<String>, _>("session_ref")?.as_deref()
                    == Some(session_ref) => {}
            "ready" => {
                return Err(FleetError::Conflict(
                    "session opening was replayed with a different native reference".to_owned(),
                ));
            }
            _ => {
                return Err(FleetError::Conflict(format!(
                    "session reference cannot be recorded while binding is {state}"
                )));
            }
        }
        let session = binding_by_identity(&mut transaction, agent_id, binding).await?;
        transaction.commit().await?;
        Ok(session)
    }

    /// Atomically arms a reserved invocation and activates its exact durable
    /// session owner fence.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale or non-ready session owner, mismatched
    /// native reference, conflicting active turn, invalid invocation fence, or
    /// persistence failure.
    pub async fn arm_session_invocation(
        &self,
        agent_id: &str,
        invocation_id: &str,
        binding: &Binding,
        session_ref: &str,
        input: ArmInvocation,
    ) -> Result<BoundInvocation, FleetError> {
        validate_binding(binding)?;
        validate_bounded("session reference", session_ref, MAX_SESSION_REF_BYTES)?;
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = exact_binding_row(&mut transaction, agent_id, binding).await?;
        validate_owner(&row, binding)?;
        validate_session_reference(&row, session_ref)?;
        validate_binding_activation(&row, invocation_id)?;
        let invocation =
            arm_invocation_transaction(&mut transaction, agent_id, invocation_id, &input, now)
                .await?;
        activate_binding_turn(&mut transaction, agent_id, invocation_id, binding, now).await?;
        let session = binding_by_identity(&mut transaction, agent_id, binding).await?;
        transaction.commit().await?;
        Ok(BoundInvocation {
            invocation,
            session,
        })
    }

    /// Atomically publishes a known invocation result, acknowledges its input,
    /// and returns its session binding to ready.
    ///
    /// # Errors
    ///
    /// Returns an error for a stale binding fence, non-active binding turn,
    /// invalid completion replay, expired invocation lease, or persistence
    /// failure.
    pub async fn complete_session_invocation(
        &self,
        agent_id: &str,
        invocation_id: &str,
        binding: &Binding,
        persistence: SessionPersistence,
        input: CompleteInvocation,
    ) -> Result<(InvocationCompletion, bool), FleetError> {
        validate_binding(binding)?;
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        validate_bound_turn(&mut transaction, agent_id, invocation_id, binding, true).await?;
        let completion = complete_invocation_transaction(
            &mut transaction,
            agent_id,
            invocation_id,
            &input,
            now,
            true,
        )
        .await?;
        settle_quiescent_turn(
            &mut transaction,
            agent_id,
            invocation_id,
            binding,
            persistence,
            now,
        )
        .await?;
        transaction.commit().await?;
        Ok(completion)
    }

    /// Fences an active session as uncertain before its delivery is parked.
    /// The same reason is idempotent; changed evidence conflicts.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid evidence, stale ownership, a non-active
    /// turn, conflicting replay, or persistence failure.
    pub async fn mark_session_invocation_uncertain(
        &self,
        agent_id: &str,
        invocation_id: &str,
        binding: &Binding,
        reason: &str,
    ) -> Result<SessionBinding, FleetError> {
        validate_binding(binding)?;
        validate_bounded("uncertain reason", reason, MAX_REASON_BYTES)?;
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        validate_bound_turn(&mut transaction, agent_id, invocation_id, binding, false).await?;
        let row = exact_binding_row(&mut transaction, agent_id, binding).await?;
        let state: String = row.try_get("state")?;
        if state == "uncertain" {
            if row
                .try_get::<Option<String>, _>("uncertain_reason")?
                .as_deref()
                != Some(reason)
            {
                return Err(FleetError::Conflict(
                    "session uncertainty was already recorded with different evidence".to_owned(),
                ));
            }
        } else if state == "active" {
            mark_turn_uncertain(
                &mut transaction,
                agent_id,
                invocation_id,
                binding,
                reason,
                now,
            )
            .await?;
        } else {
            return Err(FleetError::Conflict(format!(
                "session turn cannot become uncertain while binding is {state}"
            )));
        }
        let session = binding_by_identity(&mut transaction, agent_id, binding).await?;
        transaction.commit().await?;
        Ok(session)
    }

    /// Retires a non-active binding generation under its exact owner fence.
    /// A later acquisition creates the next generation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid evidence, stale ownership, an active turn,
    /// conflicting replay, unknown binding, or persistence failure.
    pub async fn retire_session_binding(
        &self,
        agent_id: &str,
        binding: &Binding,
        reason: &str,
    ) -> Result<SessionBinding, FleetError> {
        validate_binding(binding)?;
        validate_bounded("retirement reason", reason, MAX_REASON_BYTES)?;
        let generation = as_i64("binding generation", binding.binding_generation)?;
        let owner_epoch = as_i64("owner epoch", binding.owner_epoch)?;
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = exact_binding_row(&mut transaction, agent_id, binding).await?;
        validate_owner(&row, binding)?;
        let state: String = row.try_get("state")?;
        if state == "active" {
            return Err(FleetError::Conflict(
                "active session binding must become uncertain before retirement".to_owned(),
            ));
        }
        if state == "retired" {
            if row
                .try_get::<Option<String>, _>("retired_reason")?
                .as_deref()
                != Some(reason)
            {
                return Err(FleetError::Conflict(
                    "session binding was already retired for a different reason".to_owned(),
                ));
            }
        } else {
            let updated = sqlx::query(
                r"
                UPDATE session_bindings
                SET state = 'retired', active_invocation_id = NULL,
                    retired_reason = ?, retired_at_ms = ?, updated_at_ms = ?
                WHERE binding_id = ? AND binding_generation = ? AND agent_id = ?
                  AND owner_epoch = ? AND state != 'active' AND state != 'retired'
                ",
            )
            .bind(reason)
            .bind(now)
            .bind(now)
            .bind(&binding.binding_id)
            .bind(generation)
            .bind(agent_id)
            .bind(owner_epoch)
            .execute(&mut *transaction)
            .await?;
            require_one(
                updated.rows_affected(),
                "session binding changed during retirement",
            )?;
        }
        let session = binding_by_identity(&mut transaction, agent_id, binding).await?;
        transaction.commit().await?;
        Ok(session)
    }

    /// Lists durable session generations for controller or operator inspection.
    ///
    /// # Errors
    ///
    /// Returns an error when stored records cannot be read or decoded.
    pub async fn list_session_bindings(
        &self,
        agent_id: Option<&str>,
    ) -> Result<Vec<SessionBinding>, FleetError> {
        let rows = match agent_id {
            Some(agent_id) => sqlx::query(&format!(
                "{} WHERE agent_id = ? ORDER BY updated_at_ms DESC, binding_id, binding_generation DESC LIMIT 500",
                binding_select()
            ))
            .bind(agent_id)
            .fetch_all(&self.pool)
            .await?,
            None => sqlx::query(&format!(
                "{} ORDER BY updated_at_ms DESC, binding_id, binding_generation DESC LIMIT 500",
                binding_select()
            ))
            .fetch_all(&self.pool)
            .await?,
        };
        rows.iter().map(binding_from_row).collect()
    }
}

pub(crate) async fn ensure_invocation_not_active_on_session(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation_id: &str,
) -> Result<(), FleetError> {
    let state: Option<String> =
        sqlx::query_scalar("SELECT state FROM session_binding_turns WHERE invocation_id = ?")
            .bind(invocation_id)
            .fetch_optional(&mut **transaction)
            .await?;
    if state
        .as_deref()
        .is_some_and(|state| state == "active" || state == "uncertain")
    {
        return Err(FleetError::Conflict(
            "session-bound invocation must be settled through its binding fence".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) async fn ensure_bound_turn_allows_delivery_settlement(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_id: &str,
    lease_token: &str,
    allow_uncertain_block: bool,
) -> Result<(), FleetError> {
    let state: Option<String> = sqlx::query_scalar(
        r"
        SELECT t.state
        FROM invocations i
        JOIN messages m ON m.seq = i.message_seq
        JOIN session_binding_turns t ON t.invocation_id = i.id
        WHERE i.agent_id = ? AND m.id = ? AND i.lease_token = ?
          AND i.state != 'terminal'
        ",
    )
    .bind(agent_id)
    .bind(message_id)
    .bind(lease_token)
    .fetch_optional(&mut **transaction)
    .await?;
    match state.as_deref() {
        None | Some("quiescent") => Ok(()),
        Some("uncertain") if allow_uncertain_block => Ok(()),
        Some("active" | "uncertain") => Err(FleetError::Conflict(
            "session-bound invocation must be settled through its binding fence".to_owned(),
        )),
        Some(_) => Err(invalid_stored("session binding turn state is invalid")),
    }
}

pub(crate) async fn mark_expired_session_turn_uncertain(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation_id: &str,
    reason: &str,
    now: i64,
) -> Result<(), FleetError> {
    let row = sqlx::query(
        r"
        SELECT t.binding_id, t.binding_generation, t.owner_epoch,
               t.state AS turn_state, s.agent_id, s.state AS binding_state,
               s.active_invocation_id
        FROM session_binding_turns t
        JOIN session_bindings s
          ON s.binding_id = t.binding_id
         AND s.binding_generation = t.binding_generation
        WHERE t.invocation_id = ?
        ",
    )
    .bind(invocation_id)
    .fetch_optional(&mut **transaction)
    .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let turn_state: String = row.try_get("turn_state")?;
    let binding_state: String = row.try_get("binding_state")?;
    let active_invocation_id: Option<String> = row.try_get("active_invocation_id")?;
    if active_invocation_id.as_deref() != Some(invocation_id) {
        return Err(FleetError::Conflict(
            "expired session turn is not the binding's active invocation".to_owned(),
        ));
    }
    if turn_state == "uncertain" && binding_state == "uncertain" {
        return Ok(());
    }
    if turn_state != "active" || binding_state != "active" {
        return Err(FleetError::Conflict(
            "expired invocation has inconsistent session binding state".to_owned(),
        ));
    }
    let binding = Binding {
        binding_id: row.try_get("binding_id")?,
        binding_generation: positive_u64("binding generation", row.try_get("binding_generation")?)?,
        owner_epoch: positive_u64("owner epoch", row.try_get("owner_epoch")?)?,
    };
    let agent_id: String = row.try_get("agent_id")?;
    mark_turn_uncertain(transaction, &agent_id, invocation_id, &binding, reason, now).await
}

async fn insert_after_latest_retired(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    input: &AcquireSessionBinding,
    directories_json: &str,
    now: i64,
) -> Result<SessionAcquisition, FleetError> {
    let latest =
        latest_binding_row(transaction, agent_id, &input.lane_policy, &input.lane_key).await?;
    let (binding_id, generation) = match latest {
        None => (Uuid::new_v4().to_string(), 1),
        Some(row) => {
            let latest = binding_from_row(&row)?;
            if latest.state != SessionBindingState::Retired {
                return Err(FleetError::Conflict(
                    "non-retired session binding was omitted from the current lane".to_owned(),
                ));
            }
            let generation = latest
                .binding
                .binding_generation
                .checked_add(1)
                .ok_or_else(|| FleetError::Conflict("binding generation overflowed".to_owned()))?;
            (latest.binding.binding_id, generation)
        }
    };
    insert_binding(
        transaction,
        binding_id,
        generation,
        agent_id,
        input,
        directories_json,
        now,
    )
    .await
}

async fn acquire_existing(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: sqlx::sqlite::SqliteRow,
    agent_id: &str,
    input: &AcquireSessionBinding,
    directories_json: &str,
    now: i64,
) -> Result<SessionAcquisition, FleetError> {
    let existing = binding_from_row(&row)?;
    if existing.owner_instance_id == input.owner_instance_id {
        if !configuration_matches(&existing, input) {
            return Err(FleetError::Conflict(
                "session acquisition replay changed immutable configuration".to_owned(),
            ));
        }
        return acquisition_for_owned(existing);
    }
    match existing.state {
        SessionBindingState::Opening => {
            rotate_binding(
                transaction,
                &existing,
                agent_id,
                input,
                directories_json,
                now,
                "opening_superseded_by_new_owner",
            )
            .await
        }
        SessionBindingState::Ready if configuration_matches(&existing, input) => {
            adopt_binding(transaction, existing, &input.owner_instance_id, now).await
        }
        SessionBindingState::Ready => {
            rotate_binding(
                transaction,
                &existing,
                agent_id,
                input,
                directories_json,
                now,
                "incompatible_profile_rotation",
            )
            .await
        }
        SessionBindingState::Active => Err(FleetError::Conflict(
            "active session binding cannot be adopted".to_owned(),
        )),
        SessionBindingState::Uncertain => Err(FleetError::Conflict(
            "uncertain session binding requires explicit reconciliation or retirement".to_owned(),
        )),
        SessionBindingState::Retired => Err(FleetError::Conflict(
            "retired binding was selected as the current lane".to_owned(),
        )),
    }
}

fn acquisition_for_owned(existing: SessionBinding) -> Result<SessionAcquisition, FleetError> {
    let mode = match existing.state {
        SessionBindingState::Opening => SessionAcquisitionMode::Create,
        SessionBindingState::Ready => SessionAcquisitionMode::Resume {
            session_ref: existing
                .session_ref
                .clone()
                .ok_or_else(|| invalid_stored("ready binding omitted session reference"))?,
        },
        SessionBindingState::Active => {
            return Err(FleetError::Conflict(
                "session binding already has an active invocation".to_owned(),
            ));
        }
        SessionBindingState::Uncertain => {
            return Err(FleetError::Conflict(
                "session binding is uncertain".to_owned(),
            ));
        }
        SessionBindingState::Retired => {
            return Err(FleetError::Conflict(
                "session binding is retired".to_owned(),
            ));
        }
    };
    Ok(SessionAcquisition {
        session: existing,
        mode,
    })
}

async fn insert_binding(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    binding_id: String,
    generation: u64,
    agent_id: &str,
    input: &AcquireSessionBinding,
    directories_json: &str,
    now: i64,
) -> Result<SessionAcquisition, FleetError> {
    let generation_i64 = as_i64("binding generation", generation)?;
    sqlx::query(
        r"
        INSERT INTO session_bindings (
            binding_id, binding_generation, agent_id, lane_policy, lane_key,
            owner_epoch, owner_instance_id, profile_digest, compatibility_digest,
            working_directory, additional_directories_json, state,
            created_at_ms, updated_at_ms
        ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, 'opening', ?, ?)
        ",
    )
    .bind(&binding_id)
    .bind(generation_i64)
    .bind(agent_id)
    .bind(&input.lane_policy)
    .bind(&input.lane_key)
    .bind(&input.owner_instance_id)
    .bind(&input.profile_digest)
    .bind(&input.compatibility_digest)
    .bind(&input.working_directory)
    .bind(directories_json)
    .bind(now)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    let binding = Binding {
        binding_id,
        binding_generation: generation,
        owner_epoch: 1,
    };
    Ok(SessionAcquisition {
        session: binding_by_identity(transaction, agent_id, &binding).await?,
        mode: SessionAcquisitionMode::Create,
    })
}

async fn rotate_binding(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    existing: &SessionBinding,
    agent_id: &str,
    input: &AcquireSessionBinding,
    directories_json: &str,
    now: i64,
    reason: &str,
) -> Result<SessionAcquisition, FleetError> {
    let generation = existing
        .binding
        .binding_generation
        .checked_add(1)
        .ok_or_else(|| FleetError::Conflict("binding generation overflowed".to_owned()))?;
    let updated = sqlx::query(
        r"
        UPDATE session_bindings
        SET state = 'retired', active_invocation_id = NULL,
            retired_reason = ?, retired_at_ms = ?, updated_at_ms = ?
        WHERE binding_id = ? AND binding_generation = ? AND agent_id = ?
          AND owner_epoch = ? AND state IN ('opening', 'ready')
        ",
    )
    .bind(reason)
    .bind(now)
    .bind(now)
    .bind(&existing.binding.binding_id)
    .bind(as_i64(
        "binding generation",
        existing.binding.binding_generation,
    )?)
    .bind(agent_id)
    .bind(as_i64("owner epoch", existing.binding.owner_epoch)?)
    .execute(&mut **transaction)
    .await?;
    require_one(
        updated.rows_affected(),
        "session binding changed during rotation",
    )?;
    insert_binding(
        transaction,
        existing.binding.binding_id.clone(),
        generation,
        agent_id,
        input,
        directories_json,
        now,
    )
    .await
}

async fn adopt_binding(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    existing: SessionBinding,
    owner_instance_id: &str,
    now: i64,
) -> Result<SessionAcquisition, FleetError> {
    let owner_epoch = existing
        .binding
        .owner_epoch
        .checked_add(1)
        .ok_or_else(|| FleetError::Conflict("owner epoch overflowed".to_owned()))?;
    let updated = sqlx::query(
        r"
        UPDATE session_bindings
        SET owner_epoch = ?, owner_instance_id = ?, updated_at_ms = ?
        WHERE binding_id = ? AND binding_generation = ? AND agent_id = ?
          AND owner_epoch = ? AND state = 'ready'
        ",
    )
    .bind(as_i64("owner epoch", owner_epoch)?)
    .bind(owner_instance_id)
    .bind(now)
    .bind(&existing.binding.binding_id)
    .bind(as_i64(
        "binding generation",
        existing.binding.binding_generation,
    )?)
    .bind(&existing.agent_id)
    .bind(as_i64("owner epoch", existing.binding.owner_epoch)?)
    .execute(&mut **transaction)
    .await?;
    require_one(
        updated.rows_affected(),
        "session binding changed during adoption",
    )?;
    let binding = Binding {
        binding_id: existing.binding.binding_id,
        binding_generation: existing.binding.binding_generation,
        owner_epoch,
    };
    let session = binding_by_identity(transaction, &existing.agent_id, &binding).await?;
    let session_ref = session
        .session_ref
        .clone()
        .ok_or_else(|| invalid_stored("adopted ready binding omitted session reference"))?;
    Ok(SessionAcquisition {
        session,
        mode: SessionAcquisitionMode::Resume { session_ref },
    })
}

async fn activate_binding_turn(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
    binding: &Binding,
    now: i64,
) -> Result<(), FleetError> {
    let existing = sqlx::query(
        r"
        SELECT binding_id, binding_generation, owner_epoch, state
        FROM session_binding_turns WHERE invocation_id = ?
        ",
    )
    .bind(invocation_id)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(existing) = existing {
        let same = existing.try_get::<String, _>("binding_id")? == binding.binding_id
            && existing.try_get::<i64, _>("binding_generation")?
                == as_i64("binding generation", binding.binding_generation)?
            && existing.try_get::<i64, _>("owner_epoch")?
                == as_i64("owner epoch", binding.owner_epoch)?
            && existing.try_get::<String, _>("state")? == "active";
        if !same {
            return Err(FleetError::Conflict(
                "invocation is already bound to different session ownership".to_owned(),
            ));
        }
        return Ok(());
    }
    sqlx::query(
        r"
        INSERT INTO session_binding_turns (
            invocation_id, binding_id, binding_generation, owner_epoch, state, started_at_ms
        ) VALUES (?, ?, ?, ?, 'active', ?)
        ",
    )
    .bind(invocation_id)
    .bind(&binding.binding_id)
    .bind(as_i64("binding generation", binding.binding_generation)?)
    .bind(as_i64("owner epoch", binding.owner_epoch)?)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    let updated = sqlx::query(
        r"
        UPDATE session_bindings
        SET state = 'active', active_invocation_id = ?, updated_at_ms = ?
        WHERE binding_id = ? AND binding_generation = ? AND agent_id = ?
          AND owner_epoch = ? AND state = 'ready' AND active_invocation_id IS NULL
        ",
    )
    .bind(invocation_id)
    .bind(now)
    .bind(&binding.binding_id)
    .bind(as_i64("binding generation", binding.binding_generation)?)
    .bind(agent_id)
    .bind(as_i64("owner epoch", binding.owner_epoch)?)
    .execute(&mut **transaction)
    .await?;
    require_one(
        updated.rows_affected(),
        "session binding changed while turn was armed",
    )
}

async fn settle_quiescent_turn(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
    binding: &Binding,
    persistence: SessionPersistence,
    now: i64,
) -> Result<(), FleetError> {
    let persistence_name = persistence_name(persistence);
    let turn = bound_turn_row(transaction, invocation_id).await?;
    let turn_state: String = turn.try_get("state")?;
    if turn_state == "quiescent" {
        if turn
            .try_get::<Option<String>, _>("session_persistence")?
            .as_deref()
            != Some(persistence_name)
        {
            return Err(FleetError::Conflict(
                "session completion replay changed persistence evidence".to_owned(),
            ));
        }
        return Ok(());
    }
    if turn_state != "active" {
        return Err(FleetError::Conflict(
            "non-active session turn cannot become quiescent".to_owned(),
        ));
    }
    let turn_updated = sqlx::query(
        r"
        UPDATE session_binding_turns
        SET state = 'quiescent', terminal_at_ms = ?, session_persistence = ?
        WHERE invocation_id = ? AND state = 'active'
        ",
    )
    .bind(now)
    .bind(persistence_name)
    .bind(invocation_id)
    .execute(&mut **transaction)
    .await?;
    require_one(
        turn_updated.rows_affected(),
        "session turn changed during completion",
    )?;
    let binding_updated = sqlx::query(
        r"
        UPDATE session_bindings
        SET state = 'ready', active_invocation_id = NULL,
            last_quiescent_invocation_id = ?, session_persistence = ?, updated_at_ms = ?
        WHERE binding_id = ? AND binding_generation = ? AND agent_id = ?
          AND owner_epoch = ? AND state = 'active' AND active_invocation_id = ?
        ",
    )
    .bind(invocation_id)
    .bind(persistence_name)
    .bind(now)
    .bind(&binding.binding_id)
    .bind(as_i64("binding generation", binding.binding_generation)?)
    .bind(agent_id)
    .bind(as_i64("owner epoch", binding.owner_epoch)?)
    .bind(invocation_id)
    .execute(&mut **transaction)
    .await?;
    require_one(
        binding_updated.rows_affected(),
        "session binding changed during completion",
    )
}

async fn mark_turn_uncertain(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
    binding: &Binding,
    reason: &str,
    now: i64,
) -> Result<(), FleetError> {
    let turn_updated = sqlx::query(
        r"
        UPDATE session_binding_turns
        SET state = 'uncertain', terminal_at_ms = ?, uncertain_reason = ?
        WHERE invocation_id = ? AND state = 'active'
        ",
    )
    .bind(now)
    .bind(reason)
    .bind(invocation_id)
    .execute(&mut **transaction)
    .await?;
    require_one(
        turn_updated.rows_affected(),
        "session turn changed while fencing uncertainty",
    )?;
    let binding_updated = sqlx::query(
        r"
        UPDATE session_bindings
        SET state = 'uncertain', uncertain_reason = ?, updated_at_ms = ?
        WHERE binding_id = ? AND binding_generation = ? AND agent_id = ?
          AND owner_epoch = ? AND state = 'active' AND active_invocation_id = ?
        ",
    )
    .bind(reason)
    .bind(now)
    .bind(&binding.binding_id)
    .bind(as_i64("binding generation", binding.binding_generation)?)
    .bind(agent_id)
    .bind(as_i64("owner epoch", binding.owner_epoch)?)
    .bind(invocation_id)
    .execute(&mut **transaction)
    .await?;
    require_one(
        binding_updated.rows_affected(),
        "session binding changed while fencing uncertainty",
    )
}

async fn validate_bound_turn(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
    binding: &Binding,
    allow_quiescent_replay: bool,
) -> Result<(), FleetError> {
    let turn = bound_turn_row(transaction, invocation_id).await?;
    validate_turn_fence(&turn, binding)?;
    if allow_quiescent_replay && turn.try_get::<String, _>("state")? == "quiescent" {
        return Ok(());
    }
    let row = exact_binding_row(transaction, agent_id, binding).await?;
    validate_owner(&row, binding)?;
    let active_invocation: Option<String> = row.try_get("active_invocation_id")?;
    let state: String = row.try_get("state")?;
    let valid_binding = (state == "active" || state == "uncertain")
        && active_invocation.as_deref() == Some(invocation_id);
    if !valid_binding {
        return Err(FleetError::Conflict(
            "invocation does not own the current session binding state".to_owned(),
        ));
    }
    Ok(())
}

fn validate_binding_activation(
    row: &sqlx::sqlite::SqliteRow,
    invocation_id: &str,
) -> Result<(), FleetError> {
    let state: String = row.try_get("state")?;
    let active: Option<String> = row.try_get("active_invocation_id")?;
    if state == "ready" && active.is_none()
        || state == "active" && active.as_deref() == Some(invocation_id)
    {
        return Ok(());
    }
    Err(FleetError::Conflict(
        "session binding is not ready for this invocation".to_owned(),
    ))
}

fn validate_session_reference(
    row: &sqlx::sqlite::SqliteRow,
    session_ref: &str,
) -> Result<(), FleetError> {
    if row.try_get::<Option<String>, _>("session_ref")?.as_deref() != Some(session_ref) {
        return Err(FleetError::LeaseConflict(
            "native session reference does not match durable binding".to_owned(),
        ));
    }
    Ok(())
}

fn validate_owner(row: &sqlx::sqlite::SqliteRow, binding: &Binding) -> Result<(), FleetError> {
    let generation: i64 = row.try_get("binding_generation")?;
    let owner_epoch: i64 = row.try_get("owner_epoch")?;
    if generation != as_i64("binding generation", binding.binding_generation)?
        || owner_epoch != as_i64("owner epoch", binding.owner_epoch)?
    {
        return Err(FleetError::LeaseConflict(
            "session binding owner fence is stale".to_owned(),
        ));
    }
    Ok(())
}

fn validate_turn_fence(row: &sqlx::sqlite::SqliteRow, binding: &Binding) -> Result<(), FleetError> {
    if row.try_get::<String, _>("binding_id")? != binding.binding_id
        || row.try_get::<i64, _>("binding_generation")?
            != as_i64("binding generation", binding.binding_generation)?
        || row.try_get::<i64, _>("owner_epoch")? != as_i64("owner epoch", binding.owner_epoch)?
    {
        return Err(FleetError::LeaseConflict(
            "session turn fence does not match durable ownership".to_owned(),
        ));
    }
    Ok(())
}

async fn current_binding_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    lane_policy: &str,
    lane_key: &str,
) -> Result<Option<sqlx::sqlite::SqliteRow>, FleetError> {
    Ok(sqlx::query(&format!(
        "{} WHERE agent_id = ? AND lane_policy = ? AND lane_key = ? AND state != 'retired'",
        binding_select()
    ))
    .bind(agent_id)
    .bind(lane_policy)
    .bind(lane_key)
    .fetch_optional(&mut **transaction)
    .await?)
}

async fn latest_binding_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    lane_policy: &str,
    lane_key: &str,
) -> Result<Option<sqlx::sqlite::SqliteRow>, FleetError> {
    Ok(sqlx::query(&format!(
        "{} WHERE agent_id = ? AND lane_policy = ? AND lane_key = ? ORDER BY binding_generation DESC LIMIT 1",
        binding_select()
    ))
    .bind(agent_id)
    .bind(lane_policy)
    .bind(lane_key)
    .fetch_optional(&mut **transaction)
    .await?)
}

async fn exact_binding_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    binding: &Binding,
) -> Result<sqlx::sqlite::SqliteRow, FleetError> {
    sqlx::query(&format!(
        "{} WHERE binding_id = ? AND binding_generation = ? AND agent_id = ?",
        binding_select()
    ))
    .bind(&binding.binding_id)
    .bind(as_i64("binding generation", binding.binding_generation)?)
    .bind(agent_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| FleetError::NotFound {
        entity: "session binding",
        id: format!("{}:{}", binding.binding_id, binding.binding_generation),
    })
}

async fn binding_by_identity(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    binding: &Binding,
) -> Result<SessionBinding, FleetError> {
    binding_from_row(&exact_binding_row(transaction, agent_id, binding).await?)
}

async fn bound_turn_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation_id: &str,
) -> Result<sqlx::sqlite::SqliteRow, FleetError> {
    sqlx::query(
        r"
        SELECT invocation_id, binding_id, binding_generation, owner_epoch,
               state, started_at_ms, terminal_at_ms, session_persistence,
               uncertain_reason
        FROM session_binding_turns WHERE invocation_id = ?
        ",
    )
    .bind(invocation_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| FleetError::NotFound {
        entity: "session binding turn",
        id: invocation_id.to_owned(),
    })
}

const fn binding_select() -> &'static str {
    r"
    SELECT binding_id, binding_generation, agent_id, lane_policy, lane_key,
           owner_epoch, owner_instance_id, profile_digest, compatibility_digest,
           working_directory, additional_directories_json, session_ref, state,
           active_invocation_id, last_quiescent_invocation_id,
           session_persistence, uncertain_reason, retired_reason,
           created_at_ms, updated_at_ms, opened_at_ms, retired_at_ms
    FROM session_bindings
    "
}

fn binding_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<SessionBinding, FleetError> {
    let state: String = row.try_get("state")?;
    let persistence: Option<String> = row.try_get("session_persistence")?;
    Ok(SessionBinding {
        binding: Binding {
            binding_id: row.try_get("binding_id")?,
            binding_generation: positive_u64(
                "binding generation",
                row.try_get("binding_generation")?,
            )?,
            owner_epoch: positive_u64("owner epoch", row.try_get("owner_epoch")?)?,
        },
        agent_id: row.try_get("agent_id")?,
        lane_policy: row.try_get("lane_policy")?,
        lane_key: row.try_get("lane_key")?,
        owner_instance_id: row.try_get("owner_instance_id")?,
        profile_digest: row.try_get("profile_digest")?,
        compatibility_digest: row.try_get("compatibility_digest")?,
        working_directory: row.try_get("working_directory")?,
        additional_directories: serde_json::from_str(
            &row.try_get::<String, _>("additional_directories_json")?,
        )?,
        session_ref: row.try_get("session_ref")?,
        state: parse_state(&state)?,
        active_invocation_id: row.try_get("active_invocation_id")?,
        last_quiescent_invocation_id: row.try_get("last_quiescent_invocation_id")?,
        session_persistence: persistence.as_deref().map(parse_persistence).transpose()?,
        uncertain_reason: row.try_get("uncertain_reason")?,
        retired_reason: row.try_get("retired_reason")?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
        opened_at_ms: row.try_get("opened_at_ms")?,
        retired_at_ms: row.try_get("retired_at_ms")?,
    })
}

fn configuration_matches(binding: &SessionBinding, input: &AcquireSessionBinding) -> bool {
    binding.profile_digest == input.profile_digest
        && binding.compatibility_digest == input.compatibility_digest
        && binding.working_directory == input.working_directory
        && binding.additional_directories == input.additional_directories
}

fn validate_acquisition(agent_id: &str, input: &AcquireSessionBinding) -> Result<(), FleetError> {
    validate_bounded("agent ID", agent_id, MAX_ID_BYTES)?;
    validate_bounded("lane policy", &input.lane_policy, MAX_ID_BYTES)?;
    validate_bounded("lane key", &input.lane_key, MAX_LANE_KEY_BYTES)?;
    validate_bounded("owner instance ID", &input.owner_instance_id, MAX_ID_BYTES)?;
    validate_bounded("profile digest", &input.profile_digest, MAX_ID_BYTES)?;
    validate_bounded(
        "compatibility digest",
        &input.compatibility_digest,
        MAX_ID_BYTES,
    )?;
    validate_absolute_path("working directory", &input.working_directory)?;
    if input.additional_directories.len() > MAX_ADDITIONAL_DIRECTORIES {
        return Err(FleetError::Invalid(format!(
            "additional directories must not exceed {MAX_ADDITIONAL_DIRECTORIES} entries"
        )));
    }
    for directory in &input.additional_directories {
        validate_absolute_path("additional directory", directory)?;
    }
    Ok(())
}

fn validate_binding(binding: &Binding) -> Result<(), FleetError> {
    validate_bounded("binding ID", &binding.binding_id, MAX_ID_BYTES)?;
    as_i64("binding generation", binding.binding_generation)?;
    as_i64("owner epoch", binding.owner_epoch)?;
    Ok(())
}

fn validate_absolute_path(label: &str, value: &str) -> Result<(), FleetError> {
    validate_bounded(label, value, MAX_PATH_BYTES)?;
    if !Path::new(value).is_absolute() {
        return Err(FleetError::Invalid(format!(
            "{label} must be an absolute path"
        )));
    }
    Ok(())
}

fn validate_bounded(label: &str, value: &str, limit: usize) -> Result<(), FleetError> {
    if value.trim().is_empty() {
        return Err(FleetError::Invalid(format!("{label} must not be empty")));
    }
    if value.len() > limit {
        return Err(FleetError::Invalid(format!(
            "{label} must not exceed {limit} bytes"
        )));
    }
    Ok(())
}

fn as_i64(label: &str, value: u64) -> Result<i64, FleetError> {
    if value == 0 {
        return Err(FleetError::Invalid(format!(
            "{label} must be greater than zero"
        )));
    }
    i64::try_from(value).map_err(|_| FleetError::Invalid(format!("{label} is too large")))
}

fn positive_u64(label: &str, value: i64) -> Result<u64, FleetError> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| invalid_stored(&format!("{label} is not positive")))
}

fn parse_state(value: &str) -> Result<SessionBindingState, FleetError> {
    match value {
        "opening" => Ok(SessionBindingState::Opening),
        "ready" => Ok(SessionBindingState::Ready),
        "active" => Ok(SessionBindingState::Active),
        "uncertain" => Ok(SessionBindingState::Uncertain),
        "retired" => Ok(SessionBindingState::Retired),
        _ => Err(invalid_stored("session binding state is invalid")),
    }
}

const fn persistence_name(value: SessionPersistence) -> &'static str {
    match value {
        SessionPersistence::Confirmed => "confirmed",
        SessionPersistence::RuntimeClaimed => "runtime_claimed",
        SessionPersistence::Unknown => "unknown",
    }
}

fn parse_persistence(value: &str) -> Result<SessionPersistence, FleetError> {
    match value {
        "confirmed" => Ok(SessionPersistence::Confirmed),
        "runtime_claimed" => Ok(SessionPersistence::RuntimeClaimed),
        "unknown" => Ok(SessionPersistence::Unknown),
        _ => Err(invalid_stored("session persistence state is invalid")),
    }
}

fn require_one(rows: u64, message: &str) -> Result<(), FleetError> {
    if rows != 1 {
        return Err(FleetError::Conflict(message.to_owned()));
    }
    Ok(())
}

fn invalid_stored(message: &str) -> FleetError {
    FleetError::Invalid(format!("stored {message}"))
}
