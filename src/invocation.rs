use std::collections::BTreeSet;

use sqlx::Row;
use uuid::Uuid;

use crate::{
    error::FleetError,
    model::{
        ArmInvocation, ClaimDeliveries, CompleteInvocation, CreateMessage, ExecutionCertainty,
        Invocation, InvocationBatch, InvocationCompletion, InvocationState, Message,
    },
    store::{Store, insert_message, message_from_row, now_ms},
};

impl Store {
    /// Atomically leases eligible deliveries and creates their durable managed
    /// invocation records.
    ///
    /// Expired reservations that were never armed are proven not started and
    /// may be reclaimed. Expired armed invocations are parked instead.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, an unknown agent, or a persistence
    /// failure.
    pub async fn reserve_invocations(
        &self,
        agent_id: &str,
        input: ClaimDeliveries,
    ) -> Result<InvocationBatch, FleetError> {
        self.reserve_invocations_filtered(agent_id, input, None)
            .await
    }

    /// Atomically reserves only deliveries whose opaque envelope kind appears
    /// in the adapter-owned exact acceptance set.
    ///
    /// This trusted worker path does not interpret a kind or acknowledge
    /// skipped deliveries. Non-matching deliveries remain pending with their
    /// attempt count unchanged.
    pub(crate) async fn reserve_invocations_by_kind(
        &self,
        agent_id: &str,
        input: ClaimDeliveries,
        message_kinds: &BTreeSet<String>,
    ) -> Result<InvocationBatch, FleetError> {
        if message_kinds.is_empty() {
            return Err(FleetError::Invalid(
                "invocation message-kind selector must not be empty".to_owned(),
            ));
        }
        self.reserve_invocations_filtered(agent_id, input, Some(message_kinds))
            .await
    }

    /// Atomically leases eligible deliveries and creates their durable managed
    /// invocation records.
    ///
    /// Expired reservations that were never armed are proven not started and
    /// may be reclaimed. Expired armed invocations are parked instead.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, an unknown agent, or a persistence
    /// failure.
    async fn reserve_invocations_filtered(
        &self,
        agent_id: &str,
        input: ClaimDeliveries,
        message_kinds: Option<&BTreeSet<String>>,
    ) -> Result<InvocationBatch, FleetError> {
        crate::delivery::validate_claim(&input)?;
        crate::delivery::ensure_agent(&self.pool, agent_id).await?;
        let now = now_ms();
        let lease_duration = i64::try_from(input.lease_duration_ms)
            .map_err(|_| FleetError::Invalid("lease duration is too large".to_owned()))?;
        let lease_expires_at_ms = now
            .checked_add(lease_duration)
            .ok_or_else(|| FleetError::Invalid("lease expiry overflowed".to_owned()))?;
        let lease_token = Uuid::new_v4().to_string();
        let message_kinds_json = message_kinds.map(serde_json::to_string).transpose()?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        recover_expired_invocations(&mut transaction, agent_id, now).await?;
        sqlx::query(
            r"
            UPDATE agent_deliveries
            SET state = 'leased', attempt = attempt + 1,
                lease_token = ?, lease_expires_at_ms = ?
            WHERE rowid IN (
                SELECT d.rowid
                FROM agent_deliveries d
                JOIN messages m ON m.seq = d.message_seq
                WHERE d.agent_id = ?
                  AND d.available_at_ms <= ?
                  AND (
                    d.state = 'pending'
                    OR (d.state = 'leased' AND d.lease_expires_at_ms <= ?)
                  )
                  AND (
                    ? IS NULL
                    OR m.kind IN (SELECT value FROM json_each(?))
                  )
                ORDER BY d.message_seq
                LIMIT ?
            )
            ",
        )
        .bind(&lease_token)
        .bind(lease_expires_at_ms)
        .bind(agent_id)
        .bind(now)
        .bind(now)
        .bind(&message_kinds_json)
        .bind(&message_kinds_json)
        .bind(i64::from(input.limit))
        .execute(&mut *transaction)
        .await?;
        let rows = sqlx::query(
            r"
            SELECT m.seq, m.id, m.channel_id, m.sender_id, m.recipient_id,
                   m.kind, m.payload_json, m.correlation_id, m.causation_id,
                   m.created_at_ms, d.attempt
            FROM agent_deliveries d
            JOIN messages m ON m.seq = d.message_seq
            WHERE d.agent_id = ? AND d.state = 'leased' AND d.lease_token = ?
            ORDER BY m.seq
            ",
        )
        .bind(agent_id)
        .bind(&lease_token)
        .fetch_all(&mut *transaction)
        .await?;
        let mut invocations = Vec::with_capacity(rows.len());
        for row in &rows {
            let invocation = reserve_row(
                &mut transaction,
                row,
                agent_id,
                &lease_token,
                lease_expires_at_ms,
                now,
            )
            .await?;
            invocations.push(invocation);
        }
        transaction.commit().await?;
        Ok(InvocationBatch { invocations })
    }

    /// Durably arms one invocation immediately before an effectful dispatch.
    ///
    /// The controller must not send the effectful request until this operation
    /// commits. An identical replay is idempotent while the lease remains live.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid or stale fences, expired leases, an unknown
    /// invocation, conflicting state, or a persistence failure.
    pub async fn arm_invocation(
        &self,
        agent_id: &str,
        invocation_id: &str,
        input: ArmInvocation,
    ) -> Result<Invocation, FleetError> {
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let invocation =
            arm_invocation_transaction(&mut transaction, agent_id, invocation_id, &input, now)
                .await?;
        transaction.commit().await?;
        Ok(invocation)
    }

    /// Atomically publishes the invocation result and acknowledges its input
    /// delivery.
    ///
    /// The result is addressed back to the input sender in the same channel,
    /// carries the input correlation, and uses the input message as causation.
    /// An identical replay returns the original completion.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid or stale fences, an unarmed or terminal
    /// invocation, conflicting result replay, membership failure, or a
    /// persistence failure.
    pub async fn complete_invocation(
        &self,
        agent_id: &str,
        invocation_id: &str,
        input: CompleteInvocation,
    ) -> Result<(InvocationCompletion, bool), FleetError> {
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let completion = complete_invocation_transaction(
            &mut transaction,
            agent_id,
            invocation_id,
            &input,
            now,
            false,
        )
        .await?;
        transaction.commit().await?;
        Ok(completion)
    }

    /// Lists the latest durable invocation records for operator inspection.
    ///
    /// # Errors
    ///
    /// Returns an error when stored records cannot be read or decoded.
    pub async fn list_invocations(
        &self,
        agent_id: Option<&str>,
    ) -> Result<Vec<Invocation>, FleetError> {
        let rows = match agent_id {
            Some(agent_id) => {
                sqlx::query(&invocation_list_query("AND i.agent_id = ?"))
                    .bind(agent_id)
                    .fetch_all(&self.pool)
                    .await?
            }
            None => {
                sqlx::query(&invocation_list_query(""))
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.iter().map(invocation_from_row).collect()
    }
}

pub(crate) async fn arm_invocation_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
    input: &ArmInvocation,
    now: i64,
) -> Result<Invocation, FleetError> {
    validate_arm(invocation_id, input)?;
    let row = invocation_with_delivery(transaction, agent_id, invocation_id).await?;
    validate_invocation_fence(&row, input, now)?;
    let state: String = row.try_get("state")?;
    match state.as_str() {
        "reserved" => {
            let result = sqlx::query(
                r"
                UPDATE invocations
                SET state = 'dispatch_armed', dispatch_armed_at_ms = ?
                WHERE id = ? AND agent_id = ? AND state = 'reserved'
                ",
            )
            .bind(now)
            .bind(invocation_id)
            .bind(agent_id)
            .execute(&mut **transaction)
            .await?;
            if result.rows_affected() != 1 {
                return Err(FleetError::Conflict(
                    "invocation changed while dispatch was armed".to_owned(),
                ));
            }
        }
        "dispatch_armed" => {}
        "terminal" => {
            return Err(FleetError::Conflict(
                "terminal invocation cannot be dispatched".to_owned(),
            ));
        }
        _ => return Err(invalid_stored_state("invocation", &state)),
    }
    invocation_by_id(transaction, invocation_id).await
}

pub(crate) async fn complete_invocation_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
    input: &CompleteInvocation,
    now: i64,
    allow_active_session_binding: bool,
) -> Result<(InvocationCompletion, bool), FleetError> {
    validate_completion(invocation_id, input)?;
    if !allow_active_session_binding {
        crate::session_binding::ensure_invocation_not_active_on_session(transaction, invocation_id)
            .await?;
    }
    let row = invocation_with_delivery(transaction, agent_id, invocation_id).await?;
    validate_static_fence(&row, &input.lease_token, &input.fence_token)?;
    let expected = result_input(agent_id, invocation_id, &row, input)?;
    let state: String = row.try_get("state")?;
    if state == "terminal" {
        let completion = completed_replay(transaction, invocation_id, &row, &expected).await?;
        return Ok((completion, false));
    }
    if state != "dispatch_armed" {
        return Err(FleetError::Conflict(
            "invocation must be armed before completion".to_owned(),
        ));
    }
    validate_live_lease(&row, &input.lease_token, now)?;
    ensure_result_key_unused(transaction, agent_id, invocation_id).await?;
    let channel_id: String = row.try_get("input_channel_id")?;
    let result_message = insert_message(transaction, &channel_id, expected).await?;
    acknowledge_invocation_delivery(transaction, &row, agent_id, &input.lease_token, now).await?;
    let updated = sqlx::query(
        r"
        UPDATE invocations
        SET state = 'terminal', terminal_at_ms = ?,
            execution_certainty = 'outcome_known',
            terminal_reason = 'completed', result_message_seq = ?
        WHERE id = ? AND agent_id = ? AND state = 'dispatch_armed'
        ",
    )
    .bind(now)
    .bind(result_message.seq)
    .bind(invocation_id)
    .bind(agent_id)
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(FleetError::Conflict(
            "invocation changed during completion".to_owned(),
        ));
    }
    let invocation = invocation_by_id(transaction, invocation_id).await?;
    Ok((
        InvocationCompletion {
            invocation,
            result: result_message,
        },
        true,
    ))
}

async fn reserve_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &sqlx::sqlite::SqliteRow,
    agent_id: &str,
    lease_token: &str,
    lease_expires_at_ms: i64,
    reserved_at_ms: i64,
) -> Result<Invocation, FleetError> {
    let id = Uuid::new_v4().to_string();
    let fence_token = Uuid::new_v4().to_string();
    let delivery_attempt: i64 = row.try_get("attempt")?;
    let message_seq: i64 = row.try_get("seq")?;
    sqlx::query(
        r"
        INSERT INTO invocations (
            id, message_seq, agent_id, delivery_attempt, lease_token,
            lease_expires_at_ms, fence_token, state, reserved_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'reserved', ?)
        ",
    )
    .bind(&id)
    .bind(message_seq)
    .bind(agent_id)
    .bind(delivery_attempt)
    .bind(lease_token)
    .bind(lease_expires_at_ms)
    .bind(&fence_token)
    .bind(reserved_at_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(Invocation {
        id,
        agent_id: agent_id.to_owned(),
        message: message_from_row(row)?,
        delivery_attempt,
        lease_token: lease_token.to_owned(),
        lease_expires_at_ms,
        fence_token,
        state: InvocationState::Reserved,
        reserved_at_ms,
        dispatch_armed_at_ms: None,
        terminal_at_ms: None,
        execution_certainty: None,
        terminal_reason: None,
        result_message_id: None,
    })
}

pub(crate) async fn recover_expired_invocations(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    now: i64,
) -> Result<(), FleetError> {
    terminalize_expired_reservations(transaction, agent_id, now).await?;
    let rows = sqlx::query(
        r"
        SELECT i.id, i.message_seq, i.lease_token, d.attempt
        FROM invocations i
        JOIN agent_deliveries d
          ON d.message_seq = i.message_seq AND d.agent_id = i.agent_id
        WHERE i.agent_id = ?
          AND i.state = 'dispatch_armed'
          AND i.lease_expires_at_ms <= ?
          AND d.state = 'leased'
          AND d.lease_token = i.lease_token
          AND d.lease_expires_at_ms <= ?
        ORDER BY i.reserved_at_ms, i.id
        ",
    )
    .bind(agent_id)
    .bind(now)
    .bind(now)
    .fetch_all(&mut **transaction)
    .await?;
    for row in rows {
        park_expired_armed_invocation(transaction, &row, agent_id, now).await?;
    }
    Ok(())
}

async fn terminalize_expired_reservations(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    now: i64,
) -> Result<(), FleetError> {
    sqlx::query(
        r"
        UPDATE invocations
        SET state = 'terminal', terminal_at_ms = ?,
            execution_certainty = 'not_started',
            terminal_reason = 'reservation_expired_before_dispatch'
        WHERE agent_id = ? AND state = 'reserved' AND lease_expires_at_ms <= ?
          AND EXISTS (
            SELECT 1 FROM agent_deliveries d
            WHERE d.message_seq = invocations.message_seq
              AND d.agent_id = invocations.agent_id
              AND d.state = 'leased'
              AND d.lease_token = invocations.lease_token
              AND d.lease_expires_at_ms <= ?
          )
        ",
    )
    .bind(now)
    .bind(agent_id)
    .bind(now)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn park_expired_armed_invocation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &sqlx::sqlite::SqliteRow,
    agent_id: &str,
    now: i64,
) -> Result<(), FleetError> {
    let invocation_id: String = row.try_get("id")?;
    let message_seq: i64 = row.try_get("message_seq")?;
    let lease_token: String = row.try_get("lease_token")?;
    let attempt: i64 = row.try_get("attempt")?;
    let reason = format!("invocation {invocation_id} lease expired after dispatch was armed");
    let result = sqlx::query(
        r"
        UPDATE agent_deliveries
        SET state = 'blocked', lease_token = NULL, lease_expires_at_ms = NULL,
            last_error = ?, last_settled_lease_token = ?,
            last_settlement = 'blocked', acknowledged_at_ms = NULL
        WHERE message_seq = ? AND agent_id = ? AND state = 'leased'
          AND lease_token = ? AND lease_expires_at_ms <= ?
        ",
    )
    .bind(&reason)
    .bind(&lease_token)
    .bind(message_seq)
    .bind(agent_id)
    .bind(&lease_token)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() != 1 {
        return Err(FleetError::Conflict(
            "expired invocation delivery changed during recovery".to_owned(),
        ));
    }
    sqlx::query(
        r"
        INSERT INTO delivery_blocks (
            message_seq, agent_id, attempt, lease_token, reason, blocked_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?)
        ",
    )
    .bind(message_seq)
    .bind(agent_id)
    .bind(attempt)
    .bind(&lease_token)
    .bind(&reason)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    let result = sqlx::query(
        r"
        UPDATE invocations
        SET state = 'terminal', terminal_at_ms = ?,
            execution_certainty = 'outcome_unknown',
            terminal_reason = 'lease_expired_after_dispatch_armed'
        WHERE id = ? AND state = 'dispatch_armed'
        ",
    )
    .bind(now)
    .bind(&invocation_id)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() != 1 {
        return Err(FleetError::Conflict(
            "expired invocation changed during recovery".to_owned(),
        ));
    }
    crate::session_binding::mark_expired_session_turn_uncertain(
        transaction,
        &invocation_id,
        &reason,
        now,
    )
    .await?;
    Ok(())
}

pub(crate) async fn ensure_retry_is_safe(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_id: &str,
    lease_token: &str,
) -> Result<(), FleetError> {
    let state: Option<String> = sqlx::query_scalar(
        r"
        SELECT i.state
        FROM invocations i
        JOIN messages m ON m.seq = i.message_seq
        WHERE i.agent_id = ? AND m.id = ? AND i.lease_token = ?
          AND i.state != 'terminal'
        ",
    )
    .bind(agent_id)
    .bind(message_id)
    .bind(lease_token)
    .fetch_optional(&mut **transaction)
    .await?;
    if state.as_deref() == Some("dispatch_armed") {
        return Err(FleetError::Conflict(
            "dispatch-armed invocation must be blocked or acknowledged".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) async fn terminalize_invocation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_id: &str,
    lease_token: &str,
    certainty: ExecutionCertainty,
    reason: &str,
    now: i64,
) -> Result<(), FleetError> {
    crate::session_binding::ensure_bound_turn_allows_delivery_settlement(
        transaction,
        agent_id,
        message_id,
        lease_token,
        certainty == ExecutionCertainty::OutcomeUnknown && reason == "blocked",
    )
    .await?;
    let certainty = certainty_name(&certainty);
    sqlx::query(
        r"
        UPDATE invocations
        SET state = 'terminal', terminal_at_ms = ?,
            execution_certainty = ?, terminal_reason = ?
        WHERE agent_id = ?
          AND message_seq = (SELECT seq FROM messages WHERE id = ?)
          AND lease_token = ? AND state != 'terminal'
        ",
    )
    .bind(now)
    .bind(certainty)
    .bind(reason)
    .bind(agent_id)
    .bind(message_id)
    .bind(lease_token)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn invocation_with_delivery(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
) -> Result<sqlx::sqlite::SqliteRow, FleetError> {
    sqlx::query(
        r"
        SELECT i.*, d.state AS delivery_state, d.lease_token AS delivery_lease_token,
               d.lease_expires_at_ms AS delivery_lease_expires_at_ms,
               m.id AS input_message_id, m.channel_id AS input_channel_id,
               m.sender_id AS input_sender_id,
               m.correlation_id AS input_correlation_id
        FROM invocations i
        JOIN agent_deliveries d
          ON d.message_seq = i.message_seq AND d.agent_id = i.agent_id
        JOIN messages m ON m.seq = i.message_seq
        WHERE i.id = ? AND i.agent_id = ?
        ",
    )
    .bind(invocation_id)
    .bind(agent_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| FleetError::NotFound {
        entity: "invocation",
        id: invocation_id.to_owned(),
    })
}

fn validate_invocation_fence(
    row: &sqlx::sqlite::SqliteRow,
    input: &ArmInvocation,
    now: i64,
) -> Result<(), FleetError> {
    validate_static_fence(row, &input.lease_token, &input.fence_token)?;
    validate_live_lease(row, &input.lease_token, now)
}

fn validate_static_fence(
    row: &sqlx::sqlite::SqliteRow,
    lease_token: &str,
    fence_token: &str,
) -> Result<(), FleetError> {
    let stored_lease: String = row.try_get("lease_token")?;
    let stored_fence: String = row.try_get("fence_token")?;
    if stored_lease != lease_token || stored_fence != fence_token {
        return Err(FleetError::LeaseConflict(
            "invocation lease or fence token is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn validate_live_lease(
    row: &sqlx::sqlite::SqliteRow,
    lease_token: &str,
    now: i64,
) -> Result<(), FleetError> {
    let lease_expires_at_ms: i64 = row.try_get("lease_expires_at_ms")?;
    let delivery_state: String = row.try_get("delivery_state")?;
    let delivery_lease_token: Option<String> = row.try_get("delivery_lease_token")?;
    let delivery_expiry: Option<i64> = row.try_get("delivery_lease_expires_at_ms")?;
    if lease_expires_at_ms <= now
        || delivery_expiry.is_none_or(|expiry| expiry <= now)
        || delivery_state != "leased"
        || delivery_lease_token.as_deref() != Some(lease_token)
    {
        return Err(FleetError::LeaseConflict(
            "invocation lease is expired or no longer owns the delivery".to_owned(),
        ));
    }
    Ok(())
}

fn validate_arm(invocation_id: &str, input: &ArmInvocation) -> Result<(), FleetError> {
    for (label, value) in [
        ("invocation ID", invocation_id),
        ("lease token", input.lease_token.as_str()),
        ("fence token", input.fence_token.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(FleetError::Invalid(format!("{label} must not be empty")));
        }
    }
    Ok(())
}

fn validate_completion(invocation_id: &str, input: &CompleteInvocation) -> Result<(), FleetError> {
    for (label, value) in [
        ("invocation ID", invocation_id),
        ("lease token", input.lease_token.as_str()),
        ("fence token", input.fence_token.as_str()),
        ("result kind", input.kind.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(FleetError::Invalid(format!("{label} must not be empty")));
        }
    }
    Ok(())
}

fn result_input(
    agent_id: &str,
    invocation_id: &str,
    row: &sqlx::sqlite::SqliteRow,
    input: &CompleteInvocation,
) -> Result<CreateMessage, FleetError> {
    Ok(CreateMessage {
        sender_id: agent_id.to_owned(),
        idempotency_key: Some(format!("invocation/{invocation_id}/result")),
        recipient_id: Some(row.try_get("input_sender_id")?),
        kind: input.kind.clone(),
        payload: input.payload.clone(),
        correlation_id: row.try_get("input_correlation_id")?,
        causation_id: Some(row.try_get("input_message_id")?),
    })
}

async fn ensure_result_key_unused(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
) -> Result<(), FleetError> {
    let exists: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM messages WHERE sender_id = ? AND idempotency_key = ?",
    )
    .bind(agent_id)
    .bind(format!("invocation/{invocation_id}/result"))
    .fetch_one(&mut **transaction)
    .await?;
    if exists != 0 {
        return Err(FleetError::Conflict(
            "invocation result key was already used outside completion".to_owned(),
        ));
    }
    Ok(())
}

async fn acknowledge_invocation_delivery(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &sqlx::sqlite::SqliteRow,
    agent_id: &str,
    lease_token: &str,
    now: i64,
) -> Result<(), FleetError> {
    let message_seq: i64 = row.try_get("message_seq")?;
    let result = sqlx::query(
        r"
        UPDATE agent_deliveries
        SET state = 'acknowledged', lease_token = NULL, lease_expires_at_ms = NULL,
            last_settled_lease_token = ?, last_settlement = 'acknowledged',
            acknowledged_at_ms = ?
        WHERE message_seq = ? AND agent_id = ? AND state = 'leased'
          AND lease_token = ? AND lease_expires_at_ms > ?
        ",
    )
    .bind(lease_token)
    .bind(now)
    .bind(message_seq)
    .bind(agent_id)
    .bind(lease_token)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() != 1 {
        return Err(FleetError::LeaseConflict(
            "invocation no longer owns its input delivery".to_owned(),
        ));
    }
    Ok(())
}

async fn completed_replay(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation_id: &str,
    row: &sqlx::sqlite::SqliteRow,
    expected: &CreateMessage,
) -> Result<InvocationCompletion, FleetError> {
    let reason: Option<String> = row.try_get("terminal_reason")?;
    let result_seq: Option<i64> = row.try_get("result_message_seq")?;
    let Some(result_seq) = result_seq else {
        return Err(FleetError::Conflict(
            "terminal invocation was not completed with a result".to_owned(),
        ));
    };
    if reason.as_deref() != Some("completed") {
        return Err(FleetError::Conflict(
            "terminal invocation was not completed with a result".to_owned(),
        ));
    }
    let result = message_by_seq(transaction, result_seq).await?;
    let channel_id: String = row.try_get("input_channel_id")?;
    if !message_matches_result(&result, &channel_id, expected) {
        return Err(FleetError::Conflict(
            "invocation was already completed with a different result".to_owned(),
        ));
    }
    Ok(InvocationCompletion {
        invocation: invocation_by_id(transaction, invocation_id).await?,
        result,
    })
}

async fn message_by_seq(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    message_seq: i64,
) -> Result<Message, FleetError> {
    let row = sqlx::query(
        r"
        SELECT seq, id, channel_id, sender_id, recipient_id, kind, payload_json,
               correlation_id, causation_id, created_at_ms
        FROM messages WHERE seq = ?
        ",
    )
    .bind(message_seq)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| FleetError::NotFound {
        entity: "message",
        id: message_seq.to_string(),
    })?;
    message_from_row(&row)
}

fn message_matches_result(message: &Message, channel_id: &str, input: &CreateMessage) -> bool {
    message.channel_id == channel_id
        && message.sender_id == input.sender_id
        && message.recipient_id == input.recipient_id
        && message.kind == input.kind
        && message.payload == input.payload
        && message.correlation_id == input.correlation_id
        && message.causation_id == input.causation_id
}

async fn invocation_by_id(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    invocation_id: &str,
) -> Result<Invocation, FleetError> {
    let row = sqlx::query(&format!("{} WHERE i.id = ?", invocation_select()))
        .bind(invocation_id)
        .fetch_optional(&mut **transaction)
        .await?;
    row.as_ref()
        .map(invocation_from_row)
        .transpose()?
        .ok_or_else(|| FleetError::NotFound {
            entity: "invocation",
            id: invocation_id.to_owned(),
        })
}

fn invocation_list_query(filter: &str) -> String {
    format!(
        "{} WHERE 1 = 1 {filter} ORDER BY i.reserved_at_ms DESC, i.id DESC LIMIT 500",
        invocation_select()
    )
}

const fn invocation_select() -> &'static str {
    r"
    SELECT i.id AS invocation_id, i.agent_id, i.delivery_attempt,
           i.lease_token, i.lease_expires_at_ms, i.fence_token, i.state,
           i.reserved_at_ms, i.dispatch_armed_at_ms, i.terminal_at_ms,
           i.execution_certainty, i.terminal_reason,
           r.id AS result_message_id,
           m.seq, m.id, m.channel_id, m.sender_id, m.recipient_id, m.kind,
           m.payload_json, m.correlation_id, m.causation_id, m.created_at_ms
    FROM invocations i
    JOIN messages m ON m.seq = i.message_seq
    LEFT JOIN messages r ON r.seq = i.result_message_seq
    "
}

fn invocation_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Invocation, FleetError> {
    let state: String = row.try_get("state")?;
    let certainty: Option<String> = row.try_get("execution_certainty")?;
    Ok(Invocation {
        id: row.try_get("invocation_id")?,
        agent_id: row.try_get("agent_id")?,
        message: message_from_row(row)?,
        delivery_attempt: row.try_get("delivery_attempt")?,
        lease_token: row.try_get("lease_token")?,
        lease_expires_at_ms: row.try_get("lease_expires_at_ms")?,
        fence_token: row.try_get("fence_token")?,
        state: invocation_state(&state)?,
        reserved_at_ms: row.try_get("reserved_at_ms")?,
        dispatch_armed_at_ms: row.try_get("dispatch_armed_at_ms")?,
        terminal_at_ms: row.try_get("terminal_at_ms")?,
        execution_certainty: certainty.as_deref().map(execution_certainty).transpose()?,
        terminal_reason: row.try_get("terminal_reason")?,
        result_message_id: row.try_get("result_message_id")?,
    })
}

fn invocation_state(state: &str) -> Result<InvocationState, FleetError> {
    match state {
        "reserved" => Ok(InvocationState::Reserved),
        "dispatch_armed" => Ok(InvocationState::DispatchArmed),
        "terminal" => Ok(InvocationState::Terminal),
        _ => Err(invalid_stored_state("invocation", state)),
    }
}

fn execution_certainty(certainty: &str) -> Result<ExecutionCertainty, FleetError> {
    match certainty {
        "not_started" => Ok(ExecutionCertainty::NotStarted),
        "outcome_known" => Ok(ExecutionCertainty::OutcomeKnown),
        "outcome_unknown" => Ok(ExecutionCertainty::OutcomeUnknown),
        _ => Err(invalid_stored_state("execution certainty", certainty)),
    }
}

const fn certainty_name(certainty: &ExecutionCertainty) -> &'static str {
    match certainty {
        ExecutionCertainty::NotStarted => "not_started",
        ExecutionCertainty::OutcomeKnown => "outcome_known",
        ExecutionCertainty::OutcomeUnknown => "outcome_unknown",
    }
}

fn invalid_stored_state(entity: &str, value: &str) -> FleetError {
    FleetError::Invalid(format!("stored {entity} state is invalid: {value}"))
}
