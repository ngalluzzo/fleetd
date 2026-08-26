//! The durable delivery row and its state machine.
//!
//! Every transition a delivery row can make lives here, and only here. What
//! else belongs in the same commit — terminalizing an invocation fence, for
//! instance — is the caller\'s decision, so these return whether the row moved
//! rather than committing on their own.

use sqlx::Row;

use crate::{
    error::FleetError,
    store::{Store, message_from_row, now_ms},
};
use fleetd_proto::model::{
    BlockDelivery, BlockResolution, BlockedDelivery, ClaimDeliveries, Delivery,
    ResolveDeliveryBlock, RetryDelivery,
};

const MAX_CLAIM_LIMIT: u32 = 100;
const MAX_LEASE_DURATION_MS: u64 = 3_600_000;
const MAX_RETRY_DELAY_MS: u64 = 86_400_000;
const MAX_ERROR_LENGTH: usize = 4_096;

impl Store {
    /// Lists unresolved blocked deliveries for operator review.
    ///
    /// # Errors
    ///
    /// Returns an error when stored rows cannot be read or decoded.
    pub async fn list_blocked_deliveries(
        &self,
        agent_id: Option<&str>,
    ) -> Result<Vec<BlockedDelivery>, FleetError> {
        let rows = match agent_id {
            Some(agent_id) => {
                sqlx::query(&blocked_delivery_query("AND b.agent_id = ?"))
                    .bind(agent_id)
                    .fetch_all(&self.pool)
                    .await?
            }
            None => {
                sqlx::query(&blocked_delivery_query(""))
                    .fetch_all(&self.pool)
                    .await?
            }
        };
        rows.iter().map(blocked_delivery_from_row).collect()
    }

    /// Applies an operator decision to one exact blocked-delivery record.
    ///
    /// An identical replay is idempotent. A conflicting second decision fails
    /// without changing the delivery.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, an unknown block, conflicting replay,
    /// invalid block state, or a persistence failure.
    pub async fn resolve_delivery_block(
        &self,
        block_id: i64,
        input: ResolveDeliveryBlock,
    ) -> Result<(), FleetError> {
        validate_resolution(block_id, &input)?;
        let retry_after_ms = i64::try_from(input.retry_after_ms)
            .map_err(|_| FleetError::Invalid("retry delay is too large".to_owned()))?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = resolution_row(&mut transaction, block_id).await?;
        if resolution_is_replay(&row, &input, retry_after_ms)? {
            transaction.commit().await?;
            return Ok(());
        }
        let state: String = row.try_get("delivery_state")?;
        if state != "blocked" {
            return Err(FleetError::Conflict(
                "delivery block is not the current unresolved block".to_owned(),
            ));
        }
        let now = now_ms();
        resolve_delivery_row(&mut transaction, &row, &input, retry_after_ms, now).await?;
        let resolution = resolution_name(&input.resolution);
        let result = sqlx::query(
            r"
            UPDATE delivery_blocks
            SET resolved_at_ms = ?, resolution = ?, resolution_note = ?, retry_after_ms = ?
            WHERE id = ? AND resolved_at_ms IS NULL
            ",
        )
        .bind(now)
        .bind(resolution)
        .bind(&input.note)
        .bind(retry_after_ms)
        .bind(block_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() != 1 {
            return Err(FleetError::Conflict(
                "delivery block changed during resolution".to_owned(),
            ));
        }
        transaction.commit().await?;
        Ok(())
    }
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error for a limit or lease duration outside its bounds.
pub fn validate_claim(input: &ClaimDeliveries) -> Result<(), FleetError> {
    if input.limit == 0 || input.limit > MAX_CLAIM_LIMIT {
        return Err(FleetError::Invalid(format!(
            "claim limit must be between 1 and {MAX_CLAIM_LIMIT}"
        )));
    }
    if input.lease_duration_ms == 0 || input.lease_duration_ms > MAX_LEASE_DURATION_MS {
        return Err(FleetError::Invalid(format!(
            "lease duration must be between 1 and {MAX_LEASE_DURATION_MS} milliseconds"
        )));
    }
    Ok(())
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error for a malformed token, an oversized error string, or a retry delay outside its bounds.
pub fn validate_retry(input: &RetryDelivery) -> Result<(), FleetError> {
    validate_token(&input.lease_token)?;
    if input.retry_after_ms > MAX_RETRY_DELAY_MS {
        return Err(FleetError::Invalid(format!(
            "retry delay must not exceed {MAX_RETRY_DELAY_MS} milliseconds"
        )));
    }
    if input
        .error
        .as_ref()
        .is_some_and(|error| error.len() > MAX_ERROR_LENGTH)
    {
        return Err(FleetError::Invalid(format!(
            "retry error must not exceed {MAX_ERROR_LENGTH} bytes"
        )));
    }
    Ok(())
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error for a malformed token or oversized evidence.
pub fn validate_block(input: &BlockDelivery) -> Result<(), FleetError> {
    validate_token(&input.lease_token)?;
    validate_evidence("block reason", Some(&input.reason))
}

fn validate_resolution(block_id: i64, input: &ResolveDeliveryBlock) -> Result<(), FleetError> {
    if block_id <= 0 {
        return Err(FleetError::Invalid(
            "delivery block ID must be positive".to_owned(),
        ));
    }
    if input.retry_after_ms > MAX_RETRY_DELAY_MS {
        return Err(FleetError::Invalid(format!(
            "retry delay must not exceed {MAX_RETRY_DELAY_MS} milliseconds"
        )));
    }
    if input.resolution == BlockResolution::Abandon && input.retry_after_ms != 0 {
        return Err(FleetError::Invalid(
            "abandoned blocks cannot have a retry delay".to_owned(),
        ));
    }
    validate_evidence("resolution note", input.note.as_deref())
}

fn validate_evidence(label: &str, value: Option<&str>) -> Result<(), FleetError> {
    if value.is_some_and(|value| value.len() > MAX_ERROR_LENGTH) {
        return Err(FleetError::Invalid(format!(
            "{label} must not exceed {MAX_ERROR_LENGTH} bytes"
        )));
    }
    if value.is_some_and(|value| value.trim().is_empty()) {
        return Err(FleetError::Invalid(format!("{label} must not be empty")));
    }
    Ok(())
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error when the token is empty or malformed.
pub fn validate_token(lease_token: &str) -> Result<(), FleetError> {
    if lease_token.trim().is_empty() {
        return Err(FleetError::Invalid(
            "lease token must not be empty".to_owned(),
        ));
    }
    Ok(())
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error when the agent does not exist or cannot be read.
pub async fn ensure_agent(pool: &sqlx::SqlitePool, agent_id: &str) -> Result<(), FleetError> {
    let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agents WHERE id = ?")
        .bind(agent_id)
        .fetch_one(pool)
        .await?;
    if exists == 0 {
        return Err(FleetError::NotFound {
            entity: "agent",
            id: agent_id.to_owned(),
        });
    }
    Ok(())
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error when the delivery is unknown, or when the lease is expired, foreign, or settled differently.
pub async fn settle_miss(
    pool: &sqlx::SqlitePool,
    agent_id: &str,
    message_id: &str,
    lease_token: &str,
    expected_settlement: &str,
) -> Result<(), FleetError> {
    let row = sqlx::query(
        r"
        SELECT d.last_settled_lease_token, d.last_settlement
        FROM agent_deliveries d
        JOIN messages m ON m.seq = d.message_seq
        WHERE d.agent_id = ? AND m.id = ?
        ",
    )
    .bind(agent_id)
    .bind(message_id)
    .fetch_optional(pool)
    .await?;
    let Some(row) = row else {
        return Err(FleetError::NotFound {
            entity: "delivery",
            id: format!("{agent_id}/{message_id}"),
        });
    };
    let last_token: Option<String> = row.try_get("last_settled_lease_token")?;
    let last_settlement: Option<String> = row.try_get("last_settlement")?;
    if last_token.as_deref() == Some(lease_token)
        && last_settlement.as_deref() == Some(expected_settlement)
    {
        return Ok(());
    }
    Err(FleetError::LeaseConflict(
        "lease is expired, invalid, or owned by another worker".to_owned(),
    ))
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error when the blocked delivery changed before its evidence was recorded.
pub async fn insert_block_record(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_id: &str,
    input: &BlockDelivery,
    blocked_at_ms: i64,
) -> Result<i64, FleetError> {
    let result = sqlx::query(
        r"
        INSERT INTO delivery_blocks (
            message_seq, agent_id, attempt, lease_token, reason, blocked_at_ms
        )
        SELECT d.message_seq, d.agent_id, d.attempt, ?, ?, ?
        FROM agent_deliveries d
        JOIN messages m ON m.seq = d.message_seq
        WHERE d.agent_id = ? AND m.id = ? AND d.state = 'blocked'
        ",
    )
    .bind(&input.lease_token)
    .bind(&input.reason)
    .bind(blocked_at_ms)
    .bind(agent_id)
    .bind(message_id)
    .execute(&mut **transaction)
    .await?;
    if result.rows_affected() != 1 {
        return Err(FleetError::LeaseConflict(
            "blocked delivery changed before evidence was recorded".to_owned(),
        ));
    }
    Ok(result.last_insert_rowid())
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error when the block record is unknown or cannot be decoded.
pub async fn blocked_delivery_by_id(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    block_id: i64,
) -> Result<BlockedDelivery, FleetError> {
    let row = sqlx::query(&format!("{} WHERE b.id = ?", blocked_delivery_select()))
        .bind(block_id)
        .fetch_optional(&mut **transaction)
        .await?;
    row.as_ref()
        .map(blocked_delivery_from_row)
        .transpose()?
        .ok_or_else(|| FleetError::NotFound {
            entity: "delivery block",
            id: block_id.to_string(),
        })
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error when stored rows cannot be read or decoded.
pub async fn blocked_delivery_by_lease(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_id: &str,
    lease_token: &str,
) -> Result<Option<(BlockedDelivery, String)>, FleetError> {
    let row = sqlx::query(&format!(
        "{} WHERE b.agent_id = ? AND m.id = ? AND b.lease_token = ?",
        blocked_delivery_select()
    ))
    .bind(agent_id)
    .bind(message_id)
    .bind(lease_token)
    .fetch_optional(&mut **transaction)
    .await?;
    row.as_ref()
        .map(|row| Ok((blocked_delivery_from_row(row)?, row.try_get("reason")?)))
        .transpose()
}

fn blocked_delivery_query(filter: &str) -> String {
    format!(
        "{} WHERE b.resolved_at_ms IS NULL {filter} ORDER BY b.blocked_at_ms, b.id LIMIT 500",
        blocked_delivery_select()
    )
}

const fn blocked_delivery_select() -> &'static str {
    r"
    SELECT b.id AS block_id, b.agent_id, b.attempt, b.reason, b.blocked_at_ms,
           m.seq, m.id, m.channel_id, m.sender_id, m.recipient_id, m.kind,
           m.payload_json, m.correlation_id, m.causation_id, m.created_at_ms
    FROM delivery_blocks b
    JOIN messages m ON m.seq = b.message_seq
    "
}

fn blocked_delivery_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<BlockedDelivery, FleetError> {
    Ok(BlockedDelivery {
        block_id: row.try_get("block_id")?,
        agent_id: row.try_get("agent_id")?,
        message: message_from_row(row)?,
        attempt: row.try_get("attempt")?,
        reason: row.try_get("reason")?,
        blocked_at_ms: row.try_get("blocked_at_ms")?,
    })
}

async fn resolution_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    block_id: i64,
) -> Result<sqlx::sqlite::SqliteRow, FleetError> {
    sqlx::query(
        r"
        SELECT b.id, b.message_seq, b.agent_id, b.resolved_at_ms, b.resolution,
               b.resolution_note, b.retry_after_ms, d.state AS delivery_state
        FROM delivery_blocks b
        JOIN agent_deliveries d
          ON d.message_seq = b.message_seq AND d.agent_id = b.agent_id
        WHERE b.id = ?
        ",
    )
    .bind(block_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| FleetError::NotFound {
        entity: "delivery block",
        id: block_id.to_string(),
    })
}

fn resolution_is_replay(
    row: &sqlx::sqlite::SqliteRow,
    input: &ResolveDeliveryBlock,
    retry_after_ms: i64,
) -> Result<bool, FleetError> {
    let resolved_at_ms: Option<i64> = row.try_get("resolved_at_ms")?;
    if resolved_at_ms.is_none() {
        return Ok(false);
    }
    let resolution: Option<String> = row.try_get("resolution")?;
    let note: Option<String> = row.try_get("resolution_note")?;
    let stored_retry: Option<i64> = row.try_get("retry_after_ms")?;
    if resolution.as_deref() == Some(resolution_name(&input.resolution))
        && note == input.note
        && stored_retry == Some(retry_after_ms)
    {
        return Ok(true);
    }
    Err(FleetError::Conflict(
        "delivery block was already resolved differently".to_owned(),
    ))
}

async fn resolve_delivery_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    row: &sqlx::sqlite::SqliteRow,
    input: &ResolveDeliveryBlock,
    retry_after_ms: i64,
    now: i64,
) -> Result<(), FleetError> {
    let message_seq: i64 = row.try_get("message_seq")?;
    let agent_id: String = row.try_get("agent_id")?;
    let result = match input.resolution {
        BlockResolution::Requeue => {
            let available_at_ms = now
                .checked_add(retry_after_ms)
                .ok_or_else(|| FleetError::Invalid("retry time overflowed".to_owned()))?;
            sqlx::query(
                r"
                UPDATE agent_deliveries
                SET state = 'pending', available_at_ms = ?,
                    last_error = COALESCE(?, last_error),
                    last_settled_lease_token = NULL, last_settlement = NULL
                WHERE message_seq = ? AND agent_id = ? AND state = 'blocked'
                ",
            )
            .bind(available_at_ms)
            .bind(&input.note)
            .bind(message_seq)
            .bind(&agent_id)
            .execute(&mut **transaction)
            .await?
        }
        BlockResolution::Abandon => {
            sqlx::query(
                r"
            UPDATE agent_deliveries
            SET state = 'dead', last_error = COALESCE(?, last_error),
                last_settled_lease_token = NULL, last_settlement = NULL
            WHERE message_seq = ? AND agent_id = ? AND state = 'blocked'
            ",
            )
            .bind(&input.note)
            .bind(message_seq)
            .bind(&agent_id)
            .execute(&mut **transaction)
            .await?
        }
    };
    if result.rows_affected() != 1 {
        return Err(FleetError::Conflict(
            "delivery block changed during resolution".to_owned(),
        ));
    }
    Ok(())
}

const fn resolution_name(resolution: &BlockResolution) -> &'static str {
    match resolution {
        BlockResolution::Requeue => "requeued",
        BlockResolution::Abandon => "abandoned",
    }
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error when the row cannot be decoded.
pub fn delivery_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Delivery, FleetError> {
    Ok(Delivery {
        message: message_from_row(row)?,
        attempt: row.try_get("attempt")?,
        lease_expires_at_ms: row.try_get("lease_expires_at_ms")?,
        last_error: row.try_get("last_error")?,
    })
}

/// Leases the oldest eligible rows in one agent inbox.
///
/// Expired leases are eligible for a later attempt. `message_kinds_json` is a
/// JSON array restricting the lease to exact message kinds; `None` leases any
/// claimable row.
///
/// # Errors
///
/// Returns an error when the update cannot be applied.
pub async fn lease_claimable(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    lease_token: &str,
    lease_expires_at_ms: i64,
    now: i64,
    limit: u32,
    message_kinds_json: Option<&str>,
) -> Result<(), FleetError> {
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
    .bind(lease_token)
    .bind(lease_expires_at_ms)
    .bind(agent_id)
    .bind(now)
    .bind(now)
    .bind(message_kinds_json)
    .bind(message_kinds_json)
    .bind(i64::from(limit))
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Reads the deliveries currently held under one lease token.
///
/// # Errors
///
/// Returns an error when stored rows cannot be read or decoded.
pub async fn leased_batch(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    lease_token: &str,
) -> Result<Vec<Delivery>, FleetError> {
    let rows = sqlx::query(
        r"
        SELECT m.seq, m.id, m.channel_id, m.sender_id, m.recipient_id,
               m.kind, m.payload_json, m.correlation_id, m.causation_id,
               m.created_at_ms, d.attempt, d.lease_expires_at_ms, d.last_error
        FROM agent_deliveries d
        JOIN messages m ON m.seq = d.message_seq
        WHERE d.agent_id = ? AND d.state = 'leased' AND d.lease_token = ?
        ORDER BY m.seq
        ",
    )
    .bind(agent_id)
    .bind(lease_token)
    .fetch_all(&mut **transaction)
    .await?;
    rows.iter().map(delivery_from_row).collect()
}

/// Moves one leased row to `acknowledged`.
///
/// Returns whether the row transitioned. A `false` result means the lease is
/// expired, foreign, or already settled, and the caller must not commit.
///
/// # Errors
///
/// Returns an error when the update cannot be applied.
pub async fn mark_acknowledged(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_id: &str,
    lease_token: &str,
    now: i64,
) -> Result<bool, FleetError> {
    let result = sqlx::query(
        r"
        UPDATE agent_deliveries
        SET state = 'acknowledged',
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            last_settled_lease_token = ?,
            last_settlement = 'acknowledged',
            acknowledged_at_ms = ?
        WHERE agent_id = ?
          AND message_seq = (SELECT seq FROM messages WHERE id = ?)
          AND state = 'leased'
          AND lease_token = ?
          AND lease_expires_at_ms > ?
        ",
    )
    .bind(lease_token)
    .bind(now)
    .bind(agent_id)
    .bind(message_id)
    .bind(lease_token)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Releases one leased row for a later attempt.
///
/// Returns whether the row transitioned.
///
/// # Errors
///
/// Returns an error when the update cannot be applied.
pub async fn mark_retry(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_id: &str,
    input: &RetryDelivery,
    available_at_ms: i64,
    now: i64,
) -> Result<bool, FleetError> {
    let result = sqlx::query(
        r"
        UPDATE agent_deliveries
        SET state = 'pending',
            available_at_ms = ?,
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            last_error = ?,
            last_settled_lease_token = ?,
            last_settlement = 'retry',
            acknowledged_at_ms = NULL
        WHERE agent_id = ?
          AND message_seq = (SELECT seq FROM messages WHERE id = ?)
          AND state = 'leased'
          AND lease_token = ?
          AND lease_expires_at_ms > ?
        ",
    )
    .bind(available_at_ms)
    .bind(&input.error)
    .bind(&input.lease_token)
    .bind(agent_id)
    .bind(message_id)
    .bind(&input.lease_token)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Parks one leased row so it never becomes automatically claimable again.
///
/// Returns whether the row transitioned.
///
/// # Errors
///
/// Returns an error when the update cannot be applied.
pub async fn mark_blocked(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_id: &str,
    input: &BlockDelivery,
    now: i64,
) -> Result<bool, FleetError> {
    let result = sqlx::query(
        r"
        UPDATE agent_deliveries
        SET state = 'blocked',
            lease_token = NULL,
            lease_expires_at_ms = NULL,
            last_error = ?,
            last_settled_lease_token = ?,
            last_settlement = 'blocked',
            acknowledged_at_ms = NULL
        WHERE agent_id = ?
          AND message_seq = (SELECT seq FROM messages WHERE id = ?)
          AND state = 'leased'
          AND lease_token = ?
          AND lease_expires_at_ms > ?
        ",
    )
    .bind(&input.reason)
    .bind(&input.lease_token)
    .bind(agent_id)
    .bind(message_id)
    .bind(&input.lease_token)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Blocks the row an armed lease held, keyed by sequence.
///
/// Returns whether the row transitioned.
///
/// # Errors
///
/// Returns an error when the update cannot be applied.
pub async fn block_expired_lease(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_seq: i64,
    lease_token: &str,
    reason: &str,
    now: i64,
) -> Result<bool, FleetError> {
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
    .bind(reason)
    .bind(lease_token)
    .bind(message_seq)
    .bind(agent_id)
    .bind(lease_token)
    .bind(now)
    .execute(&mut **transaction)
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Records block evidence for a row identified by sequence.
///
/// # Errors
///
/// Returns an error when the insert cannot be applied.
pub async fn record_block(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    message_seq: i64,
    agent_id: &str,
    attempt: i64,
    lease_token: &str,
    reason: &str,
    blocked_at_ms: i64,
) -> Result<(), FleetError> {
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
    .bind(lease_token)
    .bind(reason)
    .bind(blocked_at_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Moves one leased row to `acknowledged`, keyed by sequence.
///
/// Returns whether the row transitioned.
///
/// # Errors
///
/// Returns an error when the update cannot be applied.
pub async fn acknowledge_leased_seq(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    message_seq: i64,
    lease_token: &str,
    now: i64,
) -> Result<bool, FleetError> {
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
    Ok(result.rows_affected() == 1)
}
