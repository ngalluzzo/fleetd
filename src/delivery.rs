use sqlx::Row;
use uuid::Uuid;

use crate::{
    error::FleetError,
    model::{
        BlockDelivery, BlockResolution, BlockedDelivery, ClaimBatch, ClaimDeliveries, Delivery,
        ResolveDeliveryBlock, RetryDelivery,
    },
    store::{Store, message_from_row, now_ms},
};

const MAX_CLAIM_LIMIT: u32 = 100;
const MAX_LEASE_DURATION_MS: u64 = 3_600_000;
const MAX_RETRY_DELAY_MS: u64 = 86_400_000;
const MAX_ERROR_LENGTH: usize = 4_096;

impl Store {
    /// Atomically leases the oldest eligible entries from one agent inbox.
    ///
    /// Expired leases are eligible for a later attempt. An empty batch is a
    /// successful result and means no delivery was currently claimable.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, an unknown agent, or a persistence
    /// failure.
    pub async fn claim_deliveries(
        &self,
        agent_id: &str,
        input: ClaimDeliveries,
    ) -> Result<ClaimBatch, FleetError> {
        validate_claim(&input)?;
        let now = now_ms();
        let lease_duration = i64::try_from(input.lease_duration_ms)
            .map_err(|_| FleetError::Invalid("lease duration is too large".to_owned()))?;
        let lease_expires_at_ms = now
            .checked_add(lease_duration)
            .ok_or_else(|| FleetError::Invalid("lease expiry overflowed".to_owned()))?;
        let lease_token = Uuid::new_v4().to_string();
        ensure_agent(&self.pool, agent_id).await?;
        let mut transaction = self.pool.begin().await?;
        sqlx::query(
            r"
            UPDATE agent_deliveries
            SET state = 'leased',
                attempt = attempt + 1,
                lease_token = ?,
                lease_expires_at_ms = ?
            WHERE rowid IN (
                SELECT rowid
                FROM agent_deliveries
                WHERE agent_id = ?
                  AND available_at_ms <= ?
                  AND (
                    state = 'pending'
                    OR (state = 'leased' AND lease_expires_at_ms <= ?)
                  )
                ORDER BY message_seq
                LIMIT ?
            )
            ",
        )
        .bind(&lease_token)
        .bind(lease_expires_at_ms)
        .bind(agent_id)
        .bind(now)
        .bind(now)
        .bind(i64::from(input.limit))
        .execute(&mut *transaction)
        .await?;
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
        .bind(&lease_token)
        .fetch_all(&mut *transaction)
        .await?;
        let deliveries = rows
            .iter()
            .map(delivery_from_row)
            .collect::<Result<_, _>>()?;
        transaction.commit().await?;
        Ok(ClaimBatch {
            lease_token,
            lease_expires_at_ms,
            deliveries,
        })
    }

    /// Acknowledges successful processing under the active lease.
    ///
    /// Repeating an acknowledgement with the same token is idempotent.
    ///
    /// # Errors
    ///
    /// Returns an error when the delivery is unknown, the lease is expired or
    /// owned by another worker, or persistence fails.
    pub async fn acknowledge_delivery(
        &self,
        agent_id: &str,
        message_id: &str,
        lease_token: &str,
    ) -> Result<(), FleetError> {
        validate_token(lease_token)?;
        let now = now_ms();
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
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        settle_miss(
            &self.pool,
            agent_id,
            message_id,
            lease_token,
            "acknowledged",
        )
        .await
    }

    /// Releases a failed delivery for a later attempt under the active lease.
    ///
    /// Repeating a retry with the same token is idempotent and never disturbs a
    /// newer lease.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid bounds, an unknown delivery, an expired or
    /// foreign lease, or a persistence failure.
    pub async fn retry_delivery(
        &self,
        agent_id: &str,
        message_id: &str,
        input: RetryDelivery,
    ) -> Result<(), FleetError> {
        validate_retry(&input)?;
        let now = now_ms();
        let retry_delay = i64::try_from(input.retry_after_ms)
            .map_err(|_| FleetError::Invalid("retry delay is too large".to_owned()))?;
        let available_at_ms = now
            .checked_add(retry_delay)
            .ok_or_else(|| FleetError::Invalid("retry time overflowed".to_owned()))?;
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
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 1 {
            return Ok(());
        }
        settle_miss(
            &self.pool,
            agent_id,
            message_id,
            &input.lease_token,
            "retry",
        )
        .await
    }

    /// Parks an ambiguously executed delivery under its active lease.
    ///
    /// Repeating the same block operation after a lost response returns the
    /// original block record. Blocked deliveries are never automatically
    /// claimable, including after the former lease expiry.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid evidence, an unknown delivery, an expired
    /// or foreign lease, conflicting replay, or a persistence failure.
    pub async fn block_delivery(
        &self,
        agent_id: &str,
        message_id: &str,
        input: BlockDelivery,
    ) -> Result<(BlockedDelivery, bool), FleetError> {
        validate_block(&input)?;
        let now = now_ms();
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
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
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 1 {
            let block_id =
                insert_block_record(&mut transaction, agent_id, message_id, &input, now).await?;
            let blocked = blocked_delivery_by_id(&mut transaction, block_id).await?;
            transaction.commit().await?;
            return Ok((blocked, true));
        }
        let existing =
            blocked_delivery_by_lease(&mut transaction, agent_id, message_id, &input.lease_token)
                .await?;
        if let Some((blocked, reason)) = existing {
            if reason != input.reason {
                return Err(FleetError::Conflict(
                    "lease was already blocked with different evidence".to_owned(),
                ));
            }
            transaction.commit().await?;
            return Ok((blocked, false));
        }
        transaction.rollback().await?;
        settle_miss(
            &self.pool,
            agent_id,
            message_id,
            &input.lease_token,
            "blocked",
        )
        .await?;
        Err(FleetError::LeaseConflict(
            "blocked settlement evidence is missing".to_owned(),
        ))
    }

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

fn validate_claim(input: &ClaimDeliveries) -> Result<(), FleetError> {
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

fn validate_retry(input: &RetryDelivery) -> Result<(), FleetError> {
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

fn validate_block(input: &BlockDelivery) -> Result<(), FleetError> {
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

fn validate_token(lease_token: &str) -> Result<(), FleetError> {
    if lease_token.trim().is_empty() {
        return Err(FleetError::Invalid(
            "lease token must not be empty".to_owned(),
        ));
    }
    Ok(())
}

async fn ensure_agent(pool: &sqlx::SqlitePool, agent_id: &str) -> Result<(), FleetError> {
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

async fn settle_miss(
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

async fn insert_block_record(
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

async fn blocked_delivery_by_id(
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

async fn blocked_delivery_by_lease(
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

fn delivery_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Delivery, FleetError> {
    Ok(Delivery {
        message: message_from_row(row)?,
        attempt: row.try_get("attempt")?,
        lease_expires_at_ms: row.try_get("lease_expires_at_ms")?,
        last_error: row.try_get("last_error")?,
    })
}
