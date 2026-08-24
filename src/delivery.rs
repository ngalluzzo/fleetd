use sqlx::Row;
use uuid::Uuid;

use crate::{
    error::FleetError,
    model::{ClaimBatch, ClaimDeliveries, Delivery, RetryDelivery},
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

fn delivery_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Delivery, FleetError> {
    Ok(Delivery {
        message: message_from_row(row)?,
        attempt: row.try_get("attempt")?,
        lease_expires_at_ms: row.try_get("lease_expires_at_ms")?,
        last_error: row.try_get("last_error")?,
    })
}
