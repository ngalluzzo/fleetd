//! Delivery settlement composed with the invocation fence.
//!
//! A delivery and the invocation fencing it settle together or not at all. The
//! kernel owns the delivery row's state machine and the fence owns the
//! invocation record; neither knows about the other, so this layer opens the
//! transaction, applies both, and commits once.
//!
//! Each entry point is a free function over a [`Store`] rather than a method on
//! it. The kernel owns that type, and the composition is deliberately visible
//! at the call site.

use uuid::Uuid;

use crate::{
    delivery::{
        blocked_delivery_by_id, blocked_delivery_by_lease, ensure_agent, insert_block_record,
        lease_claimable, leased_batch, mark_acknowledged, mark_blocked, mark_retry, settle_miss,
        validate_block, validate_claim, validate_retry, validate_token,
    },
    error::FleetError,
    invocation::{ensure_retry_is_safe, recover_expired_invocations, terminalize_invocation},
    model::{
        BlockDelivery, BlockedDelivery, ClaimBatch, ClaimDeliveries, ExecutionCertainty,
        RetryDelivery,
    },
    store::{Store, now_ms},
};

/// Atomically leases the oldest eligible entries from one agent inbox.
///
/// Expired invocations are recovered first, in the same transaction, so a
/// delivery whose armed turn outcome is unknown is never handed out again.
/// An empty batch is a successful result and means no delivery was claimable.
///
/// # Errors
///
/// Returns an error for invalid bounds, an unknown agent, or a persistence
/// failure.
pub async fn claim_deliveries(
    store: &Store,
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
    ensure_agent(store.pool(), agent_id).await?;
    let mut transaction = store.begin_immediate().await?;
    recover_expired_invocations(&mut transaction, agent_id, now).await?;
    lease_claimable(
        &mut transaction,
        agent_id,
        &lease_token,
        lease_expires_at_ms,
        now,
        input.limit,
        None,
    )
    .await?;
    let deliveries = leased_batch(&mut transaction, agent_id, &lease_token).await?;
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
    store: &Store,
    agent_id: &str,
    message_id: &str,
    lease_token: &str,
) -> Result<(), FleetError> {
    validate_token(lease_token)?;
    let now = now_ms();
    let mut transaction = store.begin_immediate().await?;
    if mark_acknowledged(&mut transaction, agent_id, message_id, lease_token, now).await? {
        terminalize_invocation(
            &mut transaction,
            agent_id,
            message_id,
            lease_token,
            ExecutionCertainty::OutcomeKnown,
            "acknowledged",
            now,
        )
        .await?;
        transaction.commit().await?;
        return Ok(());
    }
    transaction.rollback().await?;
    settle_miss(
        store.pool(),
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
    store: &Store,
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
    let mut transaction = store.begin_immediate().await?;
    ensure_retry_is_safe(&mut transaction, agent_id, message_id, &input.lease_token).await?;
    if mark_retry(
        &mut transaction,
        agent_id,
        message_id,
        &input,
        available_at_ms,
        now,
    )
    .await?
    {
        terminalize_invocation(
            &mut transaction,
            agent_id,
            message_id,
            &input.lease_token,
            ExecutionCertainty::NotStarted,
            "retry",
            now,
        )
        .await?;
        transaction.commit().await?;
        return Ok(());
    }
    transaction.rollback().await?;
    settle_miss(
        store.pool(),
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
/// Returns an error for invalid evidence, an unknown delivery, an expired or
/// foreign lease, conflicting replay, or a persistence failure.
pub async fn block_delivery(
    store: &Store,
    agent_id: &str,
    message_id: &str,
    input: BlockDelivery,
) -> Result<(BlockedDelivery, bool), FleetError> {
    validate_block(&input)?;
    let now = now_ms();
    let mut transaction = store.begin_immediate().await?;
    if mark_blocked(&mut transaction, agent_id, message_id, &input, now).await? {
        let block_id =
            insert_block_record(&mut transaction, agent_id, message_id, &input, now).await?;
        terminalize_invocation(
            &mut transaction,
            agent_id,
            message_id,
            &input.lease_token,
            ExecutionCertainty::OutcomeUnknown,
            "blocked",
            now,
        )
        .await?;
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
        store.pool(),
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
