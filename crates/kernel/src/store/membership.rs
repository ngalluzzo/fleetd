//! Immutable channel membership and its delivery mode.

use sqlx::Row;

use crate::error::FleetError;
use fleetd_proto::model::{ChannelMember, MembershipDeliveryMode};

use super::{
    Store,
    channel::{channel_from_row, channel_row, require_mutable_shared_channel},
    ensure_exists, now_ms,
};

impl Store {
    /// Adds an agent to a channel. Repeating the operation is harmless.
    ///
    /// # Errors
    ///
    /// Returns an error when the channel or agent is unknown or persistence fails.
    pub async fn add_member(&self, channel_id: &str, agent_id: &str) -> Result<(), FleetError> {
        self.add_member_with_mode(channel_id, agent_id, MembershipDeliveryMode::Inbox)
            .await
    }

    /// Adds one membership with an exact immutable delivery mode. Repeating
    /// the same membership is idempotent; changing its mode conflicts.
    ///
    /// # Errors
    ///
    /// Returns an error when the channel or agent is unknown, the existing
    /// membership uses another mode, or persistence fails.
    pub async fn add_member_with_mode(
        &self,
        channel_id: &str,
        agent_id: &str,
        delivery_mode: MembershipDeliveryMode,
    ) -> Result<(), FleetError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let channel = channel_from_row(&channel_row(&mut transaction, channel_id).await?)?;
        require_mutable_shared_channel(&channel)?;
        ensure_exists(&mut transaction, "agents", "agent", agent_id).await?;
        let existing: Option<String> = sqlx::query_scalar(
            "SELECT delivery_mode FROM channel_members WHERE channel_id = ? AND agent_id = ?",
        )
        .bind(channel_id)
        .bind(agent_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(existing) = existing {
            if existing != delivery_mode.as_str() {
                return Err(FleetError::Conflict(format!(
                    "channel membership delivery mode is immutable: {channel_id}/{agent_id}"
                )));
            }
            transaction.commit().await?;
            return Ok(());
        }
        sqlx::query(
            r"
            INSERT INTO channel_members (
                channel_id, agent_id, joined_at_ms, delivery_mode
            ) VALUES (?, ?, ?, ?)
            ",
        )
        .bind(channel_id)
        .bind(agent_id)
        .bind(now_ms())
        .bind(delivery_mode.as_str())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Lists the exact immutable memberships for one channel.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown channel or an undecodable stored mode.
    pub async fn list_channel_members(
        &self,
        channel_id: &str,
    ) -> Result<Vec<ChannelMember>, FleetError> {
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels WHERE id = ?")
            .bind(channel_id)
            .fetch_one(&self.pool)
            .await?;
        if exists == 0 {
            return Err(FleetError::NotFound {
                entity: "channel",
                id: channel_id.to_owned(),
            });
        }
        let rows = sqlx::query(
            r"
            SELECT cm.channel_id, cm.agent_id, a.name AS agent_name,
                   cm.joined_at_ms, cm.delivery_mode
            FROM channel_members cm
            JOIN agents a ON a.id = cm.agent_id
            WHERE cm.channel_id = ?
            ORDER BY cm.joined_at_ms, cm.agent_id
            ",
        )
        .bind(channel_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(channel_member_from_row).collect()
    }

    /// Returns whether an agent is currently a member of a channel.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown channel or a persistence failure.
    pub async fn is_member(&self, channel_id: &str, agent_id: &str) -> Result<bool, FleetError> {
        let channel_exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM channels WHERE id = ?")
            .bind(channel_id)
            .fetch_one(&self.pool)
            .await?;
        if channel_exists == 0 {
            return Err(FleetError::NotFound {
                entity: "channel",
                id: channel_id.to_owned(),
            });
        }
        let membership: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM channel_members WHERE channel_id = ? AND agent_id = ?",
        )
        .bind(channel_id)
        .bind(agent_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(membership == 1)
    }
}

pub(super) async fn ensure_member(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    channel_id: &str,
    agent_id: &str,
) -> Result<(), FleetError> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM channel_members WHERE channel_id = ? AND agent_id = ?",
    )
    .bind(channel_id)
    .bind(agent_id)
    .fetch_one(&mut **transaction)
    .await?;
    if count == 0 {
        return Err(FleetError::NotMember {
            agent_id: agent_id.to_owned(),
            channel_id: channel_id.to_owned(),
        });
    }
    Ok(())
}

pub(super) fn channel_member_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<ChannelMember, FleetError> {
    let stored_mode: String = row.try_get("delivery_mode")?;
    let delivery_mode = MembershipDeliveryMode::parse(&stored_mode).ok_or_else(|| {
        FleetError::Invalid(format!(
            "stored channel membership has unknown delivery mode: {stored_mode}"
        ))
    })?;
    Ok(ChannelMember {
        channel_id: row.try_get("channel_id")?,
        agent_id: row.try_get("agent_id")?,
        agent_name: row.try_get("agent_name")?,
        joined_at_ms: row.try_get("joined_at_ms")?,
        delivery_mode,
    })
}
