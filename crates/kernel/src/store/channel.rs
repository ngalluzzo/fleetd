//! Durable channels: creation, naming, and archival.

use std::collections::HashSet;

use serde_json::Value;
use sqlx::Row;
use uuid::Uuid;

use crate::error::FleetError;
use fleetd_proto::model::{
    Channel, ConversationKind, CreateChannel, CreateChannelMember, MembershipDeliveryMode,
};

use super::{Store, ensure_exists, map_unique_conflict, now_ms, parse_json, validate_name};

impl Store {
    /// Creates a channel with an initial set of members.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, unknown members, or a persistence failure.
    pub async fn create_channel(&self, input: CreateChannel) -> Result<Channel, FleetError> {
        let mut seen = HashSet::new();
        let mut members = Vec::with_capacity(input.member_ids.len() + input.members.len());
        for agent_id in input.member_ids {
            if !seen.insert(agent_id.clone()) {
                return Err(FleetError::Invalid(format!(
                    "duplicate initial channel member: {agent_id}"
                )));
            }
            members.push(CreateChannelMember {
                agent_id,
                delivery_mode: MembershipDeliveryMode::Inbox,
            });
        }
        for member in input.members {
            if !seen.insert(member.agent_id.clone()) {
                return Err(FleetError::Invalid(format!(
                    "duplicate initial channel member: {}",
                    member.agent_id
                )));
            }
            members.push(member);
        }
        self.create_channel_with_members(input.name, input.metadata, members)
            .await
    }

    /// Creates a channel with exact immutable delivery modes for every initial
    /// membership.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, duplicate or unknown members, or a
    /// persistence failure.
    pub async fn create_channel_with_members(
        &self,
        name: String,
        metadata: Value,
        members: Vec<CreateChannelMember>,
    ) -> Result<Channel, FleetError> {
        validate_name("channel", &name)?;
        let mut seen = HashSet::new();
        for member in &members {
            if !seen.insert(&member.agent_id) {
                return Err(FleetError::Invalid(format!(
                    "duplicate initial channel member: {}",
                    member.agent_id
                )));
            }
        }
        let mut transaction = self.pool.begin().await?;
        for member in &members {
            ensure_exists(&mut transaction, "agents", "agent", &member.agent_id).await?;
        }
        let channel = Channel {
            id: Uuid::new_v4().to_string(),
            name,
            kind: ConversationKind::Shared,
            metadata,
            created_at_ms: now_ms(),
            archived_at_ms: None,
        };
        let metadata_json = serde_json::to_string(&channel.metadata)?;
        let result = sqlx::query(
            "INSERT INTO channels (id, name, metadata_json, created_at_ms) VALUES (?, ?, ?, ?)",
        )
        .bind(&channel.id)
        .bind(&channel.name)
        .bind(metadata_json)
        .bind(channel.created_at_ms)
        .execute(&mut *transaction)
        .await;
        map_unique_conflict(result, "channel name")?;
        for member in members {
            sqlx::query(
                r"
                INSERT INTO channel_members (
                    channel_id, agent_id, joined_at_ms, delivery_mode
                ) VALUES (?, ?, ?, ?)
                ",
            )
            .bind(&channel.id)
            .bind(member.agent_id)
            .bind(channel.created_at_ms)
            .bind(member.delivery_mode.as_str())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(channel)
    }

    /// Lists all channels in creation order.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored rows cannot be read or decoded.
    pub async fn list_channels(&self) -> Result<Vec<Channel>, FleetError> {
        let rows = sqlx::query(
            r"
            SELECT id, name, conversation_kind, metadata_json, created_at_ms, archived_at_ms
            FROM channels
            ORDER BY created_at_ms, id
            ",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(channel_from_row).collect()
    }

    /// Renames an active shared channel.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid name, unknown channel, direct
    /// conversation, archived channel, or duplicate shared-channel name.
    pub async fn rename_channel(
        &self,
        channel_id: &str,
        name: String,
    ) -> Result<Channel, FleetError> {
        validate_name("channel", &name)?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = channel_row(&mut transaction, channel_id).await?;
        let channel = channel_from_row(&row)?;
        require_mutable_shared_channel(&channel)?;
        let result = sqlx::query("UPDATE channels SET name = ? WHERE id = ?")
            .bind(&name)
            .bind(channel_id)
            .execute(&mut *transaction)
            .await;
        map_unique_conflict(result, "channel name")?;
        transaction.commit().await?;
        Ok(Channel { name, ..channel })
    }

    /// Archives a shared channel without deleting its membership or history.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown channel or direct conversation.
    pub async fn archive_channel(&self, channel_id: &str) -> Result<Channel, FleetError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let row = channel_row(&mut transaction, channel_id).await?;
        let mut channel = channel_from_row(&row)?;
        if channel.kind != ConversationKind::Shared {
            return Err(FleetError::Conflict(
                "direct conversations cannot be archived".to_owned(),
            ));
        }
        if channel.archived_at_ms.is_none() {
            let archived_at_ms = now_ms();
            sqlx::query("UPDATE channels SET archived_at_ms = ? WHERE id = ?")
                .bind(archived_at_ms)
                .bind(channel_id)
                .execute(&mut *transaction)
                .await?;
            channel.archived_at_ms = Some(archived_at_ms);
        }
        transaction.commit().await?;
        Ok(channel)
    }
}

pub(super) async fn channel_row(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    channel_id: &str,
) -> Result<sqlx::sqlite::SqliteRow, FleetError> {
    sqlx::query(
        r"
        SELECT id, name, conversation_kind, metadata_json, created_at_ms, archived_at_ms
        FROM channels
        WHERE id = ?
        ",
    )
    .bind(channel_id)
    .fetch_optional(&mut **transaction)
    .await?
    .ok_or_else(|| FleetError::NotFound {
        entity: "channel",
        id: channel_id.to_owned(),
    })
}

pub(super) fn require_mutable_shared_channel(channel: &Channel) -> Result<(), FleetError> {
    if channel.kind != ConversationKind::Shared {
        return Err(FleetError::Conflict(
            "direct conversation membership and name are immutable".to_owned(),
        ));
    }
    if channel.archived_at_ms.is_some() {
        return Err(FleetError::Conflict(format!(
            "channel is archived: {}",
            channel.id
        )));
    }
    Ok(())
}

pub(super) fn channel_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Channel, FleetError> {
    let stored_kind: String = row.try_get("conversation_kind")?;
    let kind = ConversationKind::parse(&stored_kind).ok_or_else(|| {
        FleetError::Invalid(format!(
            "stored channel has unknown conversation kind: {stored_kind}"
        ))
    })?;
    Ok(Channel {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        kind,
        metadata: parse_json(&row.try_get::<String, _>("metadata_json")?)?,
        created_at_ms: row.try_get("created_at_ms")?,
        archived_at_ms: row.try_get("archived_at_ms")?,
    })
}
