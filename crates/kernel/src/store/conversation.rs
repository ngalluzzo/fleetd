//! The presentation projection over channels, and direct-pair opening.

use std::collections::HashMap;

use sqlx::Row;
use uuid::Uuid;

use crate::error::FleetError;
use fleetd_proto::model::{
    Channel, ChannelMember, ConversationKind, ConversationSummary, CreateChannelMember,
    OpenDirectConversation,
};

use super::{
    Store, channel::channel_from_row, ensure_exists, membership::channel_member_from_row, now_ms,
};

impl Store {
    /// Lists the common presentation projection for shared and direct conversations.
    ///
    /// # Errors
    ///
    /// Returns an error when durable rows cannot be decoded.
    pub async fn list_conversations(
        &self,
        include_archived: bool,
    ) -> Result<Vec<ConversationSummary>, FleetError> {
        let rows = sqlx::query(&format!(
            "{} WHERE ? OR c.archived_at_ms IS NULL ORDER BY c.created_at_ms, c.id",
            conversation_summary_select()
        ))
        .bind(include_archived)
        .fetch_all(&self.pool)
        .await?;
        let mut members = self.members_by_conversation(include_archived).await?;
        rows.iter()
            .map(|row| {
                let channel_id: String = row.try_get("id")?;
                let members = members.remove(&channel_id).unwrap_or_default();
                conversation_summary_from_row(row, members)
            })
            .collect()
    }

    /// Reads the membership of every listed conversation in one query.
    ///
    /// `list_channel_members` answers for one channel and proves it exists
    /// first, which is the right shape for a caller holding one ID and the
    /// wrong one for a listing: asking per row made a single listing cost two
    /// further queries for every conversation it returned. A conversation with
    /// no members is simply absent here.
    async fn members_by_conversation(
        &self,
        include_archived: bool,
    ) -> Result<HashMap<String, Vec<ChannelMember>>, FleetError> {
        let rows = sqlx::query(
            r"
            SELECT cm.channel_id, cm.agent_id, a.name AS agent_name,
                   cm.joined_at_ms, cm.delivery_mode
            FROM channel_members cm
            JOIN agents a ON a.id = cm.agent_id
            JOIN channels c ON c.id = cm.channel_id
            WHERE ? OR c.archived_at_ms IS NULL
            ORDER BY cm.channel_id, cm.joined_at_ms, cm.agent_id
            ",
        )
        .bind(include_archived)
        .fetch_all(&self.pool)
        .await?;
        let mut grouped: HashMap<String, Vec<ChannelMember>> = HashMap::new();
        for row in &rows {
            let member = channel_member_from_row(row)?;
            grouped
                .entry(member.channel_id.clone())
                .or_default()
                .push(member);
        }
        Ok(grouped)
    }

    /// Idempotently opens the direct conversation for one exact participant pair.
    ///
    /// # Errors
    ///
    /// Returns an error unless exactly two distinct known agents are supplied,
    /// or when an existing pair was opened with different immutable modes.
    pub async fn open_direct_conversation(
        &self,
        input: OpenDirectConversation,
    ) -> Result<OpenDirectConversationResult, FleetError> {
        let members = validate_direct_members(input.members)?;
        let pair_key = direct_pair_key(&members[0].agent_id, &members[1].agent_id);
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        for member in &members {
            ensure_exists(&mut transaction, "agents", "agent", &member.agent_id).await?;
        }
        let (channel, created) =
            open_direct_in_transaction(&mut transaction, &pair_key, &members).await?;
        transaction.commit().await?;
        Ok(OpenDirectConversationResult {
            conversation: self.conversation_summary(&channel.id).await?,
            created,
        })
    }

    async fn conversation_summary(
        &self,
        channel_id: &str,
    ) -> Result<ConversationSummary, FleetError> {
        let row = sqlx::query(&format!("{} WHERE c.id = ?", conversation_summary_select()))
            .bind(channel_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| FleetError::NotFound {
                entity: "channel",
                id: channel_id.to_owned(),
            })?;
        let members = self.list_channel_members(channel_id).await?;
        conversation_summary_from_row(&row, members)
    }
}

/// The durable result of opening one exact direct-conversation pair.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenDirectConversationResult {
    pub conversation: ConversationSummary,
    pub created: bool,
}

fn direct_pair_key(first_agent_id: &str, second_agent_id: &str) -> String {
    format!("{first_agent_id}:{second_agent_id}")
}

fn validate_direct_members(
    mut members: Vec<CreateChannelMember>,
) -> Result<Vec<CreateChannelMember>, FleetError> {
    if members.len() != 2 {
        return Err(FleetError::Invalid(
            "direct conversation requires exactly two participants".to_owned(),
        ));
    }
    members.sort_by(|left, right| left.agent_id.cmp(&right.agent_id));
    if members[0].agent_id == members[1].agent_id {
        return Err(FleetError::Invalid(
            "direct conversation participants must be distinct".to_owned(),
        ));
    }
    Ok(members)
}

async fn open_direct_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    pair_key: &str,
    members: &[CreateChannelMember],
) -> Result<(Channel, bool), FleetError> {
    let existing = sqlx::query(
        r"
        SELECT id, name, conversation_kind, metadata_json, created_at_ms, archived_at_ms
        FROM channels
        WHERE direct_pair_key = ?
        ",
    )
    .bind(pair_key)
    .fetch_optional(&mut **transaction)
    .await?;
    if let Some(row) = existing {
        let channel = channel_from_row(&row)?;
        ensure_direct_modes_match(transaction, &channel.id, members).await?;
        return Ok((channel, false));
    }
    Ok((
        insert_direct_conversation(transaction, pair_key, members).await?,
        true,
    ))
}

async fn ensure_direct_modes_match(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    channel_id: &str,
    members: &[CreateChannelMember],
) -> Result<(), FleetError> {
    let existing: Vec<(String, String)> = sqlx::query_as(
        r"
        SELECT agent_id, delivery_mode
        FROM channel_members
        WHERE channel_id = ?
        ORDER BY agent_id
        ",
    )
    .bind(channel_id)
    .fetch_all(&mut **transaction)
    .await?;
    let requested: Vec<_> = members
        .iter()
        .map(|member| {
            (
                member.agent_id.clone(),
                member.delivery_mode.as_str().to_owned(),
            )
        })
        .collect();
    if existing != requested {
        return Err(FleetError::Conflict(
            "direct conversation participant delivery modes are immutable".to_owned(),
        ));
    }
    Ok(())
}

async fn insert_direct_conversation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    pair_key: &str,
    members: &[CreateChannelMember],
) -> Result<Channel, FleetError> {
    let created_at_ms = now_ms();
    let channel = Channel {
        id: Uuid::new_v4().to_string(),
        name: format!("direct-{}", Uuid::new_v4()),
        kind: ConversationKind::Direct,
        metadata: serde_json::json!({}),
        created_at_ms,
        archived_at_ms: None,
    };
    sqlx::query(
        r"
        INSERT INTO channels (
            id, name, conversation_kind, direct_pair_key,
            metadata_json, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?)
        ",
    )
    .bind(&channel.id)
    .bind(&channel.name)
    .bind(channel.kind.as_str())
    .bind(pair_key)
    .bind(serde_json::to_string(&channel.metadata)?)
    .bind(channel.created_at_ms)
    .execute(&mut **transaction)
    .await?;
    for member in members {
        sqlx::query(
            r"
            INSERT INTO channel_members (
                channel_id, agent_id, joined_at_ms, delivery_mode
            ) VALUES (?, ?, ?, ?)
            ",
        )
        .bind(&channel.id)
        .bind(&member.agent_id)
        .bind(created_at_ms)
        .bind(member.delivery_mode.as_str())
        .execute(&mut **transaction)
        .await?;
    }
    Ok(channel)
}

/// The presentation projection every conversation read shares.
///
/// Both reads select the same columns and the same two correlated lookups for
/// the latest message, so what a conversation looks like is stated once and
/// each caller adds only its own filter and ordering.
fn conversation_summary_select() -> &'static str {
    r"
    SELECT c.id, c.name, c.conversation_kind, c.metadata_json,
           c.created_at_ms, c.archived_at_ms,
           (
               SELECT m.seq FROM messages m
               WHERE m.channel_id = c.id
               ORDER BY m.seq DESC LIMIT 1
           ) AS latest_message_seq,
           (
               SELECT m.created_at_ms FROM messages m
               WHERE m.channel_id = c.id
               ORDER BY m.seq DESC LIMIT 1
           ) AS latest_message_at_ms
    FROM channels c
    "
}

/// Assembles one summary from its projected row and its membership.
///
/// Members are passed in rather than read here, so a listing can read every
/// conversation's membership at once instead of once per row.
fn conversation_summary_from_row(
    row: &sqlx::sqlite::SqliteRow,
    members: Vec<ChannelMember>,
) -> Result<ConversationSummary, FleetError> {
    let channel = channel_from_row(row)?;
    Ok(ConversationSummary {
        members,
        id: channel.id,
        name: channel.name,
        kind: channel.kind,
        metadata: channel.metadata,
        created_at_ms: channel.created_at_ms,
        archived_at_ms: channel.archived_at_ms,
        latest_message_seq: row.try_get("latest_message_seq")?,
        latest_message_at_ms: row.try_get("latest_message_at_ms")?,
    })
}
