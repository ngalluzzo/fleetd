//! How a channel is presented as a conversation.
//!
//! The substrate stores a routing scope, its immutable membership, and an
//! ordered log of messages. A *conversation* is what those look like to someone
//! reading them: the participants, and how recently anything was said.
//!
//! This is a read model and nothing else. It holds no pool, opens no
//! transaction, and writes no row -- opening a direct pair is a substrate write
//! and lives in the kernel. A caller that needs both composes them, which is
//! why the kernel has no idea this crate exists.

use std::collections::HashMap;

use fleetd_kernel::{
    error::FleetError,
    store::{Store, channel::channel_from_row, membership::channel_member_from_row},
};
use fleetd_proto::model::{ChannelMember, ConversationAttention, ConversationSummary};
use sqlx::Row;

/// Lists every conversation, most recently created last.
///
/// # Errors
///
/// Returns an error when durable rows cannot be decoded.
pub async fn list(
    store: &Store,
    include_archived: bool,
) -> Result<Vec<ConversationSummary>, FleetError> {
    let rows = sqlx::query(&format!(
        "{} WHERE ? OR c.archived_at_ms IS NULL ORDER BY c.created_at_ms, c.id",
        summary_select()
    ))
    .bind(include_archived)
    .fetch_all(store.pool())
    .await?;
    let mut members = members_by_conversation(store, include_archived).await?;
    rows.iter()
        .map(|row| {
            let channel_id: String = row.try_get("id")?;
            let members = members.remove(&channel_id).unwrap_or_default();
            summary_from_row(row, members)
        })
        .collect()
}

/// Presents one channel as a conversation.
///
/// # Errors
///
/// Returns an error for an unknown channel, or when rows cannot be decoded.
pub async fn summary(store: &Store, channel_id: &str) -> Result<ConversationSummary, FleetError> {
    let row = sqlx::query(&format!("{} WHERE c.id = ?", summary_select()))
        .bind(channel_id)
        .fetch_optional(store.pool())
        .await?
        .ok_or_else(|| FleetError::NotFound {
            entity: "channel",
            id: channel_id.to_owned(),
        })?;
    let members = store.list_channel_members(channel_id).await?;
    summary_from_row(&row, members)
}

/// Lists exact unread state for every conversation the participant belongs to.
///
/// # Errors
///
/// Returns an error when durable rows cannot be decoded.
pub async fn attention(
    store: &Store,
    agent_id: &str,
) -> Result<Vec<ConversationAttention>, FleetError> {
    let rows = sqlx::query(&format!(
        "{} WHERE cm.agent_id = ? GROUP BY cm.channel_id, cm.read_through_seq \
         ORDER BY cm.joined_at_ms, cm.channel_id",
        attention_select()
    ))
    .bind(agent_id)
    .fetch_all(store.pool())
    .await?;
    rows.iter().map(attention_from_row).collect()
}

/// Reads exact unread state for one participant membership.
///
/// # Errors
///
/// Returns an error when the membership does not exist or durable rows cannot
/// be decoded.
pub async fn attention_for(
    store: &Store,
    agent_id: &str,
    channel_id: &str,
) -> Result<ConversationAttention, FleetError> {
    let row = sqlx::query(&format!(
        "{} WHERE cm.agent_id = ? AND cm.channel_id = ? \
         GROUP BY cm.channel_id, cm.read_through_seq",
        attention_select()
    ))
    .bind(agent_id)
    .bind(channel_id)
    .fetch_optional(store.pool())
    .await?
    .ok_or_else(|| FleetError::NotMember {
        agent_id: agent_id.to_owned(),
        channel_id: channel_id.to_owned(),
    })?;
    attention_from_row(&row)
}

/// Reads the membership of every listed conversation in one query.
///
/// `Store::list_channel_members` answers for one channel and proves it exists
/// first, which is the right shape for a caller holding one ID and the wrong one
/// for a listing: asking per row made a single listing cost two further queries
/// for every conversation it returned. A conversation with no members is simply
/// absent here.
async fn members_by_conversation(
    store: &Store,
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
    .fetch_all(store.pool())
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

/// The presentation projection every conversation read shares.
///
/// Both reads select the same columns and the same two correlated lookups for
/// the latest message, so what a conversation looks like is stated once and
/// each caller adds only its own filter and ordering.
fn summary_select() -> &'static str {
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

/// One participant-specific read model over membership and immutable messages.
fn attention_select() -> &'static str {
    r"
    SELECT cm.channel_id, cm.read_through_seq,
           MAX(m.seq) AS latest_message_seq,
           SUM(CASE
               WHEN m.seq > cm.read_through_seq AND m.sender_id <> cm.agent_id
               THEN 1 ELSE 0 END)
               AS unread_count,
           SUM(CASE
               WHEN m.seq > cm.read_through_seq
                    AND m.sender_id <> cm.agent_id
                    AND m.recipient_id = cm.agent_id
               THEN 1 ELSE 0 END) AS addressed_unread_count,
           MIN(CASE
               WHEN m.seq > cm.read_through_seq AND m.sender_id <> cm.agent_id
               THEN m.seq END)
               AS first_unread_seq,
           MIN(CASE
               WHEN m.seq > cm.read_through_seq
                    AND m.sender_id <> cm.agent_id
                    AND m.recipient_id = cm.agent_id
               THEN m.seq END) AS first_addressed_unread_seq,
           cm.joined_at_ms
    FROM channel_members cm
    LEFT JOIN messages m ON m.channel_id = cm.channel_id
    "
}

fn attention_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ConversationAttention, FleetError> {
    Ok(ConversationAttention {
        channel_id: row.try_get("channel_id")?,
        read_through_seq: row.try_get("read_through_seq")?,
        latest_message_seq: row.try_get("latest_message_seq")?,
        unread_count: row.try_get("unread_count")?,
        addressed_unread_count: row.try_get("addressed_unread_count")?,
        first_unread_seq: row.try_get("first_unread_seq")?,
        first_addressed_unread_seq: row.try_get("first_addressed_unread_seq")?,
    })
}

/// Assembles one summary from its projected row and its membership.
///
/// Members are passed in rather than read here, so a listing can read every
/// conversation's membership at once instead of once per row.
fn summary_from_row(
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
