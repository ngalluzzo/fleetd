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
use fleetd_proto::model::{ChannelMember, ConversationSummary};
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
