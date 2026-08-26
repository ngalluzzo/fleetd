//! Immutable messages, idempotent append, and the delivery snapshot.

use sqlx::Row;
use uuid::Uuid;

use crate::error::FleetError;
use fleetd_proto::model::{CreateMessage, Message, MessagePage};

use super::{
    Store,
    channel::{channel_from_row, channel_row},
    membership::ensure_member,
    now_ms, parse_json,
};

impl Store {
    /// Appends one immutable message after validating its channel membership.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, unknown entities, invalid membership,
    /// or a persistence failure.
    pub async fn append_message(
        &self,
        channel_id: &str,
        input: CreateMessage,
    ) -> Result<Message, FleetError> {
        Ok(self
            .append_message_idempotent(channel_id, input)
            .await?
            .message)
    }

    /// Appends one immutable message or returns the existing identical message
    /// for an agent-scoped idempotency key.
    ///
    /// `created` is false only when the same sender previously used the key for
    /// an identical message. Reusing a key for different content is a conflict.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid input, unknown entities, invalid membership,
    /// conflicting idempotency-key reuse, or a persistence failure.
    pub async fn append_message_idempotent(
        &self,
        channel_id: &str,
        input: CreateMessage,
    ) -> Result<AppendMessageResult, FleetError> {
        if input.kind.trim().is_empty() {
            return Err(FleetError::Invalid(
                "message kind must not be empty".to_owned(),
            ));
        }
        validate_idempotency_key(input.idempotency_key.as_deref())?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        if let Some(idempotency_key) = &input.idempotency_key {
            let existing = sqlx::query(
                r"
                SELECT seq, id, channel_id, sender_id, recipient_id, kind, payload_json,
                       correlation_id, causation_id, created_at_ms
                FROM messages
                WHERE sender_id = ? AND idempotency_key = ?
                ",
            )
            .bind(&input.sender_id)
            .bind(idempotency_key)
            .fetch_optional(&mut *transaction)
            .await?;
            if let Some(row) = existing {
                let message = message_from_row(&row)?;
                if !message_matches_input(&message, channel_id, &input) {
                    return Err(FleetError::Conflict(
                        "idempotency key was already used for a different message".to_owned(),
                    ));
                }
                transaction.commit().await?;
                return Ok(AppendMessageResult {
                    message,
                    created: false,
                });
            }
        }

        let message = insert_message(&mut transaction, channel_id, input).await?;
        transaction.commit().await?;
        self.notify_message_commit(true);
        Ok(AppendMessageResult {
            message,
            created: true,
        })
    }

    /// Reads one exact immutable message by stable identity.
    ///
    /// # Errors
    ///
    /// Returns not found for an unknown message, or a decoding error for
    /// invalid persisted state.
    pub async fn get_message(&self, message_id: &str) -> Result<Message, FleetError> {
        let row = sqlx::query(
            r"
            SELECT seq, id, channel_id, sender_id, recipient_id, kind,
                   payload_json, correlation_id, causation_id, created_at_ms
            FROM messages
            WHERE id = ?
            ",
        )
        .bind(message_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| FleetError::NotFound {
            entity: "message",
            id: message_id.to_owned(),
        })?;
        message_from_row(&row)
    }

    /// Reads a page of channel messages strictly after the supplied cursor.
    ///
    /// Every authorized reader of a channel sees the same immutable history.
    /// `recipient_id` addresses inbox delivery and does not narrow channel
    /// visibility; private communication belongs in a `direct` conversation,
    /// whose two-member membership is the boundary. Authorization happens
    /// before this call, which does not re-check it.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid cursor, an unknown channel, or a read or
    /// decoding failure.
    pub async fn list_messages(
        &self,
        channel_id: &str,
        after: i64,
        limit: u32,
    ) -> Result<MessagePage, FleetError> {
        if after < 0 {
            return Err(FleetError::Invalid(
                "message cursor must not be negative".to_owned(),
            ));
        }
        let limit = limit.clamp(1, 500);
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
            SELECT seq, id, channel_id, sender_id, recipient_id, kind, payload_json,
                   correlation_id, causation_id, created_at_ms
            FROM messages
            WHERE channel_id = ? AND seq > ?
            ORDER BY seq
            LIMIT ?
            ",
        )
        .bind(channel_id)
        .bind(after)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        let messages: Vec<_> = rows
            .iter()
            .map(message_from_row)
            .collect::<Result<_, _>>()?;
        let next_cursor = messages.last().map_or(after, |message| message.seq);
        Ok(MessagePage {
            messages,
            next_cursor,
        })
    }
}

const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 256;

/// The durable result of appending a message with optional idempotency.
#[derive(Clone, Debug, PartialEq)]
pub struct AppendMessageResult {
    pub message: Message,
    pub created: bool,
}

fn validate_idempotency_key(idempotency_key: Option<&str>) -> Result<(), FleetError> {
    let Some(idempotency_key) = idempotency_key else {
        return Ok(());
    };
    if idempotency_key.trim().is_empty() {
        return Err(FleetError::Invalid(
            "idempotency key must not be empty".to_owned(),
        ));
    }
    if idempotency_key.len() > MAX_IDEMPOTENCY_KEY_LENGTH {
        return Err(FleetError::Invalid(format!(
            "idempotency key must not exceed {MAX_IDEMPOTENCY_KEY_LENGTH} bytes"
        )));
    }
    Ok(())
}

fn message_matches_input(message: &Message, channel_id: &str, input: &CreateMessage) -> bool {
    message.channel_id == channel_id
        && message.sender_id == input.sender_id
        && message.recipient_id == input.recipient_id
        && message.kind == input.kind
        && message.payload == input.payload
        && message.correlation_id == input.correlation_id
        && message.causation_id == input.causation_id
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error for an unknown channel, a non-member sender or recipient, a conflicting idempotency key, or a persistence failure.
pub async fn insert_message(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    channel_id: &str,
    input: CreateMessage,
) -> Result<Message, FleetError> {
    let channel = channel_from_row(&channel_row(transaction, channel_id).await?)?;
    if channel.archived_at_ms.is_some() {
        return Err(FleetError::Conflict(format!(
            "channel is archived: {channel_id}"
        )));
    }
    ensure_member(transaction, channel_id, &input.sender_id).await?;
    if let Some(recipient_id) = &input.recipient_id {
        ensure_member(transaction, channel_id, recipient_id).await?;
    }
    let id = Uuid::new_v4().to_string();
    let created_at_ms = now_ms();
    let payload_json = serde_json::to_string(&input.payload)?;
    let result = sqlx::query(
        r"
        INSERT INTO messages (
            id, channel_id, sender_id, idempotency_key, recipient_id, kind,
            payload_json, correlation_id, causation_id, created_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ",
    )
    .bind(&id)
    .bind(channel_id)
    .bind(&input.sender_id)
    .bind(&input.idempotency_key)
    .bind(&input.recipient_id)
    .bind(&input.kind)
    .bind(payload_json)
    .bind(&input.correlation_id)
    .bind(&input.causation_id)
    .bind(created_at_ms)
    .execute(&mut **transaction)
    .await?;
    let message_seq = result.last_insert_rowid();
    insert_delivery_snapshot(
        transaction,
        channel_id,
        &input.sender_id,
        input.recipient_id.as_deref(),
        message_seq,
        created_at_ms,
    )
    .await?;
    Ok(Message {
        seq: message_seq,
        id,
        channel_id: channel_id.to_owned(),
        sender_id: input.sender_id,
        recipient_id: input.recipient_id,
        kind: input.kind,
        payload: input.payload,
        correlation_id: input.correlation_id,
        causation_id: input.causation_id,
        created_at_ms,
    })
}

async fn insert_delivery_snapshot(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    channel_id: &str,
    sender_id: &str,
    recipient_id: Option<&str>,
    message_seq: i64,
    created_at_ms: i64,
) -> Result<(), FleetError> {
    if let Some(recipient_id) = recipient_id {
        sqlx::query(
            r"
            INSERT INTO agent_deliveries (
                message_seq, agent_id, available_at_ms, created_at_ms
            )
            SELECT ?, ?, ?, ?
            FROM channel_members
            WHERE channel_id = ? AND agent_id = ? AND delivery_mode = 'inbox'
            ",
        )
        .bind(message_seq)
        .bind(recipient_id)
        .bind(created_at_ms)
        .bind(created_at_ms)
        .bind(channel_id)
        .bind(recipient_id)
        .execute(&mut **transaction)
        .await?;
    } else {
        sqlx::query(
            r"
            INSERT INTO agent_deliveries (
                message_seq, agent_id, available_at_ms, created_at_ms
            )
            SELECT ?, agent_id, ?, ?
            FROM channel_members
            WHERE channel_id = ? AND agent_id != ? AND delivery_mode = 'inbox'
            ",
        )
        .bind(message_seq)
        .bind(created_at_ms)
        .bind(created_at_ms)
        .bind(channel_id)
        .bind(sender_id)
        .execute(&mut **transaction)
        .await?;
    }
    Ok(())
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns an error when the row cannot be decoded.
pub fn message_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Message, FleetError> {
    Ok(Message {
        seq: row.try_get("seq")?,
        id: row.try_get("id")?,
        channel_id: row.try_get("channel_id")?,
        sender_id: row.try_get("sender_id")?,
        recipient_id: row.try_get("recipient_id")?,
        kind: row.try_get("kind")?,
        payload: parse_json(&row.try_get::<String, _>("payload_json")?)?,
        correlation_id: row.try_get("correlation_id")?,
        causation_id: row.try_get("causation_id")?,
        created_at_ms: row.try_get("created_at_ms")?,
    })
}
