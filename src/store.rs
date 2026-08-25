use std::{collections::HashSet, path::Path, time::Duration, time::SystemTime};

use serde_json::Value;
use sqlx::{
    Row, SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use uuid::Uuid;

use crate::{
    error::FleetError,
    message_commit_hint::MessageCommitNotifier,
    model::{
        Agent, Channel, ChannelMember, CreateAgent, CreateChannel, CreateChannelMember,
        CreateMessage, MembershipDeliveryMode, Message, MessagePage,
    },
};

const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 256;

/// The durable result of appending a message with optional idempotency.
#[derive(Clone, Debug, PartialEq)]
pub struct AppendMessageResult {
    pub message: Message,
    pub created: bool,
}

static MIGRATOR: Migrator = sqlx::migrate!();

/// SQLite-backed durable state for the coordination kernel.
#[derive(Clone)]
pub struct Store {
    pub(crate) pool: SqlitePool,
    message_commit_notifier: Option<MessageCommitNotifier>,
}

impl Store {
    /// Opens or creates a database and applies the embedded schema.
    ///
    /// # Errors
    ///
    /// Returns an error when `SQLite` cannot open the path or apply the schema.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, FleetError> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await?;
        let store = Self {
            pool,
            message_commit_notifier: None,
        };
        MIGRATOR.run(&store.pool).await?;
        Ok(store)
    }

    /// Opens the authoritative database with best-effort cross-process message
    /// commit wakeups directed at its local daemon.
    ///
    /// The notifier carries no message data or authority. A missing listener is
    /// not an operation failure because reconnect replay remains authoritative.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`Self::open`] or an error resolving the
    /// private local hint address.
    pub async fn open_with_message_commit_hints(
        path: impl AsRef<Path>,
    ) -> Result<Self, FleetError> {
        let path = path.as_ref();
        let mut store = Self::open(path).await?;
        store.message_commit_notifier = Some(MessageCommitNotifier::for_database(path)?);
        Ok(store)
    }

    pub(crate) fn notify_message_commit(&self, created: bool) {
        if created && let Some(notifier) = &self.message_commit_notifier {
            notifier.notify();
        }
    }

    /// Registers a new addressable agent.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty or duplicate name or a persistence failure.
    pub async fn create_agent(&self, input: CreateAgent) -> Result<Agent, FleetError> {
        validate_name("agent", &input.name)?;
        let agent = Agent {
            id: Uuid::new_v4().to_string(),
            name: input.name,
            metadata: input.metadata,
            created_at_ms: now_ms(),
        };
        let metadata_json = serde_json::to_string(&agent.metadata)?;
        let result = sqlx::query(
            "INSERT INTO agents (id, name, metadata_json, created_at_ms) VALUES (?, ?, ?, ?)",
        )
        .bind(&agent.id)
        .bind(&agent.name)
        .bind(metadata_json)
        .bind(agent.created_at_ms)
        .execute(&self.pool)
        .await;
        map_unique_conflict(result, "agent name")?;
        Ok(agent)
    }

    /// Lists all registered agents in creation order.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored rows cannot be read or decoded.
    pub async fn list_agents(&self) -> Result<Vec<Agent>, FleetError> {
        let rows = sqlx::query(
            "SELECT id, name, metadata_json, created_at_ms FROM agents ORDER BY created_at_ms, id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(agent_from_row).collect()
    }

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
            metadata,
            created_at_ms: now_ms(),
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
            "SELECT id, name, metadata_json, created_at_ms FROM channels ORDER BY created_at_ms, id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(channel_from_row).collect()
    }

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
        ensure_exists(&mut transaction, "channels", "channel", channel_id).await?;
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

    /// Reads a page of channel messages strictly after the supplied cursor.
    ///
    /// When `viewer_agent_id` is supplied, direct messages are limited to those
    /// the viewer sent or received. `None` reads every message and is reserved
    /// for operator scope.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid cursor, an unknown channel, or a read or
    /// decoding failure.
    pub async fn list_messages(
        &self,
        channel_id: &str,
        viewer_agent_id: Option<&str>,
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
        let mut query = match viewer_agent_id {
            Some(_) => sqlx::query(
                r"
                SELECT seq, id, channel_id, sender_id, recipient_id, kind, payload_json,
                       correlation_id, causation_id, created_at_ms
                FROM messages
                WHERE channel_id = ? AND seq > ?
                  AND (recipient_id IS NULL OR recipient_id = ? OR sender_id = ?)
                ORDER BY seq
                LIMIT ?
                ",
            ),
            None => sqlx::query(
                r"
                SELECT seq, id, channel_id, sender_id, recipient_id, kind, payload_json,
                       correlation_id, causation_id, created_at_ms
                FROM messages
                WHERE channel_id = ? AND seq > ?
                ORDER BY seq
                LIMIT ?
                ",
            ),
        };
        query = query.bind(channel_id).bind(after);
        if let Some(viewer) = viewer_agent_id {
            query = query.bind(viewer).bind(viewer);
        }
        let rows = query.bind(i64::from(limit)).fetch_all(&self.pool).await?;
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

pub(crate) async fn insert_message(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    channel_id: &str,
    input: CreateMessage,
) -> Result<Message, FleetError> {
    ensure_exists(transaction, "channels", "channel", channel_id).await?;
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

pub(crate) fn validate_name(entity: &str, name: &str) -> Result<(), FleetError> {
    if name.trim().is_empty() {
        return Err(FleetError::Invalid(format!(
            "{entity} name must not be empty"
        )));
    }
    Ok(())
}

pub(crate) fn map_unique_conflict(
    result: Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error>,
    field: &str,
) -> Result<(), FleetError> {
    match result {
        Ok(_) => Ok(()),
        Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
            Err(FleetError::Conflict(format!("{field} already exists")))
        }
        Err(error) => Err(FleetError::Database(error)),
    }
}

async fn ensure_exists(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &'static str,
    entity: &'static str,
    id: &str,
) -> Result<(), FleetError> {
    let query = format!("SELECT COUNT(*) FROM {table} WHERE id = ?");
    let count: i64 = sqlx::query_scalar(&query)
        .bind(id)
        .fetch_one(&mut **transaction)
        .await?;
    if count == 0 {
        return Err(FleetError::NotFound {
            entity,
            id: id.to_owned(),
        });
    }
    Ok(())
}

async fn ensure_member(
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

fn agent_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Agent, FleetError> {
    Ok(Agent {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        metadata: parse_json(&row.try_get::<String, _>("metadata_json")?)?,
        created_at_ms: row.try_get("created_at_ms")?,
    })
}

fn channel_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Channel, FleetError> {
    Ok(Channel {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        metadata: parse_json(&row.try_get::<String, _>("metadata_json")?)?,
        created_at_ms: row.try_get("created_at_ms")?,
    })
}

fn channel_member_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ChannelMember, FleetError> {
    let delivery_mode = match row.try_get::<String, _>("delivery_mode")?.as_str() {
        "inbox" => MembershipDeliveryMode::Inbox,
        "stream_only" => MembershipDeliveryMode::StreamOnly,
        value => {
            return Err(FleetError::Invalid(format!(
                "stored channel membership has unknown delivery mode: {value}"
            )));
        }
    };
    Ok(ChannelMember {
        channel_id: row.try_get("channel_id")?,
        agent_id: row.try_get("agent_id")?,
        agent_name: row.try_get("agent_name")?,
        joined_at_ms: row.try_get("joined_at_ms")?,
        delivery_mode,
    })
}

pub(crate) fn message_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Message, FleetError> {
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

fn parse_json(value: &str) -> Result<Value, FleetError> {
    Ok(serde_json::from_str(value)?)
}

pub(crate) fn now_ms() -> i64 {
    let millis = SystemTime::UNIX_EPOCH
        .elapsed()
        .map_or(0, |time| time.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}
