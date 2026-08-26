use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::Duration,
    time::SystemTime,
};

use serde_json::Value;
use sqlx::{
    Row, SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use uuid::Uuid;

use crate::{error::FleetError, message_commit_hint::MessageCommitNotifier};
use fleetd_proto::model::{
    Agent, Channel, ChannelMember, ConversationKind, ConversationSummary, CreateAgent,
    CreateChannel, CreateChannelMember, CreateMessage, MembershipDeliveryMode, Message,
    MessagePage, OpenDirectConversation,
};

const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 256;

/// The durable result of appending a message with optional idempotency.
#[derive(Clone, Debug, PartialEq)]
pub struct AppendMessageResult {
    pub message: Message,
    pub created: bool,
}

/// The durable result of opening one exact direct-conversation pair.
#[derive(Clone, Debug, PartialEq)]
pub struct OpenDirectConversationResult {
    pub conversation: ConversationSummary,
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

    /// Begins an immediate write transaction against the authoritative store.
    ///
    /// Callers above the kernel compose their own work into this transaction so
    /// that state the kernel owns and state they own commit together.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction cannot be started.
    pub async fn begin_immediate(&self) -> Result<sqlx::Transaction<'_, sqlx::Sqlite>, FleetError> {
        Ok(self.pool.begin_with("BEGIN IMMEDIATE").await?)
    }

    /// Returns the read handle for queries the kernel does not itself model.
    #[must_use]
    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    pub fn notify_message_commit(&self, created: bool) {
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
/// Returns an error when the name is empty or exceeds its bound.
pub fn validate_name(entity: &str, name: &str) -> Result<(), FleetError> {
    if name.trim().is_empty() {
        return Err(FleetError::Invalid(format!(
            "{entity} name must not be empty"
        )));
    }
    Ok(())
}

/// Kernel operation used by the layers above.
///
/// # Errors
///
/// Returns the mapped conflict, or the original error when it is not a uniqueness violation.
pub fn map_unique_conflict(
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

async fn channel_row(
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

fn require_mutable_shared_channel(channel: &Channel) -> Result<(), FleetError> {
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

fn channel_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Channel, FleetError> {
    let kind = match row.try_get::<String, _>("conversation_kind")?.as_str() {
        "shared" => ConversationKind::Shared,
        "direct" => ConversationKind::Direct,
        value => {
            return Err(FleetError::Invalid(format!(
                "stored channel has unknown conversation kind: {value}"
            )));
        }
    };
    Ok(Channel {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        kind,
        metadata: parse_json(&row.try_get::<String, _>("metadata_json")?)?,
        created_at_ms: row.try_get("created_at_ms")?,
        archived_at_ms: row.try_get("archived_at_ms")?,
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

fn parse_json(value: &str) -> Result<Value, FleetError> {
    Ok(serde_json::from_str(value)?)
}

#[must_use]
pub fn now_ms() -> i64 {
    let millis = SystemTime::UNIX_EPOCH
        .elapsed()
        .map_or(0, |time| time.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}
