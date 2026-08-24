use std::{collections::HashSet, path::Path, time::SystemTime};

use serde_json::Value;
use sqlx::{Row, SqlitePool, sqlite::SqliteConnectOptions};
use uuid::Uuid;

use crate::{
    error::FleetError,
    model::{Agent, Channel, CreateAgent, CreateChannel, CreateMessage, Message, MessagePage},
};

/// SQLite-backed durable state for the coordination kernel.
#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
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
            .foreign_keys(true);
        let pool = SqlitePool::connect_with(options).await?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<(), FleetError> {
        sqlx::raw_sql(
            r"
            CREATE TABLE IF NOT EXISTS agents (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                metadata_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channels (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                metadata_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channel_members (
                channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                joined_at_ms INTEGER NOT NULL,
                PRIMARY KEY (channel_id, agent_id)
            );

            CREATE TABLE IF NOT EXISTS messages (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                channel_id TEXT NOT NULL REFERENCES channels(id),
                sender_id TEXT NOT NULL REFERENCES agents(id),
                recipient_id TEXT REFERENCES agents(id),
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                correlation_id TEXT,
                causation_id TEXT,
                created_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS messages_channel_seq
                ON messages(channel_id, seq);
            ",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
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
        validate_name("channel", &input.name)?;
        let member_ids: HashSet<_> = input.member_ids.into_iter().collect();
        let mut transaction = self.pool.begin().await?;
        for agent_id in &member_ids {
            ensure_exists(&mut transaction, "agents", "agent", agent_id).await?;
        }
        let channel = Channel {
            id: Uuid::new_v4().to_string(),
            name: input.name,
            metadata: input.metadata,
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
        for agent_id in member_ids {
            sqlx::query(
                "INSERT INTO channel_members (channel_id, agent_id, joined_at_ms) VALUES (?, ?, ?)",
            )
            .bind(&channel.id)
            .bind(agent_id)
            .bind(channel.created_at_ms)
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
        let mut transaction = self.pool.begin().await?;
        ensure_exists(&mut transaction, "channels", "channel", channel_id).await?;
        ensure_exists(&mut transaction, "agents", "agent", agent_id).await?;
        sqlx::query(
            "INSERT OR IGNORE INTO channel_members (channel_id, agent_id, joined_at_ms) VALUES (?, ?, ?)",
        )
        .bind(channel_id)
        .bind(agent_id)
        .bind(now_ms())
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
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
        if input.kind.trim().is_empty() {
            return Err(FleetError::Invalid(
                "message kind must not be empty".to_owned(),
            ));
        }
        let mut transaction = self.pool.begin().await?;
        ensure_exists(&mut transaction, "channels", "channel", channel_id).await?;
        ensure_member(&mut transaction, channel_id, &input.sender_id).await?;
        if let Some(recipient_id) = &input.recipient_id {
            ensure_member(&mut transaction, channel_id, recipient_id).await?;
        }
        let id = Uuid::new_v4().to_string();
        let created_at_ms = now_ms();
        let payload_json = serde_json::to_string(&input.payload)?;
        let result = sqlx::query(
            r"
            INSERT INTO messages (
                id, channel_id, sender_id, recipient_id, kind, payload_json,
                correlation_id, causation_id, created_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(&id)
        .bind(channel_id)
        .bind(&input.sender_id)
        .bind(&input.recipient_id)
        .bind(&input.kind)
        .bind(payload_json)
        .bind(&input.correlation_id)
        .bind(&input.causation_id)
        .bind(created_at_ms)
        .execute(&mut *transaction)
        .await?;
        let message = Message {
            seq: result.last_insert_rowid(),
            id,
            channel_id: channel_id.to_owned(),
            sender_id: input.sender_id,
            recipient_id: input.recipient_id,
            kind: input.kind,
            payload: input.payload,
            correlation_id: input.correlation_id,
            causation_id: input.causation_id,
            created_at_ms,
        };
        transaction.commit().await?;
        Ok(message)
    }

    /// Reads a page of channel messages strictly after the supplied cursor.
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

fn validate_name(entity: &str, name: &str) -> Result<(), FleetError> {
    if name.trim().is_empty() {
        return Err(FleetError::Invalid(format!(
            "{entity} name must not be empty"
        )));
    }
    Ok(())
}

fn map_unique_conflict(
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

fn message_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Message, FleetError> {
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

fn now_ms() -> i64 {
    let millis = SystemTime::UNIX_EPOCH
        .elapsed()
        .map_or(0, |time| time.as_millis());
    i64::try_from(millis).unwrap_or(i64::MAX)
}
