//! The durable store: one connection pool, and one module per concept.
//!
//! This module owns the pool, the migration set, and the few helpers every
//! concept needs. Each concept -- agents, channels, membership, messages,
//! triggers, and the conversation projection over them -- owns its own file
//! and adds its own `impl Store` block, so two concepts can change without
//! touching the same file.
//!
//! Those blocks reach `Store`'s fields directly because they are descendants of
//! this module, which is why the split costs no call site anything: every
//! `store.method()` in the workspace still resolves exactly as before.

pub mod agent;
pub mod channel;
pub mod membership;
pub mod message;
pub mod trigger;

use std::{path::Path, time::Duration, time::SystemTime};

use serde_json::Value;
use sqlx::{
    SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};

use crate::{error::FleetError, message_commit_hint::MessageCommitNotifier};

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

    /// Returns the pool, for a layer querying the tables it owns itself.
    ///
    /// This cannot be narrowed into a read-only or table-scoped handle: a sqlx
    /// executor accepts any statement, so any handle at all grants every table.
    /// [`Store::begin_immediate`] grants exactly the same reach and has to stay
    /// public, because a delivery transition and the fence settling it must
    /// commit together. So "only the kernel writes kernel tables" is a rule
    /// `tests/crate_boundaries.rs` reads the source to check, not one the
    /// compiler can hold.
    #[must_use]
    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    pub fn notify_message_commit(&self, created: bool) {
        if created && let Some(notifier) = &self.message_commit_notifier {
            notifier.notify();
        }
    }
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
