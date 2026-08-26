//! Durable agent identity.

use sqlx::Row;
use uuid::Uuid;

use crate::error::FleetError;
use fleetd_proto::model::{Agent, CreateAgent};

use super::{Store, map_unique_conflict, now_ms, parse_json, validate_name};

impl Store {
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
}

fn agent_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Agent, FleetError> {
    Ok(Agent {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        metadata: parse_json(&row.try_get::<String, _>("metadata_json")?)?,
        created_at_ms: row.try_get("created_at_ms")?,
    })
}
