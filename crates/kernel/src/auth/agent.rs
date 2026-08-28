//! Credentials bound to one agent.
//!
//! An agent's authority is over itself, so its credential is issued alongside
//! its identity and rotated without touching that identity. Registration is one
//! transaction because an agent that exists without a way to act, or a
//! credential naming an agent that does not exist, are both states nothing above
//! the kernel could repair.

use uuid::Uuid;

use crate::{
    error::FleetError,
    store::{map_unique_conflict, validate_name},
};
use fleetd_proto::model::{Agent, CreateAgent, IssuedCredential, RegisteredAgent};

use super::{
    AuthService,
    token::{AGENT_TOKEN_PREFIX, issue_credential, token_digest},
};

impl AuthService {
    /// Registers an agent and its first credential in one transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid or duplicate agent input, unavailable
    /// entropy, serialization failure, or persistence failure.
    pub async fn register_agent(&self, input: CreateAgent) -> Result<RegisteredAgent, FleetError> {
        validate_name("agent", &input.name)?;
        let issued = issue_credential(AGENT_TOKEN_PREFIX)?;
        let digest = token_digest(&issued.token);
        let agent = Agent {
            id: Uuid::new_v4().to_string(),
            name: input.name,
            metadata: input.metadata,
            created_at_ms: issued.created_at_ms,
        };
        let metadata_json = serde_json::to_string(&agent.metadata)?;
        let mut transaction = self.store.pool().begin().await?;
        let result = sqlx::query(
            "INSERT INTO agents (id, name, metadata_json, created_at_ms) VALUES (?, ?, ?, ?)",
        )
        .bind(&agent.id)
        .bind(&agent.name)
        .bind(metadata_json)
        .bind(agent.created_at_ms)
        .execute(&mut *transaction)
        .await;
        map_unique_conflict(result, "agent name")?;
        insert_agent_credential(&mut transaction, &agent.id, &issued, &digest).await?;
        transaction.commit().await?;
        Ok(RegisteredAgent {
            agent,
            credential: issued,
        })
    }

    /// Revokes every active credential for an agent and returns a replacement.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown agent, unavailable entropy, or a
    /// persistence failure.
    pub async fn rotate_agent_credential(
        &self,
        agent_id: &str,
    ) -> Result<IssuedCredential, FleetError> {
        let issued = issue_credential(AGENT_TOKEN_PREFIX)?;
        let digest = token_digest(&issued.token);
        let mut transaction = self.store.pool().begin().await?;
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agents WHERE id = ?")
            .bind(agent_id)
            .fetch_one(&mut *transaction)
            .await?;
        if exists == 0 {
            return Err(FleetError::NotFound {
                entity: "agent",
                id: agent_id.to_owned(),
            });
        }
        sqlx::query(
            r"
            UPDATE auth_credentials
            SET revoked_at_ms = ?
            WHERE principal_kind = 'agent'
              AND agent_id = ?
              AND revoked_at_ms IS NULL
            ",
        )
        .bind(issued.created_at_ms)
        .bind(agent_id)
        .execute(&mut *transaction)
        .await?;
        insert_agent_credential(&mut transaction, agent_id, &issued, &digest).await?;
        transaction.commit().await?;
        Ok(issued)
    }
}

async fn insert_agent_credential(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    issued: &IssuedCredential,
    digest: &[u8; 32],
) -> Result<(), FleetError> {
    sqlx::query(
        r"
        INSERT INTO auth_credentials (
            id, principal_kind, agent_id, token_digest, created_at_ms
        ) VALUES (?, 'agent', ?, ?, ?)
        ",
    )
    .bind(&issued.id)
    .bind(agent_id)
    .bind(&digest[..])
    .bind(issued.created_at_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
