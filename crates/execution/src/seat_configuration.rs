//! Durable desired execution for stable agent identities.
//!
//! The stored profile is deliberately only an identifier. A surface may let an
//! operator choose it, but only a machine-local supervisor can resolve it to an
//! executable, arguments, tool grants, and harness configuration. This keeps a
//! browser-held operator credential from becoming arbitrary code execution.

use fleetd_kernel::{
    error::FleetError,
    store::{Store, now_ms},
};
use fleetd_proto::operations::{AgentSeatConfiguration, AgentSeatDesiredState, ConfigureAgentSeat};
use sqlx::Row;

const MAX_PROFILE_ID_BYTES: usize = 128;
const MAX_INSTRUCTIONS_BYTES: usize = 32 * 1024;

/// Lists every configured agent seat in stable identity order.
///
/// # Errors
///
/// Returns an error when durable state cannot be read or decoded.
pub async fn list(store: &Store) -> Result<Vec<AgentSeatConfiguration>, FleetError> {
    let rows = sqlx::query(
        "SELECT agent_id, profile_id, instructions, desired_state, revision, \
         created_at_ms, updated_at_ms \
         FROM agent_seat_configurations ORDER BY agent_id ASC",
    )
    .fetch_all(store.pool())
    .await?;
    rows.iter().map(from_row).collect()
}

/// Reads the configured seat for one agent, when it has one.
///
/// # Errors
///
/// Returns an error when durable state cannot be read or decoded.
pub async fn get(
    store: &Store,
    agent_id: &str,
) -> Result<Option<AgentSeatConfiguration>, FleetError> {
    let row = sqlx::query(
        "SELECT agent_id, profile_id, instructions, desired_state, revision, \
         created_at_ms, updated_at_ms \
         FROM agent_seat_configurations WHERE agent_id = ?",
    )
    .bind(agent_id)
    .fetch_optional(store.pool())
    .await?;
    row.as_ref().map(from_row).transpose()
}

/// Creates or changes one agent's desired local execution.
///
/// An exact replay is idempotent and leaves the revision unchanged. Any actual
/// change increments it, giving supervisors a durable restart fence.
///
/// # Errors
///
/// Returns invalid for an out-of-bounds profile or instruction block, not found
/// for an unknown agent, or a persistence failure.
pub async fn configure(
    store: &Store,
    agent_id: &str,
    requested: &ConfigureAgentSeat,
) -> Result<AgentSeatConfiguration, FleetError> {
    validate(requested)?;
    let now = now_ms();
    let mut transaction = store.begin_immediate().await?;
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
        "INSERT INTO agent_seat_configurations (\
            agent_id, profile_id, instructions, desired_state, revision, created_at_ms, updated_at_ms\
         ) VALUES (?, ?, ?, ?, 1, ?, ?) \
         ON CONFLICT(agent_id) DO UPDATE SET \
            profile_id = excluded.profile_id, \
            instructions = excluded.instructions, \
            desired_state = excluded.desired_state, \
            revision = agent_seat_configurations.revision + 1, \
            updated_at_ms = excluded.updated_at_ms \
         WHERE agent_seat_configurations.profile_id != excluded.profile_id \
            OR agent_seat_configurations.instructions != excluded.instructions \
            OR agent_seat_configurations.desired_state != excluded.desired_state",
    )
    .bind(agent_id)
    .bind(&requested.profile_id)
    .bind(&requested.instructions)
    .bind(requested.desired_state.as_str())
    .bind(now)
    .bind(now)
    .execute(&mut *transaction)
    .await?;
    let row = sqlx::query(
        "SELECT agent_id, profile_id, instructions, desired_state, revision, \
         created_at_ms, updated_at_ms \
         FROM agent_seat_configurations WHERE agent_id = ?",
    )
    .bind(agent_id)
    .fetch_one(&mut *transaction)
    .await?;
    let configured = from_row(&row)?;
    transaction.commit().await?;
    Ok(configured)
}

/// Advances the revision of a running seat so its local supervisor replaces it.
///
/// # Errors
///
/// Returns not found for an unconfigured agent, conflict when it is stopped, or
/// a persistence failure.
pub async fn restart(store: &Store, agent_id: &str) -> Result<AgentSeatConfiguration, FleetError> {
    let mut transaction = store.begin_immediate().await?;
    let changed = sqlx::query(
        "UPDATE agent_seat_configurations \
         SET revision = revision + 1, updated_at_ms = ? \
         WHERE agent_id = ? AND desired_state = 'running'",
    )
    .bind(now_ms())
    .bind(agent_id)
    .execute(&mut *transaction)
    .await?
    .rows_affected();
    if changed == 0 {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT desired_state FROM agent_seat_configurations WHERE agent_id = ?",
        )
        .bind(agent_id)
        .fetch_optional(&mut *transaction)
        .await?;
        return match state {
            None => Err(FleetError::NotFound {
                entity: "agent seat configuration",
                id: agent_id.to_owned(),
            }),
            Some(_) => Err(FleetError::Conflict(format!(
                "agent seat {agent_id} is stopped; start it before restarting"
            ))),
        };
    }
    let row = sqlx::query(
        "SELECT agent_id, profile_id, instructions, desired_state, revision, \
         created_at_ms, updated_at_ms \
         FROM agent_seat_configurations WHERE agent_id = ?",
    )
    .bind(agent_id)
    .fetch_one(&mut *transaction)
    .await?;
    let configured = from_row(&row)?;
    transaction.commit().await?;
    Ok(configured)
}

fn validate(requested: &ConfigureAgentSeat) -> Result<(), FleetError> {
    if requested.profile_id.trim().is_empty()
        || requested.profile_id.len() > MAX_PROFILE_ID_BYTES
        || !requested
            .profile_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(FleetError::Invalid(format!(
            "profile_id must contain 1 to {MAX_PROFILE_ID_BYTES} ASCII letters, digits, dots, dashes, or underscores"
        )));
    }
    if requested.instructions.len() > MAX_INSTRUCTIONS_BYTES {
        return Err(FleetError::Invalid(format!(
            "instructions must not exceed {MAX_INSTRUCTIONS_BYTES} bytes"
        )));
    }
    Ok(())
}

fn from_row(row: &sqlx::sqlite::SqliteRow) -> Result<AgentSeatConfiguration, FleetError> {
    let desired_state: String = row.try_get("desired_state")?;
    let desired_state = AgentSeatDesiredState::parse(&desired_state).ok_or_else(|| {
        FleetError::Invalid(format!(
            "stored agent seat desired state is invalid: {desired_state}"
        ))
    })?;
    let revision: i64 = row.try_get("revision")?;
    Ok(AgentSeatConfiguration {
        agent_id: row.try_get("agent_id")?,
        profile_id: row.try_get("profile_id")?,
        instructions: row.try_get("instructions")?,
        desired_state,
        revision: u64::try_from(revision)
            .map_err(|_| FleetError::Invalid("stored agent seat revision is invalid".to_owned()))?,
        created_at_ms: row.try_get("created_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
    })
}

#[cfg(test)]
mod tests {
    use fleetd_kernel::store::Store;
    use fleetd_proto::{
        model::CreateAgent,
        operations::{AgentSeatDesiredState, ConfigureAgentSeat},
    };
    use serde_json::json;

    use super::{configure, restart};

    #[tokio::test]
    async fn exact_configuration_replay_is_idempotent_and_restart_is_explicit() {
        let path = std::env::temp_dir().join(format!(
            "fleetd-seat-configuration-{}.db",
            uuid::Uuid::new_v4()
        ));
        let store = Store::open(&path).await.expect("store");
        let registered = store
            .create_agent(CreateAgent {
                name: "builder".to_owned(),
                metadata: json!({}),
            })
            .await
            .expect("agent");
        let request = ConfigureAgentSeat {
            profile_id: "opencode.glm".to_owned(),
            instructions: "Build and converse.".to_owned(),
            desired_state: AgentSeatDesiredState::Running,
        };
        let first = configure(&store, &registered.id, &request)
            .await
            .expect("first");
        let replay = configure(&store, &registered.id, &request)
            .await
            .expect("replay");
        assert_eq!(first.revision, 1);
        assert_eq!(replay.revision, 1);
        let restarted = restart(&store, &registered.id).await.expect("restart");
        assert_eq!(restarted.revision, 2);
        drop(store);
        std::fs::remove_file(path).expect("remove test database");
    }
}
