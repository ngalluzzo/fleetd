//! Persistence for bounded durable observations of workers and managed turns.

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;

pub use fleetd_proto::operations::{
    AgentSeat, AgentSeatReason, AgentSeatState, DeliveryCensus, EvidenceCursor, EvidenceOrder,
    FleetHealth, InvocationEventCounts, InvocationObservation, InvocationTrace,
    ObservedPluginInterface, PluginGeneration, PluginGenerationDisposition, PluginGenerationHealth,
    PluginGenerationState, PluginShutdownOutcome,
};

use fleetd_kernel::{
    error::FleetError,
    store::{Store, now_ms},
};
use fleetd_plugin_host::{
    Binding, DescribeResult, PluginIdentity, PluginInterface, SessionPersistence, TurnTerminal,
};
use fleetd_proto::model::{DeliveryState, ExecutionCertainty, InvocationState};
use fleetd_proto::session::SessionBindingState;

const MAX_EVIDENCE_PAGE: u32 = 500;

/// Which page of one evidence listing to read, and which way to walk.
///
/// The two evidence listings are the same question asked of two tables, so
/// they take the same request rather than growing parallel argument lists.
#[derive(Clone, Copy, Debug)]
pub struct EvidencePage<'a> {
    pub agent_id: Option<&'a str>,
    pub after: Option<&'a EvidenceCursor>,
    pub limit: u32,
    pub settled: bool,
    pub order: EvidenceOrder,
}

impl<'a> EvidencePage<'a> {
    /// The whole newest page, which is what every operator surface asks for.
    #[must_use]
    pub const fn newest(agent_id: Option<&'a str>) -> Self {
        Self {
            agent_id,
            after: None,
            limit: MAX_EVIDENCE_PAGE,
            settled: false,
            order: EvidenceOrder::Newest,
        }
    }

    /// Checks the cursor and returns the page size actually readable.
    ///
    /// A negative cursor is rejected rather than clamped: it can only come
    /// from a caller that computed a position wrongly, and silently reading
    /// from the beginning would hide that. An oversized limit is clamped
    /// because asking for more than the bound is a request to be bounded.
    fn validate(&self) -> Result<u32, FleetError> {
        if let Some(after) = self.after
            && after.changed_at_ms < 0
        {
            return Err(FleetError::Invalid(
                "evidence cursor must not be negative".to_owned(),
            ));
        }
        Ok(self.limit.clamp(1, MAX_EVIDENCE_PAGE))
    }
}

/// Where one evidence table keeps the columns a page is addressed by.
struct EvidenceColumns {
    agent: &'static str,
    changed: &'static str,
    id: &'static str,
    settled: &'static str,
}

const GENERATION_COLUMNS: EvidenceColumns = EvidenceColumns {
    agent: "agent_id",
    changed: "last_heartbeat_at_ms",
    id: "id",
    settled: "state = 'stopped'",
};

const OBSERVATION_COLUMNS: EvidenceColumns = EvidenceColumns {
    agent: "i.agent_id",
    changed: "o.updated_at_ms",
    id: "o.invocation_id",
    settled: "o.terminal_at_ms IS NOT NULL",
};

/// Builds the filter, keyset comparison, and ordering for one evidence page.
///
/// The comparison and the ordering are derived from the same direction, so a
/// page cannot be ordered one way and walked the other.
fn evidence_sql(select: &str, columns: &EvidenceColumns, page: &EvidencePage<'_>) -> String {
    use std::fmt::Write as _;

    let (comparison, direction) = match page.order {
        EvidenceOrder::Newest => ("<", "DESC"),
        EvidenceOrder::Oldest => (">", "ASC"),
    };
    let EvidenceColumns {
        agent,
        changed,
        id,
        settled,
    } = columns;
    let mut sql = format!("{select} WHERE 1 = 1");
    if page.agent_id.is_some() {
        let _ = write!(sql, " AND {agent} = ?");
    }
    if page.settled {
        let _ = write!(sql, " AND {settled}");
    }
    if page.after.is_some() {
        let _ = write!(sql, " AND ({changed}, {id}) {comparison} (?, ?)");
    }
    let _ = write!(
        sql,
        " ORDER BY {changed} {direction}, {id} {direction} LIMIT ?"
    );
    sql
}

/// Binds an evidence page in the order `evidence_sql` wrote its placeholders.
fn bind_evidence_page<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    page: &EvidencePage<'q>,
    limit: u32,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    let mut query = query;
    if let Some(agent_id) = page.agent_id {
        query = query.bind(agent_id);
    }
    if let Some(after) = page.after {
        query = query.bind(after.changed_at_ms).bind(after.id.as_str());
    }
    query.bind(i64::from(limit))
}

const MAX_EVIDENCE_JSON_BYTES: usize = 1024 * 1024;
const MAX_REASON_BYTES: usize = 4_096;
const MIN_STALE_AFTER_MS: i64 = 15_000;

/// Exact evidence required before a worker may route work to a generation.
#[derive(Clone, Debug)]
pub struct NewPluginGeneration {
    pub id: String,
    pub agent_id: String,
    pub plugin: PluginIdentity,
    pub interfaces: Vec<PluginInterface>,
    pub process_id: Option<u32>,
    pub description: DescribeResult,
    pub compatibility_digest: String,
    pub heartbeat_interval_ms: u64,
}

/// Terminal evidence attached when a worker retires a generation.
#[derive(Clone, Debug)]
pub struct StopPluginGeneration {
    pub disposition: PluginGenerationDisposition,
    pub reason: String,
    pub shutdown_outcome: PluginShutdownOutcome,
    pub shutdown_exit_code: Option<i32>,
}

/// Persists exact identity for a ready plugin generation before work routes
/// through it.
///
/// # Errors
///
/// Returns an error when evidence is invalid or oversized, the agent does
/// not exist, the generation ID conflicts, or persistence fails.
pub async fn record_plugin_generation(
    store: &Store,
    generation: NewPluginGeneration,
) -> Result<PluginGeneration, FleetError> {
    validate_generation(&generation)?;
    let interfaces_json = bounded_json(&generation.interfaces, "plugin interfaces")?;
    let capabilities_json = bounded_json(
        &generation.description.agent_capabilities,
        "agent capabilities",
    )?;
    let initialize_json = bounded_json(
        &generation.description.raw_initialize_result,
        "raw initialize result",
    )?;
    let now = now_ms();
    let process_id = generation.process_id.map(i64::from);
    let heartbeat_interval_ms = i64::try_from(generation.heartbeat_interval_ms)
        .map_err(|_| FleetError::Invalid("heartbeat interval is too large".to_owned()))?;
    let max_frame_bytes = i64::try_from(generation.description.limits.max_frame_bytes)
        .map_err(|_| FleetError::Invalid("maximum frame size is too large".to_owned()))?;
    let result = sqlx::query(
        r"
        INSERT INTO plugin_generations (
            id, agent_id, plugin_id, plugin_name, plugin_version,
            interfaces_json, process_id, driver_version, acp_sdk_version,
            acp_protocol_version, runtime_name, runtime_version,
            runtime_executable_digest, agent_capabilities_json,
            max_concurrent_turns, max_frame_bytes, profile_digest,
            compatibility_digest, raw_initialize_result_json,
            heartbeat_interval_ms, state, started_at_ms, last_heartbeat_at_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
        ",
    )
    .bind(&generation.id)
    .bind(&generation.agent_id)
    .bind(&generation.plugin.id)
    .bind(&generation.plugin.name)
    .bind(generation.plugin.version.to_string())
    .bind(interfaces_json)
    .bind(process_id)
    .bind(&generation.description.driver.version)
    .bind(&generation.description.driver.acp_sdk_version)
    .bind(i64::from(
        generation.description.driver.acp_protocol_version,
    ))
    .bind(&generation.description.runtime.name)
    .bind(&generation.description.runtime.version)
    .bind(&generation.description.runtime.executable_digest)
    .bind(capabilities_json)
    .bind(i64::from(
        generation.description.limits.max_concurrent_turns,
    ))
    .bind(max_frame_bytes)
    .bind(&generation.description.profile_digest)
    .bind(&generation.compatibility_digest)
    .bind(initialize_json)
    .bind(heartbeat_interval_ms)
    .bind(now)
    .bind(now)
    .execute(store.pool())
    .await;
    match result {
        Ok(_) => plugin_generation(store, &generation.id).await,
        Err(error) if is_foreign_key_violation(&error) => Err(FleetError::NotFound {
            entity: "agent",
            id: generation.agent_id,
        }),
        Err(error) if is_unique_violation(&error) => Err(FleetError::Conflict(format!(
            "plugin generation already exists: {}",
            generation.id
        ))),
        Err(error) => Err(error.into()),
    }
}

/// Advances liveness for one active generation.
///
/// # Errors
///
/// Returns an error when the generation is absent or stopped, or when
/// persistence fails.
pub async fn heartbeat_plugin_generation(
    store: &Store,
    generation_id: &str,
) -> Result<(), FleetError> {
    let result = sqlx::query(
        "UPDATE plugin_generations SET last_heartbeat_at_ms = ? WHERE id = ? AND state = 'active'",
    )
    .bind(now_ms())
    .bind(generation_id)
    .execute(store.pool())
    .await?;
    if result.rows_affected() != 1 {
        return Err(FleetError::Conflict(format!(
            "plugin generation is not active: {generation_id}"
        )));
    }
    Ok(())
}

/// Durably retires one generation. An exact replay is idempotent.
///
/// # Errors
///
/// Returns an error for invalid evidence, a changed replay, an unknown
/// generation, or a persistence failure.
pub async fn stop_plugin_generation(
    store: &Store,
    generation_id: &str,
    stop: StopPluginGeneration,
) -> Result<PluginGeneration, FleetError> {
    validate_reason(&stop.reason)?;
    let disposition = stop.disposition.as_str();
    let shutdown = stop.shutdown_outcome.as_str();
    let now = now_ms();
    let result = sqlx::query(
        r"
        UPDATE plugin_generations
        SET state = 'stopped', last_heartbeat_at_ms = ?, stopped_at_ms = ?,
            stop_disposition = ?, stop_reason = ?, shutdown_outcome = ?,
            shutdown_exit_code = ?
        WHERE id = ? AND state = 'active'
        ",
    )
    .bind(now)
    .bind(now)
    .bind(disposition)
    .bind(&stop.reason)
    .bind(shutdown)
    .bind(stop.shutdown_exit_code)
    .bind(generation_id)
    .execute(store.pool())
    .await?;
    if result.rows_affected() == 0 {
        let existing = plugin_generation(store, generation_id).await?;
        if existing.stop_disposition == Some(stop.disposition)
            && existing.stop_reason.as_deref() == Some(stop.reason.as_str())
            && existing.shutdown_outcome == Some(stop.shutdown_outcome)
            && existing.shutdown_exit_code == stop.shutdown_exit_code
        {
            return Ok(existing);
        }
        return Err(FleetError::Conflict(format!(
            "plugin generation already stopped with different evidence: {generation_id}"
        )));
    }
    plugin_generation(store, generation_id).await
}

/// Reads one cursor-addressed page of durable generation records.
///
/// Generations are ordered by their heartbeat clock, which every durable
/// change advances: the row is inserted with it, a heartbeat moves it, and
/// retirement moves it a final time. That makes it the change clock a
/// collector needs, and it freezes on a stopped generation, so a settled page
/// is stable evidence rather than a moving target. A caller resumes from the
/// last row's `last_heartbeat_at_ms` and `id`.
///
/// # Errors
///
/// Returns an error for a negative cursor, or when persisted evidence is
/// invalid or cannot be read.
pub async fn list_plugin_generations(
    store: &Store,
    page: &EvidencePage<'_>,
) -> Result<Vec<PluginGeneration>, FleetError> {
    let limit = page.validate()?;
    let sql = evidence_sql(generation_select(), &GENERATION_COLUMNS, page);
    let rows = bind_evidence_page(sqlx::query(&sql), page, limit)
        .fetch_all(store.pool())
        .await?;
    rows.iter().map(generation_from_row).collect()
}

/// Builds one exact invocation trace from durable evidence.
///
/// A reserved invocation may not yet have an observation, a session, a plugin
/// generation, or a result. Once an observation exists, the session and
/// generation it names must still be readable, so a missing one surfaces as an
/// error rather than an absent field.
///
/// # Errors
///
/// Returns not found for an unknown invocation, or a read or decoding error
/// for referenced durable evidence.
pub async fn invocation_trace(
    store: &Store,
    invocation_id: &str,
) -> Result<InvocationTrace, FleetError> {
    let invocation = crate::invocation::get_invocation(store, invocation_id).await?;
    let observation = invocation_observation(store, invocation_id).await?;
    let (session, plugin_generation) = match &observation {
        Some(observation) => (
            Some(
                crate::session_binding::get_session_binding(
                    store,
                    &observation.binding_id,
                    observation.binding_generation,
                )
                .await?,
            ),
            Some(plugin_generation(store, &observation.generation_id).await?),
        ),
        None => (None, None),
    };
    let result = match invocation.result_message_id.as_deref() {
        Some(message_id) => Some(store.get_message(message_id).await?),
        None => None,
    };
    Ok(InvocationTrace {
        invocation,
        observation,
        session,
        plugin_generation,
        result,
    })
}

async fn invocation_observation(
    store: &Store,
    invocation_id: &str,
) -> Result<Option<InvocationObservation>, FleetError> {
    let row = sqlx::query(&format!(
        "{} WHERE o.invocation_id = ?",
        observation_select()
    ))
    .bind(invocation_id)
    .fetch_optional(store.pool())
    .await?;
    row.as_ref().map(observation_from_row).transpose()
}

async fn plugin_generation(
    store: &Store,
    generation_id: &str,
) -> Result<PluginGeneration, FleetError> {
    let row = sqlx::query(&format!("{} WHERE id = ?", generation_select()))
        .bind(generation_id)
        .fetch_optional(store.pool())
        .await?
        .ok_or_else(|| FleetError::NotFound {
            entity: "plugin generation",
            id: generation_id.to_owned(),
        })?;
    generation_from_row(&row)
}

/// Folds one exact harness update into the invocation's bounded event
/// counters and chain digest. Exact duplicate delivery is idempotent.
///
/// # Errors
///
/// Returns an error for invalid, missing, stale, non-contiguous, changed,
/// post-terminal, or unpersistable evidence.
pub async fn record_invocation_event(
    store: &Store,
    generation_id: &str,
    invocation_id: &str,
    event_seq: u64,
    observed_at_ms: i64,
    classification: &str,
    raw: &Value,
) -> Result<(), FleetError> {
    if event_seq == 0 || observed_at_ms <= 0 || classification.trim().is_empty() {
        return Err(FleetError::Invalid(
            "invocation event sequence, time, and classification must be valid".to_owned(),
        ));
    }
    let event_seq = i64::try_from(event_seq)
        .map_err(|_| FleetError::Invalid("event sequence is too large".to_owned()))?;
    let raw_bytes = serde_json::to_vec(raw)?;
    let raw_len = i64::try_from(raw_bytes.len())
        .map_err(|_| FleetError::Invalid("event payload is too large".to_owned()))?;
    let event_digest = event_digest(event_seq, observed_at_ms, classification, &raw_bytes);
    let mut transaction = store.begin_immediate().await?;
    let row = sqlx::query(
        r"
        SELECT o.last_event_seq, o.last_event_digest, o.event_chain_digest,
               o.terminal_at_ms, g.state AS generation_state
        FROM invocation_observations o
        JOIN plugin_generations g ON g.id = o.generation_id
        WHERE o.invocation_id = ? AND o.generation_id = ?
        ",
    )
    .bind(invocation_id)
    .bind(generation_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| FleetError::NotFound {
        entity: "invocation observation",
        id: invocation_id.to_owned(),
    })?;
    if row.try_get::<String, _>("generation_state")? != "active" {
        return Err(FleetError::Conflict(
            "invocation generation is no longer active".to_owned(),
        ));
    }
    if row.try_get::<Option<i64>, _>("terminal_at_ms")?.is_some() {
        return Err(FleetError::Conflict(
            "invocation observation is already terminal".to_owned(),
        ));
    }
    let previous_seq: i64 = row.try_get("last_event_seq")?;
    if event_seq == previous_seq {
        let previous_digest: Option<String> = row.try_get("last_event_digest")?;
        if previous_digest.as_deref() == Some(event_digest.as_str()) {
            transaction.commit().await?;
            return Ok(());
        }
        return Err(FleetError::Conflict(
            "event sequence was reused with different evidence".to_owned(),
        ));
    }
    if event_seq != previous_seq.saturating_add(1) {
        return Err(FleetError::Conflict(format!(
            "event sequence is not contiguous: expected {}, received {event_seq}",
            previous_seq.saturating_add(1)
        )));
    }
    let previous_chain: Option<String> = row.try_get("event_chain_digest")?;
    apply_event_fold(
        &mut transaction,
        EventFold {
            generation_id,
            invocation_id,
            previous_seq,
            event_seq,
            observed_at_ms,
            payload_bytes: raw_len,
            chain_digest: chain_digest(previous_chain.as_deref(), &event_digest),
            event_digest,
            counts: event_increments(classification),
        },
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

/// Records the exact terminal summary before result settlement. Transcript
/// content remains in the bounded result message rather than this row.
///
/// # Errors
///
/// Returns an error when terminal evidence is oversized, does not match
/// the durable event sequence, conflicts with a replay, or cannot persist.
pub async fn record_invocation_terminal(
    store: &Store,
    generation_id: &str,
    terminal: &TurnTerminal,
) -> Result<(), FleetError> {
    let invocation_id = &terminal.fence.invocation_id;
    let last_event_seq = i64::try_from(terminal.last_event_seq)
        .map_err(|_| FleetError::Invalid("terminal event sequence is too large".to_owned()))?;
    let usage_json = bounded_json(&terminal.usage, "terminal usage")?;
    let certainty = ExecutionCertainty::from(terminal.execution_certainty).as_str();
    let persistence = terminal.session_persistence.as_str();
    let mut transaction = store.begin_immediate().await?;
    let row = sqlx::query(
        r"
        SELECT last_event_seq, terminal_at_ms, stop_reason,
               runtime_stop_reason, execution_certainty, session_quiescent,
               session_persistence, usage_json
        FROM invocation_observations
        WHERE invocation_id = ? AND generation_id = ?
        ",
    )
    .bind(invocation_id)
    .bind(generation_id)
    .fetch_optional(&mut *transaction)
    .await?
    .ok_or_else(|| FleetError::NotFound {
        entity: "invocation observation",
        id: invocation_id.clone(),
    })?;
    if row.try_get::<i64, _>("last_event_seq")? != last_event_seq {
        return Err(FleetError::Conflict(
            "terminal event sequence does not match durable observations".to_owned(),
        ));
    }
    if row.try_get::<Option<i64>, _>("terminal_at_ms")?.is_some() {
        if terminal_matches(&row, terminal, certainty, persistence, &usage_json)? {
            transaction.commit().await?;
            return Ok(());
        }
        return Err(FleetError::Conflict(
            "invocation terminal was already recorded with different evidence".to_owned(),
        ));
    }
    let now = now_ms();
    sqlx::query(
        r"
        UPDATE invocation_observations
        SET updated_at_ms = ?, terminal_at_ms = ?, stop_reason = ?,
            runtime_stop_reason = ?, execution_certainty = ?,
            session_quiescent = ?, session_persistence = ?, usage_json = ?
        WHERE invocation_id = ? AND generation_id = ? AND terminal_at_ms IS NULL
        ",
    )
    .bind(now)
    .bind(now)
    .bind(&terminal.stop_reason)
    .bind(&terminal.runtime_stop_reason)
    .bind(certainty)
    .bind(terminal.session_quiescent)
    .bind(persistence)
    .bind(usage_json)
    .bind(invocation_id)
    .bind(generation_id)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "UPDATE plugin_generations SET last_heartbeat_at_ms = ? WHERE id = ? AND state = 'active'",
    )
    .bind(now)
    .bind(generation_id)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;
    Ok(())
}

/// Reads one cursor-addressed page of bounded invocation observations.
///
/// Observations are ordered by `updated_at_ms`, which the arming insert, every
/// folded event, and the terminal response each advance. A terminal row is
/// never written again, so its position stops moving once it settles. A caller
/// resumes from the last row's `updated_at_ms` and `invocation_id`.
///
/// # Errors
///
/// Returns an error for a negative cursor, or when persisted evidence is
/// invalid or cannot be read.
pub async fn list_invocation_observations(
    store: &Store,
    page: &EvidencePage<'_>,
) -> Result<Vec<InvocationObservation>, FleetError> {
    let limit = page.validate()?;
    let sql = evidence_sql(observation_select(), &OBSERVATION_COLUMNS, page);
    let rows = bind_evidence_page(sqlx::query(&sql), page, limit)
        .fetch_all(store.pool())
        .await?;
    rows.iter().map(observation_from_row).collect()
}

struct EventFold<'a> {
    generation_id: &'a str,
    invocation_id: &'a str,
    previous_seq: i64,
    event_seq: i64,
    observed_at_ms: i64,
    payload_bytes: i64,
    event_digest: String,
    chain_digest: String,
    counts: EventIncrements,
}

async fn apply_event_fold(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: EventFold<'_>,
) -> Result<(), FleetError> {
    let now = now_ms();
    let updated = sqlx::query(
        r"
        UPDATE invocation_observations
        SET updated_at_ms = ?,
            first_event_at_ms = COALESCE(first_event_at_ms, ?),
            last_event_at_ms = ?, event_count = event_count + 1,
            observed_payload_bytes = observed_payload_bytes + ?,
            last_event_seq = ?, last_event_digest = ?, event_chain_digest = ?,
            assistant_event_count = assistant_event_count + ?,
            reasoning_event_count = reasoning_event_count + ?,
            tool_event_count = tool_event_count + ?,
            plan_event_count = plan_event_count + ?,
            usage_event_count = usage_event_count + ?,
            metadata_event_count = metadata_event_count + ?,
            permission_event_count = permission_event_count + ?,
            unknown_event_count = unknown_event_count + ?
        WHERE invocation_id = ? AND generation_id = ? AND last_event_seq = ?
        ",
    )
    .bind(now)
    .bind(event.observed_at_ms)
    .bind(event.observed_at_ms)
    .bind(event.payload_bytes)
    .bind(event.event_seq)
    .bind(event.event_digest)
    .bind(event.chain_digest)
    .bind(event.counts.assistant)
    .bind(event.counts.reasoning)
    .bind(event.counts.tool)
    .bind(event.counts.plan)
    .bind(event.counts.usage)
    .bind(event.counts.metadata)
    .bind(event.counts.permission)
    .bind(event.counts.unknown)
    .bind(event.invocation_id)
    .bind(event.generation_id)
    .bind(event.previous_seq)
    .execute(&mut **transaction)
    .await?;
    if updated.rows_affected() != 1 {
        return Err(FleetError::Conflict(
            "invocation observation changed concurrently".to_owned(),
        ));
    }
    sqlx::query(
        "UPDATE plugin_generations SET last_heartbeat_at_ms = ? WHERE id = ? AND state = 'active'",
    )
    .bind(now)
    .bind(event.generation_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

pub(crate) async fn begin_invocation_observation(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    invocation_id: &str,
    generation_id: &str,
    binding: &Binding,
    now: i64,
) -> Result<(), FleetError> {
    let result = sqlx::query(
        r"
        INSERT INTO invocation_observations (
            invocation_id, generation_id, binding_id, binding_generation,
            owner_epoch, started_at_ms, updated_at_ms
        )
        SELECT ?, g.id, ?, ?, ?, ?, ?
        FROM plugin_generations g
        WHERE g.id = ? AND g.agent_id = ? AND g.state = 'active'
        ",
    )
    .bind(invocation_id)
    .bind(&binding.binding_id)
    .bind(
        i64::try_from(binding.binding_generation)
            .map_err(|_| FleetError::Invalid("binding generation is too large".to_owned()))?,
    )
    .bind(
        i64::try_from(binding.owner_epoch)
            .map_err(|_| FleetError::Invalid("owner epoch is too large".to_owned()))?,
    )
    .bind(now)
    .bind(now)
    .bind(generation_id)
    .bind(agent_id)
    .execute(&mut **transaction)
    .await;
    match result {
        Ok(result) if result.rows_affected() == 1 => Ok(()),
        Ok(_) => Err(FleetError::Conflict(format!(
            "plugin generation is not active for agent {agent_id}: {generation_id}"
        ))),
        Err(error) if is_unique_violation(&error) => Err(FleetError::Conflict(format!(
            "invocation observation already exists: {invocation_id}"
        ))),
        Err(error) => Err(error.into()),
    }
}

fn validate_generation(generation: &NewPluginGeneration) -> Result<(), FleetError> {
    if generation.id.trim().is_empty()
        || generation.agent_id.trim().is_empty()
        || generation.compatibility_digest.trim().is_empty()
        || generation.heartbeat_interval_ms == 0
        || generation.interfaces.is_empty()
    {
        return Err(FleetError::Invalid(
            "generation identity, agent, interfaces, compatibility, and poll interval are required"
                .to_owned(),
        ));
    }
    Ok(())
}

fn validate_reason(reason: &str) -> Result<(), FleetError> {
    if reason.trim().is_empty() || reason.len() > MAX_REASON_BYTES {
        return Err(FleetError::Invalid(format!(
            "generation stop reason must contain between 1 and {MAX_REASON_BYTES} bytes"
        )));
    }
    Ok(())
}

fn bounded_json(value: &impl Serialize, label: &str) -> Result<String, FleetError> {
    let encoded = serde_json::to_string(value)?;
    if encoded.len() > MAX_EVIDENCE_JSON_BYTES {
        return Err(FleetError::Invalid(format!(
            "{label} exceeds {MAX_EVIDENCE_JSON_BYTES} bytes"
        )));
    }
    Ok(encoded)
}

fn generation_select() -> &'static str {
    r"
    SELECT id, agent_id, plugin_id, plugin_name, plugin_version,
           interfaces_json, process_id, driver_version, acp_sdk_version,
           acp_protocol_version, runtime_name, runtime_version,
           runtime_executable_digest, agent_capabilities_json,
           max_concurrent_turns, max_frame_bytes, profile_digest,
           compatibility_digest, raw_initialize_result_json,
           heartbeat_interval_ms, state, started_at_ms, last_heartbeat_at_ms,
           stopped_at_ms, stop_disposition, stop_reason, shutdown_outcome,
           shutdown_exit_code
    FROM plugin_generations
    "
}

fn generation_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<PluginGeneration, FleetError> {
    let interfaces = serde_json::from_str::<Vec<PluginInterface>>(row.try_get("interfaces_json")?)?
        .into_iter()
        .map(|interface| ObservedPluginInterface {
            id: interface.id,
            version: interface.version.to_string(),
        })
        .collect();
    let state = parse_generation_state(row.try_get("state")?)?;
    let heartbeat_interval_ms =
        to_u64(row.try_get("heartbeat_interval_ms")?, "heartbeat interval")?;
    let last_heartbeat_at_ms = row.try_get("last_heartbeat_at_ms")?;
    Ok(PluginGeneration {
        id: row.try_get("id")?,
        agent_id: row.try_get("agent_id")?,
        plugin_id: row.try_get("plugin_id")?,
        plugin_name: row.try_get("plugin_name")?,
        plugin_version: row.try_get("plugin_version")?,
        interfaces,
        process_id: row
            .try_get::<Option<i64>, _>("process_id")?
            .map(|value| to_u32(value, "process ID"))
            .transpose()?,
        driver_version: row.try_get("driver_version")?,
        acp_sdk_version: row.try_get("acp_sdk_version")?,
        acp_protocol_version: to_u32(row.try_get("acp_protocol_version")?, "ACP protocol version")?,
        runtime_name: row.try_get("runtime_name")?,
        runtime_version: row.try_get("runtime_version")?,
        runtime_executable_digest: row.try_get("runtime_executable_digest")?,
        agent_capabilities: serde_json::from_str(row.try_get("agent_capabilities_json")?)?,
        max_concurrent_turns: to_u32(
            row.try_get("max_concurrent_turns")?,
            "maximum concurrent turns",
        )?,
        max_frame_bytes: to_usize(row.try_get("max_frame_bytes")?, "maximum frame bytes")?,
        profile_digest: row.try_get("profile_digest")?,
        compatibility_digest: row.try_get("compatibility_digest")?,
        raw_initialize_result: serde_json::from_str(row.try_get("raw_initialize_result_json")?)?,
        heartbeat_interval_ms,
        state,
        health: generation_health(state, last_heartbeat_at_ms, heartbeat_interval_ms),
        started_at_ms: row.try_get("started_at_ms")?,
        last_heartbeat_at_ms,
        stopped_at_ms: row.try_get("stopped_at_ms")?,
        stop_disposition: row
            .try_get::<Option<String>, _>("stop_disposition")?
            .as_deref()
            .map(|value| {
                PluginGenerationDisposition::parse(value)
                    .ok_or_else(|| invalid_stored("plugin generation disposition", value))
            })
            .transpose()?,
        stop_reason: row.try_get("stop_reason")?,
        shutdown_outcome: row
            .try_get::<Option<String>, _>("shutdown_outcome")?
            .as_deref()
            .map(|value| {
                PluginShutdownOutcome::parse(value)
                    .ok_or_else(|| invalid_stored("plugin shutdown outcome", value))
            })
            .transpose()?,
        shutdown_exit_code: row.try_get("shutdown_exit_code")?,
    })
}

fn observation_select() -> &'static str {
    r"
    SELECT o.invocation_id, i.agent_id, source.id AS source_message_id,
           result.id AS result_message_id, o.generation_id, o.binding_id,
           o.binding_generation, o.owner_epoch, o.started_at_ms, o.updated_at_ms,
           o.first_event_at_ms, o.last_event_at_ms, o.event_count,
           o.observed_payload_bytes, o.last_event_seq, o.event_chain_digest,
           o.assistant_event_count, o.reasoning_event_count, o.tool_event_count,
           o.plan_event_count, o.usage_event_count, o.metadata_event_count,
           o.permission_event_count, o.unknown_event_count, o.terminal_at_ms,
           o.stop_reason, o.runtime_stop_reason, o.execution_certainty,
           o.session_quiescent, o.session_persistence, o.usage_json
    FROM invocation_observations o
    JOIN invocations i ON i.id = o.invocation_id
    JOIN messages source ON source.seq = i.message_seq
    LEFT JOIN messages result ON result.seq = i.result_message_seq
    "
}

fn observation_from_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<InvocationObservation, FleetError> {
    Ok(InvocationObservation {
        invocation_id: row.try_get("invocation_id")?,
        agent_id: row.try_get("agent_id")?,
        source_message_id: row.try_get("source_message_id")?,
        result_message_id: row.try_get("result_message_id")?,
        generation_id: row.try_get("generation_id")?,
        binding_id: row.try_get("binding_id")?,
        binding_generation: to_u64(row.try_get("binding_generation")?, "binding generation")?,
        owner_epoch: to_u64(row.try_get("owner_epoch")?, "owner epoch")?,
        started_at_ms: row.try_get("started_at_ms")?,
        updated_at_ms: row.try_get("updated_at_ms")?,
        first_event_at_ms: row.try_get("first_event_at_ms")?,
        last_event_at_ms: row.try_get("last_event_at_ms")?,
        event_count: to_u64(row.try_get("event_count")?, "event count")?,
        observed_payload_bytes: to_u64(
            row.try_get("observed_payload_bytes")?,
            "observed payload bytes",
        )?,
        last_event_seq: to_u64(row.try_get("last_event_seq")?, "last event sequence")?,
        event_chain_digest: row.try_get("event_chain_digest")?,
        counts: InvocationEventCounts {
            assistant: to_u64(row.try_get("assistant_event_count")?, "assistant events")?,
            reasoning: to_u64(row.try_get("reasoning_event_count")?, "reasoning events")?,
            tool: to_u64(row.try_get("tool_event_count")?, "tool events")?,
            plan: to_u64(row.try_get("plan_event_count")?, "plan events")?,
            usage: to_u64(row.try_get("usage_event_count")?, "usage events")?,
            metadata: to_u64(row.try_get("metadata_event_count")?, "metadata events")?,
            permission: to_u64(row.try_get("permission_event_count")?, "permission events")?,
            unknown: to_u64(row.try_get("unknown_event_count")?, "unknown events")?,
        },
        terminal_at_ms: row.try_get("terminal_at_ms")?,
        stop_reason: row.try_get("stop_reason")?,
        runtime_stop_reason: row.try_get("runtime_stop_reason")?,
        execution_certainty: row
            .try_get::<Option<String>, _>("execution_certainty")?
            .as_deref()
            .map(|value| {
                ExecutionCertainty::parse(value)
                    .ok_or_else(|| invalid_stored("execution certainty", value))
            })
            .transpose()?,
        session_quiescent: row
            .try_get::<Option<i64>, _>("session_quiescent")?
            .map(|value| value != 0),
        session_persistence: row
            .try_get::<Option<String>, _>("session_persistence")?
            .as_deref()
            .map(|value| {
                SessionPersistence::parse(value)
                    .ok_or_else(|| invalid_stored("session persistence", value))
            })
            .transpose()?,
        usage: row
            .try_get::<Option<String>, _>("usage_json")?
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
    })
}

/// Projects the current seat for every agent, or one named agent.
///
/// A seat joins the worker generation, the native-session lane, the managed
/// invocation, and the delivery it came from. The point is that an interrupted
/// seat is visible in one read: recovering one is a separate operation, and
/// without this an operator has to correlate four endpoints to find it.
///
/// # Errors
///
/// Returns an error when durable rows cannot be read or decoded.
pub async fn list_agent_seats(
    store: &Store,
    agent_id: Option<&str>,
) -> Result<Vec<AgentSeat>, FleetError> {
    let rows = sqlx::query(agent_seat_select())
        .bind(agent_id)
        .bind(agent_id)
        .fetch_all(store.pool())
        .await?;
    let now = now_ms();
    rows.iter()
        .map(|row| agent_seat_from_row(row, now))
        .collect()
}

/// Reads one stored state through the codec that owns its spelling.
///
/// The three states a seat projects are stored as text in three different
/// tables. Each enum knows its own spelling, so this only has to say which
/// column failed.
fn stored_state<T>(
    stored: Option<String>,
    parse: impl Fn(&str) -> Option<T>,
    column: &str,
) -> Result<Option<T>, FleetError> {
    stored
        .map(|value| parse(&value).ok_or_else(|| invalid_stored(column, &value)))
        .transpose()
}

fn agent_seat_from_row(row: &sqlx::sqlite::SqliteRow, now: i64) -> Result<AgentSeat, FleetError> {
    let generation_health = generation_health_from_seat_row(row)?;
    let session_state = stored_state(
        row.try_get("session_state")?,
        SessionBindingState::parse,
        "session binding state",
    )?;
    let invocation_state = stored_state(
        row.try_get("invocation_state")?,
        InvocationState::parse,
        "invocation state",
    )?;
    let delivery_state = stored_state(
        row.try_get("delivery_state")?,
        DeliveryState::parse,
        "delivery state",
    )?;
    let lease_expires_at_ms: Option<i64> = row.try_get("lease_expires_at_ms")?;
    let delivery_lease_expires_at_ms: Option<i64> = row.try_get("delivery_lease_expires_at_ms")?;
    let lease_expired = lease_expires_at_ms.is_some_and(|expiry| expiry <= now)
        || (delivery_state == Some(DeliveryState::Leased)
            && delivery_lease_expires_at_ms.is_some_and(|expiry| expiry <= now));
    let (state, reason) = seat_state(
        session_state,
        invocation_state.as_ref(),
        generation_health,
        lease_expired,
    );
    Ok(AgentSeat {
        agent_id: row.try_get("agent_id")?,
        state,
        reason,
        generation_id: row.try_get("generation_id")?,
        generation_health,
        binding_id: row.try_get("binding_id")?,
        binding_generation: row
            .try_get::<Option<i64>, _>("binding_generation")?
            .map(|value| to_u64(value, "binding generation"))
            .transpose()?,
        owner_epoch: row
            .try_get::<Option<i64>, _>("owner_epoch")?
            .map(|value| to_u64(value, "owner epoch"))
            .transpose()?,
        session_state,
        invocation_id: row.try_get("invocation_id")?,
        invocation_state,
        source_message_id: row.try_get("source_message_id")?,
        delivery_state,
        lease_expires_at_ms,
        lease_expired,
        last_progress_at_ms: row
            .try_get::<Option<i64>, _>("last_event_at_ms")?
            .or(row.try_get("observation_updated_at_ms")?)
            .or(row.try_get("dispatch_armed_at_ms")?)
            .or(row.try_get("reserved_at_ms")?),
        unresolved_block_id: row.try_get("unresolved_block_id")?,
    })
}

/// Decides the seat state and the exact evidence responsible for it.
///
/// Ordered most-urgent first: an uncertain session needs an operator before
/// anything else can use the lane, and an armed attempt whose lease or worker
/// is gone is interrupted rather than working.
fn seat_state(
    session_state: Option<SessionBindingState>,
    invocation_state: Option<&InvocationState>,
    generation_health: Option<PluginGenerationHealth>,
    lease_expired: bool,
) -> (AgentSeatState, AgentSeatReason) {
    let working = generation_health == Some(PluginGenerationHealth::Active);
    match (session_state, invocation_state) {
        (Some(SessionBindingState::Uncertain), _) => (
            AgentSeatState::RecoveryRequired,
            AgentSeatReason::UncertainSession,
        ),
        (_, Some(InvocationState::DispatchArmed)) if lease_expired => (
            AgentSeatState::Interrupted,
            AgentSeatReason::ArmedLeaseExpired,
        ),
        (_, Some(InvocationState::DispatchArmed)) if !working => (
            AgentSeatState::Interrupted,
            AgentSeatReason::ArmedGenerationUnavailable,
        ),
        (_, Some(InvocationState::DispatchArmed)) => {
            (AgentSeatState::Working, AgentSeatReason::ActiveInvocation)
        }
        (_, Some(InvocationState::Reserved)) if !working => (
            AgentSeatState::Interrupted,
            AgentSeatReason::ReservedGenerationUnavailable,
        ),
        (_, Some(InvocationState::Reserved)) => {
            (AgentSeatState::Working, AgentSeatReason::ReservedInvocation)
        }
        _ => match generation_health {
            Some(PluginGenerationHealth::Active) => match session_state {
                Some(SessionBindingState::Opening) => {
                    (AgentSeatState::Working, AgentSeatReason::OpeningSession)
                }
                Some(SessionBindingState::Active) => {
                    (AgentSeatState::Idle, AgentSeatReason::ReadySession)
                }
                _ => (AgentSeatState::Idle, AgentSeatReason::Ready),
            },
            Some(PluginGenerationHealth::Stale) => {
                (AgentSeatState::Offline, AgentSeatReason::GenerationStale)
            }
            Some(PluginGenerationHealth::Stopped) => {
                (AgentSeatState::Offline, AgentSeatReason::GenerationStopped)
            }
            None if session_state.is_some_and(|state| state != SessionBindingState::Retired) => (
                AgentSeatState::Interrupted,
                AgentSeatReason::SessionWithoutWorker,
            ),
            None => (AgentSeatState::Unmanaged, AgentSeatReason::NoWorkerObserved),
        },
    }
}

fn generation_health_from_seat_row(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<Option<PluginGenerationHealth>, FleetError> {
    let Some(state) = row.try_get::<Option<String>, _>("generation_state")? else {
        return Ok(None);
    };
    let state = parse_generation_state(&state)?;
    let last_heartbeat_at_ms = row.try_get("last_heartbeat_at_ms")?;
    let heartbeat_interval_ms =
        to_u64(row.try_get("heartbeat_interval_ms")?, "heartbeat interval")?;
    Ok(Some(generation_health(
        state,
        last_heartbeat_at_ms,
        heartbeat_interval_ms,
    )))
}

const fn agent_seat_select() -> &'static str {
    r"
    SELECT a.id AS agent_id,
           g.id AS generation_id, g.state AS generation_state,
           g.last_heartbeat_at_ms, g.heartbeat_interval_ms,
           s.binding_id, s.binding_generation, s.owner_epoch,
           s.state AS session_state,
           i.id AS invocation_id, i.state AS invocation_state,
           i.lease_expires_at_ms, i.reserved_at_ms, i.dispatch_armed_at_ms,
           source.id AS source_message_id,
           o.updated_at_ms AS observation_updated_at_ms, o.last_event_at_ms,
           d.state AS delivery_state,
           d.lease_expires_at_ms AS delivery_lease_expires_at_ms,
           b.id AS unresolved_block_id
    FROM agents a
    LEFT JOIN session_bindings s ON s.rowid = (
        SELECT candidate.rowid
        FROM session_bindings candidate
        WHERE candidate.agent_id = a.id
        ORDER BY CASE WHEN candidate.state = 'retired' THEN 1 ELSE 0 END,
                 candidate.updated_at_ms DESC, candidate.binding_id,
                 candidate.binding_generation DESC
        LIMIT 1
    )
    LEFT JOIN invocations i ON i.id = COALESCE(
        s.active_invocation_id,
        (
            SELECT candidate.id
            FROM invocations candidate
            WHERE candidate.agent_id = a.id AND candidate.state != 'terminal'
            ORDER BY candidate.reserved_at_ms DESC, candidate.id DESC
            LIMIT 1
        )
    )
    LEFT JOIN invocation_observations o ON o.invocation_id = i.id
    LEFT JOIN plugin_generations g ON g.id = COALESCE(
        o.generation_id,
        (
            SELECT candidate.id
            FROM plugin_generations candidate
            WHERE candidate.agent_id = a.id
            ORDER BY candidate.started_at_ms DESC, candidate.id
            LIMIT 1
        )
    )
    LEFT JOIN messages source ON source.seq = i.message_seq
    LEFT JOIN agent_deliveries d
      ON d.message_seq = i.message_seq AND d.agent_id = i.agent_id
    LEFT JOIN delivery_blocks b
      ON b.message_seq = d.message_seq AND b.agent_id = d.agent_id
     AND b.resolved_at_ms IS NULL
    WHERE (? IS NULL OR a.id = ?)
    ORDER BY a.created_at_ms, a.id
    LIMIT 500
    "
}

fn event_digest(event_seq: i64, observed_at_ms: i64, classification: &str, raw: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(event_seq.to_be_bytes());
    digest.update(observed_at_ms.to_be_bytes());
    digest.update(classification.as_bytes());
    digest.update([0]);
    digest.update(raw);
    format!("sha256:{:x}", digest.finalize())
}

fn chain_digest(previous: Option<&str>, event: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(previous.unwrap_or("sha256:genesis").as_bytes());
    digest.update([0]);
    digest.update(event.as_bytes());
    format!("sha256:{:x}", digest.finalize())
}

#[derive(Default)]
struct EventIncrements {
    assistant: i64,
    reasoning: i64,
    tool: i64,
    plan: i64,
    usage: i64,
    metadata: i64,
    permission: i64,
    unknown: i64,
}

fn event_increments(classification: &str) -> EventIncrements {
    let mut counts = EventIncrements::default();
    match classification {
        "agent_message_content" => counts.assistant = 1,
        "reasoning_content" => counts.reasoning = 1,
        "tool_call" | "tool_call_update" => counts.tool = 1,
        "plan_update" => counts.plan = 1,
        "usage" => counts.usage = 1,
        "metadata" => counts.metadata = 1,
        "permission_request" => counts.permission = 1,
        _ => counts.unknown = 1,
    }
    counts
}

fn terminal_matches(
    row: &sqlx::sqlite::SqliteRow,
    terminal: &TurnTerminal,
    certainty: &str,
    persistence: &str,
    usage_json: &str,
) -> Result<bool, FleetError> {
    Ok(row.try_get::<Option<String>, _>("stop_reason")?.as_deref()
        == Some(terminal.stop_reason.as_str())
        && row.try_get::<Option<String>, _>("runtime_stop_reason")? == terminal.runtime_stop_reason
        && row
            .try_get::<Option<String>, _>("execution_certainty")?
            .as_deref()
            == Some(certainty)
        && row
            .try_get::<Option<i64>, _>("session_quiescent")?
            .map(|value| value != 0)
            == Some(terminal.session_quiescent)
        && row
            .try_get::<Option<String>, _>("session_persistence")?
            .as_deref()
            == Some(persistence)
        && row.try_get::<Option<String>, _>("usage_json")?.as_deref() == Some(usage_json))
}

fn generation_health(
    state: PluginGenerationState,
    heartbeat_at_ms: i64,
    heartbeat_interval_ms: u64,
) -> PluginGenerationHealth {
    if state == PluginGenerationState::Stopped {
        return PluginGenerationHealth::Stopped;
    }
    let heartbeat_interval_ms = i64::try_from(heartbeat_interval_ms).unwrap_or(i64::MAX);
    let stale_after = heartbeat_interval_ms
        .saturating_mul(3)
        .max(MIN_STALE_AFTER_MS);
    if now_ms().saturating_sub(heartbeat_at_ms) > stale_after {
        PluginGenerationHealth::Stale
    } else {
        PluginGenerationHealth::Active
    }
}

fn parse_generation_state(value: &str) -> Result<PluginGenerationState, FleetError> {
    match value {
        "active" => Ok(PluginGenerationState::Active),
        "stopped" => Ok(PluginGenerationState::Stopped),
        _ => Err(invalid_stored("plugin generation state", value)),
    }
}

fn to_u64(value: i64, label: &str) -> Result<u64, FleetError> {
    u64::try_from(value).map_err(|_| {
        FleetError::Database(sqlx::Error::Decode(
            format!("stored {label} is negative").into(),
        ))
    })
}

fn to_u32(value: i64, label: &str) -> Result<u32, FleetError> {
    u32::try_from(value).map_err(|_| {
        FleetError::Database(sqlx::Error::Decode(
            format!("stored {label} is out of range").into(),
        ))
    })
}

fn to_usize(value: i64, label: &str) -> Result<usize, FleetError> {
    usize::try_from(value).map_err(|_| {
        FleetError::Database(sqlx::Error::Decode(
            format!("stored {label} is out of range").into(),
        ))
    })
}

fn invalid_stored(label: &str, value: &str) -> FleetError {
    FleetError::Database(sqlx::Error::Decode(
        format!("invalid stored {label}: {value}").into(),
    ))
}

fn is_unique_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.is_unique_violation())
}

fn is_foreign_key_violation(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.code().as_deref() == Some("787"))
}
