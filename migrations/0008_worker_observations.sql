CREATE TABLE plugin_generations (
    id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL REFERENCES agents(id),
    plugin_id TEXT NOT NULL,
    plugin_name TEXT NOT NULL,
    plugin_version TEXT NOT NULL,
    interfaces_json TEXT NOT NULL,
    process_id INTEGER,
    driver_version TEXT NOT NULL,
    acp_sdk_version TEXT NOT NULL,
    acp_protocol_version INTEGER NOT NULL CHECK (acp_protocol_version > 0),
    runtime_name TEXT NOT NULL,
    runtime_version TEXT NOT NULL,
    runtime_executable_digest TEXT NOT NULL,
    agent_capabilities_json TEXT NOT NULL,
    max_concurrent_turns INTEGER NOT NULL CHECK (max_concurrent_turns > 0),
    max_frame_bytes INTEGER NOT NULL CHECK (max_frame_bytes > 0),
    profile_digest TEXT NOT NULL,
    compatibility_digest TEXT NOT NULL,
    raw_initialize_result_json TEXT NOT NULL,
    heartbeat_interval_ms INTEGER NOT NULL CHECK (heartbeat_interval_ms > 0),
    state TEXT NOT NULL CHECK (state IN ('active', 'stopped')),
    started_at_ms INTEGER NOT NULL,
    last_heartbeat_at_ms INTEGER NOT NULL,
    stopped_at_ms INTEGER,
    stop_disposition TEXT CHECK (stop_disposition IN ('stopped', 'restart', 'fatal')),
    stop_reason TEXT,
    shutdown_outcome TEXT CHECK (shutdown_outcome IN ('graceful', 'forced', 'failed')),
    shutdown_exit_code INTEGER,
    CHECK (
        (state = 'active'
            AND stopped_at_ms IS NULL
            AND stop_disposition IS NULL
            AND stop_reason IS NULL
            AND shutdown_outcome IS NULL
            AND shutdown_exit_code IS NULL)
        OR
        (state = 'stopped'
            AND stopped_at_ms IS NOT NULL
            AND stop_disposition IS NOT NULL
            AND stop_reason IS NOT NULL
            AND shutdown_outcome IS NOT NULL)
    )
);

CREATE INDEX plugin_generations_agent_started
    ON plugin_generations(agent_id, started_at_ms DESC, id);

CREATE INDEX plugin_generations_active_heartbeat
    ON plugin_generations(state, last_heartbeat_at_ms);

CREATE TABLE invocation_observations (
    invocation_id TEXT PRIMARY KEY REFERENCES invocations(id),
    generation_id TEXT NOT NULL REFERENCES plugin_generations(id),
    binding_id TEXT NOT NULL,
    binding_generation INTEGER NOT NULL CHECK (binding_generation > 0),
    owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0),
    started_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    first_event_at_ms INTEGER,
    last_event_at_ms INTEGER,
    event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0),
    observed_payload_bytes INTEGER NOT NULL DEFAULT 0 CHECK (observed_payload_bytes >= 0),
    last_event_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_event_seq >= 0),
    last_event_digest TEXT,
    event_chain_digest TEXT,
    assistant_event_count INTEGER NOT NULL DEFAULT 0 CHECK (assistant_event_count >= 0),
    reasoning_event_count INTEGER NOT NULL DEFAULT 0 CHECK (reasoning_event_count >= 0),
    tool_event_count INTEGER NOT NULL DEFAULT 0 CHECK (tool_event_count >= 0),
    plan_event_count INTEGER NOT NULL DEFAULT 0 CHECK (plan_event_count >= 0),
    usage_event_count INTEGER NOT NULL DEFAULT 0 CHECK (usage_event_count >= 0),
    metadata_event_count INTEGER NOT NULL DEFAULT 0 CHECK (metadata_event_count >= 0),
    permission_event_count INTEGER NOT NULL DEFAULT 0 CHECK (permission_event_count >= 0),
    unknown_event_count INTEGER NOT NULL DEFAULT 0 CHECK (unknown_event_count >= 0),
    terminal_at_ms INTEGER,
    stop_reason TEXT,
    runtime_stop_reason TEXT,
    execution_certainty TEXT CHECK (execution_certainty IN ('not_started', 'outcome_known', 'outcome_unknown')),
    session_quiescent INTEGER CHECK (session_quiescent IN (0, 1)),
    session_persistence TEXT CHECK (session_persistence IN ('confirmed', 'runtime_claimed', 'unknown')),
    usage_json TEXT,
    FOREIGN KEY (binding_id, binding_generation)
        REFERENCES session_bindings(binding_id, binding_generation),
    CHECK (
        (event_count = 0
            AND first_event_at_ms IS NULL
            AND last_event_at_ms IS NULL
            AND last_event_seq = 0
            AND last_event_digest IS NULL
            AND event_chain_digest IS NULL)
        OR
        (event_count > 0
            AND first_event_at_ms IS NOT NULL
            AND last_event_at_ms IS NOT NULL
            AND last_event_seq > 0
            AND last_event_digest IS NOT NULL
            AND event_chain_digest IS NOT NULL)
    ),
    CHECK (
        (terminal_at_ms IS NULL
            AND stop_reason IS NULL
            AND runtime_stop_reason IS NULL
            AND execution_certainty IS NULL
            AND session_quiescent IS NULL
            AND session_persistence IS NULL
            AND usage_json IS NULL)
        OR
        (terminal_at_ms IS NOT NULL
            AND stop_reason IS NOT NULL
            AND execution_certainty IS NOT NULL
            AND session_quiescent IS NOT NULL
            AND session_persistence IS NOT NULL
            AND usage_json IS NOT NULL)
    )
);

CREATE INDEX invocation_observations_generation
    ON invocation_observations(generation_id, started_at_ms DESC, invocation_id);

CREATE INDEX invocation_observations_terminal
    ON invocation_observations(terminal_at_ms, updated_at_ms DESC);
