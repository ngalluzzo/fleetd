CREATE TABLE session_bindings (
    binding_id TEXT NOT NULL,
    binding_generation INTEGER NOT NULL CHECK (binding_generation > 0),
    agent_id TEXT NOT NULL REFERENCES agents(id),
    lane_policy TEXT NOT NULL,
    lane_key TEXT NOT NULL,
    owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0),
    owner_instance_id TEXT NOT NULL,
    profile_digest TEXT NOT NULL,
    compatibility_digest TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    additional_directories_json TEXT NOT NULL,
    session_ref TEXT,
    state TEXT NOT NULL
        CHECK (state IN ('opening', 'ready', 'active', 'uncertain', 'retired')),
    active_invocation_id TEXT REFERENCES invocations(id),
    last_quiescent_invocation_id TEXT REFERENCES invocations(id),
    session_persistence TEXT
        CHECK (session_persistence IN ('confirmed', 'runtime_claimed', 'unknown')),
    uncertain_reason TEXT,
    retired_reason TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    opened_at_ms INTEGER,
    retired_at_ms INTEGER,
    PRIMARY KEY (binding_id, binding_generation),
    UNIQUE (agent_id, lane_policy, lane_key, binding_generation),
    CHECK (
        (state = 'opening'
            AND session_ref IS NULL
            AND active_invocation_id IS NULL
            AND uncertain_reason IS NULL
            AND retired_reason IS NULL
            AND opened_at_ms IS NULL
            AND retired_at_ms IS NULL)
        OR
        (state = 'ready'
            AND session_ref IS NOT NULL
            AND active_invocation_id IS NULL
            AND uncertain_reason IS NULL
            AND retired_reason IS NULL
            AND opened_at_ms IS NOT NULL
            AND retired_at_ms IS NULL)
        OR
        (state = 'active'
            AND session_ref IS NOT NULL
            AND active_invocation_id IS NOT NULL
            AND uncertain_reason IS NULL
            AND retired_reason IS NULL
            AND opened_at_ms IS NOT NULL
            AND retired_at_ms IS NULL)
        OR
        (state = 'uncertain'
            AND session_ref IS NOT NULL
            AND active_invocation_id IS NOT NULL
            AND uncertain_reason IS NOT NULL
            AND retired_reason IS NULL
            AND opened_at_ms IS NOT NULL
            AND retired_at_ms IS NULL)
        OR
        (state = 'retired'
            AND active_invocation_id IS NULL
            AND retired_reason IS NOT NULL
            AND retired_at_ms IS NOT NULL)
    )
);

CREATE UNIQUE INDEX session_bindings_current_lane
    ON session_bindings(agent_id, lane_policy, lane_key)
    WHERE state != 'retired';

CREATE INDEX session_bindings_agent_state
    ON session_bindings(agent_id, state, updated_at_ms);

CREATE TABLE session_binding_turns (
    invocation_id TEXT PRIMARY KEY REFERENCES invocations(id),
    binding_id TEXT NOT NULL,
    binding_generation INTEGER NOT NULL,
    owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0),
    state TEXT NOT NULL CHECK (state IN ('active', 'quiescent', 'uncertain')),
    started_at_ms INTEGER NOT NULL,
    terminal_at_ms INTEGER,
    session_persistence TEXT
        CHECK (session_persistence IN ('confirmed', 'runtime_claimed', 'unknown')),
    uncertain_reason TEXT,
    FOREIGN KEY (binding_id, binding_generation)
        REFERENCES session_bindings(binding_id, binding_generation),
    CHECK (
        (state = 'active'
            AND terminal_at_ms IS NULL
            AND session_persistence IS NULL
            AND uncertain_reason IS NULL)
        OR
        (state = 'quiescent'
            AND terminal_at_ms IS NOT NULL
            AND session_persistence IS NOT NULL
            AND uncertain_reason IS NULL)
        OR
        (state = 'uncertain'
            AND terminal_at_ms IS NOT NULL
            AND session_persistence IS NULL
            AND uncertain_reason IS NOT NULL)
    )
);

CREATE INDEX session_binding_turns_binding
    ON session_binding_turns(binding_id, binding_generation, started_at_ms);
