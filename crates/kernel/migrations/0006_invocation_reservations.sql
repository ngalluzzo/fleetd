CREATE TABLE invocations (
    id TEXT PRIMARY KEY,
    message_seq INTEGER NOT NULL,
    agent_id TEXT NOT NULL,
    delivery_attempt INTEGER NOT NULL CHECK (delivery_attempt > 0),
    lease_token TEXT NOT NULL,
    lease_expires_at_ms INTEGER NOT NULL,
    fence_token TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL
        CHECK (state IN ('reserved', 'dispatch_armed', 'terminal')),
    reserved_at_ms INTEGER NOT NULL,
    dispatch_armed_at_ms INTEGER,
    terminal_at_ms INTEGER,
    execution_certainty TEXT
        CHECK (execution_certainty IN ('not_started', 'outcome_known', 'outcome_unknown')),
    terminal_reason TEXT,
    result_message_seq INTEGER UNIQUE REFERENCES messages(seq),
    UNIQUE (message_seq, agent_id, delivery_attempt),
    FOREIGN KEY (message_seq, agent_id)
        REFERENCES agent_deliveries(message_seq, agent_id),
    CHECK (
        (state = 'reserved'
            AND dispatch_armed_at_ms IS NULL
            AND terminal_at_ms IS NULL
            AND execution_certainty IS NULL
            AND terminal_reason IS NULL)
        OR
        (state = 'dispatch_armed'
            AND dispatch_armed_at_ms IS NOT NULL
            AND terminal_at_ms IS NULL
            AND execution_certainty IS NULL
            AND terminal_reason IS NULL)
        OR
        (state = 'terminal'
            AND terminal_at_ms IS NOT NULL
            AND execution_certainty IS NOT NULL
            AND terminal_reason IS NOT NULL)
    ),
    CHECK (
        (terminal_reason = 'completed' AND result_message_seq IS NOT NULL)
        OR
        (terminal_reason IS NULL AND result_message_seq IS NULL)
        OR
        (terminal_reason != 'completed' AND result_message_seq IS NULL)
    )
);

CREATE INDEX invocations_agent_reserved
    ON invocations(agent_id, reserved_at_ms, id);

CREATE INDEX invocations_active_lease
    ON invocations(agent_id, state, lease_expires_at_ms);
