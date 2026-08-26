DROP INDEX agent_deliveries_claimable;

ALTER TABLE agent_deliveries RENAME TO agent_deliveries_before_blocking;

CREATE TABLE agent_deliveries (
    message_seq INTEGER NOT NULL REFERENCES messages(seq),
    agent_id TEXT NOT NULL REFERENCES agents(id),
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'blocked', 'acknowledged', 'dead')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    available_at_ms INTEGER NOT NULL,
    lease_token TEXT,
    lease_expires_at_ms INTEGER,
    last_error TEXT,
    last_settled_lease_token TEXT,
    last_settlement TEXT CHECK (last_settlement IN ('acknowledged', 'retry', 'blocked')),
    acknowledged_at_ms INTEGER,
    created_at_ms INTEGER NOT NULL,
    PRIMARY KEY (message_seq, agent_id),
    CHECK (
        (state = 'leased' AND lease_token IS NOT NULL AND lease_expires_at_ms IS NOT NULL)
        OR
        (state != 'leased' AND lease_token IS NULL AND lease_expires_at_ms IS NULL)
    ),
    CHECK (state != 'acknowledged' OR acknowledged_at_ms IS NOT NULL)
);

INSERT INTO agent_deliveries (
    message_seq, agent_id, state, attempt, available_at_ms, lease_token,
    lease_expires_at_ms, last_error, last_settled_lease_token,
    last_settlement, acknowledged_at_ms, created_at_ms
)
SELECT
    message_seq, agent_id, state, attempt, available_at_ms, lease_token,
    lease_expires_at_ms, last_error, last_settled_lease_token,
    last_settlement, acknowledged_at_ms, created_at_ms
FROM agent_deliveries_before_blocking;

DROP TABLE agent_deliveries_before_blocking;

CREATE INDEX agent_deliveries_claimable
    ON agent_deliveries(agent_id, state, available_at_ms, message_seq);

CREATE TABLE delivery_blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_seq INTEGER NOT NULL,
    agent_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    lease_token TEXT NOT NULL,
    reason TEXT NOT NULL,
    blocked_at_ms INTEGER NOT NULL,
    resolved_at_ms INTEGER,
    resolution TEXT CHECK (resolution IN ('requeued', 'abandoned')),
    resolution_note TEXT,
    retry_after_ms INTEGER CHECK (retry_after_ms >= 0),
    UNIQUE (message_seq, agent_id, lease_token),
    FOREIGN KEY (message_seq, agent_id)
        REFERENCES agent_deliveries(message_seq, agent_id),
    CHECK (
        (resolved_at_ms IS NULL AND resolution IS NULL)
        OR
        (resolved_at_ms IS NOT NULL AND resolution IS NOT NULL)
    )
);

CREATE INDEX delivery_blocks_unresolved
    ON delivery_blocks(agent_id, blocked_at_ms, id)
    WHERE resolved_at_ms IS NULL;
