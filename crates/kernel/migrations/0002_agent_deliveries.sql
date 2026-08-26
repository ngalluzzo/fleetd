CREATE TABLE agent_deliveries (
    message_seq INTEGER NOT NULL REFERENCES messages(seq),
    agent_id TEXT NOT NULL REFERENCES agents(id),
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'acknowledged', 'dead')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    available_at_ms INTEGER NOT NULL,
    lease_token TEXT,
    lease_expires_at_ms INTEGER,
    last_error TEXT,
    last_settled_lease_token TEXT,
    last_settlement TEXT CHECK (last_settlement IN ('acknowledged', 'retry')),
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

CREATE INDEX agent_deliveries_claimable
    ON agent_deliveries(agent_id, state, available_at_ms, message_seq);
