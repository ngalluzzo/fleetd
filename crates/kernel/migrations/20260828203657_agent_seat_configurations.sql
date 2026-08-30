-- Desired execution for a stable agent identity. This is execution-owned
-- state: the kernel still knows only that the agent exists and participates in
-- channels. A local supervisor resolves profile_id against its private,
-- machine-owned catalog before it starts anything.
CREATE TABLE agent_seat_configurations (
    agent_id TEXT PRIMARY KEY NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
    profile_id TEXT NOT NULL,
    instructions TEXT NOT NULL,
    desired_state TEXT NOT NULL CHECK (desired_state IN ('running', 'stopped')),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX agent_seat_configurations_desired_state_updated
    ON agent_seat_configurations(desired_state, updated_at_ms, agent_id);
