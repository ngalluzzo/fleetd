CREATE TABLE auth_credentials (
    id TEXT PRIMARY KEY,
    principal_kind TEXT NOT NULL
        CHECK (principal_kind IN ('operator', 'agent')),
    agent_id TEXT REFERENCES agents(id) ON DELETE CASCADE,
    token_digest BLOB NOT NULL UNIQUE CHECK (length(token_digest) = 32),
    created_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER,
    CHECK (
        (principal_kind = 'operator' AND agent_id IS NULL)
        OR
        (principal_kind = 'agent' AND agent_id IS NOT NULL)
    )
);

CREATE INDEX auth_credentials_agent
    ON auth_credentials(agent_id, revoked_at_ms);
