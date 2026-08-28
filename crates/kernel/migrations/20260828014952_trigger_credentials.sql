-- trigger_credentials
--
-- The authority model gains a third category. It was operator-held or
-- invocation-scoped; it becomes operator-held, invocation-scoped, or
-- trigger-standing, and this is where the third one becomes storable.
--
-- A trigger credential binds to the registration rather than to an agent. The
-- sender is derived from the registration, so binding to the trigger is what
-- keeps a trigger from choosing who it speaks as.
--
-- SQLite cannot alter a CHECK constraint, so the table is rebuilt. The
-- constraint is the reason this migration exists at all: without it, `trigger`
-- would be a value the column happens to hold rather than one it permits.

DROP INDEX auth_credentials_agent;

ALTER TABLE auth_credentials RENAME TO auth_credentials_before_triggers;

CREATE TABLE auth_credentials (
    id TEXT PRIMARY KEY,
    principal_kind TEXT NOT NULL
        CHECK (principal_kind IN ('operator', 'agent', 'trigger')),
    agent_id TEXT REFERENCES agents(id) ON DELETE CASCADE,
    trigger_id TEXT REFERENCES triggers(id) ON DELETE CASCADE,
    token_digest BLOB NOT NULL UNIQUE CHECK (length(token_digest) = 32),
    created_at_ms INTEGER NOT NULL,
    revoked_at_ms INTEGER,
    -- Exactly one binding, or none for an operator. A credential holding both
    -- would authenticate as whichever the reader looked for first.
    CHECK (
        (principal_kind = 'operator' AND agent_id IS NULL AND trigger_id IS NULL)
        OR
        (principal_kind = 'agent' AND agent_id IS NOT NULL AND trigger_id IS NULL)
        OR
        (principal_kind = 'trigger' AND agent_id IS NULL AND trigger_id IS NOT NULL)
    )
);

INSERT INTO auth_credentials (
    id, principal_kind, agent_id, trigger_id, token_digest, created_at_ms,
    revoked_at_ms
)
SELECT
    id, principal_kind, agent_id, NULL, token_digest, created_at_ms,
    revoked_at_ms
FROM auth_credentials_before_triggers;

DROP TABLE auth_credentials_before_triggers;

CREATE INDEX auth_credentials_agent
    ON auth_credentials(agent_id, revoked_at_ms);

CREATE INDEX auth_credentials_trigger
    ON auth_credentials(trigger_id, revoked_at_ms);
