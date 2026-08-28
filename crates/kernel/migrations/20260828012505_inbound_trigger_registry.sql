-- inbound_trigger_registry
--
-- A trigger creates work with no human present and no invocation to scope to.
-- ADR 0031 gives it standing but narrow authority, and this table is what makes
-- "narrow" checkable: the channel it may reach and the kinds it may create are
-- stored here, not supplied when it fires.
--
-- `sender_id` references an existing agent rather than introducing a principal
-- of its own, so a trigger's messages stay attributable to a durable identity
-- the fleet already knows.
--
-- The firing columns are the reason to register a trigger at all. A crontab
-- entry that stopped firing on Tuesday leaves no trace, and an idle fleet looks
-- exactly like a healthy quiet one.
CREATE TABLE triggers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    channel_id TEXT NOT NULL REFERENCES channels(id),
    sender_id TEXT NOT NULL REFERENCES agents(id),
    -- A sorted, deduplicated JSON array. Sorted so the stored form is one
    -- representation of one set, which is what lets it participate in identity.
    accepted_kinds_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    last_occurrence_id TEXT,
    last_fired_at_ms INTEGER,
    accepted_occurrences INTEGER NOT NULL DEFAULT 0
        CHECK (accepted_occurrences >= 0),
    retired_at_ms INTEGER,
    retired_reason TEXT,
    -- A trigger has fired or it has not; a half-recorded firing is a bug in the
    -- composition above, not a state this table will hold.
    CHECK (
        (accepted_occurrences = 0
            AND last_occurrence_id IS NULL
            AND last_fired_at_ms IS NULL)
        OR
        (accepted_occurrences > 0
            AND last_occurrence_id IS NOT NULL
            AND last_fired_at_ms IS NOT NULL)
    ),
    CHECK (
        (state = 'active' AND retired_at_ms IS NULL AND retired_reason IS NULL)
        OR
        (state = 'retired' AND retired_at_ms IS NOT NULL)
    )
);

CREATE INDEX triggers_by_channel ON triggers (channel_id, state);
