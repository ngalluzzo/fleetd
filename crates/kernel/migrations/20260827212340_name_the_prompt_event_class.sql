-- name_the_prompt_event_class
--
-- ACP's `user_message_chunk` had no case in the host's classifier, so it fell
-- through to `unknown`. A real harness emits one per turn -- it is the prompt
-- Fleetd sent, echoed back -- so every managed OpenCode turn carried a constant
-- `unknown_event_count` of at least one for a kind that is entirely recognised.
-- ADR 0020 keeps that counter because an unrecognised update is the one an
-- operator most needs to see, and a permanent offset is exactly what makes such
-- a signal unreadable.
--
-- Rows written before this migration keep their existing totals. Their prompt
-- events were already counted as unknown and cannot be reattributed, because
-- the raw updates that would say how many were never retained. So this adds a
-- column and does not rewrite history: an observation from before this point
-- reports `prompt_event_count = 0` and an `unknown_event_count` that still
-- includes its prompts.
ALTER TABLE invocation_observations
    ADD COLUMN prompt_event_count INTEGER NOT NULL DEFAULT 0
        CHECK (prompt_event_count >= 0);
