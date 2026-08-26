ALTER TABLE channels
ADD COLUMN conversation_kind TEXT NOT NULL DEFAULT 'shared'
    CHECK (conversation_kind IN ('shared', 'direct'));

ALTER TABLE channels
ADD COLUMN direct_pair_key TEXT;

ALTER TABLE channels
ADD COLUMN archived_at_ms INTEGER;

CREATE UNIQUE INDEX channels_direct_pair_key
    ON channels(direct_pair_key)
    WHERE direct_pair_key IS NOT NULL;

CREATE TRIGGER channels_conversation_identity_on_insert
BEFORE INSERT ON channels
WHEN (NEW.conversation_kind = 'shared' AND NEW.direct_pair_key IS NOT NULL)
  OR (NEW.conversation_kind = 'direct' AND NEW.direct_pair_key IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'conversation kind and direct pair key disagree');
END;

CREATE TRIGGER channels_conversation_identity_is_immutable
BEFORE UPDATE OF conversation_kind, direct_pair_key ON channels
WHEN OLD.conversation_kind != NEW.conversation_kind
  OR OLD.direct_pair_key IS NOT NEW.direct_pair_key
BEGIN
    SELECT RAISE(ABORT, 'conversation identity is immutable');
END;

CREATE TRIGGER direct_conversation_lifecycle_is_fixed
BEFORE UPDATE OF name, archived_at_ms ON channels
WHEN OLD.conversation_kind = 'direct'
  AND (OLD.name != NEW.name OR OLD.archived_at_ms IS NOT NEW.archived_at_ms)
BEGIN
    SELECT RAISE(ABORT, 'direct conversation lifecycle is fixed');
END;

CREATE TRIGGER direct_conversation_has_at_most_two_members
BEFORE INSERT ON channel_members
WHEN (SELECT conversation_kind FROM channels WHERE id = NEW.channel_id) = 'direct'
 AND (SELECT COUNT(*) FROM channel_members WHERE channel_id = NEW.channel_id) >= 2
BEGIN
    SELECT RAISE(ABORT, 'direct conversation already has two members');
END;
