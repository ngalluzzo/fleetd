-- backfill_membership_read_cursor
UPDATE channel_members
SET read_through_seq = COALESCE(
    (
        SELECT MAX(messages.seq)
        FROM messages
        WHERE messages.channel_id = channel_members.channel_id
    ),
    0
);
