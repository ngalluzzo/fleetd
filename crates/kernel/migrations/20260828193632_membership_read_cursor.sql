ALTER TABLE channel_members
ADD COLUMN read_through_seq INTEGER NOT NULL DEFAULT 0
    CHECK (read_through_seq >= 0);
