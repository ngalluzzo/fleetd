ALTER TABLE channel_members
ADD COLUMN delivery_mode TEXT NOT NULL DEFAULT 'inbox'
    CHECK (delivery_mode IN ('inbox', 'stream_only'));
