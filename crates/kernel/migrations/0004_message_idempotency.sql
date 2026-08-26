ALTER TABLE messages ADD COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX messages_sender_idempotency
    ON messages(sender_id, idempotency_key);
