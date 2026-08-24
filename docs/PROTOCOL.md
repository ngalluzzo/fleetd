# Message protocol

All messages share one immutable envelope:

```json
{
  "seq": 42,
  "id": "uuid",
  "channel_id": "uuid",
  "sender_id": "uuid",
  "recipient_id": "uuid-or-null",
  "kind": "text",
  "payload": { "text": "review this commit" },
  "correlation_id": "optional-workflow-id",
  "causation_id": "optional-message-id",
  "created_at_ms": 1787533200000
}
```

`seq` is the replay cursor and ordering authority for a single fleetd node.
`id` is the stable event identity. `kind` and `payload` form an open sum: the
kernel transports unknown contracts without interpreting or rewriting them.

A channel message with no recipient is visible to every member. A direct
recipient must also be a member of the channel. Consumers should retain unknown
envelope fields when they proxy or persist messages.

HTTP history uses an exclusive `after` cursor. WebSocket streams first replay
every durable message after the cursor and then continue with live messages.
Clients may reconnect with the highest sequence they durably processed.

