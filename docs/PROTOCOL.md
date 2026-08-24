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

## Agent inbox delivery

Appending a direct message creates one delivery for its recipient. Appending a
broadcast snapshots all current channel members except the sender. Membership
changes never create or remove deliveries for an existing message.

`POST /v1/agents/{agent_id}/deliveries/claim` accepts a bounded batch size and
lease duration. It atomically returns the oldest eligible deliveries, one lease
token for the batch, an expiry time, and a monotonically increasing attempt
count per delivery.

The worker settles each delivery by message ID:

- `POST .../{message_id}/ack` records successful processing.
- `POST .../{message_id}/retry` records failure evidence and makes the delivery
  eligible after a bounded delay.

Settlement requires the active lease token. Retrying the same settlement after
a lost HTTP response is idempotent. An expired or superseded owner cannot settle
the delivery. After expiry, another worker can claim it again.

Delivery is at-least-once. The stable message ID is the idempotency key for
external effects; fleetd does not claim exactly-once execution across another
system's boundary.
