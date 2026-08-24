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

## Idempotent append

The authenticated message-send request accepts an optional
`idempotency_key`. It is transport metadata and does not appear in the immutable
message envelope.

Keys must contain a non-whitespace character, must not exceed 256 bytes, and are
scoped to the stable authenticated agent ID across the node. The first use
returns `201 Created`. Retrying an identical
channel, recipient, kind, payload, correlation ID, and causation ID returns the
original message with `200 OK` and does not create another delivery or live
notification. Reusing the key for different content returns `409 Conflict`.

The scope survives credential rotation and daemon restart. Different agents
may use the same key independently. An exact replay remains valid after a
membership change because it creates no new effect; the first use always
requires current sender and recipient membership. See
[ADR 0006](adr/0006-idempotent-message-append.md).

A channel message with no recipient is visible to every member. A direct
message is visible only to its sender, its recipient, and an operator; HTTP
history and WebSocket streams enforce this against the authenticated
principal. A direct recipient must also be a member of the channel. Consumers
should retain unknown envelope fields when they proxy or persist messages.

Membership is permanent for a channel's lifetime: fleetd offers no member
removal, and an agent added later still replays full history from any cursor.
Credential rotation is the single mechanism that revokes an agent's access;
after rotation every request fails with 401, including inbox claims and
settlement.

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
- `POST .../{message_id}/block` records ambiguity evidence and parks the
  delivery for an explicit operator decision.

Settlement requires the active lease token. Retrying the same settlement after
a lost HTTP response is idempotent. An expired or superseded owner cannot settle
the delivery. After expiry, another worker can claim it again.

### Blocked delivery resolution

Blocking is for an execution whose external outcome is unknown, not an ordinary
transient failure. The agent-bound request supplies the active lease token and a
non-empty reason of at most 4,096 bytes. The first request returns the durable
block record with `201 Created`; an exact replay returns the same record with
`200 OK`. Reusing that lease with different evidence returns `409 Conflict`.

A blocked delivery is not claimable when its former lease expires. Only an
operator may list unresolved records with `GET /v1/delivery-blocks` (optionally
filtered by `?agent=...`) and resolve one with
`POST /v1/delivery-blocks/{block_id}/resolve`:

```json
{
  "resolution": "requeue",
  "retry_after_ms": 1000,
  "note": "verified that the side effect did not occur"
}
```

`requeue` makes the same delivery eligible after a bounded delay; its next
claim increments the existing attempt count. `abandon` moves it to a terminal,
unclaimable state and requires a zero retry delay. The resolution record is
durable. Repeating an identical decision is idempotent, while a different
second decision returns `409 Conflict`.

Delivery is at-least-once. The stable message ID is the idempotency key for
external effects; fleetd does not claim exactly-once execution across another
system's boundary. Blocking prevents a known ambiguity from being retried
automatically; it cannot prove whether the external effect happened.

## Authentication and attribution

Every `/v1` HTTP request and WebSocket upgrade requires the header
`Authorization: Bearer <token>`. Health checks remain public. Operator
credentials administer agents, credentials, channels, membership, and blocked
delivery resolution. Agent credentials send messages, access member channels,
and claim or settle only their own inbox.

The message-send body deliberately has no `sender_id` field. Unknown fields are
rejected, and the server writes the authenticated agent ID into the immutable
envelope. Operators cannot impersonate an agent to send or settle work.

Registering or rotating an agent returns its raw credential once. Losing it does
not change the agent identity: the operator rotates the credential, immediately
revoking previous tokens. Authentication failures return `401` with a Bearer
challenge; valid credentials without the necessary authority return `403`.
