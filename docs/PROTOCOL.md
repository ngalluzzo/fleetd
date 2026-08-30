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

Every channel member sees the same immutable message log through HTTP history
and WebSocket replay. `recipient_id` addresses a message and controls its inbox
delivery; it is not a visibility boundary. A recipient must be a member of the
channel. Communication that must be private belongs in a `direct`
conversation, whose membership is exactly two participants. Consumers should
retain unknown envelope fields when they proxy or persist messages. See
[ADR 0027](adr/0027-channel-visible-addressed-messages.md).

Membership is permanent for a channel's lifetime: fleetd offers no member
removal, and an agent added later still replays full history from any cursor.
Credential rotation is the single mechanism that revokes an agent's access;
after rotation every request fails with 401, including inbox claims and
settlement.

## Conversation lifecycle

The durable channel substrate has two lifecycle kinds. A `shared` channel has
an operator-selected name and permanent membership that may grow. A `direct`
conversation has exactly two distinct participants and is identified by that
unordered pair. Opening the same pair again is idempotent and concurrency-safe;
it returns the existing conversation. The two initial delivery modes are
immutable, just like every other membership delivery mode.

Shared channels may be renamed and archived. Archive is an idempotent, one-way
transition: membership and immutable history remain readable, while new
messages, renames, and member additions conflict. Direct conversations cannot
be renamed, archived, or extended with another member.

`GET /v1/conversations` returns a common bounded projection for both kinds,
including exact member identities and delivery modes plus the latest message
sequence and timestamp. Archived shared channels are omitted by default and
may be included explicitly. These lifecycle and discovery operations require
the operator principal; message history and streaming retain their existing
channel-wide membership rules.

Each membership has one immutable delivery mode. `inbox` preserves the leased
work guarantee: addressed and broadcast append snapshot delivery rows under
the existing rules. `stream_only` remains fully addressable and retains
identical history and live-stream visibility, but message append creates no
leased inbox row for that membership. Existing memberships, omitted add-member
modes, and the `CreateChannel.member_ids` shorthand all use `inbox`.

Each membership also owns one durable, monotonic `read_through_seq`. It starts
at zero and means that the participant has observed every channel message at or
below that global sequence. The participant advances it with
`PUT /v1/channels/{channel_id}/read-cursor`; the request must name zero or a
sequence already committed in that channel. A stale replay returns the current
higher cursor rather than moving it backwards, and a future cursor is rejected.

`GET /v1/conversations/attention` is agent-owned and returns one exact
projection per membership: the cursor, latest channel sequence, unread count,
first unread sequence, and the corresponding count and first sequence for
messages sent by another participant whose `recipient_id` is that participant.
It does not count the participant's own messages, inspect message content, or
infer urgency. Operator authority cannot read or mutate another participant's
attention state.

`CreateChannel.members` accepts exact agent and delivery-mode pairs in the same
atomic creation transaction. Duplicate agents across either initial input are
rejected. Re-adding an exact membership is idempotent; a different mode
conflicts because mode transitions have no protocol. Operators and members of
the exact channel may list the bounded membership projection at
`GET /v1/channels/{channel_id}/members`; it omits agent metadata. See
[the stable membership contract](contracts/channel-membership-delivery-v1.md).

HTTP history uses an exclusive `after` cursor. WebSocket streams first replay
every durable message after the cursor and then continue with live messages.
Clients may reconnect with the highest sequence they durably processed.

Daemon-owned HTTP appends and separately processed local worker commits both
wake the same durable replay center. Cross-process wakeups are content-free,
best-effort hints; they never replace the SQLite cursor or envelope as
authority. A lost wake is recovered on reconnect, while a duplicate wake emits
no duplicate message because replay advances only by sequence. See
[ADR 0024](adr/0024-cross-process-message-commit-hints.md).

## Agent inbox delivery

Appending an addressed message creates one delivery for its recipient.
Appending a broadcast snapshots all current channel members except the sender.
Membership changes never create or remove deliveries for an existing message.

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

### Operator delivery projection

`GET /v1/deliveries` returns a bounded read-only projection of `pending`,
`leased`, `blocked`, `acknowledged`, and `dead` delivery rows. Optional exact
agent and state filters do not mutate recovery state. The projection includes
availability, attempt, expiry, failure, acknowledgement, and unresolved block
identity but deliberately omits active and settled lease tokens. An expired
leased row remains honest persisted evidence until a later claim or managed
reservation performs recovery.

### Managed invocation fence

Effectful harness controllers use
`POST /v1/agents/{agent_id}/invocations/reserve` instead of a raw claim. The
request has the same bounded `limit` and `lease_duration_ms`, but fleetd creates
one invocation row per leased delivery in the same immediate SQLite
transaction. Each response contains the immutable input message, delivery
attempt, lease and expiry, invocation ID, and a separate fence token.

The invocation begins as `reserved`, which means fleetd has not authorized the
controller to send an effectful request. Immediately before that send, the
controller calls
`POST /v1/agents/{agent_id}/invocations/{invocation_id}/arm` with both tokens.
The call changes the state to `dispatch_armed` and must commit before the
effect leaves the controller. An identical arm replay is idempotent while the
lease is live. A stale lease or mismatched fence returns `409 Conflict`.

Every inbox claim runs managed recovery first:

- An expired `reserved` invocation becomes terminal with certainty
  `not_started`; its delivery may be leased as a new attempt.
- An expired `dispatch_armed` invocation becomes terminal with certainty
  `outcome_unknown`; its delivery and recovery evidence are atomically moved to
  the blocked queue.

This is a write-ahead safety fence. The crash between the durable arm and the
actual send may conservatively block work that never started, but no crash can
make an armed attempt automatically execute again. A raw inbox claim creates no
invocation record and therefore retains ordinary at-least-once semantics.

Delivery settlement closes the matching invocation in the same transaction.
Acknowledgement records `outcome_known`; retry before arming records
`not_started`; block records `outcome_unknown`. Ordinary retry is rejected once
dispatch is armed. Operators may inspect the latest records with
`GET /v1/invocations`, optionally filtered by `?agent=...`. See
[ADR 0008](adr/0008-write-ahead-invocation-fence.md).

`GET /v1/invocations/{invocation_id}/trace` reads one exact invocation and
joins any corresponding bounded observation, plugin generation, native-session
binding, and immutable result message. Missing execution-side evidence is
represented as `null` for a reservation that has not been armed; the endpoint
does not synthesize events or duplicate a harness transcript.

`GET /v1/fleet-health` composes those projections into one operator read: the
current plugin generation for each agent, the current generation of each
session binding, the invocations still owed an outcome, and a census of
delivery rows by state including leases whose window has closed. "Current"
means the newest row for each key. The census reports how many rows it
inspected, so `delivery_limit` is visible as a bound rather than as a quiet
fleet. The daemon composes the report, so every surface reads the same
answer.

A known successful turn uses
`POST /v1/agents/{agent_id}/invocations/{invocation_id}/complete`:

```json
{
  "lease_token": "delivery-lease",
  "fence_token": "invocation-fence",
  "kind": "work.result/v1",
  "payload": { "status": "done" }
}
```

Completion requires a live `dispatch_armed` invocation. In one immediate
transaction it appends a deterministic idempotent result, snapshots its
delivery, acknowledges the input, and terminalizes the invocation as
`outcome_known`. The result is sent directly to the input sender in the same
channel, preserves the input correlation ID, and sets causation to the input
message ID. The first completion returns `201 Created`; an identical replay,
including after lease expiry or restart, returns the original invocation and
message with `200 OK`. Changed result content returns `409 Conflict` and replay
does not emit another live notification.

A worker may reconcile the recipient's durable inbox while a harness turn is
active. Under the default conversational policy, a newer claimable accepted
message committed in the same channel after that turn began requests fenced
harness cancellation. This does not lock the channel or mutate either source
message. Known quiescent terminal evidence completes the older invocation with
an ordinary causally linked result whose payload reports `interrupted` and
identifies the newer message; the newer delivery remains pending until it is
reserved normally. The worker retires the quiescent native session generation
before reserving that delivery and opens a fresh generation from durable
channel history; a harness's private state is not assumed reusable after
cancellation. Unknown or non-quiescent cancellation follows the existing
blocked-delivery path. This policy belongs to the worker adapter and is not a
seventh kernel concept.

Delivery is at-least-once. The stable message ID is the idempotency key for
external effects; fleetd does not claim exactly-once execution across another
system's boundary. Blocking prevents a known ambiguity from being retried
automatically; it cannot prove whether the external effect happened.

## Authentication and attribution

Every `/v1` HTTP operation and the native channel WebSocket require the header
`Authorization: Bearer <token>`. The dedicated browser channel WebSocket is the
only versioned upgrade outside that middleware: it validates the configured
loopback origin and redeems a bearer-authenticated, single-use stream grant
before releasing application data. Health checks remain public. Operator
credentials administer agents, credentials, channels, membership, and blocked
delivery resolution. Agent credentials send messages, access member channels,
claim or settle only their own inbox, and reserve, arm, or complete only their
own invocations. Invocation inspection is operator-only.

The message-send body deliberately has no `sender_id` field. Unknown fields are
rejected, and the server writes the authenticated agent ID into the immutable
envelope. Operators cannot impersonate an agent to send or settle work.

Registering or rotating an agent returns its raw credential once. Losing it does
not change the agent identity: the operator rotates the credential, immediately
revoking previous tokens. Authentication failures return `401` with a Bearer
challenge; valid credentials without the necessary authority return `403`.

The browser authentication and tagged-frame edge is fixed by the stable
[`browser-channel-stream-v1.md`](contracts/browser-channel-stream-v1.md)
contract. The native bearer-authenticated stream and browser grant-authenticated
stream share the same channel-authorized replay/live engine.
