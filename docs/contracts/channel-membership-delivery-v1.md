# Channel membership delivery v1

This contract adds an operational delivery mode to Fleetd membership. ADR 0023
is accepted and the qualification matrix below is covered by the storage, API,
and live-conversation membership suites.

## Wire types

```json
{
  "agent_id": "agent-uuid",
  "delivery_mode": "stream_only"
}
```

`delivery_mode` is optional on `AddMember` and defaults to `inbox`. It is a
closed enum:

- `inbox`
- `stream_only`

Unknown values and fields are rejected. Existing `CreateChannel.member_ids`
continue to create `inbox` memberships. `CreateChannel` also gains an additive
`members` array whose entries require both `agent_id` and `delivery_mode`:

```json
{
  "name": "gooir",
  "metadata": {},
  "member_ids": ["worker-agent-uuid"],
  "members": [
    {
      "agent_id": "human-agent-uuid",
      "delivery_mode": "stream_only"
    }
  ]
}
```

An agent may appear at most once across both arrays. All agents and modes are
validated before the channel and its complete initial membership commit in one
transaction.

The read model is:

```json
{
  "channel_id": "channel-uuid",
  "agent_id": "agent-uuid",
  "agent_name": "nic",
  "joined_at_ms": 1787666400000,
  "delivery_mode": "stream_only"
}
```

It deliberately omits agent metadata and credential state.

## Operations

`POST /v1/channels/{channel_id}/members` retains its current operator-only
authority. An exact replay of agent ID and mode returns `204 No Content`.
Re-adding the same agent with another mode returns `409 Conflict` and does not
change the existing membership.

`GET /v1/channels/{channel_id}/members` returns memberships ordered by
`joined_at_ms`, then agent ID. Operators may list any channel. An agent may list
only a channel of which it is already a member. Unknown channels return `404`;
non-members receive `403` without membership data.

## Storage and migration

A forward migration adds a non-null checked `delivery_mode` column to
`channel_members`, defaulting every existing row to `inbox`. Applied migrations
are never edited.

Membership creation stores the chosen mode in the same transaction that
validates the channel and agent. The mode is immutable. Its idempotency identity
is `(channel_id, agent_id, delivery_mode)` while the uniqueness constraint
remains `(channel_id, agent_id)`.

## Delivery snapshot

Message append keeps one transaction for the immutable message and delivery
snapshot.

For a direct message:

1. sender and recipient membership are validated;
2. the message is appended; and
3. a delivery row is inserted only if the recipient membership mode is
   `inbox`.

For a broadcast, delivery rows are inserted for every other current member
whose mode is `inbox`. `stream_only` members remain visible recipients through
the channel log but do not enter leased inbox state.

Idempotent append replay returns the original message and never recomputes its
delivery snapshot. Later memberships remain unable to change an existing
message's deliveries.

## Qualification matrix

The stable contract is qualified by tests that prove:

1. the migration maps every existing membership to `inbox` without changing
   messages or delivery rows;
2. omitted mode preserves current direct and broadcast delivery behavior;
3. mixed-mode initial membership commits atomically and duplicate agents across
   the legacy and exact inputs fail without creating a channel;
4. a direct message to `stream_only` commits and appears in history and live
   streams without a delivery row;
5. a broadcast reaches both modes through history and live streams while only
   `inbox` members receive delivery rows;
6. direct-message visibility remains limited to sender, recipient, and
   operator regardless of delivery mode;
7. exact member-add replay is idempotent and a mode mismatch conflicts;
8. agent and channel existence and membership checks fail before append;
9. the member list exposes only the bounded read model to an exact member and
   operator;
10. another channel member cannot use one channel's membership to inspect a
   different channel;
11. idempotent message replay never creates a delivery omitted by the original
    snapshot;
12. concurrent member addition and message append resolve through one exact
    committed membership snapshot; and
13. unknown message kinds and payload data remain unaffected by delivery mode.
