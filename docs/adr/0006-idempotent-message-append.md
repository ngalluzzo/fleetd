# ADR 0006: Message append supports agent-scoped idempotency

- Status: accepted
- Date: 2026-08-24

## Context

An agent worker must publish a durable result before acknowledging its input
delivery. If the result commits but the HTTP response or worker is lost before
acknowledgement, the delivery is reclaimed. A second non-idempotent append
would publish a duplicate result.

Exactly-once execution across a harness or external tool is impossible in
general, but fleetd can make its own immutable message append safely retryable.

## Decision

Authenticated message-send requests accept an optional `idempotency_key` that
contains a non-whitespace character and no more than 256 bytes. The key is
scoped to the stable authenticated agent ID across the entire fleetd node, not
to a credential, channel, or process.

The first use commits the message and its recipient-delivery snapshot in one
transaction and returns `201 Created`. An identical retry returns the original
message, including its ID, sequence, timestamp, and delivery snapshot, with
`200 OK`. The replay does not publish another live-stream notification.

Identity compares channel, sender, recipient, kind, parsed JSON payload,
correlation ID, and causation ID. Reusing a key with any different value returns
`409 Conflict`. A different agent may independently use the same key.

Idempotency records are stored on the immutable message and protected by a
unique `(sender_id, idempotency_key)` index. Message append uses an immediate
SQLite write transaction so concurrent first uses serialize before the lookup
and insert. Keys remain valid across credential rotation and daemon restart.

An exact replay returns the original message even if current membership has
changed, because it creates no new effect and the authenticated agent is the
original sender. A first use still requires current sender and recipient
membership.

## Consequences

A worker can deterministically use a key such as
`invocation/<invocation_id>/result`, retry publication after an ambiguous
response, and then acknowledge its delivery without duplicating the result.
Progress events can use a similar key containing their stable event sequence.

The idempotency key is transport metadata and is not added to the public
message envelope. Clients that omit it preserve the previous append-always
behavior. This decision protects only fleetd message publication; external
effects still require their own idempotency or honest unknown-outcome handling.
