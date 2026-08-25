# ADR 0016: Peer messaging is an invocation-scoped grant

- Status: accepted
- Date: 2026-08-24

## Context

An agent turn must be able to send durable messages to peers without receiving
an agent bearer credential or learning Fleetd's internal HTTP API. A static MCP
token would outlive the turn, while direct plugin access to the database would
bypass attribution, membership, idempotency, and settlement boundaries.

## Decision

Worker desired state may request the named runtime grant
`fleet.messaging.send`. After the invocation and session-owner fence are
durably armed, the controller activates an ephemeral loopback MCP endpoint for
that exact invocation. It revokes the grant before result or block settlement.

The controller derives sender, channel, correlation, causation, and durable
idempotency from the invocation. The harness may choose only recipient, message
kind, opaque payload, and an operation ID used for exact retries. Self-send,
broadcast, cross-channel send, excessive payloads, and more than eight new
messages per invocation are rejected.

The endpoint token is random, exists only in memory, and is supplied through
the typed harness session setup. It is not a Fleetd bearer and conveys no other
authority. Revocation serializes behind an accepted append so settlement cannot
race a committed peer message.

## Consequences

- A harness can coordinate peers without ambient Fleetd authority.
- Accepted sends survive controller or harness crashes as immutable messages.
- Replies resume the caller through a later inbox delivery rather than a
  synchronous distributed call stack.
- The grant name is runtime policy, not a claim about semantic capabilities.
- Additional grants require their own narrow authority, validation, and
  qualification; the plugin lifecycle remains unchanged.
