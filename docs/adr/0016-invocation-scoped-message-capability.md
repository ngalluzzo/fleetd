# ADR 0016: Peer messaging is an invocation-scoped capability

- Status: accepted for experimental dogfood
- Date: 2026-08-24

## Context

Fleetd's kernel already owns immutable messages, channel membership, direct
delivery snapshots, and agent-scoped idempotency. A harness session still
could not publish a peer message without either receiving a Fleetd bearer or
teaching the harness plugin Fleetd-specific HTTP details. Both choices would
collapse semantic capability, protocol, and broad authority into one adapter.

MCP is a suitable protocol for exposing tools to ACP runtimes, but MCP itself
does not define the authority to send a Fleetd message. The capability must
remain usable through other protocols and harnesses later.

## Decision

Fleetd defines the first outbound semantic grant as
`fleet.messaging.send`. Its current operation is
`publish_durable_message`. MCP is only the first protocol projection.

The continuous worker resolves the name to a controller-owned Streamable HTTP
server on an explicit random `127.0.0.1` port. The server uses the official
Rust MCP SDK and a random ephemeral header token. The ACP host accepts only an
exact one-to-one set of requested and resolved grants and rejects non-loopback
HTTP endpoints. The token is narrow capability authority, not a Fleetd bearer,
and is neither operator-authored nor persisted.

Managed-turn capability activation is protocol-neutral. The controller
activates each capability only after the invocation and session-owner fence are
durably armed. It revokes capabilities before terminal settlement. Revocation
serializes behind an accepted append, so no authorized message remains racing
after the controller publishes a result or block.

The tool accepts only:

- a stable invocation-local operation ID;
- one exact peer recipient;
- an open message kind;
- a bounded opaque JSON payload.

Fleetd derives sender and channel from the active invocation. It carries the
source correlation or establishes it from the source message ID, fixes
causation to that source message, and derives the durable idempotency key from
the invocation and operation ID. The existing store transaction remains
authoritative for channel membership, exact replay, conflict detection,
message insert, and delivery snapshot creation.

The first grant is direct-message only, rejects self-send, permits at most
eight new messages per invocation, and caps encoded payloads at 64 KiB. A
retry of an already-admitted operation remains valid at the quota boundary.

## Consequences

- Harnesses receive no Fleetd bearer and cannot read inboxes, administer the
  fleet, choose sender/channel lineage, or access arbitrary SQLite operations.
- Plugins remain vendor-specific protocol adapters; they do not learn
  OpenCode-specific or Fleetd-message semantics beyond the typed ACP session
  projection.
- Message meaning stays in adapters and versioned contracts. The broker does
  not interpret review, approval, workflow, Git, UI, or GOOIR payloads.
- Exact retries survive ambiguous MCP response delivery without duplicating a
  committed message.
- A same-user malicious process remains outside the claimed sandbox boundary;
  random loopback authority mainly prevents accidental and browser-origin
  access.
- Peer-response waiting, inbox reads, correlation-hop policy, and richer
  capability discovery remain separate decisions.
