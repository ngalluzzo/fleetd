# Architecture

## Kernel boundary

The kernel owns five concepts:

- **Agent:** an addressable participant with opaque metadata.
- **Channel:** a durable, bounded conversation.
- **Membership:** permission to send or receive within a channel.
- **Message:** an immutable envelope in a globally ordered sequence.
- **Delivery:** a recipient snapshot and its durable processing state.

The kernel does not know what Codex, DSH, a task, a pull request, or a model is.
Those concepts are expressed by adapters and versioned message contracts.

## Data path

An HTTP write is validated against channel membership and committed to SQLite.
Only after the transaction commits is the message offered to the in-memory
broadcast bus. WebSocket consumers subscribe before replaying the durable log,
deduplicate by sequence number, and recover broadcast lag from SQLite. This
makes the database authoritative while keeping delivery responsive.

Messages intended for agent processing create delivery rows in the same
transaction. Workers claim those rows with bounded leases and explicitly
acknowledge or release them. WebSockets remain notification hints; the leased
inbox is the work guarantee. See
[ADR 0002](adr/0002-at-least-once-agent-inbox.md) for failure semantics.

## Deliberate constraints

- One trusted local node.
- SQLite is the only source of truth.
- Schema changes are forward-only, checksummed migrations.
- Messages are never edited or deleted.
- Unknown message kinds and payload fields remain opaque JSON.
- Harness execution and workflow policy live outside the kernel.
- Git remains Git; fleetd will coordinate adapters instead of hosting it.

## Next boundary

The next layer authenticates an inbox adapter as one agent. It leases addressed
messages, invokes a harness, posts correlated responses, and settles the lease.
Harness invocation and session resumption remain outside the messaging kernel.
