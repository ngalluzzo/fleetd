# Architecture

## Kernel boundary

The kernel owns six concepts:

- **Agent:** an addressable participant with opaque metadata.
- **Channel:** a durable, bounded conversation.
- **Membership:** permission to send or receive within a channel.
- **Message:** an immutable envelope in a globally ordered sequence.
- **Delivery:** a recipient snapshot and its durable processing state.
- **Principal:** an operator or one authenticated agent identity.

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

## Plugin boundary

Domain-specific code runs in separately versioned child processes rather than
inside the kernel or as Rust dynamic libraries. A strict lifecycle transport
launches an absolute executable without a shell, clears its environment, bounds
JSON-RPC frames and request deadlines, validates its identity and exact
capabilities, and terminates it on failed startup or shutdown overrun.

The boundary isolates crashes and language/toolchain choices; it is not an
operating-system security sandbox. Plugins receive only explicit opaque
configuration and no fleetd bearer credentials. Capability adapters will
mediate durable inbox work without granting plugins ambient access to kernel
storage or authority. See
[ADR 0004](adr/0004-out-of-process-capability-plugins.md) and the
[lifecycle v1 contract](contracts/plugin-lifecycle-v1.md).

## Next boundary

The next layer implements one generic ACP driver plugin and qualifies it
against Codex and DSH. ACP remains the inner harness protocol; a narrow fleetd
capability adds durable invocation identity, session fencing, deadlines, and
evidence without exposing a generic protocol tunnel. An authenticated worker
controller leases addressed messages, invokes that capability, posts
idempotent correlated responses, and settles the lease. Invocation, resumption,
retry policy, and restart policy remain outside the messaging kernel. See the
[harness execution architecture](HARNESS_EXECUTION.md) and
[ADR 0005](adr/0005-acp-harness-boundary.md).

## Identity boundary

The local operator token file is authoritative for node administration and is
readable only by its operating-system user. Its digest is reconciled
transactionally at startup, and revocation is permanent: a file holding a
revoked digest fails startup rather than reviving the credential. Agent
credentials are independently rotatable and bound to one stable agent ID.
SQLite stores only SHA-256 digests of 256-bit random bearer tokens.

Authentication is read-only on the request hot path. The API derives message
attribution from the principal, restricts inbox settlement to that agent, and
scopes channel reads: members see broadcasts plus direct messages they sent or
received, while operators see everything. See
[ADR 0003](adr/0003-agent-bound-local-credentials.md).
