# Architecture

## Kernel boundary

The kernel owns four concepts:

- **Agent:** an addressable participant with opaque metadata.
- **Channel:** a durable, bounded conversation.
- **Membership:** permission to send or receive within a channel.
- **Message:** an immutable envelope in a globally ordered sequence.

The kernel does not know what Codex, DSH, a task, a pull request, or a model is.
Those concepts are expressed by adapters and versioned message contracts.

## Data path

An HTTP write is validated against channel membership and committed to SQLite.
Only after the transaction commits is the message offered to the in-memory
broadcast bus. WebSocket consumers subscribe before replaying the durable log,
deduplicate by sequence number, and recover broadcast lag from SQLite. This
makes the database authoritative while keeping delivery responsive.

## Deliberate constraints

- One trusted local node.
- SQLite is the only source of truth.
- Messages are never edited or deleted.
- Unknown message kinds and payload fields remain opaque JSON.
- Harness execution and workflow policy live outside the kernel.
- Git remains Git; fleetd will coordinate adapters instead of hosting it.

## Next boundary

The next layer is an inbox adapter. It claims an agent identity locally,
subscribes to its channel messages, invokes a harness, and posts correlated
responses. Lease and resumption semantics will be introduced as a versioned
contract rather than baked into generic messaging.

