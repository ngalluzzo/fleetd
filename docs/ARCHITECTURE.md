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

An active worker can durably park a delivery when an external outcome is
ambiguous. Parked work never becomes claimable merely because its old lease
expires; only an operator can requeue or abandon the exact block record. This
keeps retry policy outside harness stop reasons while giving the future worker
controller a conservative kernel primitive. See
[ADR 0007](adr/0007-durable-blocked-deliveries.md).

Managed controllers claim through an outer invocation module rather than
leasing and then recording intent in two writes. Reservation atomically creates
the lease and invocation fence. A second write-ahead transition arms dispatch;
the controller may perform an external effect only after it commits. Recovery
can therefore distinguish a provably unstarted reservation from an armed
attempt whose outcome is unknown. All claim paths apply that recovery before
selecting work. Known success crosses the other crash boundary with one atomic
completion: append the correlated idempotent result, snapshot its recipients,
acknowledge the input, and terminalize the invocation. See
[ADR 0008](adr/0008-write-ahead-invocation-fence.md).

Agent-scoped idempotency keys make message publication safely retryable after a
lost response. The original message and delivery snapshot are returned for an
identical replay; conflicting key reuse fails closed. This lets a future worker
commit one correlated result before settling its input without claiming that
external harness effects are exactly once. See
[ADR 0006](adr/0006-idempotent-message-append.md).

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

## Harness boundary

The experimental harness layer has a policy-free typed ACP host library and
separately identified harness plugins. ACP remains the inner harness protocol;
a narrow fleetd capability adds invocation identity, session fencing,
deadlines, and evidence without exposing a generic protocol tunnel. OpenCode,
Codex, DSH, and future integrations own their launch configuration and
environment grants in their own plugin packages. The supervisor owns each
plugin and its ACP runtime as one process group.

A continuous local worker composes atomic reservation with durable session
binding, exclusive owner epochs, write-ahead dispatch arming, typed prompt
drain, conservative ambiguity parking, atomic correlated-result completion,
inbox polling, and supervised process restart. One process owns one serialized
seat and caches one native session per channel lane. A fresh process generation
adopts a compatible ready session under a higher epoch and performs the native
resume before it handles more work. Binding activation commits with invocation
arming, and known quiescent completion returns the binding to ready in the same
commit as result publication.

The worker opens SQLite directly as a trusted local controller. It is neither
part of the public HTTP API nor embedded in the messaging kernel. Persisted
invocation-event fragments and explicit runtime-generation evidence remain the
next controller boundary. Codex has passed one real end-to-end turn; DSH has
passed initialization but still requires an approved credential path for
session and turn qualification. Invocation, resumption, retry policy, and
restart policy remain outside the messaging kernel. See the
[harness execution architecture](HARNESS_EXECUTION.md),
[worker operations guide](WORKER.md),
[ADR 0005](adr/0005-acp-harness-boundary.md),
[ADR 0011](adr/0011-vendor-owned-harness-plugins.md),
[ADR 0010](adr/0010-durable-session-bindings-and-owner-epochs.md), and the
[OpenCode plugin qualification](qualification/opencode-plugin-2026-08-24.md).

## Capability work boundary

A GOOIR capability need becomes executable only after it is bound to exact
input fact instances. fleetd carries that provider-neutral request as
`work.capability.request/v1`; the messaging kernel preserves it as opaque JSON.
The authenticated sender supplies request authority, the explicit recipient is
the first assignment policy, the invocation binds the exact message, and the
session turn durably records binding generation and owner epoch.

A capability-work adapter admits an exact configured capability set and uses a
session lane keyed by the request's RFC 8785/SHA-256 identity. Harness ACP is
only the current execution protocol. The first response is an attempt record,
not accepted output or conformance proof. See
[ADR 0012](adr/0012-capability-needs-become-durable-work.md) and the
[capability work v1 contract](contracts/capability-work-v1.md).

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
received, while operators see everything. Channel membership never shrinks
within a channel's lifetime, so authorization is evaluated when each request
or stream upgrade arrives; rotating an agent credential is the single
mechanism that revokes an agent across all of its channels. See
[ADR 0003](adr/0003-agent-bound-local-credentials.md).
