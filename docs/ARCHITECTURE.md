# Architecture

## Topology

The tree is the architecture. Where a boundary is a crate, Cargo enforces it;
where it is a module, only `tests/crate_boundaries.rs` does, by reading source
as text. The difference is worth reading off this list:

    crates/proto          wire types crossing any boundary          (crate)
    crates/kernel         the substrate, migrations, the only pool  (crate)
    crates/conversation   a read model over the substrate           (crate)
    crates/plugin-host    child-process lifecycle and ACP client    (crate)
    src/execution         what happens to durable state             (module)
    src/http              how it is exposed                         (module)
    src/{main,cli}.rs     the binary

A module boundary is a convention a test checks after the fact. A crate boundary
is a compile error: `fleetd-kernel` does not depend on `fleetd-conversation`, so
the substrate cannot name its own projection, and `fleetd-conversation` does not
depend on a web framework, so it cannot render itself. Neither needs asserting.
Moving `execution` and `http` to crates would retire five of those text checks;
until then the list above says which boundaries are real.

Dependencies run downward only. `execution` composes over the kernel and never
extends it; `http` composes over `execution`; neither reaches back. Nothing
sits at the root of `src/` except the library root, the binary, and its command
line.

`tests/crate_boundaries.rs` checks that listing against what is on disk, so a
module cannot be added without saying which layer owns it, and a layer cannot
quietly start depending on the one above it.

## Kernel boundary

The kernel is the substrate. A *conversation* is not one of its concepts: it is
`crates/conversation`, a read model that presents a channel, its membership, and
how recently anything was said. Opening a direct pair is a substrate write and
stays here; presenting the result is the projection's job, and a caller that
wants both composes them. That is why `message.kind` is unconstrained and why
`work.request` and `text` are the same kind of thing to the kernel.

The kernel owns six concepts:

- **Agent:** an addressable participant with opaque metadata.
- **Channel:** a durable, bounded conversation.
- **Membership:** permission to send or receive within a channel.
- **Message:** an immutable envelope in a globally ordered sequence.
- **Delivery:** a recipient snapshot and its durable processing state.
- **Principal:** an operator or one authenticated agent identity.

The kernel does not know what Codex, OpenCode, DSH, a task, a pull request, a
workflow, or a semantic capability is. Adapters select exact message kinds but
the kernel preserves every kind and payload opaquely.

`fleetd-kernel` is that boundary as a crate. It owns the authoritative SQLite
store, its migrations, and every transition its own rows can make, and it is
the only crate holding a connection pool. It depends on storage and
serialization crates alone: no web framework, no HTTP client, no plugin
process. Layers above enlist their work in a kernel transaction through
`Store::begin_immediate`, so state the kernel owns and state they own commit
together without either reaching into the other.

## Data path

An HTTP write is validated against channel membership and committed to SQLite.
Only after commit is the message offered to the in-memory broadcast bus.
WebSocket consumers subscribe before replaying the durable log, deduplicate by
sequence number, and recover lag from SQLite.

A local continuous worker commits results from a separate process against the
same SQLite authority. After a newly created message commits, that writer emits
one content-free best-effort Unix datagram. The daemon converts it to a stream
wake and every affected consumer reconciles its principal-relative cursor from
SQLite. The hint carries no message or authority, and loss or duplication is
safe because reconnect replay remains the recovery path. See
[ADR 0024](adr/0024-cross-process-message-commit-hints.md).

Addressed messages create delivery rows in the same transaction. Workers claim
those rows with bounded leases and explicitly acknowledge, release, or park
them. WebSockets are notification hints; the leased inbox is the work
guarantee. See [ADR 0002](adr/0002-at-least-once-agent-inbox.md).

The kernel owns every transition a delivery row can make and commits none of
them. Settlement composes one kernel transition with the invocation fence
terminalizing in the same transaction, because a delivery acknowledged without
its invocation terminalized is exactly the crash the fence exists to prevent.
That composition is layered above the kernel, so the kernel depends on nothing
above it. See [ADR 0026](adr/0026-delivery-settlement-composition.md).

An active worker parks a delivery when an external outcome is ambiguous.
Parked work does not become claimable when a lease expires; only the operator
can requeue or abandon the exact block record. See
[ADR 0007](adr/0007-durable-blocked-deliveries.md).

## Invocation fence

Managed controllers reserve a delivery and write-ahead invocation record in
one transaction. A second durable transition arms external dispatch. Recovery
can therefore distinguish a provably unstarted reservation from an armed
attempt whose outcome is unknown.

Known completion appends the correlated idempotent result, snapshots its
recipients, acknowledges the input, and terminalizes the invocation in one
commit. Agent-scoped idempotency keys make publication safe to retry after a
lost response without claiming that harness effects happen exactly once. See
[ADR 0006](adr/0006-idempotent-message-append.md) and
[ADR 0008](adr/0008-write-ahead-invocation-fence.md).

## Wire type boundary

`fleetd-proto` owns every type that crosses a boundary: the HTTP contract's
request and response bodies, the plugin lifecycle manifest, the ACP harness
frames, and the operator read models for generations, observations, and session
bindings. It carries serialization and schema dependencies only.

A harness vendor, an external tool, or a generated client compiles that crate
instead of the daemon. Nothing in it opens a socket, a database, or a runtime,
so implementing an adapter contract no longer means building SQLite and an HTTP
server. Persistence, negotiation, and transport stay with whichever crate owns
the behavior; only the frame descriptions move.

The boundary is enforced by `tests/crate_boundaries.rs`, which pins the crate's
allowed dependencies, rejects a normal dependency on the daemon from any
workspace member, and rejects persistence or transport references in proto's
own sources.

## Plugin boundary

Harness and external-system integrations run in separately versioned child
processes rather than inside the daemon or as Rust dynamic libraries. The
lifecycle transport launches an absolute executable without a shell, clears
its environment, bounds frames and deadlines, validates plugin identity and
exact operational interfaces, and terminates the complete process group on
failure or shutdown overrun.

An operational interface identifies a wire contract spoken by the plugin. It
does not claim what semantic work an agent, model, or tool composition can do.
The lifecycle protocol exposes initialize, readiness, notifications, and
shutdown; typed interface clients own all other methods. There is no generic
execute tunnel.

Plugins receive only explicit opaque configuration and no Fleetd bearer
credentials. The boundary isolates crashes and language/toolchain choices; it
is not an operating-system security sandbox. See
[ADR 0004](adr/0004-out-of-process-plugins.md) and the
[lifecycle contract](contracts/plugin-lifecycle-v1.md).

## Harness boundary

ACP is an inner harness interoperability protocol. Fleetd's current harness
plugins negotiate the operational interface `fleetd.harness-acp@0.1.0`, whose
typed methods cover description, session open/resume, fenced turn start,
permission resolution, cancellation, ordered events, terminal evidence, and
close.

The shared ACP host owns protocol translation and process containment. Each
vendor plugin owns launch arguments, environment grants, model routing, and
profile identity. The worker owns each plugin and native runtime as one process
group. No raw JSON-RPC call surface escapes the typed client. See
[ADR 0005](adr/0005-acp-harness-boundary.md),
[ADR 0009](adr/0009-typed-acp-driver-and-process-ownership.md), and
[ADR 0011](adr/0011-vendor-owned-harness-plugins.md).

## Sessions and continuous worker

The worker is a trusted local process outside the daemon's messaging kernel.
It composes delivery reservation, native session acquisition, owner-epoch
fencing, dispatch arming, prompt drain, settlement, process restart, and
conservative ambiguity parking.

Session bindings are keyed by agent, channel, and working-directory identity.
Compatible restarts adopt a binding under a higher owner epoch. Incompatible
profiles rotate the binding generation. Active or uncertain bindings are not
silently reused. See [ADR 0010](adr/0010-durable-session-bindings-and-owner-epochs.md)
and [the worker guide](WORKER.md).

Each ready plugin process has one durable generation record with exact
negotiated identity, profile, heartbeat, and shutdown evidence. Each armed
turn has one fixed-size observation record. Ordered harness updates fold into
typed counters, byte totals, and a cryptographic chain digest; their raw
contents remain in the harness-owned transcript and the bounded Fleetd result.
See [ADR 0020](adr/0020-bounded-operational-observations.md).

Invocation observations include their exact source message and optional result
message identities. External qualification tools can therefore correlate
bounded control evidence through immutable causation rather than timing
windows.

## Qualification boundary

Exact workload injection and backend telemetry collection are standalone
operator concerns. `fleetd-soak` consumes only public Fleetd APIs, preserves
message payloads and loopback observer documents opaquely, and writes a
portable evidence artifact. It does not run inside the daemon, assign common
meaning to provider metrics, or validate application payload contracts. See
[ADR 0021](adr/0021-external-soak-evidence.md).

The envelope adapter provides the complete immutable Fleetd message to the
harness. It neither recognizes product contracts nor parses domain results.
Its exact inbound-kind allowlist is routing policy only.

## Invocation-scoped message grant

An armed turn may receive the named runtime grant `fleet.messaging.send`.
Fleetd resolves it to an ephemeral controller-owned MCP endpoint and supplies
only that endpoint to the harness. The controller derives sender, channel,
correlation, causation, and idempotency from the armed invocation.

The grant is activated after dispatch arming and revoked before settlement.
Its token is not a Fleetd bearer, cannot select another sender or channel, and
expires with the invocation. See
[ADR 0016](adr/0016-invocation-scoped-message-grant.md).

## External semantic integration

Fleetd has no semantic compiler dependency and no special semantic document
paths. An external integration may:

1. lift Fleetd's public API, plugin observations, or generated artifacts into
   its own native facts;
2. bridge those facts to semantic claims only with explicit evidence and
   qualification;
3. lower an already-linked deployment to Fleetd's public API or worker config.

That lift/bridge/lower package is independently versioned and outside Fleetd.
Fleetd transports any documents it emits as ordinary opaque messages. See
[the integration boundary](INTEGRATION_BOUNDARY.md).

## HTTP surface

One module owns composition: shared state, the route graph, the generated
document, and the authentication layer every protected route sits behind. One
module per domain owns its handlers and declares its own routes, and domains
reach composition and the shared authorization guards but never each other.

The merge order in the composition module fixes the order operations register
in, and therefore their order in the generated contract.

`tests/crate_boundaries.rs` rejects a cross-domain import and a domain that is
declared but never merged into a contract, which would leave its routes
unreachable while still compiling.

## Operator surface

The first browser surface is a static adapter over one checked-in target
contract. It uses only public authenticated endpoints and exposes blocked-work
resolution. The contract can be generated externally, but the served artifact
contains no compiler runtime dependency. See
[ADR 0014](adr/0014-generated-operator-surface.md).

The live-conversation design keeps a human operator's administrative
principal separate from their addressable participant principal. Native and
TUI clients can use the existing bearer-authenticated channel WebSocket. A
browser target now has an implemented attenuated, single-use stream-grant edge
without credentials in a URL, cookie, or subprotocol. The adversarial and
actual-client real-browser qualification matrices pass. See the
[live-conversation design](LIVE_CONVERSATION.md),
[ADR 0022](adr/0022-browser-channel-stream-grants.md), and the
[stable browser stream contract](contracts/browser-channel-stream-v1.md).

Addressable membership does not imply a leased work inbox. The stable
membership delivery mode keeps autonomous `inbox` seats distinct from
`stream_only` human or passive clients while preserving the same durable log
and visibility. Fleetd stores that operational choice on exact channel
membership rather than interpreting participant metadata. See
[ADR 0023](adr/0023-membership-delivery-mode.md) and the
[membership contract](contracts/channel-membership-delivery-v1.md).

Conversation messages and execution telemetry remain distinct. Immutable
channel messages already have durable replay plus live continuation. Mutable
operator observations have durable latest-state read models but no live event
subscription yet; browser or GUI work must not disguise polling as that missing
capability or emit synthetic activity into the channel log.

The presentation-free Phase C qualification composes a human `stream_only`
participant, the real WebKit browser client, a continuous OpenCode worker, and
the local Qwen route across browser, daemon, worker, and harness replacement.
It preserves the native session and causal durable history without adding a
chat subsystem. See the
[qualification record](qualification/live-human-agent-conversation-2026-08-25.md).

Operator-only API read models expose plugin generations, session bindings, and
invocation observations. They are the common operational source for browser,
TUI, or external projections; Fleetd does not give one presentation target a
privileged internal data path.

Presentation targets share a headless conversation session above replaceable
authority-specific transports. The browser transport composes operator
discovery with participant-owned stream and send operations without merging
their principals. GUI and future TUI hosts remain packaging adapters over that
session rather than new daemon concepts. See
[ADR 0025](adr/0025-headless-conversation-client.md).

## Deliberate constraints

- One trusted local node.
- SQLite is the only authoritative control store.
- Schema changes are forward-only and checksummed.
- Messages are immutable and unknown payload data is preserved.
- Harness and workflow semantics remain outside the kernel.
- Git remains Git; Fleetd coordinates agents instead of hosting a forge.
- Remote workers wait for encrypted transport and enrollment.
