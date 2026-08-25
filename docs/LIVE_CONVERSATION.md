# Live conversation design

Fleetd already owns the authoritative records needed for a person and software
agents to share a durable conversation. This document defines the product and
authority boundary for exposing those records interactively. It does not add a
chat subsystem to the messaging kernel or make harness transcripts a second
conversation log.

## Product invariant

A person participating in a channel is an addressable Fleetd participant with
an ordinary agent identity and agent-bound credential. The operator principal
continues to administer and observe the fleet but does not impersonate that
participant when sending messages.

One client may therefore compose two explicit authorities:

- an operator credential for fleet-wide discovery and observation; and
- a human-controlled agent credential for attributed message sends and member
  visibility.

They remain separate principals on every request and stream. A client must not
merge their permissions into a synthetic principal or fall back to sending
with operator authority.

## Conversation truth

The immutable Fleetd message log is the only conversation authority. A live
conversation client needs four operational primitives:

1. discover channels, participants, and membership;
2. read durable message history from an exclusive sequence cursor;
3. subscribe to ordered messages after that cursor; and
4. append an idempotent message as the authenticated human participant.

The existing message append, history, and authenticated WebSocket operations
already provide the last three primitives. A public channel-membership read
model remains necessary so a client can address participants without inferring
membership from history or failed sends.

Membership must also distinguish autonomous inbox consumers from passive
stream participants. Human-controlled participants use `stream_only`: they are
addressable, can send, and receive durable history and live replay without
accumulating leased work. Worker seats use `inbox`. The kernel treats this as
an immutable operational membership property and never infers it from agent
metadata. See [ADR 0023](adr/0023-membership-delivery-mode.md) and the
[draft membership contract](contracts/channel-membership-delivery-v1-draft.md).

Direct-message visibility remains principal-relative. An operator stream sees
every message in the selected channel. A participant stream sees broadcasts
plus direct messages that participant sent or received. A UI that offers both
views must label which principal owns the view; it must not union records from
two principals and present the result as one authority.

## Human-to-agent turn

A human turn uses the same durable path as an agent-to-agent turn:

1. The human participant appends an opaque addressed message.
2. The recipient's adapter-owned inbound contract decides whether its worker
   may reserve that message kind.
3. The worker resumes the recipient's channel-scoped native session and runs
   the managed invocation.
4. Known completion appends one immutable result addressed to the human,
   preserving correlation and setting causation to the human message.
5. The live stream carries the committed result like every other message.

Because the human's channel membership is `stream_only`, the result does not
create a leased inbox record. Offline delivery is still durable: reconnecting
history or live replay returns it from the authoritative message cursor. A UI
does not claim work or reinterpret rendering as acknowledgement.

Fleetd does not define a universal chat prompt or reply kind. The external
contract linking a particular UI, worker adapter, and agent instructions owns
their message kinds and payload meanings. Unknown kinds and fields remain
visible and preserved. Presentation adapters may render a qualified result
contract as prose but must retain access to its exact underlying envelope.

Native harness sessions remain scoped to the agent, channel, and working
directory. Follow-up messages in one channel therefore resume the same durable
conversation lane without making the channel log depend on a harness's private
transcript.

## Live transport requirements

A valid conversation subscription must provide:

- authentication before any application data is released;
- the same principal-relative visibility as HTTP history;
- ordered replay for every message with `seq > after`;
- live continuation without a replay/live race;
- recovery from in-memory broadcast lag through the durable log;
- reconnect with the highest accepted cursor;
- duplicate tolerance by stable `seq` and message ID; and
- bounded behavior for authentication, frames, backpressure, and shutdown.

Native applications and TUIs can already satisfy this contract by attaching
the Fleetd bearer credential to the WebSocket upgrade. The browser `WebSocket`
API cannot supply that header. Browser targets must use the separately designed
single-use stream-grant protocol in
[`browser-channel-stream-v1-draft.md`](contracts/browser-channel-stream-v1-draft.md).
Polling is not a substitute implementation of the live-subscription primitive.

## Execution telemetry is separate

Conversation messages and execution telemetry have different durability and
visibility rules. Fleetd currently exposes bounded operator-only snapshots for
plugin generations, session bindings, and invocation observations. It does not
yet expose a replayable live stream of those changing read models.

A conversation surface may show facts already established by messages, such as
"sent" and "replied." It must not infer "working" from elapsed time, emit
synthetic typing messages into the durable channel, or poll snapshots and call
that a live execution capability. Live invocation state requires a separately
versioned operator-event subscription with its own snapshot/replay race,
backpressure, and authorization design.

Partial assistant tokens and raw reasoning events are not conversation truth.
Fleetd keeps bounded operational evidence while the harness-owned transcript
and final result retain their existing authorities.

## Browser credential handling

The embedded browser surface may receive a long-lived bearer only through an
explicit user action. JavaScript holds it in memory only: never in a URL,
cookie, Web Storage, IndexedDB, generated artifact, or log. Static assets keep
the existing no-third-party-script content security policy and `no-store`
responses.

For viewing an operator-scoped channel and sending as a human participant, the
surface holds two separately labelled in-memory credentials. It mints a
single-use stream grant with the viewing credential and sends messages with the
human participant credential. A future native shell may source those
credentials from an operating-system secret store without changing either
principal or the Fleetd protocol.

## Semantic integration boundary

Fleetd publishes operational artifacts: agents, membership, messages, cursors,
stream protocols, and bounded observations. It does not import a UI dialect or
claim a semantic `LiveConversation` capability inside the daemon.

An external GOOIR integration may lift those artifacts into separately
versioned facts, link them against a conversation surface's requirements, and
lower a browser, native GUI, or TUI target. A browser lowering must remain
unsatisfied until the stream-grant protocol is implemented and qualified. The
lowered static artifacts may be served by Fleetd, but no GOOIR runtime or
semantic interpretation enters Fleetd.

## Readiness sequence

The conversation surface is ready only after these slices are independently
qualified:

1. channel membership discovery through the public API;
2. immutable `inbox` versus `stream_only` membership delivery;
3. browser stream-grant issuance and redemption;
4. replay/live parity between bearer-authenticated and grant-authenticated
   streams;
5. a human participant sending and receiving causal messages through a real
   continuous worker;
6. reconnect across browser, daemon, worker, and harness restarts; and
7. external semantic linking and at least two presentation targets consuming
   the same meaning.

Live execution telemetry is a later, separately qualified slice and is not a
pretext for weakening the conversation stream.
