# ADR 0025: Headless conversation client and replaceable presentation hosts

- Status: accepted
- Date: 2026-08-25

## Context

Phase C proves that a human participant, the public browser stream, a
continuous worker, and a real harness can complete a durable conversation
across process replacement. The next product slice must make that path usable
without creating a second conversation authority, binding conversation
behavior to one GUI toolkit, or teaching Fleetd application message semantics.

The existing browser channel-stream adapter is a wire edge. It deliberately
does not discover channels, retain a presentation projection, choose a
recipient, encode a prompt, or render a result. Putting all of those choices
directly into one DOM application would make a later TUI repeat the same
coordination logic. Putting them in the daemon would expand the kernel into a
chat product.

Bun 1.4.0's built-in `Bun.WebView` is an authoritative headless WebKit runtime;
it does not implement a visible window. A visible desktop container is
therefore a replaceable packaging concern, while `Bun.WebView` remains the
real-browser qualification runtime.

## Decision

Fleetd will add a versioned, headless conversation client above its generated
wire client and below every presentation. It has three explicit layers:

1. A `ConversationTransport` supplies authority-specific channel discovery,
   membership discovery, durable message sending, and replay/live streaming.
2. A `ConversationSession` owns target-neutral selection, cursor retention,
   immutable message projection, stable-identity conflict detection, and
   observable connection state.
3. A presentation renders session snapshots and translates deliberate user
   actions into session commands.

The first browser transport composes two credentials without combining their
authority. It uses the operator only to discover channels. It uses the exact
human participant for membership reads, stream-grant issuance, principal-
relative replay/live continuation, and attributed sends. Transport state is
network state only; it does not infer agent activity from elapsed time.

The first presentation is a same-origin static Fleetd adapter. It stores no
credential or transcript and consumes only the public HTTP and browser-stream
contracts. It owns one separately documented application contract for turning
human text into an opaque request and rendering the matching harness result.
All unrecognized kinds and payloads use an exact envelope fallback.

A small Bun-native desktop package may load that public page in a visible
system webview and hand it credential values read from explicit owner-only
files. The window package is not a Fleetd plugin, protocol provider, or source
of truth. The same page remains directly usable in an ordinary browser, and a
future TUI can implement `ConversationTransport` with the public bearer-
authenticated native WebSocket without importing browser or desktop code.

The first visible host uses Electrobun 2.0.1 with a Bun main process and the
platform-native renderer. Its strict profile refers to separate owner-only
credential files rather than embedding bearer values. It loads only the exact
configured loopback conversation URL and performs one fire-and-forget handoff
to the page's public bootstrap object. Electrobun remains replaceable because
no transport or session operation crosses into the host.

## State and race rules

- SQLite and the immutable Fleetd message log remain the only conversation
  authority.
- A new session starts replay from cursor zero. A retained channel projection
  resumes from only its highest accepted cursor.
- Channel selection is generation-fenced. Messages or membership results from
  an abandoned selection cannot mutate the newly selected channel.
- Message acceptance is ordered. Repeated exact identities are ignored;
  conflicting sequence or message identities fail the selected session.
- A successful send response may enter the projection before its stream echo,
  but both paths use the same stable-identity acceptance operation.
- A selected stream reports connecting, live, reconnecting, failed, and closed
  honestly. Those states make no claim that an agent is working.
- Credentials and one-time grants remain memory-only and are cleared when the
  transport closes. No URL, cookie, Web Storage, IndexedDB, generated asset,
  log, or evidence artifact may contain them.

## Application contract boundary

Fleetd still defines no universal chat prompt or reply. The first presentation
accepts an exact external profile containing a request kind and result kind.
For that profile only, it sends a JSON object with a `text` string and renders
the bounded assistant-message content from the matching managed completion.
The exact immutable envelope is always available, and every other kind is
rendered as opaque JSON.

This mapping belongs to the presentation package. A worker profile must
independently accept the configured request kind and emit the configured result
kind. Fleetd transports both without importing or validating this application
contract.

## Consequences

- Browser, visible desktop, and later TUI targets can share selection,
  projection, conflict, and send behavior.
- The first useful surface can ship before a semantic compiler is available,
  while remaining straightforward to lift and replace later.
- A desktop window framework can be upgraded or replaced without changing the
  Fleetd daemon protocol or the conversation state model.
- Live execution telemetry remains absent. A future surface may consume it only
  after the separately versioned operator-event subscription exists.

## Qualification

The client contract requires deterministic tests for authority routing,
selection races, cursor resumption, duplicate and conflict handling, send/echo
convergence, teardown, and unknown-envelope preservation. The actual browser
presentation must also pass in Bun's WebKit backend against a fresh daemon and
real continuous worker. A visible-host smoke test is additional packaging
evidence; it does not replace the WebKit product-loop qualification.
