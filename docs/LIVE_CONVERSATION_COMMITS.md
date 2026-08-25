# Live conversation implementation commit stack

This stack follows design commit
`95dedb0c2af6388e148eb00ae42f9ef4815fb6ca`. Each commit must build and pass the
complete repository gates independently. Public operations appear only when
their complete authorization and failure behavior exists; no commit introduces
polling or an incomplete browser authentication path.

## Phase A — Addressable passive participants

### 1. `feat: add immutable membership delivery modes`

Implement the operational kernel invariant from ADR 0023 without changing the
public HTTP contract yet.

- Add forward migration `0009_channel_membership_delivery_mode.sql` with a
  checked non-null `delivery_mode`, defaulting existing rows to `inbox`.
- Add the closed Rust enum and bounded membership read model.
- Add exact store operations for creating and listing membership modes while
  preserving the existing `add_member` behavior as `inbox`.
- Make direct and broadcast delivery snapshots select only `inbox` membership.
- Keep history, WebSocket visibility, message identity, and idempotent replay
  independent of delivery mode.
- Prove migration preservation, direct and broadcast snapshots, idempotent
  replay, and concurrent append/member creation in store-level tests.

This commit does not expose `stream_only` through HTTP and does not add browser
code.

### 2. `feat: publish exact channel membership contracts`

Expose the complete membership contract as one generated API change.

- Add `CreateChannel.members` while preserving `member_ids` as the `inbox`
  shorthand; reject duplicates across both inputs.
- Add optional `AddMember.delivery_mode`, defaulting to `inbox`.
- Make an exact member-add replay idempotent and a mode mismatch conflict.
- Add `GET /v1/channels/{channel_id}/members` for the operator or an exact
  channel member, returning no opaque agent metadata.
- Add authorization and behavioral API tests.
- Regenerate `openapi/fleetd-v1.json` and the pinned TypeScript client, then run
  its typecheck.

No conversation presentation contract is introduced.

### 3. `test: qualify stream-only conversation membership`

Prove the public composition rather than adding another feature.

- Create one human-controlled `stream_only` identity and one worker `inbox`
  identity through the public API.
- Prove human-to-worker messages create exactly the worker delivery.
- Complete an invocation back to the human and prove the result creates no
  human inbox delivery while remaining present in history and native live
  replay.
- Prove operator and participant visibility remain different and exact.
- Restart the daemon and replay the result from the prior cursor.
- Exercise broadcast behavior with both membership modes.

The qualification must inspect SQLite to prove absence of false delivery rows,
not infer it from an empty claim response alone.

### 4. `docs: accept membership delivery mode`

Only after commits 1–3 pass:

- change ADR 0023 from `proposed` to `accepted`;
- rename the draft membership contract to its stable v1 path;
- update protocol, architecture, API, and milestone prose; and
- record the exact qualification revision and commands.

## Phase B — Browser-compatible live replay

### 5. `refactor: isolate the authorized channel stream`

Extract the already-qualified replay/live engine without changing its wire
behavior.

- Represent an authorized stream as exact channel, cursor, credential ID,
  principal kind, and optional viewer agent ID.
- Keep access checks outside the shared engine.
- Keep subscription-before-replay, visibility filtering, lag recovery, and
  ascending cursor behavior in one implementation.
- Retain raw `Message` frames for the existing native/TUI endpoint.
- Add parity tests proving the refactor emits the identical sequence for every
  existing native stream case.

This commit adds no grants and no public route.

### 6. `feat: add bounded single-use stream grants`

Implement the process-local authority broker with no HTTP or WebSocket route.

- Generate 256 bits of entropy and store only its digest plus exact scope.
- Enforce monotonic 15-second expiry, per-credential and global unused bounds,
  and atomic remove-before-use redemption.
- Bind credential ID, principal shape, channel, cursor, daemon process, and
  protocol.
- Add active-stream slot accounting and guaranteed release guards.
- Add an internal credential-ID revalidation operation that returns no raw
  credential data.
- Prove expiry, pruning, concurrent single winner, capacity, revocation, and
  debug/log redaction in unit tests.

The broker remains unreachable outside the process in this commit.

### 7. `feat: define the browser channel stream edge`

Add protocol DTOs and the fail-closed browser-origin policy without registering
a route.

- Define strict grant-issue, redemption, ready, and tagged message-frame types.
- Derive one canonical browser origin from the exact loopback listen IP,
  scheme, and port.
- Reject hostname aliases, wildcard, missing, `null`, or mismatched origins and
  authorities.
- Validate the constant WebSocket subprotocol without placing secrets in it.
- Encode exact pre-authentication socket, first-frame, active-stream,
  credential-revalidation, and send-deadline bounds.
- Unit-test every parser and origin/authority decision.

Keeping the types and policy internal lets the full public edge land atomically
in the next commit.

### 8. `feat: add origin-bound browser channel streams`

Register issuance and redemption together so no released API mints an
unredeemable or under-protected grant.

- Add authenticated
  `POST /v1/channels/{channel_id}/stream-grants` with `no-store` responses.
- Add the dedicated same-origin browser WebSocket upgrade outside ordinary
  bearer middleware.
- Release no application data before bounded first-frame redemption.
- Revalidate credential and channel access, reserve an active-stream slot, send
  `ready`, and enter the shared stream engine.
- Wrap browser messages in the tagged browser frame without rewriting the
  immutable envelope.
- Revalidate the credential before each emitted message and within the idle
  bound; enforce send deadlines and release capacity on every close path.
- Keep message sending on the existing attributed HTTP operation.
- Generate OpenAPI and TypeScript artifacts for grant issuance and the exact
  WebSocket extension.
- Include complete happy-path, rejection, visibility, reconnect, and
  revocation API tests in the same commit.

There is still no browser UI.

### 9. `test: exhaust the browser stream failure matrix`

Add adversarial and concurrency coverage that is easier to review separately
from the endpoint implementation.

- Race duplicate redemption and prove one winner.
- Commit messages across issuance, upgrade, redemption, subscription, replay,
  and live handoff boundaries.
- Force broadcast lag and slow-client send timeout, then recover by cursor.
- Rotate credentials before and after readiness.
- Exhaust and recover unused-grant, pre-authentication, per-credential, and
  global stream limits.
- Inspect logs, OpenAPI, SQLite, paths, headers, and subprotocols for raw bearer
  or grant material.
- Prove daemon restart invalidates grants without changing durable replay.

### 10. `test: qualify stream grants in a real browser`

Use an authoritative browser automation runtime against the embedded static
origin; do not emulate the JavaScript `WebSocket` constructor with a Rust
client.

- Negotiate the constant subprotocol from the real constructor.
- Mint over authenticated Fetch, redeem as the first frame, and receive ready,
  replay, and live messages.
- Prove the content security policy permits only the exact same-origin socket.
- Prove a foreign-origin page and hostname alias fail before application data.
- Prove the browser never places either secret in location, history, storage,
  cookies, subprotocol, console, or error reporting.
- Check in a reproducible qualification record with exact browser and server
  versions.

### 11. `docs: accept browser channel stream grants`

Only after commits 5–10 pass:

- change ADR 0022 from `proposed` to `accepted`;
- rename the browser draft contract to its stable v1 path;
- update the protocol and API documentation with the two equivalent stream
  authentication edges; and
- record the exact automated and real-browser qualification revisions.

## Phase C — Product-loop qualification before presentation

### 12. `test: qualify a live human-to-agent conversation`

Run the first product-level composition with no custom UI.

- Provision a human `stream_only` participant and a real continuous worker
  through public Fleetd operations.
- Open the browser-grant stream as either the human or explicitly labelled
  operator viewer.
- Send as the human agent credential, never as operator.
- Prove the worker resumes the channel-scoped native session and publishes one
  causal result visible through the same live stream.
- Continue the conversation after browser reconnect, daemon restart, worker
  restart, and compatible harness-session adoption.
- Preserve unknown message and result fields and inspect the absence of a human
  leased inbox.
- Publish the exact evidence artifact and revision.

Passing this commit means Fleetd offers the operational substrate needed by a
conversation target. It does not mean a browser or TUI presentation has been
lowered.

## Work deliberately outside this stack

Live execution telemetry remains a separate design effort. Before any surface
shows `working`, reasoning, tool activity, or session health live, Fleetd needs
a proposed operator-event-stream ADR covering authorization, snapshot/live
handoff, monotonic revisions, lag recovery, and backpressure. Presentation code
must not poll current observations or write synthetic activity messages.

GOOIR integration also remains outside Fleetd. After commit 12, a separately
versioned integration package may lift the stable OpenAPI and stream artifacts,
link them to conversation requirements, and lower native/TUI and browser
targets. Fleetd contains neither that semantic contract nor the compiler
runtime. The first presentation commit begins only after the operational offers
above are stable and qualified.

## Gate for every commit

Run the repository-required Rust gates on every commit. Any commit that changes
the public API also regenerates and reviews OpenAPI and TypeScript artifacts and
runs the pinned TypeScript typecheck. Qualification commits additionally record
all external runtime versions and leave no daemon, worker, harness, browser, or
model-server process they started running after completion.
