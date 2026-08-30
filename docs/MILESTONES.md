# Milestones

## M0 — Agents can talk

- [x] Durable identities, channels, and membership.
- [x] Immutable structured messages.
- [x] Cursor replay plus live WebSocket delivery.
- [x] A CLI exposing the complete messaging slice.

## M1 — Reliable harness inbox

- [x] Local bearer credentials bound to agent identities.
- [x] Out-of-process plugin lifecycle with exact operational-interface
  negotiation.
- [x] Atomic delivery reservation and write-ahead dispatch fence.
- [x] Atomic idempotent result publication and input acknowledgement.
- [x] Durable session bindings, generations, and owner epochs.
- [x] Managed continuous worker with supervised plugin restart and native
  session adoption.
- [x] Durable unknown-outcome parking with operator-only resolution.
- [ ] Qualify two independently versioned vendor harness plugins against the
  complete `fleetd.harness-acp@0.1.0` and `@0.2.0` matrix. OpenCode is the first
  production-shaped integration; Codex and DeepSeek Harness still need
  real-runtime qualification. DeepSeek currently omits ACP `session/load` and
  therefore truthfully implements only `0.1.0`; `0.2.0` adds transcript
  retrieval and has so far only been exercised against a mock ACP runtime, so
  it raises this bar rather than clearing any of it.
- [x] Persist plugin-generation and bounded invocation-event evidence rather
  than keeping it only in controller memory and harness transcripts.

## M2 — Continuous workforce

- [x] Invocation-scoped durable peer-message grant without Fleetd bearer
  credentials in the harness.
- [x] Bounded A → B → A run with exact correlation and causation lineage.
- [x] Resume the originating native session after worker restart.
- [x] Interrupt an active conversational turn when newer accepted input is
  committed in the same channel, retire the interrupted native session, then
  continue the durable lane in a fresh session from refreshed history without
  blocking other participants.
- [x] Explicit inbound message-kind acceptance so results do not recursively
  become new work.
- [x] Make agent membership executable through durable desired state and a
  machine-private approved runtime catalog, with in-product start, stop, and
  restart controls rather than a workflow graph.
- [ ] Run several real seats continuously against one daemon for a full night
  while recording restart, latency, throughput, and ambiguity evidence.
- [x] Add operator-visible plugin-generation, session, and invocation
  health through explicit read models.
- [ ] Exercise an author/reviewer loop on Fleetd itself using only opaque
  messages and external agent instructions.

## M3 — Integration ecosystem

- [x] Compile the first Fleetd HTTP adapter through independent native HTTP and
  Axum dialects, admit only the content-addressed Rust candidate, and retain no
  semantic compiler dependency in Fleetd.
- Stabilize the plugin authoring SDK only after two independent integrations
  pass the same operational-interface suite.
- [x] Introduce the experimental `fleetd.inference-openai@0.1.0` lifecycle and
  route interface, with independently identified MLX-VLM and llama.cpp plugin
  packages, strict vendor-owned configuration, shared machine supervision, and
  executable-shaped contract tests.
- [ ] Complete the remaining llama.cpp real-runtime qualification; MLX-VLM's
  Qwen3.8 27B lifecycle, real turn, direct follow-up, and restart-resumption
  proof passed on 2026-08-28.
- Keep Git, GitHub, GitLab, issue trackers, and model servers in external
  adapters or agent tools; do not add repository or workflow semantics to the
  daemon.
- Publish reproducible plugin qualification records and launch-profile digests.
- Permit external lift/bridge/lower packages to consume public Fleetd artifacts
  without adding their semantic systems to this repository.
- [x] Name the third authority category. An inbound trigger creates work under a
  registration that fixes its channel and its message kinds, with idempotency
  derived from the trigger and its occurrence, so a recurring job, webhook
  receiver, or file watcher stops needing a full bearer token to append anything
  anywhere.
- [ ] Supervise a trigger the way a plugin generation is supervised: health
  while it runs and restart with bounded backoff. Today a trigger is any process
  holding its credential, so a trigger that stopped firing on Tuesday is
  readable but nothing brings it back.
- [ ] A contributed scheduler, which is what settles whether a trigger's
  lifecycle really is a plugin generation's shape. Fleetd parses no cron
  expression and ships no calendar.
- [ ] What bounds a session lane a trigger feeds forever. One durable binding
  serves a channel, so nightly work accumulates in a native session that is
  never rotated, and transcript retrieval has only been measured against
  sessions holding one or two invocations.

## M4 — Operator surface

- [x] First small browser surface backed only by public Fleetd APIs.
- [x] Public channel-membership discovery plus immutable `inbox` versus
  `stream_only` delivery for addressable participants.
- [x] Single-use browser stream grants with origin validation, exact replay/live
  parity, and no polling fallback.
- [x] Human participant → continuous worker → causal result qualification across
  daemon, worker, and harness restart.
- [x] Qualify the served conversation presentation through trusted WebKit user
  input, exact rendered envelopes, and credential-free ephemeral storage.
- [x] Durable participant-owned read cursors with exact unread and explicitly
  addressed projections across client and daemon restarts.
- [ ] A separately versioned live operator-event subscription for bounded
  invocation activity; do not encode activity as synthetic channel messages.
- [x] Productized local initialization, worker/plugin status, delivery views,
  exact invocation/session traces, explicit recovery controls, tagged native
  binaries, offline backup/restore, and a repeatable hard-restart proof.
- [x] Optional OpenTelemetry egress of in-flight trajectory, absent by default,
  lossy by contract, and leaving the durable record unchanged.
- [x] Harness transcript retrieval through a short-lived second plugin process,
  so an operator can read the reasoning and tool calls Fleetd deliberately does
  not store, without disturbing the seat that owns the session.
- [ ] Per-model throughput. Fleet health, blocked work, invocation traces, and
  session ownership already ship through the productized commands above; this is
  the one part of that set still missing.
- [x] Qualify transcript retrieval against a real vendor harness, with exact
  per-invocation attribution: the envelope adapter names its invocation in the
  prompt and a replay carries prompt text verbatim, so a session holding two
  OpenCode invocations resolved both segments to invocations Fleetd dispatched.
- [ ] Qualify the same attribution across a session holding a night of
  invocations, where compaction, pruning, or a rewritten prompt could break the
  key that makes it exact.
- [ ] Bound a harness at the OS level rather than by its own good behaviour
  ([ADR 0034](adr/0034-os-level-harness-sandboxing.md)). The macOS Seatbelt
  foundation now wraps the complete plugin process group, and a real syscall
  test proves declared writes succeed while a sibling write is denied. Typed
  ACP `allow_once` resolution is available only under that boundary
  ([ADR 0038](adr/0038-one-shot-acp-permission-requires-an-os-boundary.md)).
  This remains open until a real vendor write turn passes, outbound provider
  traffic is destination-bounded, and the non-macOS posture is explicit. The
  first Claude qualification failed closed before any event because its local
  subscription credential had expired; no repository or canary write occurred.
- Remote workers only after authenticated encrypted transport and enrollment.
