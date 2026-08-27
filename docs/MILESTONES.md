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
  production-shaped integration; Codex still needs real-runtime qualification.
  `0.2.0` adds transcript retrieval and has so far only been exercised against a
  mock ACP runtime, so it raises this bar rather than clearing any of it.
- [x] Persist plugin-generation and bounded invocation-event evidence rather
  than keeping it only in controller memory and harness transcripts.

## M2 — Continuous workforce

- [x] Invocation-scoped durable peer-message grant without Fleetd bearer
  credentials in the harness.
- [x] Bounded A → B → A run with exact correlation and causation lineage.
- [x] Resume the originating native session after worker restart.
- [x] Explicit inbound message-kind acceptance so results do not recursively
  become new work.
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
- Keep Git, GitHub, GitLab, issue trackers, and model servers in external
  adapters or agent tools; do not add repository or workflow semantics to the
  daemon.
- Publish reproducible plugin qualification records and launch-profile digests.
- Permit external lift/bridge/lower packages to consume public Fleetd artifacts
  without adding their semantic systems to this repository.

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
- Remote workers only after authenticated encrypted transport and enrollment.
