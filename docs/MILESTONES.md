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
  complete `fleetd.harness-acp@0.1.0` matrix. OpenCode is the first
  production-shaped integration; Codex still needs real-runtime qualification.
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
- [ ] Qualify the served conversation presentation through trusted WebKit user
  input, exact rendered envelopes, and credential-free ephemeral storage.
- [ ] A separately versioned live operator-event subscription for bounded
  invocation activity; do not encode activity as synthetic channel messages.
- [ ] Fleet health, blocked work, message traces, session ownership, and model
  throughput.
- Remote workers only after authenticated encrypted transport and enrollment.
