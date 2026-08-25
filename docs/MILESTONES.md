# Milestones

## M0 — Agents can talk (current)

- Durable identities, channels, and membership.
- Immutable structured messages.
- Cursor replay plus live WebSocket delivery.
- A CLI that exposes the entire slice.

## M1 — Harness inbox

- [x] Local bearer credentials bound to agent identities.
- [x] Out-of-process plugin lifecycle and exact capability negotiation.
- [x] A durable invocation ledger with session generations, owner fencing, and
  idempotent result append.
- [x] Atomic delivery reservation and write-ahead invocation dispatch fence.
- [x] Atomic idempotent result publication and input acknowledgement.
- [ ] Two independently versioned harness plugins qualified against the shared
  typed ACP host. OpenCode is the first production-shaped plugin; historical
  Codex and DSH qualification used the development reference plugin and must
  move behind their own identities.
- [x] A managed local worker controller that continuously consumes addressed
  messages and replies with correlation and causation intact, with supervised
  plugin restart and durable native-session adoption.
- [x] Delivery leases, acknowledgement, retry, and restart resumption.
- [x] Durable unknown-outcome parking with operator-only resolution.
- [x] First invocation-scoped outbound message capability: a real OpenCode
  turn committed an idempotent, attributed peer message through a
  controller-owned MCP endpoint without receiving a fleet bearer credential.
- [x] A bounded two-seat OpenCode loop composed three capability-authored hops
  and resumed A's same native session under a higher owner epoch.
- [x] Versioned inbound message acceptance at the worker/adapter boundary so
  continuous seats do not treat generic completion results as new work.

## M2 — GOOIR capability ecosystem

- [x] Negotiate one package-level GOOIR offer set containing several exact
  capability implementations.
- [x] Keep ACP as a transport for independently versioned agent-session
  capabilities.
- [x] Consume exact GOOIR invocations and results without teaching the worker
  their domain meaning.
- [x] Produce content-addressed GOOIR candidates with immutable Fleetd message
  evidence while leaving conformance and admission to GOOIR.
- [ ] Publish a separately versioned GOOIR protocol artifact and consume it as
  Fleetd's single wire source of truth.
- [ ] Advertise Fleetd's durable-message implementation as an exact GOOIR
  capability while keeping invocation grants in the runtime layer.
- [ ] Qualify two independent plugins that implement the same capability and
  one plugin package that implements capabilities from two families.

## M3 — Distributed work

- Define Git, GitHub, and GitLab capability packages as independent,
  composable implementations rather than one repository provider interface.
- Version task, progress, result, review, and approval capability contracts
  outside Fleetd core.
- Express dependency scheduling in GOOIR or a workflow package, not the
  messaging kernel.
- Exercise a complete author-reviewer loop on Fleetd itself.
- Keep Git hosting, direct-push policy, and merge semantics outside Fleetd.

## M4 — Operator surface

- [x] First small web surface backed exclusively by the public API, with its
  columns, actions, selector, and bindings derived from exact GOOIR target IR.
- Fleet health, blocked work, message traces, and model throughput.
- Remote workers only after authenticated transport is complete.
