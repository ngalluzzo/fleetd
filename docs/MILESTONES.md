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

## M2 — Work contracts

- [x] Bind a GOOIR capability need to exact facts and carry it through one
  durable, owner-fenced provider attempt.
- [x] Persist exact semantic-provider context and strictly lift a raw attempt
  into a content-addressed candidate or explicit unable result.
- [x] Implement the first real independently identified conformance provider
  and a GOOIR-derived blocked-delivery web artifact.
- [x] Return the exact durable candidate through conformance and re-plan the
  GOOIR graph with its admitted fact.
- [x] Preserve ACP assistant-message boundaries and lift a model's final
  structured result without discarding its progress transcript.
- [x] Delegate one exact-revision repository inspection through a specialized
  capability adapter and validate its complete report and Git citations.
- [x] Define exact-revision repository patch proposals and deterministically
  conform fixture candidates through an isolated Git index without mutating the
  source checkout.
- [ ] Qualify a real repository-patch provider. The first cloud and local Qwen
  attempts failed closed before producing a candidate.
- Versioned task, progress, result, review, and approval contracts.
- Dependency scheduling expressed outside the messaging kernel.
- A complete author-reviewer loop exercised on fleetd itself.

## M3 — Git adapter

- [x] Worktree isolation and exact-revision evidence for inspection and patch
  proposals.
- [ ] Patch publication and independent review. Patch artifact conformance is
  implemented; no provider or reviewer is qualified and no publication
  authority exists.
- Atomic merge authorization bound to reviewed base and head revisions.
- No Git hosting and no direct-push policy emulation.

## M4 — Operator surface

- [x] First small web surface backed exclusively by the public API, with its
  columns, actions, selector, and bindings derived from exact GOOIR target IR.
- Fleet health, blocked work, message traces, and model throughput.
- Remote workers only after authenticated transport is complete.
