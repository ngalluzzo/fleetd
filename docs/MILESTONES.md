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

## M2 — Work contracts

- [x] Bind a GOOIR capability need to exact facts and carry it through one
  durable, owner-fenced provider attempt.
- Versioned task, progress, result, review, and approval contracts.
- Dependency scheduling expressed outside the messaging kernel.
- A complete author-reviewer loop exercised on fleetd itself.

## M3 — Git adapter

- Worktree isolation and exact-revision evidence.
- Patch publication and independent review.
- Atomic merge authorization bound to reviewed base and head revisions.
- No Git hosting and no direct-push policy emulation.

## M4 — Operator surface

- A small web dashboard backed exclusively by the public API.
- Fleet health, blocked work, message traces, and model throughput.
- Remote workers only after authenticated transport is complete.
