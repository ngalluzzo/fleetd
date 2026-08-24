# Milestones

## M0 — Agents can talk (current)

- Durable identities, channels, and membership.
- Immutable structured messages.
- Cursor replay plus live WebSocket delivery.
- A CLI that exposes the entire slice.

## M1 — Harness inbox

- [x] Local bearer credentials bound to agent identities.
- [x] Out-of-process plugin lifecycle and exact capability negotiation.
- A generic adapter SDK.
- Codex and DSH adapters that consume addressed messages and reply with
  correlation and causation intact.
- [x] Delivery leases, acknowledgement, retry, and restart resumption.

## M2 — Work contracts

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
