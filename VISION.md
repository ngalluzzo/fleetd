# fleetd vision

Software agents should be able to form a dependable workforce without being
trapped inside one model vendor, harness, forge, chat product, or identity
protocol.

`fleetd` is the local-first coordination plane for that workforce. It gives
agents durable conversations, reliable inboxes, explicit work contracts, and an
operator-visible history. Existing tools continue doing what they are already
good at: Git stores code, harnesses run agents, model servers produce tokens,
and external services expose their own APIs.

## Product promises

- **The night shift resumes.** A crashed daemon, harness, or machine does not
  silently discard assigned work.
- **Agents can change seats.** Codex, DSH, or a future harness can implement the
  same adapter contract without changing the coordination kernel.
- **Every consequential action has evidence.** Messages, claims, results,
  reviews, and merge authorization remain attributable and inspectable.
- **The operator stays sovereign.** One person can run a useful fleet on one
  machine with ordinary files and SQLite. Cloud services are optional adapters.
- **Policy is explicit.** Approval and merge rules are evaluated in one place,
  against exact artifacts and revisions.
- **Failure is honest.** Delivery is described as at-least-once, ambiguity is
  surfaced, and consumers receive stable identities for idempotency.

## Engineering posture

The public protocol is a product surface. Contracts are versioned, migrations
are forward-only and tested, unknown data survives transport, and observability
is designed alongside execution. Features are not complete until restart,
concurrency, and partial-failure behavior is known.

## Non-goals

`fleetd` is not a Git forge, model server, general chat network, social identity
system, or replacement for mature harnesses. Federation may eventually be an
adapter; it is not an architectural prerequisite.
