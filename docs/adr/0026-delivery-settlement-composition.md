# ADR 0026: Delivery settlement composes the kernel and the invocation fence

## Status

Accepted.

## Context

`ARCHITECTURE.md` gives the kernel six concepts, delivery among them, and
describes the invocation fence as a separate concern layered above it. The code
disagreed. `delivery.rs` called into `invocation.rs` at five points — recovering
expired invocations before a claim, and terminalizing the bound invocation on
acknowledge, retry, and block — while `invocation.rs` called back into
`delivery.rs` for agent and claim validation. Together with a third edge through
`session_binding.rs`, the three modules formed one cycle.

The cycle is not accidental. A delivery and the invocation fencing it must
settle in the same commit, or a crash between the two leaves a delivery
acknowledged with an invocation that never terminalized. That is exactly what
ADR 0008's write-ahead fence exists to prevent. `complete_invocation_transaction`
makes the same point from the other side: it appends a message, acknowledges a
delivery, and terminalizes an invocation in one transaction, so kernel and
execution state share one pool by construction.

A cycle cannot cross a crate boundary, so the documented kernel was not
buildable as a separate crate.

## Decision

Split responsibility rather than the transaction.

The kernel owns the delivery row's state machine. Every transition a row can
make — lease, acknowledge, retry, block, and the two sequence-keyed variants the
fence needs — is a transactional function in `delivery.rs` that reports whether
the row moved and commits nothing.

Composition moves above the kernel into `settlement.rs`. It opens one immediate
transaction, applies the kernel transition, terminalizes the invocation through
the fence, and commits once. Atomicity is unchanged; what changed is which
module decides that both belong in the same commit.

The kernel exposes `Store::begin_immediate` so callers above it can enlist their
own work in a kernel transaction without holding the pool.

Settlement entry points are free functions over `&Store` rather than methods on
it. The kernel owns that type, so once these layers become crates, methods could
not be added from outside; free functions also make the composing layer visible
at the call site.

## Consequences

`delivery.rs` has no outgoing dependency above the kernel, so the kernel is now
extractable as a crate. Two cycles remain — `invocation` with `session_binding`,
and `channel_stream` with `stream_grant_broker` — and both sit inside a single
prospective crate, where a cycle is legal and carries no cost.

The delivery state machine is written once. Before this change, `invocation.rs`
hand-wrote four transitions against `agent_deliveries` and `delivery_blocks`
with their own copies of the lease predicates, and the kind-filtered claim was a
near-duplicate of the plain one; that duplicate is now the same function with an
optional filter.

Two assertions in `tests/crate_boundaries.rs` hold the line: no kernel module
may reference a module layered above it, and no module above the kernel may
write a kernel table.

Callers changed shape: `store.claim_deliveries(..)` is now
`settlement::claim_deliveries(&store, ..)`, and likewise for acknowledge, retry,
and block.
