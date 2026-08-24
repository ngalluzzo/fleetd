# ADR 0010: Harness sessions use durable bindings and owner epochs

- Status: accepted
- Date: 2026-08-24

## Context

A native harness session reference is not sufficient ownership. After a
controller or driver restart, two processes may both know the same reference;
an old process may still emit a terminal response after a replacement has
resumed; and a configuration change may make the old transcript unsafe to
load. Keeping the binding only in controller memory also leaves the invocation
write-ahead fence and the harness session state able to disagree across a
crash.

The messaging kernel must remain unaware of sessions and harnesses. These are
controller records, but they share SQLite transactions with the managed
invocation ledger where a single commit is required for safety.

## Decision

fleetd stores session bindings and their invocation turns in controller-owned
tables introduced by migration 0007. A logical lane is selected by opaque
`(agent_id, lane_policy, lane_key)` values. Exactly one generation of that lane
may be non-retired. The binding ID remains stable across rotations,
`binding_generation` increases when a new native transcript is required, and
`owner_epoch` increases whenever another controller instance adopts a
compatible ready session.

Acquisition runs in an immediate SQLite transaction:

- the same controller-instance ID and immutable configuration replay
  idempotently;
- a compatible `ready` binding is resumed with `owner_epoch + 1`;
- an incompatible `ready` binding or an `opening` binding abandoned by another
  owner is retired and replaced by the next binding generation;
- `active` and `uncertain` bindings are never adopted automatically.

The caller supplies profile and compatibility digests plus absolute workspace
paths. These are controller-policy inputs, not facts inferred by the storage
module. Compatibility must therefore come from a qualified profile; an opaque
matching string is not evidence by itself.

A new binding starts as `opening`. The controller opens or resumes the native
session and persists its opaque reference before any prompt may be armed.
Arming then changes the reserved invocation to `dispatch_armed`, inserts the
exact `(binding_id, binding_generation, owner_epoch, invocation_id)` turn, and
changes the binding to `active` in one transaction. A stale epoch, mismatched
session reference, or second active turn fails before dispatch authorization is
committed.

Known quiescent completion is also one transaction: publish the deterministic
result, acknowledge the input, terminalize the invocation, record persistence
evidence, and return the binding to `ready`. Exact completion replay remains
read-only and valid even after a later owner epoch has adopted the ready
session.

Post-arm ambiguity first changes the binding and turn to `uncertain`, then
parks the delivery. If the controller dies between those commits, ordinary
invocation lease recovery still parks the delivery. Conversely, recovery of an
expired bound invocation changes the active binding to `uncertain` in the same
transaction that blocks the delivery and terminalizes the invocation. Generic
acknowledgement, blocking, and completion cannot settle an active bound turn;
they must cross the binding-aware transition.

Uncertainty is not converted to safety by timeout, process exit, or missing
evidence. An operator or a future harness-specific reconciler must retire or
otherwise reconcile the exact generation before the lane can advance. The
initial API supports explicit retirement, which preserves the old turn and
binding evidence and causes the next acquisition to create a new generation.

## Consequences

Only the latest owner epoch can authorize a new turn, including when controller
acquisitions race. A controller crash can strand or rotate an unused native
session, but cannot make an ambiguous prompt automatically execute again.
Session history and raw trajectories remain owned by the harness; fleetd stores
only opaque references and bounded control evidence.

The first implementation has no public session-binding HTTP or CLI surface.
The trusted managed controller uses the `Store` boundary directly while a
continuous worker and operator reconciliation surface are designed. Persisted
plugin generations, bounded invocation events, compatibility qualification,
and proven in-place reconciliation of uncertain sessions remain separate work.
