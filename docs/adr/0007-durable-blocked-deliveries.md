# ADR 0007: Ambiguous deliveries are durably blocked

- Status: accepted
- Date: 2026-08-24

## Context

At-least-once delivery recovers ordinary worker crashes by allowing an expired
lease to be claimed again. That is unsafe when a worker sent an external tool
request but lost the response: retrying might repeat an effect, while
acknowledging might discard unfinished work. A timeout, process exit, or missing
observation is not evidence that the effect did not happen.

Harnesses can report evidence and retry advice, but they cannot decide fleet
retry policy. The kernel needs a semantically agnostic way to stop automatic
delivery without learning about tools, sessions, or harness stop reasons.

## Decision

The agent holding the current unexpired delivery lease may settle it as
`blocked` with a non-empty reason of at most 4,096 bytes. Blocking clears the
lease, stores a durable evidence record bound to the message, agent, attempt,
and lease token, and makes the delivery ineligible for claims. Lease expiry does
not release blocked work.

The first block request returns `201 Created`. An exact replay with the same
lease and reason returns the original record with `200 OK`; different evidence
for the same lease fails with `409 Conflict`. An expired, foreign, or superseded
lease cannot block a delivery. Concurrent identical requests serialize through
an immediate SQLite transaction and converge on one record.

Only the operator may list unresolved blocks and resolve a specific block.
`requeue` makes the existing delivery pending after a bounded delay. `abandon`
makes it terminal and unclaimable. The decision, optional bounded note, and
delay are written once onto the durable block record. An identical resolution
replay is idempotent; a different second decision conflicts.

The kernel treats reason and note as opaque evidence. It does not infer
execution certainty, authorize a retry from a harness stop reason, or understand
the external effect.

## Consequences

Known ambiguity no longer turns into an automatic retry when a lease expires.
Operators have an auditable decision point, and a future worker controller can
conservatively map `outcome_unknown` evidence to a kernel primitive without
adding harness semantics to the kernel.

This is not exactly-once execution. A controller can still crash after an
external effect and before recording a block. Durable invocation reservation
and recovery policy must identify that window before a reclaimed delivery is
executed again; [ADR 0008](0008-write-ahead-invocation-fence.md) supplies that
write-ahead boundary. Block reasons may also contain sensitive tool evidence,
so adapters must redact them before submission.
