# ADR 0008: Effectful dispatch uses a durable write-ahead invocation fence

- Status: accepted
- Date: 2026-08-24

## Context

Leased delivery alone cannot distinguish two controller crashes. If the process
dies before sending a harness request, retry is safe. If it dies after sending
the request but before recording the ambiguous outcome, automatic retry may
repeat an effect. Recording an invocation after claiming also leaves a smaller
window in which a leased attempt has no durable controller identity.

No local transaction can atomically commit with an arbitrary external process.
The safe ordering must therefore make uncertainty conservative rather than
trying to infer that a missing response means nothing happened.

## Decision

A managed controller reserves work through an invocation boundary. fleetd
leases each selected delivery and inserts its invocation record in one immediate
SQLite transaction. The record binds the invocation ID, agent, message,
delivery attempt, lease token and expiry, a distinct random fence token, and the
immutable input message. Concurrent reservers cannot create two records for one
attempt.

An invocation starts as `reserved`. The controller must durably change it to
`dispatch_armed` immediately before sending any effectful harness prompt or
request. Arming requires the matching agent credential, live delivery lease,
invocation ID, lease token, and fence token. It is idempotent for an identical
replay while the lease remains live. The plugin never receives the delivery
lease or agent credential.

All inbox claim paths recover expired managed invocations before selecting
work. An expired `reserved` invocation becomes terminal with certainty
`not_started`, after which the delivery may be reclaimed. An expired
`dispatch_armed` invocation becomes terminal with certainty `outcome_unknown`,
and fleetd atomically parks the delivery plus bounded recovery evidence in the
blocked queue. The latter cannot be retried without an operator decision.

Acknowledgement, retry, and block terminalize a matching invocation in the same
transaction as delivery settlement. A reserved invocation may retry and records
`not_started`. A dispatch-armed invocation cannot use ordinary retry; it must be
acknowledged as known or blocked as unknown.

Known successful execution completes through one invocation operation. fleetd
derives a deterministic `invocation/<id>/result` key and, in one immediate
transaction, appends the result plus delivery snapshot, acknowledges the input,
and terminalizes the invocation as `outcome_known`. The result stays in the
input channel, is addressed to the input sender, preserves correlation, and
sets causation to the input message. Exact completion replay returns the
original record even after restart or lease expiry; changed content conflicts.

The initial ledger deliberately implements only
`reserved | dispatch_armed | terminal`. Session bindings, owner epochs, plugin
generations, detailed runtime states, and event evidence remain in the worker
controller layer and will extend the ledger without changing this write-ahead
ordering rule.

## Consequences

There is no longer a crash point that both authorizes an effectful managed
attempt and allows it to be automatically executed again without review. A
crash between `dispatch_armed` and the actual send may block work that never
started; this false positive is the conservative cost of avoiding a duplicate
effect.

This is not exactly-once execution. Raw inbox claims intentionally retain
ordinary at-least-once behavior, and external systems still need idempotency or
reconciliation. Managed result publication and input settlement are atomic, but
arbitrary effects performed by a harness cannot participate in that SQLite
transaction and remain governed by the write-ahead fence and reconciliation.
