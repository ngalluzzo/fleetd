# ADR 0020: Operational observations are fixed-size control records

- Status: accepted for experimental dogfood
- Date: 2026-08-24

## Context

The continuous worker could recover delivery and session ownership, but its
plugin identity, lifecycle outcome, and streamed turn evidence disappeared
with the controller process. That made an overnight fleet difficult to
diagnose and impossible to project consistently into browser and terminal
operator surfaces.

Copying every ACP update into SQLite would create a second append-only
transcript store. Reasoning, tool output, and unknown updates can be large, may
contain sensitive content, and already have an authoritative owner in the
native harness. A generic row-per-event ledger would also turn Fleetd's control
database into an unbounded analytics store before retention and redaction
requirements exist.

## Decision

Fleetd persists two controller-owned operational records outside the messaging
kernel:

1. One `plugin_generations` row for every plugin process that reaches readiness.
   It captures exact negotiated plugin, interface, driver, runtime, profile,
   compatibility, process, and initialization evidence before work can route
   through the generation. A heartbeat supplies advisory liveness. Retirement
   records the worker disposition and observed process-group shutdown outcome.
2. One `invocation_observations` row for every managed invocation, created in
   the same transaction that arms its dispatch and activates its exact session
   owner fence. Each contiguous harness update folds into fixed counters, total
   encoded bytes, latest-event digest, and a cryptographic chain digest. The
   terminal response contributes stop, certainty, quiescence, persistence, and
   usage evidence.

Raw update JSON is hashed while being observed but is not retained in the
control database. The native harness transcript owns the raw trajectory. The
bounded immutable result message owns output transported through Fleetd. Exact
replay of the latest event is idempotent; sequence gaps, changed replays, stale
generations, and post-terminal updates fail closed.

Operator-only API read models expose generations, session bindings, and
invocation observations. A GUI, TUI, or external collector consumes those same
public contracts; none receives a private internal projection.

## Consequences

- Worker restart, plugin replacement, liveness loss, and shutdown behavior are
  inspectable after the process exits.
- Operational storage grows by one bounded row per generation and invocation,
  not by trajectory length.
- The event chain proves ordering and detects changed evidence but cannot
  reconstruct redacted raw content. Deep trajectory inspection remains a
  harness operation.
- Heartbeat health is advisory process evidence, not proof that a model,
  provider, or external dependency is healthy.
- Selected raw artifacts, retention policies, and time-series export may be
  added later behind independently designed sinks without changing the
  authoritative control record.
