# ADR 0014: The operator surface consumes exact GOOIR target IR

## Status

Accepted for the first Fleetd/GOOIR dogfood artifact.

## Context

Fleetd already had the authenticated blocked-delivery API. Implementing a web
page directly from that API would create another independently maintained copy
of its columns, actions, selectors, and endpoint bindings—the duplication GOOIR
is intended to remove.

The first dogfood also demonstrated why an agent response is not the artifact.
Useful brownfield source and exact semantic facts can coexist, but neither
automatically validates the other.

## Decision

The first operator surface is a same-origin static adapter at `/operator/`.
`/operator/contract.json` contains one exact GOOIR web target fact. The
JavaScript reads that document and derives:

- table columns;
- action labels and outcomes;
- the selector field and path parameter;
- list and resolution methods and paths.

The browser adapter does not contain a second Fleetd-specific API model. It may
make presentation choices, such as field formatting and confirmation for a
dead outcome, without changing semantic action names or bindings.

The four static assets are served outside OpenAPI routing under a restrictive
Content Security Policy. They are public bootstrap resources; the existing
operator-only endpoints remain the sole authority for data and effects. The
operator token is held in JavaScript memory only and is cleared from the input
after connection.

## Consequences

- GOOIR target IR is now executable product input rather than documentation.
- The same target fact can be compared exactly at lift, runtime, conformance,
  and admission boundaries.
- UI presentation may evolve without changing the kernel API or semantic
  contract.
- A future generated adapter can replace these static source files while
  preserving the same artifact and conformance contracts.
- Brownfield source remains valuable evidence, but is not automatically a
  trusted GOOIR fact.
