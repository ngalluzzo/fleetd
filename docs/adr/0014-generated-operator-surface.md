# ADR 0014: The operator surface consumes a generated target contract

- Status: accepted
- Date: 2026-08-24

## Context

The first operator UI needs blocked-delivery columns, actions, selectors, and
endpoint bindings. Duplicating those details independently in server prose and
browser code creates drift.

## Decision

`web/operator/contract.json` is a checked-in generated target artifact. The
static browser adapter reads it and uses only Fleetd's public authenticated API.
The server embeds and serves the artifact but has no dependency on the system
that generated it.

The contract selects the blocked-delivery read model, requeue and abandon
actions, exact endpoint templates, and table columns. Tests verify the served
contract, browser bindings, authentication behavior, and both resolution
effects.

Any external generator must operate through the
[integration boundary](../INTEGRATION_BOUNDARY.md): it may lift Fleetd's public
artifacts and lower a target contract, but Fleetd does not import its IR or
runtime.

## Consequences

The UI can be regenerated or independently reimplemented from one target
artifact. The browser remains a small Fleetd adapter, while compiler choice and
semantic derivation remain external and replaceable.
