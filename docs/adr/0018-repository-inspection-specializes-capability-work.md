# ADR 0018: Repository inspection specializes capability work

- Status: accepted for experimental dogfood
- Date: 2026-08-24

## Context

The generic capability-work loop proved that Fleetd can carry a GOOIR
`CapabilityNeed`, bind exact facts, run a selected semantic provider, preserve
the raw attempt, and lift a candidate. The next dogfood goal is useful software
work against Fleetd itself. Encoding that as an unbounded prose task or a
generic `execute` method would discard the meaning that made the first loop
safe and would prematurely introduce task, Git, and workflow concepts.

Repository inspection is narrower. Its inputs are an exact Git revision,
bounded path scope, and explicit questions. Its output is a report whose claims
remain untrusted but whose coverage and source locations can be checked
deterministically.

## Decision

Define repository inspection as a versioned specialization of capability work:

- one exact inspection capability consumes one complete inspection-brief fact
  and produces one complete report fact;
- the generic request, invocation, session, provider, attempt, and candidate
  identities remain unchanged;
- the semantic adapter binds one exact provider and preflights an isolated,
  clean checkout at the requested commit before arming;
- Git remains the authoritative parser for repository identity, revision, work
  tree state, and source objects; and
- a separately invoked deterministic suite validates the exact answer set,
  evidence scope, and Git line locations after the generic strict lift.

The suite calls a result `conformant_candidate`, not accepted truth. Natural
language conclusions remain claims. The path scope constrains admissible
evidence but is not represented as an operating-system read sandbox.

## Consequences

- Fleetd can delegate useful brownfield analysis without adding a generic task
  system or teaching its kernel about repositories.
- GOOIR capability needs and Fleetd plugins now meet at the same exact semantic
  boundary: a plugin is one implementation of a capability, while the request
  stays implementation- and harness-neutral.
- Exact facts, not prose conversation state, determine what was inspected.
- The worker fails closed on a wrong revision, dirty checkout, provider
  mismatch, incomplete question set, or unverifiable citation.
- Inspection gives no write authority. Patch production, review, and merge must
  be distinct capabilities with their own evidence and policy.
- The first implementation remains experimental under the two-implementation
  rule.

See the [repository-inspection contract](../contracts/repository-inspection-v1.md)
and [first qualification](../qualification/repository-inspection-opencode-2026-08-24.md).

