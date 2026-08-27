# ADR 0030: HTTP mechanism adapters are generated outside Fleetd

Status: accepted

## Context

Fleetd's HTTP modules historically wrote the same route meaning directly into
Axum extractors, Utoipa annotations, and route registration. That made Axum the
authoring model and left no reusable semantic contract for another target. It
also mixed replaceable mechanism glue with authorization and durable product
behavior.

Fleetd must not embed a semantic compiler or move product decisions into a
generated runtime. HTTP, CLI, and MCP remain peer mechanisms, while an
operations dialect remains an optional semantic waist rather than their
mandatory parent.

## Decision

Product-specific mechanism facts and bindings live in a separately versioned
integration outside Fleetd and the reusable compiler repositories. For HTTP,
the integration compiles this explicit path:

```text
native HTTP + handler bindings + target profile
                    -> Axum service
                    -> Rust source tree
```

The native HTTP dialect is independently expressive. Axum is an implementation
dialect, and the Rust source tree is the terminal artifact. Providers run over
exact neutral invocation and result documents and emit candidates without
filesystem authority.

Fleetd admits only a reviewed source candidate under its content digest. The
generated module owns Axum extraction, output wrapping, route registration, and
Utoipa metadata. It calls a handwritten, Axum-free operation port that retains
authorization and durable behavior. The complete compilation bundle remains in
the external integration. It identifies installed semantic packages by content
digest, exact provider and independent attester artifacts, loader-derived
offers, admitted source observations, and the authority for each selected hop.
Fleetd verifies the terminal source digest and behavior without treating an
integration checkout revision as provenance the bundle does not claim.

Generation happens during contribution, not in Fleetd's build or request path.
Fleetd has no dependency on GOOIR facts, plans, providers, conformance types, or
runtime code. OpenAPI, the TypeScript client, and the served bundle continue to
derive downstream from the admitted Rust adapter.

The first admitted slice is protected `GET /v1/agents`, compiled from the
Fleetd operation port at
`cf00d81831128a1ea45006535347438ffb53a3e5`. Its current qualification replay
used `gooir-fleetd-http` checkout
`d9b918a2679d603d60c2787058064a43ef24da13` as reproducibility context. The
CompilerDriver-based bundle deliberately claims no GOOIR or `gooir-http` source
revision. Its output is byte-for-byte identical to the already-admitted module,
so the replay required no source re-admission or runtime change.

## Consequences

- Mechanism semantics become reusable facts instead of framework macro input.
- Fleetd's product logic remains ordinary handwritten Rust and is testable
  without a server.
- Generated source is reviewable, compile-time only, and replaceable without a
  semantic runtime migration.
- Contributors must regenerate in the external integration, admit the exact
  candidate, and then regenerate Fleetd's downstream artifacts.
- The migration is incremental. Handwritten adapters remain until each route
  has an explicit contract, binding, and behavioral proof.
