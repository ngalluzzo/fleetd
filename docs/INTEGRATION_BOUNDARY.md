# External semantic integration boundary

Fleetd is an agent coordination runtime. It does not embed a semantic compiler
or treat its plugin model as a semantic capability system.

Every supported relationship with GOOIR or another semantic compiler passes
through an independently versioned integration package outside both cores:

```text
Fleetd public artifacts
        │
        │ lift
        ▼
Fleetd-native operational facts
        │
        │ explicit evidence bridge
        ▼
semantic capability offers

linked semantic implementation deployment
        │
        │ lower
        ▼
Fleetd public API calls or worker configuration

native mechanism facts + implementation bindings
        │
        │ compile outside Fleetd
        ▼
content-addressed ordinary source candidate
        │
        │ independent admission
        ▼
Fleetd mechanism adapter
```

## Lift

A lifter may inspect public, attributable Fleetd artifacts such as:

- the OpenAPI document and immutable messages;
- a plugin lifecycle manifest or qualification record;
- an effective harness profile with executable and configuration digests;
- a generated operator-surface artifact.

The native result describes only what was observed: identities, interface
versions, digests, methods, limits, evidence scope, and uncertainty. Merely
speaking a Fleetd plugin interface does not establish a semantic capability.

## Bridge

A semantic bridge may create an implementation offer only when an explicit
mapping and independent qualification bind the effective composition to the
capability. The composition can include the plugin, harness, model, tools,
configuration, and external services; the plugin executable alone is not
assumed to own the meaning.

Unknown configuration, missing evidence, changed digests, or incomplete
qualification must remain unknown or partial. Names and method shapes are not
semantic proof.

## Lower

A lowering starts from an already-linked implementation deployment, not an
abstract capability. It may produce Fleetd agent registration, plugin desired
state, worker configuration, channel membership, and opaque message envelopes
using Fleetd's public contracts.

Fleetd validates only its own operational contract. It does not re-plan the
capability graph, choose an implementation, or validate semantic results.

## Build-time source generation

A product integration may pair independently authored HTTP, CLI, or MCP facts
with an implementation dialect and emit an ordinary source tree. The external
integration owns the facts, compilation bundle, installed package documents,
provider and attester artifacts, loader-derived offers, source observations,
and per-hop authority records. Fleetd admits only the selected source bytes and
a qualification record, then compiles and exercises them through its normal
tests. An integration checkout may be recorded as reproducibility context, but
it is not source provenance unless the bundle itself establishes that claim.
The generated adapter may extract inputs, call an explicit product port, wrap
outputs, register the route, and declare contract metadata. It may not own
authorization policy, durable state transitions, or product decisions.

Generation is a contribution-time operation. Fleetd does not load GOOIR,
interpret semantic facts, or invoke providers at build time or runtime.

## Repository rule

The integration package does not live in the Fleetd runtime repository or the
semantic compiler core. Fleetd publishes operational artifacts and explicit
operation ports; the compiler publishes semantic artifacts; the integration
depends on both and neither core depends on it. An admitted generated source
file is a terminal artifact, not either side of the integration package.

Any proposal that adds semantic fact, offer, invocation, candidate,
conformance, or planner types to Fleetd bypasses this boundary and must be
rejected. Any proposal that adds Fleetd sessions, leases, workers, plugin
processes, or message authority to a semantic core must likewise be rejected.
