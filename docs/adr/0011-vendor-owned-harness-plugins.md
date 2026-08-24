# ADR 0011: Harness launch policy belongs to vendor-owned plugins

- Status: accepted
- Date: 2026-08-24
- Supersedes: the single generic production-driver decisions in ADR 0005 and
  ADR 0009

## Context

The first ACP implementation combined two different kinds of reuse in one
executable: typed ACP protocol translation and harness launch policy. Its
runtime environment allowlist consequently named `CODEX_HOME`, `DSH_HOME`, and
`NO_BROWSER`.

The first OpenCode dogfood attempt made the coupling explicit. OpenCode ACP
does not expose per-session model selection, so a correct seat must select its
model through OpenCode-owned configuration. Adding `OPENCODE_CONFIG` to the
generic driver would turn every new harness into another central conditional
and make the reusable layer the owner of vendor behavior.

That is the opposite of fleetd's plugin thesis. A capability contract may be
shared; launch policy and integration-specific configuration must remain
replaceable.

## Decision

ACP protocol translation and process-group containment are a policy-free Rust
library. The library knows the typed `harness.acp` contract, ACP v1, ordered
turn evidence, session operations, and process ownership. It does not own a
catalog of harnesses or environment-variable names.

Each harness integration is a separately identified plugin executable. A
plugin owns:

- its fleetd plugin ID and version;
- its strict configuration schema;
- expected native runtime identity and arguments;
- the exact environment names granted to that runtime;
- conversion of typed operator choices into native configuration; and
- the material used to derive its effective profile digest.

`fleetd.harness.opencode` is the first implementation of this shape. Its
configuration accepts an exact OpenCode executable/version and a typed
`provider/model` route. It constructs `OPENCODE_CONFIG_CONTENT` internally and
derives the profile digest from the effective model, executable content, and
non-secret launch settings. Arbitrary environment maps and credential fields
are rejected. Provider authentication remains OpenCode-owned state reached
through its explicit home directory; fleetd neither stores nor forwards raw
provider keys.

The `fleetd.acp-reference` executable remains a development and qualification
fixture. It accepts a generic ACP runtime description but permits only portable
process settings. It is not the production integration point for Codex, DSH,
OpenCode, or future harnesses.

The worker selects a plugin by exact identity and negotiates only
`harness.acp` v1. It contains no OpenCode, Codex, DSH, model, or vendor
conditionals.

## Consequences

Adding a harness means adding a plugin package, not editing the worker or ACP
host. Vendor changes can ship and qualify independently while session fencing,
turn draining, and failure semantics remain shared.

Some code is intentionally repeated at the configuration edge: each plugin
must state its own strict inputs and grants. Shared code belongs below that
edge only after two implementations demonstrate identical semantics.

Codex and DSH must move from the historical reference-driver qualification to
their own plugin identities before fleetd treats them as deployable seats.
OpenCode provides the first production-shaped proof; a second vendor plugin is
still required before the shared host/plugin authoring surface is stable.
