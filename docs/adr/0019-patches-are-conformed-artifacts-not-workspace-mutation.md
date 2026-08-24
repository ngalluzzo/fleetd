# ADR 0019: Patches are conformed artifacts, not workspace mutation

- Status: accepted for experimental dogfood
- Date: 2026-08-24

## Context

Repository inspection proved that a model can answer bounded questions about
one exact revision while Fleetd independently validates the evidence boundary.
The next useful step is change production. Giving a harness a generic writable
workspace and treating its final state as the result would collapse several
different capabilities: proposing a change, applying it, testing it, reviewing
it, committing it, and authorizing a merge. It would also make the provider's
ambient filesystem state part of the protocol.

A patch is a narrower semantic boundary. It can be content-addressed, checked
against an exact base, inspected without trusting the provider, and passed to
later test, review, or publication capabilities without granting those powers
to the authoring turn.

## Decision

Define `propose_repository_patch@0.1.0` as a specialization of capability work:

- one complete change brief binds an exact repository identity, base revision,
  path scope, objective, acceptance criteria, and constraints;
- one exact configured semantic provider returns an unverified patch artifact
  claim through the existing structured-attempt boundary;
- the adapter proves a clean exact checkout before dispatch and instructs the
  provider not to mutate it;
- the deterministic suite applies the candidate only to a temporary Git index,
  using Git itself for patch parsing, applicability, path enumeration, modes,
  binary detection, and canonical diff generation; and
- successful conformance produces a content-addressed patch artifact without
  modifying the worktree, real index, branch, or repository history.

Conformance establishes patch syntax, applicability, scope, regular-text mode,
canonical bytes, and provenance. The brief's natural-language criteria remain
claims for separate build, test, analyzer, and review capabilities.

## Consequences

- A provider can contribute a real repository change without receiving commit,
  push, approval, merge, or Fleetd messaging authority.
- Patch authoring and patch consumption share exact facts and evidence but do
  not share ambient mutable state.
- Git remains the authoritative mature implementation; Fleetd does not parse or
  apply unified diffs itself.
- A failed, timed-out, malformed, out-of-scope, binary, or mode-changing attempt
  produces no conformant artifact.
- Review can later bind the exact base plus patch digest, and publication can
  require that reviewed identity rather than a branch name.
- The first three real provider attempts produced no candidate, so the contract
  remains experimental and the provider is explicitly unqualified.

See the [repository-patch contract](../contracts/repository-patch-v1.md) and
[first provider qualification attempt](../qualification/repository-patch-provider-2026-08-24.md).
