# ADR 0038: One-shot ACP permission requires an OS boundary

- Status: accepted
- Date: 2026-08-29

## Context

[ADR 0034](0034-os-level-harness-sandboxing.md) separates consent from
containment: a harness permission prompt is evidence of intent, while the
operating system bounds what the process can actually reach. Fleetd originally
cancelled every ACP permission request because it had no independent boundary.
That made mutation-capable harnesses unusable, but replacing cancellation with
command matching would only move trust into strings supplied by a vendor
adapter.

ACP already carries the semantic choice Fleetd needs. A permission request has
typed options such as `allow_once`, `allow_always`, and rejection. Option IDs
are adapter-owned and may change. Tool titles, raw command input, HTTP status
codes, and human-readable labels have no portable Fleetd meaning.

## Decision

Fleetd supports two controller permission policies:

- `deny`, the default, cancels every permission request;
- `allow_once` selects exactly one ACP option whose typed kind is
  `allow_once`.

`allow_once` is valid only when the plugin process group is launched inside an
operator-declared OS sandbox. A configuration that combines one-shot approval
with an unsandboxed plugin fails before startup.

The controller never interprets the tool title, command, path, URL, or
adapter-specific option ID. It selects the typed ACP meaning, uses the exact ID
carried by that request, and fails closed when the option is missing or
ambiguous. It never selects `allow_always`.

The effective sandbox digest and permission policy participate in native
session compatibility. Changing either rotates the compatibility generation;
Fleetd does not resume a session opened under different authority.

The durable permission event includes the bounded controller resolution. This
records request and decision together without inventing a second transcript or
claiming that the harness reported more than it did.

## Consequences

One logical permission rule can serve Claude Code, OpenCode, and future ACP
adapters without Fleetd learning their command dialects. The policy is small
because the sandbox bears the safety claim: a permitted action can mutate only
the declared writable roots even if its description is misleading or absent.

One-shot does not mean one turn and does not create standing authority. A
harness must request each permission again; Fleetd resolves each request from
its typed options. Provider access and any brokered Fleetd capability remain
separate grants.

The first implementation uses macOS Seatbelt. It grants recursive read/write
only to declared writable roots, recursive read only to explicit runtime roots,
and exact-literal ancestor reads needed for executable resolution. Network is
either denied or allowed outbound as a whole. Destination allowlisting, Linux
support, and Windows support remain gaps.

## Rejected alternatives

**Match commands, paths, or tool names.** These are vendor dialects and are not
portable authority. Equivalent operations have multiple spellings, while an
innocent label can hide a different action.

**Accept `allow_always`.** A standing capability must be registered and
auditable as desired state. A model-time permission option is not that
registration.

**Trust the harness's own sandbox or permission implementation.** Fleetd cannot
base a boundary on the process it is trying to bound.

**Treat `allow_once` as safe without an OS sandbox.** Consent limits how often
the adapter may proceed, not what a buggy or compromised process can reach.
