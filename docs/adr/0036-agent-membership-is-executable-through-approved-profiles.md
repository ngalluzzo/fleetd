# ADR 0036: Agent membership is executable through approved local profiles

- Status: accepted
- Date: 2026-08-28

## Context

Fleetd already had the durable collaboration substrate: agent identities,
channels, membership, immutable messages, reliable inboxes, and continuous
harness workers. What it did not have was the product connection between them.
An operator could add an agent to a channel and still had to leave the product,
copy a worker file, insert the stable ID, and supervise a separate process.
Membership described who could converse but did not make that participant
present.

A browser implementation that accepts an executable, arguments, environment,
model credentials, or arbitrary tool configuration would close that gap by
turning the operator bearer already held by the page into local code execution.
Putting harness or model semantics in the daemon would instead violate the
plugin boundary and make Fleetd responsible for every integration it is meant
to compose.

There was also a product trap: solving lifecycle by introducing assignments,
task state, or a workflow graph would prescribe how collaborators must work.
The goal is the opposite. A channel is the durable shared context; members
converse, address one another, and decide what happens next.

## Decision

**A stable agent identity has durable desired execution, resolved only through
a machine-private catalog of approved runtime profiles. Channel membership
makes that identity available to the conversation; it does not define a
workflow.**

The durable configuration is execution-owned and contains only:

- the stable `agent_id`;
- a bounded `profile_id` reference;
- bounded standing instructions;
- `running` or `stopped`; and
- a monotonic revision used as an explicit restart fence.

The profile catalog is an owner-only local file. It owns the executable,
arguments, plugin configuration, model route, directories, message-kind
acceptance, tool grants, timeouts, and optional observability. A profile may not
set the stable agent ID or standing instructions; the supervisor injects those
from durable desired state after resolving the approved profile. Unknown
profile IDs execute nothing.

The browser receives only profile IDs, labels, and descriptions from its native
host. It may configure, stop, or restart an agent through operator-only HTTP
operations, but no HTTP request can carry launch details. The native host starts
one `fleetd worker supervise` reconciler for the authoritative database. A
database-adjacent process lock prevents duplicate supervisors on one machine.

Execution is agent-global because the existing reliable inbox and seat
projection are agent-global. The worker continues to keep one native harness
session per channel, so conversational memory remains channel-scoped. Standing
instructions follow the identity wherever it participates. A future need for
channel-specific guidance should arrive as channel-visible messages or a
separately versioned adapter contract, not by duplicating workers for one
identity.

## Consequences

Adding an agent and activating it becomes one product journey. Restart survives
page, daemon, and desktop-host replacement because desired state and its
revision are durable. Manual `worker run` remains available for qualification
and diagnostics, but is no longer required for ordinary desktop operation.

Fleetd still does not understand a model, harness, skill, role, task, plan, or
workflow. Those remain private profile configuration, standing natural-language
instructions, opaque message contracts, or external integrations. The kernel
still has six concepts; desired execution lives above it.

The catalog is powerful local authority and must be protected like other
owner-only operational files. A stolen operator credential can choose among
already-approved profiles and change instructions, but cannot select a new
executable or expand tools. A stolen or malicious catalog can run what the local
user approved by installing it; the browser cannot create one.

One limitation is deliberate: profile discovery is native-host supplied. An
ordinary browser connection without a trusted host may inspect existing
configuration but receives no approved choices and cannot create a runnable
one. That is the honest boundary until remote enrollment and encrypted worker
transport exist.
