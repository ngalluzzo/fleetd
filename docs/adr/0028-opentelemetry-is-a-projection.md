# ADR 0028: OpenTelemetry is a projection, never the evidence record

- Status: accepted for experimental dogfood
- Date: 2026-08-27

## Context

Fleetd's durable record answers what happened, in what order, and whether the
outcome is known. It does not answer what the agent was thinking. That gap is
deliberate: `record_invocation_event` receives every `TurnEvent` with its
`classification` and `raw_update`, folds it into fixed counters, a latest-event
digest, and a chain digest, and retains no content. ADR 0020 chose that so the
control database would not become a second unbounded transcript store, and
`a8f7041` finished the collector path by giving `/v1/plugin-generations` and
`/v1/invocation-observations` keyset cursors, so an external reader can now
tail both losslessly.

So the reasoning, tool arguments, and intermediate plans exist for the duration
of one `drain_turn` loop and then exist nowhere Fleetd can reach. An operator
who wants to understand how a task was approached must open the native harness
and correlate by hand.

OpenTelemetry is the obvious candidate, and the obvious way to adopt it is
wrong. It is lossy by construction: batch processors drop on overflow, there is
no acknowledgement, no idempotency key, and no replay. A span is also opened
and closed inside one process, while a Fleetd invocation survives daemon death,
parks for hours under `outcome_unknown`, and resumes under a higher owner
epoch. Live instrumentation therefore loses the trace exactly at the crash ADR
0008's fence exists to survive. Nothing Fleetd promises — attributable
evidence, proven ordering, at-least-once settlement — can rest on it.

Dismissing it is equally wrong, because it owns four things Fleetd would
otherwise invent. The GenAI semantic conventions already name agent work as
`invoke_agent` / `chat` / `execute_tool` with operation names that land close
to one-for-one on the classifications `event_increments` already counts. W3C
`traceparent` is a specified, widely implemented version of the correlation the
envelope carries as `correlation_id` and `causation_id`. Span links express a
restart-adopted attempt without pretending it is the same attempt. And the
Collector gives the operator storage, retention, redaction, and a viewer that
Fleetd does not have to build or host.

Maturity constrains how far that trust can go. As of mid-2026 every `gen_ai.*`
attribute, span, metric, and event is badged Development, and the semantic
conventions repository split in June 2026. In `opentelemetry-rust` 0.32, Logs
and Metrics are stable but Traces and the OTLP trace exporter are Beta, every
crate is pre-1.0, and breaking changes land in minor releases.

## Decision

Fleetd treats OpenTelemetry as an export projection with two sinks of different
loss tolerance, and takes no dependency on an OpenTelemetry crate for the
durable half.

**The durable projection runs outside the process.** An external collector
tails the two evidence listings through their public cursors, derives a trace
identity from the run's `correlation_id` and a span identity from
`invocation_id`, sets explicit start and end timestamps from `started_at_ms` and
`terminal_at_ms`, and links attempts that share a `binding_id` across owner
epochs. The live sink derives both the same way, so the two halves land in one
trace rather than two disconnected views of one run. Because it is a pure
function of durable rows, it is idempotent and replayable, and a collector that
was offline for a week emits the same spans when it returns. This needs no new
Fleetd code, no contract change, and no privileged internal data path.

**The live trajectory sink is explicitly lossy and lives at the one place the
content exists**: beside `record_invocation_event` in the `drain_turn` loop,
before the fold discards `raw_update`. It is off by default and configured per
worker seat rather than globally. It may drop, and dropping must never fail a
turn, delay a settlement, or influence a fence. Fleetd persists nothing raw
either way; the trace backend becomes the operator's optional transcript store,
with its own retention and redaction, which is exactly the sink ADR 0020
reserved room for.

**Model and user text is never exported unless it is named.** Prompts and
reasoning are the sensitive material ADR 0020 declined to store, and exporting
them moves them off the machine. The sink's default level carries the shape of
the work — tool names, statuses, plan sizes, stop reasons — and no model or user
text; assistant text, reasoning, and tool arguments require an explicit
per-seat level, never a consequence of enabling tracing. The seat
configuration, span shape, attribute mapping, and bounds are the
[trajectory egress contract](../contracts/worker-trajectory-egress-v1.md).

**Fleetd adopts the vocabulary, not the schema.** `gen_ai.*` spellings are used
where they exist; invocation, binding, owner epoch, and fence identities remain
Fleetd-namespaced attributes. Because those conventions are still in
development, the mapping is versioned with the sink and never with the API
contract or the database.

**Nothing in the control plane may read a span.** OpenTelemetry is not the live
operator-event subscription M4 still owes, not a source for `fleetd status`,
and not an input to any settlement, parking, or recovery decision.

## Consequences

The two questions get two systems matched to their loss tolerance. "Prove what
this invocation did" stays a durable read with a chain digest. "Show me how
this task was approached" becomes a trace the operator may keep, sample, or
discard.

The daemon's dependency tree stays free of a Beta trace SDK. A breaking minor
release in the OpenTelemetry crates can only affect a sink, never `bin/ci` or
the daemon, and an operator running no collector loses nothing they have today.

The record and the trace can disagree, and the disagreement is detectable
rather than silent: `event_count` and `event_chain_digest` state how many
events occurred and in what order, so a trace missing spans is visibly
incomplete. The durable row remains authoritative on every question the trace
also appears to answer.

The plugin is not involved. Plugins launch with an empty environment and no
ambient variables, so standard `OTEL_*` autoconfiguration cannot reach them,
and a collector authorization header would be a credential handed to a plugin.
The worker already receives every `TurnEvent`, so exporting from there needs no
change to `fleetd.harness-acp@0.1.0`.

Longitudinal questions — how an agent's approach changed over a month — are not
tracing-backend questions; those are built for short traces and weeks of
retention. The same sink can fan out to a columnar store, and the tailable
listings are a replayable source for backfilling one.

Deliberately not here: retention and redaction policy for whatever backend the
operator runs, a Fleetd-owned collector, and per-tool child spans derived from
the durable record. The counters cannot produce that tree, and adding per-event
rows to make them able to would reverse ADR 0020.
