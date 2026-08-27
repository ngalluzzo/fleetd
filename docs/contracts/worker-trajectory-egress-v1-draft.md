# Worker trajectory egress v1 draft

Status: proposed. Nothing here is implemented, and `fleetd worker run` currently
refuses an `egress` block as an unknown field. The decision this contract serves
is [ADR 0028](../adr/0028-opentelemetry-is-a-projection.md).

## Purpose

Trajectory egress is the optional, explicitly lossy sink ADR 0028 places at the
one point where harness reasoning still exists: the `drain_turn` loop, beside
`record_invocation_event`, before the fold reduces an update to counters and a
digest. It exports what the durable record deliberately does not keep.

It is not evidence. Nothing an operator is promised may depend on it, and no
control-plane decision may read it. The durable `invocation_observations` row
remains authoritative on every question a span also appears to answer.

## Seat configuration

Egress is configured on one worker seat and is absent by default. With no
`egress` block, no exporter is constructed and no queue exists.

```json
{
  "schema_version": 2,
  "egress": {
    "schema_version": 1,
    "kind": "otlp_http",
    "endpoint": "http://127.0.0.1:4318/v1/traces",
    "headers_file": "/absolute/path/.fleetd/collector.headers",
    "content": "metadata",
    "classifications": [
      "assistant",
      "reasoning",
      "tool",
      "plan",
      "permission",
      "unknown"
    ],
    "resource_attributes": {
      "deployment.environment": "dogfood"
    },
    "max_attribute_bytes": 4096,
    "queue_capacity": 1024,
    "export_timeout_ms": 5000,
    "shutdown_flush_ms": 2000
  }
}
```

The inner `schema_version` accepts 1 only, and it versions the semantic
convention mapping rather than the file. Every `gen_ai.*` attribute, span, and
metric is still badged Development upstream, so that mapping will change; when
it does, this contract gains a v2 and the worker file, the API contract, and the
database are untouched.

`kind` accepts `otlp_http` only: OTLP over HTTP with protobuf encoding, which
needs no gRPC stack in the daemon. An `otlp_grpc` or airgapped `file` variant is
a later tagged variant, not a reshaping.

## Field rules

`endpoint` is required and absolute, and is refused unless its host is a
loopback literal or its scheme is `https`. A plaintext remote collector would
carry agent reasoning across a network Fleetd has already declined to trust for
its own listeners. The rule follows the shape of
`acp-host::validate_loopback_mcp_url` — explicit host, no embedded credentials,
no query, no fragment — but is a separate check, because that one exists to
refuse anything that is not exactly `127.0.0.1` and this one must also admit a
remote `https` collector.

`headers_file` is optional and absolute, and its mode is verified to be
owner-only before it is read. That is stricter than the existing token-file
path, where `auth::secure_file_permissions` sets `0o600` on a file Fleetd
writes itself; a headers file is supplied by the operator, so Fleetd cannot
have set its mode and must check it. Each line is one `Name: value` pair.
Values are never logged, never placed in argv, and never written to a durable
row or a span attribute. Inline headers are not accepted at any version of this
contract.

`content` selects what may be lifted out of `raw_update`:

- `none` — nothing. Spans carry timing, ordering, and counts only.
- `metadata` (default) — tool name, tool call id, tool kind and status, plan
  entry count, stop reason. Never model or user text, never tool arguments or
  tool output.
- `full` — assistant text, reasoning text, tool arguments, and tool output,
  each truncated to `max_attribute_bytes`.

`metadata` is the default because writing an `egress` block is already the
explicit act, and this level cannot carry model or user text. `full` must be
named. At `full`, an `unknown` update exports its raw JSON, since an
unrecognized update has no fields to select.

`classifications` is an optional allowlist over the vocabulary
`event_increments` already counts, defaulting to all of them. It is orthogonal
to `content`: removing `reasoning` here drops those events entirely, which is a
stronger control than redacting them. `unknown` defaults on because an
unrecognized update is the one an operator most needs to see.

`resource_attributes` is a flat string map, at most 32 entries of at most 256
bytes each. Fleetd sets `service.name`, `service.instance.id`, and the seat
identity itself; an operator key colliding with a reserved key is refused
rather than merged.

Bounds, all validated in `validate_config` before a plugin starts, so a
malformed block cannot be discovered after a turn is armed:
`max_attribute_bytes` 1..=65536, default 4096, a second and tighter bound than
`turn.max_captured_output_bytes` because this one governs what leaves the
process; `queue_capacity` 1..=65536, default 1024; `export_timeout_ms`
1..=30000, default 5000; `shutdown_flush_ms` 0..=30000, default 2000.

## Span shape

One `invoke_agent` span per managed invocation. It opens when the arming
transaction commits — the same moment the `invocation_observations` row is
created — and closes at `record_invocation_terminal`. Its parent is the
envelope's `traceparent` when one is present, and it is a root span otherwise.

Children and events follow what the update actually is:

- `tool` — one `execute_tool` child span per tool call id, opened on its first
  update and closed on its last.
- `permission` — one child span carrying the resolved outcome. It has a real
  begin, end, and result.
- `assistant`, `reasoning`, `plan` — span events on the parent. They are chunks
  of one operation, not operations.
- `usage` — attributes on the parent at terminal, from the same evidence
  `InvocationObservation.usage` records.

Attributes use `gen_ai.*` spellings where they exist: `gen_ai.operation.name`,
`gen_ai.request.model` from the plugin's negotiated model route,
`gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, and
`gen_ai.tool.name`. Fleetd identities stay Fleetd-namespaced:
`fleetd.invocation_id`, `fleetd.agent_id`, `fleetd.binding_id`,
`fleetd.binding_generation`, `fleetd.owner_epoch`, `fleetd.generation_id`,
`fleetd.event_seq`, `fleetd.event_chain_digest`, and
`fleetd.execution_certainty`.

Span status is error when an invocation parks under `outcome_unknown`. That is
the case an operator must find and the case where no result message exists to
find it by.

`fleetd.event_chain_digest` and the event count on the parent are how a reader
ties a trace back to its durable row and detects a trace missing spans the
counters counted.

## Loss is bounded and counted

The queue is bounded and the send is non-blocking. On a full queue the event is
dropped and the sink's own drop counter increments. Export failures are counted
the same way, never retried into a turn's critical path, and never raised as a
`ContinuousWorkerError`.

The fold into `invocation_observations` happens first and the sink observes a
copy, so an unreachable collector cannot delay a settlement, influence a fence,
or fail a turn.

At generation retirement the exporter is flushed for at most
`shutdown_flush_ms` and then abandoned. Shutdown outcome evidence must not
depend on a collector being reachable.

## Where the counters live

The sink owns its counters. They do not join `WorkerReport`.

`WorkerReport` counts settlement — generations, restarts, reservations,
completions, blocks, pre-arm retries, idle polls — and `fleetd worker run`
prints it as its final JSON. That output is what an operator and the
qualification tooling read to learn what a run settled. An absent-by-default
lossy sink's health is not part of that answer, and adding fields to it would
make every consumer of that JSON carry an optional subsystem's concern.

Counters are scoped to the plugin generation, which is how every other
process-lifetime fact here is scoped, and they surface through the log stream at
two moments: a warning on the first drop of a generation, once rather than per
event, and one summary at generation retirement carrying accepted, exported, and
dropped totals alongside the `generation_id` they belong to.

Logs are the surface, not the test seam. The sink is directly constructible with
`queue_capacity` 1, so drop accounting is asserted against the counter rather
than against log output.

Three alternatives are rejected deliberately. Exporting the counters as OTLP
metrics to the same collector is self-defeating, because the drops that most
need reporting are the ones an unreachable collector caused. A column on
`plugin_generations` would make a lossy sink's health into durable evidence,
requiring a forward migration and placing egress in the public API contract,
against the rule that no control-plane read touches the sink. A second report
returned beside `WorkerReport` would change the signature of `run` and give the
CLI a second thing to print for a subsystem most runs do not configure.

## Session compatibility

The egress block does not participate in `worker_compatibility_digest`.
Inbound acceptance does, because it changes which envelopes reach the harness.
Egress changes nothing the harness sees, so enabling it must not rotate a
binding generation and discard native conversational state.

## Why this is not worker schema 3

`WorkerFileConfig` is `deny_unknown_fields` and requires `schema_version` to
equal 2, so an old binary reading a config that carries `egress` fails closed
and names the unknown field. Failing closed is the property that matters: an
old binary never silently ignores an egress directive and leaves an operator
believing they are exporting.

Bumping the outer version would force every existing seat file to be edited for
a feature it does not use, and would buy only a better-worded error for a case
that already fails. This is the same reasoning as the additive evidence
pagination: stay additive when the existing mechanism already prevents the
silent loss.

## Deliberate limits

V1 does not sample. Every accepted event is exported or counted as dropped;
sampling is a collector feature.

V1 emits traces only. It has no metrics or logs signal, and it does not export
the durable evidence rows — an external collector tails
`/v1/invocation-observations` for those, losslessly and idempotently, with no
OpenTelemetry crate in the daemon.

V1 does not buffer durably. A collector that is down loses that window, by
design; the tailable evidence listing is where a lossless reader belongs.

Egress is not an API surface. No HTTP route configures or reports it, and it is
not the live operator-event subscription M4 still owes.

No egress runs in a plugin. Plugins launch with an empty environment and no
ambient variables, so `OTEL_*` autoconfiguration cannot reach them, and a
collector authorization header would be a credential handed to a plugin. The
worker already receives every `TurnEvent`, so `fleetd.harness-acp@0.1.0` does
not change.
