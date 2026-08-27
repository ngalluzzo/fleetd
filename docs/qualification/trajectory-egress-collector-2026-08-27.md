# Trajectory egress collector qualification — 2026-08-27

## Scope

This checkpoint exercises the ADR 0028 live sink against a real
OpenTelemetry collector. It proves the span projection, the redaction levels,
and the loss accounting on one real managed turn. It does not qualify a vendor
harness, a production collector deployment, or the durable half of ADR 0028,
which needs no Fleetd code and was not exercised here.

The run used:

- `otel/opentelemetry-collector` 0.159.0 in Docker, OTLP/HTTP on loopback
  `4318`, with both the `debug` and `file` exporters;
- `fleetd.acp-reference`, the development-only reference plugin;
- a mock ACP agent emitting one `agent_thought_chunk`, one `plan`, a
  `tool_call` and its `tool_call_update` under one `toolCallId`, and one
  `agent_message_chunk`;
- the semantic-neutral envelope adapter and `worker run --once`.

Each level ran against an isolated database, loopback daemon, and two agent
identities created solely for this qualification. Every model-authored string
in the mock carried a unique marker so leakage could be searched for rather
than inspected by eye.

## What the collector received

One `invoke_agent` root span, one `execute_tool` child under it, both in one
trace. Trace identity derived from the source message id, the documented
fallback when a root request carries no `correlation_id`.

```
SPAN execute_tool   parent=7f03a4d166a4868d
  gen_ai.operation.name = execute_tool
  gen_ai.tool.name      = read
  fleetd.tool_call_id   = call-egress-1
  fleetd.tool_status    = completed
  EVENT tool_call        {fleetd.event_seq: 3}
  EVENT tool_call_update {fleetd.event_seq: 4}

SPAN invoke_agent   parent=<root>  status=Ok  duration=5ms
  fleetd.execution_certainty   = outcome_known
  fleetd.observed_event_count  = 5
  fleetd.stop_reason           = end_turn
  fleetd.session_quiescent     = true
  EVENT reasoning {fleetd.event_seq: 1}
  EVENT plan      {fleetd.event_seq: 2, fleetd.plan_entries: 2}
  EVENT assistant {fleetd.event_seq: 5}
```

Both tool updates folded into one child span rather than two, which is the
grouping the contract specifies. Resource carried `service.name=fleetd`,
`fleetd.agent_id`, and the operator's own `deployment.environment`; scope was
`fleetd.trajectory 0.1.0`.

## Redaction

At `content: "metadata"` all four planted markers appeared zero times in
everything the collector received. At `content: "full"` each appeared exactly
once, in `fleetd.content` for reasoning and assistant text and in
`fleetd.tool_title`, `fleetd.tool_input`, and `fleetd.tool_output` for the tool
call. The dial is therefore load-bearing in both directions: the default level
withholds model and user text, and the explicit level does emit it.

## The defect this run found

The first attempt exported nothing while reporting `accepted=7 dropped=0
exported=0 failed=0`. `opentelemetry-otlp`'s default feature set selects
`reqwest-blocking-client`, whose client builds its own runtime and panics when
dropped inside an async context. The drain task died on its first export; the
sender side kept accepting offers, so every counter looked healthy.

Two fixes. The crate now takes `default-features = false` with the async
`reqwest-client`. And `flush` reports an unacknowledged flush at `warn` instead
of discarding the timeout, so a dead or stalled drain task can no longer
present itself as a clean retirement. A unit test drops the receiver and
asserts the sink reports rather than swallows it.

No in-process test would have caught this: every assertion up to that point
was on `SpanData` before it reached a transport, and the transport was the
thing that was wrong.

## Limits

One turn per level, one mock harness, one collector on loopback. Not
exercised: a real vendor harness, a remote `https` collector, a `headers_file`
credential, a full queue under real load, sustained volume, or a parked turn
reaching the collector as a failed span — the last is covered only by unit
test.
