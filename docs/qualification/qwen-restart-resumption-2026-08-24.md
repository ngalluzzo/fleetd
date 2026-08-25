# Qwen continuous restart and resumption qualification — 2026-08-24

## Scope

This checkpoint exercises two continuous OpenCode seats against the local Qwen
route while deliberately replacing seat A between its outbound delegation and
the reply it must consume. It qualifies Fleetd commit
`735f9cfc54fd77d428582bd738d2ce14a084e440` for:

- durable generation start, heartbeat, stop, and shutdown evidence;
- invocation-scoped `fleet.messaging.send` authority and lineage;
- pending delivery while a seat is absent;
- native session adoption under a higher owner epoch;
- bounded per-invocation event summaries and chain digests;
- exact-kind non-consumption by continuously polling seats.

It does not qualify a typed application payload, provider token accounting,
decode throughput, or a multi-hour unattended soak.

## Exact composition

- `fleetd.harness.opencode` 0.1.0 using ACP SDK 2.0.0 and ACP protocol 1;
- OpenCode 1.4.0, executable digest
  `sha256:3d2c79a23f8a17d7ac35c819fba5bfac9393642de51434896adf7887629cc763`;
- plugin profile digest
  `sha256:15802340506dd3d0d54225551fcf3492e835d1c7a04003ef9324170099817380`;
- local route `fleet-local//Users/ngalluzzo/Models/qwen3.8-27b-8bit`;
- `mlx-vlm` 0.6.15 and MLX 0.32.1, with the local 8-bit MTP draft model,
  draft block size 4, and one server sequence;
- worker schema 2 and `fleet.messaging.send` on both seats;
- A accepted only `loop.start` and `loop.reply`; B accepted only
  `loop.delegate`.

A and B have different compatibility digests because their exact inbound
acceptance contracts differ. A's digest remained identical across replacement.

## Procedure and durable exchange

Both workers started before the seed. Upstream sent `loop.start` to A. A
published one delegation to B and completed its first invocation. A was then
stopped cleanly. B published its reply while A was absent; catalog inspection
showed that delivery still `pending`, at attempt 0, with no lease. A was
restarted and consumed the pending reply.

| Seq | Message | Sender → recipient | Kind | Correlation | Causation |
| --- | --- | --- | --- | --- | --- |
| 1 | `909e3713-d753-4e03-9b47-90de99aab4f1` | upstream → A | `loop.start` | none | none |
| 2 | `f8f2bc7a-b257-4b91-ac42-82cc604c55c8` | A → B | `loop.delegate` | seq 1 | seq 1 |
| 4 | `cfa20c62-b3b8-4324-9306-813b69cb998c` | B → A | `loop.reply` | seq 1 | seq 2 |
| 6 | `c94dadbf-872b-4dc5-8704-4f3909162570` | A → upstream | `loop.final` | seq 1 | seq 4 |

Fleetd derived every sender, channel, correlation, causation, and idempotency
key from the active invocation grant. Neither harness received an agent bearer
credential.

## Generation and session evidence

| Seat | Generation | Lifetime | Stop | Shutdown |
| --- | --- | ---: | --- | --- |
| A before replacement | `fccd4da6-7ecd-4030-9f95-a8b4211ae934` | 101.647 s | stopped | graceful, exit 0 |
| B | `2e95f2cf-e708-4ba3-aec5-7fb0fb80ee20` | 228.085 s | stopped | graceful, exit 0 |
| A after replacement | `5bee263b-fc7b-41bc-98c9-c09af07bca8f` | 94.814 s | stopped | graceful, exit 0 |

A preserved binding `0163348f-8c46-4342-87f4-71249151ec4b`, binding
generation 1, and native session
`ses_fc8b7e0ebffe59nPXX8Nx1r12y`. Replacement advanced only the owner epoch
from 1 to 2 and left the binding ready with `runtime_claimed` persistence after
the second turn. B remained at binding generation 1 and owner epoch 1.

## Bounded invocation observations

| Turn | Generation | End-to-end | Events | Bytes | Assistant | Tool | Unknown |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A delegates | pre-replacement A | 51.551 s | 91 | 16,766 | 87 | 3 | 1 |
| B replies | B | 46.995 s | 124 | 19,118 | 120 | 3 | 1 |
| A finalizes | post-replacement A | 39.219 s | 91 | 13,777 | 87 | 3 | 1 |

All three observations ended `end_turn`, `outcome_known`, quiescent, and
`runtime_claimed`. Every row has a distinct SHA-256 event-chain digest. There
were three invocations total, zero retries, zero blocks, and zero unresolved
delivery blocks.

The timing is end-to-end harness latency. It is not decode tokens per second.
The ACP terminal supplied no usage value and no usage update was observed, so
Fleetd correctly records missing usage rather than zero.

## Non-consumption proof

After the final turn, both continuous workers remained live and heartbeating.
The automatic result messages at sequences 3, 5, and 7 all remained `pending`
at attempt 0. Invocation count stayed exactly three. This confirms that a
result outside a seat's declared exact-kind acceptance is neither leased nor
turned into recursive work.

## Semantic boundary exposed

Qwen called the correct MCP operation with the correct recipient, kind, and
lineage, but encoded each requested object payload as a JSON string containing
that object. The final durable payload is therefore a JSON string, not an
object. Fleetd preserved it exactly.

This is not a Fleetd transport failure and must not be repaired by runtime
normalization. `fleet.messaging.send` transports bounded opaque JSON; the
application contract owner must validate the payload and reject or reinterpret
it with explicit evidence. The run therefore passes the operational transport,
replacement, and resumption claims above, while typed payload conformance
remains unqualified.
