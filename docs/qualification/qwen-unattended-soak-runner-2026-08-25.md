# Qwen unattended soak-runner qualification — 2026-08-25

## Scope

This record qualifies the standalone `fleetd-soak` runner against the real
two-seat OpenCode/Qwen composition. It exercises a new exact workload through
Fleetd's public API, correlates all managed invocations through immutable
message causation, waits for operational settlement, captures raw MLX telemetry
before and after the workload, and atomically publishes one report.

It is a bounded qualification run, not the full-night M2 soak. It qualifies
transport and evidence collection, not the application payload contract.

## Artifact identity

- run ID: `qwen-unattended-2026-08-25-02`;
- plan SHA-256:
  `76c7edd589040536e23452971f3d36785e07793233c0c90bb755f407c8cf1a64`;
- report SHA-256:
  `721f0aacfe4b1498426842e4479cab3d61b47692ac0c60b252022f20cc45a11a`;
- report schema: 1;
- result: passed in 191.177 seconds;
- polling interval: 500 ms;
- required observer: `http://127.0.0.1:18082/metrics`, limited to 1 MiB.

The local raw report remains at
`.fleetd/soak-qwen-2026-08-24/report-2026-08-25-02.json`. The report embeds the
sanitized exact workload declaration and contains neither bearer values nor
credential-file paths.

## Exact composition

- two `fleetd.harness.opencode` 0.1.0 worker seats;
- OpenCode 1.4.0 through the typed ACP plugin interface;
- local `/Users/ngalluzzo/Models/qwen3.8-27b-8bit` route;
- `mlx-vlm` 0.6.15 and MLX 0.32.1 with the 8-bit MTP draft model;
- continuous batching enabled with one server sequence;
- Qwen3 Coder tool parser, 262,144-token effective context, and APC enabled;
- exact inbound kinds: A accepted `loop.start` and `loop.reply`; B accepted
  `loop.delegate`.

Both native sessions were adopted rather than recreated. A retained binding
generation 1 at owner epoch 4; B retained binding generation 1 at owner epoch
3. Both ended ready with `runtime_claimed` persistence.

## Causal exchange

| Seq | Message | Sender → recipient | Kind | Correlation | Causation |
| --- | --- | --- | --- | --- | --- |
| 15 | `b3b1ab65-f83c-4ebe-8382-bf73138d58dd` | upstream → A | `loop.start` | none | none |
| 16 | `82dd3835-dbb2-43a5-aa9d-938515e786ba` | A → B | `loop.delegate` | seq 15 | seq 15 |
| 18 | `bf085ece-3e65-43c4-a839-d672b11062ab` | B → A | `loop.reply` | seq 15 | seq 16 |
| 19 | `b4908b0a-c0d5-47fc-a232-987492b0abd6` | A → upstream | `loop.final` | seq 15 | seq 18 |

The runner did not infer these relationships from a time window. Each bounded
observation exposed its source and result message IDs, and the runner selected
only observations whose source was causally descended from the exact seed.

## Fleetd operational evidence

| Turn | Source | Result | Generation | Duration | Events | Assistant | Tool |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| A delegates | seq 15 | seq 17 | `473f372a-…` | 22.901 s | 94 | 90 | 3 |
| B replies | seq 16 | seq 20 | `36170afa-…` | 128.383 s | 120 | 116 | 3 |
| A finalizes | seq 18 | seq 21 | `473f372a-…` | 128.182 s | 93 | 89 | 3 |

All three invocations ended `end_turn`, `outcome_known`, quiescent, and
`runtime_claimed`. There were zero retries, zero blocks, and zero unresolved
delivery blocks. Both plugin generations stopped gracefully after the run.

The final message appeared before B and A had finished draining their
post-tool responses. The runner correctly withheld its pass verdict until all
three exact observations were terminal.

## Authoritative model-server telemetry

The MLX summary advanced by exactly six completed requests, 93,404 prompt
tokens, and 1,126 completion tokens during the evidence window. The six
requests were three tool-call completions and three post-tool terminal turns.

| Metric | Observed |
| --- | ---: |
| Weighted decode throughput | 29.135 tokens/s |
| Mean per-request decode throughput | 27.790 tokens/s |
| Decode range | 23.253–31.786 tokens/s |
| Mean TTFT | 39.847 s |
| Maximum TTFT | 78.591 s |
| Peak model-server memory | 123.734 GB |
| Model-server request failures during window | 0 |

The first resumed A request prefetched 15,777 prompt tokens at 5,906 tokens/s
and reached first token in 3.571 seconds, consistent with a strong prefix-cache
reuse. Later uncached prefills were approximately 418–425 tokens/s.

Decode remained stable while latency grew. With one server sequence, a seat can
publish its peer message before its post-tool turn is terminal; the next causal
seat then submits work while the previous seat still needs a follow-up request.
Those requests queue. The last two requests each had approximately 78 seconds
of TTFT despite normal decode rate. This makes `max_num_seqs = 1`, rather than
Fleetd dispatch or model decode, the dominant throughput constraint for this
composition.

## Semantic boundary

Qwen again selected the correct tool operation, recipient, kind, and lineage,
but encoded all three requested object payloads as JSON strings containing the
objects. Fleetd and `fleetd-soak` preserved those values unchanged. The run
therefore passes its declared operational contract while typed payload
conformance remains unqualified and belongs to an external contract-aware
validator.
