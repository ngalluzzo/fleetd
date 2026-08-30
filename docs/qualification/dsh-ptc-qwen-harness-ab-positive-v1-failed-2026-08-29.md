# DSH/PTC vs OpenCode Qwen harness A/B — positive control v1 failed

- Date: 2026-08-29
- Status: **stopped after the first DSH/PTC positive-control arm**
- Positive-control verdict: **DSH/PTC failed**
- Promotion verdict: **not evaluated; no comparative evidence**
- OpenCode positive control: **not run**
- Seq-119 primary replay: **not run and payload not retrieved**

This is a new versioned record. The adjacent
`dsh-ptc-qwen-harness-ab-write-scoped-blocked-2026-08-29` evidence remains
unchanged.

## Outcome

The option-1 input change was frozen successfully. Both fresh evaluation roots
were detached at GOOI commit
`92c4df7edefd8d195afe7944daa05f4248866d0a`, the direct child that first adds
the required runner. A third, independent checkout reproduced the required duel
summary and the expected canonical artifact bytes before either harness was
dispatched.

The DSH/PTC arm produced the exact expected artifact, restored the runner-owned
tracked evidence to `HEAD`, and left no other visible source/config change. It
still **failed the positive control** for four independent reasons:

1. Fleetd's authoritative `work.result/v1` has `status: "failed"`,
   `stop_reason: "host_wall_deadline"`, and
   `runtime_stop_reason: "cancelled"`. The wall interval was 120,013 ms.
2. The frozen prompt required the exact unwrapped runner command. DSH instead
   executed `node .../run-duel.mjs; echo "exit=$?"`. Its own check recorded
   `RAW_STDOUT` with the added `exit=0` line and `EXPECTED_MATCH: false`, but its
   final prose incorrectly claimed that exact stdout matched.
3. DSH's inner `workspace-write` sandbox could not initialize beneath the outer
   Fleetd Seatbelt. The first shell call failed closed with
   `sandbox-exec: sandbox_apply: Operation not permitted`. The model then made
   seven typed escalation requests using `danger-full-access`; Fleetd answered
   each `allowed-once`. The outer `write_scoped` Seatbelt still confined the
   whole process group, but the required DSH workspace-write defense in depth
   was absent.
4. The host deadline interrupted the final response mid-sentence. The durable
   result contains a truncated assistant message even though the adapter marks
   its normalized assistant block complete.

The experiment gate required both positive controls to pass. Consequently the
OpenCode arm was not dispatched, the durable seq-119 payload was not retrieved,
and no primary inference was run. This run provides no evidence that DSH/PTC is
better than OpenCode; under the frozen positive protocol, DSH/PTC regressed.

## Frozen protocol

### Fresh roots and GOOI inputs

The roots were created before dispatch and were identical except for their
arm-specific private Fleetd state paths:

```text
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/runs/positive-v1/dsh/workspace
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/runs/positive-v1/opencode/workspace
```

Both resolved `HEAD` to
`92c4df7edefd8d195afe7944daa05f4248866d0a`. The two command artifacts are
ignored build outputs rather than bytes in that commit; the same independently
hashed copies were staged into both roots before dispatch and explicitly joined
the frozen input set.

| input | SHA-256 |
| --- | --- |
| P2 manifest | `6c9d86109e34db14b7eb8e6172ea507db4164046eedbfbdd414590c33463710d` |
| P2 reference-boundary README | `befc6b685fde8842752949315e6bea1691abbd63676f5e7614cbe0e23843f5b9` |
| P2 value-schema corpus | `6fddb18a99200319ccccdeb6770f65377fe56b446b28d351c08f7a37a773bc83` |
| committed runner | `6aa8dcd73686f12e12c9dc84c59462994437834fdb892088419e9e43d521cb43` |
| Rust command artifact | `6e18ecd59967c89af58ddb9e80554bff7ef56468ee24d90f17f3f324972ce43b` |
| TypeScript command artifact | `13d770e329e18e90890c833e292475db4699eb5bbe74cdaac554a90885f0ee22` |

The required UTF-8 runner command plus one LF was:

```text
node experiments/real-vertical-slice/federated/kernel-duel-v0.3/p2/run-duel.mjs
```

Its SHA-256 was
`3d65b3d64c3a56eb1c45595cb57893805a92d874f2c64ba24179cb6d1af88aef`.

The committed runner's independently verified stdout was exactly:

```text
{"status":"pass","cases":107,"classifications":214,"divergences":0}
```

including one terminal LF. Its checkout-dependent evidence files were restored
after the verifier run. Their committed bytes were:

- `duel-result.json`:
  `3ca132ba093eebdc8505cbc9ffdc8403d05a8499bbedb320afe3babc6546bbeb`
- `DUEL_RESULT.md`:
  `59a1d83ef3ece2826400d136589f0c89c5813b8152475841196af530e702b411`

### Prompt and expected artifact

- prompt fixture:
  `docs/qualification/fixtures/dsh-qwen-positive-v1.prompt.txt`
- prompt size: 3,042 bytes
- prompt SHA-256:
  `4cd9536110e27ba48ffcd566e61c5303e9abb821b6ac81ac76a4c00b176b34c3`
- canonical `{task: prompt}` payload SHA-256:
  `622e46ee562f7c8b8892eefd76e57e734dedd10673b11e548e908b73718da69a`
- expected fixture:
  `docs/qualification/fixtures/dsh-qwen-positive-v1.expected.json`
- expected artifact size: 425 bytes
- expected artifact SHA-256:
  `f9adbad0987217eea433dc75935d0c34a8e9fb66badcad3eaf18abc036c11882`

The expected artifact was derived independently from the committed duel result,
serialized as one canonical JSON line plus LF, and byte-compared with the
fixture before dispatch. The exact prompt bytes were identical inputs for both
arms; OpenCode never consumed them because the DSH gate failed first.

### Harness, backend, and configuration identity

| item | identity |
| --- | --- |
| Fleetd source base | `8af2743b78ac8220d090221440ad1f1a9d7d8935` plus recorded uncommitted qualification changes |
| Fleetd executable | `c781514f1571143c757321eadffb2b572e4f5cb2131e10fde2ffd3761495391f` |
| DSH adapter | `2807ad0540d050705adc1ea430b65ec8ac4a71da3fb03901fd5340089549a99d` |
| OpenCode adapter | `8271927f22a669e8b3114c66ce51531e1ce29c768c8706ef394f920ade21e1b6` |
| DSH source | tag `dsh-v0.1.2-alpha.1`, commit `cd5ef8148158c3a752a658978873241fdf8e2bbc` |
| DSH CLI entry point | `dc23f6c5dd7df8834e3e38bdb9609d77b459834681ae9b7133b417b0c35f3166` |
| DSH composition identity | `2753509caf68d89f7bb6b7dfc449da72c5dfb81be1d7f9b273159fa3d5f41dfb` |
| generated composition bytes | `52649d42d7f6d6568ff202c5e192001bedc4075e3e2d886d8d0995605386b5cd` |
| OpenCode | `1.4.0`, binary `3d2c79a23f8a17d7ac35c819fba5bfac9393642de51434896adf7887629cc763` |
| Node | `v25.9.0`, binary `32e234a5b6bec67d72a016f2baadf7fadf3afd328470b395b73af473fdee0d85` |
| DSH worker config | `5988c7c599143a8358115583d81878ca06a448d327e2bc8382aebaddd1b38f58` |
| OpenCode worker config | `ea3cfc32c272ce9fba9b595fbae7ad3c4ce8d153244b5adac92570769a7f1e5e` |
| DSH effective Seatbelt profile | `sha256:1f343236b4de7f31222938458dea99b2d7f7686c081d9fbb00032252d04093e0` |
| OpenCode effective Seatbelt profile | `sha256:1090e72a970602859535f5bf78213ac9904a57719778a6275e9d7ae4534d0f77` |
| inference launch profile | `sha256:27270216d39378dce1d53771e71eed044de9352feb24f392541873ae6d5b0e6c` |
| Qwen model directory manifest | `0811141b17da265c712aac5373524b15ecbaa2feadd2dbbb06869e5638502282` |
| MTP draft directory manifest | `7fed863214adb93d3b203031c98ce24d70fe441181e30950255a949e10581dde` |

Both arm configurations resolved the same credential-free supervised route:

```text
MLX-VLM 0.6.15
http://127.0.0.1:18082/v1
/Users/ngalluzzo/Models/qwen3.8-27b-8bit
draft=/Users/ngalluzzo/Models/qwen3.8-27b-mtp-8bit kind=mtp block=4
context/KV=262144 max_num_seqs=1 APC=enabled
reasoning=none max_output_tokens=8192
```

The DSH request header proves `maxTokens: 8192` and
`reasoningEffort: "off"`; backend metrics prove `thinking_enabled: false` and
zero reasoning tokens. Neither the DSH request evidence nor the backend metrics
exposed `temperature` or `top_p`. They remain `null`/unobserved; no override was
added for this causal harness comparison.

The worker connected directly to the already supervisor-owned loopback backend
to avoid restarting any unrelated seat. No production backend, model, DSH
source, global package state, GOOI source, or unrelated worker was changed.

## DSH execution evidence

### Durable Fleetd identity

- channel: `83ec66ee-343b-4962-8c9a-7cce365289fc`
- request: seq `121`, message
  `990830a7-85e9-4ce7-83ac-7638b1ca8137`
- result: seq `122`, message
  `b33c3c90-ceff-4c09-afd5-fbc06ff4526a`
- invocation: `f809f490-fecc-4ca3-9696-15e331b98626`
- generation: `8522f24a-eb1c-477f-92ad-bd67f4823165`
- generation runtime profile:
  `sha256:d17c09b108e0f76fb2ca6bbba5621119d0cc1b375ff48b9f401cbda34b57888e`
- generation compatibility:
  `sha256:0e6ef120b8becb49540b92ff46964e7f81f8499baa6c695e01dbf4b2120c8484`
- session binding: `f3918695-ddab-4cb5-83f8-2454df056820`
- DSH session: `6dc05901-688a-4e57-ba40-8f0e7f73fd29`
- observation chain:
  `sha256:dc5c9581e40b125b095e95d20f974e0852333063d84e72d27b1a76f5658a28df`

The durable invocation row says terminal reason `completed` only because a
result message was durably published. The payload inside that result is the
authoritative task outcome and says `status: failed`,
`stop_reason: host_wall_deadline`.

The DSH-native compressed session is retained at:

```text
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/runs/positive-v1/dsh/workspace/.fleetd/dsh-home/sessions/--Users-ngalluzzo-repos-fleetd-.fleetd-qualification-dsh-qwen-write-scoped-2026-08-29-runs-positive-v1-dsh-workspace--/6dc05901-688a-4e57-ba40-8f0e7f73fd29/session.jsonl.zstd
```

It contains 226 records and has SHA-256
`8d8de607ae3abc2397f9e6528841f2d4901496ef152f65a318bda9105cd84622`.
`fleetd transcript` could not render this historical generation because the
current binary reports `MissingInterface fleetd.harness-acp@0.2.0`; the durable
DB observation and raw DSH session were therefore inspected directly.

### Timing and event counts

- reserved: `1788025858367` ms
- dispatch armed: `1788025858444` ms
- first Fleetd event: `1788025881081` ms, 22,637 ms after arm
- artifact tool result: `1788025958131` ms, 99,687 ms after arm
- terminal: `1788025978457` ms, 120,013 ms after arm
- Fleetd events: 35; observed payload: 19,163 bytes
- semantic DSH `run_code` calls: 9
- semantic subtool dispatches: 9
- Fleetd adapter tool events: 18
- typed permission events: 7, all `allowed-once`
- assistant events: 1; reasoning events: 0; usage events: 9
- configured tool budget: 16

The difference between 9 semantic calls and 18 Fleetd tool events is adapter
call/result accounting; it is not 18 model-selected tools.

### Provider metrics

The backend recorded 10 streaming requests: 9 completed with `tool_calls`, and
the tenth failed `stream_closed_before_completion` when Fleetd cancelled at the
wall deadline. Across completed requests it recorded 88,568 prompt tokens,
1,955 generated/completion tokens, and zero reasoning tokens. The failed stream
did not preserve token counts.

| completed request | prompt | completion | TTFT s | elapsed s |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 8,324 | 90 | 19.942 | 22.501 |
| 2 | 8,756 | 125 | 1.345 | 5.373 |
| 3 | 9,014 | 318 | 0.968 | 9.883 |
| 4 | 9,363 | 303 | 1.187 | 9.613 |
| 5 | 10,163 | 221 | 2.309 | 9.245 |
| 6 | 10,454 | 114 | 1.331 | 4.974 |
| 7 | 10,614 | 158 | 0.774 | 6.641 |
| 8 | 10,840 | 178 | 0.946 | 6.973 |
| 9 | 11,040 | 448 | 0.849 | 16.878 |

### Filesystem result and containment

The DSH artifact exists, is 425 bytes, and byte-compares equal to the frozen
expected fixture:

```text
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/runs/positive-v1/dsh/workspace/qualification-positive-control.json
sha256:f9adbad0987217eea433dc75935d0c34a8e9fb66badcad3eaf18abc036c11882
```

The DSH root has no tracked diff and exactly that one visible untracked path.
The two runner-owned outputs match their committed digests after restoration.
The OpenCode root has no tracked diff, no visible untracked path, and no
artifact. No outside-root write was observed. The already-qualified outer
`write_scoped` process-group Seatbelt remained active for every DSH descendant;
the `danger-full-access` label widened only DSH's unavailable inner tool policy
and did not remove the OS write boundary.

This is write confinement only. Reads and network remained unrestricted, as
declared by the posture; this record does not claim hermeticity, read
confidentiality, or network confidentiality.

## Exact protocol failures

The first DSH shell dispatch returned:

```text
Error: sandbox mode "workspace-write" is requested but no sandbox backend is usable on this host; refusing to run the command unconfined. ... Runner failure: sandbox-exec: sandbox_apply: Operation not permitted
```

The runner dispatch was not the frozen command. It was:

```text
node experiments/real-vertical-slice/federated/kernel-duel-v0.3/p2/run-duel.mjs; echo "exit=$?"
```

DSH's own comparison returned:

```text
RAW_STDOUT: "{\"status\":\"pass\",\"cases\":107,\"classifications\":214,\"divergences\":0}\nexit=0\n"
EXPECTED_MATCH: false
```

The result artifact is correct, but artifact correctness cannot erase these
execution-contract and lifecycle failures.

## Evidence and read-only verification

```sh
cd /Users/ngalluzzo/repos/fleetd

shasum -a 256 \
  docs/qualification/fixtures/dsh-qwen-positive-v1.prompt.txt \
  docs/qualification/fixtures/dsh-qwen-positive-v1.expected.json \
  .fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/positive-v1/dsh-worker.json \
  .fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/positive-v1/opencode-worker.json

cmp docs/qualification/fixtures/dsh-qwen-positive-v1.expected.json \
  .fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/runs/positive-v1/dsh/workspace/qualification-positive-control.json

git -C .fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/runs/positive-v1/dsh/workspace \
  status --short --untracked-files=all
git -C .fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/runs/positive-v1/opencode/workspace \
  status --short --untracked-files=all

sqlite3 -json .fleetd/fleetd.db \
  "SELECT * FROM invocations WHERE id='f809f490-fecc-4ca3-9696-15e331b98626';"
sqlite3 -json .fleetd/fleetd.db \
  "SELECT * FROM invocation_observations WHERE invocation_id='f809f490-fecc-4ca3-9696-15e331b98626';"
```

The exact prompt is durably preserved in message seq 121. A model replay is
intentionally not included in the read-only verification block because the
gate forbids another dispatch after this failure.

## Controlled additions in this resumed record

```text
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/positive-v1/dsh-worker.json
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/positive-v1/opencode-worker.json
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/runs/positive-v1/**
docs/qualification/fixtures/dsh-qwen-positive-v1.prompt.txt
docs/qualification/fixtures/dsh-qwen-positive-v1.expected.json
docs/qualification/dsh-ptc-qwen-harness-ab-positive-v1-failed-2026-08-29.md
docs/qualification/dsh-ptc-qwen-harness-ab-positive-v1-failed-2026-08-29.json
```

No commit was created. The OpenCode arm and seq-119 primary remain untouched.

