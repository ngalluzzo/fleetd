# Qwen `max_num_seqs` matrix — 2026-08-25

## Scope

This exploratory matrix compares MLX continuous batching with one versus two
active sequences under the real two-seat OpenCode/Qwen A → B → A workload. It
uses `fleetd-soak` to run two exact sequential workloads in each condition and
to preserve Fleetd and MLX evidence around every workload.

The result supports a configuration decision for this causal composition. It
does not yet qualify throughput for independent parallel jobs or replace the
full-night M2 soak.

## Controlled composition

Both conditions used:

- the same M3 Ultra host, Fleetd database, agents, channel, worker code, and
  typed OpenCode plugin;
- `/Users/ngalluzzo/Models/qwen3.8-27b-8bit` with the same 8-bit MTP draft
  model, draft block size 4, and maximum output 8,192;
- `mlx-vlm` 0.6.15, MLX 0.32.1, Qwen3 Coder tool parsing, and a 262,144-token
  context;
- `APC_ENABLED=1` and `APC_NUM_BLOCKS=4096`;
- one dedicated server on `127.0.0.1:18083`, restarted cold between
  conditions;
- two exact sequential A → B → A workloads, with condition names differing by
  only the `1`/`2` digit;
- fresh worker profiles and native-session generations for each condition.

The only model-server launch change was `--max-num-seqs 1` versus
`--max-num-seqs 2`. The separately supervised server on port 18082 remained
idle in both conditions. Its resident weights may affect absolute memory
numbers but were a constant background condition.

MLX `/health` and `/metrics` expose that continuous batching and APC are active,
but do not expose the configured `max_num_seqs`. The exact launch commands are
operator evidence rather than self-describing report evidence. A future formal
qualification should add this field to a backend-owned health document or
capture a separate condition manifest observer.

## Artifact identity

| Condition | Plan SHA-256 | Report SHA-256 |
| --- | --- | --- |
| 1 sequence | `7f75505328e556c339f40464e8f7dd56038a3b86482aaec4560e8b6e095f4d43` | `e34d64446ffa50e97927fb4c98ff31ed3ab22a0977014b674ae621258440e952` |
| 2 sequences | `a64812f88f90aa294ccc89cae9c79f06a4c248a7078efcd01c83fa3da5cf29bf` | `5f36cd6b3d0ac70e9e2dc1be7a3576cb338107b2e10834a0d8acb1ffad2af673` |

The local reports remain at:

- `.fleetd/soak-qwen-2026-08-24/matrix-maxseq1-report.json`;
- `.fleetd/soak-qwen-2026-08-24/matrix-maxseq2-report.json`.

Both reports passed two workloads, six exact causal invocations, zero retries,
zero blocks, and zero unresolved delivery blocks. All four worker generations
stopped gracefully. The one-sequence condition used binding generation 2 and
the two-sequence condition used binding generation 3; both began at owner epoch
1, preventing a prior native session from favoring either condition.

## Results

Each condition completed exactly 14 model requests: two fresh-session title
requests plus six requests per workload. Prompt volume differed by only 0.26%.
The six tool-call requests produced exactly 1,454 completion tokens in both
conditions.

| Metric | 1 sequence | 2 sequences | Change with 2 |
| --- | ---: | ---: | ---: |
| Total wall time | 339.634 s | 361.739 s | +6.51% |
| Workload 1 wall time | 176.119 s | 190.717 s | +8.29% |
| Workload 2 wall time | 163.494 s | 171.004 s | +4.59% |
| Prompt tokens | 150,454 | 150,060 | −0.26% |
| Completion tokens | 2,152 | 2,196 | +2.04% |
| Effective completion throughput | 6.336 tok/s | 6.071 tok/s | −4.19% |
| Weighted decode throughput | 30.453 tok/s | 20.340 tok/s | −33.21% |
| Tool-call weighted decode | 34.076 tok/s | 27.038 tok/s | −20.65% |
| Non-tool weighted decode | 24.930 tok/s | 13.692 tok/s | −45.08% |
| Mean TTFT | 32.495 s | 33.412 s | +2.82% |
| Maximum TTFT | 68.357 s | 70.559 s | +3.22% |
| Sum of request elapsed time | 525.615 s | 575.767 s | +9.54% |

APC behavior was effectively identical: each condition recorded three exact
hits and 28 exact stores. Matched prompt tokens were 37,332 with one sequence
and 37,224 with two.

The warmed second workload is the least output-sensitive comparison. The
two-sequence condition generated 13.8% fewer completion tokens for that
workload but still took 4.6% longer. This supports the scheduler result beyond
the model's nondeterministic response lengths.

## Interpretation

Two sequences did perform concurrent work at the intended contention points:
B's post-tool terminal drain overlapped A's causal final turn. That overlap did
not remove the queue-shaped TTFT tail. It divided decode capacity sharply,
increased aggregate request work, and produced a net wall-clock regression.

For this model, draft configuration, and causal agent loop, one sequence is the
better operating point. The supervised server should remain at
`max_num_seqs = 1`.

This is not evidence that two sequences are universally worse. The causal loop
has a narrow useful-overlap window and substantial long-context prefill. Two
independent seats with simultaneously available work may amortize the decode
penalty differently. That requires a separate exact concurrent-start matrix;
the sequential soak plan must not be reinterpreted as that experiment.

As in earlier runs, Qwen selected the correct tool operations and lineage while
encoding requested JSON objects as strings. Fleetd preserved the mismatch and
the matrix made no semantic normalization.
