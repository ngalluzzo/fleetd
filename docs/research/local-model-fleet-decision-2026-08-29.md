# Local model fleet decision — 2026-08-29

- Status: decision record, not a qualification record
- Evidence cutoff: 2026-08-29 (America/Los_Angeles)
- Host in scope: Apple M3 Ultra, 512 GB unified memory

## Decision

Keep the current `Qwen3.8-27B` 8-bit route as Fleetd's **qualified local
quality lane**, but do not use it as the whole fleet. It is a strong, very
recent model and the only local route in this comparison with Fleetd-owned
runtime evidence. Its dense 27B decode, long thinking turns, and current
single-sequence configuration make it a poor universal worker pool.

Build toward three roles:

1. **Quality author/reviewer:** the current Qwen3.8 27B 8-bit route, with
   `xhigh` reserved for consequential protocol and implementation decisions.
2. **High-throughput bounded workers:** qualify Qwen3.6 35B-A3B at 4 or 6 bits
   for fixture generation, test writing, reconnaissance, and small patches.
3. **Independent adjudicator:** use a remote model from a different family,
   currently GLM-5.3, Claude, or Codex, for decisions where correlated Qwen
   errors matter.

Treat a 6-bit Qwen3.8 27B conversion as a possible replacement for the current
8-bit weights, not as another permanent model. Evaluate Qwen3.8-Flash-Next now
through an API, but do not make its two-day-old Apple runtime a Fleetd default.
Flash-Next is a capability successor; it is not yet an operational successor.

This is an external qualification decision. It does not give the Fleetd
daemon model semantics. Fleetd continues to supervise an opaque, exact local
route behind the experimental inference interface.

## Evidence vocabulary

The labels below are used throughout this record.

- **Local measurement:** observed on this machine through Fleetd or its
  backend observer. It applies only to the recorded route and workload.
- **Upstream claim:** architecture or benchmark result published by the model
  author. It is useful comparison evidence, not independent reproduction.
- **Independent benchmark:** a third party's standardized evaluation. Hosted
  API throughput is not local Apple throughput.
- **Community measurement:** a reproducible-looking Apple result outside this
  repository. It is directional until repeated on this exact host and route.
- **Estimate:** derived from parameter count or quantization. It is not a
  measured capacity claim.

## Exact current route

### Model and runtime identity

The supervised backend currently resolves to:

| Property | Current value | Evidence |
| --- | --- | --- |
| Local model path | `/Users/ngalluzzo/Models/qwen3.8-27b-8bit` | Local configuration |
| Conversion | `mlx-community/Qwen3.8-27B-8bit` | Local model card and config |
| Base model | `Qwen/Qwen3.8-27B` | Local model card |
| Architecture | `Qwen3_5ForConditionalGeneration`; `qwen3_5`; vision-language model | Local `config.json` |
| Language model | Dense 27B; 64 layers; hybrid Gated DeltaNet/full attention | Local config and upstream model card |
| Weight quantization | MLX affine 8-bit, group size 64 | Local `config.json` |
| Main weight files | 29.501 decimal GB (27.475 GiB) | Local file sizes |
| Draft model | `/Users/ngalluzzo/Models/qwen3.8-27b-mtp-8bit`; 451,270,785 bytes | Local configuration and file size |
| Speculation | MTP, draft block size 4 | Local Fleetd profile |
| Runtime | MLX-VLM 0.6.15; MLX 0.32.1 | Local environment and qualification record |
| Context/output bounds | 262,144 input/cache tokens; 8,192 output tokens | Local Fleetd profile |
| Server concurrency | `max_num_seqs = 1` | Local Fleetd profile |
| Cache | APC enabled, 4,096 blocks | Local Fleetd profile |
| Thinking | Enabled; current profiles select `xhigh`, `medium`, `low`, or none by role | Local Fleetd profile catalog |

The local `README.md` and `config.json` byte-match the current Hugging Face
conversion at revision
`815b83c0df8ffd1d1b5244cf75fd6ef14fca9ef9`. The download does not retain a
content revision for every shard, and Fleetd has not hashed the complete model
directory into a manifest. Therefore this is **not** a claim that all local
weights are content-identical to that remote revision. The existing
[runtime qualification](../qualification/inference-mlx-vlm-qwen-2026-08-28.md)
correctly says that model content identity is not established.

The installed MLX-VLM package contains `qwen3_5` and `qwen3_5_moe` model
implementations but no `qwen4_exp` implementation. That is a concrete blocker
for treating Qwen3.8-Flash-Next as a drop-in replacement on the currently
qualified runtime.

### Measured local behavior

The strongest local evidence is operational, not a general model benchmark:

- The first qualified tool-producing request decoded at about **31 tokens/s**.
- In the bounded unattended A to B to A run, weighted decode was **29.135
  tokens/s**, with a per-request range of **23.253–31.786 tokens/s**.
- Uncached prefill was approximately **418–425 tokens/s**. A strong prefix
  cache hit prefetched 15,777 tokens at **5,906 tokens/s**.
- Mean TTFT was **39.847 seconds** and maximum TTFT was **78.591 seconds**.
  Normal decode continued while requests queued behind the single server
  sequence.
- Peak model-server memory in that observer window was **123.734 GB**. This is
  a process peak under the configured long-context cache and APC policy, not
  the static weight size.
- Six observed requests completed with zero backend request failures.
- In the one-versus-two sequence matrix, `max_num_seqs = 2` made the tested
  causal workload **6.51% slower** end to end and reduced weighted decode from
  30.453 to 20.340 tokens/s. That result supports one sequence for that exact
  A to B to A composition. It does not answer how independent, simultaneously
  ready jobs behave.
- A later GOOI review emitted about **4,089 reasoning-delta events over roughly
  273 seconds** before a newer Fleetd message interrupted it. Event count is
  not token count. This is latency and interruption evidence, not a throughput
  measurement.

See the
[unattended run](../qualification/qwen-unattended-soak-runner-2026-08-25.md),
[sequence matrix](../qualification/qwen-max-num-seqs-matrix-2026-08-25.md),
and [interruptibility record](../qualification/interruptible-qwen-turn-2026-08-28.md).

The model selected the intended tools, recipients, and causal lineage in the
qualified agent loop, but repeatedly encoded requested JSON objects as JSON
strings. Fleetd preserved the mismatch. For GOOI, model output must remain
behind schemas, conformance cases, authority checks, and deterministic
canonicalization. No benchmark score changes that requirement.

## Candidate comparison

Numbers in the quality column are not interchangeable across harnesses. They
are included to show direction and role fit, not to synthesize one ranking.

| Candidate | Capability evidence | Apple Silicon evidence and limits | Decision |
| --- | --- | --- | --- |
| **Qwen3.8 27B, current Q8** | Upstream: Terminal-Bench 2.1 73.0, SWE-bench Pro 61.7, DeepSWE 1.1 42.2, NL2Repo 42.3. Independent: Artificial Analysis Intelligence Index 52 at `xhigh`, 44 at `medium`, 43 at `low`, and 35 without reasoning. | Local: about 29–31 tok/s decode under MTP; 123.734 GB server peak under the recorded long-context/APC workload; one configured sequence. Exact quantized application quality has not been separately measured against BF16. | **Keep now** as the one local quality lane. Do not fan several active agents into the serialized route and call that parallelism. |
| **Qwen3.8 27B, Q6 or mixed Q4** | Same upstream base. A mixed Q4 conversion reports BFCL-V3 simple 93.5 and HumanEval 92.1, but that small suite is not a GOOI or agent-loop qualification. | Community M3 Ultra result without Fleetd: Q6 27.1 tok/s and 22.1 GB short-context peak; Q4 36.9 tok/s and 15.7 GB. MTP and Fleetd's cache policy can change both. The Q6 conversion reported only hairline perplexity changes on its sampled English/code corpora; that is not typed-output equivalence. | **Test first.** Promote Q6 over Q8 only if the frozen GOOI corpus shows no new hard failures and useful task-throughput improvement. |
| **Qwen3.6 35B-A3B Q4/Q6** | 35B total, 3B active. Upstream: SWE-bench Pro 49.5, Terminal-Bench 2.0 51.5. Independent: AA Intelligence 32. Clearly below Qwen3.8 at difficult reasoning. | Community M3 Ultra: standard Q6 MLX-VLM around 55 tok/s and 30 GB short-context peak. A Q4+DFlash setup measured about 100 tok/s baseline and 162 tok/s on one favorable short reasoning prompt; speculative gains were workload-sensitive. | **Best fast-worker candidate.** Use for bounded tasks with executable checks, not final protocol authority. |
| **Qwen3.8-Flash-Next** | Released 2026-08-26. Upstream: 125B language parameters with 6B active, plus 51B n-gram embeddings and 4B MTP; DeepSWE 58.7, SWE-bench Pro 62.5, NL2Repo 48.1, Toolathlon 73.5. Independent: AA Intelligence 56. It improves strongly over 27B on DeepSWE and modestly on SWE-Pro. | A community MLX Q8 conversion is 203 GB and measured 24.9 tok/s with MTP on an M3 Studio. It required explicit `qwen4_exp` support in an oMLX release candidate. The current Fleetd environment lacks that architecture. The upstream release calls this an experimental preview of the architecture intended for Qwen4. License is Qwen Community 1.0, not Apache 2.0. | **Capability successor, not operational successor.** Evaluate by API now; wait for a stable, pinned local runtime before Fleetd qualification. |
| **DeepSeek-V4-Flash-0731** | 284B total, 13B active, MIT license, 1M context. Upstream: Terminal-Bench 2.1 82.7, NL2Repo 54.2, DeepSWE 54.4, Toolathlon 70.3. Independent: AA Intelligence 52. | MLX support has had model-loading, short-prompt, token-dropping, and long-generation resource issues. A community 2.4-bit 92.8 GB conversion scored 7.3 MMLU-Pro points below the hosted BF16 comparison; its author explicitly did not qualify agentic or code behavior. | **Do not operationalize yet.** Useful model-family challenger after mainline runtime and long-turn stability exist. |
| **GLM-5.3** | 753B total, 40B active, 1M context. Upstream claims open-weight coding leadership. Independent: AA Intelligence 60, above every local candidate in this table. | Estimate: raw 4-bit weights alone are roughly 377 GB before scales, runtime, caches, and working memory. Even if made to load, it would consume the machine's concurrency budget and has no Fleetd MLX qualification. | **Use remotely** as the independent adjudicator. It is not a sensible local swarm worker on this host. |

The M3 Ultra provides 819 GB/s unified-memory bandwidth. Several resident
models can fit in 512 GB, but simultaneous decode still contends for that one
memory system. Memory capacity is not aggregate throughput. Fleetd needs an
exact concurrent-start matrix before selecting server counts or active
sequence counts for independent work.

## Recommended fleet topology

### Lane 1: local quality

- One shared Qwen3.8 27B Q8 backend.
- One active high-consequence turn at a time while `max_num_seqs = 1` remains
  the qualified setting.
- Use `xhigh` for protocol review, final design review, and difficult bounded
  implementation. Use lower effort only when executable checks make retry
  cheap; the independent score gap between `xhigh` and `medium` is material.
- Give long reasoning turns an idle deadline based on observed behavior, not a
  chat-oriented 60- or 120-second assumption. Preserve Fleetd's newer-message
  interruption path.

### Lane 2: local throughput

- Qualify one Qwen3.6 35B-A3B Q4 or Q6 backend.
- Multiplex several logical Fleetd seats onto it only after an exact
  concurrent-start test establishes the useful sequence count.
- Assign discovery, fixture construction, test generation, mechanical edits,
  and implementations whose success is decided by deterministic checks.
- Escalate ambiguity or a failing conformance case to the quality lane; do not
  let the fast lane adjudicate its own exception.

### Lane 3: independent review

- Use remote GLM-5.3, Claude, or Codex so that author and reviewer are not
  merely two samplings of the same Qwen family.
- Prefer disagreement artifacts: exact claim, evidence, counterexample, and
  proposed decision. Do not ask a remote model to silently merge two outputs.
- Keep credentials, subscriptions, and remote provider policy in their
  harness adapters. The Fleetd kernel remains unaware of the model family.

### Probation lane: Flash-Next

Use the hosted Flash-Next API in the promotion experiment. Do not add a local
backend profile until the revisit gate below passes. Its better benchmark
scores justify evaluation, while its new architecture, 203 GB Q8 conversion,
license change, and release-candidate runtime do not justify replacing a
qualified route.

## Frozen 12-task promotion experiment

The next decision should come from GOOI's real failure modes, not another
general leaderboard. Freeze one repository revision, tool set, prompt
template, time budget, and expected-answer manifest. Give every run a fresh
native session and prevent candidates from reading another candidate's output.

### Corpus

1. **Four protocol-review cases**
   - unknown facet versus missing registered package;
   - retraction and derived-descendant ordering;
   - semantic-set canonicalization before claim revision;
   - graph, plan, obligation, and diagnostic consistency.
2. **Four canonical transformation cases**
   - known keyed-set composition;
   - legal opaque unknown facet preservation;
   - explicit authority rejection;
   - completeness, deletion, retraction, and orphaning.
3. **Four bounded implementation cases**
   - one Rust parser/composer change;
   - one TypeScript parser/composer change;
   - one conformance-fixture or schema change;
   - one defect localization and minimal patch task.

The cases should use exact files and expected digests or assertions. A task may
contain seeded defects, but the answer key must remain outside every model's
read grant.

### Candidates and controls

- Current Qwen3.8 Q8 route.
- Qwen3.8 Q6 candidate, with the same model template, reasoning effort,
  sampler, MTP policy, and context as Q8.
- Qwen3.6 35B-A3B local worker candidate.
- Qwen3.8-Flash-Next hosted API.
- Three runs per task and candidate, with randomized run order.
- Identical harness/tool grants and no network access unless the case itself
  declares it.
- Record cold and warm-cache conditions separately. Do not average them into
  one unexplained latency number.

### Required measurements

Correctness:

- JSON/schema validity before any repair;
- conformance verdict and canonical output digest;
- tests passed and expected defects found;
- false-positive defects;
- edit containment and unrelated-file changes;
- authority violations, unsupported assumptions, and invented semantics;
- tool-call and typed-payload failures.

Operations:

- end-to-end task wall time and time to first useful action;
- model TTFT, prefill, decode, and reasoning/output volume when available;
- retries, interruptions, deadline exits, and operator corrections;
- model-server peak memory, cache hits, queue delay, and completed tasks/hour;
- exact model, quantization, runtime, prompt, harness, and source revision.

Reasoning-delta event counts may be recorded as Fleetd operational evidence,
but must not be relabeled as model tokens.

### Promotion rules

- **Q6 replaces Q8:** zero new hard schema, authority, digest, or test failures
  across the corpus, with at least 20% lower median wall time or a demonstrated
  increase in safe concurrent task throughput.
- **Qwen3.6 becomes a worker lane:** no authority or edit-containment failures
  on the transformation and patch cases, at least 90% of Q8's hard-case pass
  count, and at least 1.5 times Q8's completed tasks/hour under the same host
  contention.
- **Flash-Next becomes the local quality candidate:** it materially exceeds
  Q8 on seeded-defect recall or hard-case passes without increasing false
  positives, and it passes the runtime gate below. Hosted success alone does
  not qualify the local conversion.

Publish raw run artifacts and the declared scoring program before choosing a
winner. A failed promotion leaves the current route unchanged.

## Flash-Next revisit gate

**Dated review:** revisit on **2026-09-12**, or earlier when all three upstream
events occur:

1. a stable MLX-VLM or MLX-LM release used by Fleetd contains explicit
   `qwen4_exp` and Qwen Sparse Attention support;
2. a trusted conversion publishes exact source revision, quantization,
   complete file hashes, MTP compatibility, and license; and
3. its OpenAI-compatible server path supports Fleetd's required health, exact
   model listing, thinking controls, tool calls, cancellation, and bounded
   shutdown without a custom unmerged branch.

At that review, run model load, 12-task corpus, exact concurrent-start matrix,
restart/resumption, interruption, and an eight-hour soak. Record native
262K-context behavior separately from shorter practical contexts. If the gate
has not passed by **2026-10-01**, reassess the wider local market rather than
waiting indefinitely for one architecture.

## Evidence limits

- Upstream benchmark tables use different harnesses, temperatures, context
  limits, timeouts, and sometimes corrected datasets. They are not direct
  measurements of GOOI or Fleetd.
- Artificial Analysis measures hosted APIs. Its quality comparison is useful;
  its output speed is not a prediction of this Mac.
- Community Apple benchmarks vary by GPU-core count, runtime, quantization,
  context length, speculative acceptance, and cache state.
- Static weight size does not predict process peak memory. The current 29.501
  GB weights reached a 123.734 GB server peak under Fleetd's recorded cache
  configuration.
- `max_num_seqs = 2` regressed one causal two-seat workload. Independent
  parallel work remains unmeasured.
- Quantization quality is workload-specific. Perplexity and HumanEval do not
  establish typed tool use, authority discipline, or canonical JSON behavior.
- The current model's complete local content revision is not established.

## Sources

### Primary model and platform sources

- [Current MLX conversion: Qwen3.8-27B 8-bit](https://huggingface.co/mlx-community/Qwen3.8-27B-8bit)
- [Qwen3.8-27B model card](https://huggingface.co/Qwen/Qwen3.8-27B)
- [Qwen3.8-Flash-Next model card](https://huggingface.co/Qwen/Qwen3.8-Flash-Next)
- [Qwen3.6-35B-A3B model card](https://huggingface.co/Qwen/Qwen3.6-35B-A3B)
- [DeepSeek-V4-Flash-0731 model card](https://huggingface.co/deepseek-ai/DeepSeek-V4-Flash-0731)
- [GLM-5.3 model card](https://huggingface.co/zai-org/GLM-5.3)
- [Apple M3 Ultra Mac Studio specification](https://www.apple.com/shop/product/g1ce8ll/a/Refurbished-Mac-Studio-Apple-M3-Ultra-chip-with-32%E2%80%91Core-CPU-and-80%E2%80%91Core-GPU)
- [MLX-VLM upstream repository](https://github.com/Blaizzy/mlx-vlm)
- [MLX-LM DeepSeek V4 support request and status](https://github.com/ml-explore/mlx-lm/issues/1233)
- [MLX-LM DeepSeek V4 long-generation resource issue](https://github.com/ml-explore/mlx-lm/issues/1332)

### Independent benchmark sources

- [Artificial Analysis: Qwen3.8 27B](https://artificialanalysis.ai/models/qwen3-8-27b)
- [Artificial Analysis: Qwen3.8-Flash-Next](https://artificialanalysis.ai/models/qwen3-8-flash-next)
- [Artificial Analysis: Qwen3.6 35B-A3B](https://artificialanalysis.ai/models/qwen3-6-35b-a3b)
- [Artificial Analysis: DeepSeek V4 Flash 0731](https://artificialanalysis.ai/models/deepseek-v4-flash)
- [Artificial Analysis: GLM-5.3](https://artificialanalysis.ai/models/glm-5-3)

### Community Apple Silicon measurements

- [M3 Ultra MLX standalone benchmark repository](https://github.com/chanunc/local-llm-mac-studio/blob/main/docs/models/benchmarks/model-benchmark-standalone.md)
- [Qwen3.8-27B graded Q6/Q4 Apple measurements](https://huggingface.co/avlp12/Qwen3.8-27B-Alis-MLX-6bit)
- [Qwen3.8-27B mixed-Q4 conversion and small evaluation suite](https://huggingface.co/mlx-community/Qwen3.8-27B-OptiQ-4bit)
- [Qwen3.8-Flash-Next MLX Q8/MTP conversion and Apple measurement](https://huggingface.co/Vontra/Qwen3.8-Flash-Next-MLX-8bit-MTP)
- [DeepSeek V4 Flash 0731 2.4-bit comparison](https://huggingface.co/mlx-community/DeepSeek-V4-Flash-0731-2.4bit-mixed)
