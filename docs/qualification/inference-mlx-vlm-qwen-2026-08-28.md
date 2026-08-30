# MLX-VLM Qwen real-runtime qualification — 2026-08-28

This record qualifies one real MLX-VLM backend and one OpenCode agent through
the experimental `fleetd.inference-openai@0.1.0` interface. It does not qualify
llama.cpp and does not stabilize the interface.

## Runtime

- host: Apple Silicon macOS;
- backend integration: `fleetd.inference.mlx-vlm` 0.1.0;
- MLX-VLM: 0.6.15 through a Python 3.12.14 virtual environment;
- backend plugin SHA-256:
  `8966b539051a237152bb8aa67e4c07ea9efbffac210d855063346e36906879bc`;
- Python executable SHA-256:
  `bc56ea9cdc0fface1eb75712f871a454324f6cbfec4e30311b197f208a7f3d07`;
- model route: `/Users/ngalluzzo/Models/qwen3.8-27b-8bit`;
- MTP draft model: `/Users/ngalluzzo/Models/qwen3.8-27b-mtp-8bit`;
- loopback origin: `http://127.0.0.1:18082`;
- context bound: 262,144 tokens;
- output bound: 8,192 tokens;
- concurrent sequences: 1; and
- APC: enabled with 4,096 blocks.

The model path has no established content revision, so this record makes no
content-addressed model claim.

## Boundary proof

The worker supervisor launched one backend plugin, which launched the exact
virtual-environment Python path without a shell. Initialization observed
MLX-VLM 0.6.15, waited for `/health`, required the exact configured model ID in
`/v1/models`, and supplied only the resolved loopback route to OpenCode 1.4.0.
The live health response reported the configured context bound, Qwen tool
parser, continuous batching, and APC.

The first real request was message
`b111ff9a-7920-4be1-ac25-a10acce7a110` in channel
`a0eb5d3a-d9f3-4745-880b-5904a214131f`. Agent `planner` inspected the checkout,
used model-selected tools, sent direct reply
`fb2b9ac2-de29-49e1-adff-14d1165fac74`, and completed invocation
`cfffb277-db85-4125-b92d-b97646d01ecc` with `outcome_known`. The delivery was
acknowledged once. Across that turn MLX-VLM completed 12 streaming inference
requests with zero backend failures; observed decode throughput was about 31
tokens per second on the first tool-producing request.

Fleetd and the supervisor were then stopped and started under `launchd`.
Follow-up message `b7ed662c-16e7-4923-a463-7dc730c5414b` adopted the same
OpenCode session reference under owner epoch 2. Agent `planner` sent direct
reply `bdace269-c004-4729-bce5-ede77648d142` and completed invocation
`43c6889e-4a08-4b68-8bab-21ad772c3d21`; its delivery was acknowledged once.

## Defects exposed

The real runtime exposed two integration problems before the proof passed:

1. canonicalizing the virtual-environment Python symlink before launch caused
   Python to lose its environment and reject the MLX-VLM version probe; the
   host now validates the resolved file while preserving the approved absolute
   launch path; and
2. an existing `launchd` job kept the same port alive, causing Fleetd to admit
   the unrelated healthy endpoint before its own child failed to bind. The
   legacy job was unloaded, its plist retained for recovery, and Fleetd became
   the sole lifecycle owner.

The first Qwen turn also identified the next reliability risk: one transient
backend health failure currently stops every dependent agent and begins a
fresh model load. Before this interface stabilizes, a process-alive backend
must tolerate bounded transient probe failure and record whether unavailability
was a child exit, timeout, HTTP failure, or model-route mismatch.
