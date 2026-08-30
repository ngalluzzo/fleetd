# DSH/PTC versus OpenCode on local Qwen: blocked qualification

Date: 2026-08-29  
Verdict: **blocked before the first model request; no comparative performance evidence**

## Outcome

The typed Fleetd DSH adapter is unit-, contract-, formatting-, and lint-qualified. A
credential-free DSH ACP identity check also succeeded outside Seatbelt. The live
positive-control gate did not run because both harnesses failed to initialize inside
the required macOS Seatbelt boundary:

- pinned DSH alpha followed Node's lexical dependency search into the ambient Fleetd
  repository and attempted to read `fleetd/node_modules/argparse/package.json`; the
  sandbox correctly denied it;
- OpenCode 1.4.0's ACP process attempted to start an ephemeral local server, while
  Fleetd's current typed sandbox contract permits outbound network access but has no
  loopback bind/listen capability.

Both qualification seats were stopped at revision 2. No work message was sent, no
invocation was created, no model request occurred, and neither expected artifact
exists. Consequently this run says nothing about whether DSH, PTC, or OpenCode is
better for Qwen. It is not valid to count either pre-model boot failure as a model or
harness-task failure.

The DSH adapter changes are **test-qualified only**. Live qualification remains
blocked on the two narrow prerequisite contracts below. This run did not patch DSH,
upgrade any production runtime, grant the sandbox read access to Fleetd's ambient
`node_modules`, or weaken either arm to unsandboxed execution.

## Gate record

| Gate | Result | Evidence |
| --- | --- | --- |
| Focused adapter tests | pass | 5 unit tests and 1 process contract test |
| Formatting and clippy | pass | exact commands below |
| Pinned DSH CLI identity | pass | `0.1.2-alpha.1`, source commit `cd5ef814...` |
| DSH ACP identity outside Seatbelt | diagnostic pass | runtime `deepseek-harness-acp` `0.0.1`; clean shutdown |
| DSH/PTC positive-control boot in Seatbelt | blocked | denied ambient dependency-manifest read before ACP initialize |
| OpenCode positive-control boot in Seatbelt | blocked | ephemeral loopback server could not bind before ACP initialize |
| Positive-control work turn | not started | zero messages, invocations, and model calls |
| seq 119 primary replay | not started | positive-control gate did not pass |
| paired repetitions / DSH-native arm | not started | primary was never reached |

## Frozen identities

### Harnesses and adapter

- DSH release: [`dsh-v0.1.2-alpha.1`](https://github.com/deepseek-ai/deepseek-harness/releases/tag/dsh-v0.1.2-alpha.1)
- DSH commit: `cd5ef8148158c3a752a658978873241fdf8e2bbc`
- Node: `v25.9.0`
- pnpm: `11.7.0`
- DSH source archive SHA-256: `1fe7d2380d3e53eac2f6ee92ee5c81850ddc9b735b5910bae132cf1fc12b7211`
- DSH `pnpm-lock.yaml` SHA-256: `506ad1fc7c40f71ce8c6afe08724fdd55020c1a527d7a7a185c559d39ecfcaf1`
- DSH built `lib`/`dist` closure (7,980 files) SHA-256:
  `22ce84b95e2a6a53fb6d912dee5c17d800c1322b7659cb1e0db808da2fc4ff2c`
- DSH CLI `apps/cli/lib/bin.js` SHA-256:
  `dc23f6c5dd7df8834e3e38bdb9609d77b459834681ae9b7133b417b0c35f3166`
- Combined DSH source/runtime identity:
  `063f62a7c1f9e505171307fa2d6819acc4d1b97b055d27af8b6d5d25b417d60d`
- Fleetd DSH adapter binary SHA-256:
  `b3d5673b2321d97918ff249862f3bb516c383b213c06aa40563154101d37222f`
- OpenCode: `1.4.0`; binary SHA-256:
  `3d2c79a23f8a17d7ac35c819fba5bfac9393642de51434896adf7887629cc763`
- Fleetd OpenCode adapter binary SHA-256:
  `8b9698eeb393226c08697e01fb17e67fbfe952c8713d0dde186affb3c4e01bfb`

The DSH release is an alpha and its own release notes say it has not yet had a
security audit. Its documented `native`, `code`, and combined tool modes explain the
PTC hypothesis, but do not establish a performance result; see the official
[tool-mode documentation](https://github.com/deepseek-ai/deepseek-harness/blob/master/packages/core/tools/README.md).

### Generated DSH composition

- tool presentation: `ptc`
- reasoning effort: `none`
- maximum output: `8192`
- context window: `262144`
- stream idle deadline: `300000 ms`
- composition identity in filename:
  `2753509caf68d89f7bb6b7dfc449da72c5dfb81be1d7f9b273159fa3d5f41dfb`
- composition file SHA-256:
  `52649d42d7f6d6568ff202c5e192001bedc4075e3e2d886d8d0995605386b5cd`
- empty home patch SHA-256:
  `37517e5f3dc66819f61f5a7bb8ace1921282415f10551d2defa5c3eb0985b570`
- empty settings SHA-256:
  `ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356`
- diagnostic, non-Seatbelt ACP profile digest:
  `sha256:5274b3c25ef38b92729f2cbf8a084ceda49acc8ddf2ec234e322390664c391a4`

The diagnostic profile digest was produced with a fake credential-free backend
description solely to test boot and ACP identity. It is not presented as the live
qualification profile digest. Neither supervised adapter completed initialization,
so neither returned a live effective runtime profile digest. For stable comparison,
the canonical catalog-profile SHA-256 values are:

- OpenCode positive profile: `fc73245bccc42ebc6810c679afd4a172d957d004abb54dd16a394c07e38dad42`
- DSH/PTC positive profile: `b8ab8d768339584b2b8d1e25a47b255453197da0b29d9e311d8b3d91b9384424`
- shared backend profile: `bc83ea9768dff6aed96985aa364259af8d5cc01116d7039b68b92fbfa3434ea9`

Each catalog hash is over `jq -cS` canonical JSON for that selected object.

### Shared inference route

Both arms selected `mlx-qwen-local`, resolved through
`fleetd.inference.mlx-vlm` to MLX-VLM `0.6.15` with:

```text
/Users/ngalluzzo/.cache/buzz-inference-lab/venvs/mlx-vlm-d734bd28/bin/python \
  -m mlx_vlm.server \
  --host 127.0.0.1 --port 18082 \
  --model /Users/ngalluzzo/Models/qwen3.8-27b-8bit \
  --draft-model /Users/ngalluzzo/Models/qwen3.8-27b-mtp-8bit \
  --draft-kind mtp --draft-block-size 4 \
  --max-kv-size 262144 --max-tokens 8192 --max-num-seqs 1 \
  --enable-thinking
```

The server reported the exact model path, effective context `262144`, tool parser
`qwen3_coder`, continuous batching enabled, and APC enabled. The primary comparison
was frozen at reasoning `none` and `8192` output tokens so it would test the harness,
not a new Qwen tuning recipe.

Neither harness emitted a provider request, so the effective request-level
`temperature` and `top_p` are **not observed**. Neither qualification profile sets
them. Had a request omitted both fields, MLX-VLM 0.6.15's pinned request normalizer
would have fallen back to the model's `generation_config.json` (`temperature: 1.0`,
`top_p: 0.95`), but this is a prospective shared default, not evidence from this
blocked run. The proposed `xhigh`/32K/`temperature=1`/`top_p=.95` combination remains
a separately named future optimization arm and must not be mixed into the primary.

### Frozen work inputs

- GOOI commit: `b18bbdeb2c5cd195e52d29270008b040cb8c2145`
- positive-control manifest SHA-256:
  `6c9d86109e34db14b7eb8e6172ea507db4164046eedbfbdd414590c33463710d`
- P2 conformance README SHA-256:
  `befc6b685fde8842752949315e6bea1691abbd63676f5e7614cbe0e23843f5b9`
- corpus SHA-256:
  `6fddb18a99200319ccccdeb6770f65377fe56b446b28d351c08f7a37a773bc83`
- seq 119 channel: `3c6f6c83-90ec-4276-b9e6-04ca35780d68`
- seq 119 request ID: `c3b8c577-aeae-4a5a-9bf3-787c38af68b0`
- failed historical invocation to replay: `98c79887-b1e2-4cfc-a343-e531c5b255be`
- frozen Rust command artifact:
  `6e18ecd59967c89af58ddb9e80554bff7ef56468ee24d90f17f3f324972ce43b`
- frozen TypeScript CLI command artifact:
  `13d770e329e18e90890c833e292475db4699eb5bbe74cdaac554a90885f0ee22`

The two positive-control roots independently resolve to the frozen GOOI commit and
the three input digests match in both roots. The expected output path in each root
was `qualification-positive-control.json`.

## Exact blockers

### DSH: ambient lexical dependency resolution

The supervisor surfaced:

```text
fleetd DeepSeek Harness failed: inner ACP runtime error: ACP runtime exited during initialize
```

Running the same generated DSH command under the same derived Seatbelt profile made
the hidden cause observable before any model call:

```text
Error: EPERM: operation not permitted, open '/Users/ngalluzzo/repos/fleetd/node_modules/argparse/package.json'
    at readModuleFallbackManifest (.../packages/boot/app-boot/lib/index.js:583:20)
    at resolveModuleFallbackEntries (.../packages/boot/app-boot/lib/index.js:609:14)
    at healProfilesModuleFallback (.../packages/boot/app-boot/lib/index.js:662:36)
...
Node.js v25.9.0
```

The pinned implementation reads dependency manifests in a breadth-first traversal
and obtains candidates from `createRequire(anchor).resolve.paths(...)`; see
[`profile.ts` at the pinned tag](https://github.com/deepseek-ai/deepseek-harness/blob/dsh-v0.1.2-alpha.1/packages/boot/app-boot/src/profile.ts).
The install's lexical ancestry reached the unrelated Fleetd checkout and selected
its `node_modules/argparse`. Non-sandbox boot therefore was not hermetic and was not
an acceptable qualification result. The Seatbelt denial was the correct behavior.

### OpenCode: missing typed loopback bind capability

The adapter surfaced an ACP initialize exit. Running OpenCode 1.4.0's exact ACP
entry under the derived Seatbelt profile produced:

```text
Error: Unexpected error, check log file at .../.fleetd/home/.local/share/opencode/log/2026-08-29T164619.log for more details

Failed to start server. Is port 0 in use?
```

OpenCode's ACP entry starts a private server on port `0` (an OS-selected ephemeral
port). Fleetd's current Seatbelt generator adds only `(allow network-outbound)` when
the typed profile selects `allow_outbound`; it grants no bind/listen operation. A
fair comparison cannot run OpenCode unsandboxed or add an untyped blanket network
grant.

## Proof that no model experiment ran

At the post-stop evidence checkpoint the shared inference server returned:

```json
{
  "latest": null,
  "summary": {
    "requests_started": 0,
    "requests_completed": 0,
    "requests_failed": 0,
    "streaming_requests": 0,
    "in_flight": 0,
    "prompt_tokens_total": 0,
    "completion_tokens_total": 0,
    "last_request_at": null,
    "last_error": null
  }
}
```

SQLite independently reports zero sent messages, received messages, and invocations
for both qualification agent IDs. Neither positive-control artifact exists. No
seq 119 message was copied or sent, and no primary evaluation root was created.

The durable final seat state is:

| Agent | Profile | Desired state | Revision | Updated at (ms) |
| --- | --- | --- | ---: | ---: |
| `7d5177c1-f5f9-429c-b7ff-6ba5c8ffd647` | `qual-dsh-ptc-qwen-positive` | stopped | 2 | 1788022013924 |
| `8d7cb92d-ed8a-48ff-be64-00e083a8d7bb` | `qual-opencode-qwen-positive` | stopped | 2 | 1788022013916 |

## Prerequisite contracts

### A. Hermetic content-addressed JavaScript runtime

Implement a Fleetd-owned runtime installation contract without patching the pinned
DSH source:

1. Build in isolated staging, then install the official packed/release closure into
   a content-addressed runtime store whose lexical ancestors are not a project or JS
   package tree. A suitable macOS location is beneath Fleetd's application-support
   directory, not beneath the Fleetd checkout.
2. Record the immutable manifest of every regular file, symlink, target, mode, and
   digest; bind the catalog entry to the resulting closure identity.
3. Before launch, resolve every configured entry point and package fallback from the
   installed location. Fail closed if any resolved target escapes the closure or if
   an ancestor `node_modules` can influence resolution.
4. Add a real test with a deliberately poisoned ancestor `node_modules/argparse`.
   DSH boot must neither open nor select the poison and must keep the same runtime
   digest when the ambient repository changes.
5. Keep the current runtime version and source commit for the resumed A/B. A new DSH
   build or patched upstream source is a different arm and requires a new identity.

This re-home is narrowly about installation identity and lexical resolution. It does
not authorize arbitrary environment variables, argv, mutable global `DSH_HOME`, or
additional repository read grants.

### B. Typed loopback-only ephemeral bind

Extend the sandbox schema with a narrow capability such as
`loopback_bind: ephemeral`; do not overload `allow_outbound` and do not expose raw
Seatbelt text.

The compiler must produce the smallest macOS rule that permits an OS-selected local
listener on `127.0.0.1`/`::1` while continuing to deny wildcard/external listeners,
fixed external ports, and unrelated inbound access. The exact SBPL syntax is an
implementation question and must be proven against the deployed macOS version,
not guessed in the schema. The capability and normalized endpoints must participate
in the sandbox/profile digest.

Focused qualification must include:

- unit/serialization tests for accepted and rejected typed combinations;
- a real Seatbelt fixture where `127.0.0.1:0` succeeds;
- negative fixtures where `0.0.0.0:0`, an external-interface bind, and disallowed
  fixed endpoints fail;
- proof that outbound behavior is unchanged;
- a real OpenCode 1.4.0 ACP initialize/clean-shutdown test with no model request;
- continued enforcement of `allow_once` only when the OS sandbox is active.

## Files intentionally changed or created by this qualification

Source/config edits:

- `plugins/deepseek/Cargo.toml` — `a9f7218a4ed9cb052b3f0b66393e61e42d5a6f726a95fd6787bd538664aaefd5`
- `plugins/deepseek/src/main.rs` — `0bd8db4ba7e1bfda8db88ff573b4716f9ff2524d7962731069a8efe8f557c430`
- `plugins/deepseek/tests/plugin.rs` — `9a1a44e625bfa058c36fa399db44ee6cf75e2312baef3fd9cbdd992122ce892d`
- `plugins/deepseek/tests/fixtures/mock_deepseek.py` — `f0c6ef74dbf62a6e02200e25a7fe7847114a6e7d07e555bb09bfc08d9a1d54c1`
- `.fleetd/worker-profiles.json` — `a6ec77400ac09b7442e9477a40b2949e7dec998c9d6d82209bae9b2e0a1844fd`
- this report and its adjacent machine-readable manifest

Isolated, generated experiment state:

- `.fleetd/runtimes/dsh-0.1.2-alpha.1-cd5ef814/`
- `.fleetd/qualification/dsh-qwen-2026-08-29/boot/`
- `.fleetd/qualification/dsh-qwen-2026-08-29/runs/positive/opencode/`
- `.fleetd/qualification/dsh-qwen-2026-08-29/runs/positive/dsh/`
- two owner-only agent credential files under those positive roots
- the two durable stopped seat configurations listed above

No GOOI kernel, frozen input, command artifact, production backend, or global npm
installation was modified. The repository was already broadly dirty; unrelated
tracked and untracked changes were left untouched and are not attributed to this
qualification.

## Commands and resumption procedure

Adapter checks that passed:

```sh
cd /Users/ngalluzzo/repos/fleetd
cargo test -p fleetd-harness-deepseek
cargo fmt --check -- plugins/deepseek/src/main.rs plugins/deepseek/tests/plugin.rs
cargo clippy -p fleetd-harness-deepseek --all-targets -- -D warnings
PATH=/opt/homebrew/opt/node/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin \
  .fleetd/runtimes/dsh-0.1.2-alpha.1-cd5ef814/apps/cli/lib/bin.js --version
```

Read-only evidence commands:

```sh
cd /Users/ngalluzzo/repos/fleetd
curl -fsS http://127.0.0.1:18082/metrics | jq
sqlite3 -header -column .fleetd/fleetd.db \
  "SELECT agent_id, profile_id, desired_state, revision, updated_at_ms
   FROM agent_seat_configurations
   WHERE agent_id IN
     ('7d5177c1-f5f9-429c-b7ff-6ba5c8ffd647',
      '8d7cb92d-ed8a-48ff-be64-00e083a8d7bb')
   ORDER BY agent_id;"
sqlite3 -header -column .fleetd/fleetd.db \
  "WITH q(agent_id) AS
     (VALUES ('7d5177c1-f5f9-429c-b7ff-6ba5c8ffd647'),
             ('8d7cb92d-ed8a-48ff-be64-00e083a8d7bb'))
   SELECT q.agent_id,
          (SELECT COUNT(*) FROM invocations i WHERE i.agent_id=q.agent_id) invocations,
          (SELECT COUNT(*) FROM messages m WHERE m.sender_id=q.agent_id) sent_messages,
          (SELECT COUNT(*) FROM messages m WHERE m.recipient_id=q.agent_id) received_messages
   FROM q ORDER BY q.agent_id;"
test ! -e .fleetd/qualification/dsh-qwen-2026-08-29/runs/positive/opencode/workspace/qualification-positive-control.json
test ! -e .fleetd/qualification/dsh-qwen-2026-08-29/runs/positive/dsh/workspace/qualification-positive-control.json
```

After both prerequisite contracts pass, re-run the focused tests and the two
model-free ACP boot checks first. Then restart these same frozen seats serially,
never concurrently, and send the identical positive-control request with a 120 s
wall. Stop again if either arm fails. Only after both positive artifacts match may
the exact seq 119 payload be replayed with the previously frozen six-minute wall,
tool budget, permissions, inputs, and output gates. If the first DSH/PTC primary
passes while OpenCode fails, complete two more alternating paired runs and one
DSH-native arm. Promotion still requires DSH/PTC to pass at least 2/3 primary pairs
while OpenCode passes fewer, with no containment or permission regression.

Do not reuse the non-sandbox DSH diagnostic as a gate, do not grant Fleetd
`node_modules`, and do not run the xhigh/32K optimization arm until the causal
harness comparison has concluded.
