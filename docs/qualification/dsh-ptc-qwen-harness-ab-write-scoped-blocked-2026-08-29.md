# DSH/PTC vs OpenCode Qwen harness A/B — `write_scoped` qualification stopped

- Date: 2026-08-29
- Status: **blocked before the first model request**
- Promotion verdict: **not evaluated**
- Previous evidence is preserved in the adjacent strict-Seatbelt blocked and
  Seatbelt-network-limit records.

## Outcome

The explicitly named macOS `write_scoped` posture is implemented and passed
focused unit, real Seatbelt, and real harness boot controls. The existing
`strict` posture remains deny-by-default and unchanged. `write_scoped` makes
only a write-confinement claim: reads and network are unrestricted.

Both real harnesses initialized and shut down through ACP without a model
request:

| arm | ACP control | generation | runtime profile | compatibility |
| --- | --- | --- | --- | --- |
| DSH/PTC | pass, graceful shutdown | `cdb7711c-575e-495a-82bb-77422f7a7b73` | `sha256:35a273d978ab6de67ce88f4961f99c75ccb6f700b989670112318b5cd1ed0535` | `sha256:7e68c7839c51f11e83dede995a6059fce4a4cdf3612bf213ce3c63b6eb277943` |
| OpenCode | pass, graceful shutdown | `3b15b721-97de-4128-a58b-5e5a6964029a` | `sha256:9a5d0b8188c5b3e0b2f62b45d59378b9b4186624a051e827d21b39366e6e82e8` | `sha256:a9d642e6e3f293ca0ce0ec51eef8a4856a7c9bc7c29143e60f81202d5716e5e0` |

The positive-control protocol then stopped before its prompt was frozen. At
the required GOOI revision
`b18bbdeb2c5cd195e52d29270008b040cb8c2145`, the required command

```text
node experiments/real-vertical-slice/federated/kernel-duel-v0.3/p2/run-duel.mjs
```

cannot run because that path does not exist. Node returns
`MODULE_NOT_FOUND`. The file first appears in direct child commit
`92c4df7edefd8d195afe7944daa05f4248866d0a`, whose parent is the frozen
revision. Its committed `run-duel.mjs` bytes have SHA-256
`6aa8dcd73686f12e12c9dc84c59462994437834fdb892088419e9e43d521cb43`.
Changing the checkout or copying the command into it would change the frozen
input. No substitution was made.

Therefore no positive-control prompt, expected artifact bytes, seq-119 replay,
or promotion claim exists for this run. MLX-VLM metrics remained at zero model
requests.

## Boundary qualified

Private worker desired state now has two typed Seatbelt postures:

- `strict`: the existing deny-default profile and existing digest domain;
- `write_scoped`: `(allow default)`, then `(deny file-write*)`, with writes
  restored only to literal `/dev/null` and canonical declared writable roots.

`write_scoped` fails closed unless desired state explicitly declares
`read_access: unrestricted`, `network: unrestricted`, a private state
directory, and a private temp directory. It rejects read-only roots because it
does not constrain reads. The working directory, additional directories,
explicit writable directories, and private state/temp roots form the write
allowlist. The filesystem root remains invalid.

The sandbox wraps the outer plugin launch. Fleetd creates the process group at
that wrapper, so the adapter, vendor harness, tools, and descendants inherit the
same write boundary. Environment clearing, credential-free loopback inference,
DSH's workspace-only tool policy, and typed Fleetd `allow_once` handling remain
separate defenses.

This posture is **not hermetic**. It provides no read confidentiality, network
confidentiality, destination control, or loopback-only bind guarantee. During
the real model-free OpenCode boot, the vendor runtime had a real listener on
`127.0.0.1:60908` and also opened external TLS connections. Those observations
are consistent with the explicit `network: unrestricted` limitation.

The effective Seatbelt profile identities for the controlled roots were:

- OpenCode: `sha256:94bdef93020ff6a485c50f92e190bbb8631a0a0e4fd48614e4c5a5a18b66069b`
- DSH: `sha256:dd90957cc9a3ddc851e56fa6c20ef6cc3eb14d23359dbd68602cef4a88967dcf`
- scope string: `writes_scoped_reads_and_network_unrestricted`

### Exact negative syscall control

A descendant Python process attempted:

```text
os.open(target, O_WRONLY|O_CREAT|O_TRUNC, 0600)
```

against a sibling outside every declared writable root. The kernel result was:

```text
errno=1 name=EPERM message=Operation not permitted
```

The target did not appear. In the same real Seatbelt test, the process read a
runtime fixture outside all writable roots, wrote inside workspace/state/temp,
wrote `/dev/null`, launched a descendant that wrote only inside the workspace,
and bound a localhost ephemeral listener. The complete plugin lifecycle then
shut down normally.

## Runtime and route identities

- Fleetd source base: `8af2743b78ac8220d090221440ad1f1a9d7d8935`
  with the uncommitted changes listed below
- built Fleetd: `sha256:c781514f1571143c757321eadffb2b572e4f5cb2131e10fde2ffd3761495391f`
- DSH source: tag `dsh-v0.1.2-alpha.1`, commit
  `cd5ef8148158c3a752a658978873241fdf8e2bbc`
- DSH CLI: `sha256:dc23f6c5dd7df8834e3e38bdb9609d77b459834681ae9b7133b417b0c35f3166`
- DSH adapter: `sha256:b3d5673b2321d97918ff249862f3bb516c383b213c06aa40563154101d37222f`
- DSH composition identity:
  `2753509caf68d89f7bb6b7dfc449da72c5dfb81be1d7f9b273159fa3d5f41dfb`
- OpenCode: `1.4.0`, binary
  `sha256:3d2c79a23f8a17d7ac35c819fba5bfac9393642de51434896adf7887629cc763`
- OpenCode adapter:
  `sha256:d97dbc688ec5d9ac41654a8e74ae74c2a1ad0364f1f56a1eefc81c2adbd0e453`
- MLX-VLM: `0.6.15`, Python
  `sha256:bc56ea9cdc0fface1eb75712f871a454324f6cbfec4e30311b197f208a7f3d07`
- backend launch profile:
  `sha256:27270216d39378dce1d53771e71eed044de9352feb24f392541873ae6d5b0e6c`
- backend catalog object:
  `sha256:bc83ea9768dff6aed96985aa364259af8d5cc01116d7039b68b92fbfa3434ea9`
- route: `http://127.0.0.1:18082/v1`, model
  `/Users/ngalluzzo/Models/qwen3.8-27b-8bit`
- MTP draft: `/Users/ngalluzzo/Models/qwen3.8-27b-mtp-8bit`, kind `mtp`, block 4
- fixed primary controls remained reasoning `none`, output 8192; no request was
  emitted, so effective request `temperature` and `top_p` remain unobserved.

The production catalog continues to forbid a profile-authored `inference`
block. The supervisor injects the typed backend description only after backend
readiness. To avoid restarting unrelated active seats, these model-free controls
ran as direct qualification workers against the already supervisor-owned route,
using the exact resolved description derived from the same pinned launch
material. This did not send a prompt or exercise sampling.

## Tests

All passed after the final source edit:

```text
cargo test -p fleetd-plugin-host                         4 passed
cargo test --test plugin_lifecycle                      9 passed
cargo test cli::worker::tests --bin fleetd              6 passed
cargo test cli::worker_supervisor::tests --bin fleetd   3 passed
cargo test -p fleetd-harness-deepseek                   6 passed
cargo test -p fleetd-harness-opencode                   8 passed
cargo clippy -p fleetd-plugin-host --all-targets -- -D warnings
cargo clippy --bin fleetd --tests -- -D warnings
cargo fmt --all -- --check
```

The model-free workers reported one plugin generation, zero reservations, zero
completed/interrupted/blocked turns, and graceful exit code 0. MLX metrics after
both controls were `requests_started: 0`, `requests_completed: 0`,
`requests_failed: 0`, `latest: null`, and zero prompt/completion tokens.

## Controlled files

Implementation and focused tests:

```text
crates/plugin-host/src/lib.rs
crates/plugin-host/src/sandbox.rs
crates/plugin-host/src/supervisor.rs
src/cli/worker.rs
tests/fixtures/mock_plugin.sh
tests/fixtures/write_scoped_runtime.txt
tests/plugin_lifecycle.rs
```

Typed DSH integration already under this qualification sequence:

```text
plugins/deepseek/Cargo.toml
plugins/deepseek/src/main.rs
plugins/deepseek/tests/plugin.rs
plugins/deepseek/tests/fixtures/mock_deepseek.py
```

Desired state and evidence:

```text
.fleetd/worker-profiles.json
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/control/dsh-worker.json
.fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/control/opencode-worker.json
docs/WORKER.md
docs/adr/0039-write-scoped-seatbelt-is-write-confinement-only.md
docs/qualification/dsh-ptc-qwen-harness-ab-write-scoped-blocked-2026-08-29.md
docs/qualification/dsh-ptc-qwen-harness-ab-write-scoped-blocked-2026-08-29.json
```

No GOOI source, reference kernel, conformance input, pinned DSH source, global
npm state, or production MLX backend was changed. No commit was created.

## Exact reproduction

```sh
cd /Users/ngalluzzo/repos/fleetd
cargo test -p fleetd-plugin-host
cargo test --test plugin_lifecycle
cargo test cli::worker::tests --bin fleetd
cargo test cli::worker_supervisor::tests --bin fleetd
cargo test -p fleetd-harness-deepseek
cargo test -p fleetd-harness-opencode
cargo clippy -p fleetd-plugin-host --all-targets -- -D warnings
cargo clippy --bin fleetd --tests -- -D warnings
cargo fmt --all -- --check

RUST_LOG=info ./target/debug/fleetd worker run \
  --db .fleetd/fleetd.db \
  --config .fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/control/dsh-worker.json

RUST_LOG=info ./target/debug/fleetd worker run \
  --db .fleetd/fleetd.db \
  --config .fleetd/qualification/dsh-qwen-write-scoped-2026-08-29/control/opencode-worker.json

git -C /Users/ngalluzzo/repos/gooi cat-file -e \
  b18bbdeb2c5cd195e52d29270008b040cb8c2145:experiments/real-vertical-slice/federated/kernel-duel-v0.3/p2/run-duel.mjs

git -C /Users/ngalluzzo/repos/gooi show \
  92c4df7edefd8d195afe7944daa05f4248866d0a:experiments/real-vertical-slice/federated/kernel-duel-v0.3/p2/run-duel.mjs \
  | shasum -a 256
```

The first `git cat-file` command must fail for the recorded block to reproduce;
the second prints `6aa8dcd7…`.

## Narrow unblock

Choose and freeze one coherent input identity before resuming:

1. move both arms to GOOI commit
   `92c4df7edefd8d195afe7944daa05f4248866d0a`, then recompute and freeze every
   input and command digest; or
2. supply a separately content-addressed command artifact while retaining
   `b18bbde`, and explicitly make that artifact part of the frozen input set.

After that choice, create fresh identical roots, freeze a newly versioned
neutral prompt and expected artifact bytes before either dispatch, run the two
positive controls serially, and only then retrieve the exact seq-119 payload
from Fleetd's durable record for the six-minute primary.
