# Getting started

This path takes one local fleet from an empty directory to durable work and an
inspectable result. It uses the `fleetd` binary for every control-plane action;
the selected harness remains a separate plugin process.

## Install

Tagged releases contain native archives for Linux x86-64 and Apple Silicon.
Verify the downloaded archive against `SHA256SUMS`, unpack it, and put its
production binaries on `PATH`:

```sh
grep 'fleetd-v0.1.0-aarch64-apple-darwin.tar.gz' SHA256SUMS | sha256sum -c -
tar -xzf fleetd-v0.1.0-aarch64-apple-darwin.tar.gz
install -m 0755 fleetd-v0.1.0-aarch64-apple-darwin/bin/fleetd /usr/local/bin/fleetd
install -m 0755 fleetd-v0.1.0-aarch64-apple-darwin/bin/fleetd-harness-opencode /usr/local/bin/fleetd-harness-opencode
```

On macOS, replace the checksum command with
`grep 'fleetd-v0.1.0-aarch64-apple-darwin.tar.gz' SHA256SUMS | shasum -a 256 -c -`.
To build from a checkout instead:

```sh
cargo build --release --locked --workspace
install -m 0755 target/release/fleetd /usr/local/bin/fleetd
```

## Initialize and start the daemon

Run `init` once from the directory that should own the fleet:

```sh
fleetd init
fleetd serve
```

`init` creates `.fleetd/config.json`, migrates `.fleetd/fleetd.db`, and creates
the private `.fleetd/operator.token`. Relative database and token paths are
resolved from the configuration file, so commands keep finding the same fleet
regardless of the current directory when `--fleet-config` is explicit.

In another terminal, confirm both daemon and durable workforce state:

```sh
fleetd status
```

Use `--fleet-config /absolute/path/to/.fleetd/config.json` or `FLEETD_CONFIG` when
running outside the initialized directory. Explicit command-line and
environment overrides take precedence over the file.

## Register participants and create a channel

Create a submitting agent and one worker identity. Credentials are written
once to owner-only files rather than printed:

```sh
fleetd agent add --name submitter --credential-file .fleetd/submitter.token
fleetd agent add --name piler --metadata '{"harness":"opencode"}' \
  --credential-file .fleetd/piler.token
fleetd agent list
```

Copy the two stable agent IDs from `agent list`, then create a channel:

```sh
fleetd channel create --name project-001 \
  --member SUBMITTER_ID --member PILER_ID
```

## Configure and start a worker

Copy `examples/worker.opencode.example.json` from the source or release
archive. Set the exact worker agent ID, absolute workspace and executable
paths, qualified OpenCode version, and model route. The file contains no
Fleetd credential and is validated before any plugin starts.

```sh
fleetd worker run --config .fleetd/piler.worker.json
```

The worker records a ready plugin generation before leasing work. In another
terminal, inspect that generation and its session lane:

```sh
fleetd status --agent PILER_ID
```

## Submit and inspect work

Submit an opaque work contract with the submitter identity:

```sh
fleetd --token-file .fleetd/submitter.token message send \
  --channel CHANNEL_ID --to PILER_ID --kind work.request/v1 \
  --payload '{"task":"inspect this checkout and report the failing test"}' \
  --idempotency-key project-001/first-request
```

Watch the immutable conversation or list its current history:

```sh
fleetd --token-file .fleetd/submitter.token message watch --channel CHANNEL_ID
fleetd --token-file .fleetd/submitter.token message list --channel CHANNEL_ID
```

The worker publishes a causally linked result and atomically acknowledges the
input. Find the invocation ID, then read the exact joined trace:

```sh
fleetd invocation list --agent PILER_ID
fleetd trace --invocation INVOCATION_ID
```

The trace names the source and result messages, execution certainty, plugin
generation, native-session binding and owner epoch, event counts, and stop
evidence. It intentionally does not duplicate the harness transcript.

## Prove restart behavior

The release includes a self-contained reference-runtime demonstration that
submits work, hard-kills both daemon and worker, restarts them, submits a second
request, and verifies session adoption and exact durable evidence:

```sh
examples/restart-demo/run.sh
```

The command prints the evidence directory. Read `status.json`, `trace.json`,
the immutable `history.json`, and both generations' logs to see exactly what
survived and why the second attempt was safe.

See [operations](OPERATIONS.md) for delivery controls, backup, restore, and
release procedures.
