# Contributing

Keep the messaging kernel independent of harnesses and workflow domains. Add new
semantics as versioned message contracts or adapters, not fields interpreted by
the core transport.

Before submitting a change, run everything CI runs:

```sh
bin/ci
```

That mirrors `.github/workflows/ci.yml` job for job and adds the checks that
only run locally. To run just the Rust checks while iterating:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

The JavaScript packages are one npm workspace. Install once from the root and
run them together when a change touches `clients/` or `apps/`:

```sh
npm ci && npm run verify
```

`npm run verify` runs every workspace's tests and typecheck, then regenerates
each committed artifact and fails if the result differs: the TypeScript client
against `openapi/fleetd-v1.json`, the served bundle against
`apps/conversation-web`, and the client's version against the contract's.

`web/` is build output. Edit `apps/conversation-web` and rebuild; the shell and
its target contract are sources of that app.

Every committed artifact is generated from the one before it, so `bin/regenerate`
rebuilds them in that order. These paths are also marked unmergeable in
`.gitattributes`: a conflict there is resolved by regenerating, never by editing
the generated file. See `docs/PARALLEL_WORK.md`.

`.github/workflows/ci.yml` runs these commands on every pull request, plus
`examples/restart-demo/run.sh`, which hard-kills a daemon and a worker and
checks that the restarted pair adopts the native session. A change is not
complete until that workflow is green. `bin/ci` additionally runs `cargo doc`
with warnings denied, a production-only `npm audit`, and a compile check of the
qualification harness; the workflow is authoritative when the two disagree.

Each behavioral change should include a test at the narrowest stable boundary.
Changes to delivery behavior must cover concurrency and restart or expiry where
applicable. Never describe delivery as exactly-once.

Use one branch for one coherent change and keep `main` releasable. Migration
files are immutable after they have shipped; correct them with a new migration.

Credential-bearing types must redact secrets from `Debug`. Authentication must
remain read-only on the request hot path, and authorization changes require
cross-principal integration tests.

Plugin lifecycle changes must test both a conforming child process and the
failure boundary they affect, such as malformed frames, timeouts, capability or
identity mismatches, unexpected exits, or shutdown overruns. Keep lifecycle
transport separate from domain capability contracts; a capability should be
stabilized only after at least two implementations demonstrate the shared
semantics.
