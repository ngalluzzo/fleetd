# Contributing

Keep the messaging kernel independent of harnesses and workflow domains. Add new
semantics as versioned message contracts or adapters, not fields interpreted by
the core transport.

Before submitting a change, run the Rust checks:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
```

Those commands do not cover the TypeScript client or the presentation hosts, so
run their suites too when a change touches `clients/` or `apps/`:

```sh
(cd clients/typescript && npm ci && npm test && npm run typecheck)
(cd apps/conversation-web && bun test && npm run typecheck)
(cd apps/conversation-desktop && npm ci && bun test && npm run typecheck)
```

`apps/conversation-web` has no dependencies of its own and type-checks with the
client package's `tsc`, so install `clients/typescript` before it.

`.github/workflows/ci.yml` runs exactly these commands on every pull request.
A change is not complete until that workflow is green.

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
