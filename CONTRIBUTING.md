# Contributing

Keep the messaging kernel independent of harnesses and workflow domains. Add new
semantics as versioned message contracts or adapters, not fields interpreted by
the core transport.

Before submitting a change, run:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

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
