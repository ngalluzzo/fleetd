# fleetd agent instructions

Read `VISION.md`, `README.md`, `docs/ARCHITECTURE.md`, `docs/PROTOCOL.md`, and
`docs/MILESTONES.md` before non-trivial changes. Read `docs/PARALLEL_WORK.md`
before changing anything generated or shared, and when several agents are
working at once.

- Keep the kernel limited to agents, channels, membership, and immutable
  messages. Harness and workflow semantics belong in adapters or versioned
  contracts.
- Preserve unknown message kinds and JSON payload fields.
- Treat SQLite as authoritative; in-memory delivery may be lost and must always
  be recoverable through cursor replay or the durable inbox.
- Preserve at-least-once delivery semantics. New settlement paths must reject
  stale leases and remain safe to retry after a lost response.
- Never edit an applied migration; add a forward migration, named by UTC
  timestamp with `bin/new-migration <description>`. A sequential ordinal
  collides silently between authors: the build passes and one database later
  refuses to migrate.
- Never log or persist raw credentials. Agent authority must come from the
  authenticated principal, not caller-supplied identity fields.
- Keep network listeners on loopback until encrypted transport and enrollment
  are explicitly implemented.
- Put every new module in the layer that owns it: `src/execution` for what
  happens to durable state, `src/http` for how it is exposed. Nothing new
  belongs at the root of `src/`.
- Never hand-merge a generated artifact. The contract, the generated client, and
  the served bundle are marked unmergeable; take either side and run
  `bin/regenerate`, which rebuilds them in the order they depend on each other.
- Split a module into a directory once it holds more than one concept, giving
  each concept its own source and its own `impl` block. Reaching a parent's
  private state from a child module is allowed, so the split costs call sites
  nothing.
- Name a new module's source in the boundary assertions by concept, not by file:
  `tests/crate_boundaries.rs` resolves a module to a file or to every source
  under a directory, and splitting one must not shrink what it checks.
- The kernel is `crates/kernel` and holds the only connection pool. Compose
  above it with free functions over `&Store`; never add methods to `Store` or
  reach for the pool from outside.
- Keep every delivery row transition in the kernel and compose it with the
  invocation fence above the kernel. Nothing layered above may write a kernel
  table directly.
- Reach a type through the module that owns it. The crate root re-exports
  modules, never individual items; a flat root list is a conflict magnet that
  every change has to edit.
- Keep boundary-crossing types in `fleetd-proto` and everything that reads,
  stores, or transports them in the crate that owns that behavior. Plugins,
  hosts, and tools depend on the wire crate, never on the daemon.
- Put harness and external-system integrations in out-of-process plugins behind
  narrow, independently versioned operational interfaces. Do not add a generic
  execution escape hatch.
- Plugin manifests negotiate transport interfaces, never semantic capability
  claims. Fleetd must not import a semantic compiler or understand its facts,
  plans, offers, invocations, candidates, or conformance results.
- Semantic integrations use public Fleetd artifacts through a separately
  versioned lift/bridge/lower package. Fleetd source contains neither side of
  that bridge and transports any resulting documents as opaque message data.
- Launch plugin executables directly without a shell. Plugin stdout is protocol
  traffic only, and plugins must not receive fleetd credentials or ambient
  environment variables.
- Exercise a proposed plugin interface in at least two real integrations before
  treating its contract as stable.
- Reuse Git, harnesses, parsers, and model servers instead of rebuilding them.
- Run formatting, clippy with warnings denied, and all tests before claiming a
  change is complete.
