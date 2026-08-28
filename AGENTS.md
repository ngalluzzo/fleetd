# fleetd agent instructions

Read `VISION.md`, `README.md`, `docs/ARCHITECTURE.md`, `docs/PROTOCOL.md`, and
`docs/MILESTONES.md` before non-trivial changes. Read `docs/PARALLEL_WORK.md`
before changing anything generated or shared, and when several agents are
working at once. Read `docs/GETTING_STARTED.md` and `docs/OPERATIONS.md` before
changing the CLI or an operator read model: they are what an operator is
promised, and every command they name is expected to exist.

- Keep the kernel limited to the six concepts its crate doc names: agents,
  channels, membership, immutable messages, deliveries, and principals. Harness
  and workflow semantics belong in adapters or versioned contracts. An inbound
  trigger is a shape of principal, not a seventh concept: its registration is
  durable so its authority can be narrow, and everything it does still arrives
  as an ordinary message.
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
- Put every new module in the crate that owns it: `crates/kernel` stores, above
  it `crates/execution` decides what happens to durable state, and a surface
  exposes it over one mechanism -- `crates/http` and `crates/mcp` are peers,
  both named for a mechanism. `src/` holds only the binary and its command
  surface; `SOURCE_LAYERS` is empty and a new directory there needs a reason
  the boundary test can state.
- A surface is not a home for logic. HTTP, MCP, and the CLI are three ways to
  ask the same question, so whatever answers it belongs below all of them --
  composed over `&Store`, testable with no server running. If a second surface
  would have to reimplement a rule to offer the same feature, the rule is in
  the wrong place. `fleetd status` is one request and a print because
  `execution::health` decides what "current" and "active" mean.
- A surface may provision a transport; `execution` may not. Whoever starts an
  endpoint hands the worker a `TurnGrant`, so arranging a turn never means
  knowing that endpoints can be started.
- Name a new HTTP route domain in `route_domains!` and nowhere else, appending
  rather than inserting: the list fixes the order operations appear in the
  generated contract. Declare a schema no route body mentions beside the type,
  not in the composition module.
- Put an HTTP-surface test in `tests/api_<domain>.rs` for the domain it asserts
  about, over the shared harness in `tests/common/api.rs`. A suite may be large
  when it covers one domain; it may not cover several.
- Never hand-merge a generated artifact. Regenerate an HTTP adapter in its
  external pinned integration and admit its exact candidate first; then run
  `bin/regenerate` for the contract, client, and served bundle in dependency
  order.
- Split a module into a directory once it holds more than one concept, giving
  each concept its own source and its own `impl` block. Reaching a parent's
  private state from a child module is allowed, so the split costs call sites
  nothing.
- Name a new module's source in the boundary assertions by concept, not by file:
  `tests/crate_boundaries.rs` resolves a module to a file or to every source
  under a directory, and splitting one must not shrink what it checks.
- The kernel is `crates/kernel` and holds the only connection pool. A method on
  `Store` may only be written inside that crate; above it, compose with free
  functions over `&Store` and never reach for the pool. The orphan rule enforces
  the first half, which is why a getter the kernel owns is a method and a join
  above it is a function.
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
- Semantic compilers and product-specific facts stay in separately versioned
  external integrations. Fleetd may admit an ordinary generated source
  candidate under exact provenance and behavioral tests, but it imports no
  semantic compiler, fact, plan, provider, or conformance runtime. Generated
  adapters contain mechanism glue only; product behavior remains handwritten.
- Launch plugin executables directly without a shell. Plugin stdout is protocol
  traffic only, and plugins must not receive fleetd credentials or ambient
  environment variables.
- Exercise a proposed plugin interface in at least two real integrations before
  treating its contract as stable.
- Reuse Git, harnesses, parsers, and model servers instead of rebuilding them.
- Number a new ADR by reading `docs/adr/` first, not by incrementing the number
  in the branch it was drafted on. Two branches drafting against the same
  highest ordinal both pick it, and nothing fails: the collision surfaces as two
  files claiming one number. `bin/new-migration` exists because migrations have
  the same failure mode.
- Run `bin/ci` before claiming a change is complete. It mirrors
  `.github/workflows/ci.yml` job for job and adds the checks that only run
  locally; when the two disagree, the workflow is authoritative because it is
  what gates a merge.
- The JavaScript packages are one npm workspace: dependencies install once at
  the root and each package is addressed with `-w`. There is no per-package
  lockfile, and the client's version in `package.json` tracks the contract's
  `info.version`.
