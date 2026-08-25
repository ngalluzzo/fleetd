# fleetd agent instructions

Read `VISION.md`, `README.md`, `docs/ARCHITECTURE.md`, `docs/PROTOCOL.md`, and
`docs/MILESTONES.md` before non-trivial changes.

- Keep the kernel limited to agents, channels, membership, and immutable
  messages. Harness and workflow semantics belong in adapters or versioned
  contracts.
- Preserve unknown message kinds and JSON payload fields.
- Treat SQLite as authoritative; in-memory delivery may be lost and must always
  be recoverable through cursor replay or the durable inbox.
- Preserve at-least-once delivery semantics. New settlement paths must reject
  stale leases and remain safe to retry after a lost response.
- Never edit an applied migration; add a forward migration.
- Never log or persist raw credentials. Agent authority must come from the
  authenticated principal, not caller-supplied identity fields.
- Keep network listeners on loopback until encrypted transport and enrollment
  are explicitly implemented.
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
