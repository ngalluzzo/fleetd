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
- Reuse Git, harnesses, parsers, and model servers instead of rebuilding them.
- Run formatting, clippy with warnings denied, and all tests before claiming a
  change is complete.
