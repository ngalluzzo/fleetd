# fleetd agent instructions

Read `README.md`, `docs/ARCHITECTURE.md`, `docs/PROTOCOL.md`, and
`docs/MILESTONES.md` before non-trivial changes.

- Keep the kernel limited to agents, channels, membership, and immutable
  messages. Harness and workflow semantics belong in adapters or versioned
  contracts.
- Preserve unknown message kinds and JSON payload fields.
- Treat SQLite as authoritative; in-memory delivery may be lost and must always
  be recoverable through cursor replay.
- Do not expose the unauthenticated development API beyond localhost.
- Reuse Git, harnesses, parsers, and model servers instead of rebuilding them.
- Run formatting, clippy with warnings denied, and all tests before claiming a
  change is complete.

