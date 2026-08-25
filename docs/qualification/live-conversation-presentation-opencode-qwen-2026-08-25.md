# OpenCode/Qwen conversation presentation qualification — 2026-08-25

## Scope

This record qualifies the complete visible human-to-agent composition at
Fleetd revision `6f4a5d5a4509669244d95ef9550b8fe563cde234`. The served
`/conversation/` page, Bun 1.4.0 WebKit, a continuous Fleetd worker,
`fleetd.harness.opencode`, OpenCode 1.4.0, and the already-running local Qwen
route completed four causal turns across browser, daemon, worker, and harness
replacement.

The credential-free machine record is
[`live-conversation-presentation-opencode-qwen-2026-08-25.json`](live-conversation-presentation-opencode-qwen-2026-08-25.json),
with SHA-256
`8603b930846d2e5320c907a4810ff2d20e95bfe4008a7e1069fa798df10b1719`.
It passed under run ID `a8756c96-d88e-4a6b-b0dd-c12a106f67c2`.

## Exact composition

- Fleetd executable digest
  `sha256:f589f3b357e348124174c1dba0a580ce8f1918898d3236fa04772af3be34c9e3`;
- `fleetd.harness.opencode` 0.1.0 executable digest
  `sha256:eece0145addcc8073c35c07d2d5ab5db5577ef2be4cbdb6b86366f66a328046b`;
- OpenCode 1.4.0 executable digest
  `sha256:3d2c79a23f8a17d7ac35c819fba5bfac9393642de51434896adf7887629cc763`;
- Bun 1.4.0 `WebView` with the explicit `webkit` backend;
- local OpenAI-compatible route for
  `/Users/ngalluzzo/Models/qwen3.8-27b-8bit` at
  `http://127.0.0.1:18082/v1`;
- externally supervised `mlx_vlm.server` with the matching Qwen 8-bit model,
  Qwen 8-bit MTP draft model, draft block size 4, and one server sequence;
- qualification-profile digest
  `sha256:3d2914b73d54951013f650768d2be4ee587b2cbe9dbe890c0580f4ee6eaedf56`;
- plugin profile digest
  `sha256:5dbfae6b4e9d355686844068a5077931b6e5973a8c0d587489026271f221b9cd`;
- compatibility digest
  `sha256:739ae0488c343c89c286eef56eba1a3e8c93b5e9520ff90c8e50fa1acc4abc28`;
  and
- opaque application kinds `conversation.prompt/phase-c-v1` and
  `conversation.result/phase-c-v1`.

The model server was already running, was not started or stopped by the
runner, and remained outside runner process ownership.

## Visible durable conversation

Each phase loaded the actual served page in a fresh ephemeral WebKit view. The
runner selected the channel, entered an exact prompt, and activated Send
through trusted browser input. The resulting page projected all durable
messages through the production browser stream.

| Phase | Browser cursor | Request seq | Result seq | Request | Result |
| --- | ---: | ---: | ---: | --- | --- |
| initial | 0 | 1 | 2 | `5cb06d3a-0530-4775-90a2-ba9357290a57` | `ebe01615-f3d0-429b-aa31-ac2bf6f16872` |
| browser reconnect | 2 | 3 | 4 | `291d0c73-d784-4bb8-b377-0862a3e7cb4b` | `272b7fe5-0ec4-45d0-b787-fa82b0a7ac97` |
| daemon restart | 4 | 5 | 6 | `20bfde78-f32c-4850-a918-cee89ea4c5ba` | `2decd1ea-6734-4c12-8161-af8256c4dab7` |
| worker and harness restart | 6 | 7 | 8 | `1e810b9b-d218-4dd6-8ee2-b85e21170707` | `7d5853d5-f358-4aba-8a5b-e54757a72517` |

For every turn, request and result payloads, correlation, causation, rendered
prose, and inspectable envelopes matched durable history exactly. All four
fresh views selected `fleetd.channel-stream.browser.v1`, made zero page
history-poll requests, persisted no credential, and accepted the complete
projection exactly once. The final cursor was 8 and the human `stream_only`
participant had zero delivery rows.

The final screenshot was 168,010 bytes with SHA-256
`361b7cff994c67c2597a916b634ee4b90765d647862c0242b3449fb59f85591e`.
It showed the live channel, real Qwen results, selected agent, complete durable
cursor, and usable composer inside the 1280 × 800 viewport.

## Restart, session, and cleanup evidence

The run used four browser connections, two daemon processes, two worker
processes, and two OpenCode plugin generations. Both generations stopped
gracefully. The replacement worker adopted binding
`791ea00e-2fe8-4ef2-b6e0-2cb660e2629e` at binding generation 1, advanced the
owner epoch from 1 to 2, and preserved the same native OpenCode session
reference with `runtime_claimed` persistence.

The first three invocations ran under generation
`cd33c5c8-ac25-4211-88b3-5391f63e7d20`; the post-replacement turn ran under
`daa26abe-002e-4c49-bd7a-755a5b68edfb`. Every invocation ended
`end_turn`, `outcome_known`, and quiescent with a distinct event-chain
digest. Both daemon processes and both workers exited with code 0, and the
runner removed its database, credentials, isolated OpenCode home, and
generated worker state.

## Reproduction

```sh
cargo build --workspace
bun run tools/qualify-live-conversation.ts \
  target/live-conversation-opencode-profile.json \
  --presentation \
  --screenshot=/absolute/path/to/presentation.png
```

The schema-1 profile is credential-free and pins the Fleetd revision,
executables, worker bounds, OpenCode route, model identity, message kinds, and
digest-producing plugin configuration. Runtime credentials and isolated state
are generated in an owner-only temporary directory.

## Limits

- This qualifies one local OpenCode/Qwen composition and macOS WebKit, not
  every harness, provider, model, browser engine, or machine.
- It does not qualify provider token accounting, decode throughput, model
  quality, or raw harness transcript durability.
- It does not add or qualify live invocation-event streaming, partial-token
  presentation, a TUI, or remote transport.
- The request and result contracts remain opaque to Fleetd. This run adds no
  GOOIR runtime or semantic interpretation to the daemon.
