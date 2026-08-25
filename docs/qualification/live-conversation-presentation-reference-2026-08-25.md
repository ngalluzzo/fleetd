# Served conversation presentation qualification — 2026-08-25

## Scope

This record qualifies Fleetd's served conversation presentation at revision
`19292dfdc98ef0bebb26fa846a73b9308aa6e515`. A real Bun 1.4.0 WebKit view
loaded `/conversation/`, selected the channel, entered each prompt, and
submitted it through trusted browser input. Four causal turns passed through a
real continuous worker and the deterministic ACP reference plugin across a
fresh browser view, daemon replacement, and worker plus plugin replacement.

The credential-free machine record is
[`live-conversation-presentation-reference-2026-08-25.json`](live-conversation-presentation-reference-2026-08-25.json),
with SHA-256
`34007e807d6c5a91d767f6bbd86f17dfaa8f5501c9513446ff003f25b31fcec6`.
It passed under run ID `e8ffa6e9-d8b7-451b-a64b-d57376002040`.

## Exact composition

- Fleetd executable digest
  `sha256:95daaafb8b27802da249e558572f8275fccbdec820acb35f2049b2a1b60fa315`;
- `fleetd.acp-reference` 0.1.0 executable digest
  `sha256:aac9ad752d8ea54d2b4c0f74ff145b977218244098634065de82505a59064b5c`;
- mock ACP runtime 1.0.0 executable digest
  `sha256:d3b45fed65740984ba39e77c6366ca41bd4511743dc7f9a4fb6db57bc8183bc0`;
- Bun 1.4.0 `WebView` with the explicit `webkit` backend and a fresh
  ephemeral data store for every phase;
- qualification-profile digest
  `sha256:1944831c4be1efedc2456ec1dcb48920d4f80069b9469bd35dd82e61e7a18117`;
- compatibility digest
  `sha256:1daa99c8f7c3c6a753737caee7438571d429a4726b520650de9076a4d00aba2b`;
  and
- opaque application kinds `conversation.prompt/reference-v1` and
  `conversation.result/reference-v1`.

The reference plugin exercises the production continuous-worker, invocation,
observation, durable-session-binding, and owner-epoch paths while providing a
deterministic reply. This isolates presentation correctness from model quality
or provider availability; the separate OpenCode/Qwen record remains the
real-model qualification.

## Procedure and rendered conversation

The runner created a fresh SQLite catalog, daemon, human `stream_only`
participant, worker `inbox` participant, and channel through public Fleetd
operations. Each phase loaded the actual served page in a fresh WebKit view,
connected through its public bootstrap, selected the channel, typed an exact
marker-bearing prompt, and activated the Send control. Page instrumentation
recorded the click, input, and submission events as trusted.

| Phase | Browser cursor | Request seq | Result seq | Request | Result |
| --- | ---: | ---: | ---: | --- | --- |
| initial | 0 | 1 | 2 | `dcbbecf0-8f70-4115-91ba-3890b50e65d3` | `4e8fbc4c-cd36-4494-883f-ffc10e4394f4` |
| browser reconnect | 2 | 3 | 4 | `46f15def-6306-46e1-9eab-f97f55ba1ae4` | `6ccc8b6b-db3d-4f9f-b13e-d2235aa218e9` |
| daemon restart | 4 | 5 | 6 | `af1dc1f3-df51-4e1e-92ee-ac5961319b27` | `b39cce33-51dd-4584-8b3c-6cef714d374b` |
| worker and plugin restart | 6 | 7 | 8 | `f1413212-3c9a-4e8d-8a8e-dccb580e0593` | `c6712987-a8f8-4a3b-b9ef-963a21e4e98c` |

For all four phases, the rendered human request, assistant result, and
expandable JSON envelope matched the corresponding durable messages exactly.
Correlation and result causation were preserved. The final durable cursor was
8, the complete rendered projection contained all eight accepted message IDs,
and the passive human participant had zero delivery rows.

## Browser-boundary evidence

Every page selected only `fleetd.channel-stream.browser.v1` at the fixed,
secret-free `/v1/browser/channel-stream` URL. The page made zero HTTP message
history reads: replay and continuation came through the browser stream, while
the runner's out-of-page public history read served only as the acceptance
oracle.

After each turn, the runner checked the complete rendered DOM, current URL,
cookies, local storage, session storage, and enumerated IndexedDB databases.
Neither long-lived credential appeared in the DOM or URL, and all persistent
browser stores remained empty. Each view used an ephemeral WebKit data store.

The optional final screenshot was 166,229 bytes with SHA-256
`e6101d75d1f5ef5919576355d0502dcb44b0376ca7739ff36826985442bb94c2`.
It showed the complete channel, live status, eight durable messages, selected
worker, and usable composer inside the declared 1280 × 800 viewport. The
screenshot is visual corroboration and is not conversation authority.

## Restart and cleanup evidence

The run used four browser connections, two daemon processes, two worker
processes, and two plugin generations. Both generations stopped gracefully.
The replacement worker adopted binding
`0be19a22-babc-4ad3-8d0e-4e4b73b603f0` at binding generation 1, advanced its
owner epoch from 1 to 2, and preserved the native session reference with
`runtime_claimed` persistence.

All four invocations ended `end_turn`, `outcome_known`, and quiescent with
distinct event-chain digests. Both daemon processes and both workers exited
with code 0. The runner removed its temporary database, credentials, and
generated worker state.

## Reproduction

Build the exact revision, create a schema-1 profile from the checked-in
example, and run:

```sh
cargo build --workspace
bun run tools/qualify-live-conversation.ts \
  target/live-conversation-reference-profile.json \
  --presentation \
  --screenshot=/absolute/path/to/presentation.png
```

Omitting `--presentation` retains the presentation-free qualification path.
That path also passed immediately after this run against the same revision,
executable, reference plugin, four restart phases, and final cursor.

## Limits

- This deterministic run qualifies the presentation boundary, not provider or
  model quality. The OpenCode/Qwen product-loop record covers the real-model
  composition.
- It exercises macOS WebKit through Bun 1.4.0, not Chromium or another browser
  engine.
- The visible Electrobun host was built and smoke-tested separately; this
  record qualifies the page it hosts rather than native window packaging.
- It does not add or qualify a live operator-event stream, partial-token
  rendering, execution status, model throughput, or a TUI target.
- The application contracts remain opaque to Fleetd, and this qualification
  introduces no GOOIR runtime or semantic interpretation into the daemon.
