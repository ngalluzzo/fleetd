# Live human-to-agent conversation qualification — 2026-08-25

## Scope

This record qualifies the presentation-free Phase C product loop at Fleetd
revision `06924a106c7da9bd6f704a552bc3fbf5a41da485`. A real human participant
credential sent four turns through the production browser-stream client, a
continuous Fleetd worker, the OpenCode harness plugin, OpenCode 1.4.0, and the
already-running local Qwen route. Every result returned through the same
WebKit stream as one immutable causal Fleetd message.

The exact credential-free machine record is
[`live-human-agent-conversation-2026-08-25.json`](live-human-agent-conversation-2026-08-25.json),
with SHA-256
`03e134ecddb5e533f1e3611ef6cb1a5f0ecf107b3460f549eb619879aff08df0`.
It passed under run ID `d7e209d0-41c8-4f42-95a1-a8a93e3344d1`.

## Exact composition

- Fleetd executable digest
  `sha256:a5614204c043d753263bb38b6722c3f83b02154c01c792d61b3e526f19d4cba9`;
- `fleetd.harness.opencode` 0.1.0 executable digest
  `sha256:ebbba94f3fac3a6799f4ee0ace96b710308f845c8a6f36e190b164cf55ebc200`;
- OpenCode 1.4.0 executable digest
  `sha256:3d2c79a23f8a17d7ac35c819fba5bfac9393642de51434896adf7887629cc763`;
- Bun 1.4.0 `WebView` with the explicit `webkit` backend;
- local OpenAI-compatible route
  `fleet-local//Users/ngalluzzo/Models/qwen3.8-27b-8bit` at
  `http://127.0.0.1:18082/v1`;
- externally supervised `mlx_vlm.server` with the matching Qwen 8-bit model,
  Qwen 8-bit MTP draft model, draft block size 4, and one server sequence;
- qualification-profile digest
  `sha256:6315fcbc77a3b28fed5a729f73f9af4c909120e44b5262e2e4d436a2e1e1786f`;
- plugin profile digest
  `sha256:5dbfae6b4e9d355686844068a5077931b6e5973a8c0d587489026271f221b9cd`;
- compatibility digest
  `sha256:739ae0488c343c89c286eef56eba1a3e8c93b5e9520ff90c8e50fa1acc4abc28`;
  and
- opaque application kinds `conversation.prompt/phase-c-v1` and
  `conversation.result/phase-c-v1`.

The runner created an isolated OpenCode home and generated all Fleetd
credentials in a temporary owner-only directory. The profile and emitted
artifact contained no credential. The model server was already running, was
not started by the runner, and remained outside runner process ownership.

## Procedure and durable conversation

The runner created a fresh SQLite catalog, daemon, human `stream_only`
participant, worker `inbox` participant, and channel through public Fleetd
operations. For each turn it loaded the exported production browser client in
a fresh native WebKit view, minted a single-use browser stream grant, sent the
prompt with the human participant credential, and accepted the causal result
from that stream. Operator authority was used only for administration and the
exact read models.

| Phase | Browser cursor | Request seq | Result seq | Request | Result |
| --- | ---: | ---: | ---: | --- | --- |
| initial | 0 | 1 | 2 | `4237e185-c593-4d00-a11e-05447b3ea87e` | `f4f37dce-ad76-46ed-bb0e-6d2873d813d7` |
| browser reconnect | 2 | 3 | 4 | `6df001ab-3305-47b6-9628-0c18c002ba22` | `f57344f2-3290-49c9-b516-9d6b8b8c4942` |
| daemon restart | 4 | 5 | 6 | `8b50244b-5814-485a-a445-b73fd5095d4d` | `623c4d10-ab52-4e07-a616-6f87f02092b4` |
| worker and harness restart | 6 | 7 | 8 | `22edb337-936f-4cc6-9e90-5e76ac64210e` | `029e2904-108a-4a11-92f7-1852cc8fe53a` |

All four WebKit connections selected
`fleetd.channel-stream.browser.v1`. Every accepted request/result pair matched
the public durable history exactly. Correlation, result causation, opaque
request extension values, result payloads, stable IDs, and sequence order were
preserved without product-specific parsing or normalization.

## Restart and session evidence

The run used four browser connections, two daemon processes, two worker
processes, and two harness-plugin generations. Both plugin generations stopped
gracefully. The replacement worker adopted binding
`7a30e545-64dd-4e71-8798-94ea044b6c82` at binding generation 1, advanced its
owner epoch from 1 to 2, and preserved the same native OpenCode session
reference with `runtime_claimed` persistence.

The first three turns ran under generation
`45484cdd-9264-4580-b145-203623f010ee` at owner epoch 1. The post-replacement
turn ran under generation `1cf6c4c8-c889-4798-837c-9442d3707cfe` at owner
epoch 2. Every invocation ended `end_turn`, `outcome_known`, and quiescent with
a distinct event-chain digest. The observations contained 2, 4, 4, and 6
bounded events respectively; unknown event categories remained counted rather
than being discarded or reinterpreted.

## Delivery and cleanup proof

The runner compared browser acceptance with public history and then opened the
catalog read-only. The final durable cursor was 8 and the human participant had
exactly zero delivery rows, proving that four addressed results remained
replayable without becoming leased work for a `stream_only` member.

Both daemon processes and both worker-owned plugin process groups exited with
code 0. The runner removed its temporary database, credentials, and generated
worker state. No runner-owned Fleetd, worker, plugin, ACP, or WebView process
remained. The external model server was deliberately neither stopped nor
claimed as runner-owned.

## Reproduction

The exact revision was built before the run, then qualified with:

```sh
cargo build --workspace
bun run tools/qualify-live-conversation.ts \
  target/live-conversation-opencode-profile.json \
  > target/live-conversation-opencode-evidence.json
```

The target profile followed
[`live-conversation-profile.example.json`](../../tools/live-conversation-profile.example.json)
and pinned every path, runtime version, digest-producing configuration value,
worker bound, model route, and message kind listed above.

## Limits

- This qualifies Fleetd's operational substrate, not a browser, native GUI, or
  TUI presentation.
- It does not add or qualify a replayable live operator-event stream. No
  polling or synthetic typing messages were used as a substitute.
- It does not qualify provider token accounting, model quality, decode
  throughput, or raw harness transcript durability.
- The request and result contracts remain opaque to Fleetd; application-level
  semantic conformance is outside this claim.
- It does not introduce GOOIR or any semantic compiler into Fleetd.
