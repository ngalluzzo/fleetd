# API contract

fleetd has one machine-readable HTTP contract: the OpenAPI 3.1 document at
`openapi/fleetd-v1.json`. A running daemon serves the same document from
`GET /openapi.json`.

The Rust wire types and admitted route adapters are the implementation
authority. An adapter's Utoipa annotation and `OpenApiRouter` registration may
be generated from an external native HTTP contract and exact implementation
bindings; its called operation remains handwritten Fleetd behavior. The
committed JSON is the language-neutral artifact consumed by other repositories.
A test regenerates it and fails when the snapshot drifts, so neither side should
copy request or response shapes from prose or infer them from operation bodies.

## Consumer workflow

The checked-in Fetch client and TypeScript types live in
`clients/typescript/src/generated`. They are generated code and must not be
edited.

```sh
cargo run --quiet --bin export-openapi
cd clients/typescript
npm ci
npm run generate
npm run typecheck
```

UI work in another repository may either consume the committed OpenAPI JSON
with its own pinned generator or copy the `clients/typescript` package. Stable
`operationId` values are the generated function names. JSON fields retain the
server's snake_case spelling; UI view models may map them at the presentation
boundary, but transport code should not.

The TypeScript generator and compiler are exact-pinned because both are still
moving quickly. Upgrade them deliberately, regenerate, typecheck, and review
the diff as a dependency change.

## Versioning

`/v1` and `fleetd-v1.json` identify the wire-contract generation. Additive,
backward-compatible operations or optional fields increment the OpenAPI info
minor version. A required-field change, removal, semantic reinterpretation, or
incompatible response change requires a new major contract and URL prefix.

"Required-field change" means an existing field's requiredness changing, or a
new required field on a *request*, which a caller would then have to supply.
A new required field on a *response* is a minor version: a consumer generated
against the older contract ignores it, and a consumer generated against the
newer one is versioned to the contract it came from — `verify:generated` fails
the build when the client's version and the contract's disagree, so the pairing
that would break, a newer client against an older daemon, is one this repository
already does not ship.

Cursor-addressed listings stay plain arrays. A page wrapper would be an
incompatible response change under the rule above, and every evidence row
already carries both halves of its own cursor, so the position a caller
resumes from is read off the last row rather than returned beside it.

The daemon accepts and emits the shapes in the contract; it does not negotiate
different minor versions per request. Consumers should preserve unknown
message `kind`, `payload`, and envelope data when proxying durable events.

## Authentication and authority

Every `/v1` operation except the dedicated browser WebSocket upgrade requires
`Authorization: Bearer <token>`. The bearer scheme in OpenAPI expresses
authentication; each operation description states the narrower runtime
authority:

| Authority | Permitted surface |
| --- | --- |
| Public | Health, contract discovery, and the origin-bound browser upgrade before grant redemption |
| Operator | Agent, credential, channel, membership, block, and invocation administration |
| Agent | Message append; history and stream access for member channels |
| Bound agent | Claim and settle only that agent's deliveries; reserve, arm, and complete only that agent's invocations |

Authentication failures return `401` and a Bearer challenge. An authenticated
principal with insufficient authority returns `403`. fleetd domain failures
use the generated `ErrorResponse` JSON envelope. Framework-level malformed
path, query, or JSON extraction failures may be plain HTTP rejection bodies and
must not be decoded as `ErrorResponse` without checking the content type.

## Channel membership

`CreateChannel.member_ids` remains the backward-compatible shorthand for
`inbox` membership. `CreateChannel.members` accepts exact `agent_id` and
`delivery_mode` pairs, where the closed modes are `inbox` and `stream_only`.
`AddMember.delivery_mode` is optional and defaults to `inbox`; an exact replay
returns success while an attempted mode change returns `409 Conflict`.

`GET /v1/channels/{channel_id}/members` returns the bounded channel ID, agent
ID, agent name, join timestamp, and delivery mode. Operators may inspect any
existing channel. An agent may inspect only a channel of which it is a member.
The response deliberately omits opaque agent metadata and credential state.
Delivery mode affects only durable inbox snapshot creation, never message
visibility or cursor replay.

## Conversation lifecycle

`POST /v1/channels` creates an active `shared` conversation. Operators rename
one through `PATCH /v1/channels/{channel_id}` and archive one idempotently
through `POST /v1/channels/{channel_id}/archive`. Archive preserves membership
and history but closes the conversation to new messages and membership writes.

`POST /v1/direct-conversations` accepts exactly two distinct
`CreateChannelMember` values. The unordered agent pair is a durable unique key:
the first open returns `201 Created`, while the same pair and exact delivery
modes return the existing conversation with `200 OK`, including under
concurrent requests. Direct membership, delivery modes, and identity are fixed;
direct conversations cannot use the shared rename, archive, or add-member
operations.

`GET /v1/conversations` is the operator discovery surface for both kinds. Its
`ConversationSummary` includes the durable channel fields, bounded member
projections, and optional latest-message sequence and timestamp. It omits
archived shared channels unless `include_archived=true` is supplied.

## WebSocket stream

`GET /v1/channels/{channel_id}/stream` is an HTTP WebSocket upgrade, not a
normal Fetch request. Its OpenAPI operation carries `x-fleetd-websocket`, which
defines the text-frame direction and references the canonical `Message`
schema. On connection, the server replays messages with `seq > after` in
ascending order and then sends live frames. Each server frame is exactly one
JSON `Message`. Clients reconnect with the highest sequence they have durably
processed.

The TypeScript generator removes operations carrying this extension. That is
intentional: a generated Fetch function would not perform the upgrade. Native
stream code constructs a WebSocket request with the same bearer credential and
decodes frames as the generated `Message` type.

The stable browser-equivalent edge mints a single-use grant through the
authenticated `POST /v1/channels/{channel_id}/stream-grants` operation, with
`Cache-Control: no-store`, then redeems it as the first application frame on
the origin-bound `GET /v1/browser/channel-stream` WebSocket. The upgrade itself
has no bearer header and is the only `/v1` operation outside ordinary bearer
middleware. Its tagged ready/message frames and first-client-message schema are
published in `x-fleetd-websocket`; the Fetch client generator omits both
WebSocket operations.

The handwritten TypeScript browser adapter exports this exact linkage from
`@fleetd/client`. It advances its cursor only after consumer acceptance,
reconnects with bounded duplicate tolerance, and fails closed without an HTTP
history fallback. The stable wire contract is
[`browser-channel-stream-v1.md`](contracts/browser-channel-stream-v1.md).

## Operational read models

Durable plugin generations, session bindings and owner epochs, and bounded
invocation observations have explicit operator-only read models. Invocation
observations include source and optional result message IDs so external tools
can join control evidence to immutable message causation without receiving
lease or fence credentials. Plugin lifecycle calls and session mutation remain
internal controller APIs.

`GET /v1/deliveries` is the operator's bounded read-only inbox projection. It
may be filtered by exact agent and state, reports whether a persisted lease has
expired, and never returns lease tokens. `GET /v1/invocations/{id}/trace`
performs one exact durable join across the invocation, source/result messages,
bounded observation, plugin generation, and native-session binding. A reserved
invocation legitimately has no observation or execution-side evidence yet.

`GET /v1/fleet-health` composes those read models into one operator answer: the
current plugin generation per agent, the current generation of each session
binding, the invocations still owed an outcome, and a bounded delivery census.
The daemon composes it, so the CLI and any later surface report the same thing
rather than each deciding what "current" means. Its census reports how many
rows it inspected, so a bounded read is visible as a bound.

## Changing the API

For any wire change:

1. Change the Rust DTO and handwritten operation behavior.
2. For a generated adapter, change its external native contract or bindings,
   compile the exact candidate, and admit its digest; never patch it locally.
3. Add or update behavioral API tests.
4. Regenerate `openapi/fleetd-v1.json`.
5. Regenerate and typecheck `clients/typescript`.
6. Review all generated diffs for compatibility and run `bin/ci`.

Do not directly patch the OpenAPI JSON or generated TypeScript to make a UI
compile. A mismatch there means the server authority or generator policy needs
to change.
