# API contract

fleetd has one machine-readable HTTP contract: the OpenAPI 3.1 document at
`openapi/fleetd-v1.json`. A running daemon serves the same document from
`GET /openapi.json`.

The Rust wire types, handler annotations, and `OpenApiRouter` registrations are
the implementation authority. The committed JSON is the language-neutral
artifact consumed by other repositories. A test regenerates it and fails when
the snapshot drifts, so neither side should copy request or response shapes
from prose or infer them from handler bodies.

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

The daemon accepts and emits the shapes in the contract; it does not negotiate
different minor versions per request. Consumers should preserve unknown
message `kind`, `payload`, and envelope data when proxying durable events.

## Authentication and authority

Every `/v1` operation requires `Authorization: Bearer <token>`. The bearer
scheme in OpenAPI expresses authentication; each operation description states
the narrower runtime authority:

| Authority | Permitted surface |
| --- | --- |
| Public | Health and contract discovery |
| Operator | Agent, credential, channel, membership, block, and invocation administration |
| Agent | Message append; history and stream access for member channels |
| Bound agent | Claim and settle only that agent's deliveries; reserve, arm, and complete only that agent's invocations |

Authentication failures return `401` and a Bearer challenge. An authenticated
principal with insufficient authority returns `403`. fleetd domain failures
use the generated `ErrorResponse` JSON envelope. Framework-level malformed
path, query, or JSON extraction failures may be plain HTTP rejection bodies and
must not be decoded as `ErrorResponse` without checking the content type.

## WebSocket stream

`GET /v1/channels/{channel_id}/stream` is an HTTP WebSocket upgrade, not a
normal Fetch request. Its OpenAPI operation carries `x-fleetd-websocket`, which
defines the text-frame direction and references the canonical `Message`
schema. On connection, the server replays messages with `seq > after` in
ascending order and then sends live frames. Each server frame is exactly one
JSON `Message`. Clients reconnect with the highest sequence they have durably
processed.

The TypeScript generator removes operations carrying this extension. That is
intentional: a generated Fetch function would not perform the upgrade. UI
stream code should construct a WebSocket URL, pass the same bearer credential
during the upgrade in environments that permit custom headers, and decode
frames as the generated `Message` type. Browser credential transport needs an
explicit server design before browser UIs connect directly; tokens must not be
placed in query strings by convention.

## Internal boundaries

Durable harness session bindings, owner epochs, and plugin lifecycle calls are
currently internal Rust APIs. Their types are deliberately absent from the
HTTP contract. A UI must not infer endpoints for them; exposing that state will
be a separate API issue with an explicit operator read model.

## Changing the API

For any wire change:

1. Change the Rust DTO and annotated handler together.
2. Add or update behavioral API tests.
3. Regenerate `openapi/fleetd-v1.json`.
4. Regenerate and typecheck `clients/typescript`.
5. Review both generated diffs for compatibility and run `bin/ci`.

Do not directly patch the OpenAPI JSON or generated TypeScript to make a UI
compile. A mismatch there means the server authority or generator policy needs
to change.
