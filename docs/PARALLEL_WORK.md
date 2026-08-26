# Parallel work

Several agents can change this codebase at once. They do not collide in logic:
every layer is a crate, and the dependency graph is a compile error rather than
a convention. They collide on **registries** and on **regenerated blobs** — the
shared lists a vertical slice has to edit, and the derived files every change
rebuilds.

This document names those points and says what to do about them.

## What the compiler holds

    proto ← kernel ← conversation
                   ← execution ← http
                               ← mcp

Each `[dependencies]` block states one permitted direction. Nothing else is
needed to keep these apart, and the direction is checked by trying it rather
than asserted:

    execution naming a surface        error[E0432]: unresolved import `fleetd_http`
    one surface naming the other      error[E0432]: unresolved import `fleetd_http`
    a layer adding a Store method     error[E0116]: impl for a type outside the crate
    the kernel naming its projection  error[E0432]: no external crate `fleetd_conversation`

`src/` holds the binary and nothing else. A directory appearing there means a
layer was added to the daemon instead of beside it, which is why `SOURCE_LAYERS`
in `tests/crate_boundaries.rs` is an empty list.

**HTTP and MCP are peers.** Both are named for a mechanism, because that is what
they are: two ways to reach the same decisions, not two domains. Neither can see
the other, and `execution` can see neither — a surface provisions a transport
and hands the worker a `TurnGrant`, so nothing below a surface can bind a port.
A third transport is a new crate beside them, not a fork of what they contain.

## What a feature actually touches

A change to the API surface is a vertical slice through every layer. Measured
over the nineteen commits that touched `src/api.rs`, the single file the surface
lived in before it was split, one such commit is a median of **16 files**, and
co-changes with the wire model (47%), the kernel store (47%), migrations (36%),
the generated contract (31%), and the generated TypeScript client (26%).

Those numbers describe the tree before the crates existed, and they are the
reason the crates exist. The question was never whether the layering was right.
It is how many *shared* files a slice is forced through on its way out.

## Derived artifacts are never merged

Three paths are generated, and each is generated from the one above it:

    the handlers  ->  openapi/fleetd-v1.json
    the contract  ->  clients/typescript/src/generated/**
    the client    ->  web/conversation/conversation.{js,css}

About 15,000 lines in total. They are committed because `bin/ci` verifies each
still matches its source and because the daemon embeds the conversation bundle
with `include_str!`, so two concurrent changes both rebuild them.

`.gitattributes` marks these paths unmergeable: Git leaves the file intact and
flags a conflict rather than interleaving two regenerated blobs, because a text
merge of a 4,000-line generated document produces something that is neither side
and does not parse. Resolve by regenerating:

    bin/regenerate

In that order or not at all — the stages consume each other, and building them
out of order produces artifacts that disagree.

## One file per concept

`crates/kernel/src/store/` is a directory. Each concept the substrate owns —
agents, channels, membership, messages — has its own source and its own
`impl Store` block. The pool, the migration set, and the few genuinely shared
helpers stay in `mod.rs`.

Those blocks reach `Store`'s private fields directly because they are
descendants of the module that defines it, which is what makes the shape free:
every `store.method()` in the workspace resolves as it did when this was one
file.

A *conversation* is not one of those concepts. It is `crates/conversation`, a
read model that presents a channel, its membership, and how recently anything
was said. Opening a direct pair writes substrate tables and stays in the kernel;
presenting the result is the projection's word, and a caller that wants both
composes them.

Prefer a directory when a module starts holding more than one concept. Prefer a
crate when the thing it holds could be replaced, or must not reach something.

## Migrations are named by timestamp

`bin/new-migration <description>` creates the file. The name carries a UTC
timestamp, because a sequential ordinal is the one collision that hides: two
authors both reach for `0011`, both builds pass — `sqlx::migrate!()` does not
check for duplicate versions — and then a database refuses to migrate with

    UNIQUE constraint failed: _sqlx_migrations.version

which names neither file. `tests/migrations.rs` rejects a duplicate version and
rejects a new ordinal outright, so the collision surfaces as a test failure that
names both files instead.

The first ten migrations keep their ordinals. Renaming an applied migration
changes the version its checksum is recorded under, and every existing database
would reject it.

## A route domain is named once

`crates/http/src/lib.rs` composes the contract and owns nothing else. Adding an
authenticated domain is one line:

    route_domains!(agents, channels, messages, streams, deliveries, ...)

That list expands to both the module declarations and the merge chain, so a
domain cannot be declared without being reachable — which used to be two lists
that could disagree. Its order fixes the order operations appear in the
generated document, so append rather than insert.

Schemas no route body mentions are declared beside the types instead. The
browser edge speaks WebSocket frames, so its nine types are registered by
`browser_stream_edge::Schemas` rather than by a list in the composition module.

The `tags(...)` list stays. Tags group operations in the document and are not a
property of a domain: `channels`, `messages`, and `streams` all publish under the
`channels` tag, and neither `messages` nor `streams` added a tag when they
arrived. A genuinely new resource family adds one line here; a new domain
usually adds none.

## An API suite per domain

`tests/api_<domain>.rs` holds the HTTP-surface tests for one domain, pairing with
`crates/http/src/<domain>.rs`. They share `tests/common/api.rs`, which starts a
daemon with an operator credential and offers the moves every suite needs:
register an agent, open a channel, send a message, claim an inbox.

This replaced one 970-line suite that was cohesive by concern and spread across
six domains, so every domain owner had to edit it. `tests/browser_stream.rs` is
larger still and was deliberately left alone: all of its tests are the browser
edge, and big-but-cohesive costs a fleet nothing.

## What a test still has to read source for

`tests/crate_boundaries.rs` still reads source, for two different reasons. The
note at the top of that file records which rules were retired when the layers
became crates, and what holds each now.

Three are dependency rules a crate boundary cannot reach:

- **Only the kernel writes kernel tables.** `Store::pool()` and
  `Store::begin_immediate()` both return a sqlx executor, and an executor
  accepts any statement, so no signature distinguishes reading a table a layer
  owns from deleting one it does not. `begin_immediate` cannot be withdrawn
  either: a delivery transition and the invocation fence settling it have to
  commit together. Separate databases would make this structural and would also
  make that shared transaction impossible.
- **Route domains inside `fleetd-http` must not import each other.** They are
  modules of one crate, so only a text check separates them. Making each a crate
  would fix that and is almost certainly not worth it.
- **`fleetd-proto` names no persistence or transport.** Its dependency list
  covers `sqlx`, `axum`, `tokio`, and `reqwest`, but the same check also forbids
  `std::fs` and `std::net` — and nothing in a manifest can catch those, because
  the standard library needs no entry.

The rest are inventory and style: the declared layers matching the tree, every
route domain appearing in `route_domains!`, the crate root re-exporting modules
rather than items, and the JavaScript packages importing each other by name.
None of those was ever going to be a type.

Adding a crate above the substrate means adding it to `ABOVE_SUBSTRATE_CRATES`.
A crate missing from that list is unchecked, not compliant.

## Known chokepoints

Ranked by what they cost a fleet. All but the last are addressed above.

| Chokepoint | Status |
| --- | --- |
| Derived artifacts (~15k lines) | unmergeable + `bin/regenerate` |
| Layers that were only conventions | every layer is a crate |
| `store.rs` holding five concepts | one source per concept; the projection is its own crate |
| Migration ordinals | timestamp naming, enforced by test |
| Contract composition | one `route_domains!` list; schemas moved to their owner |
| Cross-domain test suites | one `tests/api_<domain>.rs` over a shared harness |
| `docs/**` | **open** — 78% co-change, mostly shared documents rather than per-domain ones |

## Owning a slice

These slices are close to disjoint, and are the natural unit of ownership:

    agents                 identity and credentials          kernel, http
    channels + messages    durable conversation state        kernel, conversation, http
    deliveries             the leased inbox                  kernel, execution, http
    invocations            the managed-turn fence            execution, http
    sessions + plugins     harness ownership and lifecycle   execution, plugin-host, acp-host
    streams                live transport and browser edge   http
    publishing             invocation-scoped messages        execution, mcp
    clients + web          the generated client and the UI   clients/, apps/

Two agents in the same slice should expect to serialise. Two in different slices
should not have to.
