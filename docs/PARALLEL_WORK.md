# Parallel work

Several agents can change this codebase at once. They do not collide in logic —
the layers are enforced and each domain owns its handlers. They collide on
**registries** and on **regenerated blobs**: the shared lists every vertical
slice has to edit, and the derived files every change rebuilds.

This document names those points and says what to do about them.

## What a feature actually touches

A change to the API surface is a vertical slice through every layer. Measured
over the API surface's history, one such commit is a median of **16 files**, and
co-changes with the wire model (47%), the kernel store (47%), migrations (36%),
the generated contract (31%), and the generated TypeScript client (26%).

So the question is not whether the layering is right. It is how many *shared*
files a slice is forced through on its way out.

## Derived artifacts are never merged

Five paths are generated, and each is generated from the one above it:

    the handlers            ->  openapi/fleetd-v1.json
    the contract           ->  clients/typescript/src/generated/**
    the client             ->  web/conversation/conversation.{js,css}

They are committed because `bin/ci` verifies each still matches its source and
because the daemon embeds the conversation bundle with `include_str!`. Two
concurrent changes therefore both rebuild them.

`.gitattributes` marks these paths unmergeable, so Git leaves the file intact
and flags a conflict rather than interleaving two regenerated blobs — a text
merge of a 4,000-line generated document produces something that is neither
side and does not parse. Resolve by regenerating:

    bin/regenerate

Run it in that order or not at all; the stages consume each other, and building
them out of order produces artifacts that disagree.

## One file per concept

`crates/kernel/src/store/` is a directory, not a file. Each concept — agents,
channels, membership, messages, and the conversation projection over them —
owns its own source and adds its own `impl Store` block. The pool, the
migration set, and the handful of genuinely shared helpers stay in `mod.rs`.

Those blocks reach `Store`'s private fields directly because they are
descendants of the module that defines it. That is what makes the shape free:
every `store.method()` call in the workspace resolves exactly as it did when
this was one file.

Prefer this shape when a module starts holding more than one concept.
`tests/crate_boundaries.rs` resolves a module to a file *or* to every source
under a directory, so splitting one never shrinks what the boundary assertions
cover.

## Migrations are named by timestamp

`bin/new-migration <description>` creates the file. The name carries a UTC
timestamp, because a sequential ordinal is the one collision that hides: two
authors both reach for `0011`, both builds pass — `sqlx::migrate!()` does not
check for duplicate versions — and then a database refuses to migrate with

    UNIQUE constraint failed: _sqlx_migrations.version

which names neither file. `tests/migrations.rs` now rejects a duplicate version
and rejects a new ordinal outright, so the collision surfaces as a test failure
that names both files instead.

The first ten migrations keep their ordinals. Renaming an applied migration
changes the version its checksum is recorded under, and every existing database
would reject it.

## A route domain is named once

`src/http/mod.rs` composes the contract and owns nothing else. Adding an
authenticated domain is one line:

    route_domains!(agents, channels, messages, streams, deliveries, ...)

That list expands to both the module declarations and the merge chain, so a
domain cannot be declared without being reachable — which used to be two lists
that could disagree. Its order still fixes the order operations appear in the
generated document, so append rather than insert.

Schemas that no route body mentions are declared beside the types instead. The
browser edge speaks WebSocket frames, so its nine types are registered by
`browser_stream_edge::Schemas` rather than by a list in the composition module.

The `tags(...)` list stays where it is. Tags are how the document groups
operations, not a property of a domain: `channels`, `messages`, and `streams`
all publish under the `channels` tag, and neither `messages` nor `streams` added
a tag when they arrived. A genuinely new resource family adds one line here; a
new domain usually adds none.

## An API suite per domain

`tests/api_<domain>.rs` holds the HTTP-surface tests for one domain, pairing with
`src/http/<domain>.rs`. They share `tests/common/api.rs`, which starts a daemon
with an operator credential and offers the moves every suite needs — register an
agent, open a channel, send a message, claim an inbox.

This replaced one 970-line suite that was cohesive by concern and spread across
six domains, so every domain owner had to edit it. `tests/browser_stream.rs` is
larger still and was deliberately left alone: all of its tests are the browser
edge, and big-but-cohesive costs a fleet nothing.

## Known chokepoints

Ranked by what they cost a fleet. All but the last are addressed above.

| Chokepoint | Status |
| --- | --- |
| Derived artifacts (~15k lines) | unmergeable + `bin/regenerate` |
| `store.rs` holding five concepts | split per concept |
| Migration ordinals | timestamp naming, enforced by test |
| `src/http/mod.rs` | one `route_domains!` list; schemas moved to their owner |
| Cross-domain test suites | one `tests/api_<domain>.rs` per domain over a shared harness |
| `docs/**` | **open** — 78% co-change, mostly shared documents rather than per-domain ones |

## Owning a slice

Once the chokepoints above are gone, these slices are close to disjoint, and
are the natural unit of ownership:

    agents                 identity and credentials
    channels + messages    durable conversation state
    deliveries             the leased inbox
    invocations            the managed-turn fence
    sessions + plugins     harness ownership and process lifecycle
    streams                live transport and the browser edge
    clients + web          the generated client and the served UI

Two agents in the same slice should expect to serialise. Two in different
slices should not have to.
