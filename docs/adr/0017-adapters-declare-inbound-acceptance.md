# ADR 0017: Adapters declare inbound message acceptance

- Status: accepted for experimental dogfood
- Date: 2026-08-24

## Context

The first two-seat message-capability run showed that outbound authority and
lineage were correct, but the semantic-neutral envelope worker reserved every
addressed delivery. Managed completion publishes a result to the input sender,
so two unbounded seats could mistake generic completion envelopes for fresh
requests and create a response loop.

Leasing a non-match and immediately releasing it would inflate attempts and
continually revisit the oldest delivery. Reading first and reserving by message
ID would introduce a race. Registering workflow meanings in the kernel would
violate the open message protocol and couple storage to application contracts.

## Decision

Every turn adapter declares versioned inbound acceptance. V1 is a bounded,
non-empty set of exact message kinds. The envelope adapter receives that set in
worker desired state; semantically strict adapters may define their set as
part of their own versioned implementation.

The trusted worker supplies the declaration to an internal atomic reservation
path. SQLite joins delivery rows to immutable messages and leases only the
oldest eligible exact-kind match. Earlier non-matches remain pending with no
attempt increment. Kind equality is an opaque envelope selector: storage does
not interpret or register its meaning, and all public unfiltered claim paths
remain unchanged.

Kind matching is not semantic acceptance. After reservation, the adapter still
validates the full payload and correlation before an invocation can arm. A
matching malformed contract therefore fails closed under the existing safe
pre-arm settlement behavior.

The worker desired-state schema advances from 1 to 2 and requires an explicit
adapter. Envelope configuration embeds inbound acceptance schema 1. The
acceptance version and canonical set are included in native-session
compatibility, so a changed input policy rotates the binding generation.

## Consequences

- Mutually addressed continuous seats can ignore each other's generic results
  without deleting, acknowledging, or repeatedly leasing them.
- Unknown message kinds remain durable and available to a differently
  configured adapter or the unfiltered kernel API.
- Output kinds do not become inputs implicitly; cycles must be declared.
- Existing schema-1 worker files must be migrated intentionally.
- V1 cannot express sender, channel, payload, lineage, or capability predicates.
  Richer selectors require another version and evidence from real workloads.

See the
[acceptance contract](../contracts/worker-inbound-acceptance-v1.md) and
[continuous two-seat qualification](../qualification/continuous-two-seat-opencode-loop-2026-08-24.md).
