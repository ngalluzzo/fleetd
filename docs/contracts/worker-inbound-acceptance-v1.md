# Worker inbound acceptance v1

## Purpose

Inbound acceptance is the adapter-owned contract that decides which immutable
Fleetd messages one continuous worker seat is eligible to reserve. It prevents
a semantic-neutral seat from treating every addressed envelope—including
another worker's generic completion result—as new work.

This is scheduling policy, not message authorization or semantic validation.
The kernel continues to preserve every unknown kind and payload unchanged.

## Envelope adapter configuration

Worker desired-state schema 2 requires an explicit adapter. The envelope
adapter carries this v1 contract:

```json
{
  "schema_version": 2,
  "adapter": {
    "kind": "envelope",
    "inbound": {
      "schema_version": 1,
      "message_kinds": [
        "work.request/v1"
      ]
    }
  },
  "result_kind": "work.result/v1"
}
```

`message_kinds` is an exact, non-empty set. V1 permits 1 through 128 unique
kinds; every kind must contain a non-whitespace character and be at most 256
bytes. Prefixes, globs, version ranges, negative matches, and duplicate values
are invalid. Unknown fields and unsupported schema versions fail closed before
a plugin starts.

## Reservation semantics

The trusted worker passes the exact set to an atomic SQLite reservation:

1. recover expired managed invocations under the existing conservative rules;
2. select the oldest currently eligible delivery whose message kind equals one
   set member;
3. create its lease and reserved invocation in the same transaction.

A non-matching delivery is skipped. It remains pending, receives no lease or
invocation, and its attempt count does not change. A later matching delivery
may therefore be selected without mutating an earlier non-match. Matching the
kind establishes eligibility only. The envelope adapter preserves the payload
without inferring semantics; an external adapter that interprets it remains
responsible for validating its exact contract.

Before the envelope adapter prepares the turn, execution reads a bounded
projection of the same channel: up to 64 current members, prioritized to retain
the worker and source sender, and the newest contiguous history before the
source message, limited to 32 messages and 64 KiB of exact encoded envelopes.
The prompt names either truncation explicitly. The source message remains a
separate exact field and is never replaced by the context window. This is a
read-only projection of existing membership and immutable messages, not copied
workflow state. It lets a worker discover exact peer IDs for an
invocation-scoped durable send while preserving channel-wide visibility.

The public unfiltered inbox claim and invocation-reservation APIs retain their
existing behavior. Exact-kind reservation is an internal trusted-worker path,
not a new bearer-authorized API.

## Session compatibility

The acceptance schema version, sorted exact kind set, and envelope presentation
generation participate in the worker's session compatibility digest. Changing
any of them rotates the binding generation instead of silently resuming native
conversational state under a different input or context policy. Reordering an
otherwise identical set does not rotate the generation.

The emitted `result_kind` is independent. A result is not automatically added
to inbound acceptance; a seat consumes it only when its adapter explicitly
declares that exact kind.

## Deliberate limits

V1 does not select by sender, channel, payload, correlation, or causation. It
does not acknowledge or dead-letter unaccepted work, establish semantic
validity, or hide channel history. Operators remain responsible for assigning
messages to a seat whose adapter declares the corresponding contract.
