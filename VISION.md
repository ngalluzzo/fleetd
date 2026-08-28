# fleetd vision

Software agents should be able to form a dependable workforce without being
trapped inside one model vendor, harness, forge, chat product, or identity
protocol.

`fleetd` is the local-first coordination plane for that workforce. It gives
agents durable conversations, reliable inboxes, explicit work contracts, and an
operator-visible history. Existing tools continue doing what they are already
good at: Git stores code, harnesses run agents, model servers produce tokens,
and external services expose their own APIs.

## The hard part

An agent turn is an effect on the world. It writes files, pushes commits, calls
paid APIs, and messages other agents. So the only question that matters when a
process dies mid-turn is whether that effect happened, and most coordination
systems cannot answer it. They offer two states, done and not-done, and retry
into the gap — which is where duplicate pull requests, double-charged
inferences, and two agents editing one branch come from.

Fleetd answers with three states. A crash before the write-ahead dispatch fence
commits is *provably unstarted* and safe to retry. A drained terminal is
*known*. An armed attempt whose outcome cannot be proven is *parked* for a
person, never repeated. Every other design decision here — the immutable log,
the leased inbox, owner epochs on native sessions, bounded evidence rows —
exists to keep that third state small, honest, and inspectable.

That is the claim the project stands on, and it is the reason the kernel stays
as small as it does.

## Product promises

Each promise names the evidence behind it. A promise with no evidence is
labelled as such rather than implied.

- **The night shift resumes.** A crashed daemon, harness, or machine does not
  silently discard assigned work. *Demonstrated:* `bin/ci` hard-kills a daemon
  and worker mid-flight on every build and verifies native-session adoption
  under a higher owner epoch; see the
  [restart resumption](docs/qualification/qwen-restart-resumption-2026-08-24.md)
  and
  [human-to-agent](docs/qualification/live-human-agent-conversation-2026-08-25.md)
  records.
- **Failure is honest.** Delivery is described as at-least-once, ambiguity is
  surfaced rather than resolved by guessing, and consumers receive stable
  identities for idempotency. *Demonstrated:* the three states above are
  [ADR 0008](docs/adr/0008-write-ahead-invocation-fence.md) and
  [ADR 0007](docs/adr/0007-durable-blocked-deliveries.md); `fleetd inbox
  blocked` and `fleetd inbox resolve` are how a person settles the third one.
- **Every consequential action has evidence.** Messages, claims, results, and
  reviews remain attributable and inspectable. *Demonstrated:* one bounded row
  per managed invocation with fixed counters and a cryptographic chain digest
  ([ADR 0020](docs/adr/0020-bounded-operational-observations.md)), read through
  `fleetd trace` and three operator endpoints, tailable losslessly by an
  external collector, and exportable as OpenTelemetry spans
  ([ADR 0028](docs/adr/0028-opentelemetry-is-a-projection.md)). What an agent
  reasoned and which tools it called stays the harness's own record, retrievable
  with `fleetd transcript` rather than copied into Fleetd
  ([ADR 0029](docs/adr/0029-harness-transcript-retrieval.md)).
- **The operator stays sovereign.** One person runs a useful fleet on one
  machine with ordinary files and SQLite. Cloud services are optional adapters.
  *Demonstrated:* `fleetd init` through
  [getting started](docs/GETTING_STARTED.md); every listener is loopback and
  every credential is an owner-only local file.
- **Agents can change seats.** A harness implements one adapter contract
  without changing the coordination kernel. *Partially demonstrated:* OpenCode
  is production-shaped and qualified against real turns
  ([record](docs/qualification/opencode-plugin-2026-08-24.md)). Codex
  implements the same interface but has no real-runtime qualification, so this
  promise rests on one integration, not two.
- **Policy is explicit.** Approval and merge rules are evaluated in one place,
  against exact artifacts and revisions. *Designed, not demonstrated:* the
  [author-review draft](docs/contracts/author-review-workflow-draft.md) lives
  outside the daemon as a deliberately unstable contract and has not been run
  against real work.

## Engineering posture

The public protocol is a product surface. Contracts are versioned, migrations
are forward-only and tested, unknown data survives transport, and observability
is designed alongside execution rather than added afterwards.

Three habits keep that from being a slogan.

*Architecture is a build fact, not a document.* `tests/crate_boundaries.rs`
holds the layering: the kernel cannot name what is above it, `execution` cannot
name a transport, and a crate that reaches a kernel table it does not own fails
the suite. Rules that live only in prose drift the moment someone adds a
convenient import.

*Decisions are written down and numbered.* Thirty-two ADRs record what was
chosen, what it cost, and what was deliberately left out. A reader can
reconstruct why the kernel has six concepts without asking anyone.

*Claims are qualified with reproducible records.* Nineteen qualification
documents carry exact message identifiers, real model routes, and content
hashes; the soak runner hashes both its plan and its report. "It works" is not
a status this project reports.

Features are not complete until restart, concurrency, and partial-failure
behavior is known.

## What is not built yet

Stated plainly so the promises above can be trusted.

- **Remote workers.** Listeners are loopback-only. Local bearer credentials are
  authentication, not encrypted transport; remote seats wait for TLS and
  enrollment.
- **The full-night soak.** The longest recorded unattended run is 191 seconds.
  Several real seats against one daemon overnight, with restart, latency,
  throughput, and ambiguity evidence, is still open.
- **Fleetd building fleetd.** The author/reviewer loop has not been exercised on
  this repository using only opaque messages. That is the demonstration that
  would close the argument.
- **Model throughput.** Fleet health, blocked work, invocation traces, and
  session ownership all ship; per-model throughput does not.
- **A second qualified harness.** See the seat-change promise above.

## Non-goals

`fleetd` is not a Git forge, model server, general chat network, social identity
system, or replacement for mature harnesses. Federation may eventually be an
adapter; it is not an architectural prerequisite.
