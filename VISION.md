# fleetd vision

`fleetd` is the durable control plane for any coding agent.

A coding agent should be a durable participant, not the process, model, harness,
or chat session temporarily running it. Work assigned to that agent should
remain owed after a crash. Its results should remain attributable after a
restart. When an external effect may or may not have happened, recovery should
stop for judgment rather than guess.

Fleetd gives agents stable identities, durable conversations, reliable inboxes,
recoverable execution, and an operator-visible record. A harness is a
replaceable runtime occupying an agent's seat. A cockpit is a replaceable way
to observe and direct it. Git remains the authority for code, harnesses remain
the authority for their native sessions and transcripts, and external systems
continue to own their domains.

Any harness can occupy the seat. The work survives the harness.

## The durable agent

Most coding products collapse an agent into one session. Close the terminal,
lose the process, exhaust the context window, or change vendors, and its
practical identity disappears with its private state. Unfinished obligations
and execution certainty are left scattered across transcripts, worktrees,
issue trackers, and human memory.

Fleetd separates the durable agent from its current execution:

- the **agent** is a stable, addressable identity with work and history;
- the **harness** is a replaceable runtime that performs a turn;
- the **cockpit** is a replaceable client over the same durable truth;
- **Git** remains the authority for code and review artifacts; and
- **Fleetd** is the authority for coordination, delivery, and recovery state.

"Any coding agent" does not mean the kernel learns every harness. It means a
harness implements a narrow, versioned operational contract without bringing
its model, tool, workflow, or product semantics into the control plane. A new
runtime changes an adapter, not the meaning of an agent or the durability of
its work.

The result is a workforce whose members can continue across process and harness
replacement. A person or another agent addresses the stable identity. An
approved runtime takes the turn. Compatible replacements adopt the native
session when that is safe; incompatible replacements retain the durable
conversation and obligations without pretending private harness state is
portable. The interface may change and the runtime may change. The work does
not disappear.

## The hard promise

An agent turn is an effect on the world. It writes files, pushes commits, calls
paid APIs, and messages other agents. When a process dies mid-turn, the question
that matters is whether the effect happened. A two-state system offers only
done and not-done, then retries into the gap -- producing duplicate pull
requests, double-charged inference, and concurrent edits made under the false
assumption that the first attempt never started.

Fleetd preserves three states:

1. A crash before the write-ahead dispatch fence commits is **provably
   unstarted** and safe to retry.
2. A drained terminal with accepted evidence has a **known outcome** and can be
   settled.
3. An armed attempt whose outcome cannot be proven is **outcome unknown** and
   is parked for a person, never blindly repeated.

This is not an exactly-once claim across systems Fleetd does not control.
Delivery is at least once; stable identities and idempotency make retries safe
where they can be safe. The control plane's stronger promise is honesty: no
obligation silently disappears, and no known ambiguity is resolved by
guessing.

Every other design decision -- the immutable log, leased inboxes, invocation
fence, owner epochs on native sessions, and bounded evidence rows -- exists to
keep the third state small, explicit, and inspectable. That is the claim the
project stands on, and it is why the kernel stays small.

## Product promises

Each promise names the evidence behind it. A promise with no evidence is
labelled as such rather than implied.

- **Work survives failure.** A crashed daemon, harness, or machine does not
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
- **Harnesses are replaceable.** One adapter contract lets a runtime occupy a
  seat without changing the coordination kernel. *Partially demonstrated:*
  OpenCode is production-shaped and qualified against real turns
  ([record](docs/qualification/opencode-plugin-2026-08-24.md)). Codex and
  DeepSeek Harness implement the same turn interface but have no real-runtime
  qualification, so this promise rests on one integration, not two.
- **Consequential actions have evidence.** Messages, claims, results, and
  reviews remain attributable and inspectable. *Demonstrated:* one bounded row
  per managed invocation with fixed counters and a cryptographic chain digest
  ([ADR 0020](docs/adr/0020-bounded-operational-observations.md)), read through
  `fleetd trace` and operator endpoints, tailable losslessly by an
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
- **Policy is explicit.** Approval and merge rules are evaluated in one place,
  against exact artifacts and revisions. *Designed, not demonstrated:* the
  [author-review draft](docs/contracts/author-review-workflow-draft.md) lives
  outside the daemon as a deliberately unstable contract and has not been run
  against real work.

## The boundary is the product

Fleetd externalizes obligations, not intelligence. It does not try to preserve
an agent by copying every private thought or tool event into its database. The
immutable conversation and managed execution record are durable; raw reasoning
and tool use remain native to the harness and may be projected through explicit
transcript retrieval or lossy telemetry.

Fleetd also does not define what software work means. Task graphs, pull-request
policy, author-review loops, product facts, and conformance rules belong in
external contracts and adapters. The kernel transports their messages without
interpreting them. This lets a cockpit, workflow, or issue tracker build a
projection over Fleetd without becoming a second source of execution truth.

That separation is what makes the control plane useful beneath many products:
harnesses can improve, models can change, and cockpits can compete without
migrating the durable identities and obligations between them.

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

*Decisions are written down and numbered.* The ADRs record what was
chosen, what it cost, and what was deliberately left out. A reader can
reconstruct why the kernel has six concepts without asking anyone.

*Claims are qualified with reproducible records.* Qualification documents carry
exact message identifiers, real model routes, and content hashes; the soak
runner hashes both its plan and its report. "It works" is not a status this
project reports.

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
- **Fleetd building Fleetd.** The author/reviewer loop has not been exercised on
  this repository using only opaque messages. That is the demonstration that
  would close the argument.
- **Model throughput.** Fleet health, blocked work, invocation traces, and
  session ownership all ship; per-model throughput does not. Experimental
  backend plugins can expose provider-native observer URLs, but Fleetd neither
  normalizes nor durably presents those metrics yet.
- **Real inference-plugin qualification.** MLX-VLM and llama.cpp implement the
  same experimental lifecycle and loopback-route interface and pass
  executable-shaped tests. MLX-VLM has completed one real-runtime Qwen
  qualification through that interface; llama.cpp remains unqualified, so the
  interface is not stable.
- **A second qualified harness.** See the replaceable-harness promise above.

## Non-goals

Fleetd is not a cockpit, kanban board, workflow engine, Git forge, model server,
general chat network, social identity system, or replacement for a mature
harness. It is the durable control plane beneath those products. Federation may
eventually be an adapter; it is not an architectural prerequisite.
