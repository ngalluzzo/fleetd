# ADR 0034: A harness is bounded by the OS, not by its own good behaviour

- Status: accepted
- Date: 2026-08-28

## Context

Fleetd launches a harness as a supervised child process and is careful about how.
[ADR 0004](0004-out-of-process-plugins.md) puts it out of process;
[ADR 0009](0009-typed-acp-driver-and-process-ownership.md) owns the process
group; every harness plugin declares an environment allowlist, clears everything
else, resolves an absolute executable, and content-addresses it into a launch
profile digest. The Claude plugin deliberately omits `IS_SANDBOX` from its
allowlist on the grounds that weakening a harness's own safety behaviour is not
this plugin's to do.

All of that is care about *what fleetd hands the process*. None of it bounds
what the process then does. A harness that receives an allowlisted environment
and an absolute executable can still read any file the daemon's user can read,
write any file it can write, and reach any network it can reach. The only thing
standing between a turn and the filesystem is the harness asking politely, which
[ADR 0033](0033-turn-scoped-transcript-read.md) documents fleetd answering with
an unconditional refusal.

That refusal is doing more work than it should. It is not a boundary; it is a
harness's own conscience, mediated. It holds exactly as far as the harness
chooses to ask, and a harness that does not ask — because a tool call was
in-scope by its own reckoning, because a permission rule matched, because a
prompt injection persuaded it, because of a bug — is unbounded. The durable
record shows the shape: an author invocation ran 967 tool events with zero
permission requests. Every one of those was inside its working directory *as
OpenCode understood it*, and fleetd took OpenCode's word for it, because fleetd
has no way to check and no way to enforce.

The field settled this, and not on fleetd's side of it. Claude Code runs two
independent layers: permission rules evaluated before a tool runs, and
`sandbox-runtime` enforcing filesystem and network limits at the OS level
through `sandbox-exec` on macOS and `bubblewrap` on Linux, deny by default, with
network egress passed through a proxy judged against an allowlist outside the
sandbox. Codex CLI is the same conclusion in different clothes: Seatbelt on
macOS, Landlock and seccomp on Linux, restricted tokens on Windows, default-on,
expressed as declared modes — `read-only`, `workspace-write`,
`danger-full-access` — rather than as per-call consent. Unattended cloud agents
skip consent altogether and rely on ephemeral isolation. The consistent
architecture is that rules carry *intent* and the sandbox carries the
*boundary*, because the rules are matched against strings the agent wrote.

The evidence that rules alone are insufficient is public and specific. Claude
Code's permission rules are string prefixes rather than parsed semantics, so
`Bash(git push --force:*)` is defeated by `git push -f` and `Bash(git:*)` does
not match `/usr/bin/git status`. CVE-2026-24053 is a path-restriction bypass
through ZSH clobber syntax that wrote outside the working directory with no
prompt at all. Fleetd's refusal is not vulnerable in those particular ways
because it permits nothing, but it inherits the same structural weakness: it
depends on the harness routing a decision to fleetd, and nothing makes it.

This decision is the prerequisite ADR 0033 named. It is worth taking on its own
terms regardless, because it is the only one of the two that bounds a harness
that never asks.

## Decision

**Fleetd sandboxes the harness process at the OS level, and the sandbox is the
boundary of a seat rather than a hardening option.**

**Declared, not inferred.** A seat's sandbox is derived from the desired state it
already carries: `working_directory` is writable, `additional_directories` are
whatever they were declared as, and everything else is denied. Nothing infers
scope from a tool call, a command string, or a path the harness mentions.
Fleetd already validates those directories at startup and already stores them on
the session binding, where `configuration_matches` treats a change as
incompatible and refuses to resume across it. So this adds enforcement to a
declaration fleetd already holds and already defends, rather than a new thing
for an operator to maintain.

**Deny by default, and the same default everywhere.** Read, write, and network
are each denied unless the profile grants them. A seat that has never needed
network egress does not have it, which is the single largest reduction available
and costs a correctly-configured seat nothing.

**The sandbox participates in session compatibility.** A native session opened
under one boundary must not silently resume under a wider one. Nothing new is
needed for the directories themselves — `configuration_matches` already compares
`working_directory` and `additional_directories` exactly — but anything else the
sandbox is derived from has to join that comparison, or a widened boundary would
adopt a session that was opened under a narrower one.

**A platform without an enforcement primitive fails closed and says so.** Fleetd
already refuses to write a credential file on a platform without owner-only
permissions rather than writing a world-readable one. The same rule: a seat that
cannot be sandboxed does not start, and the error names the platform and the
missing primitive. An operator who genuinely wants an unsandboxed seat declares
that explicitly, in desired state, under a name that reads like what it is.

**The sandbox does not replace the permission refusal, and this ADR does not
change it.** The two layers are independent on purpose, which is the whole
lesson: consent that the harness routes to fleetd, and a boundary that holds
whether it routes anything or not. Whether an allow policy becomes arguable once
this lands is ADR 0033's question to reopen, not this one's to answer.

**Fleetd does not write its own sandbox.** `sandbox-exec`, `bubblewrap`,
Landlock, and seccomp are the primitives; a maintained wrapper over them is
preferable to a hand-rolled profile, and the choice of wrapper is an
implementation decision this ADR deliberately leaves open. What it fixes is that
the boundary is enforced by the kernel and configured from desired state.

## Consequences

The trust model changes shape, and it is worth being precise about how. Today a
harness is trusted with the daemon user's whole filesystem and is asked to
behave; afterwards it is bounded, and misbehaviour is a failed syscall rather
than a policy question. That is the difference between a harness fleetd
supervises and one it merely launches.

Prompt injection stops being unbounded. It does not stop being a problem — an
injected instruction can still misuse everything inside the sandbox, which
includes the repository the seat is there to change — but "read every file this
machine can read" and "post it somewhere" become failed operations rather than
successful ones. For a system whose seats read issue bodies and pull request
descriptions written by strangers, that is the change that matters most.

Some seats will break, and that is the mechanism working. A harness that quietly
depended on a config directory outside its declared roots, a cache in `$HOME`,
or an unmentioned network call will fail once, loudly, at a boundary the
operator can widen deliberately. The first few will be irritating and every one
of them is a scope fleetd was previously granting without knowing it.

Fleetd acquires platform-specific code, which it has mostly avoided. The
credential-file permission split is the existing precedent and it is small; this
is larger, and the honest expectation is that Linux and macOS are supported
first and Windows is a stated gap rather than a silent one.

Transcript retrieval, ADR 0033's grant, and any future capability all get
simpler to reason about, because "what can this seat reach" becomes answerable
from desired state instead of from a harness's implementation. That is also the
condition under which a permission policy could ever be safe.

## Deliberately not here

Which wrapper or primitive to use, and the profile it generates. That is where
the platform detail lives and it should be decided against a running seat, not
in advance.

Sandboxing fleetd's own daemon, the worker, or the plugin *host*. This bounds
the harness, which is the process running model-directed code. Bounding the
supervisor is a different threat model and a much larger change.

Network allowlisting by destination. Deny-by-default egress is in scope; a proxy
that judges domains against a list — which is what Claude Code's sandbox does —
is a second mechanism with its own failure modes, and a seat with no egress at
all is the useful first step.

Container or microVM isolation. It is what hosted agent products use and it is
strictly stronger; it also assumes a deployment fleetd does not have, since a
local-first daemon runs beside an operator's own checkout. Revisit if fleetd
ever runs seats somewhere it does not own.

A permission policy of any shape. ADR 0033 refused one on the grounds that this
ADR was missing. It becoming arguable is not the same as it being decided, and
`allow_always` should stay refused regardless: a standing grant nobody
registered is the category [ADR 0031](0031-inbound-triggers.md) exists to guard.

Whether a sandboxed harness can still be trusted to report its own tool calls
honestly. Every observation fleetd folds into the durable record arrives from
the harness, so a compromised one can lie about what it did while the sandbox
bounds what it *could* do. Those are different guarantees and only one of them
is addressed here.
