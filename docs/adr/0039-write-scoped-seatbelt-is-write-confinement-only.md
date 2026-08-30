# ADR 0039: `write_scoped` Seatbelt is write confinement only

- Status: accepted
- Date: 2026-08-29

## Context

ADR 0034 established Fleetd's strict macOS boundary: deny by default, then
restore only declared filesystem and network reach. That is the preferred
posture and remains unchanged. Two real vendor harnesses exposed a different
compatibility need before their model loop could start:

- DeepSeek Harness's Node dependency resolver reads installation dependencies
  through lexical package ancestry;
- OpenCode's ACP adapter opens a private localhost listener on an
  OS-selected port.

Seatbelt's installed network dialect cannot express loopback-only,
ephemeral-only listen authority. Its accepted `localhost:*` filter also admits
wildcard, external-interface, and fixed-port listeners. Widening the strict
posture with that rule would therefore make desired state claim a boundary the
kernel does not enforce.

DeepSeek Harness's own macOS command sandbox uses a smaller claim: allow by
default, deny every filesystem write, then restore writes to `/dev/null`, the
workspace, and private temp roots. That mechanism does not constrain reads or
network access, but it does turn an out-of-scope mutation into a failed syscall
for the complete confined descendant tree.

## Decision

Fleetd names two distinct macOS Seatbelt postures:

- `strict` retains the deny-default profile and admits the literal `/dev/null`
  sink in addition to declared writable roots. Git opens that device read/write
  while repairing inherited standard descriptors. This single-device rule is
  content-addressed and rotated the strict profile digest domain; it does not
  admit another filesystem path;
- `write_scoped` is allow-default write confinement. It emits
  `(deny file-write*)` and restores writes only to literal `/dev/null` plus
  canonical declared writable roots.

`write_scoped` desired state must explicitly say
`read_access: unrestricted` and `network: unrestricted`. It must declare one
private state directory and one private temp directory. Omitting those fields,
declaring read-only roots, or implying a narrower read/network policy is
invalid before the plugin starts.

The posture name and effective SBPL bytes are content-addressed. The resulting
sandbox digest participates in native-session compatibility, so a session
cannot move between strict, write-scoped, or unsandboxed authority without a
new compatibility generation. Operator qualification evidence must carry the
posture name, digest, and the exact scope string
`writes_scoped_reads_and_network_unrestricted`.

The existing plugin supervisor still launches `sandbox-exec` as the process
group root. The plugin, ACP adapter, harness, tools, and their descendants
inherit the same write boundary. Environment clearing, credential-free
inference injection, vendor tool policy, and Fleetd's typed ACP permission
policy remain independent defenses.

## Consequences

`write_scoped` makes one useful and testable promise: a harness process cannot
write outside the declared roots even when it never asks permission. It makes
no read-confidentiality, network-confidentiality, destination-control, bind,
or hermetic dependency claim. A process under this posture may read user files
that its Unix identity can read and may use arbitrary network operations.

The posture is appropriate only for operator-owned local dogfood where write
containment is the accepted boundary. Work containing untrusted secrets or
requiring egress isolation must use `strict` or a stronger future mechanism.

## Rejected alternatives

**Call the broad Seatbelt network rule loopback-only.** Real syscall tests
proved that claim false.

**Silently fall back from strict.** Changing the security boundary is desired
state, not runtime recovery.

**Treat harness permission prompts as confinement.** They remain consent
signals. The OS boundary must hold when the harness never asks.

**Call `write_scoped` hermetic.** Ambient reads and network are explicitly
unrestricted, so hermeticity would be a false provenance claim.
