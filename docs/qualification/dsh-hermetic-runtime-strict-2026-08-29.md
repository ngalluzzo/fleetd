# DSH hermetic runtime and strict-seat qualification — 2026-08-29

## Decision

The direct-provider DSH route is qualified to boot and complete a real GLM-5.3
turn from a content-addressed JavaScript dependency closure under Fleetd's
deny-by-default macOS Seatbelt posture. It does not depend on a parent
`node_modules` tree. This is a process/dependency isolation result, not a claim
that DSH is a full operating-system image.

The qualified closure is:

- source: `deepseek-ai/deepseek-harness` tag `dsh-v0.1.2-alpha.1`, commit
  `cd5ef8148158c3a752a658978873241fdf8e2bbc`;
- root: `/Users/ngalluzzo/Library/Application Support/fleetd/runtimes/dsh-0.1.2-alpha.1-sha256-9703cbdfaae5`;
- manifest: `/Users/ngalluzzo/Library/Application Support/fleetd/runtime-manifests/sha256-9703cbdfaae50b201b7e47820e3df128f1df2eed5992ca9dd034c0c6b1c4d16c.json`;
- closure digest:
  `sha256:9703cbdfaae50b201b7e47820e3df128f1df2eed5992ca9dd034c0c6b1c4d16c`.

The manifest identity covers every relative path, entry kind, permission mode,
file size and SHA-256, plus the literal target of every in-closure symlink.
Symlinks that resolve outside the root are rejected. The manifest itself is
written outside the closure so it cannot recursively alter the identity it
describes. `node --test tools/runtime-closure-manifest.test.mjs` passes both
the mutation-sensitivity and escaping-symlink tests.

## Failure that established the boundary

The first packed runtime initialized ACP and then disposed immediately. Direct
ACP replay exposed the real dependency defect: the closure lacked Sharp's
Darwin ARM64 optional package. The fix was to install the pinned native package
for this platform (`sharp@0.35.4`, Darwin ARM64, optional dependencies
included), rebuild lifecycle dependencies, and remove root lock/package
metadata that referred to Fleetd build locations. The closure was regenerated
and renamed only after its final digest was known.

This matters because the symptom was compatible with several wrong diagnoses:
duplicate Cordis ownership, Fleetd lifecycle handling, or DSH itself. The
direct replay reduced it to one absent runtime dependency before the Fleetd
qualification was repeated.

The rejected closure remains recoverable at
`/Users/ngalluzzo/Library/Application Support/fleetd/runtimes/.rejected-dsh-0.1.2-alpha.1-sha256-52ccd06942b9`; its manifest is similarly prefixed
`.rejected-`. A failed Node 25 build remains under the runtime directory with
the `.failed-node25-` prefix. Neither path is admitted by the qualified worker
configuration.

## Poisoned-ancestor proof

To distinguish a closed dependency graph from an accidentally successful
ancestor lookup, an invalid `argparse` package was placed at the ancestor path
`/Users/ngalluzzo/Library/Application Support/fleetd/node_modules/argparse`.
The package has since been moved recoverably to
`/Users/ngalluzzo/Library/Application Support/fleetd/quarantine/poison-argparse-20260829`.

The qualifying seat used strict Seatbelt with declared/system reads only. Its
read roots admitted the content-addressed DSH closure, the exact Fleetd debug
binaries, the explicit Node/Homebrew runtime, timezone data, and the isolated
working directory. The poisoned ancestor was not admitted. Only the private
DSH home was additionally writable, and outbound network was explicitly
enabled for the DSH-owned `zai` provider route.

The durable request required exactly `FLEETD_DSH_HERMETIC_OK` and prohibited
tools and file changes:

- request message `53219df3-9f54-49d2-8749-acb704a8a336`, sequence `136`;
- invocation `d0aa4eb0-a2b9-4d9f-a6dd-6efd73a9457c`;
- result message `ed43d2c6-f638-4c5e-9ea3-47c81d72dabd`, sequence `137`;
- plugin generation `a77c1cbb-9d3c-4074-96b8-0d62be4b53bb`;
- plugin profile digest
  `sha256:5a132a8e0256b47166902e655e0e5fb4011f7b8817c2d8e45e15f2bdd82c18ac`;
- compatibility digest
  `sha256:3af245a124cf2dfc4417759aab0b4a72068287ddfcfa9948b5e881bc220ca783`;
- runtime executable digest
  `sha256:dc23f6c5dd7df8834e3e38bdb9609d77b459834681ae9b7133b417b0c35f3166`.

Fleetd recorded one assistant event, one usage event, no tool or permission
events, `end_turn`, `outcome_known`, and a quiescent runtime-claimed session.
The generation stopped through graceful shutdown with exit code zero. The
worker used one generation, one reservation, one completion, and no retries or
blocked deliveries.

## What remains intentionally outside this claim

- Node itself is an explicit, pinned seat dependency but is not yet acquired
  by a Fleetd runtime acquisition manager.
- Native-session adoption after process replacement is still a separate gate.
- DSH's invocation-scoped Streamable HTTP MCP grant is still a separate gate.
- The credential file remains owned by DSH in its owner-only home; it is not
  copied into Fleetd configuration, runtime manifests, or evidence.
- Local Qwen and OpenCode loopback routes have separate qualification records;
  this direct-provider result does not certify them.

