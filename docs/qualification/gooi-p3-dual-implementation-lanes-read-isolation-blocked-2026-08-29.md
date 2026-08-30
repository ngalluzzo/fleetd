# GOOI P3 dual implementation lanes — read-isolation preflight blocked

Date: 2026-08-29

Verdict: `BLOCKED_BEFORE_IDENTITY_CREATION`.

Neither implementation request was dispatched. The requested Claude/Rust lane can satisfy the declared read/write boundary, but the requested OpenCode/GLM lane cannot satisfy that same boundary with Fleetd's currently qualified macOS sandbox postures. The instruction required both lanes to be configured before either dispatch, so the run stopped before creating agents, channels, messages, invocations, or credential copies.

## Frozen intended inputs

- GOOI commit `0b27dd33f4c7816fab2222e437a3af161e062dd6`;
- P3 payload tree SHA-256 `cb9531768d1faba3155d4312dfa8b4460a0e79a23d7f04a571bedf31a83df270`;
- P3 manifest SHA-256 `2a65956e4917c4f5d9b563ef3136580c66849ce478e7506fc1acde0fb8d0cdb8`.

GOOI was clean at that commit before and after the preflight.

## Exact conflict

Fleetd's `strict` Seatbelt posture is deny-by-default and can grant only the lane's writable reference root, private state/temp, explicit runtime reads, and the read-only protocol tree. That is suitable for the maintained Claude ACP lane.

OpenCode 1.4.0 ACP requires a private localhost listener. Fleetd `strict` exposes only typed network `deny` or `allow_outbound`; neither grants bind/listen. The already preserved real Seatbelt matrix proved that the narrowest accepted listener rule,

```scheme
(allow network-bind (local tcp "localhost:*"))
(allow network-inbound (local tcp "localhost:*"))
```

also permits `0.0.0.0:0`, the machine's external-interface address, and fixed loopback ports. The parser rejects numeric loopback hosts and ephemeral-port-only expressions. Fleetd correctly declined to represent that rule as a loopback-only typed capability.

The only qualified posture that boots OpenCode is `write_scoped`. Its typed security scope is `writes_scoped_reads_and_network_unrestricted`: it confines writes to declared roots, but begins with `allow default` and makes no read or network confidentiality claim. It therefore cannot enforce the requirement that the TypeScript lane read only its own reference directory and the frozen protocol input tree.

Using `write_scoped` without explicit coordinator authorization would silently weaken a material isolation requirement. No such substitution was made.

## Exact runtime/source evidence

- Fleetd source commit at preflight: `38911f6e1f802fb34d746d102d250ff302fe5ddc` with unrelated existing worktree changes;
- sandbox implementation SHA-256 `42c987c2516dc0e56637adfacab44308fefc7b2032802411b734cbd43867b4ed`;
- worker contract SHA-256 `48ff9e9274dcb94daf9fec49c624da00539a39fb1c3a58ae29c8b18d165c60b8`;
- preserved real Seatbelt limit report SHA-256 `86a184e81c962ce438e76d9991662dfc7fd82f06d1a8fc8f69b5cc5a598893ac`;
- OpenCode 1.4.0 executable SHA-256 `3d2c79a23f8a17d7ac35c819fba5bfac9393642de51434896adf7887629cc763`;
- current Fleetd OpenCode plugin SHA-256 `cfbe228bb4c85fc92449b8a3653e9eb6b128b1a249951ab499d753db044dbea6`.

## Narrow choices to resume

One explicit boundary decision is required:

1. authorize the OpenCode/GLM lane under `write_scoped`, accepting ambient reads and unrestricted network while retaining write confinement;
2. use a GLM-capable Fleetd harness that does not require a localhost listener and can boot under `strict`; or
3. add a different isolation mechanism, such as a runtime-adopted pre-bound loopback socket, a broker, or VM boundary, that can prove the requested read set and listener restrictions.

No GOOI file, Fleetd seat, credential, agent, channel, message, invocation, runtime, or unrelated worker was changed or restarted. No commit was created.

Machine-readable evidence: `docs/qualification/gooi-p3-dual-implementation-lanes-read-isolation-blocked-2026-08-29.json`.
