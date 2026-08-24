# Repository-patch provider qualification attempt — 2026-08-24

## Outcome

The repository-patch contract and deterministic Git suite passed fixture
conformance, but no real model attempt produced a candidate. The provider is
not qualified. All three attempts left the detached source worktree clean at
the exact base revision, and no patch was applied, committed, or pushed.

This failed run was still productive: it exposed two missing controls in the
OpenCode integration and one incorrect success boundary in Fleetd's controller.
Those controls are now encoded and regression tested rather than retained as
operator convention.

## Exact request

| Item | Value |
| --- | --- |
| Repository | `dev.fleetd/fleetd` |
| Base revision | `32580f6da4d3ce261b49fe3cc9f2622f622bb00e` |
| Path scope | `docs`, `openapi`, `src`, `tests` |
| Request ID | `sha256:4dc30271ab989d20b07967fd0d5e37ebc269cdd8e332512ef2b71b4e3e4b47bc` |
| Input fact ID | `sha256:5daa1115d6b3e54c1b705f69664400506bd00a41599eef0b3c3948f94bc7578a` |
| Capability | `dev.fleetd.capability/propose_repository_patch@0.1.0` |
| Suite | `dev.fleetd.conformance/repository_patch@0.1.0` |

The brief asked for the narrowest operator-authenticated, read-only API that can
observe ordinary pending attempt-0 deliveries without claiming or mutating
them. It required deterministic bounded pagination, exact agent filtering,
complete message envelopes, agent denial, store and HTTP tests, and committed
OpenAPI agreement.

## Attempts

| Attempt | Provider implementation | Bound | Observed result |
| --- | --- | --- | --- |
| OpenCode cloud / GPT-5.6 | `sha256:09877a114d2f516f13d256a27027004b50e83d17c5887110c5ba79f7f4dc4474` | Request `5a7a6bd2-9f11-48d7-8731-e3d580d4d350`; invocation `8c0ac1e2-7fa0-45ed-964e-565144fb9a2a`; attempt `aed9836b-49ab-4b2a-9e76-448f9fe01b78` | Provider balance exhaustion after USD 0.1467964. OpenCode emitted runtime `end_turn`, no assistant message, and therefore no structured result. |
| OpenCode / local Qwen, inherited home | `sha256:1db417cc2bca0041bd5fccfd0facee383e9189b36357da8f1e888e318e23135f` | Request `fef245c8-f6a7-40a3-9584-b728bf2f3737`; invocation `574b4ae9-2693-4978-b640-d0cc23e3dbc1`; attempt `fd0973e6-b255-45c5-8d07-685c8626f2f6` | The model invoked OpenCode's nested `task` agent. That work was not visible through Fleetd's ACP event/tool budget. A 180-second idle cancellation retained only a prose preamble, so final JSON was unavailable. |
| OpenCode / local Qwen, clean bounded profile | `sha256:3150f9ec6a98e0a344d15cad018007b815a57520ef48faf2d2c8b9d8e933ddc1` | Request `023aada9-6f0f-49a2-b5ba-6348b534e166`; invocation `ef17df5a-cf27-493c-b584-966dc45c17fe`; attempt `355e7a58-6726-40cc-a3e7-147888bfebfe` | An empty OpenCode home and policy version 2 denied nested `task`. The trace reached 63 model steps and 71 visible tool events, then the 15-minute wall cancelled it before final JSON. |

The local route used OpenCode 1.4.0 through a credential-free loopback
OpenAI-compatible provider at `127.0.0.1:18082` and the exact model path
`/Users/ngalluzzo/Models/qwen3.8-27b-8bit`. A direct small inference check
observed approximately 113 prompt tokens/s, 11.58 decoded tokens/s, and 82.26
GB peak memory. Those throughput numbers qualify only that direct inference
check, not the agent workload.

## Controls learned from the failure

### OpenCode owns its local-provider policy

The OpenCode plugin now accepts one typed optional `openai_compatible` block,
constructs the native `@ai-sdk/openai-compatible` configuration itself, and
requires a credential-free explicit loopback HTTP address. The selected model
route must exactly equal the configured provider and model IDs. The complete
configuration and policy version participate in the session profile digest.

OpenCode's nested `task` permission is denied by the plugin. A nested agent that
does not surface its tools and progress through Fleetd's typed ACP boundary
cannot count against the controller's evidence or budget, so it is not admitted
for this provider profile.

### Runtime termination is not result success

At the time of these attempts, OpenCode reported `end_turn` after both backend
failure and host cancellation. Fleetd durably settled those known, quiescent
turns but incorrectly wrote attempt payload status `completed` even though
structured capture was unavailable. The strict lift still rejected all three,
so no candidate escaped, but the raw status was misleading.

The controller now marks final-JSON capture successful only when the runtime
ended normally, every captured message is complete, and one protocol-bounded
final JSON value was actually captured. The shared ACP host preserves its own
`wall_deadline` or `idle_deadline` over a later native `end_turn`; an outer
controller wall records `host_wall_deadline`. Both retain the native claim
separately as `runtime_stop_reason` and produce failed attempt evidence. Known
quiescence still permits atomic settlement and session reuse; it does not imply
provider success.

## Qualification boundary

- The contract implementation is fixture-tested, including isolated-index
  canonicalization and proof that the source worktree remains clean.
- No real attempt reached generic candidate lift or Git-backed patch
  conformance, so no model/provider is qualified.
- The 15-minute bounded run shows that local inference is available and tools
  are observable; it does not show that this model/profile can complete this
  repository-scale task efficiently.
- A future qualification must produce exact final JSON, pass generic strict
  lift, pass isolated Git conformance, keep the checkout clean, and then pass a
  separate semantic review/test capability before any publication authority is
  considered.
