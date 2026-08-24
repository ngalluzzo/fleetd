# GOOIR runnable-web qualification — 2026-08-24

## Scope

This qualification exercised the first complete cross-repository capability
loop on the Fleetd blocked-delivery operator surface:

```text
GOOIR target fact
  -> durable Fleetd capability request
  -> provider attempts and brownfield implementation
  -> strict candidate extraction
  -> independent GOOIR conformance
  -> admitted artifact fact
  -> zero-need re-plan
```

The qualified Fleetd revision is
`98c73ba08c47eff77769c12f142442cdebb29ace`. The GOOIR artifact,
brownfield projection, and verifier implementation are on revision
`55386b449f831c5e6d90a49b8008a13a4fac4371`.

## Exact semantic identities

- capability request:
  `sha256:1bcd8e12abcf03d4c91b9e9a528446e5347bf5668c95fffc1d03cac9e4f3e01b`
- bound web target fact:
  `sha256:2680f462f04cc9bb474e0cd277903e3014d88ed8b0ca5977d4bebc7debf54a80`
- capability:
  `dev.fleetd.capability/generate_runnable_web_surface@0.1.0`
- suite:
  `dev.fleetd.conformance.runnable_web_surface@0.1.0`

## Provider attempts

The request was not converted to prose. Each model received the exact request
through `CapabilityWorkTurnAdapter`, and every terminal result was retained as
`work.capability.attempt/v1` evidence.

| Provider route | Terminal observation | Strict extraction |
| --- | --- | --- |
| `zai-coding-plan/glm-5.3`, first turn | stopped after “Now I'll build” with no source change | rejected: response was prose |
| same provider, resumed turn | stopped before the promised file write | rejected: response was prose |
| `opencode/qwen3.6-plus` | returned a large inline page in a Markdown fence; persisted the token and changed no source | rejected: response was Markdown |
| `opencode/gpt-5.6-sol` | used tools, changed source, and ran repository checks, but aggregated progress prose with the final JSON | rejected: response was not JSON-only |
| same GPT provider, resumed after commit | emitted JSON-only but reported its stale pre-contract artifact | extracted as candidate `sha256:79acf9c1bc3dfa393e3b32c1616509b3ef4fbc2da1b422c6cee97003124d9496` |

The stale candidate passed transport and exact request binding. The real suite
rejected its unknown `artifact` field, produced conformance result
`sha256:b6d0c66a0caaae5afedf582eeeaf2126430028e13ed859c43cd79826f6c75815`,
admitted no facts, and left the capability need present. This proves extraction
and conformance are distinct gates.

The model-reported terminal usage values were 31,458 and 34,596 for the two
GLM turns, 51,341 for Qwen, and 52,401 then 54,178 for the GPT session. These
are provider-reported context usage snapshots, not decoded-token throughput or
necessarily incremental usage, so they must not be interpreted as performance
rates.

## Brownfield projection

The useful GPT source changes were treated as brownfield material rather than
trusted output. After review and adaptation, the committed surface:

- serves `/operator/` plus separate HTML, CSS, JavaScript, and contract assets;
- serves `/operator/contract.json` exactly equal to the bound web target IR;
- derives columns, actions, selectors, methods, and paths from that contract;
- retains the operator token only in JavaScript memory; and
- leaves all data and effects behind the existing operator-authenticated API.

GOOIR's separately identified deterministic Git projector hashed the clean
revision and four assets, then returned its raw JSON through a durable Fleetd
message from agent `c2853aa9-bfe3-49a1-9492-96c5fc0f926b`. Fleetd strictly
extracted candidate
`sha256:42d157fcec55a6385630a1b11130b40f4ec05cb4ef625a63ebba6d6df4236fcf`.
Its immutable message evidence digest is
`sha256:e47d8e9b4cb389f01f2aebb3e6e063310948f3127658d78c5fc22de6538efc00`.

## Independent admission

The independently identified verifier passed four named checks:

1. exact closed artifact schema and bound target identity;
2. trusted Git checkout at the proposed pristine revision;
3. all four asset SHA-256 digests; and
4. a verifier-owned black-box test against real Fleetd durable state,
   authentication, list behavior, and both requeue and abandon effects.

The result is:

- conformance result:
  `sha256:0d0f56fae5097e1ce741b7581ffb2665636027cc6d950c74bd7c69f31111d1a3`
- admitted artifact fact:
  `sha256:5591961e7692ce6429464bf1b04abf04a44b95afb720a765d26871949f418a56`
- re-plan: zero steps, zero needs, executable.

The admitted fact derivation binds the generating provider and implementation,
input fact, request, candidate, and independent conformance result.

## Browser observation

The committed server was opened in a real in-app browser at `/operator/`. The
browser loaded the external script and stylesheet, fetched the exact contract,
derived all six target columns plus actions, presented the memory-only token
boundary, and reported no console errors. The independent runtime test—not a
candidate-authored test—exercised both consequential resolution effects.

## Original follow-up finding

Fleetd's ACP host exposed OpenCode progress text and the final response as one
aggregated assistant message for a tool-using turn. A strict JSON-only result
contract was therefore ergonomic only when the model emitted no earlier prose.
Fleetd must not relax extraction to “find JSON somewhere in the text.” The
next adapter/harness slice should introduce an explicit structured result
channel or preserve trustworthy final-message boundaries while retaining the
complete captured assistant transcript as evidence.

## Structured-result closure

Fleetd commit `bd05a76fd043846ac3473e667e39c7c0b0d6351b` closes that
finding without relaxing JSON extraction. The ACP host now preserves the
protocol's `messageId`, capability workers publish
`work.capability.attempt/v2`, and the strict lift independently recomputes the
final-message selection and JSON parse from the retained assistant transcript.

The first live v2 turn used the same exact request and the real
`fleetd.harness.opencode` plugin with `opencode/gpt-5.6-sol`. OpenCode emitted
four assistant messages with four distinct IDs across 1,069 ordered turn
events. The fourth message contained the result. Fleetd retained all four and
recorded `last_identified_assistant_message`; it did not scan the preceding
progress prose. The durable attempt was
`534fe975-ada3-4f64-bea9-ef12ab3f8fb1`, with evidence digest
`sha256:78927fde4971446a4d91854cb910b39e91fcfb396f08c9eb26386584bba9699f`.
Strict extraction produced candidate
`sha256:70173b065b3b99d4b0bdee80d030d12103039ed8012baa27b0720592e07c5a74`,
and the independent suite passed and replanned to zero needs.

After committing and rebuilding, a second request exercised durable session
adoption rather than a fresh lane. Binding generation 1 resumed under owner
epoch 2 and completed without retry or block. That turn used one identified
assistant message, so the controller recorded `only_assistant_message`:

- attempt: `87039e77-0eb6-4d6a-ac44-51c65ae1a032`;
- invocation: `4ba8ef0f-90d1-4d64-b980-103d580d436a`;
- attempt evidence:
  `sha256:7c55d49821ce6f924e82499a2265d30bc384c229696854397e9dd4616effbd8d`;
- candidate:
  `sha256:40ff89fc4775c3f938fd26e8d8dfa217da0eac7ee923c05e7244db164d71af81`;
- conformance result:
  `sha256:8816a3416403a8b9f60e2735359da7f11840ba653085daa06fb832e34b54ced9`;
- admitted fact:
  `sha256:cceb15462108f1acedf673684de592c514176e278ec0bd3cf2924e65ed56b7f8`;
- re-plan: zero steps, zero needs, executable.

The resumed turn reported a cumulative context value of 56,005 and cost of
USD 0.214638. These remain provider claims, not Fleetd-measured throughput.
Durable tool, reasoning, permission, and plan event fragments are still a
separate evidence boundary; this closure is specifically the assistant-result
boundary.
