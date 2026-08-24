# Repository-inspection OpenCode qualification — 2026-08-24

## Scope

This checkpoint is Fleetd's first useful, non-generic delegation against its
own repository. A continuous OpenCode seat consumed an exact capability request,
inspected a detached clean worktree, and returned a structured report. Fleetd
then strictly lifted the immutable attempt and independently validated every
citation against Git objects at the requested revision.

The run used OpenCode 1.4.0 with model route `opencode/gpt-5.6-sol`, no MCP
grants, and the `repository_inspection_v1` adapter.

## Exact bindings

| Item | Value |
| --- | --- |
| Repository | `dev.fleetd/fleetd` |
| Revision | `d3ca755a535327bfeb4eabe48c555439eb87f835` |
| Path scope | `src`, `docs` |
| Request ID | `sha256:8d5dfa6e585ec9cc93c055d15a3724262dd312eb99d17fcb3a1dca3c863e2271` |
| Input fact ID | `sha256:688658f92c1ad05003fce51a94a9666d1813fc15ccdfc390b060aa3b8d2f8bae` |
| Provider | `dev.fleetd.provider/opencode_repository_inspector@0.1.0` |
| Provider implementation | `sha256:96b226defa3ca0ae605ea8554bbaaa6197b82294c90e393932c61a299aaccb77` |
| Channel | `b795eff1-4c1b-4697-b507-01dea62979c7` |
| Request message | `90320fd7-c077-40b9-aa17-421e6cbbb1f1` |
| Attempt message | `6edf8841-05ec-4755-adad-6b519e370eca` |
| Invocation | `b32b7f3d-0f8b-4d7a-9577-359f3b1722cc` |
| Candidate ID | `sha256:11266ca70fb8e27fc7a7c5576e90cfdf31323755f1f9c96deb02ddf335cb0263` |
| Attempt evidence | `sha256:db1eafd95c0b6b4cf56871b7be71bd17c43aea20ed70bdd097d96151bc64c3b3` |

The sole question asked whether Fleetd's public HTTP API could list ordinary
pending deliveries that had never been claimed, including deliveries skipped
by worker inbound acceptance, and requested the narrowest missing public
surface.

## Result

The provider concluded that no such read-only operator surface existed at the
bound revision. Agent inbox access claimed and mutated deliveries, while the
operator listing exposed only blocked deliveries. It identified the narrowest
gap as an operator-authenticated, read-only delivery-list operation capable of
selecting ordinary `pending`, attempt-0 records and returning their immutable
message metadata.

The report answered the exact question with `supported` disposition and cited:

| Source | Lines | Established boundary |
| --- | ---: | --- |
| `src/api.rs` | 113–131 | Registered protected routes include no ordinary delivery list. |
| `src/api.rs` | 348–374 | The ordinary inbox operation is an agent-bound mutating claim. |
| `src/delivery.rs` | 46–61 | Claiming leases the row and increments its attempt. |
| `src/api.rs` | 501–526 | The operator delivery list is limited to unresolved blocks. |
| `docs/contracts/worker-inbound-acceptance-v1.md` | 53–61 | Skipped non-matches stay pending without a lease or attempt change. |

Strict extraction selected the sole complete ACP assistant message, whose
`messageId` was `msg_035e37d5b0019bVQ4w2CrhFEge` and whose captured event range
was 159–762. The inspection suite then returned `conformant_candidate`: request,
provider, report, revision, question coverage, path scope, and all five Git line
ranges matched exactly.

## Operational evidence

The continuous worker recorded one plugin generation, one reservation, one
known completion, zero restarts, zero blocks, and zero pre-arm retries. The
checkout remained clean at the exact detached revision after the turn. Git was
invoked directly through `/opt/homebrew/bin/git` with an empty environment.

This was a useful dogfood result rather than a canned fixture: the delegated
inspection exposed the next operator-observability gap while exercising the
same generic request, durable invocation, strict attempt lift, and candidate
identity previously used by GOOIR's runnable-web capability.

## Limits

- This qualifies one harness/model route and one semantic implementation.
- Structural conformance proves exact source locations, not the truth or
  completeness of the provider's natural-language conclusion.
- The read-only instruction is not an operating-system sandbox; the isolated
  worktree and post-turn clean check are the evidence for this run.
- The report was not granted patch, review, approval, merge, or fleet messaging
  authority.
- A second independent implementation is required before stabilizing the
  contract.

