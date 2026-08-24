# Repository inspection v1

Status: experimental capability specialization

This contract delegates bounded, read-only analysis of one exact Git revision.
It specializes the existing capability-work request and attempt contracts; it
does not add a task, repository, or Git concept to the messaging kernel.

## Exact identities

| Role | Identity |
| --- | --- |
| Capability | `dev.fleetd.capability/inspect_repository@0.1.0` |
| Input fact | `dev.fleetd.fact/repository_inspection_brief@0.1.0` |
| Output fact | `dev.fleetd.fact/repository_inspection_report@0.1.0` |
| Suite | `dev.fleetd.conformance/repository_inspection_report@0.1.0` |

The request envelope remains `work.capability.request/v1`; the provider attempt
remains `work.capability.attempt/v2`.

## Brief fact

The provider-neutral input payload is:

```json
{
  "schema_version": 1,
  "repository_id": "dev.fleetd/fleetd",
  "revision": "40-or-64-character-lowercase-hex-commit-id",
  "path_scope": ["src", "docs"],
  "questions": [
    {
      "id": "pending-delivery-observability",
      "prompt": "Can an operator list ordinary pending deliveries?"
    }
  ]
}
```

Repository IDs are opaque bounded strings. A path scope is `.` or a normalized
repository-relative path made only of normal components. Questions have unique,
bounded identifiers and prompts. V1 accepts 1–32 scopes and 1–16 questions.

`fleetd work inspect-bind --brief BRIEF.json` validates the brief and constructs
the generic capability request. The input fact derivation is exactly:

```json
{
  "kind": "git_revision",
  "repository_id": "dev.fleetd/fleetd",
  "revision": "exact-commit-id"
}
```

The fact ID binds the canonical `{payload, derivation}` pair. The ordinary
capability request ID then binds that exact fact, capability, complete-only
requirement, output type, and conformance suite.

## Provider execution

`adapter.kind: repository_inspection_v1` admits only the exact request message
kind and one configured provider whose capability equals the identity above.
Before dispatch can arm, it invokes one configured absolute Git executable
directly with an empty environment and verifies all of the following:

- the configured absolute working directory is Git's canonical top level;
- `HEAD` is the exact requested commit; and
- tracked and untracked status is empty.

The adapter composes the generic capability-work prompt with the brief and a
strict report shape. It instructs the harness to remain read-only, answer every
exact question ID, use only admitted paths, and cite 1-based inclusive source
lines. No MCP capability is required.

This is a semantic fail-closed boundary, not a filesystem sandbox. V1 does not
prevent the harness from reading paths outside the evidence scope or attempting
a write. A dirty checkout prevents later conformance, and operators should use
an isolated disposable worktree with no secrets.

## Report fact

The single complete output fact payload is:

```json
{
  "schema_version": 1,
  "request_id": "sha256:...",
  "repository_id": "dev.fleetd/fleetd",
  "revision": "exact-commit-id",
  "answers": [
    {
      "question_id": "pending-delivery-observability",
      "disposition": "supported",
      "conclusion": "Bounded conclusion.",
      "evidence": [
        {
          "path": "src/api.rs",
          "start_line": 1,
          "end_line": 20,
          "observation": "What these exact lines establish."
        }
      ]
    }
  ],
  "limitations": []
}
```

The answer set must equal the brief's question set. `supported` requires at
least one cited location; `inconclusive` may carry none. Each answer accepts at
most 16 evidence items, and one citation may span at most 200 lines. V1 bounds
all conclusions, observations, and limitations.

## Deterministic lift and conformance

The generic strict lift first authenticates the immutable attempt envelope,
reconstructs the ACP assistant-message boundary, and emits an unverified
content-addressed candidate. `fleetd work inspect-extract` then:

1. revalidates the exact inspection request and generic candidate;
2. requires a clean checkout at the exact requested `HEAD`;
3. requires the exact report identity, request, repository, revision, and
   question set;
4. rejects non-normal or out-of-scope evidence paths; and
5. reads every citation from the exact Git object with `git show
   REVISION:path`, requiring UTF-8 source and a valid line range.

Success is reported as `conformant_candidate`. This proves structural coverage,
provenance, and that every cited location exists at the bound revision. It does
not prove the truth or completeness of a natural-language conclusion. A later
semantic consumer still decides whether to admit the report fact.

## Deliberate omissions

- No generic task or arbitrary command-execution contract.
- No write, patch, review, approval, or merge authority.
- No repository discovery, branch-name semantics, or mutable revision ranges.
- No claim that path scope is a confidentiality boundary.
- No stable status until a second independent implementation is qualified.

