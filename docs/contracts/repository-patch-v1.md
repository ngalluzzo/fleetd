# Repository patch v1

Status: experimental capability specialization; no provider is qualified yet

This contract delegates production of one patch artifact against an exact Git
revision. It specializes capability work and repository evidence without
granting the provider mutable workspace, commit, push, review, approval, or
merge authority.

## Exact identities

| Role | Identity |
| --- | --- |
| Capability | `dev.fleetd.capability/propose_repository_patch@0.1.0` |
| Input fact | `dev.fleetd.fact/repository_change_brief@0.1.0` |
| Output artifact | `dev.fleetd.artifact/repository_patch@0.1.0` |
| Suite | `dev.fleetd.conformance/repository_patch@0.1.0` |

The request envelope remains `work.capability.request/v1`; the provider attempt
remains `work.capability.attempt/v2`.

## Change-brief fact

The provider-neutral input payload is:

```json
{
  "schema_version": 1,
  "repository_id": "dev.fleetd/fleetd",
  "base_revision": "40-or-64-character-lowercase-hex-commit-id",
  "path_scope": ["src", "tests"],
  "objective": "Add one bounded behavior.",
  "acceptance_criteria": ["An observable property that the change should satisfy."],
  "constraints": ["A semantic or architectural constraint."]
}
```

Repository IDs are opaque bounded strings. A path scope is `.` or a normalized,
control-free repository-relative path made only of normal components. V1 accepts
1–32 scopes, 1–32 unique acceptance criteria, and 0–32 unique constraints. The
base revision must be one lowercase 40- or 64-character commit ID.

`fleetd work patch-bind --brief BRIEF.json` validates the brief and binds its
canonical payload and this derivation into the input fact identity:

```json
{
  "kind": "git_base_revision",
  "repository_id": "dev.fleetd/fleetd",
  "base_revision": "exact-commit-id"
}
```

The generic request identity then binds that exact fact, capability,
complete-only input requirement, output artifact type, and conformance suite.

## Provider execution

`adapter.kind: repository_patch_v1` admits one exact configured semantic
provider. Before dispatch can arm, it uses one configured absolute Git
executable to prove that the worker's absolute working directory is Git's
canonical top level, `HEAD` equals the brief's base revision, and tracked plus
untracked status is empty.

The provider receives the generic capability response schema and exact patch
payload shape. It is instructed to propose only a complete unified diff and not
to modify the checkout, create files, commit, or push. This is a semantic
instruction and evidence boundary, not an operating-system sandbox. Use an
isolated worktree with no secrets.

## Patch artifact claim

The sole complete output artifact payload is:

```json
{
  "schema_version": 1,
  "request_id": "sha256:...",
  "repository_id": "dev.fleetd/fleetd",
  "base_revision": "exact-commit-id",
  "summary": "Bounded summary of the proposed change.",
  "changed_paths": ["src/example.rs", "tests/example.rs"],
  "patch": "diff --git a/src/example.rs b/src/example.rs\n...",
  "limitations": []
}
```

`changed_paths` must be the strictly sorted, unique, exact path set claimed by
the patch. V1 permits 1–64 changed paths, a patch of at most 256 KiB, regular
text files only, and bounded summary and limitation strings. The proposal is an
untrusted claim until deterministic conformance succeeds.

## Isolated Git conformance

`fleetd work patch-extract` first strictly lifts the immutable attempt through
the generic capability contract. The repository-patch suite then:

1. revalidates the exact request, candidate, proposal binding, clean worktree,
   and base `HEAD`;
2. seeds a temporary Git index from the base revision without modifying the
   worktree or its real index;
3. runs `git apply --cached --check --whitespace=error-all`, then applies the
   patch only to that temporary index;
4. asks Git for the exact no-rename, NUL-delimited changed path set and requires
   it to equal the provider's sorted claim and remain within scope;
5. rejects binary changes and any resulting symlink, Gitlink, or other
   non-regular mode; and
6. emits Git's canonical full-index, no-rename diff plus its raw-byte SHA-256
   digest.

Success is `conformant_candidate`. It proves that one exact, bounded text patch
applies to the bound revision and has the stated artifact identity. It does not
prove the natural-language objective or acceptance criteria, run the repository
test suite, review the implementation, authorize a commit, or mutate a branch.

## Deliberate omissions

- No generic task or command execution.
- No provider write access to the authoritative checkout.
- No implicit test, semantic-review, or safety claim.
- No commit, branch, push, pull-request, approval, or merge operation.
- No binary file, preserved rename metadata, symlink, submodule, or special-mode
  artifact; canonical output represents path changes with rename inference off.
- No stable status until a second independent implementation is qualified.
