# GOOIR list-agents adapter admission — 2026-08-27

Fleetd admits the generated adapter for protected `GET /v1/agents` from the
external `gooir-fleetd-http` integration commit
`912e82e5eda4b32bd392419f08639e72eb975b1f`.

The integration compiled native HTTP, exact Fleetd handler bindings, and the
Axum target profile through two neutral provider invocations. The candidate
bundle retains the input fact identities, acceptance records, correlated
results, intermediate Axum service, and final Rust source tree. The generated
source is deterministic; each bundle separately records the exact executable
that performed the compilation. The compiler read Fleetd only through the
operation-port revision
`cf00d81831128a1ea45006535347438ffb53a3e5` and never wrote this repository.

Exact provenance:

- GOOIR: `4d0cc31799be4e5ab1a4f60b5d6f894bc0eb9fa3`
- reusable HTTP/Axum providers:
  `ba82ce1d9f45799608e283beb53d0b05a470d14e`
- measured compiler executable:
  `sha256:8a9f2bfed35598cbfda407841bceea77f03707e6b3fad3d2d94f62f0fd9d671d`
  (`12,361,256` bytes)
- measured provider source closure:
  `sha256:3be6f4c144cd23b0b052817235f4c9d9b22a243a447962122dcefeb0cdb731fc`
- admitted generated source:
  `sha256:3c4e6292640ff8a52d3b0400aabf53b7e1774dee4da4a212fad0fcd3784ee5be`

The pinned Fleetd evidence includes the exact commit and tree plus Git blobs for
`Cargo.lock`, `crates/http/Cargo.toml`, `crates/http/src/agents.rs`, and
`openapi/fleetd-v1.json`. Acceptance validates the Axum-free operation port,
the protected OpenAPI operation, dependency declarations, and the selected
Axum `0.8.9`, Utoipa `5.5.0`, and Utoipa-Axum `0.2.0` lockfile versions.

Local admission verified that the candidate digest is exact, the generated
module imports no GOOIR crate, Fleetd compiles it as ordinary Axum code, and the
agent API suite preserves operator authentication, authorization, durable
ordering, JSON shape, and credential omission. The OpenAPI suite proves the
registered contract is unchanged and remains equal to the committed document.

Product behavior remains in `list_agents_operation`; the generated module owns
only extractors, JSON wrapping, registration, Utoipa metadata, and the bearer
security-scheme component. No compiler or semantic runtime is present in
Fleetd's dependency graph or request path.
