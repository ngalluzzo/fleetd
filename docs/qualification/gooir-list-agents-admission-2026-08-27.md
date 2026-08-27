# GOOIR list-agents adapter admission — 2026-08-27

Fleetd admits the generated adapter for protected `GET /v1/agents` from the
external `gooir-fleetd-http` integration commit
`bad756d13b6e0a95dd54dfad3e4d6dc8c06a38a4`.

The integration compiled native HTTP, exact Fleetd handler bindings, and the
Axum target profile through two neutral provider invocations. The candidate
bundle retains the input fact identities, acceptance records, correlated
results, intermediate Axum service, and final Rust source tree. The generated
source is deterministic; each bundle separately records the exact executable
that performed the compilation. The compiler read Fleetd only through the
operation-port revision
`cf00d81831128a1ea45006535347438ffb53a3e5` and never wrote this repository.

Exact provenance:

- GOOIR: `291519e160222e57bfb489bdf9d850bdae365ee6`
- reusable HTTP/Axum providers:
  `d72b536a5aa5616fa0edd93067e9964525085408`
- measured compiler executable:
  `sha256:dd305017ed3d18a767b8f4209b623a79c6757d6b28047e94d03a5c4145086c24`
  (`12,423,080` bytes)
- measured provider source closure:
  `sha256:a70a44f9f42065a5cf4be9f3a6e30e9f0f79c81b95ec27d0aea05d46f299e32d`
- admitted generated source:
  `sha256:3c4e6292640ff8a52d3b0400aabf53b7e1774dee4da4a212fad0fcd3784ee5be`

The pinned Fleetd evidence includes the exact commit and tree plus Git blobs for
`Cargo.lock`, `crates/http/Cargo.toml`, `crates/http/src/agents.rs`, and
`openapi/fleetd-v1.json`. Acceptance validates the Axum-free operation port,
the protected OpenAPI operation, dependency declarations, and the selected
Axum `0.8.9`, Utoipa `5.5.0`, and Utoipa-Axum `0.2.0` lockfile versions.

The provider closure is also measured from Cargo's actual sibling path inputs
at integration build time and embedded in the compiler executable. Each input
is a Cargo rebuild trigger, and compilation proceeds only when that embedded
digest equals the closure reconstructed from the pinned GOOIR and GOOIR HTTP
Git objects.

Local admission verified that the candidate digest is exact, the generated
module imports no GOOIR crate, Fleetd compiles it as ordinary Axum code, and the
agent API suite preserves operator authentication, authorization, durable
ordering, JSON shape, and credential omission. The OpenAPI suite proves the
registered contract is unchanged and remains equal to the committed document.

Product behavior remains in `list_agents_operation`; the generated module owns
only extractors, JSON wrapping, registration, Utoipa metadata, and the bearer
security-scheme component. No compiler or semantic runtime is present in
Fleetd's dependency graph or request path.
