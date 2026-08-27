# GOOIR list-agents adapter admission — 2026-08-27

This record re-qualifies the already-admitted adapter for protected
`GET /v1/agents` against the current external `gooir-fleetd-http`
CompilerDriver-based bundle. The replay used integration checkout
`d9b918a2679d603d60c2787058064a43ef24da13` as reproducibility context only.
The bundle deliberately does not claim GOOIR or `gooir-http` source revisions,
and this record does not promote the checkout revision into semantic
provenance. The external integration owns the bundle.

The integration was built and run with:

```sh
cargo build --quiet -p fleetd-http-integration-v0 --bins
target/debug/compile-list-agents --artifacts-dir target/debug
```

## Bundle and executable

- schema:
  `com.productcolab.fleetd.http/list-agents-compilation-bundle@0.2.0`
- serialized bundle stdout:
  `sha256:7ecf2bdb3979d7b337d728fbb84cf90e3d85fe4c4891d79701bd325424936b63`
- measured observer executable:
  `sha256:daf518a00c4c25f3b5b45efdf118c9fc60ef9fe212f5b2b3d22220215af5071d`
  (`14,875,240` bytes)
- pinned Fleetd operation-port commit:
  `cf00d81831128a1ea45006535347438ffb53a3e5`
- pinned Fleetd tree:
  `a08da9f78dd71bbe1b089cc73bf80842eccfc16e`

The Fleetd evidence retains exact Git blobs for `Cargo.lock`,
`crates/http/Cargo.toml`, `crates/http/src/agents.rs`, and
`openapi/fleetd-v1.json`. Its validations cover the Axum-free operation port,
protected OpenAPI operation, dependency declarations, and Axum `0.8.9`, Utoipa
`5.5.0`, and Utoipa-Axum `0.2.0` lockfile versions.

## Installed packages and artifacts

The integration loaded these package documents by content digest into the
registry consumed by CompilerDriver:

- native HTTP:
  `sha256:7eb129b9029233b86def383aed799fd059d99f0a614041347d2fd523795c8197`
- Axum dialect:
  `sha256:dbcd0aa50145200c64997a910ec87ea159fa44caf778617c42dee849e16c65b1`
- Rust source-tree dialect:
  `sha256:eb42f09b781f7a87210302238880ded9ca20b53ce4621ae368e17053eb140150`
- HTTP/Axum provider package:
  `sha256:9114ce1e532e3d717786c13a6842f751e0028cf664ebdfd44ae9c5c11e43e284`

The provider package names four exact executable artifacts:

- HTTP to Axum provider:
  `sha256:1ae5a1826c462ffab7a96679ba74b82ddd0fa255a3740025c3ba0dc5d5c9e906`
- Axum to Rust provider:
  `sha256:66df3b1dd4121cb37b339b4dabea5ce0ff6dadf6f3f4142b1898d600f784fd99`
- independent HTTP to Axum attester:
  `sha256:9c21e5e2fa6a254a684285e7e3a5a22f9b0fb9abbb91c16fe7714889eb8550c2`
- independent Axum to Rust attester:
  `sha256:f8d8101152be912ebbe9498df548c2c82d155c6f86efe151ff0a91c4e6fe769e`

The package loader derived, rather than manually assembled, these offers:

- HTTP to Axum:
  `sha256:3841d7c2878743ce9eea8755bea02312768f3e0485f8f88e9d0e2712f709429a`
- Axum to Rust:
  `sha256:ac73629c07a9d5929e36524656f70d8a11e735a9fac237f9c853ca2b5fead784`

## Source admission and selected compilation

The exact source observations and resulting authority records are:

- native HTTP observation
  `sha256:a1495c64137f5a347247dfd14debcdfa41dfb49bf3dfb0139d14c901596ec91e`,
  authority
  `sha256:60b04bb365e175414946dcd492f96b5d6ef2648f416627d25dce582ab2eb2e43`,
  decision
  `sha256:49ce26c675da05a1284387ad886496dd1a32bb8d6e527a3b559924f507c71783`
- handler-bindings observation
  `sha256:7f016483840655aabfe57121c70fc89e79654255941ca3026402a0850ff8334a`,
  authority
  `sha256:6e99ecc3646b09ce18390080dff5b29e7c3a714e79b4aa3cca5cc687008ad92a`,
  decision
  `sha256:11711423aa5207d163da1bfdd186d63570d3f51d85b28168aa3f7dca7b30df2a`
- target-profile observation
  `sha256:30bb032bf3118da24217c1b37b204be05ecf749090db723ead0e243f4c0ea627`,
  authority
  `sha256:f12e83c67bb9302d719abc0973258ac83dfe9e103ebcab417fac3bff1e191109`,
  decision
  `sha256:8c3e3262f76ae41409eaf1966f90354d8eceb45a6607e069cd3c759ec2ac142a`

All three were admitted under explicit policy
`sha256:8d7d39b7e77399cf78533128a961567a878c5944bb16c0febb5aae7f5cc7b56d`.
Selection
`sha256:d46966d5acf5a416156b058c51de1f3990a2916f35bcc96523930ea92de73d5b`
then produced two admitted hops:

- HTTP to Axum authority
  `sha256:bf19a3ab11d59a5f0b707b0dcf3746086942f1020b4c2b1bcd5e7da1f1d99b79`;
  conformance assessment
  `sha256:e3899178b87a163284da7fdeda969fd5c8186e214e3ab8d63f402848a07dfc94`
  passed; admission decision
  `sha256:82267cdc382540ec74cab56ad7d824b146b1cce7ec0bea953642ba6d6168bb34`
- final Rust source-tree fact
  `sha256:b465ed66a8c4779f98b052589eeb493fc955d37818651f8cb5260a99b7891c37`,
  authority
  `sha256:07026b91b88d5936e47d3e7767cae4637923e5c66a0cb3a12d03e82148cddbbc`;
  conformance assessment
  `sha256:835c73569375b7181d1fd5c205aaaa5e3803e9cd9b0dd5ccd58736c7065db268`
  passed; admission decision
  `sha256:4c722297bf8c96f6adb0e874bd773557d5c19772a81dd27fac57112761245862`

## Fleetd admission result

The final file is
`crates/http/src/agents/generated_list_agents.rs` with digest
`sha256:3c4e6292640ff8a52d3b0400aabf53b7e1774dee4da4a212fad0fcd3784ee5be`.
It is byte-for-byte equal to the file already admitted in Fleetd. The refreshed
bundle therefore required no source re-admission, generated-source change,
Cargo change, or runtime diff.

Product behavior remains in `list_agents_operation`; the generated module owns
only extraction, JSON wrapping, registration, Utoipa metadata, and the bearer
security-scheme component. Fleetd continues to compile it as ordinary Axum
source, with no compiler or semantic runtime in its dependency graph, build, or
request path.
