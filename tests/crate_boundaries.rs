//! Structural boundaries that keep the workspace buildable in parallel.
//!
//! These assertions are about the dependency graph, not behavior. They exist
//! because a boundary that lives only in a document drifts the moment someone
//! adds a convenient import.

use std::{fs, path::Path};

/// Everything `fleetd-proto` is allowed to depend on.
///
/// This crate is what a harness vendor or external tool compiles instead of the
/// daemon. Adding anything that opens a socket, a database, or a runtime here
/// would put the whole daemon back into every plugin build.
const PROTO_ALLOWED_DEPENDENCIES: [&str; 4] = ["semver", "serde", "serde_json", "utoipa"];

fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
}

/// Returns the crate names declared in one `[dependencies]`-style table.
fn declared_dependencies(manifest: &str, table: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut inside = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            inside = line == table;
            continue;
        }
        if !inside || line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((name, _)) = line.split_once('=') {
            names.push(name.trim().to_owned());
        }
    }
    names
}

fn member_manifests() -> Vec<(String, String)> {
    let root = workspace_root();
    let root_manifest = fs::read_to_string(root.join("Cargo.toml")).expect("root manifest");
    let members = root_manifest
        .split_once("members = [")
        .expect("workspace members")
        .1
        .split_once(']')
        .expect("workspace members terminate")
        .0;
    members
        .split(',')
        .map(|entry| entry.trim().trim_matches('"').trim())
        .filter(|entry| !entry.is_empty() && *entry != ".")
        .map(|entry| {
            let manifest = fs::read_to_string(root.join(entry).join("Cargo.toml"))
                .unwrap_or_else(|_| panic!("manifest for workspace member {entry}"));
            (entry.to_owned(), manifest)
        })
        .collect()
}

#[test]
fn proto_depends_only_on_serialization_crates() {
    let manifest = fs::read_to_string(workspace_root().join("crates/proto/Cargo.toml"))
        .expect("proto manifest");
    for table in [
        "[dependencies]",
        "[dev-dependencies]",
        "[build-dependencies]",
    ] {
        for dependency in declared_dependencies(&manifest, table) {
            assert!(
                PROTO_ALLOWED_DEPENDENCIES.contains(&dependency.as_str()),
                "fleetd-proto {table} contains `{dependency}`, which is not a serialization or \
                 schema crate. Wire types must stay compilable without the daemon; see \
                 PROTO_ALLOWED_DEPENDENCIES."
            );
        }
    }
}

#[test]
fn no_workspace_member_builds_against_the_daemon() {
    for (member, manifest) in member_manifests() {
        for dependency in declared_dependencies(&manifest, "[dependencies]") {
            assert_ne!(
                dependency, "fleetd",
                "workspace member `{member}` takes a normal dependency on the daemon crate. \
                 Plugins, hosts, and tools consume `fleetd-proto`; only test targets may reach \
                 for the daemon's own types."
            );
        }
    }
}

#[test]
fn proto_source_is_free_of_persistence_and_transport() {
    let mut checked = 0;
    inspect(&workspace_root().join("crates/proto/src"), &mut checked);
    assert!(checked > 0, "proto sources are readable");
}

fn inspect(path: &Path, checked: &mut usize) {
    if path.is_dir() {
        for entry in fs::read_dir(path).expect("proto directory is readable") {
            inspect(&entry.expect("proto entry is readable").path(), checked);
        }
        return;
    }
    if path.extension().is_none_or(|extension| extension != "rs") {
        return;
    }
    *checked += 1;
    let source = fs::read_to_string(path).expect("proto source is UTF-8");
    for forbidden in ["sqlx", "axum", "tokio", "reqwest", "std::fs", "std::net"] {
        assert!(
            !source.contains(forbidden),
            "proto source {} references `{forbidden}`. Wire types describe frames; they do \
             not read, store, or transport them.",
            path.display()
        );
    }
}
