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

/// Route modules that own one domain each.
///
/// A handler module may reach composition (`super::AppState`) and the shared
/// guards. Reaching a sibling means two domains now change together, which is
/// the coupling splitting the router was meant to remove.
const API_DOMAINS: [&str; 7] = [
    "agents",
    "channels",
    "deliveries",
    "invocations",
    "messages",
    "operations",
    "streams",
];

#[test]
fn api_domains_do_not_import_each_other() {
    let directory = workspace_root().join("src/api");
    for domain in API_DOMAINS {
        let source = fs::read_to_string(directory.join(format!("{domain}.rs")))
            .unwrap_or_else(|_| panic!("api domain module {domain}"));
        for sibling in API_DOMAINS {
            if sibling == domain {
                continue;
            }
            for reference in [
                format!("super::{sibling}"),
                format!("crate::api::{sibling}"),
            ] {
                assert!(
                    !source.contains(&reference),
                    "api domain `{domain}` reaches sibling `{sibling}`. Domains share \
                     composition and `guard`, not each other; duplicate the small thing or \
                     lift it into `guard`."
                );
            }
        }
    }
}

#[test]
fn api_composition_module_registers_every_domain() {
    let composition = fs::read_to_string(workspace_root().join("src/api/mod.rs"))
        .expect("api composition module");
    for domain in API_DOMAINS {
        assert!(
            composition.contains(&format!("mod {domain};")),
            "api composition does not declare `{domain}`"
        );
        assert!(
            composition.contains(&format!("{domain}::routes()")),
            "api domain `{domain}` is declared but never merged into a contract, so its \
             routes are unreachable"
        );
    }
}

/// Returns each `pub use ...;` statement in a source file, joined onto one line.
fn re_export_statements(source: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut rest = source;
    while let Some(start) = rest.find("pub use ") {
        rest = &rest[start..];
        let Some(end) = rest.find(';') else { break };
        statements.push(rest[..end].split_whitespace().collect::<Vec<_>>().join(" "));
        rest = &rest[end + 1..];
    }
    statements
}

#[test]
fn crate_root_re_exports_modules_only() {
    let lib = fs::read_to_string(workspace_root().join("src/lib.rs")).expect("crate root");
    for statement in re_export_statements(&lib) {
        assert!(
            !statement.contains('{'),
            "crate root re-exports individual items: `{statement}`. A flat root list is a \
             conflict magnet — every change appends to the same sorted block. Let consumers \
             name the owning module instead."
        );
        let last = statement
            .trim_end_matches(';')
            .rsplit("::")
            .next()
            .expect("re-export path segment");
        assert!(
            last.chars().next().is_some_and(char::is_lowercase),
            "crate root re-exports the type `{last}`. Only whole modules may be re-exported \
             from the root."
        );
    }
}

/// The six concepts ARCHITECTURE.md gives the kernel, as modules.
const KERNEL_MODULES: [&str; 6] = [
    "auth",
    "delivery",
    "error",
    "message_commit_hint",
    "model",
    "store",
];

/// Everything layered above the kernel.
const ABOVE_KERNEL: [&str; 9] = [
    "api",
    "controller",
    "invocation",
    "message_grant_broker",
    "operations",
    "session_binding",
    "settlement",
    "stream_grant_broker",
    "worker",
];

/// Whether a source file names another crate module, written either inline as
/// `crate::other::Item` or as an entry inside a grouped `use crate::{ .. };`.
fn references(source: &str, module: &str) -> bool {
    if source.contains(&format!("crate::{module}::")) {
        return true;
    }
    let mut rest = source;
    while let Some(start) = rest.find("use crate::{") {
        rest = &rest[start..];
        let Some(end) = rest.find("};") else { break };
        if rest[..end].contains(&format!("{module}::")) {
            return true;
        }
        rest = &rest[end + 2..];
    }
    false
}

#[test]
fn the_kernel_does_not_depend_on_what_is_layered_above_it() {
    for module in KERNEL_MODULES {
        let path = workspace_root().join(format!("src/{module}.rs"));
        let Ok(source) = fs::read_to_string(&path) else {
            continue;
        };
        for above in ABOVE_KERNEL {
            assert!(
                !references(&source, above),
                "kernel module `{module}` reaches `{above}`, which is layered above it. \
                 A delivery row transition and the invocation fence settling it belong in \
                 one transaction, but the composition belongs above the kernel — see \
                 `settlement` and docs/adr/0026-delivery-settlement-composition.md."
            );
        }
    }
}

#[test]
fn only_the_kernel_writes_kernel_tables() {
    const KERNEL_TABLES: [&str; 7] = [
        "agents",
        "auth_credentials",
        "agent_deliveries",
        "channel_members",
        "channels",
        "delivery_blocks",
        "messages",
    ];
    for module in ABOVE_KERNEL {
        let path = workspace_root().join(format!("src/{module}.rs"));
        let Ok(source) = fs::read_to_string(&path) else {
            continue;
        };
        for table in KERNEL_TABLES {
            for verb in ["INSERT INTO", "UPDATE", "DELETE FROM"] {
                assert!(
                    !source.contains(&format!("{verb} {table}")),
                    "`{module}` writes the kernel table `{table}` directly (`{verb}`). The \
                     delivery and message state machines live in the kernel; call the \
                     transactional function it exposes so one module owns each transition."
                );
            }
        }
    }
}

#[test]
fn only_the_kernel_adds_methods_to_the_store() {
    for module in ABOVE_KERNEL {
        let path = workspace_root().join(format!("src/{module}.rs"));
        let Ok(source) = fs::read_to_string(&path) else {
            continue;
        };
        assert!(
            !source.contains("impl Store"),
            "`{module}` adds methods to `Store`, which the kernel owns. Once these layers \
             are crates that is an orphan-rule error, so compose over `&Store` with a free \
             function instead."
        );
    }
}
