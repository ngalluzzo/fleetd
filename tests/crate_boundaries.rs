//! Structural boundaries that keep the workspace buildable in parallel.
//!
//! These assertions are about the dependency graph, not behavior. They exist
//! because a boundary that lives only in a document drifts the moment someone
//! adds a convenient import.
//!
//! ## What is no longer checked here
//!
//! Four assertions retired when `execution` became a crate, because the build
//! began holding them:
//!
//! - *only the kernel adds methods to `Store`* -- the orphan rule. `impl Store`
//!   outside `fleetd-kernel` is `error[E0116]`; verified rather than assumed.
//! - *execution does not depend on the layer that exposes it* -- `http` and
//!   `mcp` live in the daemon, and `no_workspace_member_builds_against_the_daemon`
//!   already forbids a member depending on it.
//! - *the kernel does not speak HTTP* and *the kernel does not depend on what is
//!   layered above it* -- both are now
//!   `the_kernel_crate_depends_only_on_storage_crates`: a crate cannot name what
//!   it does not depend on.
//!
//! A text check that survives here does so because nothing else can hold it.

use std::{
    fs,
    path::{Path, PathBuf},
};

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
#[test]
fn http_route_domains_do_not_import_each_other() {
    let directory = workspace_root().join("crates/http/src");
    for domain in HTTP_ROUTE_DOMAINS {
        let source = fs::read_to_string(directory.join(format!("{domain}.rs")))
            .unwrap_or_else(|_| panic!("http route domain module {domain}"));
        for sibling in HTTP_ROUTE_DOMAINS {
            if sibling == domain {
                continue;
            }
            for reference in [
                format!("super::{sibling}"),
                format!("crate::http::{sibling}"),
            ] {
                assert!(
                    !source.contains(&reference),
                    "http route domain `{domain}` reaches sibling `{sibling}`. Domains share \
                     composition and `guard`, not each other; duplicate the small thing or \
                     lift it into `guard`."
                );
            }
        }
    }
}

#[test]
fn http_composition_registers_every_route_domain() {
    let composition = fs::read_to_string(workspace_root().join("crates/http/src/lib.rs"))
        .expect("http composition module");
    // One list declares the modules and builds the contract, so a domain cannot
    // be declared without being reachable. What is left to check is that the
    // list and this inventory still describe the same set.
    let start = composition
        .find("route_domains!(")
        .expect("http composition declares its route domains with `route_domains!`");
    let list = &composition[start + "route_domains!(".len()..];
    let list = &list[..list
        .find(");")
        .expect("`route_domains!` list is terminated")];
    let declared: Vec<&str> = list
        .split(',')
        .map(str::trim)
        .filter(|domain| !domain.is_empty())
        .collect();

    for domain in HTTP_ROUTE_DOMAINS {
        assert!(
            declared.contains(&domain),
            "http route domain `{domain}` is missing from `route_domains!`, so it is neither \
             declared nor merged and its routes are unreachable"
        );
    }
    for domain in &declared {
        assert!(
            HTTP_ROUTE_DOMAINS.contains(domain),
            "`route_domains!` lists `{domain}`, which is not in HTTP_ROUTE_DOMAINS. Adding a \
             route domain means naming it in both, so the declared layers stay honest."
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
        // A module re-export may be grouped: `pub use other::{alpha, beta};`.
        // What may never appear at the root is an individual type.
        let path = statement
            .trim_start_matches("pub use ")
            .trim_end_matches(';');
        let leaves: Vec<&str> = match (path.find('{'), path.rfind('}')) {
            (Some(open), Some(close)) => path[open + 1..close].split(',').collect(),
            _ => vec![path.rsplit("::").next().expect("re-export path segment")],
        };
        for leaf in leaves {
            let name = leaf
                .trim()
                .rsplit("::")
                .next()
                .expect("re-export leaf")
                .split_whitespace()
                .next_back()
                .expect("re-export leaf name");
            assert!(
                name.chars().next().is_some_and(char::is_lowercase),
                "crate root re-exports the type `{name}` in `{statement}`. A flat root list \
                 is a conflict magnet — every change appends to the same block. Re-export \
                 whole modules and let consumers name the one they depend on."
            );
        }
    }
}

/// The layer that decides what happens to durable state.
const EXECUTION_MODULES: [&str; 7] = [
    "controller",
    "invocation",
    "message_grant",
    "operations",
    "session_binding",
    "settlement",
    "worker",
];

/// Every layer left in the daemon, as a directory under `src/`.
///
/// Both are surfaces, named for a mechanism, which is what having two of them
/// makes plain. A new surface is a new entry here rather than a folder nobody
/// notices. `execution` left for `crates/execution`.
const SOURCE_LAYERS: [&str; 0] = [];

/// The layer that exposes it. Route domains own handlers; the rest is
/// composition, shared guards, and the transport beneath them.
const HTTP_ROUTE_DOMAINS: [&str; 7] = [
    "agents",
    "channels",
    "deliveries",
    "invocations",
    "messages",
    "operations",
    "streams",
];

const HTTP_SUPPORT: [&str; 7] = [
    "browser_stream_edge",
    "channel_stream",
    "error",
    "guard",
    "meta",
    "stream_grant_broker",
    "surface",
];

fn collect_sources(path: &Path, sources: &mut Vec<PathBuf>) {
    if path.is_dir() {
        for entry in fs::read_dir(path).expect("module directory is readable") {
            collect_sources(&entry.expect("module entry is readable").path(), sources);
        }
        return;
    }
    if path.extension().is_some_and(|extension| extension == "rs") {
        sources.push(path.to_owned());
    }
}

/// Every crate above the substrate that could reach a kernel table.
///
/// `Store::pool()` and `Store::begin_immediate()` both hand out an executor that
/// accepts any SQL, so this rule cannot be a type -- see the note on
/// `only_the_kernel_writes_kernel_tables`. That makes the list of places it is
/// checked load-bearing: a new crate above the substrate that is missing here is
/// unchecked, not compliant.
const ABOVE_SUBSTRATE_CRATES: [&str; 4] = [
    "crates/conversation/src",
    "crates/execution/src",
    "crates/http/src",
    "crates/mcp/src",
];

/// Checks the one rule in this file that cannot be made structural.
///
/// Turning a module boundary into a crate boundary converts most of these
/// assertions into compile errors. Not this one. `Store::pool()` and
/// `Store::begin_immediate()` both return a sqlx executor, and an executor
/// accepts any SQL string, so no signature distinguishes reading a table this
/// layer owns from deleting one it does not. `begin_immediate` cannot be
/// withdrawn either: a delivery transition and the invocation fence settling it
/// have to commit in one transaction, which is the whole reason a layer above
/// the kernel is handed one.
///
/// Verified by writing `DELETE FROM channels` in `execution/settlement` through
/// `begin_immediate` alone: it compiled with no errors and only this test
/// objected. Separate databases would make it structural and would also make
/// that shared transaction impossible.
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
    let mut sources: Vec<(String, PathBuf)> = Vec::new();
    for crate_root in ABOVE_SUBSTRATE_CRATES {
        let root = workspace_root().join(crate_root);
        let mut crate_sources = Vec::new();
        collect_sources(&root, &mut crate_sources);
        assert!(
            !crate_sources.is_empty(),
            "{crate_root} holds no sources to check"
        );
        for path in crate_sources {
            sources.push((crate_root.to_owned(), path));
        }
    }
    for (module, path) in sources {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|_| panic!("module {module} at {}", path.display()));
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

/// Everything `fleetd-kernel` is allowed to depend on.
///
/// The kernel owns the authoritative store. A web framework, an HTTP client, or
/// a plugin process here would mean every consumer of durable state builds them.
const KERNEL_ALLOWED_DEPENDENCIES: [&str; 11] = [
    "base64",
    "fleetd-proto",
    "getrandom",
    "serde_json",
    "sha2",
    "sqlx",
    "tempfile",
    "thiserror",
    "tokio",
    "tokio-util",
    "tracing",
];

/// Everything `fleetd-conversation` is allowed to depend on.
///
/// It is a read model over the substrate. A web framework here would mean the
/// projection had started rendering itself, and a plugin process would mean it
/// had started doing work. This list is why neither is possible rather than
/// merely discouraged: the crate cannot name what it does not depend on.
const CONVERSATION_ALLOWED_DEPENDENCIES: [&str; 3] = ["fleetd-kernel", "fleetd-proto", "sqlx"];

/// Everything `fleetd-execution` is allowed to depend on.
///
/// This layer decides what happens to durable state. A web framework or an MCP
/// server here would mean it had started exposing something, which is what the
/// retired *execution does not speak HTTP* text check used to look for. It is a
/// fact about the build now: a surface provisions endpoints and hands the worker
/// a `TurnGrant`.
const EXECUTION_ALLOWED_DEPENDENCIES: [&str; 13] = [
    "fleetd-kernel",
    "fleetd-plugin-host",
    "fleetd-proto",
    "futures-util",
    "schemars",
    "serde",
    "serde_json",
    "sha2",
    "sqlx",
    "thiserror",
    "tokio",
    "tokio-util",
    "tracing",
];

#[test]
fn the_execution_crate_speaks_no_transport() {
    let manifest = fs::read_to_string(workspace_root().join("crates/execution/Cargo.toml"))
        .expect("execution manifest");
    for table in [
        "[dependencies]",
        "[dev-dependencies]",
        "[build-dependencies]",
    ] {
        for dependency in declared_dependencies(&manifest, table) {
            assert!(
                EXECUTION_ALLOWED_DEPENDENCIES.contains(&dependency.as_str())
                    || dependency == "uuid",
                "fleetd-execution {table} contains `{dependency}`. Deciding what happens to \
                 durable state must not mean exposing it; see EXECUTION_ALLOWED_DEPENDENCIES."
            );
        }
    }
}

#[test]
fn the_conversation_crate_depends_only_on_the_substrate() {
    let manifest = fs::read_to_string(workspace_root().join("crates/conversation/Cargo.toml"))
        .expect("conversation manifest");
    for table in [
        "[dependencies]",
        "[dev-dependencies]",
        "[build-dependencies]",
    ] {
        for dependency in declared_dependencies(&manifest, table) {
            assert!(
                CONVERSATION_ALLOWED_DEPENDENCIES.contains(&dependency.as_str()),
                "fleetd-conversation {table} contains `{dependency}`. The projection reads the \
                 substrate and shapes it; see CONVERSATION_ALLOWED_DEPENDENCIES."
            );
        }
    }
}

#[test]
fn the_substrate_does_not_know_about_its_projections() {
    let manifest =
        fs::read_to_string(workspace_root().join("crates/kernel/Cargo.toml")).expect("manifest");
    assert!(
        !manifest.contains("fleetd-conversation"),
        "fleetd-kernel depends on fleetd-conversation. A conversation is one way to read the \
         substrate, not something the substrate knows it has; the direction is what makes the \
         projection replaceable."
    );
}

#[test]
fn the_kernel_crate_depends_only_on_storage_crates() {
    let manifest = fs::read_to_string(workspace_root().join("crates/kernel/Cargo.toml"))
        .expect("kernel manifest");
    for table in [
        "[dependencies]",
        "[dev-dependencies]",
        "[build-dependencies]",
    ] {
        for dependency in declared_dependencies(&manifest, table) {
            assert!(
                KERNEL_ALLOWED_DEPENDENCIES.contains(&dependency.as_str()) || dependency == "uuid",
                "fleetd-kernel {table} contains `{dependency}`. The kernel persists the six \
                 concepts and nothing else; see KERNEL_ALLOWED_DEPENDENCIES."
            );
        }
    }
}

/// JavaScript packages in the workspace.
const JAVASCRIPT_PACKAGES: [&str; 3] = [
    "apps/conversation-desktop",
    "apps/conversation-web",
    "clients/typescript",
];

#[test]
fn javascript_packages_import_each_other_by_name() {
    for package in JAVASCRIPT_PACKAGES {
        for directory in ["src", "test"] {
            let root = workspace_root().join(package).join(directory);
            if !root.is_dir() {
                continue;
            }
            inspect_imports(&root, package);
        }
    }
}

fn inspect_imports(path: &Path, package: &str) {
    if path.is_dir() {
        for entry in fs::read_dir(path).expect("package directory is readable") {
            inspect_imports(&entry.expect("package entry is readable").path(), package);
        }
        return;
    }
    let is_source = path
        .extension()
        .is_some_and(|extension| extension == "ts" || extension == "mjs");
    if !is_source {
        return;
    }
    let source = fs::read_to_string(path).expect("package source is UTF-8");
    for line in source.lines() {
        let trimmed = line.trim_start();
        let is_import = trimmed.starts_with("import ") || trimmed.starts_with("} from ");
        // Two levels up still lands inside the package; three leaves it.
        assert!(
            !(is_import && line.contains("../../../")),
            "`{package}` reaches outside itself by relative path in {}:\n  {}\nImport the \
             other package by name so its `exports` map stays the contract.",
            path.display(),
            trimmed
        );
    }
}

/// Files at the root of `src/` that are not part of a layer.
///
/// The library root, the binary, and its command line. Everything else belongs
/// to a layer directory, so the tree reads as the architecture.
const UNLAYERED_ROOT_FILES: [&str; 3] = ["lib.rs", "main.rs", "cli.rs"];

fn rust_module_names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(directory)
        .unwrap_or_else(|_| panic!("layer directory {}", directory.display()))
        .filter_map(|entry| {
            let path = entry.expect("layer entry is readable").path();
            let is_rust = path.extension().is_some_and(|extension| extension == "rs");
            let stem = path.file_stem()?.to_str()?.to_owned();
            (is_rust && stem != "mod").then_some(stem)
        })
        .collect();
    names.sort();
    names
}

#[test]
fn the_source_tree_matches_the_declared_layers() {
    let source = workspace_root().join("src");

    // Nothing sits outside a layer.
    let mut stray: Vec<String> = fs::read_dir(&source)
        .expect("src is readable")
        .filter_map(|entry| {
            let path = entry.expect("src entry is readable").path();
            if path.is_dir() {
                return None;
            }
            let name = path.file_name()?.to_str()?.to_owned();
            (!UNLAYERED_ROOT_FILES.contains(&name.as_str())).then_some(name)
        })
        .collect();
    stray.sort();
    assert!(
        stray.is_empty(),
        "these modules sit at the root of src/ rather than in a layer: {stray:?}. Put each \
         one in a layer -- `execution`, or a surface -- so the tree keeps showing what \
         depends on what."
    );

    // A whole layer must not appear without being declared.
    let mut layers: Vec<String> = fs::read_dir(&source)
        .expect("src is readable")
        .filter_map(|entry| {
            let path = entry.expect("src entry is readable").path();
            // `bin` is Cargo's directory for extra binaries, not a layer.
            let name = path.file_name()?.to_str()?.to_owned();
            (path.is_dir() && name != "bin").then_some(name)
        })
        .collect();
    layers.sort();
    let mut expected_layers: Vec<String> = SOURCE_LAYERS
        .iter()
        .map(|layer| (*layer).to_owned())
        .collect();
    expected_layers.sort();
    assert_eq!(
        layers, expected_layers,
        "the directories under src/ do not match SOURCE_LAYERS. A layer was added or removed \
         without saying what it is: `execution` decides what happens to durable state, and a \
         surface exposes it over one mechanism."
    );

    let mut expected_execution: Vec<String> =
        EXECUTION_MODULES.iter().map(|m| (*m).to_owned()).collect();
    expected_execution.push("lib".to_owned());
    expected_execution.sort();
    assert_eq!(
        rust_module_names(&workspace_root().join("crates/execution/src")),
        expected_execution,
        "crates/execution does not match EXECUTION_MODULES. A module was added or removed \
         without saying which layer owns it."
    );

    let mut expected_http: Vec<String> = HTTP_ROUTE_DOMAINS
        .iter()
        .chain(HTTP_SUPPORT.iter())
        .map(|m| (*m).to_owned())
        .collect();
    expected_http.push("lib".to_owned());
    expected_http.sort();
    assert_eq!(
        rust_module_names(&workspace_root().join("crates/http/src")),
        expected_http,
        "crates/http does not match its declared modules. A route domain belongs in \
         HTTP_ROUTE_DOMAINS and must be merged into a contract; anything else is support."
    );
}
