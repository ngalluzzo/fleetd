use std::{fs, path::Path};

use fleetd::plugin::{PluginIdentity, PluginInterface, PluginManifest};
use semver::Version;

#[test]
fn plugin_manifest_negotiates_only_operational_interfaces() {
    let manifest = PluginManifest {
        protocol_version: 1,
        plugin: PluginIdentity {
            id: "fleetd.harness.test".to_owned(),
            name: "Test harness".to_owned(),
            version: Version::new(1, 0, 0),
        },
        interfaces: vec![PluginInterface::new(
            "fleetd.harness-acp",
            Version::new(0, 1, 0),
        )],
    };

    let encoded = serde_json::to_value(manifest).expect("manifest encodes");
    assert_eq!(encoded["interfaces"][0]["id"], "fleetd.harness-acp");
    assert!(encoded.get("capability_offers").is_none());
}

#[test]
fn runtime_workspace_contains_no_semantic_compiler_dependency() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    for relative in ["Cargo.toml", "src", "crates", "plugins"] {
        inspect(&root.join(relative));
    }
}

fn inspect(path: &Path) {
    if path.is_dir() {
        for entry in fs::read_dir(path).expect("runtime directory is readable") {
            let entry = entry.expect("runtime directory entry is readable");
            let child = entry.path();
            if child.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            inspect(&child);
        }
        return;
    }

    let relevant = path.file_name().is_some_and(|name| name == "Cargo.toml")
        || path.extension().is_some_and(|extension| extension == "rs");
    if !relevant {
        return;
    }

    let source = fs::read_to_string(path).expect("runtime source is UTF-8");
    for forbidden in [
        "gooir",
        "CapabilityOffer",
        "CapabilityInvocation",
        "CapabilityCandidate",
        "CapabilityResult",
        "CapabilityNeed",
        "CapabilitySpec",
        "capability_offers",
    ] {
        assert!(
            !source.contains(forbidden),
            "Fleetd runtime source {} contains semantic compiler concept `{forbidden}`",
            path.display()
        );
    }
}
