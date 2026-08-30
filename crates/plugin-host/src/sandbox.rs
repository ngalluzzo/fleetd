//! Operating-system boundaries for model-directed plugin processes.
//!
//! A sandbox is declared before launch and wraps the complete plugin process
//! group. It does not inspect tool calls or trust the harness to report them.

use std::{
    collections::BTreeSet,
    ffi::OsString,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

const MACOS_SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

/// Network reach granted to a sandboxed plugin process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxNetwork {
    /// No inbound or outbound network operations.
    Deny,
    /// Outbound provider and tool traffic is permitted.
    AllowOutbound,
}

/// Named enforcement posture of one macOS Seatbelt boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MacOsSeatbeltPosture {
    /// Deny by default; grant only declared filesystem and network reach.
    Strict,
    /// Allow reads and network, but deny writes outside declared roots.
    WriteScoped,
}

impl MacOsSeatbeltPosture {
    /// Stable desired-state and evidence name for this posture.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::WriteScoped => "write_scoped",
        }
    }
}

/// A macOS Seatbelt profile derived entirely from operator desired state.
#[derive(Clone)]
pub struct MacOsSeatbeltSandbox {
    posture: MacOsSeatbeltPosture,
    profile: String,
    profile_digest: String,
}

impl MacOsSeatbeltSandbox {
    /// Builds a deny-by-default sandbox around declared filesystem roots.
    ///
    /// Writable roots are also readable. System runtimes remain readable so a
    /// dynamically linked plugin and ordinary build tools can start; no user
    /// directory is implicitly granted.
    ///
    /// # Errors
    ///
    /// Returns a bounded diagnostic when this is not macOS, Seatbelt is
    /// unavailable, or a declared root is not an existing absolute directory.
    pub fn new(
        writable_roots: impl IntoIterator<Item = PathBuf>,
        read_only_roots: impl IntoIterator<Item = PathBuf>,
        network: SandboxNetwork,
    ) -> Result<Self, String> {
        if !cfg!(target_os = "macos") {
            return Err("macOS Seatbelt sandbox requested on a non-macOS host".to_owned());
        }
        let launcher = Path::new(MACOS_SANDBOX_EXEC);
        if !launcher.is_file() {
            return Err(format!(
                "macOS Seatbelt launcher is unavailable: {}",
                launcher.display()
            ));
        }

        let writable_roots = canonical_directories("writable", writable_roots)?;
        if writable_roots.is_empty() {
            return Err("sandbox requires at least one writable root".to_owned());
        }
        let read_only_roots = canonical_directories("read-only", read_only_roots)?;
        let profile = seatbelt_profile(&writable_roots, &read_only_roots, network)?;
        let mut digest = Sha256::new();
        digest.update(b"fleetd-macos-seatbelt-v3\0");
        digest.update(profile.as_bytes());
        Ok(Self {
            posture: MacOsSeatbeltPosture::Strict,
            profile,
            profile_digest: format!("sha256:{:x}", digest.finalize()),
        })
    }

    /// Builds the explicit write-confinement posture used by harnesses that
    /// require ambient dependency reads and private loopback listeners.
    ///
    /// This posture is intentionally not hermetic: reads and network access
    /// are unrestricted. It starts from `allow default`, denies every file
    /// write, then restores writes only for `/dev/null` and the declared
    /// canonical roots.
    ///
    /// # Errors
    ///
    /// Returns a bounded diagnostic when this is not macOS, Seatbelt is
    /// unavailable, or a declared writable root is invalid.
    pub fn write_scoped(writable_roots: impl IntoIterator<Item = PathBuf>) -> Result<Self, String> {
        if !cfg!(target_os = "macos") {
            return Err("macOS Seatbelt sandbox requested on a non-macOS host".to_owned());
        }
        let launcher = Path::new(MACOS_SANDBOX_EXEC);
        if !launcher.is_file() {
            return Err(format!(
                "macOS Seatbelt launcher is unavailable: {}",
                launcher.display()
            ));
        }

        let writable_roots = canonical_directories("writable", writable_roots)?;
        if writable_roots.is_empty() {
            return Err("sandbox requires at least one writable root".to_owned());
        }
        let profile = write_scoped_profile(&writable_roots)?;
        let mut digest = Sha256::new();
        digest.update(b"fleetd-macos-seatbelt-write-scoped-v1\0");
        digest.update(profile.as_bytes());
        Ok(Self {
            posture: MacOsSeatbeltPosture::WriteScoped,
            profile,
            profile_digest: format!("sha256:{:x}", digest.finalize()),
        })
    }

    /// Stable desired-state and evidence name of the effective posture.
    #[must_use]
    pub const fn posture(&self) -> MacOsSeatbeltPosture {
        self.posture
    }

    /// Honest boundary claim attached to operator evidence.
    #[must_use]
    pub const fn security_scope(&self) -> &'static str {
        match self.posture {
            MacOsSeatbeltPosture::Strict => "deny_default_declared_reach",
            MacOsSeatbeltPosture::WriteScoped => "writes_scoped_reads_and_network_unrestricted",
        }
    }

    /// Content identity of the effective Seatbelt policy.
    #[must_use]
    pub fn profile_digest(&self) -> &str {
        &self.profile_digest
    }

    pub(crate) fn launch_command(
        &self,
        executable: &Path,
        arguments: &[OsString],
    ) -> (PathBuf, Vec<OsString>) {
        let mut wrapped = vec![
            OsString::from("-p"),
            OsString::from(&self.profile),
            executable.as_os_str().to_owned(),
        ];
        wrapped.extend(arguments.iter().cloned());
        (PathBuf::from(MACOS_SANDBOX_EXEC), wrapped)
    }
}

impl std::fmt::Debug for MacOsSeatbeltSandbox {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MacOsSeatbeltSandbox")
            .field("posture", &self.posture.as_str())
            .field("security_scope", &self.security_scope())
            .field("profile_digest", &self.profile_digest)
            .finish_non_exhaustive()
    }
}

fn canonical_directories(
    label: &str,
    roots: impl IntoIterator<Item = PathBuf>,
) -> Result<Vec<PathBuf>, String> {
    let mut canonical = BTreeSet::new();
    for root in roots {
        if !root.is_absolute() || !root.is_dir() {
            return Err(format!(
                "sandbox {label} root must be an existing absolute directory: {}",
                root.display()
            ));
        }
        let root = root.canonicalize().map_err(|error| {
            format!(
                "sandbox {label} root could not be canonicalized ({}): {error}",
                root.display()
            )
        })?;
        if root == Path::new("/") {
            return Err(format!(
                "sandbox {label} root must not be the filesystem root"
            ));
        }
        canonical.insert(root);
    }
    Ok(canonical.into_iter().collect())
}

fn seatbelt_profile(
    writable_roots: &[PathBuf],
    read_only_roots: &[PathBuf],
    network: SandboxNetwork,
) -> Result<String, String> {
    let mut profile = String::from(
        "(version 1)\n\
         (deny default)\n\
         (allow process*)\n\
         (allow signal (target same-sandbox))\n\
         (allow sysctl-read)\n\
         (allow mach-lookup)\n\
         (allow ipc-posix*)\n\
         (allow file-read-metadata)\n\
         (allow file-write* (literal \"/dev/null\"))\n\
         (allow file-read*\n\
           (literal \"/\")\n\
           (subpath \"/System\")\n\
           (subpath \"/Library\")\n\
           (subpath \"/usr\")\n\
           (subpath \"/bin\")\n\
           (subpath \"/sbin\")\n\
           (subpath \"/dev\")\n\
           (subpath \"/private/etc\"))\n",
    );
    // Runtime loaders and package resolvers inspect each ancestor directory
    // while resolving a declared deep root. Grant only the directory inode at
    // each level, never its descendants, so traversal does not become sibling
    // repository access.
    let mut ancestors = BTreeSet::new();
    for root in writable_roots.iter().chain(read_only_roots) {
        for ancestor in root.ancestors().skip(1) {
            if ancestor != Path::new("/") {
                ancestors.insert(ancestor.to_path_buf());
            }
        }
    }
    for ancestor in ancestors {
        profile.push_str("(allow file-read* (literal ");
        profile.push_str(&seatbelt_string(&ancestor)?);
        profile.push_str("))\n");
    }
    for root in writable_roots.iter().chain(read_only_roots) {
        profile.push_str("(allow file-read* (subpath ");
        profile.push_str(&seatbelt_string(root)?);
        profile.push_str("))\n");
    }
    for root in writable_roots {
        profile.push_str("(allow file-write* (subpath ");
        profile.push_str(&seatbelt_string(root)?);
        profile.push_str("))\n");
    }
    if network == SandboxNetwork::AllowOutbound {
        profile.push_str("(allow network-outbound)\n");
    }
    Ok(profile)
}

fn write_scoped_profile(writable_roots: &[PathBuf]) -> Result<String, String> {
    let mut profile = String::from(
        "(version 1)\n\
         (allow default)\n\
         (deny file-write*)\n\
         (allow file-write* (literal \"/dev/null\"))\n",
    );
    for root in writable_roots {
        profile.push_str("(allow file-write* (subpath ");
        profile.push_str(&seatbelt_string(root)?);
        profile.push_str("))\n");
    }
    Ok(profile)
}

fn seatbelt_string(path: &Path) -> Result<String, String> {
    let value = path
        .to_str()
        .ok_or_else(|| format!("sandbox path is not valid UTF-8: {}", path.display()))?;
    serde_json::to_string(value).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        MacOsSeatbeltPosture, MacOsSeatbeltSandbox, SandboxNetwork, seatbelt_profile,
        write_scoped_profile,
    };

    #[test]
    fn root_scope_is_refused() {
        if cfg!(target_os = "macos") {
            let error = MacOsSeatbeltSandbox::new(
                [std::path::PathBuf::from("/")],
                [],
                SandboxNetwork::Deny,
            )
            .expect_err("the filesystem root is not a bounded seat");
            assert!(error.contains("filesystem root"));
        }
    }

    #[test]
    fn deep_roots_grant_literal_ancestor_traversal_without_sibling_subpaths() {
        let profile = seatbelt_profile(
            &[PathBuf::from("/Users/operator/repos/seat")],
            &[PathBuf::from("/Users/operator/runtime/adapter")],
            SandboxNetwork::Deny,
        )
        .expect("build profile");

        assert!(profile.contains("(allow file-read* (literal \"/Users/operator/repos\"))"));
        assert!(profile.contains("(allow file-read* (literal \"/Users/operator\"))"));
        assert!(!profile.contains("(subpath \"/Users/operator/repos\")"));
        assert!(!profile.contains("network-outbound"));
        assert!(profile.contains("(allow file-write* (literal \"/dev/null\"))"));
    }

    #[test]
    fn write_scoped_is_allow_default_write_only_and_content_addressed() {
        let profile = write_scoped_profile(&[
            PathBuf::from("/Users/operator/seat"),
            PathBuf::from("/private/tmp/seat"),
        ])
        .expect("build write-scoped profile");

        assert!(profile.contains("(allow default)"));
        assert!(profile.contains("(deny file-write*)"));
        assert!(profile.contains("(allow file-write* (literal \"/dev/null\"))"));
        assert!(profile.contains("(subpath \"/Users/operator/seat\")"));
        assert!(profile.contains("(subpath \"/private/tmp/seat\")"));
        assert!(!profile.contains("deny default"));
        assert!(!profile.contains("network-outbound"));
        assert!(!profile.contains("file-read"));

        if cfg!(target_os = "macos") {
            let sandbox = MacOsSeatbeltSandbox::write_scoped([std::env::temp_dir()])
                .expect("build write-scoped sandbox");
            assert_eq!(sandbox.posture(), MacOsSeatbeltPosture::WriteScoped);
            assert_eq!(
                sandbox.security_scope(),
                "writes_scoped_reads_and_network_unrestricted"
            );
            assert!(sandbox.profile_digest().starts_with("sha256:"));
            assert!(format!("{sandbox:?}").contains("write_scoped"));
        }
    }
}
