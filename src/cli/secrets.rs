//! Writing credential files nobody else can read.

use std::{error::Error, fs, io::Write, path::Path};

use fleetd::model::{IssuedCredential, RegisteredAgent};
use serde_json::json;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::print_json;

pub(super) fn print_registration(
    registration: &RegisteredAgent,
    credential_file: Option<&Path>,
) -> MainResult<()> {
    if let Some(path) = credential_file {
        persist_secret_file(path, &registration.credential.token)?;
        return print_json(&json!({
            "agent": registration.agent,
            "credential": {
                "id": registration.credential.id,
                "created_at_ms": registration.credential.created_at_ms,
                "token_file": path.display().to_string()
            }
        }));
    }
    print_json(&registration)
}

pub(super) fn print_credential(
    credential: &IssuedCredential,
    credential_file: Option<&Path>,
) -> MainResult<()> {
    if let Some(path) = credential_file {
        replace_secret_file(path, &credential.token)?;
        return print_json(&json!({
            "id": credential.id,
            "created_at_ms": credential.created_at_ms,
            "token_file": path.display().to_string()
        }));
    }
    print_json(&credential)
}

pub(super) fn persist_secret_file(path: &Path, token: &str) -> MainResult<()> {
    persist_secret_file_with_mode(path, token, false)
}

pub(super) fn replace_secret_file(path: &Path, token: &str) -> MainResult<()> {
    persist_secret_file_with_mode(path, token, true)
}

pub(super) fn persist_secret_file_with_mode(
    path: &Path,
    token: &str,
    replace: bool,
) -> MainResult<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    make_secret_file_private(temporary.path())?;
    writeln!(temporary, "{token}")?;
    temporary.as_file().sync_all()?;
    if replace {
        temporary.persist(path).map_err(|error| {
            format!(
                "could not replace credential file {}: {}",
                path.display(),
                error.error
            )
        })?;
    } else {
        temporary.persist_noclobber(path).map_err(|error| {
            format!(
                "could not persist credential file {}: {}",
                path.display(),
                error.error
            )
        })?;
    }
    Ok(())
}

#[cfg(unix)]
pub(super) fn make_secret_file_private(path: &Path) -> MainResult<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn make_secret_file_private(_path: &Path) -> MainResult<()> {
    Err("secure credential files are not implemented on this platform".into())
}

#[cfg(test)]
mod tests {
    use super::{persist_secret_file, replace_secret_file};

    #[test]
    #[cfg(unix)]
    fn credential_files_are_private_and_never_overwritten() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("agent.token");
        persist_secret_file(&path, "first").expect("persist token");
        let mode = std::fs::metadata(&path)
            .expect("token metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0);
        assert!(persist_secret_file(&path, "second").is_err());
        assert_eq!(
            std::fs::read_to_string(path).expect("read token"),
            "first\n"
        );
    }

    #[test]
    #[cfg(unix)]
    fn credential_rotation_atomically_replaces_the_private_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("agent.token");
        persist_secret_file(&path, "first").expect("persist initial token");
        replace_secret_file(&path, "second").expect("replace token");
        let metadata = std::fs::metadata(&path).expect("token metadata");
        assert_eq!(metadata.permissions().mode() & 0o077, 0);
        assert_eq!(
            std::fs::read_to_string(path).expect("read token"),
            "second\n"
        );
    }
}
