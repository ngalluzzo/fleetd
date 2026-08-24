//! Shared exact-Git boundary for repository capability adapters.

use std::{
    ffi::{OsStr, OsString},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
    thread,
};

use thiserror::Error;

pub(crate) const MAX_GIT_OUTPUT_BYTES: usize = 2 * 1_024 * 1_024;
const MAX_GIT_DIAGNOSTIC_BYTES: usize = 4_096;

pub(crate) fn validate_clean_checkout(
    repository_root: &Path,
    git_executable: &Path,
    revision: &str,
) -> Result<(), RepositoryGitError> {
    validate_git_inputs(repository_root, git_executable)?;
    validate_repository_root(repository_root, git_executable)?;
    let observed_revision = git_text(
        repository_root,
        git_executable,
        ["rev-parse", "--verify", "HEAD^{commit}"],
    )?;
    if observed_revision.trim() != revision {
        return Err(RepositoryGitError::RevisionMismatch {
            expected: revision.to_owned(),
            actual: observed_revision.trim().to_owned(),
        });
    }
    let status = git_text(
        repository_root,
        git_executable,
        ["status", "--porcelain=v1", "--untracked-files=all"],
    )?;
    if !status.trim().is_empty() {
        return Err(RepositoryGitError::DirtyCheckout);
    }
    Ok(())
}

pub(crate) fn validate_git_inputs(
    repository_root: &Path,
    git_executable: &Path,
) -> Result<(), RepositoryGitError> {
    if !repository_root.is_absolute() || !repository_root.is_dir() {
        return Err(RepositoryGitError::InvalidRepositoryRoot(
            repository_root.to_path_buf(),
        ));
    }
    if !git_executable.is_absolute() || !git_executable.is_file() {
        return Err(RepositoryGitError::InvalidGitExecutable(
            git_executable.to_path_buf(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_repository_root(
    repository_root: &Path,
    git_executable: &Path,
) -> Result<(), RepositoryGitError> {
    let top_level = git_text(
        repository_root,
        git_executable,
        ["rev-parse", "--show-toplevel"],
    )?;
    let configured_root = std::fs::canonicalize(repository_root)?;
    let observed_root = std::fs::canonicalize(top_level.trim())?;
    if observed_root != configured_root {
        return Err(RepositoryGitError::RepositoryRootMismatch);
    }
    Ok(())
}

pub(crate) fn validate_revision(revision: &str) -> Result<(), RepositoryGitError> {
    if !matches!(revision.len(), 40 | 64)
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RepositoryGitError::InvalidRevision(revision.to_owned()));
    }
    Ok(())
}

pub(crate) fn validate_relative_path(
    value: &str,
    allow_dot: bool,
) -> Result<(), RepositoryGitError> {
    if allow_dot && value == "." {
        return Ok(());
    }
    let path = Path::new(value);
    let normalized = path.components().collect::<PathBuf>();
    if value.is_empty()
        || value.contains('\\')
        || value.chars().any(char::is_control)
        || path.is_absolute()
        || normalized.as_os_str() != OsStr::new(value)
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(RepositoryGitError::InvalidPath(value.to_owned()));
    }
    Ok(())
}

pub(crate) fn path_is_in_scope(path: &str, scope: &str) -> bool {
    scope == "."
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|remainder| remainder.starts_with('/'))
}

pub(crate) fn git_text<I, S>(
    repository_root: &Path,
    git_executable: &Path,
    arguments: I,
) -> Result<String, RepositoryGitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let output = git_output(repository_root, git_executable, arguments)?;
    String::from_utf8(output).map_err(|_| RepositoryGitError::NonUtf8Output)
}

pub(crate) fn git_output<I, S>(
    repository_root: &Path,
    git_executable: &Path,
    arguments: I,
) -> Result<Vec<u8>, RepositoryGitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    git_output_with_input(
        repository_root,
        git_executable,
        arguments,
        None,
        &[],
        MAX_GIT_OUTPUT_BYTES,
    )
}

pub(crate) fn git_output_with_input<I, S>(
    repository_root: &Path,
    git_executable: &Path,
    arguments: I,
    input: Option<&[u8]>,
    environment: &[(&str, &OsStr)],
    output_limit: usize,
) -> Result<Vec<u8>, RepositoryGitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let arguments = arguments
        .into_iter()
        .map(|argument| argument.as_ref().to_os_string())
        .collect::<Vec<OsString>>();
    let mut command = Command::new(git_executable);
    command
        .env_clear()
        .env("LC_ALL", "C")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .arg("--no-optional-locks")
        .arg("-C")
        .arg(repository_root)
        .args(arguments)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, value) in environment {
        command.env(key, value);
    }
    if input.is_some() {
        command.stdin(Stdio::piped());
    } else {
        command.stdin(Stdio::null());
    }
    let mut child = command.spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or(RepositoryGitError::MissingOutputPipe)?;
    let stderr = child
        .stderr
        .take()
        .ok_or(RepositoryGitError::MissingOutputPipe)?;
    let stdout_reader = thread::spawn(move || read_bounded(stdout, output_limit));
    let stderr_reader = thread::spawn(move || read_bounded(stderr, output_limit));
    let write_result = if let Some(input) = input {
        child
            .stdin
            .take()
            .ok_or(RepositoryGitError::MissingStdin)?
            .write_all(input)
    } else {
        Ok(())
    };
    let status = child.wait()?;
    let stdout = stdout_reader
        .join()
        .map_err(|_| RepositoryGitError::OutputReaderPanicked)??;
    let stderr = stderr_reader
        .join()
        .map_err(|_| RepositoryGitError::OutputReaderPanicked)??;
    if let Err(error) = write_result
        && error.kind() != std::io::ErrorKind::BrokenPipe
    {
        return Err(error.into());
    }
    if !status.success() {
        let mut diagnostic = String::from_utf8_lossy(&stderr).trim().to_owned();
        if diagnostic.len() > MAX_GIT_DIAGNOSTIC_BYTES {
            let mut end = MAX_GIT_DIAGNOSTIC_BYTES;
            while !diagnostic.is_char_boundary(end) {
                end -= 1;
            }
            diagnostic.truncate(end);
        }
        return Err(RepositoryGitError::Failed {
            status: status.code(),
            diagnostic,
        });
    }
    Ok(stdout)
}

fn read_bounded(reader: impl Read, output_limit: usize) -> Result<Vec<u8>, RepositoryGitError> {
    let limit = u64::try_from(output_limit)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut output = Vec::with_capacity(output_limit.min(64 * 1_024));
    reader.take(limit).read_to_end(&mut output)?;
    if output.len() > output_limit {
        return Err(RepositoryGitError::OutputTooLarge);
    }
    Ok(output)
}

#[derive(Debug, Error)]
pub enum RepositoryGitError {
    #[error("invalid absolute Git executable: {0}")]
    InvalidGitExecutable(PathBuf),
    #[error("invalid absolute repository root: {0}")]
    InvalidRepositoryRoot(PathBuf),
    #[error("configured repository root does not equal Git's top-level directory")]
    RepositoryRootMismatch,
    #[error("repository checkout is dirty")]
    DirtyCheckout,
    #[error("repository revision mismatch: expected {expected}, observed {actual}")]
    RevisionMismatch { expected: String, actual: String },
    #[error("invalid Git revision {0}")]
    InvalidRevision(String),
    #[error("invalid repository-relative path {0}")]
    InvalidPath(String),
    #[error("Git output is not UTF-8")]
    NonUtf8Output,
    #[error("Git output exceeded the configured bound")]
    OutputTooLarge,
    #[error("Git child stdin was unavailable")]
    MissingStdin,
    #[error("Git child output pipe was unavailable")]
    MissingOutputPipe,
    #[error("Git output reader panicked")]
    OutputReaderPanicked,
    #[error("Git failed with status {status:?}: {diagnostic}")]
    Failed {
        status: Option<i32>,
        diagnostic: String,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
}
