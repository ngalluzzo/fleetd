//! Exact repository-patch proposal capability built on capability-work.

use std::{
    collections::BTreeSet,
    ffi::{OsStr, OsString},
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    CapabilityCandidate, CapabilityProviderDescriptor, CapabilityWorkBody, CapabilityWorkRequest,
    CapabilityWorkTurnAdapter, ExactIdentity, FactAcceptance, FactCoverage, FactRequirement,
    InboundAcceptance, Invocation, PreparedTurn, RepositoryGitError, TurnAdapter,
    WorkContractError,
    repository_git::{
        MAX_GIT_OUTPUT_BYTES, git_output_with_input, path_is_in_scope, validate_clean_checkout,
        validate_git_inputs, validate_relative_path, validate_revision,
    },
    work_contract::{BoundFact, canonical_digest},
};

pub const REPOSITORY_PATCH_SUITE: &str = "dev.fleetd.conformance/repository_patch@0.1.0";

const MAX_REPOSITORY_ID_BYTES: usize = 256;
const MAX_PATH_SCOPES: usize = 32;
const MAX_OBJECTIVE_BYTES: usize = 8_192;
const MAX_ACCEPTANCE_CRITERIA: usize = 32;
const MAX_CRITERION_BYTES: usize = 4_096;
const MAX_CONSTRAINTS: usize = 32;
const MAX_SUMMARY_BYTES: usize = 8_192;
const MAX_CHANGED_PATHS: usize = 64;
const MAX_PATCH_BYTES: usize = 256 * 1_024;
const MAX_LIMITATIONS: usize = 32;

#[must_use]
pub fn repository_patch_capability() -> ExactIdentity {
    ExactIdentity::new("dev.fleetd.capability", "propose_repository_patch", "0.1.0")
}

#[must_use]
pub fn repository_change_brief_fact() -> ExactIdentity {
    ExactIdentity::new("dev.fleetd.fact", "repository_change_brief", "0.1.0")
}

#[must_use]
pub fn repository_patch_artifact_fact() -> ExactIdentity {
    ExactIdentity::new("dev.fleetd.artifact", "repository_patch", "0.1.0")
}

/// Provider-neutral specification for one proposed change against an exact
/// clean Git revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryChangeBrief {
    pub schema_version: u32,
    pub repository_id: String,
    pub base_revision: String,
    pub path_scope: Vec<String>,
    pub objective: String,
    pub acceptance_criteria: Vec<String>,
    #[serde(default)]
    pub constraints: Vec<String>,
}

/// Exact artifact claim returned by the semantic provider. The patch remains
/// untrusted until Git-backed conformance succeeds.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryPatchProposal {
    pub schema_version: u32,
    pub request_id: String,
    pub repository_id: String,
    pub base_revision: String,
    pub summary: String,
    pub changed_paths: Vec<String>,
    pub patch: String,
    #[serde(default)]
    pub limitations: Vec<String>,
}

/// Git-canonicalized patch produced by deterministic conformance. This proves
/// applicability, scope, and artifact identity, not satisfaction of the
/// brief's natural-language acceptance criteria.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConformedRepositoryPatch {
    pub schema_version: u32,
    pub request_id: String,
    pub repository_id: String,
    pub base_revision: String,
    pub candidate_id: String,
    pub patch_digest: String,
    pub changed_paths: Vec<String>,
    pub canonical_patch: String,
    pub summary: String,
    pub limitations: Vec<String>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RepositoryChangeDerivation {
    kind: String,
    repository_id: String,
    base_revision: String,
}

/// Strict semantic adapter for patch proposals through one configured Git
/// executable and clean checkout.
#[derive(Clone, Debug)]
pub struct RepositoryPatchTurnAdapter {
    delegate: CapabilityWorkTurnAdapter,
    repository_root: PathBuf,
    git_executable: PathBuf,
}

impl RepositoryPatchTurnAdapter {
    /// Creates an exact repository-patch provider.
    ///
    /// # Errors
    ///
    /// Returns an error for a provider mismatch or invalid Git inputs.
    pub fn new(
        provider: CapabilityProviderDescriptor,
        repository_root: PathBuf,
        git_executable: PathBuf,
    ) -> Result<Self, RepositoryPatchError> {
        if provider.capability != repository_patch_capability() {
            return Err(RepositoryPatchError::ProviderCapabilityMismatch);
        }
        validate_git_inputs(&repository_root, &git_executable)?;
        let repository_root = fs::canonicalize(repository_root)?;
        let delegate =
            CapabilityWorkTurnAdapter::new([provider]).map_err(RepositoryPatchError::Adapter)?;
        Ok(Self {
            delegate,
            repository_root,
            git_executable,
        })
    }
}

impl TurnAdapter for RepositoryPatchTurnAdapter {
    fn inbound_acceptance(&self) -> &InboundAcceptance {
        self.delegate.inbound_acceptance()
    }

    fn prepare(&self, invocation: &Invocation) -> Result<PreparedTurn, String> {
        let mut prepared = self.delegate.prepare(invocation)?;
        let request: CapabilityWorkRequest =
            serde_json::from_value(invocation.message.payload.clone())
                .map_err(|error| format!("repository patch request is malformed: {error}"))?;
        let brief = repository_change_brief(&request).map_err(|error| error.to_string())?;
        validate_clean_checkout(
            &self.repository_root,
            &self.git_executable,
            &brief.base_revision,
        )
        .map_err(|error| error.to_string())?;
        let proposal_shape = json!({
            "schema_version": 1,
            "request_id": request.request_id,
            "repository_id": brief.repository_id,
            "base_revision": brief.base_revision,
            "summary": "bounded summary of the proposed change",
            "changed_paths": ["sorted repository-relative path"],
            "patch": "complete text unified diff against base_revision",
            "limitations": []
        });
        prepared.prompt.push(crate::PromptBlock::Text {
            text: format!(
                "Propose a patch only; do not modify the checkout, create files, commit, or push. The patch must apply to exact base revision `{base_revision}`, change only paths admitted by the request, contain only regular text files, and be no larger than {MAX_PATCH_BYTES} bytes. Return the full unified diff in the patch field and the exact sorted changed path set. Git will independently apply the patch to an isolated temporary index and canonicalize it. The payload of the single requested output fact must match this shape exactly: {proposal_shape}. The surrounding capability response schema from the previous instruction remains mandatory.",
                base_revision = brief.base_revision,
            ),
        });
        Ok(prepared)
    }
}

/// Binds a validated change brief into the generic capability-work request.
///
/// # Errors
///
/// Returns an error for malformed fields or canonicalization failure.
pub fn bind_repository_patch(
    brief: RepositoryChangeBrief,
) -> Result<CapabilityWorkRequest, RepositoryPatchError> {
    validate_brief(&brief)?;
    let payload = serde_json::to_value(&brief)?;
    let derivation = RepositoryChangeDerivation {
        kind: "git_base_revision".to_owned(),
        repository_id: brief.repository_id.clone(),
        base_revision: brief.base_revision.clone(),
    };
    let input_id = canonical_digest(&json!({
        "payload": payload,
        "derivation": derivation,
    }))?;
    let fact = repository_change_brief_fact();
    Ok(CapabilityWorkRequest::bind(CapabilityWorkBody {
        capability: repository_patch_capability(),
        requires: vec![FactRequirement {
            fact: fact.clone(),
            acceptance: FactAcceptance::CompleteOnly,
        }],
        inputs: vec![BoundFact {
            id: input_id,
            fact_type: fact,
            coverage: FactCoverage::Complete,
            payload: serde_json::to_value(&brief)?,
            derivation: serde_json::to_value(RepositoryChangeDerivation {
                kind: "git_base_revision".to_owned(),
                repository_id: brief.repository_id,
                base_revision: brief.base_revision,
            })?,
        }],
        produces: vec![repository_patch_artifact_fact()],
        conformance_suite: REPOSITORY_PATCH_SUITE.to_owned(),
    })?)
}

/// Recovers and validates the exact change brief from a generic request.
///
/// # Errors
///
/// Returns an error for any request, fact, derivation, or identity mismatch.
pub fn repository_change_brief(
    request: &CapabilityWorkRequest,
) -> Result<RepositoryChangeBrief, RepositoryPatchError> {
    request.validate()?;
    if request.body.capability != repository_patch_capability()
        || request.body.conformance_suite != REPOSITORY_PATCH_SUITE
        || request.body.requires
            != vec![FactRequirement {
                fact: repository_change_brief_fact(),
                acceptance: FactAcceptance::CompleteOnly,
            }]
        || request.body.produces != vec![repository_patch_artifact_fact()]
    {
        return Err(RepositoryPatchError::RequestShapeMismatch);
    }
    let [input] = request.body.inputs.as_slice() else {
        return Err(RepositoryPatchError::RequestShapeMismatch);
    };
    if input.fact_type != repository_change_brief_fact() || input.coverage != FactCoverage::Complete
    {
        return Err(RepositoryPatchError::RequestShapeMismatch);
    }
    let brief: RepositoryChangeBrief = serde_json::from_value(input.payload.clone())?;
    validate_brief(&brief)?;
    let expected_derivation = RepositoryChangeDerivation {
        kind: "git_base_revision".to_owned(),
        repository_id: brief.repository_id.clone(),
        base_revision: brief.base_revision.clone(),
    };
    if input.derivation != serde_json::to_value(&expected_derivation)? {
        return Err(RepositoryPatchError::RequestShapeMismatch);
    }
    let expected_id = canonical_digest(&json!({
        "payload": brief,
        "derivation": expected_derivation,
    }))?;
    if input.id != expected_id {
        return Err(RepositoryPatchError::InputIdentityMismatch);
    }
    serde_json::from_value(input.payload.clone()).map_err(Into::into)
}

/// Applies a proposed patch to an isolated temporary Git index, validates the
/// exact path and file-mode boundary, and emits a canonical patch artifact.
/// The configured worktree is never modified.
///
/// # Errors
///
/// Returns an error for request, candidate, proposal, Git, scope, mode, binary,
/// or canonical artifact mismatch.
pub fn conform_repository_patch(
    request: &CapabilityWorkRequest,
    candidate: &CapabilityCandidate,
    repository_root: &Path,
    git_executable: &Path,
) -> Result<ConformedRepositoryPatch, RepositoryPatchError> {
    candidate.validate(request)?;
    let brief = repository_change_brief(request)?;
    validate_clean_checkout(repository_root, git_executable, &brief.base_revision)?;
    let [output] = candidate.body.outputs.as_slice() else {
        return Err(RepositoryPatchError::ProposalShapeMismatch);
    };
    if output.fact_type != repository_patch_artifact_fact()
        || output.coverage != FactCoverage::Complete
    {
        return Err(RepositoryPatchError::ProposalShapeMismatch);
    }
    let proposal: RepositoryPatchProposal = serde_json::from_value(output.payload.clone())?;
    validate_proposal(&brief, request, &proposal)?;

    let temporary = tempfile::tempdir()?;
    let index_path = temporary.path().join("index");
    let environment = [("GIT_INDEX_FILE", index_path.as_os_str())];
    git_output_with_input(
        repository_root,
        git_executable,
        ["read-tree", brief.base_revision.as_str()],
        None,
        &environment,
        MAX_GIT_OUTPUT_BYTES,
    )?;
    let apply_arguments = ["apply", "--cached", "--whitespace=error-all"];
    let mut check_arguments = apply_arguments.to_vec();
    check_arguments.push("--check");
    git_output_with_input(
        repository_root,
        git_executable,
        check_arguments,
        Some(proposal.patch.as_bytes()),
        &environment,
        MAX_GIT_OUTPUT_BYTES,
    )?;
    git_output_with_input(
        repository_root,
        git_executable,
        apply_arguments,
        Some(proposal.patch.as_bytes()),
        &environment,
        MAX_GIT_OUTPUT_BYTES,
    )?;

    let changed_paths = changed_paths(repository_root, git_executable, &brief, &environment)?;
    if proposal.changed_paths != changed_paths {
        return Err(RepositoryPatchError::ChangedPathSetMismatch);
    }
    reject_binary_changes(
        repository_root,
        git_executable,
        &brief.base_revision,
        &environment,
    )?;
    validate_regular_modes(
        repository_root,
        git_executable,
        &brief.base_revision,
        &changed_paths,
        &environment,
    )?;

    let canonical = git_output_with_input(
        repository_root,
        git_executable,
        [
            "diff",
            "--cached",
            "--binary",
            "--full-index",
            "--no-ext-diff",
            "--no-renames",
            brief.base_revision.as_str(),
            "--",
        ],
        None,
        &environment,
        MAX_PATCH_BYTES,
    )?;
    if canonical.is_empty() {
        return Err(RepositoryPatchError::EmptyPatch);
    }
    let canonical_patch =
        String::from_utf8(canonical.clone()).map_err(|_| RepositoryPatchError::NonUtf8Patch)?;
    Ok(ConformedRepositoryPatch {
        schema_version: 1,
        request_id: request.request_id.clone(),
        repository_id: brief.repository_id,
        base_revision: brief.base_revision,
        candidate_id: candidate.candidate_id.clone(),
        patch_digest: digest_bytes(&canonical),
        changed_paths,
        canonical_patch,
        summary: proposal.summary,
        limitations: proposal.limitations,
    })
}

fn validate_brief(brief: &RepositoryChangeBrief) -> Result<(), RepositoryPatchError> {
    if brief.schema_version != 1 {
        return Err(RepositoryPatchError::UnsupportedSchema(
            brief.schema_version,
        ));
    }
    validate_text(
        "repository_id",
        &brief.repository_id,
        MAX_REPOSITORY_ID_BYTES,
    )?;
    validate_revision(&brief.base_revision)?;
    validate_scopes(&brief.path_scope)?;
    validate_text("objective", &brief.objective, MAX_OBJECTIVE_BYTES)?;
    validate_string_set(
        "acceptance_criteria",
        &brief.acceptance_criteria,
        1,
        MAX_ACCEPTANCE_CRITERIA,
        MAX_CRITERION_BYTES,
    )?;
    validate_string_set(
        "constraints",
        &brief.constraints,
        0,
        MAX_CONSTRAINTS,
        MAX_CRITERION_BYTES,
    )?;
    Ok(())
}

fn validate_proposal(
    brief: &RepositoryChangeBrief,
    request: &CapabilityWorkRequest,
    proposal: &RepositoryPatchProposal,
) -> Result<(), RepositoryPatchError> {
    if proposal.schema_version != 1 {
        return Err(RepositoryPatchError::UnsupportedSchema(
            proposal.schema_version,
        ));
    }
    if proposal.request_id != request.request_id
        || proposal.repository_id != brief.repository_id
        || proposal.base_revision != brief.base_revision
    {
        return Err(RepositoryPatchError::ProposalBindingMismatch);
    }
    validate_text("summary", &proposal.summary, MAX_SUMMARY_BYTES)?;
    if proposal.patch.trim().is_empty() {
        return Err(RepositoryPatchError::EmptyPatch);
    }
    if proposal.patch.len() > MAX_PATCH_BYTES {
        return Err(RepositoryPatchError::PatchTooLarge);
    }
    if proposal.patch.contains('\0') {
        return Err(RepositoryPatchError::NonUtf8Patch);
    }
    if proposal.changed_paths.is_empty() || proposal.changed_paths.len() > MAX_CHANGED_PATHS {
        return Err(RepositoryPatchError::InvalidCount("changed_paths"));
    }
    let mut previous = None;
    for path in &proposal.changed_paths {
        validate_relative_path(path, false)?;
        if !brief
            .path_scope
            .iter()
            .any(|scope| path_is_in_scope(path, scope))
        {
            return Err(RepositoryPatchError::PathOutsideScope(path.clone()));
        }
        if previous.is_some_and(|previous: &String| previous >= path) {
            return Err(RepositoryPatchError::ChangedPathsNotSorted);
        }
        previous = Some(path);
    }
    validate_string_set(
        "limitations",
        &proposal.limitations,
        0,
        MAX_LIMITATIONS,
        MAX_CRITERION_BYTES,
    )?;
    Ok(())
}

fn validate_scopes(scopes: &[String]) -> Result<(), RepositoryPatchError> {
    if scopes.is_empty() || scopes.len() > MAX_PATH_SCOPES {
        return Err(RepositoryPatchError::InvalidCount("path_scope"));
    }
    let mut unique = BTreeSet::new();
    for scope in scopes {
        validate_relative_path(scope, true)?;
        if !unique.insert(scope) {
            return Err(RepositoryPatchError::Duplicate("path_scope"));
        }
    }
    Ok(())
}

fn validate_string_set(
    field: &'static str,
    values: &[String],
    minimum: usize,
    maximum: usize,
    byte_limit: usize,
) -> Result<(), RepositoryPatchError> {
    if values.len() < minimum || values.len() > maximum {
        return Err(RepositoryPatchError::InvalidCount(field));
    }
    let mut unique = BTreeSet::new();
    for value in values {
        validate_text(field, value, byte_limit)?;
        if !unique.insert(value) {
            return Err(RepositoryPatchError::Duplicate(field));
        }
    }
    Ok(())
}

fn validate_text(
    field: &'static str,
    value: &str,
    limit: usize,
) -> Result<(), RepositoryPatchError> {
    if value.trim().is_empty() || value.len() > limit {
        return Err(RepositoryPatchError::InvalidText { field, limit });
    }
    Ok(())
}

fn changed_paths(
    repository_root: &Path,
    git_executable: &Path,
    brief: &RepositoryChangeBrief,
    environment: &[(&str, &OsStr)],
) -> Result<Vec<String>, RepositoryPatchError> {
    let output = git_output_with_input(
        repository_root,
        git_executable,
        [
            "diff",
            "--cached",
            "--name-only",
            "-z",
            "--no-renames",
            brief.base_revision.as_str(),
            "--",
        ],
        None,
        environment,
        MAX_GIT_OUTPUT_BYTES,
    )?;
    let mut paths = Vec::new();
    for encoded in output
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        let path =
            String::from_utf8(encoded.to_vec()).map_err(|_| RepositoryPatchError::NonUtf8Path)?;
        validate_relative_path(&path, false)?;
        if !brief
            .path_scope
            .iter()
            .any(|scope| path_is_in_scope(&path, scope))
        {
            return Err(RepositoryPatchError::PathOutsideScope(path));
        }
        paths.push(path);
    }
    if paths.is_empty() || paths.len() > MAX_CHANGED_PATHS {
        return Err(RepositoryPatchError::InvalidCount("changed_paths"));
    }
    if paths.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(RepositoryPatchError::ChangedPathsNotSorted);
    }
    Ok(paths)
}

fn reject_binary_changes(
    repository_root: &Path,
    git_executable: &Path,
    base_revision: &str,
    environment: &[(&str, &OsStr)],
) -> Result<(), RepositoryPatchError> {
    let output = git_output_with_input(
        repository_root,
        git_executable,
        [
            "diff",
            "--cached",
            "--numstat",
            "-z",
            "--no-renames",
            base_revision,
            "--",
        ],
        None,
        environment,
        MAX_GIT_OUTPUT_BYTES,
    )?;
    for record in output
        .split(|byte| *byte == 0)
        .filter(|record| !record.is_empty())
    {
        let record = std::str::from_utf8(record).map_err(|_| RepositoryPatchError::NonUtf8Path)?;
        let mut fields = record.splitn(3, '\t');
        let added = fields.next();
        let deleted = fields.next();
        let path = fields.next();
        let (Some(added), Some(deleted), Some(path)) = (added, deleted, path) else {
            return Err(RepositoryPatchError::MalformedGitEvidence);
        };
        if added == "-" || deleted == "-" {
            return Err(RepositoryPatchError::BinaryPatch(path.to_owned()));
        }
    }
    Ok(())
}

fn validate_regular_modes(
    repository_root: &Path,
    git_executable: &Path,
    base_revision: &str,
    paths: &[String],
    environment: &[(&str, &OsStr)],
) -> Result<(), RepositoryPatchError> {
    for path in paths {
        let base_mode = git_path_mode(
            repository_root,
            git_executable,
            [
                OsString::from("ls-tree"),
                OsString::from("-z"),
                OsString::from(base_revision),
                OsString::from("--"),
                OsString::from(path),
            ],
            &[],
        )?;
        let proposed_mode = git_path_mode(
            repository_root,
            git_executable,
            [
                OsString::from("ls-files"),
                OsString::from("--stage"),
                OsString::from("-z"),
                OsString::from("--"),
                OsString::from(path),
            ],
            environment,
        )?;
        if base_mode.as_deref().is_some_and(|mode| !regular_mode(mode))
            || proposed_mode
                .as_deref()
                .is_some_and(|mode| !regular_mode(mode))
        {
            return Err(RepositoryPatchError::NonRegularPath(path.clone()));
        }
        if base_mode.is_none() && proposed_mode.is_none() {
            return Err(RepositoryPatchError::MalformedGitEvidence);
        }
    }
    Ok(())
}

fn git_path_mode<const N: usize>(
    repository_root: &Path,
    git_executable: &Path,
    arguments: [OsString; N],
    environment: &[(&str, &OsStr)],
) -> Result<Option<String>, RepositoryPatchError> {
    let output = git_output_with_input(
        repository_root,
        git_executable,
        arguments,
        None,
        environment,
        MAX_GIT_OUTPUT_BYTES,
    )?;
    if output.is_empty() {
        return Ok(None);
    }
    let first = output
        .split(|byte| *byte == b' ')
        .next()
        .ok_or(RepositoryPatchError::MalformedGitEvidence)?;
    let mode =
        std::str::from_utf8(first).map_err(|_| RepositoryPatchError::MalformedGitEvidence)?;
    Ok(Some(mode.to_owned()))
}

fn regular_mode(mode: &str) -> bool {
    matches!(mode, "100644" | "100755")
}

fn digest_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(7 + digest.len() * 2);
    output.push_str("sha256:");
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

#[derive(Debug, Error)]
pub enum RepositoryPatchError {
    #[error("repository patch schema version {0} is unsupported")]
    UnsupportedSchema(u32),
    #[error("invalid {field}; it must contain between 1 and {limit} bytes")]
    InvalidText { field: &'static str, limit: usize },
    #[error("invalid count for {0}")]
    InvalidCount(&'static str),
    #[error("duplicate {0}")]
    Duplicate(&'static str),
    #[error("repository patch request shape does not match the exact capability")]
    RequestShapeMismatch,
    #[error("repository patch input identity does not match its payload and derivation")]
    InputIdentityMismatch,
    #[error("configured provider does not implement repository patch proposals")]
    ProviderCapabilityMismatch,
    #[error("repository patch proposal shape is invalid")]
    ProposalShapeMismatch,
    #[error("repository patch proposal does not bind the exact request")]
    ProposalBindingMismatch,
    #[error("repository patch must not be empty")]
    EmptyPatch,
    #[error("repository patch exceeds {MAX_PATCH_BYTES} bytes")]
    PatchTooLarge,
    #[error("repository patch or path is not valid UTF-8")]
    NonUtf8Patch,
    #[error("repository patch contains a non-UTF-8 path")]
    NonUtf8Path,
    #[error("changed paths must be strictly sorted and unique")]
    ChangedPathsNotSorted,
    #[error("claimed changed paths do not match Git's exact changed path set")]
    ChangedPathSetMismatch,
    #[error("changed path is outside the request scope: {0}")]
    PathOutsideScope(String),
    #[error("binary patch is unsupported for path {0}")]
    BinaryPatch(String),
    #[error("patch changes a non-regular file path: {0}")]
    NonRegularPath(String),
    #[error("Git returned malformed patch evidence")]
    MalformedGitEvidence,
    #[error("repository patch adapter is invalid: {0}")]
    Adapter(String),
    #[error(transparent)]
    Git(#[from] RepositoryGitError),
    #[error(transparent)]
    WorkContract(#[from] WorkContractError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::{env, ffi::OsStr, process::Command};

    use super::*;
    use crate::{
        AttemptEvidence, CandidateFact, CapabilityCandidateBody, InvocationState, Message,
    };

    struct GitFixture {
        _directory: tempfile::TempDir,
        root: PathBuf,
        git: PathBuf,
        revision: String,
    }

    fn find_git() -> PathBuf {
        env::split_paths(&env::var_os("PATH").expect("test PATH"))
            .map(|directory| directory.join("git"))
            .find(|candidate| candidate.is_file())
            .expect("Git executable on PATH")
    }

    fn run_git(root: &Path, git: &Path, arguments: &[&OsStr]) -> String {
        let output = Command::new(git)
            .env_clear()
            .env("LC_ALL", "C")
            .arg("-C")
            .arg(root)
            .args(arguments)
            .output()
            .expect("run fixture Git");
        assert!(
            output.status.success(),
            "Git failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).expect("UTF-8 Git output")
    }

    fn git_fixture() -> GitFixture {
        let directory = tempfile::tempdir().expect("temporary repository");
        let root = directory.path().to_path_buf();
        let git = find_git();
        run_git(&root, &git, &[OsStr::new("init")]);
        run_git(
            &root,
            &git,
            &[
                OsStr::new("config"),
                OsStr::new("user.name"),
                OsStr::new("Fleetd Test"),
            ],
        );
        run_git(
            &root,
            &git,
            &[
                OsStr::new("config"),
                OsStr::new("user.email"),
                OsStr::new("fleetd-test@example.invalid"),
            ],
        );
        fs::create_dir(root.join("src")).expect("create source directory");
        fs::write(
            root.join("src/lib.rs"),
            "pub fn inspected() -> bool {\n    true\n}\n",
        )
        .expect("write source");
        run_git(&root, &git, &[OsStr::new("add"), OsStr::new("src/lib.rs")]);
        run_git(
            &root,
            &git,
            &[
                OsStr::new("commit"),
                OsStr::new("-m"),
                OsStr::new("fixture"),
            ],
        );
        let revision = run_git(
            &root,
            &git,
            &[
                OsStr::new("rev-parse"),
                OsStr::new("--verify"),
                OsStr::new("HEAD^{commit}"),
            ],
        )
        .trim()
        .to_owned();
        GitFixture {
            _directory: directory,
            root,
            git,
            revision,
        }
    }

    fn brief(revision: &str) -> RepositoryChangeBrief {
        RepositoryChangeBrief {
            schema_version: 1,
            repository_id: "dev.fleetd/fleetd".to_owned(),
            base_revision: revision.to_owned(),
            path_scope: vec!["src".to_owned()],
            objective: "Make the inspected function return false.".to_owned(),
            acceptance_criteria: vec!["The function returns false.".to_owned()],
            constraints: vec!["Change only the existing function body.".to_owned()],
        }
    }

    fn provider() -> CapabilityProviderDescriptor {
        CapabilityProviderDescriptor {
            id: ExactIdentity::new("dev.fleetd.provider", "fixture_patcher", "0.1.0"),
            capability: repository_patch_capability(),
            implementation_digest: format!("sha256:{}", "a".repeat(64)),
        }
    }

    fn invocation(request: &CapabilityWorkRequest) -> Invocation {
        Invocation {
            id: "patch-invocation".to_owned(),
            agent_id: "patcher".to_owned(),
            message: Message {
                seq: 1,
                id: "patch-message".to_owned(),
                channel_id: "patch-channel".to_owned(),
                sender_id: "requester".to_owned(),
                recipient_id: Some("patcher".to_owned()),
                kind: crate::CAPABILITY_WORK_REQUEST_KIND.to_owned(),
                payload: serde_json::to_value(request).expect("encode request"),
                correlation_id: Some(request.request_id.clone()),
                causation_id: None,
                created_at_ms: 1,
            },
            delivery_attempt: 1,
            lease_token: "lease".to_owned(),
            lease_expires_at_ms: i64::MAX,
            fence_token: "fence".to_owned(),
            state: InvocationState::Reserved,
            reserved_at_ms: 1,
            dispatch_armed_at_ms: None,
            terminal_at_ms: None,
            execution_certainty: None,
            terminal_reason: None,
            result_message_id: None,
        }
    }

    fn proposal(request: &CapabilityWorkRequest, revision: &str) -> RepositoryPatchProposal {
        RepositoryPatchProposal {
            schema_version: 1,
            request_id: request.request_id.clone(),
            repository_id: "dev.fleetd/fleetd".to_owned(),
            base_revision: revision.to_owned(),
            summary: "Change the inspected return value.".to_owned(),
            changed_paths: vec!["src/lib.rs".to_owned()],
            patch: "diff --git a/src/lib.rs b/src/lib.rs\n--- a/src/lib.rs\n+++ b/src/lib.rs\n@@ -1,3 +1,3 @@\n pub fn inspected() -> bool {\n-    true\n+    false\n }\n"
                .to_owned(),
            limitations: Vec::new(),
        }
    }

    fn candidate(
        request: &CapabilityWorkRequest,
        proposal: &RepositoryPatchProposal,
    ) -> CapabilityCandidate {
        CapabilityCandidate::bind(
            request,
            CapabilityCandidateBody {
                request_id: request.request_id.clone(),
                provider: provider(),
                outputs: vec![CandidateFact {
                    fact_type: repository_patch_artifact_fact(),
                    coverage: FactCoverage::Complete,
                    payload: serde_json::to_value(proposal).expect("encode proposal"),
                }],
                attempt: AttemptEvidence {
                    authority: "dev.fleetd.agent/fixture".to_owned(),
                    attempt_id: "attempt".to_owned(),
                    invocation_id: "invocation".to_owned(),
                    evidence_digest: format!("sha256:{}", "b".repeat(64)),
                },
            },
        )
        .expect("bind candidate")
    }

    #[test]
    fn patch_request_is_an_exact_capability_work_specialization() {
        let fixture = git_fixture();
        let exact = brief(&fixture.revision);
        let request = bind_repository_patch(exact.clone()).expect("bind request");

        request.validate().expect("generic request validates");
        assert_eq!(request.body.capability, repository_patch_capability());
        assert_eq!(
            repository_change_brief(&request).expect("recover brief"),
            exact
        );

        let mut changed = request;
        changed.body.inputs[0].payload["objective"] = json!("changed");
        assert!(repository_change_brief(&changed).is_err());
    }

    #[test]
    fn patch_adapter_requires_clean_exact_checkout_before_arm() {
        let fixture = git_fixture();
        let request = bind_repository_patch(brief(&fixture.revision)).expect("bind request");
        let adapter =
            RepositoryPatchTurnAdapter::new(provider(), fixture.root.clone(), fixture.git.clone())
                .expect("patch adapter");

        let prepared = adapter
            .prepare(&invocation(&request))
            .expect("prepare exact patch proposal");
        assert_eq!(prepared.lane_key, request.request_id);
        assert!(matches!(
            prepared.prompt.last(),
            Some(crate::PromptBlock::Text { text }) if text.contains("Propose a patch only")
        ));

        fs::write(fixture.root.join("src/lib.rs"), "dirty\n").expect("dirty source");
        assert!(adapter.prepare(&invocation(&request)).is_err());
    }

    #[test]
    fn conformance_uses_an_isolated_index_and_emits_a_canonical_patch() {
        let fixture = git_fixture();
        let request = bind_repository_patch(brief(&fixture.revision)).expect("bind request");
        let proposal = proposal(&request, &fixture.revision);
        let candidate = candidate(&request, &proposal);

        let conformed = conform_repository_patch(&request, &candidate, &fixture.root, &fixture.git)
            .expect("conform patch");

        assert_eq!(conformed.changed_paths, vec!["src/lib.rs"]);
        assert!(conformed.canonical_patch.contains("+    false"));
        assert!(conformed.patch_digest.starts_with("sha256:"));
        assert_eq!(conformed.candidate_id, candidate.candidate_id);
        assert!(
            run_git(
                &fixture.root,
                &fixture.git,
                &[
                    OsStr::new("status"),
                    OsStr::new("--porcelain=v1"),
                    OsStr::new("--untracked-files=all")
                ]
            )
            .is_empty(),
            "conformance must not modify the checkout"
        );
    }

    #[test]
    fn conformance_rejects_scope_claim_and_patch_mismatches() {
        let fixture = git_fixture();
        let request = bind_repository_patch(brief(&fixture.revision)).expect("bind request");

        let mut outside = proposal(&request, &fixture.revision);
        outside.changed_paths = vec!["README.md".to_owned()];
        assert!(matches!(
            conform_repository_patch(
                &request,
                &candidate(&request, &outside),
                &fixture.root,
                &fixture.git
            ),
            Err(RepositoryPatchError::PathOutsideScope(_))
        ));

        let mut mismatch = proposal(&request, &fixture.revision);
        mismatch.changed_paths = vec!["src/other.rs".to_owned()];
        assert!(matches!(
            conform_repository_patch(
                &request,
                &candidate(&request, &mismatch),
                &fixture.root,
                &fixture.git
            ),
            Err(RepositoryPatchError::ChangedPathSetMismatch)
        ));

        let mut malformed = proposal(&request, &fixture.revision);
        malformed.patch = "not a patch\n".to_owned();
        assert!(matches!(
            conform_repository_patch(
                &request,
                &candidate(&request, &malformed),
                &fixture.root,
                &fixture.git
            ),
            Err(RepositoryPatchError::Git(RepositoryGitError::Failed { .. }))
        ));
    }

    #[test]
    fn conformance_rejects_non_regular_file_modes() {
        let fixture = git_fixture();
        let request = bind_repository_patch(brief(&fixture.revision)).expect("bind request");
        let mut symlink = proposal(&request, &fixture.revision);
        symlink.changed_paths = vec!["src/link".to_owned()];
        symlink.patch = "diff --git a/src/link b/src/link\nnew file mode 120000\n--- /dev/null\n+++ b/src/link\n@@ -0,0 +1 @@\n+lib.rs\n\\ No newline at end of file\n"
            .to_owned();

        assert!(matches!(
            conform_repository_patch(
                &request,
                &candidate(&request, &symlink),
                &fixture.root,
                &fixture.git
            ),
            Err(RepositoryPatchError::NonRegularPath(path)) if path == "src/link"
        ));
    }
}
