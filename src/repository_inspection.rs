//! Exact repository-inspection capability built on capability-work contracts.

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;

use crate::{
    CapabilityCandidate, CapabilityProviderDescriptor, CapabilityWorkBody, CapabilityWorkRequest,
    CapabilityWorkTurnAdapter, ExactIdentity, FactAcceptance, FactCoverage, FactRequirement,
    InboundAcceptance, Invocation, PreparedTurn, TurnAdapter, WorkContractError,
    work_contract::{BoundFact, canonical_digest},
};

pub const REPOSITORY_INSPECTION_SUITE: &str =
    "dev.fleetd.conformance/repository_inspection_report@0.1.0";

const MAX_REPOSITORY_ID_BYTES: usize = 256;
const MAX_PATH_SCOPES: usize = 32;
const MAX_QUESTIONS: usize = 16;
const MAX_QUESTION_ID_BYTES: usize = 128;
const MAX_QUESTION_BYTES: usize = 4_096;
const MAX_ANSWERS: usize = MAX_QUESTIONS;
const MAX_EVIDENCE_PER_ANSWER: usize = 16;
const MAX_CONCLUSION_BYTES: usize = 8_192;
const MAX_OBSERVATION_BYTES: usize = 4_096;
const MAX_LIMITATIONS: usize = 32;
const MAX_LINE_SPAN: u32 = 200;
const MAX_GIT_OUTPUT_BYTES: usize = 2 * 1_024 * 1_024;

#[must_use]
pub fn repository_inspection_capability() -> ExactIdentity {
    ExactIdentity::new("dev.fleetd.capability", "inspect_repository", "0.1.0")
}

#[must_use]
pub fn repository_inspection_brief_fact() -> ExactIdentity {
    ExactIdentity::new("dev.fleetd.fact", "repository_inspection_brief", "0.1.0")
}

#[must_use]
pub fn repository_inspection_report_fact() -> ExactIdentity {
    ExactIdentity::new("dev.fleetd.fact", "repository_inspection_report", "0.1.0")
}

/// One bounded question whose identity is stable within an inspection request.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryInspectionQuestion {
    pub id: String,
    pub prompt: String,
}

/// Provider-neutral input fact for one exact clean Git revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryInspectionBrief {
    pub schema_version: u32,
    pub repository_id: String,
    pub revision: String,
    pub path_scope: Vec<String>,
    pub questions: Vec<RepositoryInspectionQuestion>,
}

/// Whether the inspected revision supports one answer or leaves it unresolved.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InspectionDisposition {
    Supported,
    Inconclusive,
}

/// One source location in the exact Git revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryInspectionEvidence {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub observation: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryInspectionAnswer {
    pub question_id: String,
    pub disposition: InspectionDisposition,
    pub conclusion: String,
    pub evidence: Vec<RepositoryInspectionEvidence>,
}

/// Structured output fact claimed by the inspection provider.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryInspectionReport {
    pub schema_version: u32,
    pub request_id: String,
    pub repository_id: String,
    pub revision: String,
    pub answers: Vec<RepositoryInspectionAnswer>,
    #[serde(default)]
    pub limitations: Vec<String>,
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RepositoryInspectionDerivation {
    kind: String,
    repository_id: String,
    revision: String,
}

/// Strict semantic adapter for repository inspection through a configured Git
/// executable and clean checkout.
#[derive(Clone, Debug)]
pub struct RepositoryInspectionTurnAdapter {
    delegate: CapabilityWorkTurnAdapter,
    repository_root: PathBuf,
    git_executable: PathBuf,
}

impl RepositoryInspectionTurnAdapter {
    /// Creates an exact repository-inspection provider.
    ///
    /// # Errors
    ///
    /// Returns an error for a provider mismatch, non-absolute Git executable,
    /// or invalid repository root.
    pub fn new(
        provider: CapabilityProviderDescriptor,
        repository_root: PathBuf,
        git_executable: PathBuf,
    ) -> Result<Self, RepositoryInspectionError> {
        if provider.capability != repository_inspection_capability() {
            return Err(RepositoryInspectionError::ProviderCapabilityMismatch);
        }
        if !git_executable.is_absolute() || !git_executable.is_file() {
            return Err(RepositoryInspectionError::InvalidGitExecutable(
                git_executable,
            ));
        }
        if !repository_root.is_absolute() || !repository_root.is_dir() {
            return Err(RepositoryInspectionError::InvalidRepositoryRoot(
                repository_root,
            ));
        }
        let repository_root = fs::canonicalize(repository_root)?;
        let delegate = CapabilityWorkTurnAdapter::new([provider])
            .map_err(RepositoryInspectionError::Adapter)?;
        Ok(Self {
            delegate,
            repository_root,
            git_executable,
        })
    }
}

impl TurnAdapter for RepositoryInspectionTurnAdapter {
    fn inbound_acceptance(&self) -> &InboundAcceptance {
        self.delegate.inbound_acceptance()
    }

    fn prepare(&self, invocation: &Invocation) -> Result<PreparedTurn, String> {
        let mut prepared = self.delegate.prepare(invocation)?;
        let request: CapabilityWorkRequest =
            serde_json::from_value(invocation.message.payload.clone())
                .map_err(|error| format!("repository inspection request is malformed: {error}"))?;
        let brief = inspection_brief(&request).map_err(|error| error.to_string())?;
        validate_clean_checkout(&self.repository_root, &self.git_executable, &brief)
            .map_err(|error| error.to_string())?;
        let question_ids = brief
            .questions
            .iter()
            .map(|question| question.id.as_str())
            .collect::<Vec<_>>();
        let report_schema = json!({
            "schema_version": 1,
            "request_id": request.request_id,
            "repository_id": brief.repository_id,
            "revision": brief.revision,
            "answers": [{
                "question_id": "one exact configured question id",
                "disposition": "supported or inconclusive",
                "conclusion": "bounded conclusion",
                "evidence": [{
                    "path": "repository-relative path inside path_scope",
                    "start_line": 1,
                    "end_line": 1,
                    "observation": "what those exact lines establish"
                }]
            }],
            "limitations": []
        });
        prepared.prompt.push(crate::PromptBlock::Text {
            text: format!(
                "This is read-only repository inspection. Do not modify the checkout. Inspect only revision `{revision}` and paths admitted by the request. Return exactly one answer for each question ID {question_ids:?}. Use 1-based inclusive line numbers from that revision. A supported answer requires at least one exact source location; use inconclusive when the revision cannot establish the answer. The payload of the single requested output fact must match this shape exactly: {report_schema}. The surrounding capability response schema from the previous instruction remains mandatory.",
                revision = brief.revision,
            ),
        });
        Ok(prepared)
    }
}

/// Binds a validated repository-inspection brief into the existing generic
/// capability-work request contract.
///
/// # Errors
///
/// Returns an error for malformed brief fields or canonicalization failure.
pub fn bind_repository_inspection(
    brief: RepositoryInspectionBrief,
) -> Result<CapabilityWorkRequest, RepositoryInspectionError> {
    validate_brief(&brief)?;
    let payload = serde_json::to_value(&brief)?;
    let derivation = RepositoryInspectionDerivation {
        kind: "git_revision".to_owned(),
        repository_id: brief.repository_id.clone(),
        revision: brief.revision.clone(),
    };
    let input_id = canonical_digest(&json!({
        "payload": payload,
        "derivation": derivation,
    }))?;
    let fact = repository_inspection_brief_fact();
    Ok(CapabilityWorkRequest::bind(CapabilityWorkBody {
        capability: repository_inspection_capability(),
        requires: vec![FactRequirement {
            fact: fact.clone(),
            acceptance: FactAcceptance::CompleteOnly,
        }],
        inputs: vec![BoundFact {
            id: input_id,
            fact_type: fact,
            coverage: FactCoverage::Complete,
            payload: serde_json::to_value(&brief)?,
            derivation: serde_json::to_value(RepositoryInspectionDerivation {
                kind: "git_revision".to_owned(),
                repository_id: brief.repository_id,
                revision: brief.revision,
            })?,
        }],
        produces: vec![repository_inspection_report_fact()],
        conformance_suite: REPOSITORY_INSPECTION_SUITE.to_owned(),
    })?)
}

/// Recovers and validates the exact repository-inspection brief from a generic
/// capability-work request.
///
/// # Errors
///
/// Returns an error for any request identity, capability, fact, derivation, or
/// brief mismatch.
pub fn inspection_brief(
    request: &CapabilityWorkRequest,
) -> Result<RepositoryInspectionBrief, RepositoryInspectionError> {
    request.validate()?;
    if request.body.capability != repository_inspection_capability()
        || request.body.conformance_suite != REPOSITORY_INSPECTION_SUITE
        || request.body.requires
            != vec![FactRequirement {
                fact: repository_inspection_brief_fact(),
                acceptance: FactAcceptance::CompleteOnly,
            }]
        || request.body.produces != vec![repository_inspection_report_fact()]
    {
        return Err(RepositoryInspectionError::RequestShapeMismatch);
    }
    let [input] = request.body.inputs.as_slice() else {
        return Err(RepositoryInspectionError::RequestShapeMismatch);
    };
    if input.fact_type != repository_inspection_brief_fact()
        || input.coverage != FactCoverage::Complete
    {
        return Err(RepositoryInspectionError::RequestShapeMismatch);
    }
    let brief: RepositoryInspectionBrief = serde_json::from_value(input.payload.clone())?;
    validate_brief(&brief)?;
    let expected_derivation = RepositoryInspectionDerivation {
        kind: "git_revision".to_owned(),
        repository_id: brief.repository_id.clone(),
        revision: brief.revision.clone(),
    };
    if input.derivation != serde_json::to_value(&expected_derivation)? {
        return Err(RepositoryInspectionError::RequestShapeMismatch);
    }
    let expected_id = canonical_digest(&json!({
        "payload": brief,
        "derivation": expected_derivation,
    }))?;
    if input.id != expected_id {
        return Err(RepositoryInspectionError::InputIdentityMismatch);
    }
    serde_json::from_value(input.payload.clone()).map_err(Into::into)
}

/// Deterministically validates a lifted candidate and every cited location
/// against Git objects from the request's exact revision.
///
/// This establishes structural conformance and valid evidence locations, not
/// the truth of natural-language conclusions.
///
/// # Errors
///
/// Returns an error for candidate, report, checkout, answer, scope, or source
/// location mismatch.
pub fn conform_repository_inspection(
    request: &CapabilityWorkRequest,
    candidate: &CapabilityCandidate,
    repository_root: &Path,
    git_executable: &Path,
) -> Result<RepositoryInspectionReport, RepositoryInspectionError> {
    candidate.validate(request)?;
    let brief = inspection_brief(request)?;
    validate_clean_checkout(repository_root, git_executable, &brief)?;
    let [output] = candidate.body.outputs.as_slice() else {
        return Err(RepositoryInspectionError::ReportShapeMismatch);
    };
    if output.fact_type != repository_inspection_report_fact()
        || output.coverage != FactCoverage::Complete
    {
        return Err(RepositoryInspectionError::ReportShapeMismatch);
    }
    let report: RepositoryInspectionReport = serde_json::from_value(output.payload.clone())?;
    validate_report(&brief, request, &report, repository_root, git_executable)?;
    Ok(report)
}

fn validate_brief(brief: &RepositoryInspectionBrief) -> Result<(), RepositoryInspectionError> {
    if brief.schema_version != 1 {
        return Err(RepositoryInspectionError::UnsupportedSchema(
            brief.schema_version,
        ));
    }
    validate_text(
        "repository_id",
        &brief.repository_id,
        MAX_REPOSITORY_ID_BYTES,
    )?;
    validate_revision(&brief.revision)?;
    if brief.path_scope.is_empty() || brief.path_scope.len() > MAX_PATH_SCOPES {
        return Err(RepositoryInspectionError::InvalidCount("path_scope"));
    }
    let mut paths = BTreeSet::new();
    for path in &brief.path_scope {
        validate_relative_path(path, true)?;
        if !paths.insert(path) {
            return Err(RepositoryInspectionError::Duplicate("path_scope"));
        }
    }
    if brief.questions.is_empty() || brief.questions.len() > MAX_QUESTIONS {
        return Err(RepositoryInspectionError::InvalidCount("questions"));
    }
    let mut question_ids = BTreeSet::new();
    for question in &brief.questions {
        validate_identifier("question id", &question.id, MAX_QUESTION_ID_BYTES)?;
        validate_text("question prompt", &question.prompt, MAX_QUESTION_BYTES)?;
        if !question_ids.insert(&question.id) {
            return Err(RepositoryInspectionError::Duplicate("question id"));
        }
    }
    Ok(())
}

fn validate_report(
    brief: &RepositoryInspectionBrief,
    request: &CapabilityWorkRequest,
    report: &RepositoryInspectionReport,
    repository_root: &Path,
    git_executable: &Path,
) -> Result<(), RepositoryInspectionError> {
    if report.schema_version != 1 {
        return Err(RepositoryInspectionError::UnsupportedSchema(
            report.schema_version,
        ));
    }
    if report.request_id != request.request_id
        || report.repository_id != brief.repository_id
        || report.revision != brief.revision
    {
        return Err(RepositoryInspectionError::ReportBindingMismatch);
    }
    if report.answers.len() != brief.questions.len() || report.answers.len() > MAX_ANSWERS {
        return Err(RepositoryInspectionError::InvalidCount("answers"));
    }
    let expected_questions = brief
        .questions
        .iter()
        .map(|question| question.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut actual_questions = BTreeSet::new();
    for answer in &report.answers {
        if !actual_questions.insert(answer.question_id.as_str()) {
            return Err(RepositoryInspectionError::Duplicate("answer question id"));
        }
        validate_text("conclusion", &answer.conclusion, MAX_CONCLUSION_BYTES)?;
        if answer.evidence.len() > MAX_EVIDENCE_PER_ANSWER
            || (answer.disposition == InspectionDisposition::Supported
                && answer.evidence.is_empty())
        {
            return Err(RepositoryInspectionError::InvalidCount("evidence"));
        }
        for evidence in &answer.evidence {
            validate_evidence(brief, evidence, repository_root, git_executable)?;
        }
    }
    if actual_questions != expected_questions {
        return Err(RepositoryInspectionError::AnswerSetMismatch);
    }
    if report.limitations.len() > MAX_LIMITATIONS {
        return Err(RepositoryInspectionError::InvalidCount("limitations"));
    }
    for limitation in &report.limitations {
        validate_text("limitation", limitation, MAX_OBSERVATION_BYTES)?;
    }
    Ok(())
}

fn validate_evidence(
    brief: &RepositoryInspectionBrief,
    evidence: &RepositoryInspectionEvidence,
    repository_root: &Path,
    git_executable: &Path,
) -> Result<(), RepositoryInspectionError> {
    validate_relative_path(&evidence.path, false)?;
    if !brief
        .path_scope
        .iter()
        .any(|scope| path_is_in_scope(&evidence.path, scope))
    {
        return Err(RepositoryInspectionError::EvidenceOutsideScope(
            evidence.path.clone(),
        ));
    }
    if evidence.start_line == 0
        || evidence.end_line < evidence.start_line
        || evidence.end_line - evidence.start_line + 1 > MAX_LINE_SPAN
    {
        return Err(RepositoryInspectionError::InvalidLineRange {
            path: evidence.path.clone(),
            start: evidence.start_line,
            end: evidence.end_line,
        });
    }
    validate_text(
        "evidence observation",
        &evidence.observation,
        MAX_OBSERVATION_BYTES,
    )?;
    let object = format!("{}:{}", brief.revision, evidence.path);
    let source = git_output(repository_root, git_executable, ["show", object.as_str()])?;
    let source = String::from_utf8(source)
        .map_err(|_| RepositoryInspectionError::NonUtf8Source(evidence.path.clone()))?;
    let line_count = source.lines().count();
    if usize::try_from(evidence.end_line).map_or(true, |end| end > line_count) {
        return Err(RepositoryInspectionError::InvalidLineRange {
            path: evidence.path.clone(),
            start: evidence.start_line,
            end: evidence.end_line,
        });
    }
    Ok(())
}

fn validate_clean_checkout(
    repository_root: &Path,
    git_executable: &Path,
    brief: &RepositoryInspectionBrief,
) -> Result<(), RepositoryInspectionError> {
    validate_git_inputs(repository_root, git_executable)?;
    validate_repository_root(repository_root, git_executable)?;
    let observed_revision = git_text(
        repository_root,
        git_executable,
        ["rev-parse", "--verify", "HEAD^{commit}"],
    )?;
    if observed_revision.trim() != brief.revision {
        return Err(RepositoryInspectionError::RevisionMismatch {
            expected: brief.revision.clone(),
            actual: observed_revision.trim().to_owned(),
        });
    }
    let status = git_text(
        repository_root,
        git_executable,
        ["status", "--porcelain=v1", "--untracked-files=all"],
    )?;
    if !status.trim().is_empty() {
        return Err(RepositoryInspectionError::DirtyCheckout);
    }
    Ok(())
}

fn validate_repository_root(
    repository_root: &Path,
    git_executable: &Path,
) -> Result<(), RepositoryInspectionError> {
    let top_level = git_text(
        repository_root,
        git_executable,
        ["rev-parse", "--show-toplevel"],
    )?;
    let configured_root = fs::canonicalize(repository_root)?;
    let observed_root = fs::canonicalize(top_level.trim())?;
    if observed_root != configured_root {
        return Err(RepositoryInspectionError::RepositoryRootMismatch);
    }
    Ok(())
}

fn validate_git_inputs(
    repository_root: &Path,
    git_executable: &Path,
) -> Result<(), RepositoryInspectionError> {
    if !repository_root.is_absolute() || !repository_root.is_dir() {
        return Err(RepositoryInspectionError::InvalidRepositoryRoot(
            repository_root.to_path_buf(),
        ));
    }
    if !git_executable.is_absolute() || !git_executable.is_file() {
        return Err(RepositoryInspectionError::InvalidGitExecutable(
            git_executable.to_path_buf(),
        ));
    }
    Ok(())
}

fn git_text<const N: usize>(
    repository_root: &Path,
    git_executable: &Path,
    arguments: [&str; N],
) -> Result<String, RepositoryInspectionError> {
    let output = git_output(repository_root, git_executable, arguments)?;
    String::from_utf8(output).map_err(|_| RepositoryInspectionError::NonUtf8GitOutput)
}

fn git_output<I, S>(
    repository_root: &Path,
    git_executable: &Path,
    arguments: I,
) -> Result<Vec<u8>, RepositoryInspectionError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new(git_executable)
        .env_clear()
        .env("LC_ALL", "C")
        .arg("-C")
        .arg(repository_root)
        .args(arguments)
        .output()?;
    if output.stdout.len() > MAX_GIT_OUTPUT_BYTES || output.stderr.len() > MAX_GIT_OUTPUT_BYTES {
        return Err(RepositoryInspectionError::GitOutputTooLarge);
    }
    if !output.status.success() {
        let mut diagnostic = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if diagnostic.len() > MAX_OBSERVATION_BYTES {
            let mut end = MAX_OBSERVATION_BYTES;
            while !diagnostic.is_char_boundary(end) {
                end -= 1;
            }
            diagnostic.truncate(end);
        }
        return Err(RepositoryInspectionError::GitFailed {
            status: output.status.code(),
            diagnostic,
        });
    }
    Ok(output.stdout)
}

fn validate_revision(revision: &str) -> Result<(), RepositoryInspectionError> {
    if !matches!(revision.len(), 40 | 64)
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RepositoryInspectionError::InvalidRevision(
            revision.to_owned(),
        ));
    }
    Ok(())
}

fn validate_identifier(
    field: &'static str,
    value: &str,
    limit: usize,
) -> Result<(), RepositoryInspectionError> {
    validate_text(field, value, limit)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(RepositoryInspectionError::InvalidIdentifier(field));
    }
    Ok(())
}

fn validate_text(
    field: &'static str,
    value: &str,
    limit: usize,
) -> Result<(), RepositoryInspectionError> {
    if value.trim().is_empty() || value.len() > limit {
        return Err(RepositoryInspectionError::InvalidText { field, limit });
    }
    Ok(())
}

fn validate_relative_path(value: &str, allow_dot: bool) -> Result<(), RepositoryInspectionError> {
    if allow_dot && value == "." {
        return Ok(());
    }
    let path = Path::new(value);
    let normalized = path.components().collect::<PathBuf>();
    if value.is_empty()
        || value.contains('\\')
        || path.is_absolute()
        || normalized.as_os_str() != std::ffi::OsStr::new(value)
        || !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
    {
        return Err(RepositoryInspectionError::InvalidPath(value.to_owned()));
    }
    Ok(())
}

fn path_is_in_scope(path: &str, scope: &str) -> bool {
    scope == "."
        || path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|remainder| remainder.starts_with('/'))
}

#[derive(Debug, Error)]
pub enum RepositoryInspectionError {
    #[error("repository inspection schema version {0} is unsupported")]
    UnsupportedSchema(u32),
    #[error("invalid {field}; it must contain between 1 and {limit} bytes")]
    InvalidText { field: &'static str, limit: usize },
    #[error("{0} contains unsupported identifier characters")]
    InvalidIdentifier(&'static str),
    #[error("invalid count for {0}")]
    InvalidCount(&'static str),
    #[error("duplicate {0}")]
    Duplicate(&'static str),
    #[error("invalid repository-relative path {0}")]
    InvalidPath(String),
    #[error("invalid Git revision {0}")]
    InvalidRevision(String),
    #[error("repository inspection request shape does not match the exact capability")]
    RequestShapeMismatch,
    #[error("repository inspection input identity does not match its payload and derivation")]
    InputIdentityMismatch,
    #[error("configured provider does not implement repository inspection")]
    ProviderCapabilityMismatch,
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
    #[error("repository inspection report shape is invalid")]
    ReportShapeMismatch,
    #[error("repository inspection report does not bind the exact request")]
    ReportBindingMismatch,
    #[error("repository inspection answer set does not match the exact questions")]
    AnswerSetMismatch,
    #[error("evidence path is outside the request scope: {0}")]
    EvidenceOutsideScope(String),
    #[error("invalid line range {start}-{end} for {path}")]
    InvalidLineRange { path: String, start: u32, end: u32 },
    #[error("repository source is not UTF-8: {0}")]
    NonUtf8Source(String),
    #[error("Git output is not UTF-8")]
    NonUtf8GitOutput,
    #[error("Git output exceeded the inspection bound")]
    GitOutputTooLarge,
    #[error("Git failed with status {status:?}: {diagnostic}")]
    GitFailed {
        status: Option<i32>,
        diagnostic: String,
    },
    #[error("repository inspection adapter is invalid: {0}")]
    Adapter(String),
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

    fn brief(revision: &str) -> RepositoryInspectionBrief {
        RepositoryInspectionBrief {
            schema_version: 1,
            repository_id: "dev.fleetd/fleetd".to_owned(),
            revision: revision.to_owned(),
            path_scope: vec!["src".to_owned()],
            questions: vec![RepositoryInspectionQuestion {
                id: "inspected-function".to_owned(),
                prompt: "Does the exact revision contain the inspected function?".to_owned(),
            }],
        }
    }

    fn provider() -> CapabilityProviderDescriptor {
        CapabilityProviderDescriptor {
            id: ExactIdentity::new("dev.fleetd.provider", "fixture_inspector", "0.1.0"),
            capability: repository_inspection_capability(),
            implementation_digest: format!("sha256:{}", "a".repeat(64)),
        }
    }

    fn invocation(request: &CapabilityWorkRequest) -> Invocation {
        Invocation {
            id: "inspection-invocation".to_owned(),
            agent_id: "inspector".to_owned(),
            message: Message {
                seq: 1,
                id: "inspection-message".to_owned(),
                channel_id: "inspection-channel".to_owned(),
                sender_id: "requester".to_owned(),
                recipient_id: Some("inspector".to_owned()),
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

    #[test]
    fn inspection_request_is_a_bound_capability_work_specialization() {
        let fixture = git_fixture();
        let request = bind_repository_inspection(brief(&fixture.revision)).expect("bind request");

        request.validate().expect("generic request validates");
        assert_eq!(request.body.capability, repository_inspection_capability());
        assert_eq!(
            inspection_brief(&request).expect("recover brief"),
            brief(&fixture.revision)
        );

        let mut changed = request;
        changed.body.inputs[0].payload["questions"][0]["prompt"] = json!("changed");
        assert!(inspection_brief(&changed).is_err());

        let mut non_normal_path = brief(&fixture.revision);
        non_normal_path.path_scope = vec!["src//nested".to_owned()];
        assert!(matches!(
            bind_repository_inspection(non_normal_path),
            Err(RepositoryInspectionError::InvalidPath(_))
        ));
    }

    #[test]
    fn inspection_adapter_composes_generic_capability_turn_after_git_preflight() {
        let fixture = git_fixture();
        let request = bind_repository_inspection(brief(&fixture.revision)).expect("bind request");
        let adapter = RepositoryInspectionTurnAdapter::new(
            provider(),
            fixture.root.clone(),
            fixture.git.clone(),
        )
        .expect("inspection adapter");

        let prepared = adapter
            .prepare(&invocation(&request))
            .expect("prepare exact inspection");

        assert_eq!(prepared.lane_key, request.request_id);
        assert_eq!(prepared.prompt.len(), 2);
        assert!(matches!(
            prepared.prompt.last(),
            Some(crate::PromptBlock::Text { text }) if text.contains("read-only repository inspection")
        ));

        fs::write(fixture.root.join("src/lib.rs"), "changed\n").expect("dirty source");
        assert!(adapter.prepare(&invocation(&request)).is_err());
    }

    #[test]
    fn conformance_checks_exact_answers_scope_and_git_line_locations() {
        let fixture = git_fixture();
        let request = bind_repository_inspection(brief(&fixture.revision)).expect("bind request");
        validate_clean_checkout(&fixture.root, &fixture.git, &brief(&fixture.revision))
            .expect("clean exact checkout");
        let report = RepositoryInspectionReport {
            schema_version: 1,
            request_id: request.request_id.clone(),
            repository_id: "dev.fleetd/fleetd".to_owned(),
            revision: fixture.revision.clone(),
            answers: vec![RepositoryInspectionAnswer {
                question_id: "inspected-function".to_owned(),
                disposition: InspectionDisposition::Supported,
                conclusion: "The function is present.".to_owned(),
                evidence: vec![RepositoryInspectionEvidence {
                    path: "src/lib.rs".to_owned(),
                    start_line: 1,
                    end_line: 3,
                    observation: "The complete function definition occupies these lines."
                        .to_owned(),
                }],
            }],
            limitations: Vec::new(),
        };
        let candidate = CapabilityCandidate::bind(
            &request,
            CapabilityCandidateBody {
                request_id: request.request_id.clone(),
                provider: provider(),
                outputs: vec![CandidateFact {
                    fact_type: repository_inspection_report_fact(),
                    coverage: FactCoverage::Complete,
                    payload: serde_json::to_value(&report).expect("encode report"),
                }],
                attempt: AttemptEvidence {
                    authority: "dev.fleetd.agent/fixture".to_owned(),
                    attempt_id: "attempt".to_owned(),
                    invocation_id: "invocation".to_owned(),
                    evidence_digest: format!("sha256:{}", "b".repeat(64)),
                },
            },
        )
        .expect("bind candidate");

        assert_eq!(
            conform_repository_inspection(&request, &candidate, &fixture.root, &fixture.git)
                .expect("conform report"),
            report
        );

        let mut outside = candidate.clone();
        outside.body.outputs[0].payload["answers"][0]["evidence"][0]["path"] = json!("README.md");
        outside.body.outputs[0].payload["answers"][0]["evidence"][0]["end_line"] = json!(1);
        let outside = CapabilityCandidate::bind(&request, outside.body)
            .expect("rebind structurally valid candidate");
        assert!(matches!(
            conform_repository_inspection(&request, &outside, &fixture.root, &fixture.git),
            Err(RepositoryInspectionError::EvidenceOutsideScope(_))
        ));

        fs::write(fixture.root.join("src/lib.rs"), "changed\n").expect("dirty source");
        assert!(matches!(
            conform_repository_inspection(&request, &candidate, &fixture.root, &fixture.git),
            Err(RepositoryInspectionError::DirtyCheckout)
        ));
    }

    #[test]
    fn clean_checkout_preflight_rejects_dirty_or_wrong_revision() {
        let fixture = git_fixture();
        let exact = brief(&fixture.revision);
        validate_clean_checkout(&fixture.root, &fixture.git, &exact).expect("exact checkout");

        fs::write(fixture.root.join("src/lib.rs"), "changed\n").expect("dirty source");
        assert!(matches!(
            validate_clean_checkout(&fixture.root, &fixture.git, &exact),
            Err(RepositoryInspectionError::DirtyCheckout)
        ));

        let wrong = brief(&"f".repeat(40));
        assert!(matches!(
            validate_clean_checkout(&fixture.root, &fixture.git, &wrong),
            Err(RepositoryInspectionError::RevisionMismatch { .. })
        ));
    }
}
