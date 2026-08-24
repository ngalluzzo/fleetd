//! Versioned capability-work contracts interpreted above the message kernel.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::Message;

pub const CAPABILITY_WORK_REQUEST_KIND: &str = "work.capability.request/v1";
pub const CAPABILITY_WORK_ATTEMPT_KIND: &str = "work.capability.attempt/v1";
pub const CAPABILITY_WORK_CANDIDATE_KIND: &str = "work.capability.candidate/v1";

const MAX_IDENTITY_PART_BYTES: usize = 256;
const MAX_FACTS: usize = 64;
const MAX_CONFORMANCE_SUITE_BYTES: usize = 512;
const MAX_DIAGNOSTICS: usize = 64;
const SHA256_PREFIX: &str = "sha256:";

/// One exact, independently versioned semantic identity.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExactIdentity {
    pub package: String,
    pub name: String,
    pub version: String,
}

impl ExactIdentity {
    #[must_use]
    pub fn new(
        package: impl Into<String>,
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        Self {
            package: package.into(),
            name: name.into(),
            version: version.into(),
        }
    }

    /// Validates the bounded wire representation of this exact identity.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, oversized, or unsupported identity parts.
    pub fn validate(&self) -> Result<(), WorkContractError> {
        validate_identity(self)
    }
}

impl std::fmt::Display for ExactIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}@{}", self.package, self.name, self.version)
    }
}

/// Whether a capability input accepts a fact with unresolved defeats.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FactAcceptance {
    CompleteOnly,
    PartialAllowed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FactRequirement {
    pub fact: ExactIdentity,
    pub acceptance: FactAcceptance,
}

/// Coverage reported by the producer of one bound input fact. Coverage is not
/// provider trust or conformance.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FactCoverage {
    Complete,
    Partial,
}

/// One immutable semantic input. The payload and derivation remain opaque to
/// fleetd; their exact JSON survives into the durable message and harness turn.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BoundFact {
    pub id: String,
    pub fact_type: ExactIdentity,
    pub coverage: FactCoverage,
    pub payload: Value,
    pub derivation: Value,
}

/// The digest-bearing portion of a work request. This is provider-neutral and
/// contains no agent, harness, workspace, or transport identity.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityWorkBody {
    pub capability: ExactIdentity,
    pub requires: Vec<FactRequirement>,
    pub inputs: Vec<BoundFact>,
    pub produces: Vec<ExactIdentity>,
    pub conformance_suite: String,
}

/// One exact capability invocation specification. The authenticated fleetd
/// message supplies requester authority; the durable invocation and session
/// turn later supply execution ownership.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityWorkRequest {
    pub request_id: String,
    #[serde(flatten)]
    pub body: CapabilityWorkBody,
}

impl CapabilityWorkRequest {
    /// Binds a validated request body to its deterministic content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when identities, requirements, bound facts, outputs,
    /// or coverage are malformed.
    pub fn bind(body: CapabilityWorkBody) -> Result<Self, WorkContractError> {
        validate_body(&body)?;
        let request_id = body_digest(&body)?;
        Ok(Self { request_id, body })
    }

    /// Validates an externally received request, including its content digest.
    ///
    /// # Errors
    ///
    /// Returns an error when the body is invalid or no longer matches the
    /// request identity.
    pub fn validate(&self) -> Result<(), WorkContractError> {
        validate_body(&self.body)?;
        let expected = body_digest(&self.body)?;
        if self.request_id != expected {
            return Err(WorkContractError::IdentityMismatch {
                expected,
                actual: self.request_id.clone(),
            });
        }
        Ok(())
    }
}

/// Exact semantic provider selected by the capability-work adapter. This is
/// distinct from the harness plugin and transport protocol used to run it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityProviderDescriptor {
    pub id: ExactIdentity,
    pub capability: ExactIdentity,
    pub implementation_digest: String,
}

impl CapabilityProviderDescriptor {
    /// Validates exact provider identity, capability, and implementation.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed identities or implementation digests.
    pub fn validate(&self) -> Result<(), WorkContractError> {
        validate_identity(&self.id)?;
        validate_identity(&self.capability)?;
        validate_sha256(&self.implementation_digest)
    }
}

/// One unverified output claimed by a provider attempt.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateFact {
    pub fact_type: ExactIdentity,
    pub coverage: FactCoverage,
    pub payload: Value,
}

/// Opaque reference to the immutable Fleetd attempt that was lifted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptEvidence {
    pub authority: String,
    pub attempt_id: String,
    pub invocation_id: String,
    pub evidence_digest: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityCandidateBody {
    pub request_id: String,
    pub provider: CapabilityProviderDescriptor,
    pub outputs: Vec<CandidateFact>,
    pub attempt: AttemptEvidence,
}

/// Provider-neutral candidate document emitted by strictly lifting a raw
/// attempt. Matching output types do not establish conformance or trust.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityCandidate {
    pub candidate_id: String,
    #[serde(flatten)]
    pub body: CapabilityCandidateBody,
}

impl CapabilityCandidate {
    /// Binds exact candidate outputs and attempt evidence to a request.
    ///
    /// # Errors
    ///
    /// Returns an error for any request, provider, output, or evidence mismatch.
    pub fn bind(
        request: &CapabilityWorkRequest,
        body: CapabilityCandidateBody,
    ) -> Result<Self, WorkContractError> {
        validate_candidate_body(request, &body)?;
        let candidate_id = canonical_digest(&body)?;
        Ok(Self { candidate_id, body })
    }

    /// Revalidates a deserialized candidate and its content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when the candidate is malformed or no longer matches
    /// its deterministic identity.
    pub fn validate(&self, request: &CapabilityWorkRequest) -> Result<(), WorkContractError> {
        validate_candidate_body(request, &self.body)?;
        let expected = canonical_digest(&self.body)?;
        if self.candidate_id != expected {
            return Err(WorkContractError::CandidateIdentityMismatch {
                expected,
                actual: self.candidate_id.clone(),
            });
        }
        Ok(())
    }
}

/// A provider may explicitly report that it could not produce a candidate.
/// This remains durable attempt evidence and never becomes a semantic fact.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityUnable {
    pub request_id: String,
    pub provider: CapabilityProviderDescriptor,
    pub attempt: AttemptEvidence,
    pub diagnostics: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CapabilityAttemptProjection {
    Candidate(CapabilityCandidate),
    Unable(CapabilityUnable),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ProviderResponseStatus {
    Candidate,
    Unable,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ProviderConformanceStatus {
    Unverified,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderResponse {
    request_id: String,
    status: ProviderResponseStatus,
    outputs: Vec<CandidateFact>,
    conformance_suite: String,
    #[serde(rename = "conformance_status")]
    _conformance_status: ProviderConformanceStatus,
    #[serde(default)]
    diagnostics: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AttemptResultContext {
    request_id: String,
    provider: CapabilityProviderDescriptor,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct HarnessAttemptPayload {
    status: String,
    invocation_id: String,
    stop_reason: String,
    output_complete: bool,
    assistant_messages: Vec<AttemptAssistantMessage>,
    #[serde(rename = "usage")]
    _usage: Value,
    #[serde(rename = "session_persistence")]
    _session_persistence: String,
    result_context: AttemptResultContext,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AttemptAssistantMessage {
    message_id: Option<String>,
    content: Vec<Value>,
    complete: bool,
    first_event_seq: u64,
    last_event_seq: u64,
}

/// Strictly lifts a known-complete harness attempt into either one exact
/// unverified candidate or an explicit unable result. The raw attempt remains
/// the evidence authority; prose and malformed JSON fail closed.
///
/// # Errors
///
/// Returns an error for incomplete terminal evidence, malformed provider JSON,
/// or any request/provider/output mismatch.
pub fn extract_capability_attempt(
    request: &CapabilityWorkRequest,
    authority: impl Into<String>,
    attempt_id: impl Into<String>,
    payload: &Value,
) -> Result<CapabilityAttemptProjection, WorkContractError> {
    request.validate()?;
    let attempt: HarnessAttemptPayload = serde_json::from_value(payload.clone())
        .map_err(|error| WorkContractError::MalformedAttempt(error.to_string()))?;
    if attempt.status != "completed"
        || attempt.stop_reason != "end_turn"
        || !attempt.output_complete
    {
        return Err(WorkContractError::IncompleteAttempt);
    }
    if attempt.result_context.request_id != request.request_id {
        return Err(WorkContractError::RequestMismatch);
    }
    attempt.result_context.provider.validate()?;
    if attempt.result_context.provider.capability != request.body.capability {
        return Err(WorkContractError::ProviderCapabilityMismatch);
    }
    let response_text = exact_assistant_json(&attempt.assistant_messages)?;
    let response: ProviderResponse = serde_json::from_str(&response_text)
        .map_err(|error| WorkContractError::MalformedProviderResponse(error.to_string()))?;
    if response.request_id != request.request_id
        || response.conformance_suite != request.body.conformance_suite
    {
        return Err(WorkContractError::RequestMismatch);
    }
    let evidence = AttemptEvidence {
        authority: authority.into(),
        attempt_id: attempt_id.into(),
        invocation_id: attempt.invocation_id,
        evidence_digest: canonical_digest(payload)?,
    };
    match response.status {
        ProviderResponseStatus::Candidate => {
            if !response.diagnostics.is_empty() {
                return Err(WorkContractError::UnexpectedDiagnostics);
            }
            Ok(CapabilityAttemptProjection::Candidate(
                CapabilityCandidate::bind(
                    request,
                    CapabilityCandidateBody {
                        request_id: request.request_id.clone(),
                        provider: attempt.result_context.provider,
                        outputs: response.outputs,
                        attempt: evidence,
                    },
                )?,
            ))
        }
        ProviderResponseStatus::Unable => {
            if !response.outputs.is_empty() || response.diagnostics.is_empty() {
                return Err(WorkContractError::InvalidUnableResponse);
            }
            if response.diagnostics.len() > MAX_DIAGNOSTICS {
                return Err(WorkContractError::TooMany {
                    field: "diagnostics",
                    limit: MAX_DIAGNOSTICS,
                });
            }
            for diagnostic in &response.diagnostics {
                validate_bounded("diagnostic", diagnostic, MAX_CONFORMANCE_SUITE_BYTES)?;
            }
            Ok(CapabilityAttemptProjection::Unable(CapabilityUnable {
                request_id: request.request_id.clone(),
                provider: attempt.result_context.provider,
                attempt: evidence,
                diagnostics: response.diagnostics,
            }))
        }
    }
}

/// Strictly lifts one immutable Fleetd attempt message, deriving attempt
/// authority and identity from the durable envelope instead of caller input.
///
/// # Errors
///
/// Returns an error when envelope kind, correlation, causation, payload, or
/// candidate bindings do not match the exact request.
pub fn extract_capability_message(
    request: &CapabilityWorkRequest,
    message: &Message,
) -> Result<CapabilityAttemptProjection, WorkContractError> {
    if message.kind != CAPABILITY_WORK_ATTEMPT_KIND {
        return Err(WorkContractError::AttemptKindMismatch);
    }
    if message.correlation_id.as_deref() != Some(request.request_id.as_str()) {
        return Err(WorkContractError::AttemptCorrelationMismatch);
    }
    if message.causation_id.is_none() {
        return Err(WorkContractError::AttemptCausationMissing);
    }
    let authority = format!("dev.fleetd.agent/{}", message.sender_id);
    let projection = extract_capability_attempt(request, authority, &message.id, &message.payload)?;
    let evidence_digest = canonical_digest(message)?;
    match projection {
        CapabilityAttemptProjection::Candidate(mut candidate) => {
            candidate.body.attempt.evidence_digest = evidence_digest;
            Ok(CapabilityAttemptProjection::Candidate(
                CapabilityCandidate::bind(request, candidate.body)?,
            ))
        }
        CapabilityAttemptProjection::Unable(mut unable) => {
            unable.attempt.evidence_digest = evidence_digest;
            Ok(CapabilityAttemptProjection::Unable(unable))
        }
    }
}

/// Adapter-owned context persisted verbatim with raw terminal evidence.
///
/// # Errors
///
/// Returns an error if the exact context cannot be represented as JSON.
pub fn capability_attempt_context(
    request: &CapabilityWorkRequest,
    provider: &CapabilityProviderDescriptor,
) -> Result<Value, WorkContractError> {
    serde_json::to_value(AttemptResultContext {
        request_id: request.request_id.clone(),
        provider: provider.clone(),
    })
    .map_err(|error| WorkContractError::Serialization(error.to_string()))
}

#[derive(Debug, Error)]
pub enum WorkContractError {
    #[error("{0} must not be empty")]
    Empty(&'static str),
    #[error("{field} exceeds {limit} bytes")]
    TooLong { field: &'static str, limit: usize },
    #[error("{field} contains unsupported characters")]
    InvalidIdentityPart { field: &'static str },
    #[error("capability work request has too many {field}; maximum is {limit}")]
    TooMany { field: &'static str, limit: usize },
    #[error("duplicate {field} identity {identity}")]
    DuplicateIdentity {
        field: &'static str,
        identity: ExactIdentity,
    },
    #[error("bound inputs do not exactly match required fact identities")]
    InputSetMismatch,
    #[error("fact {fact} requires complete coverage but input is partial")]
    PartialInputRejected { fact: ExactIdentity },
    #[error("fact identity {0} is not a lowercase SHA-256 digest")]
    InvalidFactId(String),
    #[error("request identity mismatch: expected {expected}, received {actual}")]
    IdentityMismatch { expected: String, actual: String },
    #[error("candidate identity mismatch: expected {expected}, received {actual}")]
    CandidateIdentityMismatch { expected: String, actual: String },
    #[error("candidate does not bind the exact capability request")]
    RequestMismatch,
    #[error("configured provider does not implement the requested exact capability")]
    ProviderCapabilityMismatch,
    #[error("candidate outputs do not exactly match the requested output set")]
    OutputSetMismatch,
    #[error("candidate attempt evidence is malformed")]
    InvalidAttemptEvidence,
    #[error("harness attempt is not a complete successful terminal result")]
    IncompleteAttempt,
    #[error("harness attempt payload is malformed: {0}")]
    MalformedAttempt(String),
    #[error("message is not a capability attempt")]
    AttemptKindMismatch,
    #[error("attempt correlation does not equal the capability request identity")]
    AttemptCorrelationMismatch,
    #[error("capability attempt is missing request-message causation")]
    AttemptCausationMissing,
    #[error("provider response is malformed: {0}")]
    MalformedProviderResponse(String),
    #[error("provider attempt must contain exactly one complete JSON-only assistant message")]
    AmbiguousAssistantOutput,
    #[error("candidate response must not contain diagnostics")]
    UnexpectedDiagnostics,
    #[error("unable response must contain diagnostics and no outputs")]
    InvalidUnableResponse,
    #[error("work contract could not be serialized: {0}")]
    Serialization(String),
}

fn validate_body(body: &CapabilityWorkBody) -> Result<(), WorkContractError> {
    validate_identity(&body.capability)?;
    validate_count("requirements", body.requires.len())?;
    validate_count("inputs", body.inputs.len())?;
    validate_count("outputs", body.produces.len())?;
    if body.produces.is_empty() {
        return Err(WorkContractError::Empty("produced fact set"));
    }
    validate_bounded(
        "conformance suite",
        &body.conformance_suite,
        MAX_CONFORMANCE_SUITE_BYTES,
    )?;

    let mut requirements = BTreeSet::new();
    for requirement in &body.requires {
        validate_identity(&requirement.fact)?;
        if !requirements.insert(requirement.fact.clone()) {
            return Err(WorkContractError::DuplicateIdentity {
                field: "requirement",
                identity: requirement.fact.clone(),
            });
        }
    }

    let mut inputs = BTreeSet::new();
    for input in &body.inputs {
        validate_identity(&input.fact_type)?;
        validate_sha256(&input.id)?;
        if !inputs.insert(input.fact_type.clone()) {
            return Err(WorkContractError::DuplicateIdentity {
                field: "input",
                identity: input.fact_type.clone(),
            });
        }
        if body.requires.iter().any(|requirement| {
            requirement.fact == input.fact_type
                && requirement.acceptance == FactAcceptance::CompleteOnly
                && input.coverage == FactCoverage::Partial
        }) {
            return Err(WorkContractError::PartialInputRejected {
                fact: input.fact_type.clone(),
            });
        }
    }
    if requirements != inputs {
        return Err(WorkContractError::InputSetMismatch);
    }

    let mut outputs = BTreeSet::new();
    for output in &body.produces {
        validate_identity(output)?;
        if !outputs.insert(output.clone()) {
            return Err(WorkContractError::DuplicateIdentity {
                field: "output",
                identity: output.clone(),
            });
        }
    }
    Ok(())
}

fn validate_candidate_body(
    request: &CapabilityWorkRequest,
    body: &CapabilityCandidateBody,
) -> Result<(), WorkContractError> {
    request.validate()?;
    if body.request_id != request.request_id {
        return Err(WorkContractError::RequestMismatch);
    }
    body.provider.validate()?;
    if body.provider.capability != request.body.capability {
        return Err(WorkContractError::ProviderCapabilityMismatch);
    }
    validate_count("candidate outputs", body.outputs.len())?;
    let mut outputs = BTreeSet::new();
    for output in &body.outputs {
        validate_identity(&output.fact_type)?;
        if !outputs.insert(output.fact_type.clone()) {
            return Err(WorkContractError::DuplicateIdentity {
                field: "candidate output",
                identity: output.fact_type.clone(),
            });
        }
    }
    if outputs != request.body.produces.iter().cloned().collect() {
        return Err(WorkContractError::OutputSetMismatch);
    }
    validate_bounded(
        "attempt authority",
        &body.attempt.authority,
        MAX_CONFORMANCE_SUITE_BYTES,
    )?;
    validate_bounded(
        "attempt identity",
        &body.attempt.attempt_id,
        MAX_IDENTITY_PART_BYTES,
    )?;
    validate_bounded(
        "invocation identity",
        &body.attempt.invocation_id,
        MAX_IDENTITY_PART_BYTES,
    )?;
    validate_sha256(&body.attempt.evidence_digest)
        .map_err(|_| WorkContractError::InvalidAttemptEvidence)
}

fn exact_assistant_json(messages: &[AttemptAssistantMessage]) -> Result<String, WorkContractError> {
    let [message] = messages else {
        return Err(WorkContractError::AmbiguousAssistantOutput);
    };
    if !message.complete
        || message.first_event_seq == 0
        || message.first_event_seq > message.last_event_seq
    {
        return Err(WorkContractError::AmbiguousAssistantOutput);
    }
    let _ = &message.message_id;
    if message.content.is_empty() {
        return Err(WorkContractError::AmbiguousAssistantOutput);
    }
    let mut text = String::new();
    for block in &message.content {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TextBlock {
            r#type: String,
            text: String,
        }
        let block: TextBlock = serde_json::from_value(block.clone())
            .map_err(|_| WorkContractError::AmbiguousAssistantOutput)?;
        if block.r#type != "text" {
            return Err(WorkContractError::AmbiguousAssistantOutput);
        }
        text.push_str(&block.text);
    }
    if text.trim().is_empty() {
        return Err(WorkContractError::AmbiguousAssistantOutput);
    }
    Ok(text)
}

fn validate_identity(identity: &ExactIdentity) -> Result<(), WorkContractError> {
    validate_identity_part("identity package", &identity.package)?;
    validate_identity_part("identity name", &identity.name)?;
    validate_identity_part("identity version", &identity.version)
}

fn validate_identity_part(field: &'static str, value: &str) -> Result<(), WorkContractError> {
    validate_bounded(field, value, MAX_IDENTITY_PART_BYTES)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(WorkContractError::InvalidIdentityPart { field });
    }
    Ok(())
}

fn validate_bounded(
    field: &'static str,
    value: &str,
    limit: usize,
) -> Result<(), WorkContractError> {
    if value.trim().is_empty() {
        return Err(WorkContractError::Empty(field));
    }
    if value.len() > limit {
        return Err(WorkContractError::TooLong { field, limit });
    }
    Ok(())
}

fn validate_count(field: &'static str, count: usize) -> Result<(), WorkContractError> {
    if count > MAX_FACTS {
        return Err(WorkContractError::TooMany {
            field,
            limit: MAX_FACTS,
        });
    }
    Ok(())
}

fn validate_sha256(value: &str) -> Result<(), WorkContractError> {
    let Some(hex) = value.strip_prefix(SHA256_PREFIX) else {
        return Err(WorkContractError::InvalidFactId(value.to_owned()));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(WorkContractError::InvalidFactId(value.to_owned()));
    }
    Ok(())
}

fn body_digest(body: &CapabilityWorkBody) -> Result<String, WorkContractError> {
    canonical_digest(body)
}

fn canonical_digest(value: &impl Serialize) -> Result<String, WorkContractError> {
    let bytes = serde_json_canonicalizer::to_vec(value)
        .map_err(|error| WorkContractError::Serialization(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(SHA256_PREFIX.len() + digest.len() * 2);
    output.push_str(SHA256_PREFIX);
    for byte in digest {
        let _ = write!(output, "{byte:02x}");
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fact(name: &str) -> ExactIdentity {
        ExactIdentity::new("test.fact", name, "1.0.0")
    }

    fn request(coverage: FactCoverage) -> CapabilityWorkRequest {
        let input = fact("input");
        CapabilityWorkRequest::bind(CapabilityWorkBody {
            capability: ExactIdentity::new("test.capability", "generate", "1.0.0"),
            requires: vec![FactRequirement {
                fact: input.clone(),
                acceptance: FactAcceptance::CompleteOnly,
            }],
            inputs: vec![BoundFact {
                id: format!("sha256:{}", "a".repeat(64)),
                fact_type: input,
                coverage,
                payload: json!({"unknown_extension": {"survives": true}}),
                derivation: json!({"kind": "external", "opaque": [1, 2, 3]}),
            }],
            produces: vec![fact("output")],
            conformance_suite: "test.conformance/generate@1.0.0".to_owned(),
        })
        .expect("valid work request")
    }

    #[test]
    fn exact_body_is_bound_to_the_request_identity() {
        let request = request(FactCoverage::Complete);
        request.validate().expect("request validates");
        let encoded = serde_json::to_value(&request).expect("encode request");
        assert_eq!(
            encoded["inputs"][0]["payload"]["unknown_extension"]["survives"],
            true
        );
        assert_eq!(encoded["inputs"][0]["derivation"]["opaque"][2], 3);
    }

    #[test]
    fn changed_bound_input_invalidates_the_request_identity() {
        let mut request = request(FactCoverage::Complete);
        request.body.inputs[0].payload = json!({"changed": true});
        assert!(matches!(
            request.validate(),
            Err(WorkContractError::IdentityMismatch { .. })
        ));
    }

    #[test]
    fn complete_only_requirement_rejects_partial_input() {
        let mut body = request(FactCoverage::Complete).body;
        body.inputs[0].coverage = FactCoverage::Partial;
        assert!(matches!(
            CapabilityWorkRequest::bind(body),
            Err(WorkContractError::PartialInputRejected { .. })
        ));
    }

    #[test]
    fn accepts_the_exact_request_emitted_by_gooir() {
        let request: CapabilityWorkRequest = serde_json::from_str(include_str!(
            "../tests/fixtures/gooir_runnable_web_request.json"
        ))
        .expect("GOOIR request decodes");

        request.validate().expect("GOOIR request validates");
        assert_eq!(
            request.body.capability,
            ExactIdentity::new(
                "dev.fleetd.capability",
                "generate_runnable_web_surface",
                "0.1.0"
            )
        );
        assert_eq!(request.body.inputs.len(), 1);
        assert_eq!(request.body.inputs[0].payload["selector_field"], "block_id");
    }

    #[test]
    fn unknown_contract_fields_fail_closed() {
        let request = request(FactCoverage::Complete);
        let mut encoded = serde_json::to_value(request).expect("encode request");
        encoded["unknown_contract_field"] = json!(true);

        assert!(serde_json::from_value::<CapabilityWorkRequest>(encoded).is_err());
    }

    fn provider(request: &CapabilityWorkRequest) -> CapabilityProviderDescriptor {
        CapabilityProviderDescriptor {
            id: ExactIdentity::new("test.provider", "agent", "1.0.0"),
            capability: request.body.capability.clone(),
            implementation_digest: format!("sha256:{}", "b".repeat(64)),
        }
    }

    fn attempt_payload(request: &CapabilityWorkRequest, response: &Value) -> Value {
        json!({
            "status": "completed",
            "invocation_id": "invocation-1",
            "stop_reason": "end_turn",
            "output_complete": true,
            "assistant_messages": [{
                "message_id": null,
                "content": [{"type": "text", "text": serde_json::to_string(&response).unwrap()}],
                "complete": true,
                "first_event_seq": 1,
                "last_event_seq": 1
            }],
            "usage": {},
            "session_persistence": "runtime_claimed",
            "result_context": capability_attempt_context(request, &provider(request)).unwrap()
        })
    }

    #[test]
    fn exact_attempt_is_lifted_to_an_unverified_candidate() {
        let request = request(FactCoverage::Complete);
        let response = json!({
            "request_id": request.request_id,
            "status": "candidate",
            "outputs": [{
                "fact_type": request.body.produces[0],
                "coverage": "complete",
                "payload": {"artifact": "candidate"}
            }],
            "conformance_suite": request.body.conformance_suite,
            "conformance_status": "unverified",
            "diagnostics": []
        });
        let payload = attempt_payload(&request, &response);

        let projection =
            extract_capability_attempt(&request, "test.fleetd/worker@1", "attempt-1", &payload)
                .expect("lift exact candidate");
        let CapabilityAttemptProjection::Candidate(candidate) = projection else {
            panic!("candidate response must lift to candidate")
        };
        candidate.validate(&request).expect("candidate validates");
        assert!(candidate.candidate_id.starts_with(SHA256_PREFIX));
        assert_eq!(candidate.body.provider, provider(&request));
        assert_eq!(candidate.body.outputs[0].payload["artifact"], "candidate");
        assert_eq!(candidate.body.attempt.invocation_id, "invocation-1");
    }

    #[test]
    fn prose_or_markdown_cannot_be_mistaken_for_a_candidate() {
        let request = request(FactCoverage::Complete);
        let response = json!({
            "request_id": request.request_id,
            "status": "unable",
            "outputs": [],
            "conformance_suite": request.body.conformance_suite,
            "conformance_status": "unverified",
            "diagnostics": ["not implemented"]
        });
        let mut payload = attempt_payload(&request, &response);
        let text = payload["assistant_messages"][0]["content"][0]["text"]
            .as_str()
            .unwrap()
            .to_owned();
        payload["assistant_messages"][0]["content"][0]["text"] =
            json!(format!("```json\n{text}\n```"));

        assert!(matches!(
            extract_capability_attempt(&request, "fleetd", "attempt-1", &payload),
            Err(WorkContractError::MalformedProviderResponse(_))
        ));
    }

    #[test]
    fn unable_attempt_remains_explicit_and_produces_no_candidate() {
        let request = request(FactCoverage::Complete);
        let response = json!({
            "request_id": request.request_id,
            "status": "unable",
            "outputs": [],
            "conformance_suite": request.body.conformance_suite,
            "conformance_status": "unverified",
            "diagnostics": ["workspace authority was insufficient"]
        });
        let payload = attempt_payload(&request, &response);

        let projection =
            extract_capability_attempt(&request, "fleetd", "attempt-1", &payload).unwrap();
        let CapabilityAttemptProjection::Unable(unable) = projection else {
            panic!("unable response must not become candidate")
        };
        assert_eq!(
            unable.diagnostics,
            vec!["workspace authority was insufficient"]
        );
    }

    #[test]
    fn checked_in_gooir_attempt_message_has_the_cross_repository_candidate_identity() {
        let request: CapabilityWorkRequest = serde_json::from_str(include_str!(
            "../tests/fixtures/gooir_runnable_web_request.json"
        ))
        .unwrap();
        let attempt: Message = serde_json::from_str(include_str!(
            "../tests/fixtures/gooir_runnable_web_attempt_message.json"
        ))
        .unwrap();

        let projection = extract_capability_message(&request, &attempt).unwrap();
        let CapabilityAttemptProjection::Candidate(candidate) = projection else {
            panic!("checked-in attempt is a candidate")
        };
        assert_eq!(
            candidate.candidate_id,
            "sha256:a2262fbc6ce8af0f59b33c0ec67af7cec2398670b1c7ebb837ab8d256beb802e"
        );

        let mut wrong_envelope = attempt;
        wrong_envelope.correlation_id = Some("different-request".to_owned());
        assert!(matches!(
            extract_capability_message(&request, &wrong_envelope),
            Err(WorkContractError::AttemptCorrelationMismatch)
        ));
    }
}
