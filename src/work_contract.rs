//! Versioned capability-work contracts interpreted above the message kernel.

use std::collections::BTreeSet;
use std::fmt::Write as _;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

pub const CAPABILITY_WORK_REQUEST_KIND: &str = "work.capability.request/v1";
pub const CAPABILITY_WORK_ATTEMPT_KIND: &str = "work.capability.attempt/v1";

const MAX_IDENTITY_PART_BYTES: usize = 256;
const MAX_FACTS: usize = 64;
const MAX_CONFORMANCE_SUITE_BYTES: usize = 512;
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
    #[error("work request could not be serialized: {0}")]
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
    let bytes = serde_json_canonicalizer::to_vec(body)
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
}
