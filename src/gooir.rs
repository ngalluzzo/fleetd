//! Neutral GOOIR documents consumed and produced by the execution host.
//!
//! These types implement the versioned wire contract. Runtime ownership,
//! scheduling, sessions, leases, and process policy remain in fleetd records
//! and never become fields of the semantic invocation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::Message;

pub const CAPABILITY_OFFERS_PROTOCOL: &str = "org.gooi.capability.offers/v1";
pub const CAPABILITY_INVOCATION_PROTOCOL: &str = "org.gooi.capability.invocation/v1";
pub const CAPABILITY_RESULT_PROTOCOL: &str = "org.gooi.capability.result/v1";
pub const CAPABILITY_CANDIDATE_PROTOCOL: &str = "org.gooi.capability.candidate/v1";
pub const CAPABILITY_INVOCATION_KIND: &str = "gooir.capability.invocation/v1";
pub const CAPABILITY_RESULT_KIND: &str = "gooir.capability.result/v1";

const SHA256_PREFIX: &str = "sha256:";
const MAX_IDENTITY_PART_BYTES: usize = 256;
const MAX_FACTS: usize = 64;

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

    /// Validates every exact identity component.
    ///
    /// # Errors
    ///
    /// Returns [`GooirError::InvalidIdentity`] for an empty or oversized part.
    pub fn validate(&self) -> Result<(), GooirError> {
        for (label, part) in [
            ("package", self.package.as_str()),
            ("name", self.name.as_str()),
            ("version", self.version.as_str()),
        ] {
            if part.trim().is_empty() || part.len() > MAX_IDENTITY_PART_BYTES {
                return Err(GooirError::InvalidIdentity(format!(
                    "{label} must contain between 1 and {MAX_IDENTITY_PART_BYTES} bytes"
                )));
            }
        }
        Ok(())
    }
}

impl std::fmt::Display for ExactIdentity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}@{}", self.package, self.name, self.version)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CapabilityOffer {
    pub implementation: ExactIdentity,
    pub capability: ExactIdentity,
    pub implementation_digest: String,
}

impl CapabilityOffer {
    /// Validates the implementation, capability, and artifact digest.
    ///
    /// # Errors
    ///
    /// Returns an identity or digest error when the offer is malformed.
    pub fn validate(&self) -> Result<(), GooirError> {
        self.implementation.validate()?;
        self.capability.validate()?;
        validate_sha256(&self.implementation_digest)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilityOfferSet {
    pub protocol: String,
    pub package: ExactIdentity,
    pub offers: Vec<CapabilityOffer>,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, Value>,
}

impl CapabilityOfferSet {
    /// Validates the protocol, package, and unique exact offers.
    ///
    /// # Errors
    ///
    /// Returns a protocol, identity, digest, or offer-set error.
    pub fn validate(&self) -> Result<(), GooirError> {
        if self.protocol != CAPABILITY_OFFERS_PROTOCOL {
            return Err(GooirError::ProtocolMismatch {
                expected: CAPABILITY_OFFERS_PROTOCOL.to_owned(),
                actual: self.protocol.clone(),
            });
        }
        self.package.validate()?;
        if self.offers.is_empty() {
            return Err(GooirError::InvalidOfferSet(
                "a package must offer at least one capability".to_owned(),
            ));
        }
        let mut seen = BTreeSet::new();
        for offer in &self.offers {
            offer.validate()?;
            if !seen.insert((offer.implementation.clone(), offer.capability.clone())) {
                return Err(GooirError::InvalidOfferSet(
                    "duplicate implementation offer".to_owned(),
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn offers(&self, capability: &ExactIdentity) -> bool {
        self.offers
            .iter()
            .any(|offer| &offer.capability == capability)
    }
}

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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FactCoverage {
    Complete,
    Partial,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BoundFact {
    pub id: String,
    pub fact_type: ExactIdentity,
    pub coverage: FactCoverage,
    pub payload: Value,
    pub derivation: Value,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CapabilityInvocationBody {
    pub capability: ExactIdentity,
    pub requires: Vec<FactRequirement>,
    pub inputs: Vec<BoundFact>,
    pub produces: Vec<ExactIdentity>,
    pub conformance_suite: String,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CapabilityInvocation {
    pub protocol: String,
    pub invocation_id: String,
    #[serde(flatten)]
    pub body: CapabilityInvocationBody,
}

impl CapabilityInvocation {
    /// Revalidates the complete invocation and its content identity.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed facts, protocol, or canonical identity.
    pub fn validate(&self) -> Result<(), GooirError> {
        if self.protocol != CAPABILITY_INVOCATION_PROTOCOL {
            return Err(GooirError::ProtocolMismatch {
                expected: CAPABILITY_INVOCATION_PROTOCOL.to_owned(),
                actual: self.protocol.clone(),
            });
        }
        validate_invocation_body(&self.body)?;
        let expected = canonical_digest(&self.body)?;
        if self.invocation_id != expected {
            return Err(GooirError::IdentityMismatch {
                expected,
                actual: self.invocation_id.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ProducedFact {
    pub fact_type: ExactIdentity,
    pub coverage: FactCoverage,
    pub payload: Value,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CapabilityResult {
    pub protocol: String,
    pub invocation_id: Option<String>,
    #[serde(default)]
    pub outputs: Vec<ProducedFact>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, Value>,
}

impl CapabilityResult {
    /// Validates a result against the exact invocation it claims to satisfy.
    ///
    /// # Errors
    ///
    /// Returns an error for a mismatch, unable result, or invalid output set.
    pub fn validate(&self, invocation: &CapabilityInvocation) -> Result<(), GooirError> {
        invocation.validate()?;
        if self.protocol != CAPABILITY_RESULT_PROTOCOL {
            return Err(GooirError::ProtocolMismatch {
                expected: CAPABILITY_RESULT_PROTOCOL.to_owned(),
                actual: self.protocol.clone(),
            });
        }
        if self.invocation_id.as_ref() != Some(&invocation.invocation_id) {
            return Err(GooirError::ResultInvocationMismatch);
        }
        match (self.outputs.is_empty(), self.error.as_deref()) {
            (false, None) => {}
            (true, Some(error)) if !error.trim().is_empty() => {
                return Err(GooirError::Unable(error.to_owned()));
            }
            _ => {
                return Err(GooirError::InvalidResult(
                    "result must contain outputs or one non-empty error".to_owned(),
                ));
            }
        }
        validate_outputs(&invocation.body.produces, &self.outputs)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct EvidenceRef {
    pub evidence_type: ExactIdentity,
    pub digest: String,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, Value>,
}

impl EvidenceRef {
    /// Validates the exact evidence vocabulary and content digest.
    ///
    /// # Errors
    ///
    /// Returns an identity or digest error when the reference is malformed.
    pub fn validate(&self) -> Result<(), GooirError> {
        self.evidence_type.validate()?;
        validate_sha256(&self.digest)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CapabilityCandidateBody {
    pub invocation_id: String,
    pub offer: CapabilityOffer,
    pub outputs: Vec<ProducedFact>,
    pub evidence: Vec<EvidenceRef>,
    #[serde(default, flatten)]
    pub extensions: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct CapabilityCandidate {
    pub protocol: String,
    pub candidate_id: String,
    #[serde(flatten)]
    pub body: CapabilityCandidateBody,
}

impl CapabilityCandidate {
    /// Binds claimed outputs and execution-host evidence to an exact offer.
    ///
    /// # Errors
    ///
    /// Returns an error when invocation, offer, outputs, or evidence mismatch.
    pub fn bind(
        invocation: &CapabilityInvocation,
        offer: CapabilityOffer,
        outputs: Vec<ProducedFact>,
        evidence: Vec<EvidenceRef>,
    ) -> Result<Self, GooirError> {
        invocation.validate()?;
        let body = CapabilityCandidateBody {
            invocation_id: invocation.invocation_id.clone(),
            offer,
            outputs,
            evidence,
            extensions: BTreeMap::new(),
        };
        validate_candidate_body(invocation, &body)?;
        let candidate_id = canonical_digest(&body)?;
        Ok(Self {
            protocol: CAPABILITY_CANDIDATE_PROTOCOL.to_owned(),
            candidate_id,
            body,
        })
    }

    /// Revalidates a candidate and its content identity.
    ///
    /// # Errors
    ///
    /// Returns an error when any candidate binding or digest is invalid.
    pub fn validate(&self, invocation: &CapabilityInvocation) -> Result<(), GooirError> {
        if self.protocol != CAPABILITY_CANDIDATE_PROTOCOL {
            return Err(GooirError::ProtocolMismatch {
                expected: CAPABILITY_CANDIDATE_PROTOCOL.to_owned(),
                actual: self.protocol.clone(),
            });
        }
        validate_candidate_body(invocation, &self.body)?;
        let expected = canonical_digest(&self.body)?;
        if self.candidate_id != expected {
            return Err(GooirError::IdentityMismatch {
                expected,
                actual: self.candidate_id.clone(),
            });
        }
        Ok(())
    }
}

/// Produces an unverified GOOIR candidate from immutable Fleetd evidence.
///
/// # Errors
///
/// Returns an error for a wrong kind or correlation, invalid result, offer,
/// output set, evidence reference, or content identity.
pub fn candidate_from_result_message(
    invocation: &CapabilityInvocation,
    offer: CapabilityOffer,
    message: &Message,
) -> Result<CapabilityCandidate, GooirError> {
    if message.kind != CAPABILITY_RESULT_KIND {
        return Err(GooirError::WrongMessageKind(message.kind.clone()));
    }
    if message.correlation_id.as_deref() != Some(&invocation.invocation_id) {
        return Err(GooirError::MessageCorrelationMismatch);
    }
    let result: CapabilityResult = serde_json::from_value(message.payload.clone())?;
    result.validate(invocation)?;
    CapabilityCandidate::bind(
        invocation,
        offer,
        result.outputs,
        vec![durable_message_evidence(message)?],
    )
}

/// Produces a typed evidence reference for one immutable Fleetd message.
///
/// # Errors
///
/// Returns an error when canonical message serialization fails.
pub fn durable_message_evidence(message: &Message) -> Result<EvidenceRef, GooirError> {
    Ok(EvidenceRef {
        evidence_type: ExactIdentity::new("dev.fleetd.evidence", "durable_message", "1.0.0"),
        digest: canonical_digest(message)?,
        extensions: BTreeMap::from([
            ("message_id".to_owned(), Value::String(message.id.clone())),
            ("sequence".to_owned(), Value::from(message.seq)),
        ]),
    })
}

fn validate_invocation_body(body: &CapabilityInvocationBody) -> Result<(), GooirError> {
    body.capability.validate()?;
    if body.conformance_suite.trim().is_empty() {
        return Err(GooirError::InvalidInvocation(
            "conformance suite is empty".to_owned(),
        ));
    }
    if body.produces.is_empty() || body.produces.len() > MAX_FACTS {
        return Err(GooirError::InvalidInvocation(
            "produced fact set is empty or exceeds the bound".to_owned(),
        ));
    }
    let mut required = BTreeMap::new();
    for requirement in &body.requires {
        requirement.fact.validate()?;
        if required
            .insert(requirement.fact.clone(), requirement)
            .is_some()
        {
            return Err(GooirError::InvalidInvocation(
                "duplicate required fact".to_owned(),
            ));
        }
    }
    let mut inputs = BTreeMap::new();
    for input in &body.inputs {
        input.fact_type.validate()?;
        validate_sha256(&input.id)?;
        let encoded = serde_json::to_vec(&(
            &input.fact_type,
            input.coverage,
            &input.payload,
            &input.derivation,
        ))?;
        let expected = format!("{SHA256_PREFIX}{:x}", Sha256::digest(encoded));
        if input.id != expected {
            return Err(GooirError::IdentityMismatch {
                expected,
                actual: input.id.clone(),
            });
        }
        if inputs.insert(input.fact_type.clone(), input).is_some() {
            return Err(GooirError::InvalidInvocation(
                "duplicate input fact".to_owned(),
            ));
        }
    }
    for (fact, requirement) in required {
        let input = inputs.remove(&fact).ok_or_else(|| {
            GooirError::InvalidInvocation(format!("required input {fact} is absent"))
        })?;
        if requirement.acceptance == FactAcceptance::CompleteOnly
            && input.coverage == FactCoverage::Partial
        {
            return Err(GooirError::InvalidInvocation(format!(
                "required input {fact} is partial"
            )));
        }
    }
    if !inputs.is_empty() {
        return Err(GooirError::InvalidInvocation(
            "invocation contains an undeclared input".to_owned(),
        ));
    }
    let mut produced = BTreeSet::new();
    for fact in &body.produces {
        fact.validate()?;
        if !produced.insert(fact) {
            return Err(GooirError::InvalidInvocation(
                "duplicate produced fact".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_candidate_body(
    invocation: &CapabilityInvocation,
    body: &CapabilityCandidateBody,
) -> Result<(), GooirError> {
    invocation.validate()?;
    if body.invocation_id != invocation.invocation_id {
        return Err(GooirError::ResultInvocationMismatch);
    }
    body.offer.validate()?;
    if body.offer.capability != invocation.body.capability {
        return Err(GooirError::OfferCapabilityMismatch);
    }
    validate_outputs(&invocation.body.produces, &body.outputs)?;
    if body.evidence.is_empty() {
        return Err(GooirError::InvalidEvidence(
            "candidate has no evidence".to_owned(),
        ));
    }
    for evidence in &body.evidence {
        evidence.validate()?;
    }
    Ok(())
}

fn validate_outputs(
    expected: &[ExactIdentity],
    outputs: &[ProducedFact],
) -> Result<(), GooirError> {
    let expected = expected.iter().collect::<BTreeSet<_>>();
    let actual = outputs
        .iter()
        .map(|output| &output.fact_type)
        .collect::<BTreeSet<_>>();
    if outputs.len() != actual.len() || actual != expected {
        return Err(GooirError::OutputContractViolation);
    }
    Ok(())
}

fn canonical_digest(value: &impl Serialize) -> Result<String, GooirError> {
    let bytes = serde_json_canonicalizer::to_vec(value)
        .map_err(|error| GooirError::Canonicalization(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{SHA256_PREFIX}{digest:x}"))
}

fn validate_sha256(value: &str) -> Result<(), GooirError> {
    let valid = value.strip_prefix(SHA256_PREFIX).is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    });
    if valid {
        Ok(())
    } else {
        Err(GooirError::InvalidDigest(value.to_owned()))
    }
}

#[derive(Debug, Error)]
pub enum GooirError {
    #[error("GOOIR protocol mismatch: expected {expected}, received {actual}")]
    ProtocolMismatch { expected: String, actual: String },
    #[error("invalid GOOIR identity: {0}")]
    InvalidIdentity(String),
    #[error("invalid capability offer set: {0}")]
    InvalidOfferSet(String),
    #[error("invalid capability invocation: {0}")]
    InvalidInvocation(String),
    #[error("content identity mismatch: expected {expected}, received {actual}")]
    IdentityMismatch { expected: String, actual: String },
    #[error("result does not bind the exact invocation")]
    ResultInvocationMismatch,
    #[error("implementation offer does not implement the invocation capability")]
    OfferCapabilityMismatch,
    #[error("result output set does not match the invocation")]
    OutputContractViolation,
    #[error("invalid capability result: {0}")]
    InvalidResult(String),
    #[error("capability implementation was unable to produce a result: {0}")]
    Unable(String),
    #[error("invalid candidate evidence: {0}")]
    InvalidEvidence(String),
    #[error("expected a GOOIR capability result message, received {0}")]
    WrongMessageKind(String),
    #[error("GOOIR result message correlation does not bind the exact invocation")]
    MessageCorrelationMismatch,
    #[error("invalid SHA-256 identity: {0}")]
    InvalidDigest(String),
    #[error("canonical JSON failed: {0}")]
    Canonicalization(String),
    #[error("GOOIR JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn invocation() -> CapabilityInvocation {
        let body = CapabilityInvocationBody {
            capability: ExactIdentity::new("test.capability", "render", "1.0.0"),
            requires: Vec::new(),
            inputs: Vec::new(),
            produces: vec![ExactIdentity::new("test.fact", "artifact", "1.0.0")],
            conformance_suite: "test.conformance/render@1.0.0".to_owned(),
            extensions: BTreeMap::from([("dialect_hint".to_owned(), json!({"opaque": true}))]),
        };
        CapabilityInvocation {
            protocol: CAPABILITY_INVOCATION_PROTOCOL.to_owned(),
            invocation_id: canonical_digest(&body).expect("digest invocation body"),
            body,
        }
    }

    #[test]
    fn one_plugin_package_offers_several_exact_capabilities() {
        let offers = CapabilityOfferSet {
            protocol: CAPABILITY_OFFERS_PROTOCOL.to_owned(),
            package: ExactIdentity::new("test.plugin", "package", "1.0.0"),
            offers: vec![
                CapabilityOffer {
                    implementation: ExactIdentity::new("test.plugin", "inspect", "1.0.0"),
                    capability: ExactIdentity::new("test.capability", "inspect", "1.0.0"),
                    implementation_digest: format!("sha256:{}", "a".repeat(64)),
                },
                CapabilityOffer {
                    implementation: ExactIdentity::new("test.plugin", "render", "1.0.0"),
                    capability: ExactIdentity::new("test.capability", "render", "1.0.0"),
                    implementation_digest: format!("sha256:{}", "b".repeat(64)),
                },
            ],
            extensions: BTreeMap::from([("host_hint".to_owned(), json!("preserved"))]),
        };

        offers.validate().expect("validate package offers");
        let roundtrip: CapabilityOfferSet =
            serde_json::from_value(serde_json::to_value(&offers).expect("encode package offers"))
                .expect("decode package offers");
        assert_eq!(roundtrip, offers);
    }

    #[test]
    fn durable_result_message_becomes_unverified_candidate() {
        let invocation = invocation();
        invocation.validate().expect("validate invocation");
        let offer = CapabilityOffer {
            implementation: ExactIdentity::new("test.plugin", "renderer", "1.0.0"),
            capability: invocation.body.capability.clone(),
            implementation_digest: format!("sha256:{}", "c".repeat(64)),
        };
        let result = CapabilityResult {
            protocol: CAPABILITY_RESULT_PROTOCOL.to_owned(),
            invocation_id: Some(invocation.invocation_id.clone()),
            outputs: vec![ProducedFact {
                fact_type: invocation.body.produces[0].clone(),
                coverage: FactCoverage::Complete,
                payload: json!({"artifact": "exact"}),
                extensions: BTreeMap::from([("implementation_note".to_owned(), json!(7))]),
            }],
            error: None,
            extensions: BTreeMap::from([("result_note".to_owned(), json!("opaque"))]),
        };
        let message = Message {
            seq: 42,
            id: "message-42".to_owned(),
            channel_id: "channel".to_owned(),
            sender_id: "implementation-agent".to_owned(),
            recipient_id: Some("requester-agent".to_owned()),
            kind: CAPABILITY_RESULT_KIND.to_owned(),
            payload: serde_json::to_value(result).expect("encode result"),
            correlation_id: Some(invocation.invocation_id.clone()),
            causation_id: Some("source-message".to_owned()),
            created_at_ms: 1,
        };

        let candidate = candidate_from_result_message(&invocation, offer.clone(), &message)
            .expect("bind candidate");
        candidate.validate(&invocation).expect("validate candidate");
        assert_eq!(candidate.body.offer, offer);
        assert_eq!(
            candidate.body.outputs[0].extensions["implementation_note"],
            7
        );
        assert_eq!(
            candidate.body.evidence[0].extensions["message_id"],
            "message-42"
        );
        assert_eq!(candidate.body.evidence[0].extensions["sequence"], 42);
    }
}
