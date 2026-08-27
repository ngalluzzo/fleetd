//! The validated egress contract for one worker seat.
//!
//! Validation lives here rather than in the command surface for the same reason
//! `InboundAcceptance` validates itself: the rules belong beside the mechanism
//! that has to honour them, and a surface should be able to parse a file
//! without also owning what a legal collector endpoint is.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    time::Duration,
};

use fleetd_proto::operations::EventClass;

/// The schema version this build accepts.
///
/// It versions the semantic-convention mapping, not the worker file. Every
/// `gen_ai.*` attribute is still in development upstream, so when that mapping
/// changes this number moves and the worker file does not.
pub const EGRESS_SCHEMA_VERSION: u32 = 1;

const MAX_ATTRIBUTE_BYTES: usize = 65_536;
const MAX_QUEUE_CAPACITY: usize = 65_536;
const MAX_EXPORT_TIMEOUT_MS: u64 = 30_000;
const MAX_FLUSH_MS: u64 = 30_000;
const MAX_RESOURCE_ATTRIBUTES: usize = 32;
const MAX_RESOURCE_BYTES: usize = 256;

/// Attribute keys the sink sets itself. An operator key that collides is
/// refused rather than merged: two sources for one key is a silent winner.
const RESERVED_RESOURCE_KEYS: [&str; 2] = ["service.name", "fleetd.agent_id"];

/// How much of an observed update may leave the process.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentLevel {
    /// Timing, ordering, and counts only. Nothing is lifted out of the update.
    None,
    /// The shape of the work: tool kind, call id, status, plan size, stop
    /// reason. Never model or user text, and never a tool's agent-authored
    /// title, which can name a path or quote a request.
    Metadata,
    /// Assistant text, reasoning, the tool title, tool arguments, and tool
    /// output, each truncated to the configured attribute bound.
    Full,
}

impl ContentLevel {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    pub const ALL: [Self; 3] = [Self::None, Self::Metadata, Self::Full];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Metadata => "metadata",
            Self::Full => "full",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.iter().copied().find(|it| it.as_str() == value)
    }
}

/// Everything the sink needs, already checked.
#[derive(Clone, Debug)]
pub struct EgressConfig {
    pub endpoint: String,
    pub headers: BTreeMap<String, String>,
    pub content: ContentLevel,
    pub classifications: BTreeSet<&'static str>,
    pub resource_attributes: BTreeMap<String, String>,
    pub max_attribute_bytes: usize,
    pub queue_capacity: usize,
    pub export_timeout: Duration,
    pub shutdown_flush: Duration,
    pub agent_id: String,
}

/// What an operator wrote, before any of it is known to be usable.
///
/// The surface fills this in from its own deserialised shape and calls
/// [`EgressRequest::validate`]. Keeping the unchecked form separate is what
/// makes "validated before a plugin starts" a type rather than a convention.
#[derive(Clone, Debug)]
pub struct EgressRequest {
    pub schema_version: u32,
    pub kind: String,
    pub endpoint: String,
    pub headers_file: Option<String>,
    pub content: Option<String>,
    pub classifications: Option<Vec<String>>,
    pub resource_attributes: BTreeMap<String, String>,
    pub max_attribute_bytes: Option<usize>,
    pub queue_capacity: Option<usize>,
    pub export_timeout_ms: Option<u64>,
    pub shutdown_flush_ms: Option<u64>,
    pub agent_id: String,
}

impl EgressRequest {
    /// Checks every rule the contract states, before a plugin starts.
    ///
    /// # Errors
    ///
    /// Returns a bounded diagnostic naming the one field that is wrong. A
    /// malformed block is a configuration mistake, so it fails the seat rather
    /// than starting one that silently exports nothing.
    pub fn validate(self) -> Result<EgressConfig, String> {
        if self.schema_version != EGRESS_SCHEMA_VERSION {
            return Err(format!(
                "unsupported egress schema version {}; expected {EGRESS_SCHEMA_VERSION}",
                self.schema_version
            ));
        }
        if self.kind != "otlp_http" {
            return Err(format!(
                "unsupported egress kind `{}`; expected `otlp_http`",
                self.kind
            ));
        }
        validate_endpoint(&self.endpoint)?;
        let headers = match &self.headers_file {
            Some(path) => read_headers_file(Path::new(path))?,
            None => BTreeMap::new(),
        };
        let content = match &self.content {
            Some(level) => ContentLevel::parse(level)
                .ok_or_else(|| format!("unknown egress content level `{level}`"))?,
            None => ContentLevel::Metadata,
        };
        let classifications = match &self.classifications {
            Some(names) => validate_classifications(names)?,
            None => EventClass::ALL.iter().map(|it| it.as_str()).collect(),
        };
        validate_resource_attributes(&self.resource_attributes)?;

        Ok(EgressConfig {
            endpoint: self.endpoint,
            headers,
            content,
            classifications,
            resource_attributes: self.resource_attributes,
            max_attribute_bytes: bounded_usize(
                self.max_attribute_bytes,
                4_096,
                1,
                MAX_ATTRIBUTE_BYTES,
                "max_attribute_bytes",
            )?,
            queue_capacity: bounded_usize(
                self.queue_capacity,
                1_024,
                1,
                MAX_QUEUE_CAPACITY,
                "queue_capacity",
            )?,
            export_timeout: Duration::from_millis(bounded_u64(
                self.export_timeout_ms,
                5_000,
                1,
                MAX_EXPORT_TIMEOUT_MS,
                "export_timeout_ms",
            )?),
            shutdown_flush: Duration::from_millis(bounded_u64(
                self.shutdown_flush_ms,
                2_000,
                0,
                MAX_FLUSH_MS,
                "shutdown_flush_ms",
            )?),
            agent_id: self.agent_id,
        })
    }
}

/// Refuses an endpoint that would carry reasoning somewhere Fleetd has not
/// earned the right to send it.
///
/// A loopback collector needs no transport security because it never leaves the
/// machine. Anything else must be `https`, which is the same posture that keeps
/// Fleetd's own listeners on loopback until encrypted transport exists.
fn validate_endpoint(endpoint: &str) -> Result<(), String> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .ok_or_else(|| "egress endpoint must be an absolute http or https URL".to_owned())?;
    if scheme != "http" && scheme != "https" {
        return Err(format!("unsupported egress endpoint scheme `{scheme}`"));
    }
    if rest.contains('@') {
        return Err("egress endpoint must not embed credentials".to_owned());
    }
    if rest.contains('?') || rest.contains('#') {
        return Err("egress endpoint must not carry a query or fragment".to_owned());
    }
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.is_empty() {
        return Err("egress endpoint must name a host".to_owned());
    }
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host);
    let loopback = host == "127.0.0.1" || host == "localhost" || host == "[::1]";
    if !loopback && scheme != "https" {
        return Err(format!(
            "egress endpoint host `{host}` is not loopback, so its scheme must be https"
        ));
    }
    Ok(())
}

/// Reads `Name: value` pairs from a file whose mode says only its owner can.
///
/// Stricter than the token files Fleetd writes itself, where
/// `auth::secure_file_permissions` sets the mode on creation. This file arrives
/// from the operator, so its mode is evidence rather than something Fleetd
/// established.
fn read_headers_file(path: &Path) -> Result<BTreeMap<String, String>, String> {
    if !path.is_absolute() {
        return Err("egress headers_file must be an absolute path".to_owned());
    }
    let metadata = std::fs::metadata(path)
        .map_err(|error| format!("egress headers_file is unreadable: {error}"))?;
    verify_owner_only(&metadata)?;
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("egress headers_file is unreadable: {error}"))?;
    let mut headers = BTreeMap::new();
    for (index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (name, value) = line.split_once(':').ok_or_else(|| {
            format!(
                "egress headers_file line {} is not `Name: value`",
                index.saturating_add(1)
            )
        })?;
        let name = name.trim();
        if name.is_empty() {
            return Err(format!(
                "egress headers_file line {} has an empty header name",
                index.saturating_add(1)
            ));
        }
        if headers
            .insert(name.to_owned(), value.trim().to_owned())
            .is_some()
        {
            return Err(format!("egress headers_file repeats header `{name}`"));
        }
    }
    Ok(headers)
}

#[cfg(unix)]
fn verify_owner_only(metadata: &std::fs::Metadata) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(format!(
            "egress headers_file mode is {mode:04o}; it carries a collector credential and must \
             be owner-only"
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_owner_only(_metadata: &std::fs::Metadata) -> Result<(), String> {
    Err("egress headers_file permissions cannot be verified on this platform".to_owned())
}

/// Resolves operator-selected classifications against the counter vocabulary.
///
/// The names are the ones `InvocationEventCounts` reports, so an operator
/// selects by what they already read rather than by a wire spelling.
fn validate_classifications(names: &[String]) -> Result<BTreeSet<&'static str>, String> {
    if names.is_empty() {
        return Err("egress classifications must not be empty; omit the field for all".to_owned());
    }
    let mut selected = BTreeSet::new();
    for name in names {
        let class = EventClass::ALL
            .iter()
            .find(|class| class.as_str() == name.as_str())
            .ok_or_else(|| format!("unknown egress classification `{name}`"))?;
        if !selected.insert(class.as_str()) {
            return Err(format!("duplicate egress classification `{name}`"));
        }
    }
    Ok(selected)
}

fn validate_resource_attributes(attributes: &BTreeMap<String, String>) -> Result<(), String> {
    if attributes.len() > MAX_RESOURCE_ATTRIBUTES {
        return Err(format!(
            "egress resource_attributes exceeds {MAX_RESOURCE_ATTRIBUTES} entries"
        ));
    }
    for (key, value) in attributes {
        if key.trim().is_empty() {
            return Err("egress resource_attributes contains an empty key".to_owned());
        }
        if key.len() > MAX_RESOURCE_BYTES || value.len() > MAX_RESOURCE_BYTES {
            return Err(format!(
                "egress resource attribute `{key}` exceeds {MAX_RESOURCE_BYTES} bytes"
            ));
        }
        if RESERVED_RESOURCE_KEYS.contains(&key.as_str()) {
            return Err(format!(
                "egress resource attribute `{key}` is set by fleetd and cannot be overridden"
            ));
        }
    }
    Ok(())
}

/// Clamps nothing. An out-of-range bound is a mistake worth reporting, except
/// the upper limit on a size, where asking for more than the bound is a request
/// to be bounded.
fn bounded_usize(
    supplied: Option<usize>,
    default: usize,
    low: usize,
    high: usize,
    field: &str,
) -> Result<usize, String> {
    let value = supplied.unwrap_or(default);
    if value < low {
        return Err(format!("egress {field} must be at least {low}"));
    }
    Ok(value.min(high))
}

fn bounded_u64(
    supplied: Option<u64>,
    default: u64,
    low: u64,
    high: u64,
    field: &str,
) -> Result<u64, String> {
    let value = supplied.unwrap_or(default);
    if value < low {
        return Err(format!("egress {field} must be at least {low}"));
    }
    Ok(value.min(high))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use fleetd_proto::operations::EventClass;

    use super::{ContentLevel, EgressRequest, validate_endpoint};

    fn request() -> EgressRequest {
        EgressRequest {
            schema_version: 1,
            kind: "otlp_http".to_owned(),
            endpoint: "http://127.0.0.1:4318/v1/traces".to_owned(),
            headers_file: None,
            content: None,
            classifications: None,
            resource_attributes: BTreeMap::new(),
            max_attribute_bytes: None,
            queue_capacity: None,
            export_timeout_ms: None,
            shutdown_flush_ms: None,
            agent_id: "agent-1".to_owned(),
        }
    }

    #[test]
    fn defaults_carry_the_shape_of_the_work_and_no_text() {
        let config = request().validate().expect("valid egress");
        assert_eq!(config.content, ContentLevel::Metadata);
        assert_eq!(config.max_attribute_bytes, 4_096);
        assert_eq!(config.queue_capacity, 1_024);
        assert_eq!(
            config.classifications.len(),
            EventClass::ALL.len(),
            "the default allowlist is every class, so a new one is exported \
             unless an operator narrows it"
        );
    }

    #[test]
    fn a_future_schema_version_is_refused_rather_than_guessed() {
        let mut supplied = request();
        supplied.schema_version = 2;
        let error = supplied.validate().expect_err("refused");
        assert!(
            error.contains("unsupported egress schema version 2"),
            "{error}"
        );
    }

    #[test]
    fn a_remote_collector_must_be_encrypted_but_loopback_need_not_be() {
        validate_endpoint("http://127.0.0.1:4318/v1/traces").expect("loopback http");
        validate_endpoint("https://collector.example:4318/v1/traces").expect("remote https");
        let error = validate_endpoint("http://collector.example:4318/v1/traces")
            .expect_err("remote plaintext is refused");
        assert!(error.contains("must be https"), "{error}");
    }

    #[test]
    fn an_endpoint_may_not_carry_credentials_a_query_or_a_fragment() {
        for endpoint in [
            "https://user:secret@collector.example/v1/traces",
            "https://collector.example/v1/traces?token=secret",
            "https://collector.example/v1/traces#fragment",
        ] {
            validate_endpoint(endpoint).expect_err(endpoint);
        }
    }

    #[test]
    fn an_operator_cannot_overwrite_an_attribute_fleetd_sets() {
        let mut supplied = request();
        supplied
            .resource_attributes
            .insert("service.name".to_owned(), "not-fleetd".to_owned());
        let error = supplied.validate().expect_err("refused");
        assert!(error.contains("cannot be overridden"), "{error}");
    }

    #[test]
    fn classifications_are_selected_by_the_names_the_read_model_reports() {
        let mut supplied = request();
        supplied.classifications = Some(vec!["tool".to_owned(), "reasoning".to_owned()]);
        let config = supplied.validate().expect("valid selection");
        assert!(config.classifications.contains("tool"));
        assert!(!config.classifications.contains("assistant"));

        let mut wrong = request();
        wrong.classifications = Some(vec!["tool_call".to_owned()]);
        let error = wrong
            .validate()
            .expect_err("wire spelling is not the vocabulary");
        assert!(error.contains("unknown egress classification"), "{error}");
    }

    #[test]
    fn an_oversized_bound_is_clamped_but_a_zero_bound_is_refused() {
        let mut clamped = request();
        clamped.max_attribute_bytes = Some(usize::MAX);
        assert_eq!(
            clamped.validate().expect("clamped").max_attribute_bytes,
            65_536
        );

        let mut refused = request();
        refused.queue_capacity = Some(0);
        let error = refused.validate().expect_err("refused");
        assert!(
            error.contains("queue_capacity must be at least 1"),
            "{error}"
        );
    }
}
