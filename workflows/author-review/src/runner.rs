//! Credential-owning external runner for the draft author-review plugin.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use fleetd::{
    AckDelivery, BlockDelivery, ChannelMember, ClaimBatch, ClaimDeliveries, Delivery, Message,
    MessagePage, RetryDelivery, SendMessage,
};
use reqwest::{Client, Method, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use tokio::{
    io::AsyncWriteExt,
    process::{Child, ChildStdin, Command},
    time::timeout,
};
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};

use crate::protocol::{
    DescribeResult, EVENT_KINDS, EvaluateParams, EvaluateResult, INTERFACE_ID, INTERFACE_VERSION,
    MAX_FRAME_BYTES, MAX_HISTORY_MESSAGES, MAX_PROPOSALS, PLUGIN_ID, PLUGIN_VERSION, RpcRequest,
    RpcResponse, WorkflowMember, WorkflowMessage,
};

const CONFIG_SCHEMA_VERSION: u32 = 1;
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_ARGS: usize = 32;
const MAX_ARG_BYTES: usize = 4096;
const MAX_PAYLOAD_BYTES: usize = 256 * 1024;
const HISTORY_PAGE_SIZE: u32 = 500;
const MAX_RETRY_DELAY_MS: u64 = 86_400_000;
const FLEETD_REQUEST_TIMEOUT_MS: u64 = 10_000;
const MAX_PLUGIN_REQUEST_TIMEOUT_MS: u64 = 60_000;
const RETRY_SCHEDULING_MARGIN_MS: u64 = 1_000;
const DEFAULT_RETRY_BASE_DELAY_MS: u64 =
    FLEETD_REQUEST_TIMEOUT_MS + MAX_PLUGIN_REQUEST_TIMEOUT_MS + RETRY_SCHEDULING_MARGIN_MS;
const DEFAULT_RETRY_MAX_DELAY_MS: u64 = 300_000;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerConfiguration {
    pub schema_version: u32,
    pub fleetd: FleetdEndpoint,
    pub plugin: WorkflowPluginSpec,
    pub plugin_configuration: Value,
    pub lease_duration_ms: u64,
    pub poll_interval_ms: u64,
    #[serde(default = "default_retry_base_delay_ms")]
    pub retry_base_delay_ms: u64,
    #[serde(default = "default_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FleetdEndpoint {
    pub origin: String,
    pub agent_id: String,
    pub credential_file: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowPluginSpec {
    pub executable: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    pub request_timeout_ms: u64,
}

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error(
        "runner configuration phase failed: {0}; recovery: correct the configuration before restarting the runner"
    )]
    Configuration(String),
    #[error(
        "runner file-read phase failed for {path}: {source}; recovery: restore the file with the required owner-only access"
    )]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(
        "runner configuration JSON phase failed at line {line}, column {column}; recovery: provide one valid bounded JSON object"
    )]
    ConfigurationJson { line: usize, column: usize },
    #[error(
        "Fleetd {phase} phase is unavailable: {diagnostic}; recovery: retry with bounded backoff; any active lease releases on expiry"
    )]
    FleetdUnavailable {
        phase: &'static str,
        diagnostic: &'static str,
    },
    #[error(
        "Fleetd {phase} phase returned HTTP {status}; recovery: retry transient statuses with bounded backoff, otherwise verify runner authority and inspect the exact delivery state"
    )]
    FleetdRejected {
        phase: &'static str,
        status: StatusCode,
    },
    #[error(
        "Fleetd {phase} phase returned an invalid typed response; recovery: verify Fleetd compatibility, then inspect the exact delivery state"
    )]
    FleetdProtocol { phase: &'static str },
    #[error(
        "plugin {phase} phase is unavailable: {diagnostic}; recovery: discard the child and retry the delivery after bounded backoff"
    )]
    PluginUnavailable {
        phase: &'static str,
        diagnostic: String,
    },
    #[error(
        "plugin {phase} phase has a permanent {kind} failure: {diagnostic}; recovery: fix or replace the plugin, then requeue the exact blocked delivery"
    )]
    PluginPermanent {
        phase: &'static str,
        kind: &'static str,
        diagnostic: String,
    },
    #[error(
        "plugin evaluation phase permanently rejected the input with RPC code {code}; recovery: correct the input or plugin configuration, then requeue the exact blocked delivery"
    )]
    PluginRejected { code: i32 },
    #[error(
        "plugin semantic-validation phase failed permanently: {0}; recovery: fix the deterministic proposal, then requeue the exact blocked delivery"
    )]
    InvalidProposal(String),
    #[error(
        "Fleetd publication phase detected a divergent replay for the derived idempotency identity; recovery: inspect the existing causal effect and fix the plugin before requeueing the exact blocked delivery"
    )]
    ProposalConflict,
}

impl RunnerError {
    fn requires_block(&self) -> bool {
        matches!(
            self,
            Self::PluginPermanent { .. }
                | Self::PluginRejected { .. }
                | Self::InvalidProposal(_)
                | Self::ProposalConflict
                | Self::FleetdProtocol { .. }
        ) || matches!(
            self,
            Self::FleetdRejected { status, .. } if !transient_http_status(*status)
        )
    }

    fn is_plugin_fault(&self) -> bool {
        matches!(
            self,
            Self::PluginUnavailable { .. }
                | Self::PluginPermanent { .. }
                | Self::PluginRejected { .. }
                | Self::InvalidProposal(_)
                | Self::ProposalConflict
        )
    }

    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::FleetdUnavailable { .. } | Self::PluginUnavailable { .. }
        ) || matches!(
            self,
            Self::FleetdRejected { status, .. } if transient_http_status(*status)
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TickOutcome {
    Idle,
    Acknowledged,
    Retried {
        retry_after_ms: u64,
        diagnostic: String,
    },
    Blocked {
        diagnostic: String,
    },
}

pub struct AuthorReviewRunner {
    configuration: RunnerConfiguration,
    fleetd: FleetdClient,
    plugin: Option<WorkflowPluginClient>,
}

impl AuthorReviewRunner {
    /// Creates the external runner and verifies the child plugin's exact draft
    /// description before any work can be leased.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, credentials, endpoint, or
    /// plugin readiness.
    pub async fn start(configuration: RunnerConfiguration) -> Result<Self, RunnerError> {
        validate_configuration(&configuration)?;
        let token = read_private_credential(&configuration.fleetd.credential_file)?;
        let fleetd = FleetdClient::new(&configuration.fleetd.origin, token)?;
        let plugin = WorkflowPluginClient::spawn(&configuration.plugin).await?;
        Ok(Self {
            configuration,
            fleetd,
            plugin: Some(plugin),
        })
    }

    /// Claims at most one input and drives it through publish plus settlement.
    /// Returns `false` when the inbox was empty.
    ///
    /// # Errors
    ///
    /// Returns an error only when work could not be safely settled for retry or
    /// block. Per-input failures are otherwise durably settled and reported as
    /// processed.
    pub async fn tick(&mut self) -> Result<TickOutcome, RunnerError> {
        self.ensure_plugin().await?;
        let batch = self
            .fleetd
            .claim(
                &self.configuration.fleetd.agent_id,
                self.configuration.lease_duration_ms,
            )
            .await?;
        let Some(delivery) = batch.deliveries.first() else {
            return Ok(TickOutcome::Idle);
        };
        let result = self.evaluate_and_publish(delivery).await;
        match result {
            Ok(_) => {
                self.fleetd
                    .ack(
                        &self.configuration.fleetd.agent_id,
                        &delivery.message.id,
                        &batch.lease_token,
                    )
                    .await?;
                Ok(TickOutcome::Acknowledged)
            }
            Err(error) if error.requires_block() => {
                if error.is_plugin_fault() {
                    self.plugin = None;
                }
                let diagnostic = bounded_reason(&error.to_string());
                self.fleetd
                    .block(
                        &self.configuration.fleetd.agent_id,
                        &delivery.message.id,
                        &batch.lease_token,
                        &diagnostic,
                    )
                    .await?;
                Ok(TickOutcome::Blocked { diagnostic })
            }
            Err(error) if error.is_transient() => {
                if error.is_plugin_fault() {
                    self.plugin = None;
                }
                let diagnostic = bounded_reason(&error.to_string());
                let retry_after_ms = self.retry_delay_for_attempt(delivery.attempt);
                self.fleetd
                    .retry(
                        &self.configuration.fleetd.agent_id,
                        &delivery.message.id,
                        &batch.lease_token,
                        &diagnostic,
                        retry_after_ms,
                    )
                    .await?;
                Ok(TickOutcome::Retried {
                    retry_after_ms,
                    diagnostic,
                })
            }
            Err(error) => Err(error),
        }
    }

    /// Evaluates and publishes every deterministic proposal but deliberately
    /// does not acknowledge the lease. This separation is the crash window:
    /// replaying the delivery resends the same idempotency identities safely.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid context, plugin failure, or Fleetd publish
    /// failure.
    pub async fn evaluate_and_publish(
        &mut self,
        delivery: &Delivery,
    ) -> Result<EvaluateResult, RunnerError> {
        let workflow_id = delivery.message.correlation_id.clone().ok_or_else(|| {
            RunnerError::InvalidProposal(
                "workflow inputs require a durable correlation_id".to_owned(),
            )
        })?;
        let history = self
            .fleetd
            .workflow_history(&delivery.message.channel_id, &workflow_id)
            .await?;
        let members = self.fleetd.members(&delivery.message.channel_id).await?;
        let params = EvaluateParams {
            configuration: self.configuration.plugin_configuration.clone(),
            runner_agent_id: self.configuration.fleetd.agent_id.clone(),
            workflow_id,
            input: workflow_message(&delivery.message),
            history: history.iter().map(workflow_message).collect(),
            members: members.iter().map(workflow_member).collect(),
        };
        self.ensure_plugin().await?;
        let Some(plugin) = self.plugin.as_mut() else {
            return Err(RunnerError::PluginUnavailable {
                phase: "evaluation",
                diagnostic: "replacement child was unavailable after a successful probe".to_owned(),
            });
        };
        let description = plugin.description.clone();
        let evaluated = match plugin.evaluate(&params).await {
            Ok(evaluated) => evaluated,
            Err(error) => {
                if error.is_plugin_fault() {
                    self.plugin = None;
                }
                return Err(error);
            }
        };
        if let Err(error) = validate_proposals(&params, &evaluated, &description) {
            self.plugin = None;
            return Err(error);
        }
        for proposal in &evaluated.proposals {
            let idempotency_key =
                format!("workflow/{}/{}", delivery.message.id, proposal.operation_id);
            if idempotency_key.len() > 256 {
                return Err(RunnerError::InvalidProposal(
                    "derived idempotency key exceeds Fleetd's bound".to_owned(),
                ));
            }
            let send = SendMessage {
                idempotency_key: Some(idempotency_key),
                recipient_id: Some(proposal.recipient_id.clone()),
                kind: proposal.kind.clone(),
                payload: proposal.payload.clone(),
                correlation_id: delivery.message.correlation_id.clone(),
                causation_id: Some(delivery.message.id.clone()),
            };
            match self.fleetd.send(&delivery.message.channel_id, &send).await {
                Ok(_) => {}
                Err(RunnerError::FleetdRejected { status, .. })
                    if status == StatusCode::CONFLICT =>
                {
                    return Err(RunnerError::ProposalConflict);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(evaluated)
    }

    #[must_use]
    pub const fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.configuration.poll_interval_ms)
    }

    #[must_use]
    pub fn retry_delay_for_attempt(&self, attempt: i64) -> u64 {
        retry_delay(
            self.configuration.retry_base_delay_ms,
            self.configuration.retry_max_delay_ms,
            attempt,
        )
    }

    async fn ensure_plugin(&mut self) -> Result<(), RunnerError> {
        if self.plugin.is_none() {
            self.plugin = Some(WorkflowPluginClient::spawn(&self.configuration.plugin).await?);
        }
        Ok(())
    }
}

/// Loads and validates one bounded credential-free runner configuration.
///
/// # Errors
///
/// Returns an error for inaccessible, oversized, malformed, or invalid input.
pub fn load_configuration(path: &Path) -> Result<RunnerConfiguration, RunnerError> {
    let bytes = std::fs::read(path).map_err(|source| RunnerError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.len() > MAX_CONFIG_BYTES {
        return Err(RunnerError::Configuration(format!(
            "configuration exceeds {MAX_CONFIG_BYTES} bytes"
        )));
    }
    let configuration: RunnerConfiguration =
        serde_json::from_slice(&bytes).map_err(|error| RunnerError::ConfigurationJson {
            line: error.line(),
            column: error.column(),
        })?;
    validate_configuration(&configuration)?;
    Ok(configuration)
}

fn validate_configuration(configuration: &RunnerConfiguration) -> Result<(), RunnerError> {
    if configuration.schema_version != CONFIG_SCHEMA_VERSION {
        return Err(RunnerError::Configuration(format!(
            "schema_version must equal {CONFIG_SCHEMA_VERSION}"
        )));
    }
    validate_agent_id(&configuration.fleetd.agent_id)?;
    validate_origin(&configuration.fleetd.origin)?;
    if !configuration.fleetd.credential_file.is_absolute() {
        return Err(RunnerError::Configuration(
            "credential_file must be absolute".to_owned(),
        ));
    }
    if !configuration.plugin.executable.is_absolute() || !configuration.plugin.executable.is_file()
    {
        return Err(RunnerError::Configuration(
            "plugin executable must be an existing absolute file".to_owned(),
        ));
    }
    if configuration.plugin.args.len() > MAX_ARGS
        || configuration
            .plugin
            .args
            .iter()
            .any(|argument| argument.len() > MAX_ARG_BYTES)
    {
        return Err(RunnerError::Configuration(format!(
            "plugin args must contain at most {MAX_ARGS} values of at most {MAX_ARG_BYTES} bytes"
        )));
    }
    if !(100..=MAX_PLUGIN_REQUEST_TIMEOUT_MS).contains(&configuration.plugin.request_timeout_ms) {
        return Err(RunnerError::Configuration(format!(
            "plugin request_timeout_ms must be between 100 and {MAX_PLUGIN_REQUEST_TIMEOUT_MS}"
        )));
    }
    if configuration.lease_duration_ms
        < configuration
            .plugin
            .request_timeout_ms
            .saturating_add(10_000)
        || configuration.lease_duration_ms > 3_600_000
    {
        return Err(RunnerError::Configuration(
            "lease_duration_ms must cover plugin timeout plus 10 seconds and not exceed one hour"
                .to_owned(),
        ));
    }
    if !(100..=60_000).contains(&configuration.poll_interval_ms) {
        return Err(RunnerError::Configuration(
            "poll_interval_ms must be between 100 and 60000".to_owned(),
        ));
    }
    let minimum_retry_base_delay_ms = minimum_retry_base_delay_ms(configuration);
    if configuration.retry_base_delay_ms < minimum_retry_base_delay_ms
        || configuration.retry_base_delay_ms > MAX_RETRY_DELAY_MS
    {
        return Err(RunnerError::Configuration(format!(
            "retry_base_delay_ms must be between {minimum_retry_base_delay_ms} and {MAX_RETRY_DELAY_MS} for the configured plugin timeout"
        )));
    }
    if configuration.retry_max_delay_ms < configuration.retry_base_delay_ms
        || configuration.retry_max_delay_ms > MAX_RETRY_DELAY_MS
    {
        return Err(RunnerError::Configuration(format!(
            "retry_max_delay_ms must be at least retry_base_delay_ms and not exceed {MAX_RETRY_DELAY_MS}"
        )));
    }
    Ok(())
}

const fn default_retry_base_delay_ms() -> u64 {
    DEFAULT_RETRY_BASE_DELAY_MS
}

const fn default_retry_max_delay_ms() -> u64 {
    DEFAULT_RETRY_MAX_DELAY_MS
}

const fn minimum_retry_base_delay_ms(configuration: &RunnerConfiguration) -> u64 {
    FLEETD_REQUEST_TIMEOUT_MS
        .saturating_add(configuration.plugin.request_timeout_ms)
        .saturating_add(RETRY_SCHEDULING_MARGIN_MS)
}

fn retry_delay(base_delay_ms: u64, max_delay_ms: u64, attempt: i64) -> u64 {
    let exponent = u32::try_from(attempt.max(1).saturating_sub(1)).unwrap_or(u32::MAX);
    base_delay_ms
        .saturating_mul(1_u64 << exponent.min(63))
        .min(max_delay_ms)
}

fn validate_origin(value: &str) -> Result<Url, RunnerError> {
    let origin = Url::parse(value)
        .map_err(|error| RunnerError::Configuration(format!("invalid Fleetd origin: {error}")))?;
    if origin.scheme() != "http"
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
        || !origin
            .host_str()
            .and_then(|host| host.parse::<std::net::IpAddr>().ok())
            .is_some_and(|address| address.is_loopback())
        || origin.port().is_none()
    {
        return Err(RunnerError::Configuration(
            "Fleetd origin must be an explicit loopback HTTP IP and port".to_owned(),
        ));
    }
    Ok(origin)
}

fn validate_agent_id(value: &str) -> Result<(), RunnerError> {
    if value.trim().is_empty() || value.len() > 128 {
        return Err(RunnerError::Configuration(
            "agent_id must contain between 1 and 128 bytes".to_owned(),
        ));
    }
    Ok(())
}

fn read_private_credential(path: &Path) -> Result<String, RunnerError> {
    let metadata = std::fs::symlink_metadata(path).map_err(|source| RunnerError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(RunnerError::Configuration(
            "credential_file must be a regular non-symlink file".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(RunnerError::Configuration(
                "credential_file must not be accessible by group or other users".to_owned(),
            ));
        }
    }
    let token = std::fs::read_to_string(path).map_err(|source| RunnerError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let token = token.trim().to_owned();
    if token.is_empty() || token.len() > 4096 || token.chars().any(char::is_whitespace) {
        return Err(RunnerError::Configuration(
            "credential_file contains an invalid token".to_owned(),
        ));
    }
    Ok(token)
}

struct WorkflowPluginClient {
    child: Child,
    stdin: ChildStdin,
    stdout: FramedRead<tokio::process::ChildStdout, LinesCodec>,
    next_request_id: u64,
    request_timeout: Duration,
    description: DescribeResult,
}

impl WorkflowPluginClient {
    async fn spawn(spec: &WorkflowPluginSpec) -> Result<Self, RunnerError> {
        let mut command = Command::new(&spec.executable);
        command
            .args(&spec.args)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| RunnerError::PluginUnavailable {
                phase: "launch",
                diagnostic: io_diagnostic(&error),
            })?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| RunnerError::PluginUnavailable {
                phase: "launch",
                diagnostic: "child stdin was unavailable".to_owned(),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| RunnerError::PluginUnavailable {
                phase: "launch",
                diagnostic: "child stdout was unavailable".to_owned(),
            })?;
        let mut client = Self {
            child,
            stdin,
            stdout: FramedRead::new(stdout, LinesCodec::new_with_max_length(MAX_FRAME_BYTES)),
            next_request_id: 1,
            request_timeout: Duration::from_millis(spec.request_timeout_ms),
            description: DescribeResult {
                interface_id: String::new(),
                interface_version: String::new(),
                plugin_id: String::new(),
                plugin_version: String::new(),
                roles: Vec::new(),
                event_schemas: Vec::new(),
            },
        };
        let description: DescribeResult = client
            .call("workflow.describe", Value::Object(serde_json::Map::new()))
            .await?;
        validate_description(&description)?;
        client.description = description;
        Ok(client)
    }

    async fn evaluate(&mut self, params: &EvaluateParams) -> Result<EvaluateResult, RunnerError> {
        let params = serde_json::to_value(params).map_err(|_| RunnerError::PluginPermanent {
            phase: "evaluation request",
            kind: "encoding",
            diagnostic: "runner could not encode the bounded typed request".to_owned(),
        })?;
        self.call("workflow.evaluate", params).await
    }

    async fn call<T: DeserializeOwned>(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<T, RunnerError> {
        use futures_util::StreamExt as _;

        let id = self.next_request_id;
        self.next_request_id = self.next_request_id.saturating_add(1);
        let request = RpcRequest {
            jsonrpc: "2.0".to_owned(),
            id,
            method: method.to_owned(),
            params,
        };
        let mut encoded =
            serde_json::to_vec(&request).map_err(|_| RunnerError::PluginPermanent {
                phase: "request framing",
                kind: "encoding",
                diagnostic: "runner could not encode the typed JSON-RPC request".to_owned(),
            })?;
        if encoded.len() > MAX_FRAME_BYTES {
            return Err(RunnerError::PluginPermanent {
                phase: "request framing",
                kind: "framing",
                diagnostic: format!("request exceeds the {MAX_FRAME_BYTES}-byte frame bound"),
            });
        }
        encoded.push(b'\n');
        timeout(self.request_timeout, self.stdin.write_all(&encoded))
            .await
            .map_err(|_| RunnerError::PluginUnavailable {
                phase: "request write",
                diagnostic: "request write timed out".to_owned(),
            })?
            .map_err(|error| RunnerError::PluginUnavailable {
                phase: "request write",
                diagnostic: io_diagnostic(&error),
            })?;
        let line = timeout(self.request_timeout, self.stdout.next())
            .await
            .map_err(|_| RunnerError::PluginUnavailable {
                phase: "response read",
                diagnostic: "response read timed out".to_owned(),
            })?
            .ok_or_else(|| RunnerError::PluginUnavailable {
                phase: "response read",
                diagnostic: "child closed stdout before responding".to_owned(),
            })?
            .map_err(|error| match error {
                LinesCodecError::MaxLineLengthExceeded => RunnerError::PluginPermanent {
                    phase: "response framing",
                    kind: "framing",
                    diagnostic: format!("response exceeds the {MAX_FRAME_BYTES}-byte frame bound"),
                },
                LinesCodecError::Io(error) => RunnerError::PluginUnavailable {
                    phase: "response read",
                    diagnostic: io_diagnostic(&error),
                },
            })?;
        let response: RpcResponse =
            serde_json::from_str(&line).map_err(|error| RunnerError::PluginPermanent {
                phase: "response decoding",
                kind: "protocol decoding",
                diagnostic: format!(
                    "response is not the typed JSON-RPC envelope at line {}, column {}",
                    error.line(),
                    error.column()
                ),
            })?;
        if response.jsonrpc != "2.0" || response.id != id {
            return Err(RunnerError::PluginPermanent {
                phase: "response identity",
                kind: "protocol identity",
                diagnostic: "response version or request ID did not match the request".to_owned(),
            });
        }
        let result = match (response.result, response.error) {
            (Some(result), None) => result,
            (None, Some(error)) => return Err(RunnerError::PluginRejected { code: error.code }),
            _ => {
                return Err(RunnerError::PluginPermanent {
                    phase: "response decoding",
                    kind: "protocol decoding",
                    diagnostic: "response must contain exactly one of result or error".to_owned(),
                });
            }
        };
        serde_json::from_value(result).map_err(|_| RunnerError::PluginPermanent {
            phase: "response decoding",
            kind: "result decoding",
            diagnostic: "response result does not match the method's typed schema".to_owned(),
        })
    }
}

impl Drop for WorkflowPluginClient {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn validate_description(description: &DescribeResult) -> Result<(), RunnerError> {
    if description.interface_id != INTERFACE_ID
        || description.interface_version != INTERFACE_VERSION
        || description.plugin_id != PLUGIN_ID
        || description.plugin_version != PLUGIN_VERSION
    {
        return Err(RunnerError::PluginPermanent {
            phase: "description identity",
            kind: "identity",
            diagnostic: format!(
                "description must identify {INTERFACE_ID}@{INTERFACE_VERSION} and {PLUGIN_ID}@{PLUGIN_VERSION}"
            ),
        });
    }
    let kinds = description
        .event_schemas
        .iter()
        .map(|contract| contract.kind.as_str())
        .collect::<HashSet<_>>();
    let expected_kinds = EVENT_KINDS.into_iter().collect::<HashSet<_>>();
    let roles = description
        .roles
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let expected_roles = ["coordinator", "author", "reviewer"]
        .into_iter()
        .collect::<HashSet<_>>();
    if kinds != expected_kinds
        || roles != expected_roles
        || description.event_schemas.len() != EVENT_KINDS.len()
        || description
            .event_schemas
            .iter()
            .any(|contract| !contract.schema.is_object())
    {
        return Err(RunnerError::PluginPermanent {
            phase: "description validation",
            kind: "semantic validation",
            diagnostic: "description has an incomplete, duplicate, or invalid vocabulary"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_proposals(
    params: &EvaluateParams,
    evaluated: &EvaluateResult,
    description: &DescribeResult,
) -> Result<(), RunnerError> {
    if evaluated.proposals.len() > MAX_PROPOSALS {
        return Err(RunnerError::InvalidProposal(format!(
            "plugin exceeded {MAX_PROPOSALS} proposals"
        )));
    }
    let members = params
        .members
        .iter()
        .map(|member| member.agent_id.as_str())
        .collect::<HashSet<_>>();
    let kinds = description
        .event_schemas
        .iter()
        .map(|contract| contract.kind.as_str())
        .collect::<HashSet<_>>();
    let mut operation_ids = HashSet::new();
    for proposal in &evaluated.proposals {
        if proposal.operation_id.trim().is_empty()
            || proposal.operation_id.len() > 180
            || !operation_ids.insert(proposal.operation_id.as_str())
        {
            return Err(RunnerError::InvalidProposal(
                "operation IDs must be unique, non-empty, and bounded".to_owned(),
            ));
        }
        if !members.contains(proposal.recipient_id.as_str()) {
            return Err(RunnerError::InvalidProposal(format!(
                "recipient {} is not a channel member",
                proposal.recipient_id
            )));
        }
        if !kinds.contains(proposal.kind.as_str()) {
            return Err(RunnerError::InvalidProposal(format!(
                "kind {} was not declared by workflow.describe",
                proposal.kind
            )));
        }
        let payload_bytes = serde_json::to_vec(&proposal.payload)
            .map_err(|_| {
                RunnerError::InvalidProposal(
                    "proposal payload could not be encoded as bounded JSON".to_owned(),
                )
            })?
            .len();
        if payload_bytes > MAX_PAYLOAD_BYTES {
            return Err(RunnerError::InvalidProposal(format!(
                "proposal payload exceeds {MAX_PAYLOAD_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

struct FleetdClient {
    origin: Url,
    token: String,
    http: Client,
}

impl FleetdClient {
    fn new(origin: &str, token: String) -> Result<Self, RunnerError> {
        Ok(Self {
            origin: validate_origin(origin)?,
            token,
            http: Client::builder()
                .timeout(Duration::from_millis(FLEETD_REQUEST_TIMEOUT_MS))
                .build()
                .map_err(|_| {
                    RunnerError::Configuration(
                        "could not construct the bounded Fleetd HTTP client".to_owned(),
                    )
                })?,
        })
    }

    async fn claim(
        &self,
        agent_id: &str,
        lease_duration_ms: u64,
    ) -> Result<ClaimBatch, RunnerError> {
        self.json(
            Method::POST,
            &["v1", "agents", agent_id, "deliveries", "claim"],
            Some(&ClaimDeliveries {
                limit: 1,
                lease_duration_ms,
            }),
            "claim",
        )
        .await
    }

    async fn ack(
        &self,
        agent_id: &str,
        message_id: &str,
        lease_token: &str,
    ) -> Result<(), RunnerError> {
        self.empty(
            Method::POST,
            &["v1", "agents", agent_id, "deliveries", message_id, "ack"],
            Some(&AckDelivery {
                lease_token: lease_token.to_owned(),
            }),
            "acknowledgement settlement",
        )
        .await
    }

    async fn retry(
        &self,
        agent_id: &str,
        message_id: &str,
        lease_token: &str,
        reason: &str,
        retry_after_ms: u64,
    ) -> Result<(), RunnerError> {
        self.empty(
            Method::POST,
            &["v1", "agents", agent_id, "deliveries", message_id, "retry"],
            Some(&RetryDelivery {
                lease_token: lease_token.to_owned(),
                retry_after_ms,
                error: Some(reason.to_owned()),
            }),
            "retry settlement",
        )
        .await
    }

    async fn block(
        &self,
        agent_id: &str,
        message_id: &str,
        lease_token: &str,
        reason: &str,
    ) -> Result<(), RunnerError> {
        let _: Value = self
            .json(
                Method::POST,
                &["v1", "agents", agent_id, "deliveries", message_id, "block"],
                Some(&BlockDelivery {
                    lease_token: lease_token.to_owned(),
                    reason: reason.to_owned(),
                }),
                "block settlement",
            )
            .await?;
        Ok(())
    }

    async fn send(&self, channel_id: &str, input: &SendMessage) -> Result<Message, RunnerError> {
        self.json(
            Method::POST,
            &["v1", "channels", channel_id, "messages"],
            Some(input),
            "publication",
        )
        .await
    }

    async fn members(&self, channel_id: &str) -> Result<Vec<ChannelMember>, RunnerError> {
        self.json::<(), Vec<ChannelMember>>(
            Method::GET,
            &["v1", "channels", channel_id, "members"],
            None,
            "membership read",
        )
        .await
    }

    async fn workflow_history(
        &self,
        channel_id: &str,
        workflow_id: &str,
    ) -> Result<Vec<Message>, RunnerError> {
        let mut cursor = 0;
        let mut history = Vec::new();
        loop {
            let mut url = endpoint(&self.origin, &["v1", "channels", channel_id, "messages"])?;
            url.query_pairs_mut()
                .append_pair("after", &cursor.to_string())
                .append_pair("limit", &HISTORY_PAGE_SIZE.to_string());
            let response = self
                .http
                .get(url)
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|error| fleetd_transport("history read", &error))?;
            let page: MessagePage = decode_response(response, "history read").await?;
            let count = page.messages.len();
            for message in page.messages {
                cursor = message.seq;
                if message.correlation_id.as_deref() == Some(workflow_id) {
                    history.push(message);
                    if history.len() > MAX_HISTORY_MESSAGES {
                        return Err(RunnerError::InvalidProposal(format!(
                            "workflow history exceeds {MAX_HISTORY_MESSAGES} messages"
                        )));
                    }
                }
            }
            if count < HISTORY_PAGE_SIZE as usize {
                break;
            }
        }
        Ok(history)
    }

    async fn json<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<&B>,
        phase: &'static str,
    ) -> Result<T, RunnerError> {
        let url = endpoint(&self.origin, segments)?;
        let mut request = self.http.request(method, url).bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| fleetd_transport(phase, &error))?;
        decode_response(response, phase).await
    }

    async fn empty<B: Serialize + ?Sized>(
        &self,
        method: Method,
        segments: &[&str],
        body: Option<&B>,
        phase: &'static str,
    ) -> Result<(), RunnerError> {
        let url = endpoint(&self.origin, segments)?;
        let mut request = self.http.request(method, url).bearer_auth(&self.token);
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| fleetd_transport(phase, &error))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(http_error(&response, phase))
        }
    }
}

impl Drop for FleetdClient {
    fn drop(&mut self) {
        self.token.clear();
    }
}

async fn decode_response<T: DeserializeOwned>(
    response: reqwest::Response,
    phase: &'static str,
) -> Result<T, RunnerError> {
    if response.status().is_success() {
        return response.json().await.map_err(|error| {
            if error.is_decode() {
                RunnerError::FleetdProtocol { phase }
            } else {
                fleetd_transport(phase, &error)
            }
        });
    }
    Err(http_error(&response, phase))
}

fn http_error(response: &reqwest::Response, phase: &'static str) -> RunnerError {
    RunnerError::FleetdRejected {
        phase,
        status: response.status(),
    }
}

fn fleetd_transport(phase: &'static str, error: &reqwest::Error) -> RunnerError {
    let diagnostic = if error.is_timeout() {
        "request timed out before a complete response"
    } else if error.is_connect() {
        "connection could not be established"
    } else if error.is_body() {
        "response body was interrupted"
    } else {
        "request failed before a complete response"
    };
    RunnerError::FleetdUnavailable { phase, diagnostic }
}

fn transient_http_status(status: StatusCode) -> bool {
    status.is_server_error()
        || matches!(
            status,
            StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_EARLY | StatusCode::TOO_MANY_REQUESTS
        )
}

fn io_diagnostic(error: &std::io::Error) -> String {
    format!("operating-system I/O failed with {:?}", error.kind())
}

fn endpoint(origin: &Url, segments: &[&str]) -> Result<Url, RunnerError> {
    let mut url = origin.clone();
    {
        let mut path = url.path_segments_mut().map_err(|()| {
            RunnerError::Configuration("Fleetd origin cannot accept path segments".to_owned())
        })?;
        path.clear();
        for segment in segments {
            path.push(segment);
        }
    }
    Ok(url)
}

fn workflow_message(message: &Message) -> WorkflowMessage {
    WorkflowMessage {
        seq: message.seq,
        id: message.id.clone(),
        channel_id: message.channel_id.clone(),
        sender_id: message.sender_id.clone(),
        recipient_id: message.recipient_id.clone(),
        kind: message.kind.clone(),
        payload: message.payload.clone(),
        correlation_id: message.correlation_id.clone(),
        causation_id: message.causation_id.clone(),
        created_at_ms: message.created_at_ms,
    }
}

fn workflow_member(member: &ChannelMember) -> WorkflowMember {
    WorkflowMember {
        agent_id: member.agent_id.clone(),
        agent_name: member.agent_name.clone(),
        delivery_mode: match member.delivery_mode {
            fleetd::MembershipDeliveryMode::Inbox => "inbox",
            fleetd::MembershipDeliveryMode::StreamOnly => "stream_only",
        }
        .to_owned(),
        joined_at_ms: member.joined_at_ms,
    }
}

fn bounded_reason(value: &str) -> String {
    let mut reason = value.to_owned();
    if reason.len() > 4096 {
        let mut end = 4096;
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        reason.truncate(end);
    }
    reason
}
