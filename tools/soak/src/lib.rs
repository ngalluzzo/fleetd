//! Provider-neutral qualification driver for Fleetd's public API.

use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fleetd::{
    BlockedDelivery, InvocationObservation, Message, MessagePage, PluginGeneration, SendMessage,
    SessionBinding,
};
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

const PLAN_SCHEMA_VERSION: u32 = 1;
const REPORT_SCHEMA_VERSION: u32 = 1;
const MAX_PLAN_BYTES: usize = 1024 * 1024;
const MAX_IDENTIFIER_BYTES: usize = 256;
const MAX_WORKLOADS: usize = 10_000;
const MAX_OBSERVERS: usize = 64;
const MAX_HISTORY_PAGES: usize = 100;
const MAX_OBSERVER_BYTES: usize = 16 * 1024 * 1024;

/// Complete input for one unattended qualification run.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SoakPlan {
    pub schema_version: u32,
    pub run_id: String,
    pub fleetd: FleetdEndpoint,
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    #[serde(default)]
    pub observers: Vec<ObserverSpec>,
    pub workloads: Vec<WorkloadSpec>,
}

/// Fleetd endpoint and private credential-file locations.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FleetdEndpoint {
    pub server: String,
    pub operator_token_file: PathBuf,
    pub sender_token_file: PathBuf,
}

/// Credential-free loopback HTTP document captured without interpretation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSpec {
    pub id: String,
    pub url: String,
    #[serde(default = "default_true")]
    pub required: bool,
    #[serde(default = "default_observer_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_observer_max_bytes")]
    pub max_bytes: usize,
}

/// One exact message seed and its transport-level completion contract.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkloadSpec {
    pub id: String,
    pub seed: SeedSpec,
    pub completion: CompletionSpec,
}

/// Exact opaque Fleetd message to append.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SeedSpec {
    pub channel_id: String,
    pub recipient_id: String,
    pub kind: String,
    pub payload: Value,
    pub idempotency_key: String,
}

/// Observable transport facts required to finish one workload.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionSpec {
    pub kind: String,
    pub timeout_ms: u64,
    /// Exact agent order for terminal invocations causally descended from the seed.
    pub invocation_agents: Vec<String>,
}

/// Durable run artifact. It deliberately contains no credential values.
#[derive(Clone, Debug, Serialize)]
pub struct SoakReport {
    pub schema_version: u32,
    pub run_id: String,
    pub plan_sha256: String,
    pub fleetd_server: String,
    pub poll_interval_ms: u64,
    pub observers: Vec<ObserverSpec>,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub status: RunStatus,
    pub error: Option<String>,
    pub start: Option<EvidenceSnapshot>,
    pub finish: Option<EvidenceSnapshot>,
    pub workloads: Vec<WorkloadReport>,
}

/// Aggregate qualification state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Passed,
    Failed,
}

/// Evidence captured at a bounded point in the run.
#[derive(Clone, Debug, Serialize)]
pub struct EvidenceSnapshot {
    pub captured_at_ms: i64,
    pub fleetd: Option<FleetdSnapshot>,
    pub fleetd_error: Option<String>,
    pub observers: Vec<ObserverCapture>,
}

/// Public Fleetd operational read models.
#[derive(Clone, Debug, Serialize)]
pub struct FleetdSnapshot {
    pub plugin_generations: Vec<PluginGeneration>,
    pub session_bindings: Vec<SessionBinding>,
    pub invocation_observations: Vec<InvocationObservation>,
    pub unresolved_delivery_blocks: Vec<BlockedDelivery>,
}

/// One raw external observer capture.
#[derive(Clone, Debug, Serialize)]
pub struct ObserverCapture {
    pub id: String,
    pub url: String,
    pub required: bool,
    pub captured_at_ms: i64,
    pub document: Option<Value>,
    pub error: Option<String>,
}

/// Evidence and verdict for one exact workload.
#[derive(Clone, Debug, Serialize)]
pub struct WorkloadReport {
    pub id: String,
    pub declared: WorkloadSpec,
    pub started_at_ms: i64,
    pub finished_at_ms: i64,
    pub status: WorkloadStatus,
    pub error: Option<String>,
    pub seed: Option<Message>,
    pub completion: Option<Message>,
    pub causal_messages: Vec<Message>,
    pub invocation_observations: Vec<InvocationObservation>,
    pub before: EvidenceSnapshot,
    pub after: EvidenceSnapshot,
}

/// Transport-level workload verdict.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkloadStatus {
    Passed,
    Failed,
    TimedOut,
}

/// Plan or run failure.
#[derive(Debug, Error)]
pub enum SoakError {
    #[error("plan file is larger than {MAX_PLAN_BYTES} bytes")]
    PlanTooLarge,
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid plan JSON: {0}")]
    PlanJson(#[from] serde_json::Error),
    #[error("invalid plan: {0}")]
    InvalidPlan(String),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Fleetd returned HTTP {status}: {body}")]
    FleetdHttp { status: StatusCode, body: String },
    #[error("system clock is before the Unix epoch")]
    Clock,
}

#[derive(Clone)]
struct FleetdClients {
    base: Url,
    operator: AuthenticatedClient,
    sender: AuthenticatedClient,
    http: Client,
}

#[derive(Clone)]
struct AuthenticatedClient {
    base: Url,
    token: String,
    http: Client,
}

struct WorkloadState {
    completion: Option<Message>,
    causal_messages: Vec<Message>,
    observations: Vec<InvocationObservation>,
}

/// Reads, parses, validates, and hashes an exact plan file.
///
/// # Errors
///
/// Returns an error for inaccessible, oversized, malformed, or invalid plans.
pub fn load_plan(path: &Path) -> Result<(SoakPlan, String), SoakError> {
    let bytes = std::fs::read(path).map_err(|source| SoakError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.len() > MAX_PLAN_BYTES {
        return Err(SoakError::PlanTooLarge);
    }
    let digest = hex_digest(&bytes);
    let plan: SoakPlan = serde_json::from_slice(&bytes)?;
    validate_plan(&plan)?;
    Ok((plan, digest))
}

/// Executes all exact workloads sequentially and returns durable evidence.
///
/// # Errors
///
/// Returns an error only when credentials or endpoints cannot be initialized.
/// Runtime failures are retained in the returned report.
pub async fn execute_plan(plan: &SoakPlan, plan_sha256: String) -> Result<SoakReport, SoakError> {
    validate_plan(plan)?;
    let clients = FleetdClients::load(&plan.fleetd)?;
    let started_at_ms = now_ms()?;
    let mut report = SoakReport {
        schema_version: REPORT_SCHEMA_VERSION,
        run_id: plan.run_id.clone(),
        plan_sha256,
        fleetd_server: clients.base.to_string(),
        poll_interval_ms: plan.poll_interval_ms,
        observers: plan.observers.clone(),
        started_at_ms,
        finished_at_ms: started_at_ms,
        status: RunStatus::Passed,
        error: None,
        start: None,
        finish: None,
        workloads: Vec::with_capacity(plan.workloads.len()),
    };

    let start = clients.capture(&plan.observers).await?;
    let fleetd_failed = start.fleetd_error.is_some();
    let required_failed = required_observer_failed(&start.observers);
    report.start = Some(start);
    if fleetd_failed || required_failed {
        report.status = RunStatus::Failed;
        report.error = Some(if fleetd_failed {
            "initial Fleetd snapshot failed".to_owned()
        } else {
            "a required observer failed before dispatch".to_owned()
        });
    }

    if report.status == RunStatus::Passed {
        for workload in &plan.workloads {
            let workload_report = clients
                .run_workload(workload, &plan.observers, plan.poll_interval_ms)
                .await?;
            if workload_report.status != WorkloadStatus::Passed {
                report.status = RunStatus::Failed;
            }
            report.workloads.push(workload_report);
        }
    }

    let finish = clients.capture(&plan.observers).await?;
    if finish.fleetd_error.is_some() || required_observer_failed(&finish.observers) {
        report.status = RunStatus::Failed;
        report
            .error
            .get_or_insert_with(|| "final Fleetd or required observer snapshot failed".to_owned());
    }
    report.finish = Some(finish);
    report.finished_at_ms = now_ms()?;
    Ok(report)
}

impl FleetdClients {
    fn load(endpoint: &FleetdEndpoint) -> Result<Self, SoakError> {
        let base = loopback_http_url(&endpoint.server, "Fleetd server")?;
        let http = Client::builder().build()?;
        let operator = AuthenticatedClient {
            base: base.clone(),
            token: load_private_token(&endpoint.operator_token_file)?,
            http: http.clone(),
        };
        let sender = AuthenticatedClient {
            base: base.clone(),
            token: load_private_token(&endpoint.sender_token_file)?,
            http: http.clone(),
        };
        Ok(Self {
            base,
            operator,
            sender,
            http,
        })
    }

    async fn capture(&self, observers: &[ObserverSpec]) -> Result<EvidenceSnapshot, SoakError> {
        let captured_at_ms = now_ms()?;
        let (fleetd, fleetd_error) = match self.fleetd_snapshot().await {
            Ok(snapshot) => (Some(snapshot), None),
            Err(error) => (None, Some(error.to_string())),
        };
        let mut captures = Vec::with_capacity(observers.len());
        for observer in observers {
            captures.push(self.capture_observer(observer).await?);
        }
        Ok(EvidenceSnapshot {
            captured_at_ms,
            fleetd,
            fleetd_error,
            observers: captures,
        })
    }

    async fn fleetd_snapshot(&self) -> Result<FleetdSnapshot, SoakError> {
        let plugin_generations = self.operator.get("v1/plugin-generations").await?;
        let session_bindings = self.operator.get("v1/session-bindings").await?;
        let invocation_observations = self.operator.get("v1/invocation-observations").await?;
        let unresolved_delivery_blocks = self.operator.get("v1/delivery-blocks").await?;
        Ok(FleetdSnapshot {
            plugin_generations,
            session_bindings,
            invocation_observations,
            unresolved_delivery_blocks,
        })
    }

    async fn capture_observer(
        &self,
        observer: &ObserverSpec,
    ) -> Result<ObserverCapture, SoakError> {
        let url = loopback_http_url(&observer.url, "observer")?;
        let captured_at_ms = now_ms()?;
        let response = self
            .http
            .get(url)
            .timeout(Duration::from_millis(observer.timeout_ms))
            .send()
            .await;
        let (document, error) = match response {
            Ok(mut response) if response.status().is_success() => {
                if response
                    .content_length()
                    .is_some_and(|length| length > observer.max_bytes as u64)
                {
                    (
                        None,
                        Some(format!(
                            "observer document exceeds {} bytes",
                            observer.max_bytes
                        )),
                    )
                } else {
                    let mut bytes = Vec::new();
                    let mut read_error = None;
                    loop {
                        match response.chunk().await {
                            Ok(Some(chunk))
                                if bytes.len().saturating_add(chunk.len()) > observer.max_bytes =>
                            {
                                read_error = Some(format!(
                                    "observer document exceeds {} bytes",
                                    observer.max_bytes
                                ));
                                break;
                            }
                            Ok(Some(chunk)) => bytes.extend_from_slice(&chunk),
                            Ok(None) => break,
                            Err(error) => {
                                read_error =
                                    Some(format!("observer document read failed: {error}"));
                                break;
                            }
                        }
                    }
                    if let Some(error) = read_error {
                        (None, Some(error))
                    } else {
                        match serde_json::from_slice::<Value>(&bytes) {
                            Ok(document) => (Some(document), None),
                            Err(error) => (None, Some(format!("invalid JSON document: {error}"))),
                        }
                    }
                }
            }
            Ok(response) => (
                None,
                Some(format!("observer returned HTTP {}", response.status())),
            ),
            Err(error) => (None, Some(error.to_string())),
        };
        Ok(ObserverCapture {
            id: observer.id.clone(),
            url: observer.url.clone(),
            required: observer.required,
            captured_at_ms,
            document,
            error,
        })
    }

    async fn run_workload(
        &self,
        workload: &WorkloadSpec,
        observers: &[ObserverSpec],
        poll_interval_ms: u64,
    ) -> Result<WorkloadReport, SoakError> {
        let started_at_ms = now_ms()?;
        let before = self.capture(observers).await?;
        let mut status = WorkloadStatus::Passed;
        let mut error = None;
        let mut seed = None;
        let mut completion = None;
        let mut causal_messages = Vec::new();
        let mut invocation_observations = Vec::new();

        if before.fleetd_error.is_some() || required_observer_failed(&before.observers) {
            status = WorkloadStatus::Failed;
            error =
                Some("Fleetd or a required observer failed before workload dispatch".to_owned());
        } else {
            let input = SendMessage {
                idempotency_key: Some(workload.seed.idempotency_key.clone()),
                recipient_id: Some(workload.seed.recipient_id.clone()),
                kind: workload.seed.kind.clone(),
                payload: workload.seed.payload.clone(),
                correlation_id: None,
                causation_id: None,
            };
            match self
                .sender
                .post::<_, Message>(
                    &format!("v1/channels/{}/messages", workload.seed.channel_id),
                    &input,
                )
                .await
            {
                Ok(message) => {
                    let deadline = tokio::time::Instant::now()
                        + Duration::from_millis(workload.completion.timeout_ms);
                    seed = Some(message.clone());
                    loop {
                        let state = match self.workload_state(&message, workload).await {
                            Ok(state) => state,
                            Err(poll_error) => {
                                status = WorkloadStatus::Failed;
                                error = Some(format!("evidence poll failed: {poll_error}"));
                                break;
                            }
                        };
                        completion = state.completion;
                        causal_messages = state.causal_messages;
                        invocation_observations = state.observations;
                        if completion.is_some()
                            && invocation_agents(&invocation_observations)
                                == workload.completion.invocation_agents
                            && invocation_observations
                                .iter()
                                .all(|observation| observation.terminal_at_ms.is_some())
                        {
                            break;
                        }
                        if tokio::time::Instant::now() >= deadline {
                            status = WorkloadStatus::TimedOut;
                            error = Some(completion_mismatch(
                                workload,
                                completion.as_ref(),
                                &invocation_observations,
                            ));
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
                    }
                }
                Err(send_error) => {
                    status = WorkloadStatus::Failed;
                    error = Some(format!("seed append failed: {send_error}"));
                }
            }
        }

        let after = self.capture(observers).await?;
        if after.fleetd_error.is_some() || required_observer_failed(&after.observers) {
            status = WorkloadStatus::Failed;
            error.get_or_insert_with(|| {
                "Fleetd or a required observer failed after workload execution".to_owned()
            });
        }
        Ok(WorkloadReport {
            id: workload.id.clone(),
            declared: workload.clone(),
            started_at_ms,
            finished_at_ms: now_ms()?,
            status,
            error,
            seed,
            completion,
            causal_messages,
            invocation_observations,
            before,
            after,
        })
    }

    async fn workload_state(
        &self,
        seed: &Message,
        workload: &WorkloadSpec,
    ) -> Result<WorkloadState, SoakError> {
        let messages = self.channel_history(&seed.channel_id, seed.seq - 1).await?;
        let causal_ids = causal_descendants(seed, &messages);
        let mut causal_messages: Vec<_> = messages
            .into_iter()
            .filter(|message| causal_ids.contains(&message.id))
            .collect();
        causal_messages.sort_by_key(|message| message.seq);
        let completion = causal_messages
            .iter()
            .find(|message| {
                message.kind == workload.completion.kind
                    && message.correlation_id.as_deref() == Some(seed.id.as_str())
                    && message.recipient_id.as_deref() == Some(seed.sender_id.as_str())
            })
            .cloned();
        let mut observations: Vec<InvocationObservation> = self
            .operator
            .get::<Vec<InvocationObservation>>("v1/invocation-observations")
            .await?
            .into_iter()
            .filter(|observation| causal_ids.contains(&observation.source_message_id))
            .collect();
        let sequence_by_id: HashMap<_, _> = causal_messages
            .iter()
            .map(|message| (message.id.as_str(), message.seq))
            .collect();
        observations.sort_by_key(|observation| {
            sequence_by_id
                .get(observation.source_message_id.as_str())
                .copied()
                .unwrap_or(i64::MAX)
        });
        Ok(WorkloadState {
            completion,
            causal_messages,
            observations,
        })
    }

    async fn channel_history(
        &self,
        channel_id: &str,
        initial_cursor: i64,
    ) -> Result<Vec<Message>, SoakError> {
        let mut cursor = initial_cursor.max(0);
        let mut messages = Vec::new();
        for _ in 0..MAX_HISTORY_PAGES {
            let page: MessagePage = self
                .operator
                .get(&format!(
                    "v1/channels/{channel_id}/messages?after={cursor}&limit=500"
                ))
                .await?;
            let count = page.messages.len();
            cursor = page.next_cursor;
            messages.extend(page.messages);
            if count < 500 {
                return Ok(messages);
            }
        }
        Err(SoakError::InvalidPlan(format!(
            "channel history exceeded {MAX_HISTORY_PAGES} pages during one workload"
        )))
    }
}

impl AuthenticatedClient {
    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, SoakError> {
        let response = self
            .http
            .get(self.base.join(path).map_err(|error| {
                SoakError::InvalidEndpoint(format!("cannot join API path: {error}"))
            })?)
            .bearer_auth(&self.token)
            .send()
            .await?;
        decode_response(response).await
    }

    async fn post<I: Serialize + ?Sized, O: DeserializeOwned>(
        &self,
        path: &str,
        input: &I,
    ) -> Result<O, SoakError> {
        let response = self
            .http
            .post(self.base.join(path).map_err(|error| {
                SoakError::InvalidEndpoint(format!("cannot join API path: {error}"))
            })?)
            .bearer_auth(&self.token)
            .json(input)
            .send()
            .await?;
        decode_response(response).await
    }
}

async fn decode_response<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, SoakError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response.json().await?);
    }
    let body = response.text().await.unwrap_or_default();
    Err(SoakError::FleetdHttp {
        status,
        body: body.chars().take(4_096).collect(),
    })
}

fn validate_plan(plan: &SoakPlan) -> Result<(), SoakError> {
    if plan.schema_version != PLAN_SCHEMA_VERSION {
        return Err(SoakError::InvalidPlan(format!(
            "schema_version must be {PLAN_SCHEMA_VERSION}"
        )));
    }
    validate_identifier(&plan.run_id, "run_id")?;
    if !(50..=60_000).contains(&plan.poll_interval_ms) {
        return Err(SoakError::InvalidPlan(
            "poll_interval_ms must be between 50 and 60000".to_owned(),
        ));
    }
    if plan.workloads.is_empty() || plan.workloads.len() > MAX_WORKLOADS {
        return Err(SoakError::InvalidPlan(format!(
            "workloads must contain between 1 and {MAX_WORKLOADS} entries"
        )));
    }
    if plan.observers.len() > MAX_OBSERVERS {
        return Err(SoakError::InvalidPlan(format!(
            "observers cannot exceed {MAX_OBSERVERS} entries"
        )));
    }
    loopback_http_url(&plan.fleetd.server, "Fleetd server")?;
    let mut ids = HashSet::new();
    for observer in &plan.observers {
        validate_identifier(&observer.id, "observer id")?;
        if !ids.insert(format!("observer/{}", observer.id)) {
            return Err(SoakError::InvalidPlan(format!(
                "duplicate observer id {}",
                observer.id
            )));
        }
        loopback_http_url(&observer.url, "observer")?;
        if !(1..=60_000).contains(&observer.timeout_ms) {
            return Err(SoakError::InvalidPlan(format!(
                "observer {} timeout_ms must be between 1 and 60000",
                observer.id
            )));
        }
        if !(1_024..=MAX_OBSERVER_BYTES).contains(&observer.max_bytes) {
            return Err(SoakError::InvalidPlan(format!(
                "observer {} max_bytes must be between 1024 and {MAX_OBSERVER_BYTES}",
                observer.id
            )));
        }
    }
    for workload in &plan.workloads {
        validate_identifier(&workload.id, "workload id")?;
        if !ids.insert(format!("workload/{}", workload.id)) {
            return Err(SoakError::InvalidPlan(format!(
                "duplicate workload id {}",
                workload.id
            )));
        }
        validate_identifier(&workload.seed.channel_id, "channel_id")?;
        validate_identifier(&workload.seed.recipient_id, "recipient_id")?;
        validate_identifier(&workload.seed.kind, "seed kind")?;
        validate_identifier(&workload.seed.idempotency_key, "idempotency_key")?;
        validate_identifier(&workload.completion.kind, "completion kind")?;
        if !(1_000..=86_400_000).contains(&workload.completion.timeout_ms) {
            return Err(SoakError::InvalidPlan(format!(
                "workload {} timeout_ms must be between 1000 and 86400000",
                workload.id
            )));
        }
        if workload.completion.invocation_agents.is_empty() {
            return Err(SoakError::InvalidPlan(format!(
                "workload {} must expect at least one invocation",
                workload.id
            )));
        }
        for agent_id in &workload.completion.invocation_agents {
            validate_identifier(agent_id, "invocation agent_id")?;
        }
    }
    Ok(())
}

fn validate_identifier(value: &str, label: &str) -> Result<(), SoakError> {
    if value.trim().is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
        return Err(SoakError::InvalidPlan(format!(
            "{label} must contain 1 to {MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    Ok(())
}

fn loopback_http_url(value: &str, label: &str) -> Result<Url, SoakError> {
    let mut url = Url::parse(value)
        .map_err(|error| SoakError::InvalidEndpoint(format!("invalid {label} URL: {error}")))?;
    if url.scheme() != "http" {
        return Err(SoakError::InvalidEndpoint(format!(
            "{label} must use http on loopback"
        )));
    }
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(SoakError::InvalidEndpoint(format!(
            "{label} cannot contain credentials, a query, or a fragment"
        )));
    }
    let host = url.host_str().ok_or_else(|| {
        SoakError::InvalidEndpoint(format!("{label} must have an explicit IP address"))
    })?;
    let address = host.parse::<IpAddr>().map_err(|_| {
        SoakError::InvalidEndpoint(format!("{label} host must be an explicit loopback IP"))
    })?;
    if !address.is_loopback() {
        return Err(SoakError::InvalidEndpoint(format!(
            "{label} host must be loopback"
        )));
    }
    if url.port().is_none() {
        return Err(SoakError::InvalidEndpoint(format!(
            "{label} must include an explicit port"
        )));
    }
    if !url.path().ends_with('/') && label == "Fleetd server" {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

fn load_private_token(path: &Path) -> Result<String, SoakError> {
    assert_private_file(path)?;
    let token = std::fs::read_to_string(path).map_err(|source| SoakError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let token = token.trim();
    if token.is_empty() || token.chars().any(char::is_whitespace) {
        return Err(SoakError::InvalidPlan(format!(
            "credential file {} does not contain one token",
            path.display()
        )));
    }
    Ok(token.to_owned())
}

#[cfg(unix)]
fn assert_private_file(path: &Path) -> Result<(), SoakError> {
    use std::os::unix::fs::PermissionsExt;

    let metadata = std::fs::metadata(path).map_err(|source| SoakError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(SoakError::InvalidPlan(format!(
            "credential file {} must be a private regular file",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn assert_private_file(path: &Path) -> Result<(), SoakError> {
    let metadata = std::fs::metadata(path).map_err(|source| SoakError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    if !metadata.is_file() {
        return Err(SoakError::InvalidPlan(format!(
            "credential path {} must be a regular file",
            path.display()
        )));
    }
    Ok(())
}

fn causal_descendants(seed: &Message, messages: &[Message]) -> HashSet<String> {
    let mut causal_ids = HashSet::from([seed.id.clone()]);
    let mut changed = true;
    while changed {
        changed = false;
        for message in messages {
            if causal_ids.contains(&message.id) {
                continue;
            }
            if message
                .causation_id
                .as_ref()
                .is_some_and(|cause| causal_ids.contains(cause))
            {
                causal_ids.insert(message.id.clone());
                changed = true;
            }
        }
    }
    causal_ids
}

fn invocation_agents(observations: &[InvocationObservation]) -> Vec<String> {
    observations
        .iter()
        .map(|observation| observation.agent_id.clone())
        .collect()
}

fn completion_mismatch(
    workload: &WorkloadSpec,
    completion: Option<&Message>,
    observations: &[InvocationObservation],
) -> String {
    format!(
        "transport completion timed out: completion_found={}, expected_agents={:?}, observed_agents={:?}, all_terminal={}",
        completion.is_some(),
        workload.completion.invocation_agents,
        invocation_agents(observations),
        observations
            .iter()
            .all(|observation| observation.terminal_at_ms.is_some())
    )
}

fn required_observer_failed(captures: &[ObserverCapture]) -> bool {
    captures
        .iter()
        .any(|capture| capture.required && capture.error.is_some())
}

fn hex_digest(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        write!(encoded, "{byte:02x}").expect("writing to a string cannot fail");
    }
    encoded
}

fn now_ms() -> Result<i64, SoakError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SoakError::Clock)?;
    i64::try_from(duration.as_millis()).map_err(|_| SoakError::Clock)
}

const fn default_poll_interval_ms() -> u64 {
    500
}

const fn default_observer_timeout_ms() -> u64 {
    5_000
}

const fn default_observer_max_bytes() -> usize {
    1024 * 1024
}

const fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: &str, seq: i64, causation_id: Option<&str>) -> Message {
        Message {
            seq,
            id: id.to_owned(),
            channel_id: "channel".to_owned(),
            sender_id: "sender".to_owned(),
            recipient_id: Some("recipient".to_owned()),
            kind: "opaque".to_owned(),
            payload: Value::Null,
            correlation_id: None,
            causation_id: causation_id.map(str::to_owned),
            created_at_ms: seq,
        }
    }

    #[test]
    fn causal_lineage_excludes_timing_neighbors() {
        let seed = message("seed", 1, None);
        let messages = vec![
            seed.clone(),
            message("child", 2, Some("seed")),
            message("neighbor", 3, None),
            message("grandchild", 4, Some("child")),
            message("neighbor-child", 5, Some("neighbor")),
        ];
        let descendants = causal_descendants(&seed, &messages);
        assert_eq!(
            descendants,
            HashSet::from([
                "seed".to_owned(),
                "child".to_owned(),
                "grandchild".to_owned()
            ])
        );
    }

    #[test]
    fn endpoints_require_explicit_loopback_ip_and_port() {
        assert!(loopback_http_url("http://127.0.0.1:7429", "Fleetd server").is_ok());
        assert!(loopback_http_url("http://localhost:7429", "Fleetd server").is_err());
        assert!(loopback_http_url("https://127.0.0.1:7429", "Fleetd server").is_err());
        assert!(loopback_http_url("http://192.0.2.1:7429", "Fleetd server").is_err());
        assert!(loopback_http_url("http://127.0.0.1", "Fleetd server").is_err());
        assert!(loopback_http_url("http://127.0.0.1:7429?token=secret", "observer").is_err());
    }
}
