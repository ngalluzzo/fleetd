//! Continuous harness worker orchestration above the durable controller.

use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    AcquireSessionBinding, FleetError, HarnessAcpClient, Invocation, ManagedHarnessController,
    ManagedTurn, ManagedTurnError, ManagedTurnGrant, ManagedTurnOutcome, MessageGrantBroker,
    OpenSession, OpenSessionMode, PUBLISH_DURABLE_MESSAGE_GRANT, PluginError, PluginProcess,
    PluginSpec, PromptBlock, RetryDelivery, SessionAcquisitionMode, Store, TurnPolicy,
    TurnResultCapture, plugin::Binding,
};

const MAX_LEASE_DURATION_MS: u64 = 3_600_000;
const MAX_POLL_INTERVAL_MS: u64 = 60_000;
const MAX_RETRY_DELAY_MS: u64 = 86_400_000;
const MAX_CAPTURE_BYTES: usize = 512 * 1024;
const LEASE_MARGIN_MS: u64 = 60_000;
const MAX_ACCEPTED_MESSAGE_KINDS: usize = 128;
const MAX_MESSAGE_KIND_BYTES: usize = 256;

/// Versioned, adapter-owned declaration of which immutable message envelopes
/// one worker seat may reserve.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InboundAcceptance {
    schema_version: u32,
    message_kinds: BTreeSet<String>,
}

impl InboundAcceptance {
    /// Creates the v1 exact-kind acceptance contract.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, duplicate, malformed, or unbounded kind
    /// set. Matching a kind establishes reservation eligibility only; adapters
    /// must still validate the complete payload after reservation.
    pub fn exact_v1<I, S>(message_kinds: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let supplied = message_kinds
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        if supplied.is_empty() {
            return Err("inbound acceptance must contain at least one message kind".to_owned());
        }
        if supplied.len() > MAX_ACCEPTED_MESSAGE_KINDS {
            return Err(format!(
                "inbound acceptance exceeds {MAX_ACCEPTED_MESSAGE_KINDS} message kinds"
            ));
        }
        let mut exact = BTreeSet::new();
        for kind in supplied {
            if kind.trim().is_empty() || kind.len() > MAX_MESSAGE_KIND_BYTES {
                return Err(format!(
                    "accepted message kind must contain between 1 and {MAX_MESSAGE_KIND_BYTES} bytes"
                ));
            }
            if !exact.insert(kind.clone()) {
                return Err(format!("duplicate accepted message kind {kind}"));
            }
        }
        Ok(Self {
            schema_version: 1,
            message_kinds: exact,
        })
    }

    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    #[must_use]
    pub fn message_kinds(&self) -> &BTreeSet<String> {
        &self.message_kinds
    }
}

/// One adapter-produced turn that remains outside the messaging kernel.
#[derive(Clone, Debug)]
pub struct PreparedTurn {
    pub lane_policy: String,
    pub lane_key: String,
    pub prompt: Vec<PromptBlock>,
    pub result_kind: String,
    pub result_capture: TurnResultCapture,
    /// Adapter-owned immutable context persisted with raw terminal evidence.
    pub result_context: Value,
}

/// Converts an immutable inbox message into harness-controller input.
pub trait TurnAdapter: Send + Sync {
    /// Declares the exact message contracts this adapter is eligible to
    /// reserve. The worker applies it before a lease or invocation exists.
    fn inbound_acceptance(&self) -> &InboundAcceptance;

    /// Prepares one turn without performing external effects.
    ///
    /// # Errors
    ///
    /// Returns a bounded diagnostic when the adapter cannot represent the
    /// message. The worker safely releases the unarmed reservation before
    /// stopping so an operator can correct the adapter and restart it.
    fn prepare(&self, invocation: &Invocation) -> Result<PreparedTurn, String>;
}

/// Semantic-neutral bridge that supplies the complete fleetd envelope as JSON
/// to one native harness session per channel.
#[derive(Clone, Debug)]
pub struct EnvelopeTurnAdapter {
    result_kind: String,
    inbound_acceptance: InboundAcceptance,
}

impl EnvelopeTurnAdapter {
    /// Creates an envelope adapter with exact v1 inbound message kinds.
    ///
    /// # Errors
    ///
    /// Returns an error when the result kind or inbound contract is invalid.
    pub fn new<I, S>(result_kind: impl Into<String>, message_kinds: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let result_kind = result_kind.into();
        if result_kind.trim().is_empty() || result_kind.len() > MAX_MESSAGE_KIND_BYTES {
            return Err(format!(
                "result kind must contain between 1 and {MAX_MESSAGE_KIND_BYTES} bytes"
            ));
        }
        Ok(Self {
            result_kind,
            inbound_acceptance: InboundAcceptance::exact_v1(message_kinds)?,
        })
    }
}

impl TurnAdapter for EnvelopeTurnAdapter {
    fn inbound_acceptance(&self) -> &InboundAcceptance {
        &self.inbound_acceptance
    }

    fn prepare(&self, invocation: &Invocation) -> Result<PreparedTurn, String> {
        let envelope = serde_json::json!({
            "invocation": {
                "id": invocation.id,
                "delivery_attempt": invocation.delivery_attempt,
            },
            "message": {
                "seq": invocation.message.seq,
                "id": invocation.message.id,
                "channel_id": invocation.message.channel_id,
                "sender_id": invocation.message.sender_id,
                "recipient_id": invocation.message.recipient_id,
                "kind": invocation.message.kind,
                "payload": invocation.message.payload,
                "correlation_id": invocation.message.correlation_id,
                "causation_id": invocation.message.causation_id,
                "created_at_ms": invocation.message.created_at_ms,
            },
        });
        let encoded = serde_json::to_string_pretty(&envelope)
            .map_err(|error| format!("message envelope could not be encoded: {error}"))?;
        Ok(PreparedTurn {
            lane_policy: "per-channel".to_owned(),
            lane_key: invocation.message.channel_id.clone(),
            prompt: vec![PromptBlock::Text {
                text: format!(
                    "You received the following durable fleetd message. Act on its request and \
                     make your final response suitable to return to the sending agent. Preserve \
                     the message's intent; do not invent authority not present in the request.\n\n\
                     {encoded}"
                ),
            }],
            result_kind: self.result_kind.clone(),
            result_capture: TurnResultCapture::Transcript,
            result_context: Value::Null,
        })
    }
}

/// Desired state for one serialized local worker seat.
pub struct ContinuousWorkerConfig {
    pub agent_id: String,
    pub plugin: PluginSpec,
    pub working_directory: PathBuf,
    pub additional_directories: Vec<PathBuf>,
    pub mcp_grants: Vec<String>,
    /// Explicitly qualified compatibility class. `None` means only the exact
    /// observed profile digest may resume an existing session.
    pub compatibility_digest: Option<String>,
    pub lease_duration: Duration,
    pub poll_interval: Duration,
    pub restart_backoff: Duration,
    pub pre_arm_retry_delay: Duration,
    pub turn_policy: TurnPolicy,
}

/// Counts produced by one bounded or cancelled worker run.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct WorkerReport {
    pub plugin_generations: u64,
    pub operational_restarts: u64,
    pub reservations: u64,
    pub completed: u64,
    pub blocked: u64,
    pub pre_arm_retries: u64,
    pub idle_polls: u64,
}

impl WorkerReport {
    #[must_use]
    pub const fn settled_turns(&self) -> u64 {
        self.completed.saturating_add(self.blocked)
    }
}

/// Fatal continuous-worker configuration or settlement failure.
#[derive(Debug, Error)]
pub enum ContinuousWorkerError {
    #[error("continuous worker configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error("turn adapter rejected a reserved message: {0}")]
    Adapter(String),
    #[error("message grant broker failed: {0}")]
    MessageGrantBroker(#[from] crate::MessageGrantBrokerError),
    #[error("failed to safely release an unarmed reservation after {context}: {source}")]
    PreArmSettlement {
        context: String,
        #[source]
        source: FleetError,
    },
}

/// Long-running worker that owns plugin generations and serially drives one
/// agent inbox through the managed controller.
pub struct ContinuousHarnessWorker<'store> {
    store: &'store Store,
    config: ContinuousWorkerConfig,
    adapter: Arc<dyn TurnAdapter>,
}

impl<'store> ContinuousHarnessWorker<'store> {
    /// Creates a validated worker. No process or durable state is changed.
    ///
    /// # Errors
    ///
    /// Returns an error when paths, deadlines, lease bounds, or enforcement
    /// settings cannot support a safe managed turn.
    pub fn new(
        store: &'store Store,
        config: ContinuousWorkerConfig,
        adapter: impl TurnAdapter + 'static,
    ) -> Result<Self, ContinuousWorkerError> {
        validate_config(&config)?;
        Ok(Self {
            store,
            config,
            adapter: Arc::new(adapter),
        })
    }

    /// Runs until cancelled. Cancellation is observed between turns; an armed
    /// turn is always drained or conservatively blocked before shutdown.
    ///
    /// # Errors
    ///
    /// Returns only for fatal adapter or pre-arm settlement failures. Plugin,
    /// harness, and transient store failures restart a fresh process generation
    /// after a bounded delay.
    pub async fn run(
        &self,
        cancellation: CancellationToken,
    ) -> Result<WorkerReport, ContinuousWorkerError> {
        self.run_until(cancellation, None).await
    }

    /// Runs until cancellation or until `max_settled_turns` have completed or
    /// blocked. This bounded form supports qualification and `--once` use.
    ///
    /// # Errors
    ///
    /// Returns under the same fatal conditions as [`Self::run`].
    pub async fn run_until(
        &self,
        cancellation: CancellationToken,
        max_settled_turns: Option<u64>,
    ) -> Result<WorkerReport, ContinuousWorkerError> {
        if max_settled_turns == Some(0) {
            return Err(ContinuousWorkerError::InvalidConfig(
                "maximum settled turns must be greater than zero".to_owned(),
            ));
        }
        let broker = if self
            .config
            .mcp_grants
            .iter()
            .any(|grant| grant == PUBLISH_DURABLE_MESSAGE_GRANT)
        {
            Some(MessageGrantBroker::start(self.store.clone()).await?)
        } else {
            None
        };
        let mut report = WorkerReport::default();
        let outcome = loop {
            if cancellation.is_cancelled() || limit_reached(&report, max_settled_turns) {
                break Ok(report);
            }
            let exit = self
                .run_generation(
                    &cancellation,
                    max_settled_turns,
                    &mut report,
                    broker.as_ref(),
                )
                .await;
            match exit {
                GenerationExit::Stopped => break Ok(report),
                GenerationExit::Fatal(error) => break Err(error),
                GenerationExit::Restart(reason) => {
                    if cancellation.is_cancelled() || limit_reached(&report, max_settled_turns) {
                        break Ok(report);
                    }
                    report.operational_restarts = report.operational_restarts.saturating_add(1);
                    tracing::warn!(
                        agent_id = %self.config.agent_id,
                        reason,
                        "worker plugin generation will restart"
                    );
                    if wait_or_cancel(&cancellation, self.config.restart_backoff).await {
                        break Ok(report);
                    }
                }
            }
        };
        if let Some(broker) = broker {
            broker.shutdown().await;
        }
        outcome
    }

    async fn run_generation(
        &self,
        cancellation: &CancellationToken,
        max_settled_turns: Option<u64>,
        report: &mut WorkerReport,
        broker: Option<&MessageGrantBroker>,
    ) -> GenerationExit {
        let process = match PluginProcess::start(self.config.plugin.clone()).await {
            Ok(process) => process,
            Err(error) => {
                return GenerationExit::Restart(format!("plugin startup failed: {error}"));
            }
        };
        report.plugin_generations = report.plugin_generations.saturating_add(1);
        let mut harness = match process.into_harness_acp() {
            Ok(harness) => harness,
            Err(error) => {
                return GenerationExit::Restart(format!(
                    "typed harness negotiation failed: {error}"
                ));
            }
        };
        let description = match harness.describe().await {
            Ok(description) => description,
            Err(error) => {
                return shutdown_generation(
                    harness,
                    GenerationExit::Restart(format!("harness description failed: {error}")),
                )
                .await;
            }
        };
        let generation = GenerationIdentity {
            owner_instance_id: Uuid::new_v4().to_string(),
            compatibility_digest: worker_compatibility_digest(
                self.config
                    .compatibility_digest
                    .as_deref()
                    .unwrap_or(&description.profile_digest),
                &self.config.mcp_grants,
                self.adapter.inbound_acceptance(),
            ),
            profile_digest: description.profile_digest,
        };
        let context = GenerationContext {
            identity: &generation,
            broker,
        };
        let mut sessions = HashMap::<LaneIdentity, OpenedSession>::new();
        let exit = self
            .drive_generation(
                &mut harness,
                &context,
                &mut sessions,
                cancellation,
                max_settled_turns,
                report,
            )
            .await;
        shutdown_generation(harness, exit).await
    }

    async fn drive_generation(
        &self,
        harness: &mut HarnessAcpClient,
        context: &GenerationContext<'_>,
        sessions: &mut HashMap<LaneIdentity, OpenedSession>,
        cancellation: &CancellationToken,
        max_settled_turns: Option<u64>,
        report: &mut WorkerReport,
    ) -> GenerationExit {
        let controller = ManagedHarnessController::new(self.store);
        loop {
            if cancellation.is_cancelled() || limit_reached(report, max_settled_turns) {
                return GenerationExit::Stopped;
            }
            let reservation = match self
                .store
                .reserve_invocations_by_kind(
                    &self.config.agent_id,
                    crate::ClaimDeliveries {
                        limit: 1,
                        lease_duration_ms: duration_ms(self.config.lease_duration)
                            .expect("validated lease duration"),
                    },
                    self.adapter.inbound_acceptance().message_kinds(),
                )
                .await
            {
                Ok(reservation) => reservation,
                Err(error) => {
                    return GenerationExit::Restart(format!(
                        "invocation reservation failed: {error}"
                    ));
                }
            };
            let Some(invocation) = reservation.invocations.into_iter().next() else {
                report.idle_polls = report.idle_polls.saturating_add(1);
                if wait_or_cancel(cancellation, self.config.poll_interval).await {
                    return GenerationExit::Stopped;
                }
                continue;
            };
            report.reservations = report.reservations.saturating_add(1);
            let prepared = match self.prepare_reserved(&invocation, report).await {
                Ok(prepared) => prepared,
                Err(exit) => return exit,
            };
            if cancellation.is_cancelled() {
                return self.stop_unarmed(&invocation, report).await;
            }
            let lane = LaneIdentity {
                policy: prepared.lane_policy.clone(),
                key: prepared.lane_key.clone(),
            };
            let opened = match self
                .resolve_lane(harness, &invocation, lane, context, sessions, report)
                .await
            {
                Ok(Some(opened)) => opened,
                Ok(None) => continue,
                Err(exit) => return exit,
            };
            if cancellation.is_cancelled() {
                return self.stop_unarmed(&invocation, report).await;
            }
            let opened_turn = OpenedTurn {
                session: opened,
                grants: context
                    .broker
                    .map(MessageGrantBroker::turn_grant)
                    .into_iter()
                    .collect(),
            };
            if let Some(exit) = self
                .execute_turn(
                    &controller,
                    harness,
                    invocation,
                    prepared,
                    opened_turn,
                    report,
                )
                .await
            {
                return exit;
            }
        }
    }

    async fn prepare_reserved(
        &self,
        invocation: &Invocation,
        report: &mut WorkerReport,
    ) -> Result<PreparedTurn, GenerationExit> {
        let prepared = self.adapter.prepare(invocation).map_err(|error| {
            (
                format!("adapter rejected message: {error}"),
                ContinuousWorkerError::Adapter(error),
            )
        });
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err((reason, error)) => {
                self.release_or_exit(invocation, reason, report).await?;
                return Err(GenerationExit::Fatal(error));
            }
        };
        if let Err(error) = validate_prepared_turn(&prepared) {
            self.release_or_exit(
                invocation,
                format!("adapter output is invalid: {error}"),
                report,
            )
            .await?;
            return Err(GenerationExit::Fatal(ContinuousWorkerError::Adapter(error)));
        }
        Ok(prepared)
    }

    async fn resolve_lane(
        &self,
        harness: &HarnessAcpClient,
        invocation: &Invocation,
        lane: LaneIdentity,
        context: &GenerationContext<'_>,
        sessions: &mut HashMap<LaneIdentity, OpenedSession>,
        report: &mut WorkerReport,
    ) -> Result<Option<OpenedSession>, GenerationExit> {
        if let Some(opened) = sessions.get(&lane) {
            return Ok(Some(opened.clone()));
        }
        match self.open_lane(harness, invocation, &lane, context).await {
            Ok(opened) => {
                sessions.insert(lane, opened.clone());
                Ok(Some(opened))
            }
            Err(error) => {
                let plugin_failed = error.is_plugin();
                let reason = format!("session acquisition failed before arm: {error}");
                self.release_or_exit(invocation, reason.clone(), report)
                    .await?;
                if plugin_failed {
                    return Err(GenerationExit::Restart(reason));
                }
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    message_id = %invocation.message.id,
                    reason,
                    "unarmed invocation was released"
                );
                Ok(None)
            }
        }
    }

    async fn execute_turn(
        &self,
        controller: &ManagedHarnessController<'_>,
        harness: &mut HarnessAcpClient,
        invocation: Invocation,
        prepared: PreparedTurn,
        opened: OpenedTurn,
        report: &mut WorkerReport,
    ) -> Option<GenerationExit> {
        let result = controller
            .run(
                harness,
                ManagedTurn {
                    invocation: invocation.clone(),
                    binding: opened.session.binding,
                    session_ref: opened.session.session_ref,
                    prompt: prepared.prompt,
                    policy: self.config.turn_policy.clone(),
                    grants: opened.grants,
                    result_kind: prepared.result_kind,
                    result_capture: prepared.result_capture,
                    result_context: prepared.result_context,
                },
            )
            .await;
        match result {
            Ok(ManagedTurnOutcome::Completed(completion)) => {
                report.completed = report.completed.saturating_add(1);
                tracing::info!(
                    agent_id = %self.config.agent_id,
                    invocation_id = %completion.invocation.id,
                    result_message_id = %completion.result.id,
                    "worker completed invocation"
                );
                None
            }
            Ok(ManagedTurnOutcome::Blocked(blocked)) => {
                report.blocked = report.blocked.saturating_add(1);
                Some(GenerationExit::Restart(format!(
                    "invocation {} was conservatively blocked: {}",
                    invocation.id, blocked.reason
                )))
            }
            Err(error) => self.managed_error(invocation, error, report).await,
        }
    }

    async fn managed_error(
        &self,
        invocation: Invocation,
        error: ManagedTurnError,
        report: &mut WorkerReport,
    ) -> Option<GenerationExit> {
        match error {
            ManagedTurnError::HarnessBeforeArm(error) => {
                let reason = format!("harness failed before arm: {error}");
                match self
                    .release_or_exit(&invocation, reason.clone(), report)
                    .await
                {
                    Ok(()) => Some(GenerationExit::Restart(reason)),
                    Err(exit) => Some(exit),
                }
            }
            ManagedTurnError::Invalid(error) => {
                let reason = format!("managed turn input was invalid: {error}");
                match self.release_or_exit(&invocation, reason, report).await {
                    Ok(()) => Some(GenerationExit::Fatal(ContinuousWorkerError::Adapter(error))),
                    Err(exit) => Some(exit),
                }
            }
            ManagedTurnError::Fleet(error) => Some(GenerationExit::Restart(format!(
                "managed turn persistence failed at an uncertain phase: {error}"
            ))),
        }
    }

    async fn release_or_exit(
        &self,
        invocation: &Invocation,
        reason: String,
        report: &mut WorkerReport,
    ) -> Result<(), GenerationExit> {
        self.retry_before_arm(invocation, reason)
            .await
            .map_err(GenerationExit::Fatal)?;
        report.pre_arm_retries = report.pre_arm_retries.saturating_add(1);
        Ok(())
    }

    async fn stop_unarmed(
        &self,
        invocation: &Invocation,
        report: &mut WorkerReport,
    ) -> GenerationExit {
        match self
            .retry_unarmed(invocation, "worker cancelled before dispatch".to_owned(), 0)
            .await
        {
            Ok(()) => {
                report.pre_arm_retries = report.pre_arm_retries.saturating_add(1);
                GenerationExit::Stopped
            }
            Err(error) => GenerationExit::Fatal(error),
        }
    }

    async fn open_lane(
        &self,
        harness: &HarnessAcpClient,
        invocation: &Invocation,
        lane: &LaneIdentity,
        context: &GenerationContext<'_>,
    ) -> Result<OpenedSession, LaneOpenError> {
        let additional_directories = self
            .config
            .additional_directories
            .iter()
            .map(|directory| directory.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let working_directory = self.config.working_directory.to_string_lossy().into_owned();
        let acquisition = self
            .store
            .acquire_session_binding(
                &invocation.agent_id,
                AcquireSessionBinding {
                    lane_policy: lane.policy.clone(),
                    lane_key: lane.key.clone(),
                    owner_instance_id: context.identity.owner_instance_id.clone(),
                    profile_digest: context.identity.profile_digest.clone(),
                    compatibility_digest: context.identity.compatibility_digest.clone(),
                    working_directory: working_directory.clone(),
                    additional_directories: additional_directories.clone(),
                },
            )
            .await
            .map_err(LaneOpenError::Fleet)?;
        let mode = match acquisition.mode {
            SessionAcquisitionMode::Create => OpenSessionMode::Create,
            SessionAcquisitionMode::Resume { session_ref } => {
                OpenSessionMode::Resume { session_ref }
            }
        };
        let binding = acquisition.session.binding;
        let opened = harness
            .open_session(&OpenSession {
                binding: binding.clone(),
                mode,
                working_directory,
                additional_directories,
                mcp_grants: self.config.mcp_grants.clone(),
                resolved_mcp_grants: context
                    .broker
                    .map(MessageGrantBroker::resolved_grant)
                    .into_iter()
                    .collect(),
                profile_digest: context.identity.profile_digest.clone(),
            })
            .await
            .map_err(LaneOpenError::Plugin)?;
        self.store
            .record_session_opened(&invocation.agent_id, &binding, &opened.session_ref)
            .await
            .map_err(LaneOpenError::Fleet)?;
        Ok(OpenedSession {
            binding,
            session_ref: opened.session_ref,
        })
    }

    async fn retry_before_arm(
        &self,
        invocation: &Invocation,
        context: String,
    ) -> Result<(), ContinuousWorkerError> {
        self.retry_unarmed(
            invocation,
            context,
            duration_ms(self.config.pre_arm_retry_delay).expect("validated retry delay"),
        )
        .await
    }

    async fn retry_unarmed(
        &self,
        invocation: &Invocation,
        context: String,
        retry_after_ms: u64,
    ) -> Result<(), ContinuousWorkerError> {
        self.store
            .retry_delivery(
                &invocation.agent_id,
                &invocation.message.id,
                RetryDelivery {
                    lease_token: invocation.lease_token.clone(),
                    retry_after_ms,
                    error: Some(bounded(context.clone())),
                },
            )
            .await
            .map_err(|source| ContinuousWorkerError::PreArmSettlement { context, source })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct LaneIdentity {
    policy: String,
    key: String,
}

#[derive(Clone)]
struct OpenedSession {
    binding: Binding,
    session_ref: String,
}

struct OpenedTurn {
    session: OpenedSession,
    grants: Vec<Arc<dyn ManagedTurnGrant>>,
}

struct GenerationIdentity {
    owner_instance_id: String,
    profile_digest: String,
    compatibility_digest: String,
}

struct GenerationContext<'a> {
    identity: &'a GenerationIdentity,
    broker: Option<&'a MessageGrantBroker>,
}

enum GenerationExit {
    Stopped,
    Restart(String),
    Fatal(ContinuousWorkerError),
}

enum LaneOpenError {
    Fleet(FleetError),
    Plugin(PluginError),
}

impl LaneOpenError {
    const fn is_plugin(&self) -> bool {
        matches!(self, Self::Plugin(_))
    }
}

impl std::fmt::Display for LaneOpenError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fleet(error) => write!(formatter, "{error}"),
            Self::Plugin(error) => write!(formatter, "{error}"),
        }
    }
}

async fn shutdown_generation(harness: HarnessAcpClient, exit: GenerationExit) -> GenerationExit {
    if let Err(error) = harness.shutdown().await {
        tracing::warn!(%error, "worker plugin generation did not shut down gracefully");
    }
    exit
}

fn validate_config(config: &ContinuousWorkerConfig) -> Result<(), ContinuousWorkerError> {
    if config.agent_id.trim().is_empty() || config.agent_id.len() > 256 {
        return invalid("agent ID must contain between 1 and 256 bytes");
    }
    validate_directory("working directory", &config.working_directory)?;
    for directory in &config.additional_directories {
        validate_directory("additional directory", directory)?;
    }
    let mut mcp_grants = BTreeSet::new();
    for grant in &config.mcp_grants {
        if grant != PUBLISH_DURABLE_MESSAGE_GRANT {
            return invalid(format!("unsupported MCP grant: {grant}"));
        }
        if !mcp_grants.insert(grant) {
            return invalid(format!("duplicate MCP grant: {grant}"));
        }
    }
    if config
        .compatibility_digest
        .as_ref()
        .is_some_and(|digest| digest.trim().is_empty())
    {
        return invalid("compatibility digest must not be empty when supplied");
    }
    if config.poll_interval.is_zero() || config.restart_backoff.is_zero() {
        return invalid("poll interval and restart backoff must be greater than zero");
    }
    let poll_ms = duration_ms(config.poll_interval).ok_or_else(|| {
        ContinuousWorkerError::InvalidConfig("poll interval is too large".to_owned())
    })?;
    let restart_ms = duration_ms(config.restart_backoff).ok_or_else(|| {
        ContinuousWorkerError::InvalidConfig("restart backoff is too large".to_owned())
    })?;
    if poll_ms > MAX_POLL_INTERVAL_MS {
        return invalid("poll interval must not exceed 60,000 milliseconds");
    }
    if restart_ms > MAX_RETRY_DELAY_MS {
        return invalid("restart backoff must not exceed 86,400,000 milliseconds");
    }
    let lease_ms = duration_ms(config.lease_duration).ok_or_else(|| {
        ContinuousWorkerError::InvalidConfig("lease duration is too large".to_owned())
    })?;
    let retry_ms = duration_ms(config.pre_arm_retry_delay).ok_or_else(|| {
        ContinuousWorkerError::InvalidConfig("retry delay is too large".to_owned())
    })?;
    if lease_ms == 0 || lease_ms > MAX_LEASE_DURATION_MS {
        return invalid("lease duration must be between 1 and 3,600,000 milliseconds");
    }
    if retry_ms > MAX_RETRY_DELAY_MS {
        return invalid("pre-arm retry delay must not exceed 86,400,000 milliseconds");
    }
    let required_lease = config
        .turn_policy
        .wall_timeout_ms
        .checked_add(config.turn_policy.cancel_drain_timeout_ms)
        .and_then(|value| value.checked_add(LEASE_MARGIN_MS))
        .ok_or_else(|| {
            ContinuousWorkerError::InvalidConfig("turn deadline bounds overflow".to_owned())
        })?;
    if lease_ms < required_lease {
        return invalid(format!(
            "lease duration must cover wall timeout, cancel drain, and {LEASE_MARGIN_MS}ms margin ({required_lease}ms required)"
        ));
    }
    let policy = &config.turn_policy;
    if policy.idle_timeout_ms == 0
        || policy.wall_timeout_ms == 0
        || policy.cancel_drain_timeout_ms == 0
        || policy.max_captured_output_bytes == 0
        || policy.max_captured_output_bytes > MAX_CAPTURE_BYTES
        || policy.permission_policy != "controller"
        || policy.tool_budget.limit == 0
        || policy.tool_budget.required_enforcement != "observe_then_cancel"
        || policy.token_budget.is_some()
    {
        return invalid("turn policy requests unsupported or unbounded enforcement");
    }
    Ok(())
}

fn validate_directory(
    label: &str,
    directory: &std::path::Path,
) -> Result<(), ContinuousWorkerError> {
    if !directory.is_absolute() || !directory.is_dir() {
        return invalid(format!(
            "{label} must be an existing absolute directory: {}",
            directory.display()
        ));
    }
    Ok(())
}

fn validate_prepared_turn(prepared: &PreparedTurn) -> Result<(), String> {
    if prepared.lane_policy.trim().is_empty()
        || prepared.lane_key.trim().is_empty()
        || prepared.result_kind.trim().is_empty()
        || prepared.prompt.is_empty()
    {
        return Err("lane policy, lane key, result kind, and prompt must not be empty".to_owned());
    }
    Ok(())
}

fn invalid<T>(message: impl Into<String>) -> Result<T, ContinuousWorkerError> {
    Err(ContinuousWorkerError::InvalidConfig(message.into()))
}

fn duration_ms(duration: Duration) -> Option<u64> {
    u64::try_from(duration.as_millis()).ok()
}

fn worker_compatibility_digest(
    base: &str,
    mcp_grants: &[String],
    inbound_acceptance: &InboundAcceptance,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"fleetd-worker-compatibility-v2\0");
    digest.update(base.as_bytes());
    for grant in mcp_grants {
        digest.update(b"\0");
        digest.update(grant.as_bytes());
    }
    digest.update(b"\0inbound-schema\0");
    digest.update(inbound_acceptance.schema_version().to_be_bytes());
    for kind in inbound_acceptance.message_kinds() {
        digest.update(b"\0kind\0");
        digest.update(kind.as_bytes());
    }
    format!("sha256:{:x}", digest.finalize())
}

fn limit_reached(report: &WorkerReport, limit: Option<u64>) -> bool {
    limit.is_some_and(|limit| report.settled_turns() >= limit)
}

async fn wait_or_cancel(cancellation: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        () = cancellation.cancelled() => true,
        () = tokio::time::sleep(duration) => false,
    }
}

fn bounded(mut value: String) -> String {
    const LIMIT: usize = 4_096;
    if value.len() <= LIMIT {
        return value;
    }
    let mut end = LIMIT;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
    value
}
