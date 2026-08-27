//! Managed harness control flow above the messaging kernel.

use std::{sync::Arc, time::Duration};

use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use thiserror::Error;

use crate::operations;
use crate::session_binding;
use crate::settlement;
use crate::trajectory::{
    TrajectoryClose, TrajectoryOutcome, TrajectorySink, TrajectoryTurn, TrajectoryUpdate,
};
use fleetd_kernel::{error::FleetError, store::Store};
use fleetd_plugin_host::{
    Binding, HarnessAcpClient, HarnessAcpNotification, HarnessExecutionCertainty,
    PermissionOutcome, PermissionResolution, PromptBlock, StartTurn, TurnPolicy, TurnSource,
};
use fleetd_proto::model::{
    ArmInvocation, BlockDelivery, BlockedDelivery, CompleteInvocation, Invocation,
    InvocationCompletion, InvocationState,
};

/// One reserved inbox attempt ready to be dispatched into an already-opened,
/// durably bound native harness session.
pub struct ManagedTurn {
    pub invocation: Invocation,
    /// Exact ready plugin generation that will execute this turn.
    pub generation_id: String,
    pub binding: Binding,
    pub session_ref: String,
    pub prompt: Vec<PromptBlock>,
    pub policy: TurnPolicy,
    /// Narrow controller-owned grants activated only after the durable
    /// invocation fence and revoked before settlement.
    pub grants: Vec<Arc<dyn ManagedTurnGrant>>,
    pub result_kind: String,
    /// Adapter-selected result representation. The raw assistant transcript is
    /// always retained; structured capture only identifies and parses one
    /// protocol-bounded final message.
    pub result_capture: TurnResultCapture,
    /// Adapter-owned immutable context copied into the raw result evidence.
    pub result_context: serde_json::Value,
}

/// Invocation-scoped authority made available to a harness turn.
pub trait ManagedTurnGrant: Send + Sync {
    /// Activates this grant for one already-armed invocation.
    ///
    /// # Errors
    ///
    /// Returns a bounded diagnostic when the grant cannot establish the
    /// exact invocation scope.
    fn activate<'a>(&'a self, invocation: &'a Invocation) -> BoxFuture<'a, Result<(), String>>;

    /// Revokes the exact invocation if it is still active.
    fn deactivate<'a>(&'a self, invocation_id: &'a str) -> BoxFuture<'a, ()>;
}

/// How a turn adapter asks the controller to expose terminal output. This is
/// result transport, not semantic validation or conformance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnResultCapture {
    Transcript,
    FinalAssistantJson,
}

/// Durable settlement produced by one managed harness turn.
#[derive(Debug)]
pub enum ManagedTurnOutcome {
    Completed(Box<InvocationCompletion>),
    Blocked(Box<BlockedDelivery>),
}

/// Failure to drive or durably settle a managed turn.
#[derive(Debug, Error)]
pub enum ManagedTurnError {
    #[error("managed turn input is invalid: {0}")]
    Invalid(String),
    #[error("managed turn persistence failed: {0}")]
    Fleet(#[from] FleetError),
    #[error("managed harness failed before dispatch was armed: {0}")]
    HarnessBeforeArm(#[source] Box<fleetd_plugin_host::PluginError>),
}

/// Trusted controller that composes invocation fences with a typed harness
/// interface. It owns settlement policy but no harness-specific semantics.
pub struct ManagedHarnessController<'store> {
    store: &'store Store,
    /// Optional lossy trajectory egress. A sink observes a copy of what the
    /// durable fold is about to discard and can never fail a turn.
    sink: Option<Arc<dyn TrajectorySink>>,
}

impl<'store> ManagedHarnessController<'store> {
    #[must_use]
    pub const fn new(store: &'store Store) -> Self {
        Self { store, sink: None }
    }

    /// Attaches an optional trajectory sink provisioned by a surface.
    #[must_use]
    pub fn with_trajectory_sink(mut self, sink: Option<Arc<dyn TrajectorySink>>) -> Self {
        self.sink = sink;
        self
    }

    /// Offers the identity of a just-armed turn to the sink, if one is
    /// attached.
    fn open_trajectory(&self, invocation: &Invocation, generation_id: &str, binding: &Binding) {
        if let Some(sink) = &self.sink {
            sink.open(&TrajectoryTurn {
                invocation_id: &invocation.id,
                agent_id: &invocation.agent_id,
                channel_id: &invocation.message.channel_id,
                source_message_id: &invocation.message.id,
                generation_id,
                binding_id: &binding.binding_id,
                binding_generation: binding.binding_generation,
                owner_epoch: binding.owner_epoch,
                correlation_id: invocation.message.correlation_id.as_deref(),
                causation_id: invocation.message.causation_id.as_deref(),
                opened_at_ms: fleetd_kernel::store::now_ms(),
            });
        }
    }

    /// Offers one close to the sink, if one is attached.
    ///
    /// Close is idempotent by contract, so every post-arm exit may call this
    /// without tracking whether an earlier one already did.
    fn close_trajectory(&self, invocation_id: &str, close: TrajectoryClose<'_>) {
        if let Some(sink) = &self.sink {
            sink.close(&TrajectoryOutcome {
                invocation_id,
                closed_at_ms: fleetd_kernel::store::now_ms(),
                close,
            });
        }
    }

    /// Closes an open trajectory when a post-arm step fails outright.
    ///
    /// A failure here is not a harness outcome, so it is reported as a park:
    /// the turn is neither known-complete nor known-unstarted.
    fn closing_on_error<T>(
        &self,
        invocation_id: &str,
        result: Result<T, ManagedTurnError>,
    ) -> Result<T, ManagedTurnError> {
        if let Err(error) = &result {
            let reason = format!("managed turn failed after arming: {error}");
            self.close_trajectory(invocation_id, TrajectoryClose::Parked { reason: &reason });
        }
        result
    }

    /// Arms, dispatches, drains, and durably settles one reserved invocation.
    ///
    /// The native session must already be open and its opaque reference must
    /// have been durably recorded by the caller. This method never sends the
    /// effectful prompt before the invocation and exact session owner fence
    /// are atomically armed.
    ///
    /// # Errors
    ///
    /// Returns an error when input validation, pre-arm harness readiness, or
    /// durable settlement fails. Post-arm harness ambiguity is returned as a
    /// successful [`ManagedTurnOutcome::Blocked`] settlement.
    pub async fn run(
        &self,
        harness: &mut HarnessAcpClient,
        turn: ManagedTurn,
    ) -> Result<ManagedTurnOutcome, ManagedTurnError> {
        validate_turn(&turn)?;
        harness
            .describe()
            .await
            .map_err(|error| ManagedTurnError::HarnessBeforeArm(Box::new(error)))?;

        let ManagedTurn {
            invocation,
            generation_id,
            binding,
            session_ref,
            prompt,
            policy,
            grants,
            result_kind,
            result_capture,
            result_context,
        } = turn;
        session_binding::arm_session_invocation(
            self.store,
            &invocation.agent_id,
            &invocation.id,
            &binding,
            &session_ref,
            &generation_id,
            ArmInvocation {
                lease_token: invocation.lease_token.clone(),
                fence_token: invocation.fence_token.clone(),
            },
        )
        .await?;
        self.open_trajectory(&invocation, &generation_id, &binding);

        for (activated, grant) in grants.iter().enumerate() {
            if let Err(error) = grant.activate(&invocation).await {
                revoke_grants(&grants[..activated], &invocation.id).await;
                return self
                    .block_after_arm(
                        &invocation,
                        &binding,
                        format!("invocation grant activation failed: {error}"),
                    )
                    .await;
            }
        }

        let fence = fleetd_plugin_host::ExecutionFence {
            binding_id: binding.binding_id.clone(),
            binding_generation: binding.binding_generation,
            owner_epoch: binding.owner_epoch,
            invocation_id: invocation.id.clone(),
            fence_token: invocation.fence_token.clone(),
        };
        let request = StartTurn {
            fence: fence.clone(),
            session_ref,
            source: TurnSource {
                agent_id: invocation.agent_id.clone(),
                message_id: invocation.message.id.clone(),
                channel_id: invocation.message.channel_id.clone(),
                sender_id: invocation.message.sender_id.clone(),
                correlation_id: invocation.message.correlation_id.clone(),
                causation_id: invocation.message.causation_id.clone(),
            },
            prompt,
            policy: policy.clone(),
        };
        if let Err(error) = harness.start_turn(&request).await {
            revoke_grants(&grants, &invocation.id).await;
            return self
                .block_after_arm(&invocation, &binding, format!("turn start failed: {error}"))
                .await;
        }

        let terminal_result = self
            .await_terminal(
                harness,
                &invocation,
                &generation_id,
                &binding,
                &fence,
                &policy,
            )
            .await;
        revoke_grants(&grants, &invocation.id).await;
        let terminal = match self.closing_on_error(&invocation.id, terminal_result)? {
            TerminalDrain::Terminal(terminal) => terminal,
            TerminalDrain::Blocked(blocked) => return Ok(ManagedTurnOutcome::Blocked(blocked)),
        };
        let settled = self
            .settle_terminal(
                &invocation,
                &binding,
                result_kind,
                result_capture,
                result_context,
                terminal,
            )
            .await;
        self.closing_on_error(&invocation.id, settled)
    }

    async fn await_terminal(
        &self,
        harness: &mut HarnessAcpClient,
        invocation: &Invocation,
        generation_id: &str,
        binding: &Binding,
        fence: &fleetd_plugin_host::ExecutionFence,
        policy: &TurnPolicy,
    ) -> Result<TerminalDrain, ManagedTurnError> {
        match tokio::time::timeout(
            Duration::from_millis(policy.wall_timeout_ms),
            drain_turn(
                self.store,
                generation_id,
                harness,
                fence,
                self.sink.as_deref(),
            ),
        )
        .await
        {
            Ok(Ok(terminal)) => Ok(TerminalDrain::Terminal(TerminalEvidence {
                terminal: Box::new(terminal),
                host_stop_reason: None,
            })),
            Ok(Err(error)) => {
                self.blocked_drain(
                    invocation,
                    binding,
                    format!("turn evidence failed: {error}"),
                )
                .await
            }
            Err(_) => {
                if let Err(error) = harness.cancel_turn("wall_deadline").await {
                    return self
                        .blocked_drain(
                            invocation,
                            binding,
                            format!("host wall deadline cancellation failed: {error}"),
                        )
                        .await;
                }
                match tokio::time::timeout(
                    Duration::from_millis(policy.cancel_drain_timeout_ms),
                    drain_turn(
                        self.store,
                        generation_id,
                        harness,
                        fence,
                        self.sink.as_deref(),
                    ),
                )
                .await
                {
                    Ok(Ok(terminal)) => Ok(TerminalDrain::Terminal(TerminalEvidence {
                        terminal: Box::new(terminal),
                        host_stop_reason: Some(HostStopReason::WallDeadline),
                    })),
                    Ok(Err(error)) => {
                        self.blocked_drain(
                            invocation,
                            binding,
                            format!("cancel drain failed: {error}"),
                        )
                        .await
                    }
                    Err(_) => {
                        self.blocked_drain(
                            invocation,
                            binding,
                            "cancel drain deadline exceeded".to_owned(),
                        )
                        .await
                    }
                }
            }
        }
    }

    async fn settle_terminal(
        &self,
        invocation: &Invocation,
        binding: &Binding,
        result_kind: String,
        result_capture: TurnResultCapture,
        result_context: serde_json::Value,
        terminal_evidence: TerminalEvidence,
    ) -> Result<ManagedTurnOutcome, ManagedTurnError> {
        let TerminalEvidence {
            terminal,
            host_stop_reason,
        } = terminal_evidence;
        let terminal = *terminal;
        if terminal.execution_certainty == HarnessExecutionCertainty::OutcomeUnknown
            || !terminal.session_quiescent
        {
            let reason = format!(
                "terminal outcome is not safely settleable: certainty={:?}, quiescent={}",
                terminal.execution_certainty, terminal.session_quiescent
            );
            let blocked = self.block(invocation, binding, reason).await?;
            return Ok(ManagedTurnOutcome::Blocked(Box::new(blocked)));
        }

        let payload = terminal_payload(
            &invocation.id,
            result_capture,
            &result_context,
            &terminal,
            host_stop_reason,
        );
        let (completion, _created) = session_binding::complete_session_invocation(
            self.store,
            &invocation.agent_id,
            &invocation.id,
            binding,
            terminal.session_persistence,
            CompleteInvocation {
                lease_token: invocation.lease_token.clone(),
                fence_token: invocation.fence_token.clone(),
                kind: result_kind,
                payload,
            },
        )
        .await?;
        self.close_trajectory(
            &invocation.id,
            TrajectoryClose::Terminal {
                stop_reason: &terminal.stop_reason,
                runtime_stop_reason: terminal.runtime_stop_reason.as_deref(),
                certainty: terminal.execution_certainty.into(),
                session_quiescent: terminal.session_quiescent,
                usage: &terminal.usage,
            },
        );
        Ok(ManagedTurnOutcome::Completed(Box::new(completion)))
    }

    async fn block_after_arm(
        &self,
        invocation: &Invocation,
        binding: &Binding,
        reason: String,
    ) -> Result<ManagedTurnOutcome, ManagedTurnError> {
        let blocked = self.block(invocation, binding, reason).await?;
        Ok(ManagedTurnOutcome::Blocked(Box::new(blocked)))
    }

    async fn blocked_drain(
        &self,
        invocation: &Invocation,
        binding: &Binding,
        reason: String,
    ) -> Result<TerminalDrain, ManagedTurnError> {
        Ok(TerminalDrain::Blocked(Box::new(
            self.block(invocation, binding, reason).await?,
        )))
    }

    async fn block(
        &self,
        invocation: &Invocation,
        binding: &Binding,
        reason: String,
    ) -> Result<BlockedDelivery, FleetError> {
        let reason = bounded_reason(reason);
        session_binding::mark_session_invocation_uncertain(
            self.store,
            &invocation.agent_id,
            &invocation.id,
            binding,
            &reason,
        )
        .await?;
        let (blocked, _created) = settlement::block_delivery(
            self.store,
            &invocation.agent_id,
            &invocation.message.id,
            BlockDelivery {
                lease_token: invocation.lease_token.clone(),
                reason: reason.clone(),
            },
        )
        .await?;
        self.close_trajectory(&invocation.id, TrajectoryClose::Parked { reason: &reason });
        Ok(blocked)
    }
}

fn terminal_payload(
    invocation_id: &str,
    result_capture: TurnResultCapture,
    result_context: &Value,
    terminal: &fleetd_plugin_host::TurnTerminal,
    host_stop_reason: Option<HostStopReason>,
) -> Value {
    let transcript_complete = terminal
        .assistant_messages
        .iter()
        .all(|message| message.complete);
    let terminal_stop_reason = terminal.stop_reason.clone();
    let effective_stop_reason = host_stop_reason.map_or_else(
        || terminal_stop_reason.clone(),
        |reason| reason.as_str().to_owned(),
    );
    let host_stopped = host_stop_reason.is_some();
    let runtime_stop_reason = if host_stopped {
        terminal
            .runtime_stop_reason
            .clone()
            .or_else(|| Some(terminal_stop_reason.clone()))
    } else {
        terminal.runtime_stop_reason.clone()
    };
    let terminal_success = !host_stopped
        && runtime_stop_reason.is_none()
        && terminal_stop_reason == "end_turn"
        && transcript_complete;
    let mut payload = match result_capture {
        TurnResultCapture::Transcript => json!({
            "status": if terminal_success { "completed" } else { "failed" },
            "invocation_id": invocation_id,
            "stop_reason": effective_stop_reason,
            "output_complete": transcript_complete,
            "assistant_messages": terminal.assistant_messages,
            "usage": terminal.usage,
            "session_persistence": terminal.session_persistence,
            "result_context": result_context,
        }),
        TurnResultCapture::FinalAssistantJson => {
            let structured_result = capture_final_assistant_json(&terminal.assistant_messages);
            let structured_captured = structured_result["status"] == "captured";
            json!({
                "status": if terminal_success && structured_captured { "completed" } else { "failed" },
                "invocation_id": invocation_id,
                "stop_reason": effective_stop_reason,
                "transcript_complete": transcript_complete,
                "assistant_messages": terminal.assistant_messages,
                "structured_result": structured_result,
                "usage": terminal.usage,
                "session_persistence": terminal.session_persistence,
                "result_context": result_context,
            })
        }
    };
    if let Some(runtime_stop_reason) = runtime_stop_reason {
        payload
            .as_object_mut()
            .expect("managed result payloads are objects")
            .insert(
                "runtime_stop_reason".to_owned(),
                Value::String(runtime_stop_reason),
            );
    }
    payload
}

async fn revoke_grants(grants: &[Arc<dyn ManagedTurnGrant>], invocation_id: &str) {
    for grant in grants.iter().rev() {
        grant.deactivate(invocation_id).await;
    }
}

fn capture_final_assistant_json(messages: &[fleetd_plugin_host::AssistantMessage]) -> Value {
    let (message, selection) = match select_final_assistant_message(messages) {
        Ok(selected) => selected,
        Err(reason) => return json!({"status": "unavailable", "reason": reason}),
    };
    if !message.complete {
        return json!({"status": "unavailable", "reason": "incomplete_final_message"});
    }
    let mut text = String::new();
    for block in &message.content {
        if block.get("type").and_then(Value::as_str) != Some("text") {
            return json!({"status": "unavailable", "reason": "unsupported_final_content"});
        }
        let Some(fragment) = block.get("text").and_then(Value::as_str) else {
            return json!({"status": "unavailable", "reason": "unsupported_final_content"});
        };
        text.push_str(fragment);
    }
    if text.trim().is_empty() {
        return json!({"status": "unavailable", "reason": "empty_final_message"});
    }
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return json!({"status": "unavailable", "reason": "malformed_final_json"});
    };
    json!({
        "status": "captured",
        "source": {
            "selection": selection,
            "message_id": message.message_id,
            "first_event_seq": message.first_event_seq,
            "last_event_seq": message.last_event_seq,
        },
        "value": value,
    })
}

fn select_final_assistant_message(
    messages: &[fleetd_plugin_host::AssistantMessage],
) -> Result<(&fleetd_plugin_host::AssistantMessage, &'static str), &'static str> {
    let Some(final_message) = messages.last() else {
        return Err("no_assistant_message");
    };
    let mut previous_last = 0;
    for message in messages {
        if message.first_event_seq == 0
            || message.first_event_seq > message.last_event_seq
            || message.first_event_seq <= previous_last
        {
            return Err("invalid_message_event_bounds");
        }
        previous_last = message.last_event_seq;
    }
    if messages.len() == 1 {
        return Ok((final_message, "only_assistant_message"));
    }
    let mut ids = std::collections::BTreeSet::new();
    for message in messages {
        let Some(id) = message.message_id.as_deref() else {
            return Err("ambiguous_message_boundary");
        };
        if !ids.insert(id) {
            return Err("ambiguous_message_boundary");
        }
    }
    Ok((final_message, "last_identified_assistant_message"))
}

#[derive(Clone, Copy)]
enum HostStopReason {
    WallDeadline,
}

impl HostStopReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::WallDeadline => "host_wall_deadline",
        }
    }
}

struct TerminalEvidence {
    terminal: Box<fleetd_plugin_host::TurnTerminal>,
    host_stop_reason: Option<HostStopReason>,
}

enum TerminalDrain {
    Terminal(TerminalEvidence),
    Blocked(Box<BlockedDelivery>),
}

async fn drain_turn(
    store: &Store,
    generation_id: &str,
    harness: &mut HarnessAcpClient,
    fence: &fleetd_plugin_host::ExecutionFence,
    sink: Option<&dyn TrajectorySink>,
) -> Result<fleetd_plugin_host::TurnTerminal, TurnDrainError> {
    loop {
        match harness.next_notification().await? {
            HarnessAcpNotification::TurnEvent(event) => {
                operations::record_invocation_event(
                    store,
                    generation_id,
                    &fence.invocation_id,
                    event.event_seq,
                    event.observed_at_ms,
                    &event.classification,
                    &event.raw_update,
                )
                .await?;
                if let Some(sink) = sink {
                    sink.observe(&TrajectoryUpdate {
                        invocation_id: &fence.invocation_id,
                        event_seq: event.event_seq,
                        observed_at_ms: event.observed_at_ms,
                        classification: &event.classification,
                        raw_update: &event.raw_update,
                    });
                }
            }
            HarnessAcpNotification::PermissionRequested(permission) => {
                let raw = serde_json::to_value(&permission)?;
                let observed_at_ms = fleetd_kernel::store::now_ms();
                operations::record_invocation_event(
                    store,
                    generation_id,
                    &fence.invocation_id,
                    permission.event_seq,
                    observed_at_ms,
                    "permission_request",
                    &raw,
                )
                .await?;
                if let Some(sink) = sink {
                    sink.observe(&TrajectoryUpdate {
                        invocation_id: &fence.invocation_id,
                        event_seq: permission.event_seq,
                        observed_at_ms,
                        classification: "permission_request",
                        raw_update: &raw,
                    });
                }
                harness
                    .resolve_permission(&PermissionResolution {
                        fence: fence.clone(),
                        permission_id: permission.permission_id,
                        outcome: PermissionOutcome::Cancelled,
                    })
                    .await?;
            }
            // A transcript replay answers a question a caller asked; it cannot
            // belong to a turn. Folding one into this invocation's evidence
            // would attribute a stored conversation to work that never
            // produced it, so an arriving entry fails the drain rather than
            // being silently ignored.
            HarnessAcpNotification::TranscriptEntry(_) => {
                return Err(TurnDrainError::UnexpectedNotification("a transcript entry"));
            }
            HarnessAcpNotification::TranscriptComplete(_) => {
                return Err(TurnDrainError::UnexpectedNotification(
                    "a transcript completion",
                ));
            }
            HarnessAcpNotification::TurnTerminal(terminal) => {
                operations::record_invocation_terminal(store, generation_id, &terminal).await?;
                return Ok(terminal);
            }
        }
    }
}

#[derive(Debug, Error)]
enum TurnDrainError {
    #[error("harness sent {0} while a turn was draining")]
    UnexpectedNotification(&'static str),
    #[error("harness protocol failed: {0}")]
    Plugin(#[from] fleetd_plugin_host::PluginError),
    #[error("durable invocation evidence failed: {0}")]
    Evidence(#[from] FleetError),
    #[error("invocation evidence serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

fn validate_turn(turn: &ManagedTurn) -> Result<(), ManagedTurnError> {
    if turn.invocation.state != InvocationState::Reserved {
        return Err(ManagedTurnError::Invalid(
            "invocation must be reserved before managed dispatch".to_owned(),
        ));
    }
    if turn.generation_id.trim().is_empty() {
        return Err(ManagedTurnError::Invalid(
            "plugin generation ID must not be empty".to_owned(),
        ));
    }
    if turn.invocation.agent_id == turn.invocation.message.sender_id {
        return Err(ManagedTurnError::Invalid(
            "managed invocation cannot answer its own source message".to_owned(),
        ));
    }
    if turn.result_kind.trim().is_empty() || turn.prompt.is_empty() {
        return Err(ManagedTurnError::Invalid(
            "result kind and prompt must not be empty".to_owned(),
        ));
    }
    Ok(())
}

fn bounded_reason(mut reason: String) -> String {
    const LIMIT: usize = 4_096;
    if reason.len() <= LIMIT {
        return reason;
    }
    let mut end = LIMIT;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason.truncate(end);
    reason
}
