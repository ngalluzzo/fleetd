//! Managed harness control flow above the messaging kernel.

use std::time::Duration;

use serde_json::{Value, json};
use thiserror::Error;

use crate::{
    ArmInvocation, BlockDelivery, BlockedDelivery, CompleteInvocation, FleetError,
    HarnessAcpClient, HarnessAcpNotification, HarnessExecutionCertainty, Invocation,
    InvocationCompletion, InvocationState, PermissionOutcome, PermissionResolution, PromptBlock,
    StartTurn, Store, TurnPolicy, TurnSource, plugin::Binding,
};

/// One reserved inbox attempt ready to be dispatched into an already-opened,
/// durably bound native harness session.
pub struct ManagedTurn {
    pub invocation: Invocation,
    pub binding: Binding,
    pub session_ref: String,
    pub prompt: Vec<PromptBlock>,
    pub policy: TurnPolicy,
    pub result_kind: String,
    /// Adapter-selected result representation. The raw assistant transcript is
    /// always retained; structured capture only identifies and parses one
    /// protocol-bounded final message.
    pub result_capture: TurnResultCapture,
    /// Adapter-owned immutable context copied into the raw result evidence.
    pub result_context: serde_json::Value,
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
    HarnessBeforeArm(#[source] Box<crate::PluginError>),
}

/// Trusted controller that composes invocation fences with the typed harness
/// capability. It owns settlement policy but no harness-specific semantics.
pub struct ManagedHarnessController<'store> {
    store: &'store Store,
}

impl<'store> ManagedHarnessController<'store> {
    #[must_use]
    pub const fn new(store: &'store Store) -> Self {
        Self { store }
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
            binding,
            session_ref,
            prompt,
            policy,
            result_kind,
            result_capture,
            result_context,
        } = turn;
        self.store
            .arm_session_invocation(
                &invocation.agent_id,
                &invocation.id,
                &binding,
                &session_ref,
                ArmInvocation {
                    lease_token: invocation.lease_token.clone(),
                    fence_token: invocation.fence_token.clone(),
                },
            )
            .await?;

        let fence = crate::ExecutionFence {
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
            return self
                .block_after_arm(&invocation, &binding, format!("turn start failed: {error}"))
                .await;
        }

        let terminal = match self
            .await_terminal(harness, &invocation, &binding, &fence, &policy)
            .await?
        {
            TerminalDrain::Terminal(terminal) => *terminal,
            TerminalDrain::Blocked(blocked) => return Ok(ManagedTurnOutcome::Blocked(blocked)),
        };
        self.settle_terminal(
            &invocation,
            &binding,
            result_kind,
            result_capture,
            result_context,
            terminal,
        )
        .await
    }

    async fn await_terminal(
        &self,
        harness: &mut HarnessAcpClient,
        invocation: &Invocation,
        binding: &Binding,
        fence: &crate::ExecutionFence,
        policy: &TurnPolicy,
    ) -> Result<TerminalDrain, ManagedTurnError> {
        match tokio::time::timeout(
            Duration::from_millis(policy.wall_timeout_ms),
            drain_turn(harness, fence),
        )
        .await
        {
            Ok(Ok(terminal)) => Ok(TerminalDrain::Terminal(Box::new(terminal))),
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
                    drain_turn(harness, fence),
                )
                .await
                {
                    Ok(Ok(terminal)) => Ok(TerminalDrain::Terminal(Box::new(terminal))),
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
        terminal: crate::TurnTerminal,
    ) -> Result<ManagedTurnOutcome, ManagedTurnError> {
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

        let transcript_complete = terminal
            .assistant_messages
            .iter()
            .all(|message| message.complete);
        let payload = match result_capture {
            TurnResultCapture::Transcript => {
                let status = if terminal.stop_reason == "end_turn" && transcript_complete {
                    "completed"
                } else {
                    "failed"
                };
                json!({
                    "status": status,
                    "invocation_id": invocation.id,
                    "stop_reason": terminal.stop_reason,
                    "output_complete": transcript_complete,
                    "assistant_messages": terminal.assistant_messages,
                    "usage": terminal.usage,
                    "session_persistence": terminal.session_persistence,
                    "result_context": result_context,
                })
            }
            TurnResultCapture::FinalAssistantJson => {
                let status = if terminal.stop_reason == "end_turn" {
                    "completed"
                } else {
                    "failed"
                };
                let structured_result = capture_final_assistant_json(&terminal.assistant_messages);
                json!({
                    "status": status,
                    "invocation_id": invocation.id,
                    "stop_reason": terminal.stop_reason,
                    "transcript_complete": transcript_complete,
                    "assistant_messages": terminal.assistant_messages,
                    "structured_result": structured_result,
                    "usage": terminal.usage,
                    "session_persistence": terminal.session_persistence,
                    "result_context": result_context,
                })
            }
        };
        let (completion, _created) = self
            .store
            .complete_session_invocation(
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
        self.store
            .mark_session_invocation_uncertain(
                &invocation.agent_id,
                &invocation.id,
                binding,
                &reason,
            )
            .await?;
        let (blocked, _created) = self
            .store
            .block_delivery(
                &invocation.agent_id,
                &invocation.message.id,
                BlockDelivery {
                    lease_token: invocation.lease_token.clone(),
                    reason,
                },
            )
            .await?;
        Ok(blocked)
    }
}

fn capture_final_assistant_json(messages: &[crate::AssistantMessage]) -> Value {
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
    messages: &[crate::AssistantMessage],
) -> Result<(&crate::AssistantMessage, &'static str), &'static str> {
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

enum TerminalDrain {
    Terminal(Box<crate::TurnTerminal>),
    Blocked(Box<BlockedDelivery>),
}

async fn drain_turn(
    harness: &mut HarnessAcpClient,
    fence: &crate::ExecutionFence,
) -> Result<crate::TurnTerminal, crate::PluginError> {
    loop {
        match harness.next_notification().await? {
            HarnessAcpNotification::TurnEvent(_) => {}
            HarnessAcpNotification::PermissionRequested(permission) => {
                harness
                    .resolve_permission(&PermissionResolution {
                        fence: fence.clone(),
                        permission_id: permission.permission_id,
                        outcome: PermissionOutcome::Cancelled,
                    })
                    .await?;
            }
            HarnessAcpNotification::TurnTerminal(terminal) => return Ok(terminal),
        }
    }
}

fn validate_turn(turn: &ManagedTurn) -> Result<(), ManagedTurnError> {
    if turn.invocation.state != InvocationState::Reserved {
        return Err(ManagedTurnError::Invalid(
            "invocation must be reserved before managed dispatch".to_owned(),
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
