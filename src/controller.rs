//! Managed harness control flow above the messaging kernel.

use std::time::Duration;

use thiserror::Error;

use crate::{
    ArmInvocation, BlockDelivery, BlockedDelivery, CompleteInvocation, FleetError,
    HarnessAcpClient, HarnessAcpNotification, HarnessExecutionCertainty, Invocation,
    InvocationCompletion, InvocationState, PermissionOutcome, PermissionResolution, PromptBlock,
    StartTurn, Store, TurnPolicy, TurnSource, plugin::Binding,
};

/// One reserved inbox attempt ready to be dispatched into an already-opened
/// native harness session.
pub struct ManagedTurn {
    pub invocation: Invocation,
    pub binding: Binding,
    pub session_ref: String,
    pub prompt: Vec<PromptBlock>,
    pub policy: TurnPolicy,
    pub result_kind: String,
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
    /// effectful prompt before `arm_invocation` commits.
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

        let invocation = &turn.invocation;
        self.store
            .arm_invocation(
                &invocation.agent_id,
                &invocation.id,
                ArmInvocation {
                    lease_token: invocation.lease_token.clone(),
                    fence_token: invocation.fence_token.clone(),
                },
            )
            .await?;

        let fence = crate::ExecutionFence {
            binding_id: turn.binding.binding_id,
            binding_generation: turn.binding.binding_generation,
            owner_epoch: turn.binding.owner_epoch,
            invocation_id: invocation.id.clone(),
            fence_token: invocation.fence_token.clone(),
        };
        let request = StartTurn {
            fence: fence.clone(),
            session_ref: turn.session_ref,
            source: TurnSource {
                agent_id: invocation.agent_id.clone(),
                message_id: invocation.message.id.clone(),
                channel_id: invocation.message.channel_id.clone(),
                sender_id: invocation.message.sender_id.clone(),
                correlation_id: invocation.message.correlation_id.clone(),
                causation_id: invocation.message.causation_id.clone(),
            },
            prompt: turn.prompt,
            policy: turn.policy.clone(),
        };
        if let Err(error) = harness.start_turn(&request).await {
            return self
                .block_after_arm(invocation, format!("turn start failed: {error}"))
                .await;
        }

        let terminal = match self
            .await_terminal(harness, invocation, &fence, &turn.policy)
            .await?
        {
            TerminalDrain::Terminal(terminal) => *terminal,
            TerminalDrain::Blocked(blocked) => return Ok(ManagedTurnOutcome::Blocked(blocked)),
        };
        self.settle_terminal(invocation, turn.result_kind, terminal)
            .await
    }

    async fn await_terminal(
        &self,
        harness: &mut HarnessAcpClient,
        invocation: &Invocation,
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
                self.blocked_drain(invocation, format!("turn evidence failed: {error}"))
                    .await
            }
            Err(_) => {
                if let Err(error) = harness.cancel_turn("wall_deadline").await {
                    return self
                        .blocked_drain(
                            invocation,
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
                        self.blocked_drain(invocation, format!("cancel drain failed: {error}"))
                            .await
                    }
                    Err(_) => {
                        self.blocked_drain(invocation, "cancel drain deadline exceeded".to_owned())
                            .await
                    }
                }
            }
        }
    }

    async fn settle_terminal(
        &self,
        invocation: &Invocation,
        result_kind: String,
        terminal: crate::TurnTerminal,
    ) -> Result<ManagedTurnOutcome, ManagedTurnError> {
        if terminal.execution_certainty == HarnessExecutionCertainty::OutcomeUnknown
            || !terminal.session_quiescent
        {
            let reason = format!(
                "terminal outcome is not safely settleable: certainty={:?}, quiescent={}",
                terminal.execution_certainty, terminal.session_quiescent
            );
            let blocked = self.block(invocation, reason).await?;
            return Ok(ManagedTurnOutcome::Blocked(Box::new(blocked)));
        }

        let output_complete = terminal
            .assistant_messages
            .iter()
            .all(|message| message.complete);
        let status = if terminal.stop_reason == "end_turn" && output_complete {
            "completed"
        } else {
            "failed"
        };
        let payload = serde_json::json!({
            "status": status,
            "invocation_id": invocation.id,
            "stop_reason": terminal.stop_reason,
            "output_complete": output_complete,
            "assistant_messages": terminal.assistant_messages,
            "usage": terminal.usage,
            "session_persistence": terminal.session_persistence,
        });
        let (completion, _created) = self
            .store
            .complete_invocation(
                &invocation.agent_id,
                &invocation.id,
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
        reason: String,
    ) -> Result<ManagedTurnOutcome, ManagedTurnError> {
        let blocked = self.block(invocation, reason).await?;
        Ok(ManagedTurnOutcome::Blocked(Box::new(blocked)))
    }

    async fn blocked_drain(
        &self,
        invocation: &Invocation,
        reason: String,
    ) -> Result<TerminalDrain, ManagedTurnError> {
        Ok(TerminalDrain::Blocked(Box::new(
            self.block(invocation, reason).await?,
        )))
    }

    async fn block(
        &self,
        invocation: &Invocation,
        reason: String,
    ) -> Result<BlockedDelivery, FleetError> {
        let (blocked, _created) = self
            .store
            .block_delivery(
                &invocation.agent_id,
                &invocation.message.id,
                BlockDelivery {
                    lease_token: invocation.lease_token.clone(),
                    reason: bounded_reason(reason),
                },
            )
            .await?;
        Ok(blocked)
    }
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
