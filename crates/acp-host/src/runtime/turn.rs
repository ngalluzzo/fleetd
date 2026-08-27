//! Driving one fenced turn from prompt to terminal evidence.

use std::{sync::Arc, time::Duration};

use agent_client_protocol::{Agent, ConnectionTo, schema::v1::CancelNotification};
use fleetd_proto::harness_acp::{
    AcceptedResult, CancelTurn, EffectiveEnforcement, HarnessExecutionCertainty, PermissionOutcome,
    SessionPersistence, StartTurn, StartTurnResult, TurnTerminal,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, watch};

use super::{
    ActiveTurn, DriverError, DriverNotification, MAX_FRAME_BYTES, RawPromptRequest, RawResponse,
    SharedState, bound_json, cancel_permissions_for_fence, now_ms,
};

pub(super) const MAX_CAPTURE_BYTES: usize = 512 * 1024;

pub(super) async fn start_turn(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    request: StartTurn,
) -> Result<StartTurnResult, DriverError> {
    if request.policy.max_captured_output_bytes == 0
        || request.policy.max_captured_output_bytes > MAX_CAPTURE_BYTES
    {
        return Err(DriverError::Protocol(format!(
            "max_captured_output_bytes must be between 1 and {MAX_CAPTURE_BYTES}"
        )));
    }
    if request.policy.permission_policy != "controller"
        || request.policy.tool_budget.required_enforcement != "observe_then_cancel"
        || request.policy.token_budget.is_some()
    {
        return Err(DriverError::Protocol(
            "requested turn enforcement is not supported".to_owned(),
        ));
    }
    let (activity_tx, activity_rx) = watch::channel(now_ms());
    let (cancellation_tx, cancellation_rx) = watch::channel(None);
    {
        let mut state = shared.lock().await;
        let session = state
            .sessions
            .get_mut(&request.session_ref)
            .ok_or_else(|| {
                DriverError::Protocol("turn references an unopened session".to_owned())
            })?;
        if session.active.is_some() {
            return Err(DriverError::Protocol(
                "session already has an active turn".to_owned(),
            ));
        }
        if session.binding.binding_id != request.fence.binding_id
            || session.binding.binding_generation != request.fence.binding_generation
            || session.binding.owner_epoch != request.fence.owner_epoch
        {
            return Err(DriverError::Protocol(
                "turn fence does not match session binding".to_owned(),
            ));
        }
        session.active = Some(ActiveTurn {
            fence: request.fence.clone(),
            next_event_seq: 1,
            policy: request.policy.clone(),
            captured_bytes: 0,
            assistant_messages: Vec::new(),
            tool_calls: 0,
            usage: Value::Null,
            activity: activity_tx,
            cancellation: cancellation_tx,
        });
    }
    let prompt = serde_json::to_value(&request.prompt)?;
    let raw_request = json!({
        "sessionId": request.session_ref,
        "prompt": prompt,
        "_meta": {
            "fleetd": {
                "source": request.source,
                "fence": request.fence,
            }
        }
    });
    let sent = connection.send_request(RawPromptRequest(raw_request));
    let task_connection = connection.clone();
    let task_shared = Arc::clone(shared);
    let task_notifications = notifications.clone();
    let task_session_ref = request.session_ref.clone();
    let wall_timeout = Duration::from_millis(request.policy.wall_timeout_ms);
    let idle_timeout = Duration::from_millis(request.policy.idle_timeout_ms);
    let cancel_drain_timeout = Duration::from_millis(request.policy.cancel_drain_timeout_ms);
    tokio::spawn(async move {
        monitor_prompt(
            task_connection,
            task_shared,
            task_notifications,
            task_session_ref,
            sent,
            activity_rx,
            cancellation_rx,
            wall_timeout,
            idle_timeout,
            cancel_drain_timeout,
        )
        .await;
    });

    Ok(StartTurnResult {
        accepted: true,
        effective_enforcement: EffectiveEnforcement {
            wall_timeout: "hard".to_owned(),
            idle_timeout: "hard".to_owned(),
            cancel_drain_timeout: "hard".to_owned(),
            captured_output_bytes: "hard".to_owned(),
            tool_budget: "observe_then_cancel".to_owned(),
            token_budget: "unavailable".to_owned(),
        },
    })
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn monitor_prompt(
    connection: ConnectionTo<Agent>,
    shared: Arc<Mutex<SharedState>>,
    notifications: mpsc::Sender<DriverNotification>,
    session_ref: String,
    sent: agent_client_protocol::SentRequest<RawResponse>,
    mut activity: watch::Receiver<u64>,
    mut cancellation: watch::Receiver<Option<String>>,
    wall_timeout: Duration,
    idle_timeout: Duration,
    cancel_drain_timeout: Duration,
) {
    let response_task = tokio::spawn(sent.block_task());
    let wall_deadline = tokio::time::Instant::now() + wall_timeout;
    let mut idle_deadline = tokio::time::Instant::now() + idle_timeout;
    tokio::pin!(response_task);
    let outcome = loop {
        tokio::select! {
            response = &mut response_task => {
                break match response {
                    Ok(Ok(response)) => PromptOutcome::Known {
                        response: response.0,
                        host_stop_reason: None,
                    },
                    Ok(Err(error)) => PromptOutcome::Unknown(json!({"error": error.to_string()})),
                    Err(error) => PromptOutcome::Unknown(json!({"join_error": error.to_string()})),
                };
            }
            () = tokio::time::sleep_until(wall_deadline) => {
                break cancel_and_drain(&connection, &session_ref, &mut response_task, cancel_drain_timeout, "wall_deadline").await;
            }
            () = tokio::time::sleep_until(idle_deadline) => {
                break cancel_and_drain(&connection, &session_ref, &mut response_task, cancel_drain_timeout, "idle_deadline").await;
            }
            changed = activity.changed() => {
                if changed.is_err() {
                    break PromptOutcome::Unknown(json!({"error": "activity monitor closed"}));
                }
                idle_deadline = tokio::time::Instant::now() + idle_timeout;
            }
            changed = cancellation.changed() => {
                if changed.is_err() {
                    break PromptOutcome::Unknown(json!({"error": "cancellation monitor closed"}));
                }
                let reason = cancellation.borrow().clone();
                if let Some(reason) = reason {
                    break cancel_and_drain(&connection, &session_ref, &mut response_task, cancel_drain_timeout, &reason).await;
                }
            }
        }
    };
    let _unused = emit_terminal(&shared, &notifications, &session_ref, outcome).await;
}

pub(super) enum PromptOutcome {
    Known {
        response: Value,
        host_stop_reason: Option<String>,
    },
    Unknown(Value),
}

pub(super) async fn cancel_and_drain(
    connection: &ConnectionTo<Agent>,
    session_ref: &str,
    response: &mut tokio::task::JoinHandle<Result<RawResponse, agent_client_protocol::Error>>,
    drain_timeout: Duration,
    reason: &str,
) -> PromptOutcome {
    let _unused = connection.send_notification(CancelNotification::new(session_ref.to_owned()));
    match tokio::time::timeout(drain_timeout, &mut *response).await {
        Ok(Ok(Ok(response))) => PromptOutcome::Known {
            response: response.0,
            host_stop_reason: Some(reason.to_owned()),
        },
        Ok(Ok(Err(error))) => PromptOutcome::Unknown(json!({
            "cancel_reason": reason,
            "error": error.to_string(),
        })),
        Ok(Err(error)) => PromptOutcome::Unknown(json!({
            "cancel_reason": reason,
            "join_error": error.to_string(),
        })),
        Err(_) => {
            response.abort();
            PromptOutcome::Unknown(json!({
                "cancel_reason": reason,
                "error": "cancel drain deadline exceeded",
            }))
        }
    }
}

pub(super) async fn emit_terminal(
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    session_ref: &str,
    outcome: PromptOutcome,
) -> Result<(), DriverError> {
    let (terminal, permission_ids) = {
        let mut state = shared.lock().await;
        let session = state
            .sessions
            .get_mut(session_ref)
            .ok_or_else(|| DriverError::Protocol("terminal turn session disappeared".to_owned()))?;
        let active = session
            .active
            .take()
            .ok_or_else(|| DriverError::Protocol("terminal turn was not active".to_owned()))?;
        let permission_ids = state
            .permissions
            .iter()
            .filter(|(_, pending)| pending.fence == active.fence)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let (raw_response, certainty, quiescent, stop_reason, runtime_stop_reason) = match outcome {
            PromptOutcome::Known {
                response,
                host_stop_reason,
            } => {
                let runtime_stop = response
                    .get("stopReason")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                let (stop_reason, runtime_stop_reason) = if let Some(host_stop) = host_stop_reason {
                    (host_stop, Some(runtime_stop))
                } else {
                    (runtime_stop, None)
                };
                (
                    response,
                    HarnessExecutionCertainty::OutcomeKnown,
                    true,
                    stop_reason,
                    runtime_stop_reason,
                )
            }
            PromptOutcome::Unknown(evidence) => (
                evidence,
                HarnessExecutionCertainty::OutcomeUnknown,
                false,
                "outcome_unknown".to_owned(),
                None,
            ),
        };
        let last_event_seq = active.next_event_seq.saturating_sub(1);
        let assistant_messages = active.assistant_messages;
        (
            TurnTerminal {
                fence: active.fence,
                last_event_seq,
                stop_reason,
                runtime_stop_reason,
                execution_certainty: certainty,
                session_quiescent: quiescent,
                session_persistence: if quiescent {
                    SessionPersistence::RuntimeClaimed
                } else {
                    SessionPersistence::Unknown
                },
                assistant_messages,
                usage: active.usage,
                raw_prompt_response: bound_json(raw_response, MAX_FRAME_BYTES / 2),
            },
            permission_ids,
        )
    };
    for permission_id in permission_ids {
        if let Some(pending) = shared.lock().await.permissions.remove(&permission_id) {
            let _unused = pending.response.send(PermissionOutcome::Cancelled);
        }
    }
    notifications
        .send(DriverNotification {
            method: "harness.acp.turn.terminal".to_owned(),
            params: serde_json::to_value(terminal)?,
        })
        .await
        .map_err(|_| DriverError::Runtime("host notification channel closed".to_owned()))
}

pub(super) async fn cancel_turn(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    request: CancelTurn,
) -> Result<AcceptedResult, DriverError> {
    let session_ref = {
        let state = shared.lock().await;
        state
            .sessions
            .iter()
            .find(|(_, session)| {
                session
                    .active
                    .as_ref()
                    .is_some_and(|active| active.fence == request.fence)
            })
            .map(|(session_ref, _)| session_ref.clone())
            .ok_or_else(|| DriverError::Protocol("turn fence is not active".to_owned()))?
    };
    cancel_permissions_for_fence(shared, &request.fence).await;
    {
        let mut state = shared.lock().await;
        let active = state
            .sessions
            .get_mut(&session_ref)
            .and_then(|session| session.active.as_mut())
            .ok_or_else(|| DriverError::Protocol("turn stopped during cancellation".to_owned()))?;
        active.cancellation.send_replace(Some(request.reason));
    }
    connection
        .send_notification(CancelNotification::new(session_ref))
        .map_err(|error| DriverError::Runtime(error.to_string()))?;
    Ok(AcceptedResult { accepted: true })
}
