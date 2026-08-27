//! Resolving and cancelling harness permission requests.

use std::{sync::Arc, time::Duration};

use fleetd_proto::harness_acp::{ExecutionFence, PermissionOutcome, PermissionResolution};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot};
use uuid::Uuid;

use super::{DriverError, DriverNotification, PendingPermission, SharedState, now_ms, now_ms_i64};

pub(super) async fn handle_permission_request(
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    raw: Value,
) -> Result<Value, DriverError> {
    let session_ref = raw
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| DriverError::Protocol("permission request omitted sessionId".to_owned()))?;
    let permission_id = Uuid::new_v4().to_string();
    let (response_tx, response_rx) = oneshot::channel();
    let (event, expiry) = {
        let mut state = shared.lock().await;
        let session = state.sessions.get_mut(session_ref).ok_or_else(|| {
            DriverError::Protocol("permission request references unknown session".to_owned())
        })?;
        let active = session.active.as_mut().ok_or_else(|| {
            DriverError::Protocol("permission request arrived outside an active turn".to_owned())
        })?;
        let event_seq = active.next_event_seq;
        active.next_event_seq += 1;
        active.activity.send_replace(now_ms());
        let expiry = active
            .policy
            .idle_timeout_ms
            .min(active.policy.wall_timeout_ms);
        let fence = active.fence.clone();
        let event = fleetd_proto::harness_acp::PermissionRequested {
            fence: fence.clone(),
            permission_id: permission_id.clone(),
            event_seq,
            tool_call: raw.get("toolCall").cloned().unwrap_or(Value::Null),
            options: raw
                .get("options")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            expires_at_ms: now_ms_i64().saturating_add(i64::try_from(expiry).unwrap_or(i64::MAX)),
        };
        state.permissions.insert(
            permission_id.clone(),
            PendingPermission {
                fence,
                response: response_tx,
            },
        );
        (event, expiry)
    };
    notifications
        .send(DriverNotification {
            method: "harness.acp.permission.requested".to_owned(),
            params: serde_json::to_value(event)?,
        })
        .await
        .map_err(|_| DriverError::Runtime("host notification channel closed".to_owned()))?;
    let outcome = tokio::time::timeout(Duration::from_millis(expiry), response_rx)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or(PermissionOutcome::Cancelled);
    shared.lock().await.permissions.remove(&permission_id);
    Ok(match outcome {
        PermissionOutcome::Selected { option_id } => {
            json!({"outcome": {"outcome": "selected", "optionId": option_id}})
        }
        PermissionOutcome::Cancelled => json!({"outcome": {"outcome": "cancelled"}}),
    })
}

pub(super) async fn resolve_permission(
    shared: &Arc<Mutex<SharedState>>,
    request: PermissionResolution,
) -> Result<(), DriverError> {
    let pending = shared
        .lock()
        .await
        .permissions
        .remove(&request.permission_id)
        .ok_or_else(|| DriverError::Protocol("permission request is not pending".to_owned()))?;
    if pending.fence != request.fence {
        return Err(DriverError::Protocol(
            "permission resolution fence does not match request".to_owned(),
        ));
    }
    pending
        .response
        .send(request.outcome)
        .map_err(|_| DriverError::Protocol("permission request already expired".to_owned()))
}

pub(super) async fn cancel_permissions_for_fence(
    shared: &Arc<Mutex<SharedState>>,
    fence: &ExecutionFence,
) {
    let pending = {
        let mut state = shared.lock().await;
        let ids = state
            .permissions
            .iter()
            .filter(|(_, pending)| pending.fence == *fence)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        ids.into_iter()
            .filter_map(|id| state.permissions.remove(&id))
            .collect::<Vec<_>>()
    };
    for pending in pending {
        let _unused = pending.response.send(PermissionOutcome::Cancelled);
    }
}

pub(super) async fn cancel_all_permissions(shared: &Arc<Mutex<SharedState>>) {
    let pending = shared
        .lock()
        .await
        .permissions
        .drain()
        .map(|(_, pending)| pending)
        .collect::<Vec<_>>();
    for pending in pending {
        let _unused = pending.response.send(PermissionOutcome::Cancelled);
    }
}
