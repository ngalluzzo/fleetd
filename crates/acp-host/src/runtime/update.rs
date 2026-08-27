//! Folding streamed session updates into bounded turn evidence.

use std::sync::Arc;

use fleetd_proto::harness_acp::{AssistantMessage, TurnEvent};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, mpsc};

use super::{
    ActiveTurn, DriverError, DriverNotification, SharedState, capture_transcript_entry,
    forward_transcript_entry, now_ms, now_ms_i64,
};

pub(super) async fn handle_session_update(
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    raw: Value,
) -> Result<(), DriverError> {
    let session_ref = raw
        .get("sessionId")
        .and_then(Value::as_str)
        .ok_or_else(|| DriverError::Protocol("session/update omitted sessionId".to_owned()))?;
    let update = raw
        .get("update")
        .cloned()
        .ok_or_else(|| DriverError::Protocol("session/update omitted update".to_owned()))?;
    let event = {
        let mut state = shared.lock().await;
        let Some(session) = state.sessions.get_mut(session_ref) else {
            return Ok(());
        };
        // A replay in flight claims these updates as transcript entries. It is
        // the one case where an update outside a turn has an honest home,
        // because it is answering a question a caller asked rather than
        // reporting work.
        if let Some(capture) = session.capturing.as_mut() {
            let entry = capture_transcript_entry(capture, session_ref, &update);
            drop(state);
            return match entry {
                Some(entry) => forward_transcript_entry(notifications, entry).await,
                None => Ok(()),
            };
        }
        // Otherwise an update outside any active turn belongs to no invocation,
        // and attributing it to the next one would corrupt that invocation's
        // event count and chain digest. Adoption no longer produces these: it
        // sends `session/resume`, which must not replay. What reaches here now
        // is a runtime volunteering activity Fleetd never fenced, which is
        // exactly what there is no honest place to put.
        let Some(active) = session.active.as_mut() else {
            return Ok(());
        };
        let event_seq = active.next_event_seq;
        active.next_event_seq = active
            .next_event_seq
            .checked_add(1)
            .ok_or_else(|| DriverError::Protocol("event sequence overflowed".to_owned()))?;
        let classification = classify_update(&update);
        let recognized_activity = matches!(
            classification,
            "agent_message_content"
                | "reasoning_content"
                | "tool_call"
                | "tool_call_update"
                | "plan_update"
        );
        if recognized_activity {
            active.activity.send_replace(now_ms());
        }
        capture_update(active, event_seq, &update)?;
        if classification == "tool_call" {
            active.tool_calls = active.tool_calls.saturating_add(1);
        }
        if active.tool_calls > active.policy.tool_budget.limit {
            active
                .cancellation
                .send_replace(Some("tool_budget".to_owned()));
        }
        TurnEvent {
            fence: active.fence.clone(),
            event_seq,
            observed_at_ms: now_ms_i64(),
            classification: classification.to_owned(),
            raw_update: bound_json(update, active.policy.max_captured_output_bytes),
        }
    };
    notifications
        .send(DriverNotification {
            method: "harness.acp.turn.event".to_owned(),
            params: serde_json::to_value(event)?,
        })
        .await
        .map_err(|_| DriverError::Runtime("host notification channel closed".to_owned()))
}

pub(super) fn capture_update(
    active: &mut ActiveTurn,
    event_seq: u64,
    update: &Value,
) -> Result<(), DriverError> {
    let kind = update.get("sessionUpdate").and_then(Value::as_str);
    if kind == Some("usage_update") {
        active.usage = update.clone();
    }
    if kind != Some("agent_message_chunk") {
        return Ok(());
    }
    let message_id = match update.get("messageId") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_str().map(str::to_owned).ok_or_else(|| {
            DriverError::Protocol("agent messageId must be a string or null".to_owned())
        })?),
    };
    let content = update
        .get("content")
        .ok_or_else(|| DriverError::Protocol("agent message chunk omitted content".to_owned()))?;

    let starts_new_message = active
        .assistant_messages
        .last()
        .is_none_or(|message| message.message_id != message_id);
    if starts_new_message {
        if let Some(id) = &message_id
            && active
                .assistant_messages
                .iter()
                .any(|message| message.message_id.as_ref() == Some(id))
        {
            return Err(DriverError::Protocol(format!(
                "agent messageId {id} reappeared after a different message"
            )));
        }
        active.assistant_messages.push(AssistantMessage {
            message_id,
            content: Vec::new(),
            complete: true,
            first_event_seq: event_seq,
            last_event_seq: event_seq,
        });
    }
    let message = active
        .assistant_messages
        .last_mut()
        .expect("assistant message was created before capture");
    message.last_event_seq = event_seq;
    let remaining = active
        .policy
        .max_captured_output_bytes
        .saturating_sub(active.captured_bytes);
    if content.get("type").and_then(Value::as_str) == Some("text") {
        let text = content
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| DriverError::Protocol("agent text chunk omitted text".to_owned()))?;
        if text.len() <= remaining {
            message.content.push(content.clone());
            active.captured_bytes += text.len();
        } else {
            let mut end = remaining.min(text.len());
            while !text.is_char_boundary(end) {
                end = end.saturating_sub(1);
            }
            if end > 0 {
                let mut captured = content.clone();
                captured["text"] = Value::String(text[..end].to_owned());
                message.content.push(captured);
            }
            active.captured_bytes = active.policy.max_captured_output_bytes;
            message.complete = false;
        }
    } else {
        let encoded = serde_json::to_vec(content)?;
        if encoded.len() <= remaining {
            message.content.push(content.clone());
            active.captured_bytes += encoded.len();
        } else {
            active.captured_bytes = active.policy.max_captured_output_bytes;
            message.complete = false;
        }
    }
    Ok(())
}

pub(super) fn classify_update(update: &Value) -> &'static str {
    match update.get("sessionUpdate").and_then(Value::as_str) {
        Some("user_message_chunk") => "user_message_content",
        Some("agent_message_chunk") => "agent_message_content",
        Some("agent_thought_chunk") => "reasoning_content",
        Some("tool_call") => "tool_call",
        Some("tool_call_update") => "tool_call_update",
        Some("plan") => "plan_update",
        Some("usage_update") => "usage",
        Some(
            "session_info_update"
            | "available_commands_update"
            | "current_mode_update"
            | "config_option_update",
        ) => "metadata",
        _ => "unknown",
    }
}

pub(super) fn bound_json(value: Value, limit: usize) -> Value {
    let bytes = serde_json::to_vec(&value).unwrap_or_default();
    if bytes.len() <= limit {
        return value;
    }
    json!({
        "truncated": true,
        "observed_bytes": bytes.len(),
        "sha256": format!("sha256:{:x}", Sha256::digest(&bytes)),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::classify_update;

    /// A prompt echoed back by the runtime is a recognised ACP kind, so it must
    /// not land in `unknown`. That counter exists to mean "an update this build
    /// has never seen", and a real harness emits one prompt per turn, so leaving
    /// it unnamed put a constant offset on the one signal worth watching.
    #[test]
    fn every_acp_update_this_build_understands_is_named() {
        for (update, expected) in [
            ("user_message_chunk", "user_message_content"),
            ("agent_message_chunk", "agent_message_content"),
            ("agent_thought_chunk", "reasoning_content"),
            ("tool_call", "tool_call"),
            ("tool_call_update", "tool_call_update"),
            ("plan", "plan_update"),
            ("usage_update", "usage"),
            ("session_info_update", "metadata"),
        ] {
            let classification = classify_update(&json!({"sessionUpdate": update}));
            assert_eq!(classification, expected, "{update} was misclassified");
            assert_ne!(
                fleetd_proto::operations::EventClass::parse(classification),
                fleetd_proto::operations::EventClass::Unknown,
                "{update} is a kind this build understands and must not count as unknown"
            );
        }
        assert_eq!(
            classify_update(&json!({"sessionUpdate": "a_kind_acp_adds_later"})),
            "unknown"
        );
    }
}
