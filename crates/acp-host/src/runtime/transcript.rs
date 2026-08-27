//! Replaying one stored conversation without folding it into a turn.

use std::sync::Arc;

use agent_client_protocol::{Agent, ConnectionTo};
use fleetd_proto::harness_acp::{
    StartTranscript, StartTranscriptResult, TranscriptComplete, TranscriptEntry,
};
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc};

use super::{
    AdoptionMethods, DriverError, DriverNotification, RawLoadSessionRequest, SharedState,
    TranscriptCapture, classify_update, now_ms_i64,
};

/// The most entries one replay forwards before it reports truncation.
pub(super) const MAX_TRANSCRIPT_ENTRIES: u64 = 10_000;

/// The most encoded bytes one replay forwards before it reports truncation.
pub(super) const MAX_TRANSCRIPT_BYTES: u64 = 8 * 1024 * 1024;

/// Starts one transcript replay and answers immediately.
///
/// `session/load` is the only ACP method obliged to replay a conversation, so
/// retrieval uses it even though adoption no longer does. The request returns as
/// soon as the replay is under way: entries arrive as notifications and a
/// terminal notification closes it, because a plugin drains notifications only
/// between requests and awaiting the whole replay here would deadlock once it
/// outgrew the channel.
pub(super) async fn start_transcript(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    notifications: &mpsc::Sender<DriverNotification>,
    request: StartTranscript,
    adoption: AdoptionMethods,
) -> Result<StartTranscriptResult, DriverError> {
    if !adoption.load {
        return Err(DriverError::Protocol(
            "inner ACP runtime does not support session/load, so it cannot replay a transcript"
                .to_owned(),
        ));
    }
    let (cwd, directories) = {
        let mut state = shared.lock().await;
        let session = state
            .sessions
            .get_mut(&request.session_ref)
            .ok_or_else(|| {
                DriverError::Protocol("transcript requested for an unopened session".to_owned())
            })?;
        // Retrieval must not be able to read a lane the caller does not own.
        if session.binding.binding_id != request.binding_id
            || session.binding.binding_generation != request.binding_generation
            || session.binding.owner_epoch != request.owner_epoch
        {
            return Err(DriverError::Protocol(
                "transcript requested under a binding that does not own this session".to_owned(),
            ));
        }
        // A replay while a turn is draining would interleave entries with that
        // turn's events, and both would be wrong.
        if session.active.is_some() {
            return Err(DriverError::Protocol(
                "transcript cannot be replayed while a turn is active on this session".to_owned(),
            ));
        }
        if session.capturing.is_some() {
            return Err(DriverError::Protocol(
                "a transcript replay is already in flight for this session".to_owned(),
            ));
        }
        session.capturing = Some(TranscriptCapture {
            next_entry_seq: 1,
            entry_count: 0,
            observed_payload_bytes: 0,
            truncated: false,
        });
        (session.cwd.clone(), session.additional_directories.clone())
    };

    let sent = connection.send_request(RawLoadSessionRequest(json!({
        "sessionId": request.session_ref,
        "cwd": cwd,
        "additionalDirectories": directories,
        "mcpServers": []
    })));
    let capture_shared = Arc::clone(shared);
    let capture_notifications = notifications.clone();
    let session_ref = request.session_ref.clone();
    tokio::spawn(async move {
        let failure = sent.block_task().await.err().map(|error| error.to_string());
        let (entry_count, observed_payload_bytes, truncated) = {
            let mut state = capture_shared.lock().await;
            state
                .sessions
                .get_mut(&session_ref)
                .and_then(|session| session.capturing.take())
                .map_or((0, 0, false), |capture| {
                    (
                        capture.entry_count,
                        capture.observed_payload_bytes,
                        capture.truncated,
                    )
                })
        };
        let complete = TranscriptComplete {
            session_ref,
            entry_count,
            observed_payload_bytes,
            truncated,
            failure: failure.map(|reason| bounded_reason(&reason)),
        };
        if let Ok(params) = serde_json::to_value(&complete) {
            let _closed = capture_notifications
                .send(DriverNotification {
                    method: "harness.acp.session.transcript.complete".to_owned(),
                    params,
                })
                .await;
        }
    });

    Ok(StartTranscriptResult { accepted: true })
}

/// Bounds a runtime diagnostic before it is forwarded as evidence.
pub(super) fn bounded_reason(reason: &str) -> String {
    const LIMIT: usize = 1_024;
    if reason.len() <= LIMIT {
        return reason.to_owned();
    }
    let mut end = LIMIT;
    while end > 0 && !reason.is_char_boundary(end) {
        end -= 1;
    }
    reason[..end].to_owned()
}

/// Folds one replayed update into the capture, or refuses it past a bound.
///
/// Returning `None` marks the capture truncated and stops forwarding, so a
/// consumer learns a bound was reached instead of inferring completeness from a
/// replay that simply stopped.
pub(super) fn capture_transcript_entry(
    capture: &mut TranscriptCapture,
    session_ref: &str,
    update: &Value,
) -> Option<TranscriptEntry> {
    if capture.truncated {
        return None;
    }
    let encoded = u64::try_from(update.to_string().len()).unwrap_or(u64::MAX);
    if capture.entry_count >= MAX_TRANSCRIPT_ENTRIES
        || capture.observed_payload_bytes.saturating_add(encoded) > MAX_TRANSCRIPT_BYTES
    {
        capture.truncated = true;
        return None;
    }
    let entry_seq = capture.next_entry_seq;
    capture.next_entry_seq = capture.next_entry_seq.saturating_add(1);
    capture.entry_count = capture.entry_count.saturating_add(1);
    capture.observed_payload_bytes = capture.observed_payload_bytes.saturating_add(encoded);
    Some(TranscriptEntry {
        session_ref: session_ref.to_owned(),
        entry_seq,
        observed_at_ms: now_ms_i64(),
        classification: classify_update(update).to_owned(),
        raw_update: update.clone(),
    })
}

pub(super) async fn forward_transcript_entry(
    notifications: &mpsc::Sender<DriverNotification>,
    entry: TranscriptEntry,
) -> Result<(), DriverError> {
    notifications
        .send(DriverNotification {
            method: "harness.acp.session.transcript.entry".to_owned(),
            params: serde_json::to_value(entry)?,
        })
        .await
        .map_err(|_| DriverError::Runtime("host notification channel closed".to_owned()))
}
