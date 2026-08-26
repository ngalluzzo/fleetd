//! What an invocation-scoped message grant permits.
//!
//! A grant is armed for exactly one invocation and fixes what a publish may
//! say: the sender, the channel, the correlation and causation it inherits, and
//! how many messages it may produce. A caller chooses only the recipient, the
//! kind, and the payload.
//!
//! Exposing this over a wire is a surface's job, not this module's. The MCP
//! endpoint is a surface in the daemon.

use std::collections::BTreeSet;

use futures_util::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::controller::ManagedTurnGrant;
use fleetd_kernel::{
    error::FleetError,
    store::{Store, now_ms},
};
use fleetd_proto::model::{CreateMessage, Invocation};

/// Runtime grant name for invocation-scoped durable message publication.
pub const PUBLISH_DURABLE_MESSAGE_GRANT: &str = "fleet.messaging.send";

pub const MAX_MESSAGES_PER_INVOCATION: u32 = 8;
const MAX_OPERATION_ID_BYTES: usize = 128;
const MAX_AGENT_ID_BYTES: usize = 256;
const MAX_MESSAGE_KIND_BYTES: usize = 256;
const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

#[derive(Debug)]
struct ActiveMessageGrant {
    invocation_id: String,
    sender_id: String,
    channel_id: String,
    source_message_id: String,
    correlation_id: String,
    expires_at_ms: i64,
    published_messages: u32,
    operations: BTreeSet<String>,
}

pub struct MessageBrokerInner {
    store: Store,
    /// Held through the durable append. Revocation therefore waits for every
    /// accepted call to commit or fail before the controller settles the turn.
    active: Mutex<Option<ActiveMessageGrant>>,
}

impl MessageBrokerInner {
    /// Creates the grant state a surface arms and disarms.
    ///
    /// A surface starts an endpoint over this; it does not assemble it, which is
    /// why the fields stay private.
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self {
            store,
            active: Mutex::new(None),
        }
    }
}

impl ManagedTurnGrant for MessageBrokerInner {
    fn activate<'a>(&'a self, invocation: &'a Invocation) -> BoxFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if invocation.lease_expires_at_ms <= now_ms() {
                return Err("invocation lease already expired".to_owned());
            }
            let mut active = self.active.lock().await;
            if active.is_some() {
                return Err("message grant already has an active invocation".to_owned());
            }
            *active = Some(ActiveMessageGrant {
                invocation_id: invocation.id.clone(),
                sender_id: invocation.agent_id.clone(),
                channel_id: invocation.message.channel_id.clone(),
                source_message_id: invocation.message.id.clone(),
                correlation_id: invocation
                    .message
                    .correlation_id
                    .clone()
                    .unwrap_or_else(|| invocation.message.id.clone()),
                expires_at_ms: invocation.lease_expires_at_ms,
                published_messages: 0,
                operations: BTreeSet::new(),
            });
            Ok(())
        })
    }

    fn deactivate<'a>(&'a self, invocation_id: &'a str) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let mut active = self.active.lock().await;
            if active
                .as_ref()
                .is_some_and(|grant| grant.invocation_id == invocation_id)
            {
                *active = None;
            }
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PublishMessageInput {
    /// Stable identifier for this logical send within the current invocation.
    operation_id: String,
    /// Exact peer agent ID. Broadcast and self-send are not permitted.
    recipient_id: String,
    /// Open message kind interpreted by the receiving adapter or contract.
    kind: String,
    /// Opaque JSON payload, bounded to 64 KiB when encoded.
    payload: Value,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct PublishMessageOutput {
    message_id: String,
    seq: i64,
    created: bool,
    channel_id: String,
    correlation_id: String,
    causation_id: String,
}

impl MessageBrokerInner {
    /// Publishes one message under the armed grant.
    ///
    /// The grant fixes the sender, the channel, and the correlation and
    /// causation the message inherits; the caller chooses only the recipient,
    /// the kind, and the payload. Repeating an `operation_id` is idempotent and
    /// does not consume budget.
    ///
    /// # Errors
    ///
    /// Returns a bounded, caller-safe string when no grant is armed, the grant
    /// has expired, the recipient is the sender, the per-invocation budget is
    /// exhausted, the input is out of bounds, or the store rejects the append.
    ///
    /// # Panics
    ///
    /// Panics if the stored message lacks the correlation or causation the
    /// grant supplied, which would mean the append did not preserve them.
    pub async fn publish(
        &self,
        input: PublishMessageInput,
    ) -> Result<PublishMessageOutput, String> {
        validate_publish_input(&input)?;
        let mut active = self.active.lock().await;
        let grant = active
            .as_mut()
            .ok_or_else(|| "no active Fleetd invocation grants message publishing".to_owned())?;
        if grant.expires_at_ms <= now_ms() {
            return Err("the active Fleetd invocation grant has expired".to_owned());
        }
        if input.recipient_id == grant.sender_id {
            return Err("recipient_id must identify a peer, not the sending agent".to_owned());
        }
        let known_operation = grant.operations.contains(&input.operation_id);
        if !known_operation && grant.published_messages >= MAX_MESSAGES_PER_INVOCATION {
            return Err(format!(
                "this invocation may publish at most {MAX_MESSAGES_PER_INVOCATION} messages"
            ));
        }
        let operation_digest = Sha256::digest(input.operation_id.as_bytes());
        let idempotency_key = format!(
            "invocation:{}:publish:{operation_digest:x}",
            grant.invocation_id
        );
        let result = self
            .store
            .append_message_idempotent(
                &grant.channel_id,
                CreateMessage {
                    sender_id: grant.sender_id.clone(),
                    idempotency_key: Some(idempotency_key),
                    recipient_id: Some(input.recipient_id),
                    kind: input.kind,
                    payload: input.payload,
                    correlation_id: Some(grant.correlation_id.clone()),
                    causation_id: Some(grant.source_message_id.clone()),
                },
            )
            .await
            .map_err(public_fleet_error)?;
        if result.created {
            grant.published_messages = grant.published_messages.saturating_add(1);
        }
        grant.operations.insert(input.operation_id);
        Ok(PublishMessageOutput {
            message_id: result.message.id,
            seq: result.message.seq,
            created: result.created,
            channel_id: result.message.channel_id,
            correlation_id: result
                .message
                .correlation_id
                .expect("broker always supplies correlation ID"),
            causation_id: result
                .message
                .causation_id
                .expect("broker always supplies causation ID"),
        })
    }
}

fn validate_publish_input(input: &PublishMessageInput) -> Result<(), String> {
    if input.operation_id.trim().is_empty() || input.operation_id.len() > MAX_OPERATION_ID_BYTES {
        return Err(format!(
            "operation_id must contain between 1 and {MAX_OPERATION_ID_BYTES} bytes"
        ));
    }
    if input.recipient_id.trim().is_empty() || input.recipient_id.len() > MAX_AGENT_ID_BYTES {
        return Err(format!(
            "recipient_id must contain between 1 and {MAX_AGENT_ID_BYTES} bytes"
        ));
    }
    if input.kind.trim().is_empty() || input.kind.len() > MAX_MESSAGE_KIND_BYTES {
        return Err(format!(
            "kind must contain between 1 and {MAX_MESSAGE_KIND_BYTES} bytes"
        ));
    }
    let payload_bytes = serde_json::to_vec(&input.payload)
        .map_err(|_| "payload could not be encoded as JSON".to_owned())?;
    if payload_bytes.len() > MAX_PAYLOAD_BYTES {
        return Err(format!(
            "payload must not exceed {MAX_PAYLOAD_BYTES} encoded bytes"
        ));
    }
    Ok(())
}

fn public_fleet_error(error: FleetError) -> String {
    match error {
        FleetError::NotFound { .. }
        | FleetError::NotMember { .. }
        | FleetError::Invalid(_)
        | FleetError::Forbidden(_)
        | FleetError::Conflict(_) => error.to_string(),
        error => {
            tracing::error!(%error, "message grant commit failed");
            "Fleetd could not commit the durable message".to_owned()
        }
    }
}
