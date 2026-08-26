//! Controller-owned, invocation-scoped message grants.

use std::{collections::BTreeSet, sync::Arc};

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header::HeaderName},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use futures_util::future::BoxFuture;
use rmcp::{
    Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    schemars::JsonSchema,
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    error::FleetError,
    execution::controller::ManagedTurnGrant,
    model::{CreateMessage, Invocation},
    plugin::{ResolvedMcpEndpoint, ResolvedMcpGrant, ResolvedMcpHttpHeader},
    store::{Store, now_ms},
};

/// Runtime grant name for invocation-scoped durable message publication.
pub const PUBLISH_DURABLE_MESSAGE_GRANT: &str = "fleet.messaging.send";

const GRANT_HEADER: &str = "x-fleetd-grant-token";
const MAX_MESSAGES_PER_INVOCATION: u32 = 8;
const MAX_OPERATION_ID_BYTES: usize = 128;
const MAX_AGENT_ID_BYTES: usize = 256;
const MAX_MESSAGE_KIND_BYTES: usize = 256;
const MAX_PAYLOAD_BYTES: usize = 64 * 1024;

/// Failure to establish the controller-owned loopback endpoint.
#[derive(Debug, Error)]
pub enum MessageGrantBrokerError {
    #[error("failed to bind message grant broker: {0}")]
    Bind(#[source] std::io::Error),
}

/// Running loopback MCP endpoint plus the authority handle used by the
/// managed-turn controller.
pub struct MessageGrantBroker {
    inner: Arc<MessageBrokerInner>,
    endpoint: ResolvedMcpGrant,
    cancellation: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl MessageGrantBroker {
    /// Binds a random loopback port and starts the official MCP Streamable HTTP
    /// server. The endpoint has no active invocation until the controller arms
    /// a turn.
    ///
    /// # Errors
    ///
    /// Returns an error when the loopback listener cannot be bound.
    pub async fn start(store: Store) -> Result<Self, MessageGrantBrokerError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(MessageGrantBrokerError::Bind)?;
        let address = listener
            .local_addr()
            .map_err(MessageGrantBrokerError::Bind)?;
        let token = Uuid::new_v4().to_string();
        let inner = Arc::new(MessageBrokerInner {
            store,
            active: Mutex::new(None),
        });
        let cancellation = CancellationToken::new();
        let service_inner = Arc::clone(&inner);
        let service = StreamableHttpService::new(
            move || Ok(PublishMessageService::new(Arc::clone(&service_inner))),
            LocalSessionManager::default().into(),
            StreamableHttpServerConfig::default()
                .with_cancellation_token(cancellation.child_token()),
        );
        let router =
            Router::new()
                .nest_service("/mcp", service)
                .layer(middleware::from_fn_with_state(
                    token.clone(),
                    require_grant_token,
                ));
        let server_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, router)
                .with_graceful_shutdown(server_cancellation.cancelled_owned())
                .await
            {
                tracing::error!(%error, "message grant broker stopped unexpectedly");
            }
        });
        Ok(Self {
            inner,
            endpoint: ResolvedMcpGrant {
                name: PUBLISH_DURABLE_MESSAGE_GRANT.to_owned(),
                endpoint: ResolvedMcpEndpoint::Http {
                    url: format!("http://{address}/mcp"),
                    headers: vec![ResolvedMcpHttpHeader {
                        name: GRANT_HEADER.to_owned(),
                        value: token,
                    }],
                },
            },
            cancellation,
            task: Some(task),
        })
    }

    /// Returns the redaction-aware endpoint descriptor supplied to the trusted
    /// ACP driver.
    #[must_use]
    pub fn resolved_grant(&self) -> ResolvedMcpGrant {
        self.endpoint.clone()
    }

    /// Returns the generic managed-turn authority hook for this broker.
    #[must_use]
    pub fn turn_grant(&self) -> Arc<dyn ManagedTurnGrant> {
        self.inner.clone()
    }

    /// Stops the endpoint and waits for the server task to exit.
    pub async fn shutdown(mut self) {
        self.cancellation.cancel();
        if let Some(task) = self.task.take() {
            let _unused = task.await;
        }
    }
}

impl Drop for MessageGrantBroker {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

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

struct MessageBrokerInner {
    store: Store,
    /// Held through the durable append. Revocation therefore waits for every
    /// accepted call to commit or fail before the controller settles the turn.
    active: Mutex<Option<ActiveMessageGrant>>,
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

#[derive(Clone)]
struct PublishMessageService {
    inner: Arc<MessageBrokerInner>,
    tool_router: ToolRouter<Self>,
}

impl PublishMessageService {
    fn new(inner: Arc<MessageBrokerInner>) -> Self {
        Self {
            inner,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for PublishMessageService {}

#[tool_router(router = tool_router)]
impl PublishMessageService {
    /// Publishes one direct, durable Fleetd message under the current
    /// invocation. Fleetd derives sender, channel, correlation, causation, and
    /// idempotency; the caller controls only the peer, kind, and payload.
    #[tool(
        name = "publish_durable_message",
        description = "Commit a direct message to a peer in the current Fleetd channel. Returns the committed message identity; it does not wait for a reply. Reuse operation_id for exact retries."
    )]
    async fn publish_durable_message(
        &self,
        Parameters(input): Parameters<PublishMessageInput>,
    ) -> Result<Json<PublishMessageOutput>, String> {
        self.inner.publish(input).await.map(Json)
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct PublishMessageInput {
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
struct PublishMessageOutput {
    message_id: String,
    seq: i64,
    created: bool,
    channel_id: String,
    correlation_id: String,
    causation_id: String,
}

impl MessageBrokerInner {
    async fn publish(&self, input: PublishMessageInput) -> Result<PublishMessageOutput, String> {
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

async fn require_grant_token(
    State(expected): State<String>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let supplied = request
        .headers()
        .get(HeaderName::from_static(GRANT_HEADER))
        .and_then(|value| value.to_str().ok());
    if supplied != Some(expected.as_str()) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use crate::execution::invocation;
    use std::collections::HashMap;

    use axum::http::{HeaderName, HeaderValue};
    use rmcp::{
        ServiceExt,
        model::CallToolRequestParams,
        transport::{
            StreamableHttpClientTransport,
            streamable_http_client::StreamableHttpClientTransportConfig,
        },
    };
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::model::{Agent, Channel, ClaimDeliveries, CreateAgent, CreateChannel, Message};

    struct Fixture {
        _directory: TempDir,
        store: Store,
        worker: Agent,
        peer: Agent,
        channel: Channel,
        source: Message,
        invocation: Invocation,
    }

    #[tokio::test]
    async fn mcp_publish_is_invocation_scoped_attributed_and_idempotent() {
        let fixture = fixture().await;
        let broker = MessageGrantBroker::start(fixture.store.clone())
            .await
            .expect("start broker");
        let ResolvedMcpEndpoint::Http { url, headers } = broker.resolved_grant().endpoint;
        let unauthorized = reqwest::get(&url).await.expect("unauthorized request");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let mut custom_headers = HashMap::new();
        for header in headers {
            custom_headers.insert(
                HeaderName::from_bytes(header.name.as_bytes()).expect("header name"),
                HeaderValue::from_str(&header.value).expect("header value"),
            );
        }
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(url).custom_headers(custom_headers),
        );
        let client = ().serve(transport).await.expect("connect MCP client");
        let before_activation =
            call_publish(&client, "send-1", &fixture.peer.id, json!({"answer": 42})).await;
        assert_eq!(before_activation.is_error, Some(true));

        let authority = broker.turn_grant();
        authority
            .activate(&fixture.invocation)
            .await
            .expect("activate invocation");
        let first_id = assert_publish_semantics(&client, &fixture).await;

        authority.deactivate(&fixture.invocation.id).await;
        let after_revocation =
            call_publish(&client, "send-2", &fixture.peer.id, json!({"answer": 44})).await;
        assert_eq!(after_revocation.is_error, Some(true));
        let page = fixture
            .store
            .list_messages(
                &fixture.channel.id,
                Some(&fixture.peer.id),
                fixture.source.seq,
                20,
            )
            .await
            .expect("read peer messages");
        assert_eq!(
            page.messages.len(),
            usize::try_from(MAX_MESSAGES_PER_INVOCATION).unwrap()
        );
        let published = &page.messages[0];
        assert_eq!(published.id, first_id);
        assert_eq!(published.sender_id, fixture.worker.id);
        assert_eq!(
            published.recipient_id.as_deref(),
            Some(fixture.peer.id.as_str())
        );
        assert_eq!(
            published.correlation_id.as_deref(),
            Some(fixture.source.id.as_str())
        );
        assert_eq!(
            published.causation_id.as_deref(),
            Some(fixture.source.id.as_str())
        );

        let _unused = client.cancel().await;
        broker.shutdown().await;
    }

    async fn fixture() -> Fixture {
        let directory = TempDir::new().expect("temporary directory");
        let store = Store::open(directory.path().join("fleet.db"))
            .await
            .expect("open store");
        let sender = store
            .create_agent(CreateAgent {
                name: "sender".to_owned(),
                metadata: json!({}),
            })
            .await
            .expect("create sender");
        let worker = store
            .create_agent(CreateAgent {
                name: "worker".to_owned(),
                metadata: json!({}),
            })
            .await
            .expect("create worker");
        let peer = store
            .create_agent(CreateAgent {
                name: "peer".to_owned(),
                metadata: json!({}),
            })
            .await
            .expect("create peer");
        let channel = store
            .create_channel(CreateChannel {
                name: "grant-test".to_owned(),
                metadata: json!({}),
                member_ids: vec![sender.id.clone(), worker.id.clone(), peer.id.clone()],
                members: Vec::new(),
            })
            .await
            .expect("create channel");
        let source = store
            .append_message(
                &channel.id,
                CreateMessage {
                    sender_id: sender.id,
                    idempotency_key: None,
                    recipient_id: Some(worker.id.clone()),
                    kind: "work.request".to_owned(),
                    payload: json!({"task": "delegate"}),
                    correlation_id: None,
                    causation_id: None,
                },
            )
            .await
            .expect("append source message");
        let invocation = invocation::reserve_invocations(
            &store,
            &worker.id,
            ClaimDeliveries {
                limit: 1,
                lease_duration_ms: 300_000,
            },
        )
        .await
        .expect("reserve invocation")
        .invocations
        .pop()
        .expect("one invocation");
        Fixture {
            _directory: directory,
            store,
            worker,
            peer,
            channel,
            source,
            invocation,
        }
    }

    async fn assert_publish_semantics(
        client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
        fixture: &Fixture,
    ) -> String {
        let first = call_publish(client, "send-1", &fixture.peer.id, json!({"answer": 42})).await;
        assert_eq!(first.is_error, Some(false));
        assert_eq!(first.structured_content.as_ref().unwrap()["created"], true);
        let first_id = first.structured_content.as_ref().unwrap()["message_id"]
            .as_str()
            .expect("message id")
            .to_owned();
        let replay = call_publish(client, "send-1", &fixture.peer.id, json!({"answer": 42})).await;
        assert_eq!(replay.is_error, Some(false));
        assert_eq!(
            replay.structured_content.as_ref().unwrap()["created"],
            false
        );
        assert_eq!(
            replay.structured_content.as_ref().unwrap()["message_id"],
            first_id
        );
        let conflict =
            call_publish(client, "send-1", &fixture.peer.id, json!({"answer": 43})).await;
        assert_eq!(conflict.is_error, Some(true));
        for operation in 2..=MAX_MESSAGES_PER_INVOCATION {
            let result = call_publish(
                client,
                &format!("send-{operation}"),
                &fixture.peer.id,
                json!({"slot": operation}),
            )
            .await;
            assert_eq!(result.is_error, Some(false));
            assert_eq!(result.structured_content.as_ref().unwrap()["created"], true);
        }
        let over_quota = call_publish(client, "send-9", &fixture.peer.id, json!({"slot": 9})).await;
        assert_eq!(over_quota.is_error, Some(true));
        let replay_at_quota =
            call_publish(client, "send-1", &fixture.peer.id, json!({"answer": 42})).await;
        assert_eq!(replay_at_quota.is_error, Some(false));
        assert_eq!(
            replay_at_quota.structured_content.as_ref().unwrap()["created"],
            false
        );

        first_id
    }

    #[test]
    fn resolved_header_debug_output_redacts_the_grant_token() {
        let header = ResolvedMcpHttpHeader {
            name: GRANT_HEADER.to_owned(),
            value: "secret-token".to_owned(),
        };
        let debug = format!("{header:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret-token"));
    }

    async fn call_publish(
        client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
        operation_id: &str,
        recipient_id: &str,
        payload: Value,
    ) -> rmcp::model::CallToolResult {
        let arguments = serde_json::from_value(json!({
            "operation_id": operation_id,
            "recipient_id": recipient_id,
            "kind": "peer.result",
            "payload": payload,
        }))
        .expect("tool arguments");
        client
            .call_tool(
                CallToolRequestParams::new("publish_durable_message").with_arguments(arguments),
            )
            .await
            .expect("call publish tool")
    }
}
