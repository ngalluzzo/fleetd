//! The MCP surface for invocation-scoped message publishing.
//!
//! A peer of `http`, and named for the same reason: this is a mechanism, not a
//! domain. What may be published under a grant is decided in
//! `execution::message_grant`; this module binds a loopback listener, speaks
//! Streamable HTTP, and describes the tool. Nothing here decides policy.

use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::{Request, State},
    http::{StatusCode, header::HeaderName},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::{
    Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use fleetd_execution::{
    message_grant::{
        MessageBrokerInner, PUBLISH_DURABLE_MESSAGE_GRANT, PublishMessageInput,
        PublishMessageOutput,
    },
    worker::TurnGrant,
};
use fleetd_kernel::store::Store;
use fleetd_plugin_host::{ResolvedMcpEndpoint, ResolvedMcpGrant, ResolvedMcpHttpHeader};

const GRANT_HEADER: &str = "x-fleetd-grant-token";

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
        let inner = Arc::new(MessageBrokerInner::new(store));
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

    /// Returns the grant a worker run offers its turns.
    ///
    /// Both halves come from here because both are consequences of having
    /// started an endpoint: the authority to publish, and where to publish to.
    #[must_use]
    pub fn turn_grant(&self) -> TurnGrant {
        TurnGrant {
            authority: self.inner.clone(),
            resolved: self.endpoint.clone(),
        }
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
    /// Publishes one addressed, durable Fleetd message under the current
    /// invocation. Fleetd derives sender, channel, correlation, causation, and
    /// idempotency; the caller controls only the peer, kind, and payload.
    #[tool(
        name = "publish_durable_message",
        description = "Commit an addressed message to a peer in the current Fleetd channel. Returns the committed message identity; it does not wait for a reply. Reuse operation_id for exact retries."
    )]
    async fn publish_durable_message(
        &self,
        Parameters(input): Parameters<PublishMessageInput>,
    ) -> Result<Json<PublishMessageOutput>, String> {
        self.inner.publish(input).await.map(Json)
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
    use fleetd_execution::invocation;
    use fleetd_execution::message_grant::MAX_MESSAGES_PER_INVOCATION;
    use fleetd_proto::model::{CreateMessage, Invocation};
    use serde_json::Value;
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
    use fleetd_proto::model::{
        Agent, Channel, ClaimDeliveries, CreateAgent, CreateChannel, Message,
    };

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

        let authority = broker.turn_grant().authority;
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
            .list_messages(&fixture.channel.id, fixture.source.seq, 20)
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
