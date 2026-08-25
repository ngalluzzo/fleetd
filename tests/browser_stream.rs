use std::{net::SocketAddr, time::Duration};

use fleetd::{
    AppState, AuthService, BROWSER_STREAM_PROTOCOL, BrowserStreamGrantIssueResponse,
    BrowserStreamRedemptionMessageType, BrowserStreamRedemptionRequest, BrowserStreamServerFrame,
    CreateAgent, CreateChannel, Message, RegisteredAgent, SendMessage, Store, router,
};
use futures_util::{SinkExt, StreamExt, future::join_all};
use serde_json::{Value, json};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error as WebSocketError, Message as WebSocketMessage,
        client::IntoClientRequest,
        http::{HeaderValue, Request, header::ORIGIN, header::SEC_WEBSOCKET_PROTOCOL},
    },
};

struct BrowserDaemon {
    _directory: tempfile::TempDir,
    auth: AuthService,
    operator_token: String,
    address: SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

impl BrowserDaemon {
    async fn start() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = Store::open(directory.path().join("fleetd.db"))
            .await
            .expect("open store");
        let auth = AuthService::new(store.clone());
        let operator_path = directory.path().join("operator.token");
        auth.ensure_operator_credential(&operator_path)
            .await
            .expect("provision operator");
        let operator_token = std::fs::read_to_string(operator_path)
            .expect("read operator token")
            .trim()
            .to_owned();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind daemon");
        let address = listener.local_addr().expect("bound daemon address");
        let state = AppState::new(store)
            .with_browser_stream_listener(address)
            .expect("configure browser stream edge");
        assert_eq!(
            state.browser_origin(),
            Some(format!("http://{address}").as_str())
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, router(state))
                .await
                .expect("serve daemon");
        });
        Self {
            _directory: directory,
            auth,
            operator_token,
            address,
            server,
        }
    }

    async fn register(&self, name: &str) -> RegisteredAgent {
        self.auth
            .register_agent(CreateAgent {
                name: name.to_owned(),
                metadata: json!({"private": name}),
            })
            .await
            .expect("register agent")
    }

    async fn channel(&self, name: &str, member_ids: Vec<String>) -> fleetd::Channel {
        reqwest::Client::new()
            .post(format!("http://{}/v1/channels", self.address))
            .bearer_auth(&self.operator_token)
            .json(&CreateChannel {
                name: name.to_owned(),
                metadata: json!({}),
                member_ids,
                members: Vec::new(),
            })
            .send()
            .await
            .expect("create channel request")
            .error_for_status()
            .expect("create channel response")
            .json()
            .await
            .expect("channel body")
    }

    async fn issue(
        &self,
        channel_id: &str,
        token: &str,
        after: i64,
    ) -> BrowserStreamGrantIssueResponse {
        let response = reqwest::Client::new()
            .post(format!(
                "http://{}/v1/channels/{channel_id}/stream-grants",
                self.address
            ))
            .bearer_auth(token)
            .json(&json!({
                "after": after,
                "protocol": BROWSER_STREAM_PROTOCOL
            }))
            .send()
            .await
            .expect("issue grant request");
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        assert_eq!(
            response
                .headers()
                .get(reqwest::header::CACHE_CONTROL)
                .expect("cache control"),
            "no-store"
        );
        response.json().await.expect("grant response")
    }

    async fn send(
        &self,
        channel_id: &str,
        token: &str,
        recipient_id: Option<String>,
        kind: &str,
        payload: Value,
    ) -> Message {
        reqwest::Client::new()
            .post(format!(
                "http://{}/v1/channels/{channel_id}/messages",
                self.address
            ))
            .bearer_auth(token)
            .json(&SendMessage {
                idempotency_key: None,
                recipient_id,
                kind: kind.to_owned(),
                payload,
                correlation_id: None,
                causation_id: None,
            })
            .send()
            .await
            .expect("append message request")
            .error_for_status()
            .expect("append message response")
            .json()
            .await
            .expect("message body")
    }
}

impl Drop for BrowserDaemon {
    fn drop(&mut self) {
        self.server.abort();
    }
}

type BrowserSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
async fn browser_stream_replays_filters_continues_and_reconnects() {
    let daemon = BrowserDaemon::start().await;
    let author = daemon.register("browser-author").await;
    let recipient = daemon.register("browser-recipient").await;
    let watcher = daemon.register("browser-watcher").await;
    let channel = daemon
        .channel(
            "browser-replay",
            vec![
                author.agent.id.clone(),
                recipient.agent.id.clone(),
                watcher.agent.id.clone(),
            ],
        )
        .await;
    let hidden = daemon
        .send(
            &channel.id,
            &author.credential.token,
            Some(recipient.agent.id.clone()),
            "private.unknown/v7",
            json!({"must_not_leak": true}),
        )
        .await;
    let visible = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "broadcast.unknown/v9",
            json!({"extension": {"preserved": [1, 2, 3]}}),
        )
        .await;

    let issued = daemon
        .issue(&channel.id, &watcher.credential.token, 0)
        .await;
    let (mut socket, response) = connect_browser(&daemon, None, None).await.expect("upgrade");
    assert_eq!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .expect("selected protocol"),
        BROWSER_STREAM_PROTOCOL
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(100), socket.next())
            .await
            .is_err(),
        "the server must release no application data before redemption"
    );
    redeem(&mut socket, issued).await;
    assert_ready(next_server_frame(&mut socket).await, &channel.id, 0);
    assert_message(next_server_frame(&mut socket).await, &visible);

    let live = daemon
        .send(
            &channel.id,
            &author.credential.token,
            Some(watcher.agent.id.clone()),
            "direct.opaque/v4",
            json!({"after": "redemption"}),
        )
        .await;
    assert_message(next_server_frame(&mut socket).await, &live);

    let reconnect = daemon
        .issue(&channel.id, &watcher.credential.token, visible.seq)
        .await;
    let (mut reconnected, _) = connect_browser(&daemon, None, None)
        .await
        .expect("reconnect upgrade");
    redeem(&mut reconnected, reconnect).await;
    assert_ready(
        next_server_frame(&mut reconnected).await,
        &channel.id,
        visible.seq,
    );
    assert_message(next_server_frame(&mut reconnected).await, &live);
    assert!(hidden.seq < visible.seq);
}

#[tokio::test]
async fn browser_stream_preserves_operator_scope_and_revokes_before_delivery() {
    let daemon = BrowserDaemon::start().await;
    let author = daemon.register("operator-scope-author").await;
    let recipient = daemon.register("operator-scope-recipient").await;
    let channel = daemon
        .channel(
            "browser-operator",
            vec![author.agent.id.clone(), recipient.agent.id.clone()],
        )
        .await;
    let private = daemon
        .send(
            &channel.id,
            &author.credential.token,
            Some(recipient.agent.id.clone()),
            "operator-visible-private/v1",
            json!({"opaque": true}),
        )
        .await;

    let operator_grant = daemon.issue(&channel.id, &daemon.operator_token, 0).await;
    let (mut operator_socket, _) = connect_browser(&daemon, None, None)
        .await
        .expect("operator upgrade");
    redeem(&mut operator_socket, operator_grant).await;
    assert_ready(
        next_server_frame(&mut operator_socket).await,
        &channel.id,
        0,
    );
    assert_message(next_server_frame(&mut operator_socket).await, &private);

    let member_grant = daemon
        .issue(&channel.id, &recipient.credential.token, private.seq)
        .await;
    let (mut member_socket, _) = connect_browser(&daemon, None, None)
        .await
        .expect("member upgrade");
    redeem(&mut member_socket, member_grant).await;
    assert_ready(
        next_server_frame(&mut member_socket).await,
        &channel.id,
        private.seq,
    );
    daemon
        .auth
        .rotate_agent_credential(&recipient.agent.id)
        .await
        .expect("rotate active viewer credential");
    daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "post-revocation/v1",
            json!({"must_not_be_released": true}),
        )
        .await;
    assert_close(&mut member_socket, 4_401, "grant_rejected").await;
}

#[tokio::test]
async fn browser_live_delivery_follows_durable_sequence_under_concurrent_appends() {
    let daemon = BrowserDaemon::start().await;
    let author = daemon.register("ordered-author").await;
    let watcher = daemon.register("ordered-watcher").await;
    let channel = daemon
        .channel(
            "browser-ordering",
            vec![author.agent.id.clone(), watcher.agent.id.clone()],
        )
        .await;
    let grant = daemon
        .issue(&channel.id, &watcher.credential.token, 0)
        .await;
    let (mut socket, _) = connect_browser(&daemon, None, None)
        .await
        .expect("ordered stream upgrade");
    redeem(&mut socket, grant).await;
    assert_ready(next_server_frame(&mut socket).await, &channel.id, 0);

    let sends = (0..16).map(|index| {
        daemon.send(
            &channel.id,
            &author.credential.token,
            None,
            "concurrent.opaque/v1",
            json!({"index": index}),
        )
    });
    let mut expected = join_all(sends).await;
    expected.sort_by_key(|message| message.seq);
    let mut actual = Vec::new();
    for _ in 0..expected.len() {
        match next_server_frame(&mut socket).await {
            BrowserStreamServerFrame::Message { message } => actual.push(*message),
            BrowserStreamServerFrame::Ready { .. } => panic!("unexpected second ready frame"),
        }
    }
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn browser_edge_fails_closed_before_and_after_upgrade() {
    let daemon = BrowserDaemon::start().await;
    let member = daemon.register("edge-member").await;
    let outsider = daemon.register("edge-outsider").await;
    let channel = daemon
        .channel("browser-edge", vec![member.agent.id.clone()])
        .await;

    let no_credential = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/channels/{}/stream-grants",
            daemon.address, channel.id
        ))
        .json(&json!({"after": 0, "protocol": BROWSER_STREAM_PROTOCOL}))
        .send()
        .await
        .expect("unauthenticated issuance");
    assert_eq!(no_credential.status(), reqwest::StatusCode::UNAUTHORIZED);
    let non_member = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/channels/{}/stream-grants",
            daemon.address, channel.id
        ))
        .bearer_auth(&outsider.credential.token)
        .json(&json!({"after": 0, "protocol": BROWSER_STREAM_PROTOCOL}))
        .send()
        .await
        .expect("non-member issuance");
    assert_eq!(non_member.status(), reqwest::StatusCode::FORBIDDEN);
    let wrong_protocol = reqwest::Client::new()
        .post(format!(
            "http://{}/v1/channels/{}/stream-grants",
            daemon.address, channel.id
        ))
        .bearer_auth(&member.credential.token)
        .json(&json!({"after": 0, "protocol": "another-protocol"}))
        .send()
        .await
        .expect("wrong protocol issuance");
    assert_eq!(
        wrong_protocol.status(),
        reqwest::StatusCode::UNPROCESSABLE_ENTITY
    );

    let wrong_origin = connect_browser(&daemon, Some("http://127.0.0.1:1"), None)
        .await
        .expect_err("wrong origin must fail before upgrade");
    assert_http_error(wrong_origin, 403);
    let wrong_subprotocol = connect_browser(&daemon, None, Some("another-protocol"))
        .await
        .expect_err("wrong protocol must fail before upgrade");
    assert_http_error(wrong_subprotocol, 403);

    let invalid_frame_grant = daemon.issue(&channel.id, &member.credential.token, 0).await;
    let (mut invalid_socket, _) = connect_browser(&daemon, None, None)
        .await
        .expect("invalid-frame upgrade");
    invalid_socket
        .send(WebSocketMessage::Binary(vec![0, 1, 2].into()))
        .await
        .expect("send invalid first frame");
    assert_close(&mut invalid_socket, 4_400, "invalid_handshake").await;
    drop(invalid_frame_grant);

    let one_use = daemon.issue(&channel.id, &member.credential.token, 0).await;
    let replayed_grant = one_use.grant.clone();
    let (mut first, _) = connect_browser(&daemon, None, None)
        .await
        .expect("first redemption upgrade");
    redeem(&mut first, one_use).await;
    assert_ready(next_server_frame(&mut first).await, &channel.id, 0);
    first.close(None).await.expect("close first stream");
    let (mut second, _) = connect_browser(&daemon, None, None)
        .await
        .expect("second redemption upgrade");
    let body = BrowserStreamRedemptionRequest {
        message_type: BrowserStreamRedemptionMessageType::Redeem,
        grant: replayed_grant,
    };
    second
        .send(WebSocketMessage::Text(
            serde_json::to_string(&body)
                .expect("serialize redemption")
                .into(),
        ))
        .await
        .expect("send reused grant");
    assert_close(&mut second, 4_401, "grant_rejected").await;
}

async fn connect_browser(
    daemon: &BrowserDaemon,
    origin: Option<&str>,
    subprotocol: Option<&str>,
) -> Result<
    (
        BrowserSocket,
        tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
    ),
    WebSocketError,
> {
    connect_async(browser_request(
        daemon.address,
        origin.unwrap_or(&format!("http://{}", daemon.address)),
        subprotocol.unwrap_or(BROWSER_STREAM_PROTOCOL),
    ))
    .await
}

fn browser_request(address: SocketAddr, origin: &str, subprotocol: &str) -> Request<()> {
    let mut request = format!("ws://{address}/v1/browser/channel-stream")
        .into_client_request()
        .expect("browser stream request");
    request.headers_mut().insert(
        ORIGIN,
        HeaderValue::from_str(origin).expect("origin header"),
    );
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_str(subprotocol).expect("subprotocol header"),
    );
    request
}

async fn redeem(socket: &mut BrowserSocket, issued: BrowserStreamGrantIssueResponse) {
    assert_eq!(issued.protocol.as_str(), BROWSER_STREAM_PROTOCOL);
    assert_eq!(issued.websocket_path.as_str(), "/v1/browser/channel-stream");
    let redemption = BrowserStreamRedemptionRequest {
        message_type: BrowserStreamRedemptionMessageType::Redeem,
        grant: issued.grant,
    };
    socket
        .send(WebSocketMessage::Text(
            serde_json::to_string(&redemption)
                .expect("serialize redemption")
                .into(),
        ))
        .await
        .expect("send redemption");
}

async fn next_server_frame(socket: &mut BrowserSocket) -> BrowserStreamServerFrame {
    let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("server frame timeout")
        .expect("stream open")
        .expect("valid server frame");
    serde_json::from_str(frame.to_text().expect("text frame")).expect("tagged server frame")
}

fn assert_ready(frame: BrowserStreamServerFrame, channel_id: &str, after: i64) {
    match frame {
        BrowserStreamServerFrame::Ready {
            protocol,
            channel_id: actual_channel,
            after: actual_after,
        } => {
            assert_eq!(protocol.as_str(), BROWSER_STREAM_PROTOCOL);
            assert_eq!(actual_channel, channel_id);
            assert_eq!(actual_after.get(), after);
        }
        BrowserStreamServerFrame::Message { .. } => panic!("expected ready frame"),
    }
}

fn assert_message(frame: BrowserStreamServerFrame, expected: &Message) {
    match frame {
        BrowserStreamServerFrame::Message { message } => assert_eq!(*message, *expected),
        BrowserStreamServerFrame::Ready { .. } => panic!("expected message frame"),
    }
}

async fn assert_close(socket: &mut BrowserSocket, code: u16, reason: &str) {
    let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("close timeout")
        .expect("close frame")
        .expect("valid close frame");
    let WebSocketMessage::Close(Some(close)) = frame else {
        panic!("expected WebSocket close frame");
    };
    assert_eq!(u16::from(close.code), code);
    assert_eq!(close.reason, reason);
}

fn assert_http_error(error: WebSocketError, status: u16) {
    match error {
        WebSocketError::Http(response) => assert_eq!(response.status().as_u16(), status),
        other => panic!("expected HTTP rejection, got {other}"),
    }
}
