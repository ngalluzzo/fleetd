use std::{net::SocketAddr, time::Duration};

use fleetd::{
    api::{AppState, router},
    auth::AuthService,
    browser_stream_edge::{
        BROWSER_STREAM_PROTOCOL, BrowserStreamGrantIssueResponse, BrowserStreamServerFrame,
    },
    model::{CreateAgent, CreateChannel, RegisteredAgent},
    store::Store,
};
use futures_util::{SinkExt, StreamExt, future::join_all, stream};
use serde_json::json;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error as WebSocketError, Message as WebSocketMessage,
        client::IntoClientRequest,
        http::{
            HeaderValue, Request,
            header::{HOST, ORIGIN, SEC_WEBSOCKET_PROTOCOL},
        },
    },
};

const MAX_UNUSED_GRANTS_PER_CREDENTIAL: usize = 8;
const MAX_PRE_AUTHENTICATION_SOCKETS_PER_DAEMON: usize = 64;
const MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL: usize = 16;
const MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON: usize = 1_024;
const MAX_REDEMPTION_FRAME_BYTES: usize = 1_024;
const FIRST_FRAME_DEADLINE: Duration = Duration::from_secs(5);
const GRANT_LIFETIME: Duration = Duration::from_secs(15);
const CREDENTIAL_REVALIDATION_INTERVAL: Duration = Duration::from_secs(30);

struct BrowserDaemon {
    _directory: tempfile::TempDir,
    client: reqwest::Client,
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
        let server = tokio::spawn(async move {
            axum::serve(listener, router(state))
                .await
                .expect("serve daemon");
        });
        Self {
            _directory: directory,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("build bounded HTTP client"),
            operator_token,
            address,
            server,
        }
    }

    async fn register(&self, name: &str) -> RegisteredAgent {
        self.client
            .post(format!("http://{}/v1/agents", self.address))
            .bearer_auth(&self.operator_token)
            .json(&CreateAgent {
                name: name.to_owned(),
                metadata: json!({}),
            })
            .send()
            .await
            .expect("register agent request")
            .error_for_status()
            .expect("register agent response")
            .json()
            .await
            .expect("registration body")
    }

    async fn rotate(&self, agent_id: &str) {
        self.client
            .post(format!(
                "http://{}/v1/agents/{agent_id}/credentials/rotate",
                self.address
            ))
            .bearer_auth(&self.operator_token)
            .send()
            .await
            .expect("rotate credential request")
            .error_for_status()
            .expect("rotate credential response");
    }

    async fn channel(&self, member_id: &str) -> fleetd::model::Channel {
        self.channel_with_members(vec![member_id.to_owned()]).await
    }

    async fn channel_with_members(&self, member_ids: Vec<String>) -> fleetd::model::Channel {
        self.client
            .post(format!("http://{}/v1/channels", self.address))
            .bearer_auth(&self.operator_token)
            .json(&CreateChannel {
                name: "browser-fail-closed".to_owned(),
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

    async fn issue_response(&self, channel_id: &str, token: &str) -> reqwest::Response {
        self.client
            .post(format!(
                "http://{}/v1/channels/{channel_id}/stream-grants",
                self.address
            ))
            .bearer_auth(token)
            .json(&json!({
                "after": 0,
                "protocol": BROWSER_STREAM_PROTOCOL
            }))
            .send()
            .await
            .expect("issue grant request")
    }

    async fn issue(&self, channel_id: &str, token: &str) -> BrowserStreamGrantIssueResponse {
        let response = self.issue_response(channel_id, token).await;
        assert_eq!(response.status(), reqwest::StatusCode::CREATED);
        response.json().await.expect("grant response")
    }
}

impl Drop for BrowserDaemon {
    fn drop(&mut self) {
        self.server.abort();
    }
}

type BrowserSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
async fn browser_upgrade_rejects_missing_opaque_alias_and_rebound_origins() {
    let daemon = BrowserDaemon::start().await;
    for origin in [
        None,
        Some("null"),
        Some(&format!("https://{}", daemon.address)),
        Some(&format!("http://localhost:{}", daemon.address.port())),
        Some(&format!("http://127.0.0.1.evil:{}", daemon.address.port())),
    ] {
        let error = connect_browser(&daemon, origin)
            .await
            .expect_err("untrusted origin must fail before upgrade");
        assert_http_error(error, 403);
    }

    let mut wrong_authority = browser_request(daemon.address, Some(&canonical_origin(&daemon)));
    wrong_authority
        .headers_mut()
        .insert(HOST, HeaderValue::from_static("127.0.0.1:1"));
    let error = connect_async(wrong_authority)
        .await
        .expect_err("wrong authority must fail before upgrade");
    assert_http_error(error, 403);
}

#[tokio::test]
async fn malformed_oversized_and_late_redemption_fail_with_fixed_closes() {
    let daemon = BrowserDaemon::start().await;

    for invalid in ["{}".to_owned(), "not-json".to_owned()] {
        let mut socket = connect_browser(&daemon, Some(&canonical_origin(&daemon)))
            .await
            .expect("upgrade")
            .0;
        socket
            .send(WebSocketMessage::Text(invalid.into()))
            .await
            .expect("send invalid redemption");
        assert_close(
            &mut socket,
            4_400,
            "invalid_handshake",
            Duration::from_secs(2),
        )
        .await;
    }

    let mut oversized = connect_browser(&daemon, Some(&canonical_origin(&daemon)))
        .await
        .expect("oversized upgrade")
        .0;
    oversized
        .send(WebSocketMessage::Text(
            "x".repeat(MAX_REDEMPTION_FRAME_BYTES + 1).into(),
        ))
        .await
        .expect("send oversized redemption");
    assert_close(
        &mut oversized,
        4_400,
        "invalid_handshake",
        Duration::from_secs(2),
    )
    .await;

    let mut late = connect_browser(&daemon, Some(&canonical_origin(&daemon)))
        .await
        .expect("late upgrade")
        .0;
    assert_close(
        &mut late,
        4_408,
        "grant_timeout",
        FIRST_FRAME_DEADLINE + Duration::from_secs(2),
    )
    .await;
}

#[tokio::test]
async fn expired_and_revoked_grants_fail_with_the_same_rejection() {
    let daemon = BrowserDaemon::start().await;
    let member = daemon.register("redemption-member").await;
    let channel = daemon.channel(&member.agent.id).await;

    let mut expiring = Vec::new();
    for _ in 0..MAX_UNUSED_GRANTS_PER_CREDENTIAL {
        expiring.push(daemon.issue(&channel.id, &member.credential.token).await);
    }
    assert_eq!(
        daemon
            .issue_response(&channel.id, &member.credential.token)
            .await
            .status(),
        reqwest::StatusCode::TOO_MANY_REQUESTS
    );
    tokio::time::sleep(GRANT_LIFETIME + Duration::from_millis(100)).await;
    let _replacement = daemon.issue(&channel.id, &member.credential.token).await;
    let expired = expiring.pop().expect("issued expiring grant");
    let mut expired_socket = connect_browser(&daemon, Some(&canonical_origin(&daemon)))
        .await
        .expect("expired grant upgrade")
        .0;
    redeem_raw(&mut expired_socket, expired.grant.expose_secret()).await;
    assert_close(
        &mut expired_socket,
        4_401,
        "grant_rejected",
        Duration::from_secs(2),
    )
    .await;

    let revoked = daemon.issue(&channel.id, &member.credential.token).await;
    daemon.rotate(&member.agent.id).await;
    let mut revoked_socket = connect_browser(&daemon, Some(&canonical_origin(&daemon)))
        .await
        .expect("revoked grant upgrade")
        .0;
    redeem_raw(&mut revoked_socket, revoked.grant.expose_secret()).await;
    assert_close(
        &mut revoked_socket,
        4_401,
        "grant_rejected",
        Duration::from_secs(2),
    )
    .await;
}

#[tokio::test]
async fn concurrent_redemption_has_one_ready_stream_and_only_fixed_rejections() {
    let daemon = BrowserDaemon::start().await;
    let member = daemon.register("race-member").await;
    let channel = daemon.channel(&member.agent.id).await;
    let issued = daemon.issue(&channel.id, &member.credential.token).await;
    let raw_grant = issued.grant.expose_secret().to_owned();

    let sockets = join_all((0..16).map(|_| async {
        connect_browser(&daemon, Some(&canonical_origin(&daemon)))
            .await
            .expect("race upgrade")
            .0
    }))
    .await;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(sockets.len()));
    let outcomes = join_all(sockets.into_iter().map(|mut socket| {
        let barrier = std::sync::Arc::clone(&barrier);
        let raw_grant = raw_grant.clone();
        async move {
            barrier.wait().await;
            redeem_raw(&mut socket, &raw_grant).await;
            let frame = next_frame(&mut socket, Duration::from_secs(2)).await;
            match frame {
                WebSocketMessage::Text(text) => {
                    let frame: BrowserStreamServerFrame =
                        serde_json::from_str(&text).expect("server frame");
                    assert!(matches!(frame, BrowserStreamServerFrame::Ready { .. }));
                    1_usize
                }
                WebSocketMessage::Close(Some(close)) => {
                    assert_eq!(u16::from(close.code), 4_401);
                    assert_eq!(close.reason, "grant_rejected");
                    0
                }
                other => panic!("unexpected redemption race frame: {other:?}"),
            }
        }
    }))
    .await;
    assert_eq!(outcomes.into_iter().sum::<usize>(), 1);
}

#[tokio::test]
async fn unused_grant_bound_rejects_excess_and_redemption_releases_capacity() {
    let daemon = BrowserDaemon::start().await;
    let member = daemon.register("unused-bound-member").await;
    let channel = daemon.channel(&member.agent.id).await;
    let mut grants = Vec::new();
    for _ in 0..MAX_UNUSED_GRANTS_PER_CREDENTIAL {
        grants.push(daemon.issue(&channel.id, &member.credential.token).await);
    }
    let excess = daemon
        .issue_response(&channel.id, &member.credential.token)
        .await;
    assert_eq!(excess.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);

    let released = grants.pop().expect("issued grant");
    let mut socket = connect_browser(&daemon, Some(&canonical_origin(&daemon)))
        .await
        .expect("redemption upgrade")
        .0;
    redeem_raw(&mut socket, released.grant.expose_secret()).await;
    assert_ready(&next_frame(&mut socket, Duration::from_secs(2)).await);
    assert_eq!(
        daemon
            .issue_response(&channel.id, &member.credential.token)
            .await
            .status(),
        reqwest::StatusCode::CREATED
    );
}

#[tokio::test]
async fn preauthentication_bound_rejects_before_upgrade_and_close_releases_capacity() {
    let daemon = BrowserDaemon::start().await;
    let origin = canonical_origin(&daemon);
    let mut sockets = Vec::new();
    for _ in 0..MAX_PRE_AUTHENTICATION_SOCKETS_PER_DAEMON {
        sockets.push(
            connect_browser(&daemon, Some(&origin))
                .await
                .expect("pre-authentication upgrade")
                .0,
        );
    }
    let excess = connect_browser(&daemon, Some(&origin))
        .await
        .expect_err("excess pre-authentication socket must fail before upgrade");
    assert_http_error(excess, 503);

    let mut released = sockets.pop().expect("held pre-authentication socket");
    released.close(None).await.expect("close held socket");
    drop(released);
    let replacement = eventually_connect_browser(&daemon, &origin).await;
    drop(replacement);
}

#[tokio::test]
async fn per_credential_active_bound_consumes_excess_grant_and_close_releases_capacity() {
    let daemon = BrowserDaemon::start().await;
    let member = daemon.register("active-bound-member").await;
    let channel = daemon.channel(&member.agent.id).await;
    let origin = canonical_origin(&daemon);
    let mut active = Vec::new();
    for _ in 0..MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL {
        let issued = daemon.issue(&channel.id, &member.credential.token).await;
        let mut socket = connect_browser(&daemon, Some(&origin))
            .await
            .expect("active stream upgrade")
            .0;
        redeem_raw(&mut socket, issued.grant.expose_secret()).await;
        assert_ready(&next_frame(&mut socket, Duration::from_secs(2)).await);
        active.push(socket);
    }

    let excess = daemon.issue(&channel.id, &member.credential.token).await;
    let mut rejected = connect_browser(&daemon, Some(&origin))
        .await
        .expect("excess active stream upgrades before redemption")
        .0;
    redeem_raw(&mut rejected, excess.grant.expose_secret()).await;
    assert_close(
        &mut rejected,
        4_401,
        "grant_rejected",
        Duration::from_secs(2),
    )
    .await;

    let mut released = active.pop().expect("held active stream");
    released.close(None).await.expect("close active stream");
    drop(released);
    let replacement_socket =
        eventually_redeem_browser(&daemon, &channel.id, &member.credential.token, &origin).await;
    drop(replacement_socket);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_active_bound_consumes_excess_grants_and_releases_exactly_one_slot() {
    tokio::time::timeout(
        Duration::from_mins(1),
        Box::pin(qualify_global_active_bound()),
    )
    .await
    .expect("global active-bound qualification must finish within its total deadline");
}

async fn qualify_global_active_bound() {
    const CREDENTIALS_AT_CAPACITY: usize =
        MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON / MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL;
    const SETUP_CONCURRENCY: usize = MAX_PRE_AUTHENTICATION_SOCKETS_PER_DAEMON;

    let daemon = BrowserDaemon::start().await;
    let mut member_ids = Vec::with_capacity(CREDENTIALS_AT_CAPACITY + 1);
    let mut tokens = Vec::with_capacity(CREDENTIALS_AT_CAPACITY + 1);
    for index in 0..=CREDENTIALS_AT_CAPACITY {
        let member = daemon
            .register(&format!("global-bound-member-{index}"))
            .await;
        member_ids.push(member.agent.id);
        tokens.push(member.credential.token);
    }
    let channel = daemon.channel_with_members(member_ids).await;

    // Interleave credentials so each setup wave has at most one outstanding
    // grant per credential. The last capacity credential intentionally holds
    // 15 streams, giving the daemon exactly 1,023 active streams.
    let mut activation_tokens = Vec::with_capacity(MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON - 1);
    for round in 0..MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL {
        for (index, token) in tokens.iter().take(CREDENTIALS_AT_CAPACITY).enumerate() {
            if round + 1 == MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL
                && index + 1 == CREDENTIALS_AT_CAPACITY
            {
                continue;
            }
            activation_tokens.push(token.clone());
        }
    }
    assert_eq!(
        activation_tokens.len(),
        MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON - 1
    );

    let active: Vec<BrowserSocket> = tokio::time::timeout(
        Duration::from_secs(45),
        stream::iter(activation_tokens)
            .map(|token| activate_browser(&daemon, &channel.id, token))
            .buffer_unordered(SETUP_CONCURRENCY)
            .collect(),
    )
    .await
    .expect("1,023 public browser streams must establish within the setup deadline");
    assert_eq!(active.len(), MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON - 1);

    let boundary_token = tokens.last().expect("dedicated global-bound credential");
    let fill_grant = daemon.issue(&channel.id, boundary_token).await;
    let excess_grant = daemon.issue(&channel.id, boundary_token).await;
    let recovery_grant = daemon.issue(&channel.id, boundary_token).await;
    let second_excess_grant = daemon.issue(&channel.id, boundary_token).await;
    let origin = canonical_origin(&daemon);

    // All four upgrades happen while the global count is 1,023. This reaches
    // the authoritative reservation race instead of stopping at the advisory
    // HTTP 503 capacity check.
    let (fill, excess, recovery, second_excess) = tokio::join!(
        connect_browser(&daemon, Some(&origin)),
        connect_browser(&daemon, Some(&origin)),
        connect_browser(&daemon, Some(&origin)),
        connect_browser(&daemon, Some(&origin)),
    );
    let mut fill = fill.expect("global-bound fill upgrade").0;
    let mut excess = excess.expect("global-bound excess upgrade").0;
    let mut recovery = recovery.expect("global-bound recovery upgrade").0;
    let mut second_excess = second_excess.expect("second global-bound excess upgrade").0;

    redeem_raw(&mut fill, fill_grant.grant.expose_secret()).await;
    assert_ready(&next_frame(&mut fill, Duration::from_secs(2)).await);

    redeem_raw(&mut excess, excess_grant.grant.expose_secret()).await;
    assert_close(&mut excess, 4_401, "grant_rejected", Duration::from_secs(2)).await;

    close_and_wait(fill).await;

    // A successful upgrade is the public synchronization point proving that
    // the closed stream's global slot has actually been released. The rejected
    // grant remains rejected after that release, proving redemption consumed it.
    let mut consumed_retry = connect_after_capacity_release(&daemon, &origin).await;
    redeem_raw(&mut consumed_retry, excess_grant.grant.expose_secret()).await;
    assert_close(
        &mut consumed_retry,
        4_401,
        "grant_rejected",
        Duration::from_secs(2),
    )
    .await;

    redeem_raw(&mut recovery, recovery_grant.grant.expose_secret()).await;
    assert_ready(&next_frame(&mut recovery, Duration::from_secs(2)).await);

    // Closing one established stream released one slot, not two: exactly one
    // fresh redemption establishes and the next already-upgraded redemption is
    // rejected by the daemon-wide bound, well below its credential-local bound.
    redeem_raw(
        &mut second_excess,
        second_excess_grant.grant.expose_secret(),
    )
    .await;
    assert_close(
        &mut second_excess,
        4_401,
        "grant_rejected",
        Duration::from_secs(2),
    )
    .await;

    drop(recovery);
    drop(active);
}

#[tokio::test]
async fn idle_revalidation_closes_a_revoked_ready_stream_within_its_bound() {
    let daemon = BrowserDaemon::start().await;
    let member = daemon.register("idle-revalidation-member").await;
    let channel = daemon.channel(&member.agent.id).await;
    let issued = daemon.issue(&channel.id, &member.credential.token).await;
    let mut socket = connect_browser(&daemon, Some(&canonical_origin(&daemon)))
        .await
        .expect("idle stream upgrade")
        .0;
    redeem_raw(&mut socket, issued.grant.expose_secret()).await;
    assert_ready(&next_frame(&mut socket, Duration::from_secs(2)).await);

    daemon.rotate(&member.agent.id).await;
    assert_close(
        &mut socket,
        4_401,
        "grant_rejected",
        CREDENTIAL_REVALIDATION_INTERVAL + Duration::from_secs(2),
    )
    .await;
}

fn canonical_origin(daemon: &BrowserDaemon) -> String {
    format!("http://{}", daemon.address)
}

async fn connect_browser(
    daemon: &BrowserDaemon,
    origin: Option<&str>,
) -> Result<
    (
        BrowserSocket,
        tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
    ),
    WebSocketError,
> {
    tokio::time::timeout(
        Duration::from_secs(5),
        connect_async(browser_request(daemon.address, origin)),
    )
    .await
    .expect("browser stream upgrade must finish within the deadline")
}

fn browser_request(address: SocketAddr, origin: Option<&str>) -> Request<()> {
    let mut request = format!("ws://{address}/v1/browser/channel-stream")
        .into_client_request()
        .expect("browser stream request");
    if let Some(origin) = origin {
        request.headers_mut().insert(
            ORIGIN,
            HeaderValue::from_str(origin).expect("origin header"),
        );
    }
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(BROWSER_STREAM_PROTOCOL),
    );
    request
}

async fn eventually_connect_browser(daemon: &BrowserDaemon, origin: &str) -> BrowserSocket {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match connect_browser(daemon, Some(origin)).await {
            Ok((socket, _)) => return socket,
            Err(WebSocketError::Http(response))
                if response.status().as_u16() == 503 && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("pre-authentication capacity did not recover: {error}"),
        }
    }
}

async fn eventually_redeem_browser(
    daemon: &BrowserDaemon,
    channel_id: &str,
    token: &str,
    origin: &str,
) -> BrowserSocket {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let issued = daemon.issue(channel_id, token).await;
        let mut socket = connect_browser(daemon, Some(origin))
            .await
            .expect("replacement stream upgrade")
            .0;
        redeem_raw(&mut socket, issued.grant.expose_secret()).await;
        match next_frame(&mut socket, Duration::from_secs(2)).await {
            frame @ WebSocketMessage::Text(_) => {
                assert_ready(&frame);
                return socket;
            }
            WebSocketMessage::Close(Some(close))
                if u16::from(close.code) == 4_401
                    && close.reason == "grant_rejected"
                    && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            other => panic!("active stream capacity did not recover: {other:?}"),
        }
    }
}

async fn activate_browser(
    daemon: &BrowserDaemon,
    channel_id: &str,
    token: String,
) -> BrowserSocket {
    let issued = daemon.issue(channel_id, &token).await;
    let mut socket = connect_browser(daemon, Some(&canonical_origin(daemon)))
        .await
        .expect("active global-bound stream upgrade")
        .0;
    redeem_raw(&mut socket, issued.grant.expose_secret()).await;
    assert_ready(&next_frame(&mut socket, Duration::from_secs(2)).await);
    socket
}

async fn close_and_wait(mut socket: BrowserSocket) {
    socket
        .close(None)
        .await
        .expect("close active browser stream");
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = socket.next().await {
            match frame {
                Ok(WebSocketMessage::Close(_)) | Err(_) => return,
                Ok(_) => {}
            }
        }
    })
    .await
    .expect("closed browser stream must terminate within the deadline");
}

async fn connect_after_capacity_release(daemon: &BrowserDaemon, origin: &str) -> BrowserSocket {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match connect_browser(daemon, Some(origin)).await {
                Ok((socket, _)) => return socket,
                Err(WebSocketError::Http(response)) if response.status().as_u16() == 503 => {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("global active capacity did not recover: {error}"),
            }
        }
    })
    .await
    .expect("global active capacity must recover within the deadline")
}

async fn redeem_raw(socket: &mut BrowserSocket, grant: &str) {
    socket
        .send(WebSocketMessage::Text(
            serde_json::to_string(&json!({"type": "redeem", "grant": grant}))
                .expect("serialize redemption")
                .into(),
        ))
        .await
        .expect("send redemption");
}

async fn next_frame(socket: &mut BrowserSocket, deadline: Duration) -> WebSocketMessage {
    tokio::time::timeout(deadline, socket.next())
        .await
        .expect("server frame timeout")
        .expect("stream open")
        .expect("valid server frame")
}

fn assert_ready(frame: &WebSocketMessage) {
    let frame: BrowserStreamServerFrame =
        serde_json::from_str(frame.to_text().expect("text ready frame")).expect("server frame");
    assert!(matches!(frame, BrowserStreamServerFrame::Ready { .. }));
}

async fn assert_close(socket: &mut BrowserSocket, code: u16, reason: &str, deadline: Duration) {
    let frame = next_frame(socket, deadline).await;
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
