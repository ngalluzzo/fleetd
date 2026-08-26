use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use fleetd::{
    auth::AuthService,
    http::AppState,
    http::browser_stream_edge::{
        BROWSER_STREAM_PROTOCOL, BrowserStreamGrantIssueResponse,
        BrowserStreamRedemptionMessageType, BrowserStreamRedemptionRequest,
        BrowserStreamServerFrame,
    },
    model::{CreateAgent, CreateChannel, Message, RegisteredAgent, SendMessage},
    store::Store,
};
use futures_util::{SinkExt, StreamExt, future::join_all, stream};
use serde_json::{Value, json};

mod common;
use sqlx::{
    Row,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async, connect_async,
    tungstenite::{
        Error as WebSocketError, Message as WebSocketMessage,
        client::IntoClientRequest,
        http::{
            HeaderMap, HeaderValue, Request,
            header::{AUTHORIZATION, ORIGIN, SEC_WEBSOCKET_PROTOCOL},
        },
    },
};

struct BrowserDaemon {
    _temporary: common::TempStore,
    auth: AuthService,
    client: reqwest::Client,
    operator_token: String,
    database_path: PathBuf,
    address: SocketAddr,
    server: tokio::task::JoinHandle<()>,
}

impl BrowserDaemon {
    async fn start() -> Self {
        let temporary = common::temp_store().await;
        let auth = AuthService::new(temporary.store.clone());
        let operator_token =
            common::bootstrap_operator(&temporary.store, temporary.directory.path()).await;
        // The edge advertises the origin it is reached on, so the listener has
        // to be bound before the state that describes it can be built.
        let (listener, address) = common::bind_loopback().await;
        let state = AppState::new(temporary.store.clone())
            .with_browser_stream_listener(address)
            .expect("configure browser stream edge");
        assert_eq!(
            state.browser_origin(),
            Some(format!("http://{address}").as_str())
        );
        let server = common::spawn_server(listener, state);
        Self {
            database_path: temporary.database_path.clone(),
            _temporary: temporary,
            auth,
            client: reqwest::Client::new(),
            operator_token,
            address,
            server,
        }
    }

    async fn restart(&mut self) {
        self.server.abort();
        let _ = (&mut self.server).await;

        let store = Store::open(&self.database_path)
            .await
            .expect("reopen durable store");
        self.auth = AuthService::new(store.clone());
        let (listener, address) = common::bind_loopback().await;
        self.address = address;
        let state = AppState::new(store)
            .with_browser_stream_listener(address)
            .expect("reconfigure browser stream edge");
        self.server = common::spawn_server(listener, state);
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

    async fn channel(&self, name: &str, member_ids: Vec<String>) -> fleetd::model::Channel {
        self.client
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
        let response = self
            .client
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
        self.client
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
const PER_CREDENTIAL_BROWSER_STREAM_CAPACITY: usize = 16;
const APPLICATION_SEND_DEADLINE: Duration = Duration::from_secs(10);
const CAPACITY_OBSERVATION_DEADLINE: Duration = Duration::from_secs(20);

#[tokio::test]
async fn browser_stream_replays_every_channel_message_and_reconnects() {
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
    let targeted = daemon
        .send(
            &channel.id,
            &author.credential.token,
            Some(recipient.agent.id.clone()),
            "targeted.unknown/v7",
            json!({"addressed_to": "recipient"}),
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
    assert_message(next_server_frame(&mut socket).await, &targeted);
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
    assert!(targeted.seq < visible.seq);
}

#[tokio::test]
async fn browser_stream_revalidates_member_credentials_before_delivery() {
    let daemon = BrowserDaemon::start().await;
    let author = daemon.register("operator-scope-author").await;
    let recipient = daemon.register("operator-scope-recipient").await;
    let channel = daemon
        .channel(
            "browser-operator",
            vec![author.agent.id.clone(), recipient.agent.id.clone()],
        )
        .await;
    let addressed = daemon
        .send(
            &channel.id,
            &author.credential.token,
            Some(recipient.agent.id.clone()),
            "operator-visible-addressed/v1",
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
    assert_message(next_server_frame(&mut operator_socket).await, &addressed);

    let member_grant = daemon
        .issue(&channel.id, &recipient.credential.token, addressed.seq)
        .await;
    let (mut member_socket, _) = connect_browser(&daemon, None, None)
        .await
        .expect("member upgrade");
    redeem(&mut member_socket, member_grant).await;
    assert_ready(
        next_server_frame(&mut member_socket).await,
        &channel.id,
        addressed.seq,
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
async fn browser_stream_has_no_gap_across_authorization_and_replay_boundaries() {
    let daemon = BrowserDaemon::start().await;
    let author = daemon.register("boundary-author").await;
    let watcher = daemon.register("boundary-watcher").await;
    let channel = daemon
        .channel(
            "browser-boundaries",
            vec![author.agent.id.clone(), watcher.agent.id.clone()],
        )
        .await;

    let grant = daemon
        .issue(&channel.id, &watcher.credential.token, 0)
        .await;
    let after_issuance = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "boundary.after-issuance/v1",
            json!({"window": "issuance-to-upgrade"}),
        )
        .await;

    let (mut socket, _) = connect_browser(&daemon, None, None)
        .await
        .expect("boundary stream upgrade");
    let after_upgrade = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "boundary.after-upgrade/v1",
            json!({"window": "upgrade-to-redemption"}),
        )
        .await;

    redeem(&mut socket, grant).await;
    let after_redemption = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "boundary.after-redemption/v1",
            json!({"window": "redemption-to-ready"}),
        )
        .await;
    assert_ready(next_server_frame(&mut socket).await, &channel.id, 0);

    let during_replay = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "boundary.during-replay/v1",
            json!({"window": "subscription-to-live-handoff"}),
        )
        .await;
    let expected = vec![
        after_issuance,
        after_upgrade,
        after_redemption,
        during_replay,
    ];
    let mut delivered = Vec::new();
    for _ in 0..expected.len() {
        delivered.push(expect_message(next_server_frame(&mut socket).await));
    }
    assert_eq!(delivered, expected);

    let live = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "boundary.live/v1",
            json!({"window": "live"}),
        )
        .await;
    assert_message(next_server_frame(&mut socket).await, &live);
}

#[tokio::test]
async fn slow_browser_disconnect_replays_broadcast_lag_from_last_accepted_cursor() {
    const BROADCAST_RECEIVER_CAPACITY: usize = 1_024;

    let daemon = BrowserDaemon::start().await;
    let author = daemon.register("backpressure-author").await;
    let watcher = daemon.register("backpressure-watcher").await;
    let channel = daemon
        .channel(
            "browser-backpressure",
            vec![author.agent.id.clone(), watcher.agent.id.clone()],
        )
        .await;
    let checkpoint = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "backpressure.checkpoint/v1",
            json!({"accepted": true}),
        )
        .await;

    let grant = daemon
        .issue(&channel.id, &watcher.credential.token, 0)
        .await;
    let (mut stalled, _) = connect_browser_with_receive_buffer(&daemon, 1_024)
        .await
        .expect("slow browser upgrade");
    redeem(&mut stalled, grant).await;
    assert_ready(next_server_frame(&mut stalled).await, &channel.id, 0);
    assert_message(next_server_frame(&mut stalled).await, &checkpoint);

    // The client accepts no more application frames. Its bounded TCP receive
    // window is smaller than the first frame, so the stream task must remain
    // in its ordinary socket-send path while later messages keep committing.
    let first_blocking_message = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "backpressure.large/v1",
            json!({"opaque": "x".repeat(1_048_576)}),
        )
        .await;
    // More notifications than the production broadcast receiver can retain
    // are committed while that send is backpressured. SQLite, not the
    // broadcast queue or socket, must therefore remain the recovery source.
    let backlog_payload = "x".repeat(4_096);
    let mut backlog = stream::iter(0..=BROADCAST_RECEIVER_CAPACITY)
        .map(|index| {
            daemon.send(
                &channel.id,
                &author.credential.token,
                None,
                "backpressure.backlog/v1",
                json!({"index": index, "opaque": backlog_payload}),
            )
        })
        .buffer_unordered(64)
        .collect::<Vec<_>>()
        .await;
    backlog.push(first_blocking_message);
    backlog.sort_by_key(|message| message.seq);

    // Model a slow browser being lost before it accepts any backlog cursor.
    drop(stalled);

    let reconnect = daemon
        .issue(&channel.id, &watcher.credential.token, checkpoint.seq)
        .await;
    let (mut replay, _) = connect_browser(&daemon, None, None)
        .await
        .expect("reconnect after slow browser loss");
    redeem(&mut replay, reconnect).await;
    assert_ready(
        next_server_frame(&mut replay).await,
        &channel.id,
        checkpoint.seq,
    );
    for expected in &backlog {
        assert_message(next_server_frame(&mut replay).await, expected);
    }

    let live = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "backpressure.live/v1",
            json!({"after_replay": true}),
        )
        .await;
    assert_message(next_server_frame(&mut replay).await, &live);
}

#[tokio::test]
async fn browser_send_deadline_releases_capacity_before_cursor_replay() {
    let daemon = BrowserDaemon::start().await;
    let author = daemon.register("send-deadline-author").await;
    let watcher = daemon.register("send-deadline-watcher").await;
    let channel = daemon
        .channel(
            "browser-send-deadline",
            vec![author.agent.id.clone(), watcher.agent.id.clone()],
        )
        .await;
    let checkpoint = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "send-deadline.checkpoint/v1",
            json!({"accepted": true}),
        )
        .await;

    let stalled_sockets = saturate_browser_stream_credential(
        &daemon,
        &channel.id,
        &watcher.credential.token,
        &checkpoint,
    )
    .await;

    assert!(
        redeem_capacity_probe(
            &daemon,
            &channel.id,
            &watcher.credential.token,
            checkpoint.seq,
        )
        .await
        .is_none(),
        "the public edge did not expose the production per-credential bound"
    );

    // Every slow client has accepted exactly the checkpoint and now stops
    // reading. A one-MiB text frame cannot drain through its 1-KiB receive
    // buffer, so the ordinary production WebSocket send path becomes the only
    // way any credential-scoped active slot can be released before the
    // 30-second idle credential revalidation interval.
    let send_started = tokio::time::Instant::now();
    let first_blocking = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "send-deadline.blocking/v1",
            json!({"part": 1, "opaque": "x".repeat(1_048_576)}),
        )
        .await;
    let second_blocking = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "send-deadline.blocking/v1",
            json!({"part": 2, "opaque": "y".repeat(1_048_576)}),
        )
        .await;
    let queued_tail = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "send-deadline.queued/v1",
            json!({"after_blocked_send": true}),
        )
        .await;

    let (mut replay, ready) = wait_for_browser_stream_capacity(
        &daemon,
        &channel.id,
        &watcher.credential.token,
        checkpoint.seq,
    )
    .await;
    let capacity_released_after = send_started.elapsed();
    assert!(
        capacity_released_after >= APPLICATION_SEND_DEADLINE,
        "browser capacity was released before the fixed application send deadline: {capacity_released_after:?}"
    );
    assert_eq!(
        stalled_sockets.len(),
        PER_CREDENTIAL_BROWSER_STREAM_CAPACITY
    );

    assert_ready(ready, &channel.id, checkpoint.seq);
    for expected in [&first_blocking, &second_blocking, &queued_tail] {
        assert_message(next_server_frame(&mut replay).await, expected);
    }

    let live = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "send-deadline.live/v1",
            json!({"after_replay": true}),
        )
        .await;
    assert_message(next_server_frame(&mut replay).await, &live);
}

#[tokio::test]
async fn browser_and_native_streams_have_identical_visibility_at_every_cursor() {
    let daemon = BrowserDaemon::start().await;
    let author = daemon.register("parity-author").await;
    let recipient = daemon.register("parity-recipient").await;
    let watcher = daemon.register("parity-watcher").await;
    let channel = daemon
        .channel(
            "browser-native-parity",
            vec![
                author.agent.id.clone(),
                recipient.agent.id.clone(),
                watcher.agent.id.clone(),
            ],
        )
        .await;

    let targeted_first = daemon
        .send(
            &channel.id,
            &author.credential.token,
            Some(recipient.agent.id.clone()),
            "targeted.future-contract/v41",
            json!({"addressed_extension": {"preserved": [true, null, 7]}}),
        )
        .await;
    let broadcast = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "future.broadcast-contract/v99",
            json!({
                "extension": {
                    "nested": [1, {"unrecognized": "preserved"}],
                    "nullable": null
                }
            }),
        )
        .await;
    let direct = daemon
        .send(
            &channel.id,
            &author.credential.token,
            Some(watcher.agent.id.clone()),
            "future.direct-contract/v73",
            json!({"opaque": {"array": [3, 2, 1], "enabled": false}}),
        )
        .await;
    let targeted_last = daemon
        .send(
            &channel.id,
            &recipient.credential.token,
            Some(author.agent.id.clone()),
            "targeted.reply/v2",
            json!({"addressed_to": "author"}),
        )
        .await;
    let final_broadcast = daemon
        .send(
            &channel.id,
            &recipient.credential.token,
            None,
            "future.final/v5",
            json!({"unknown_fields": {"survive": true}}),
        )
        .await;

    let all = [
        targeted_first,
        broadcast.clone(),
        direct.clone(),
        targeted_last,
        final_broadcast.clone(),
    ];
    // Both transports now replay the whole log, so every cursor sees every
    // message after it rather than skipping the addressed ones.
    let visible = all.clone();
    let cursors = std::iter::once(0)
        .chain(all.iter().map(|message| message.seq))
        .collect::<Vec<_>>();
    for after in cursors {
        let expected = visible
            .iter()
            .filter(|message| message.seq > after)
            .cloned()
            .collect::<Vec<_>>();
        let native = collect_native(
            &daemon,
            &channel.id,
            &watcher.credential.token,
            after,
            expected.len(),
        )
        .await;
        let browser = collect_browser(
            &daemon,
            &channel.id,
            &watcher.credential.token,
            after,
            expected.len(),
        )
        .await;
        assert_eq!(native, expected, "native visibility after cursor {after}");
        assert_eq!(browser, expected, "browser visibility after cursor {after}");
    }
}

#[tokio::test]
async fn daemon_restart_invalidates_unused_grants_and_retains_durable_replay() {
    let mut daemon = BrowserDaemon::start().await;
    let author = daemon.register("restart-author").await;
    let watcher = daemon.register("restart-watcher").await;
    let channel = daemon
        .channel(
            "browser-restart",
            vec![author.agent.id.clone(), watcher.agent.id.clone()],
        )
        .await;
    let durable = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "restart.opaque/v17",
            json!({"persisted": {"unknown": ["yes"]}}),
        )
        .await;
    let stale = daemon
        .issue(&channel.id, &watcher.credential.token, 0)
        .await;

    daemon.restart().await;

    let (mut stale_socket, _) = connect_browser(&daemon, None, None)
        .await
        .expect("upgrade after restart");
    redeem(&mut stale_socket, stale).await;
    assert_close(&mut stale_socket, 4_401, "grant_rejected").await;

    let replay = daemon
        .issue(&channel.id, &watcher.credential.token, 0)
        .await;
    let (mut replay_socket, _) = connect_browser(&daemon, None, None)
        .await
        .expect("replay upgrade after restart");
    redeem(&mut replay_socket, replay).await;
    assert_ready(next_server_frame(&mut replay_socket).await, &channel.id, 0);
    assert_message(next_server_frame(&mut replay_socket).await, &durable);

    let live = daemon
        .send(
            &channel.id,
            &author.credential.token,
            None,
            "restart.live/v1",
            json!({"after_restart": true}),
        )
        .await;
    assert_message(next_server_frame(&mut replay_socket).await, &live);
}

#[tokio::test]
async fn browser_transport_debug_and_sqlite_surfaces_do_not_expand_secret_exposure() {
    let daemon = BrowserDaemon::start().await;
    let member = daemon.register("secret-surface-member").await;
    let bearer = member.credential.token.clone();
    let channel = daemon
        .channel("secret-surfaces", vec![member.agent.id.clone()])
        .await;
    let issued = issue_qualified_grant(&daemon, &channel.id, &bearer).await;
    let grant = issued.grant.expose_secret().to_owned();
    let secrets = [&bearer[..], &grant[..]];
    assert_surface_omits("registration Debug", &format!("{member:?}"), &secrets);
    assert_surface_omits("grant response Debug", &format!("{issued:?}"), &secrets);
    assert_surface_omits(
        "auth service Debug",
        &format!("{:?}", daemon.auth),
        &secrets,
    );

    let (mut socket, upgrade_response) = connect_qualified_browser(&daemon, &secrets).await;
    assert_headers_omit(
        "browser upgrade response headers",
        upgrade_response.headers(),
        &secrets,
    );

    let redemption = BrowserStreamRedemptionRequest {
        message_type: BrowserStreamRedemptionMessageType::Redeem,
        grant: issued.grant,
    };
    assert_surface_omits("redemption Debug", &format!("{redemption:?}"), &secrets);
    let redemption_frame = serde_json::to_string(&redemption).expect("serialize redemption");
    assert!(redemption_frame.contains(&grant));
    assert!(!redemption_frame.contains(&bearer));
    socket
        .send(WebSocketMessage::Text(redemption_frame.into()))
        .await
        .expect("send redemption");
    let ready = next_server_frame(&mut socket).await;
    assert_ready(ready.clone(), &channel.id, 0);
    assert_surface_omits("ready frame Debug", &format!("{ready:?}"), &secrets);

    let database_values = sqlite_quoted_values(&daemon.database_path).await;
    for secret in [
        daemon.operator_token.as_str(),
        bearer.as_str(),
        grant.as_str(),
    ] {
        assert_sqlite_values_omit(&database_values, secret);
    }
}

async fn issue_qualified_grant(
    daemon: &BrowserDaemon,
    channel_id: &str,
    bearer: &str,
) -> BrowserStreamGrantIssueResponse {
    let client = reqwest::Client::new();
    let issue_url = format!(
        "http://{}/v1/channels/{channel_id}/stream-grants",
        daemon.address
    );
    let issue_request = client
        .post(&issue_url)
        .bearer_auth(bearer)
        .json(&json!({
            "after": 0,
            "protocol": BROWSER_STREAM_PROTOCOL
        }))
        .build()
        .expect("build issuance request");
    assert_surface_omits("issuance URL", issue_request.url().as_str(), &[bearer]);
    let authorization = issue_request
        .headers()
        .get(AUTHORIZATION)
        .expect("issuance authorization")
        .to_str()
        .expect("text authorization");
    assert!(
        authorization.strip_prefix("Bearer ") == Some(bearer),
        "issuance request did not carry the expected bearer"
    );

    let issue_response = client
        .execute(issue_request)
        .await
        .expect("issue grant request");
    assert_eq!(issue_response.status(), reqwest::StatusCode::CREATED);
    assert_eq!(
        issue_response.headers().get(reqwest::header::CACHE_CONTROL),
        Some(&HeaderValue::from_static("no-store"))
    );
    let response_url = issue_response.url().to_string();
    let response_headers = issue_response.headers().clone();
    let issued: BrowserStreamGrantIssueResponse =
        issue_response.json().await.expect("grant response");
    let secrets = [bearer, issued.grant.expose_secret()];
    assert_surface_omits("issuance response URL", &response_url, &secrets);
    assert_headers_omit("issuance response headers", &response_headers, &secrets);
    issued
}

async fn connect_qualified_browser(
    daemon: &BrowserDaemon,
    secrets: &[&str],
) -> (
    BrowserSocket,
    tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
) {
    let upgrade_request = browser_request(
        daemon.address,
        &format!("http://{}", daemon.address),
        BROWSER_STREAM_PROTOCOL,
    );
    assert_eq!(upgrade_request.uri().path(), "/v1/browser/channel-stream");
    assert!(upgrade_request.uri().query().is_none());
    assert!(upgrade_request.headers().get(AUTHORIZATION).is_none());
    assert_eq!(
        upgrade_request
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .expect("browser subprotocol"),
        BROWSER_STREAM_PROTOCOL
    );
    assert_surface_omits(
        "browser upgrade URL",
        &upgrade_request.uri().to_string(),
        secrets,
    );
    assert_headers_omit(
        "browser upgrade headers",
        upgrade_request.headers(),
        secrets,
    );

    let (socket, upgrade_response) = connect_async(upgrade_request)
        .await
        .expect("browser upgrade");
    assert_eq!(
        upgrade_response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .expect("selected protocol"),
        BROWSER_STREAM_PROTOCOL
    );
    (socket, upgrade_response)
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

async fn connect_browser_with_receive_buffer(
    daemon: &BrowserDaemon,
    receive_buffer_bytes: u32,
) -> Result<
    (
        BrowserSocket,
        tokio_tungstenite::tungstenite::http::Response<Option<Vec<u8>>>,
    ),
    WebSocketError,
> {
    let socket = tokio::net::TcpSocket::new_v4().expect("create browser TCP socket");
    socket
        .set_recv_buffer_size(receive_buffer_bytes)
        .expect("bound browser receive buffer");
    let stream = socket
        .connect(daemon.address)
        .await
        .expect("connect slow browser TCP socket");
    client_async(
        browser_request(
            daemon.address,
            &format!("http://{}", daemon.address),
            BROWSER_STREAM_PROTOCOL,
        ),
        MaybeTlsStream::Plain(stream),
    )
    .await
}

async fn saturate_browser_stream_credential(
    daemon: &BrowserDaemon,
    channel_id: &str,
    token: &str,
    checkpoint: &Message,
) -> Vec<BrowserSocket> {
    let mut sockets = Vec::with_capacity(PER_CREDENTIAL_BROWSER_STREAM_CAPACITY);
    for _ in 0..PER_CREDENTIAL_BROWSER_STREAM_CAPACITY {
        let grant = daemon.issue(channel_id, token, 0).await;
        let (mut socket, _) = connect_browser_with_receive_buffer(daemon, 1_024)
            .await
            .expect("slow browser upgrade");
        redeem(&mut socket, grant).await;
        assert_ready(next_server_frame(&mut socket).await, channel_id, 0);
        assert_message(next_server_frame(&mut socket).await, checkpoint);
        sockets.push(socket);
    }
    sockets
}

async fn redeem_capacity_probe(
    daemon: &BrowserDaemon,
    channel_id: &str,
    token: &str,
    after: i64,
) -> Option<(BrowserSocket, BrowserStreamServerFrame)> {
    let issued = daemon.issue(channel_id, token, after).await;
    let (mut socket, _) = connect_browser(daemon, None, None)
        .await
        .expect("capacity probe upgrade");
    redeem(&mut socket, issued).await;
    let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("capacity probe response timeout")
        .expect("capacity probe response")
        .expect("valid capacity probe response");
    match frame {
        WebSocketMessage::Text(text) => Some((
            socket,
            serde_json::from_str(&text).expect("capacity probe server frame"),
        )),
        WebSocketMessage::Close(Some(close)) => {
            assert_eq!(u16::from(close.code), 4_401);
            assert_eq!(close.reason, "grant_rejected");
            None
        }
        other => panic!("unexpected capacity probe response: {other:?}"),
    }
}

async fn wait_for_browser_stream_capacity(
    daemon: &BrowserDaemon,
    channel_id: &str,
    token: &str,
    after: i64,
) -> (BrowserSocket, BrowserStreamServerFrame) {
    tokio::time::timeout(CAPACITY_OBSERVATION_DEADLINE, async {
        let mut probes = tokio::time::interval(Duration::from_millis(250));
        probes.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            probes.tick().await;
            if let Some(ready) = redeem_capacity_probe(daemon, channel_id, token, after).await {
                break ready;
            }
        }
    })
    .await
    .expect("production send deadline did not release browser stream capacity")
}

async fn connect_native(
    daemon: &BrowserDaemon,
    channel_id: &str,
    token: &str,
    after: i64,
) -> BrowserSocket {
    use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;

    let mut request = format!(
        "ws://{}/v1/channels/{channel_id}/stream?after={after}",
        daemon.address
    )
    .into_client_request()
    .expect("native stream request");
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("native bearer header"),
    );
    connect_async(request)
        .await
        .expect("native stream upgrade")
        .0
}

async fn collect_native(
    daemon: &BrowserDaemon,
    channel_id: &str,
    token: &str,
    after: i64,
    count: usize,
) -> Vec<Message> {
    let mut socket = connect_native(daemon, channel_id, token, after).await;
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
            .await
            .expect("native frame timeout")
            .expect("native stream open")
            .expect("valid native frame");
        messages.push(
            serde_json::from_str(frame.to_text().expect("native text frame"))
                .expect("native message envelope"),
        );
    }
    socket.close(None).await.expect("close native stream");
    messages
}

async fn collect_browser(
    daemon: &BrowserDaemon,
    channel_id: &str,
    token: &str,
    after: i64,
    count: usize,
) -> Vec<Message> {
    let issued = daemon.issue(channel_id, token, after).await;
    let (mut socket, _) = connect_browser(daemon, None, None)
        .await
        .expect("browser parity upgrade");
    redeem(&mut socket, issued).await;
    assert_ready(next_server_frame(&mut socket).await, channel_id, after);
    let mut messages = Vec::with_capacity(count);
    for _ in 0..count {
        messages.push(expect_message(next_server_frame(&mut socket).await));
    }
    socket.close(None).await.expect("close browser stream");
    messages
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

fn expect_message(frame: BrowserStreamServerFrame) -> Message {
    match frame {
        BrowserStreamServerFrame::Message { message } => *message,
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

fn assert_surface_omits(surface: &str, value: &str, secrets: &[&str]) {
    for secret in secrets {
        assert!(!value.contains(secret), "{surface} exposed a raw secret");
    }
}

fn assert_headers_omit(surface: &str, headers: &HeaderMap, secrets: &[&str]) {
    for (name, value) in headers {
        assert_surface_omits(surface, name.as_str(), secrets);
        for secret in secrets {
            assert!(
                !value
                    .as_bytes()
                    .windows(secret.len())
                    .any(|window| window == secret.as_bytes()),
                "{surface} exposed a raw secret in {name}"
            );
        }
    }
}

async fn sqlite_quoted_values(database_path: &Path) -> Vec<String> {
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .read_only(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .expect("open qualification database");
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(&pool)
    .await
    .expect("list SQLite tables");
    let mut values = Vec::new();
    for table in tables {
        let table = sqlite_identifier(&table);
        let columns = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(&pool)
            .await
            .expect("list SQLite columns");
        for column in columns {
            let column_name: String = column.try_get("name").expect("column name");
            let column = sqlite_identifier(&column_name);
            let query = format!("SELECT quote({column}) FROM {table}");
            let mut column_values: Vec<String> = sqlx::query_scalar(&query)
                .fetch_all(&pool)
                .await
                .expect("read quoted SQLite values");
            values.append(&mut column_values);
        }
    }
    pool.close().await;
    values
}

fn sqlite_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn assert_sqlite_values_omit(values: &[String], secret: &str) {
    let encoded = secret
        .as_bytes()
        .iter()
        .fold(String::new(), |mut encoded, byte| {
            use std::fmt::Write as _;
            write!(encoded, "{byte:02X}").expect("write hex");
            encoded
        });
    for value in values {
        assert!(!value.contains(secret), "SQLite stored a raw secret value");
        assert!(
            !value.to_ascii_uppercase().contains(&encoded),
            "SQLite stored raw secret bytes"
        );
    }
}
