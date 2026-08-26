use std::{path::PathBuf, time::Duration};

use fleetd::{
    api::{AppState, router},
    auth::AuthService,
    model::{
        ArmInvocation, ClaimDeliveries, CompleteInvocation, CreateAgent, InvocationBatch,
        InvocationCompletion, Message, MessagePage, RegisteredAgent, SendMessage,
    },
    store::Store,
};
use futures_util::StreamExt;
use serde_json::json;
use sqlx::{Connection, Row, sqlite::SqliteConnectOptions};

struct QualificationDaemon {
    _directory: tempfile::TempDir,
    database_path: PathBuf,
    operator_token: String,
    address: std::net::SocketAddr,
    process: Option<tokio::task::JoinHandle<()>>,
}

impl QualificationDaemon {
    async fn start() -> Self {
        let directory = tempfile::tempdir().expect("qualification directory");
        let database_path = directory.path().join("fleetd.db");
        let store = Store::open(&database_path).await.expect("open store");
        let token_path = directory.path().join("operator.token");
        AuthService::new(store.clone())
            .ensure_operator_credential(&token_path)
            .await
            .expect("bootstrap operator");
        let operator_token = std::fs::read_to_string(token_path)
            .expect("read operator token")
            .trim()
            .to_owned();
        let (address, process) = serve(store).await;
        Self {
            _directory: directory,
            database_path,
            operator_token,
            address,
            process: Some(process),
        }
    }

    async fn restart(&mut self) {
        let process = self.process.take().expect("running daemon");
        process.abort();
        let _ = process.await;
        let store = Store::open(&self.database_path)
            .await
            .expect("reopen store after restart");
        let (address, process) = serve(store).await;
        self.address = address;
        self.process = Some(process);
    }

    fn get(&self, path: &str, token: &str) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .get(format!("http://{}{path}", self.address))
            .bearer_auth(token)
    }

    fn post(&self, path: &str, token: &str) -> reqwest::RequestBuilder {
        reqwest::Client::new()
            .post(format!("http://{}{path}", self.address))
            .bearer_auth(token)
    }

    async fn register(&self, name: &str) -> RegisteredAgent {
        self.post("/v1/agents", &self.operator_token)
            .json(&CreateAgent {
                name: name.to_owned(),
                metadata: json!({ "private": format!("{name}-metadata") }),
            })
            .send()
            .await
            .expect("register request")
            .error_for_status()
            .expect("register response")
            .json()
            .await
            .expect("registration body")
    }
}

impl Drop for QualificationDaemon {
    fn drop(&mut self) {
        if let Some(process) = &self.process {
            process.abort();
        }
    }
}

struct Participants {
    human: RegisteredAgent,
    worker: RegisteredAgent,
    peer: RegisteredAgent,
    channel_id: String,
}

#[tokio::test]
async fn stream_only_membership_qualifies_a_restart_safe_conversation() {
    let mut daemon = QualificationDaemon::start().await;
    let participants = provision_public_memberships(&daemon).await;
    assert_membership_storage(&daemon.database_path, &participants).await;

    let mut human_stream = open_stream(
        daemon.address,
        &participants.channel_id,
        &participants.human.credential.token,
        0,
    )
    .await;
    let request = send_human_request(&daemon, &participants).await;
    assert_eq!(next_message(&mut human_stream).await, request);
    assert_delivery_recipients(
        &daemon.database_path,
        request.seq,
        &[&participants.worker.agent.id],
    )
    .await;

    let result = complete_worker_invocation(&daemon, &participants, &request).await;
    assert_eq!(next_message(&mut human_stream).await, result);
    assert_delivery_recipients(&daemon.database_path, result.seq, &[]).await;
    drop(human_stream);

    daemon.restart().await;
    let mut replay = open_stream(
        daemon.address,
        &participants.channel_id,
        &participants.human.credential.token,
        request.seq,
    )
    .await;
    assert_eq!(next_message(&mut replay).await, result);
    drop(replay);

    qualify_visibility_and_broadcast(&daemon, &participants, &result).await;
}

async fn provision_public_memberships(daemon: &QualificationDaemon) -> Participants {
    let human = daemon.register("qualification-human").await;
    let worker = daemon.register("qualification-worker").await;
    let peer = daemon.register("qualification-peer").await;
    let response = daemon
        .post("/v1/channels", &daemon.operator_token)
        .json(&json!({
            "name": "qualified-live-conversation",
            "member_ids": [worker.agent.id],
            "members": [
                { "agent_id": human.agent.id, "delivery_mode": "stream_only" },
                { "agent_id": peer.agent.id, "delivery_mode": "stream_only" }
            ]
        }))
        .send()
        .await
        .expect("create channel request")
        .error_for_status()
        .expect("create channel response");
    let channel: fleetd::model::Channel = response.json().await.expect("channel body");
    Participants {
        human,
        worker,
        peer,
        channel_id: channel.id,
    }
}

async fn send_human_request(daemon: &QualificationDaemon, participants: &Participants) -> Message {
    post_message(
        daemon,
        &participants.channel_id,
        &participants.human.credential.token,
        SendMessage {
            idempotency_key: Some("qualification/human/request/1".to_owned()),
            recipient_id: Some(participants.worker.agent.id.clone()),
            kind: "conversation.prompt/vendor-unknown-v7".to_owned(),
            payload: json!({
                "text": "reply through the managed invocation",
                "extension": { "must_survive": [1, 2, 3] }
            }),
            correlation_id: Some("qualification-conversation".to_owned()),
            causation_id: None,
        },
    )
    .await
}

async fn complete_worker_invocation(
    daemon: &QualificationDaemon,
    participants: &Participants,
    request: &Message,
) -> Message {
    let batch: InvocationBatch = daemon
        .post(
            &format!(
                "/v1/agents/{}/invocations/reserve",
                participants.worker.agent.id
            ),
            &participants.worker.credential.token,
        )
        .json(&ClaimDeliveries {
            limit: 1,
            lease_duration_ms: 30_000,
        })
        .send()
        .await
        .expect("reserve request")
        .error_for_status()
        .expect("reserve response")
        .json()
        .await
        .expect("invocation batch");
    let invocation = batch.invocations.first().expect("reserved invocation");
    assert_eq!(invocation.message, *request);
    let armed: fleetd::model::Invocation = daemon
        .post(
            &format!(
                "/v1/agents/{}/invocations/{}/arm",
                participants.worker.agent.id, invocation.id
            ),
            &participants.worker.credential.token,
        )
        .json(&ArmInvocation {
            lease_token: invocation.lease_token.clone(),
            fence_token: invocation.fence_token.clone(),
        })
        .send()
        .await
        .expect("arm request")
        .error_for_status()
        .expect("arm response")
        .json()
        .await
        .expect("armed invocation");
    let completion: InvocationCompletion = daemon
        .post(
            &format!(
                "/v1/agents/{}/invocations/{}/complete",
                participants.worker.agent.id, invocation.id
            ),
            &participants.worker.credential.token,
        )
        .json(&CompleteInvocation {
            lease_token: armed.lease_token,
            fence_token: armed.fence_token,
            kind: "conversation.result/vendor-unknown-v9".to_owned(),
            payload: json!({
                "text": "durable reply",
                "provider_extension": { "untouched": true }
            }),
        })
        .send()
        .await
        .expect("complete request")
        .error_for_status()
        .expect("complete response")
        .json()
        .await
        .expect("completion body");
    assert_eq!(
        completion.result.recipient_id,
        Some(participants.human.agent.id.clone())
    );
    assert_eq!(completion.result.causation_id, Some(request.id.clone()));
    completion.result
}

async fn qualify_visibility_and_broadcast(
    daemon: &QualificationDaemon,
    participants: &Participants,
    result: &Message,
) {
    let private = post_message(
        daemon,
        &participants.channel_id,
        &participants.worker.credential.token,
        SendMessage {
            idempotency_key: Some("qualification/private/peer".to_owned()),
            recipient_id: Some(participants.peer.agent.id.clone()),
            kind: "private.opaque/v1".to_owned(),
            payload: json!({ "private": true }),
            correlation_id: None,
            causation_id: None,
        },
    )
    .await;
    assert_delivery_recipients(&daemon.database_path, private.seq, &[]).await;
    let human_history = history(
        daemon,
        &participants.channel_id,
        &participants.human.credential.token,
        result.seq,
    )
    .await;
    assert!(human_history.messages.is_empty());
    let operator_history = history(
        daemon,
        &participants.channel_id,
        &daemon.operator_token,
        result.seq,
    )
    .await;
    assert_eq!(operator_history.messages, vec![private.clone()]);

    let mut human_live = open_stream(
        daemon.address,
        &participants.channel_id,
        &participants.human.credential.token,
        private.seq,
    )
    .await;
    let broadcast = post_message(
        daemon,
        &participants.channel_id,
        &participants.human.credential.token,
        SendMessage {
            idempotency_key: Some("qualification/broadcast/1".to_owned()),
            recipient_id: None,
            kind: "broadcast.opaque/v3".to_owned(),
            payload: json!({ "unknown": { "preserved": "exactly" } }),
            correlation_id: None,
            causation_id: Some(result.id.clone()),
        },
    )
    .await;
    assert_eq!(next_message(&mut human_live).await, broadcast);
    assert_delivery_recipients(
        &daemon.database_path,
        broadcast.seq,
        &[&participants.worker.agent.id],
    )
    .await;
    let peer_history = history(
        daemon,
        &participants.channel_id,
        &participants.peer.credential.token,
        private.seq,
    )
    .await;
    assert_eq!(peer_history.messages, vec![broadcast]);
}

async fn assert_membership_storage(path: &PathBuf, participants: &Participants) {
    let mut connection = database_connection(path).await;
    let rows = sqlx::query(
        "SELECT agent_id, delivery_mode FROM channel_members WHERE channel_id = ? ORDER BY agent_id",
    )
    .bind(&participants.channel_id)
    .fetch_all(&mut connection)
    .await
    .expect("inspect memberships");
    let actual: Vec<(String, String)> = rows
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("agent_id"),
                row.get::<String, _>("delivery_mode"),
            )
        })
        .collect();
    let mut expected = vec![
        (
            participants.human.agent.id.clone(),
            "stream_only".to_owned(),
        ),
        (participants.worker.agent.id.clone(), "inbox".to_owned()),
        (participants.peer.agent.id.clone(), "stream_only".to_owned()),
    ];
    expected.sort();
    assert_eq!(actual, expected);
}

async fn assert_delivery_recipients(path: &PathBuf, message_seq: i64, expected: &[&str]) {
    let mut connection = database_connection(path).await;
    let actual: Vec<String> = sqlx::query_scalar(
        "SELECT agent_id FROM agent_deliveries WHERE message_seq = ? ORDER BY agent_id",
    )
    .bind(message_seq)
    .fetch_all(&mut connection)
    .await
    .expect("inspect exact delivery rows");
    let mut expected: Vec<String> = expected.iter().map(|value| (*value).to_owned()).collect();
    expected.sort();
    assert_eq!(actual, expected);
}

async fn database_connection(path: &PathBuf) -> sqlx::SqliteConnection {
    let options = SqliteConnectOptions::new().filename(path).read_only(true);
    sqlx::SqliteConnection::connect_with(&options)
        .await
        .expect("open exact storage observer")
}

async fn post_message(
    daemon: &QualificationDaemon,
    channel_id: &str,
    token: &str,
    input: SendMessage,
) -> Message {
    daemon
        .post(&format!("/v1/channels/{channel_id}/messages"), token)
        .json(&input)
        .send()
        .await
        .expect("message request")
        .error_for_status()
        .expect("message response")
        .json()
        .await
        .expect("message body")
}

async fn history(
    daemon: &QualificationDaemon,
    channel_id: &str,
    token: &str,
    after: i64,
) -> MessagePage {
    daemon
        .get(
            &format!("/v1/channels/{channel_id}/messages?after={after}&limit=100"),
            token,
        )
        .send()
        .await
        .expect("history request")
        .error_for_status()
        .expect("history response")
        .json()
        .await
        .expect("history body")
}

async fn serve(store: Store) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind daemon");
    let address = listener.local_addr().expect("daemon address");
    let process = tokio::spawn(async move {
        axum::serve(listener, router(AppState::new(store)))
            .await
            .expect("serve API");
    });
    (address, process)
}

async fn open_stream(
    address: std::net::SocketAddr,
    channel_id: &str,
    token: &str,
    after: i64,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue};

    let url = format!("ws://{address}/v1/channels/{channel_id}/stream?after={after}");
    let mut request = url.into_client_request().expect("stream request");
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization header"),
    );
    tokio_tungstenite::connect_async(request)
        .await
        .expect("connect native stream")
        .0
}

async fn next_message<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Message
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("message delivery timeout")
        .expect("stream is open")
        .expect("valid WebSocket frame");
    serde_json::from_str(frame.to_text().expect("text frame")).expect("message envelope")
}
