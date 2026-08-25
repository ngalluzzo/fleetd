use std::time::Duration;

use fleetd::{
    AckDelivery, AppState, AuthService, ClaimBatch, ClaimDeliveries, CreateAgent, CreateChannel,
    CreateMessage, Message, SendMessage, Store, router,
};
use futures_util::StreamExt;
use serde_json::json;

#[tokio::test]
async fn websocket_replays_history_then_delivers_live_messages() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let sender = store
        .create_agent(CreateAgent {
            name: "sender".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create sender");
    let receiver = store
        .create_agent(CreateAgent {
            name: "receiver".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create receiver");
    let auth = AuthService::new(store.clone());
    let sender_credential = auth
        .rotate_agent_credential(&sender.id)
        .await
        .expect("issue sender credential");
    let receiver_credential = auth
        .rotate_agent_credential(&receiver.id)
        .await
        .expect("issue receiver credential");
    let channel = store
        .create_channel(CreateChannel {
            name: "network-test".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), receiver.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");
    let history = store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: sender.id.clone(),
                idempotency_key: None,
                recipient_id: Some(receiver.id.clone()),
                kind: "text".to_owned(),
                payload: json!({ "text": "before connect" }),
                correlation_id: None,
                causation_id: None,
            },
        )
        .await
        .expect("append history");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(AppState::new(store)))
            .await
            .expect("serve API");
    });

    let stream_url = format!("ws://{address}/v1/channels/{}/stream?after=0", channel.id);
    let request = authenticated_socket_request(&stream_url, &receiver_credential.token);
    let (mut socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .expect("connect stream");
    let replayed = next_message(&mut socket).await;
    assert_eq!(replayed, history);

    let live_input = SendMessage {
        idempotency_key: None,
        recipient_id: Some(sender.id.clone()),
        kind: "text".to_owned(),
        payload: json!({ "text": "after connect" }),
        correlation_id: None,
        causation_id: Some(history.id),
    };
    let response = reqwest::Client::new()
        .post(format!(
            "http://{address}/v1/channels/{}/messages",
            channel.id
        ))
        .bearer_auth(&receiver_credential.token)
        .json(&live_input)
        .send()
        .await
        .expect("send live message");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let expected: Message = response.json().await.expect("decode live message");
    let delivered = next_message(&mut socket).await;
    assert_eq!(delivered, expected);
    claim_and_ack(address, &delivered, &sender_credential.token).await;
    server.abort();
}

#[tokio::test]
async fn streams_do_not_leak_direct_messages_between_other_members() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let author = create_member(&store, "author").await;
    let recipient = create_member(&store, "recipient").await;
    let watcher = create_member(&store, "watcher").await;
    let auth = AuthService::new(store.clone());
    let author_credential = auth
        .rotate_agent_credential(&author)
        .await
        .expect("issue author credential");
    let watcher_credential = auth
        .rotate_agent_credential(&watcher)
        .await
        .expect("issue watcher credential");
    let channel = store
        .create_channel(CreateChannel {
            name: "discreet".to_owned(),
            metadata: json!({}),
            member_ids: vec![author.clone(), recipient.clone(), watcher.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let address = listener.local_addr().expect("server address");
    let server = tokio::spawn(async move {
        axum::serve(listener, router(AppState::new(store)))
            .await
            .expect("serve API");
    });

    let stream_url = format!("ws://{address}/v1/channels/{}/stream?after=0", channel.id);
    let (mut live_socket, _) = tokio_tungstenite::connect_async(authenticated_socket_request(
        &stream_url,
        &watcher_credential.token,
    ))
    .await
    .expect("connect live stream");

    post_message(
        address,
        &channel.id,
        &author_credential.token,
        &SendMessage {
            idempotency_key: None,
            recipient_id: Some(recipient),
            kind: "text".to_owned(),
            payload: json!({ "text": "only for recipient" }),
            correlation_id: None,
            causation_id: None,
        },
    )
    .await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let broadcast = post_message(
        address,
        &channel.id,
        &author_credential.token,
        &SendMessage {
            idempotency_key: None,
            recipient_id: None,
            kind: "text".to_owned(),
            payload: json!({ "text": "for everyone" }),
            correlation_id: None,
            causation_id: None,
        },
    )
    .await;

    let delivered = next_message(&mut live_socket).await;
    assert_eq!(delivered, broadcast);

    let (mut replay_socket, _) = tokio_tungstenite::connect_async(authenticated_socket_request(
        &stream_url,
        &watcher_credential.token,
    ))
    .await
    .expect("connect replay stream");
    let replayed = next_message(&mut replay_socket).await;
    assert_eq!(replayed, broadcast);
    server.abort();
}

async fn create_member(store: &Store, name: &str) -> String {
    store
        .create_agent(CreateAgent {
            name: name.to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create agent")
        .id
}

async fn post_message(
    address: std::net::SocketAddr,
    channel_id: &str,
    token: &str,
    input: &SendMessage,
) -> Message {
    let response = reqwest::Client::new()
        .post(format!(
            "http://{address}/v1/channels/{channel_id}/messages"
        ))
        .bearer_auth(token)
        .json(input)
        .send()
        .await
        .expect("send message");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    response.json().await.expect("decode message")
}

async fn claim_and_ack(address: std::net::SocketAddr, delivered: &Message, agent_token: &str) {
    let claim: ClaimBatch = reqwest::Client::new()
        .post(format!(
            "http://{address}/v1/agents/{}/deliveries/claim",
            delivered.recipient_id.as_deref().expect("direct recipient")
        ))
        .bearer_auth(agent_token)
        .json(&ClaimDeliveries {
            limit: 10,
            lease_duration_ms: 10_000,
        })
        .send()
        .await
        .expect("claim inbox through API")
        .error_for_status()
        .expect("successful claim")
        .json()
        .await
        .expect("decode claim batch");
    assert_eq!(claim.deliveries.len(), 1);
    assert_eq!(&claim.deliveries[0].message, delivered);
    let agent_id = delivered.recipient_id.as_deref().expect("direct recipient");
    let acknowledgement = reqwest::Client::new()
        .post(format!(
            "http://{address}/v1/agents/{agent_id}/deliveries/{}/ack",
            delivered.id
        ))
        .bearer_auth(agent_token)
        .json(&AckDelivery {
            lease_token: claim.lease_token,
        })
        .send()
        .await
        .expect("acknowledge through API");
    assert_eq!(acknowledgement.status(), reqwest::StatusCode::NO_CONTENT);
}

fn authenticated_socket_request(
    url: &str,
    token: &str,
) -> tokio_tungstenite::tungstenite::http::Request<()> {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue};

    let mut request = url.into_client_request().expect("websocket request");
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).expect("authorization header"),
    );
    request
}

async fn next_message<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Message
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = tokio::time::timeout(Duration::from_secs(2), socket.next())
        .await
        .expect("message delivery timeout")
        .expect("stream is open")
        .expect("valid websocket frame");
    serde_json::from_str(frame.to_text().expect("text frame")).expect("message envelope")
}
