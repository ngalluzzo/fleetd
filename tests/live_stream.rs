use std::time::Duration;

use fleetd::{
    AckDelivery, AppState, ClaimBatch, ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage,
    Message, Store, router,
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
    let channel = store
        .create_channel(CreateChannel {
            name: "network-test".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), receiver.id.clone()],
        })
        .await
        .expect("create channel");
    let history = store
        .append_message(
            &channel.id,
            CreateMessage {
                sender_id: sender.id.clone(),
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
    let (mut socket, _) = tokio_tungstenite::connect_async(stream_url)
        .await
        .expect("connect stream");
    let replayed = next_message(&mut socket).await;
    assert_eq!(replayed, history);

    let live_input = CreateMessage {
        sender_id: receiver.id,
        recipient_id: Some(sender.id),
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
        .json(&live_input)
        .send()
        .await
        .expect("send live message");
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let expected: Message = response.json().await.expect("decode live message");
    let delivered = next_message(&mut socket).await;
    assert_eq!(delivered, expected);
    claim_and_ack(address, &delivered).await;
    server.abort();
}

async fn claim_and_ack(address: std::net::SocketAddr, delivered: &Message) {
    let claim: ClaimBatch = reqwest::Client::new()
        .post(format!(
            "http://{address}/v1/agents/{}/deliveries/claim",
            delivered.recipient_id.as_deref().expect("direct recipient")
        ))
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
        .json(&AckDelivery {
            lease_token: claim.lease_token,
        })
        .send()
        .await
        .expect("acknowledge through API");
    assert_eq!(acknowledgement.status(), reqwest::StatusCode::NO_CONTENT);
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
