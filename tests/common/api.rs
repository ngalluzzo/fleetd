//! A running daemon, addressed over HTTP the way a client addresses it.
//!
//! Every suite that exercises the versioned surface needs the same few moves:
//! start a daemon with an operator credential, register an agent, open a
//! channel, send a message, claim an inbox. Those are shared here so a suite
//! holds only the assertions that are its own.

// Compiled into every binary that includes it, so a helper one suite needs
// reads as unused from the others.
#![allow(dead_code)]

use std::net::SocketAddr;

use fleetd::{
    http::AppState,
    model::{
        BlockResolution, ClaimBatch, ClaimDeliveries, CreateAgent, CreateChannel, Message,
        RegisteredAgent, ResolveDeliveryBlock, SendMessage,
    },
};
use serde_json::json;

use super::{TempStore, bootstrap_operator, serve, temp_store};

/// A daemon serving the versioned contract on loopback.
pub struct Daemon {
    _temporary: TempStore,
    pub address: SocketAddr,
    pub operator_token: String,
    process: tokio::task::JoinHandle<()>,
}

impl Daemon {
    pub async fn start() -> Self {
        let temporary = temp_store().await;
        let operator_token = bootstrap_operator(&temporary.store, temporary.directory.path()).await;
        let (address, process) = serve(AppState::new(temporary.store.clone())).await;
        Self {
            _temporary: temporary,
            address,
            operator_token,
            process,
        }
    }

    /// A request carrying the given credential, or none at all.
    ///
    /// The credential is optional because refusing an unauthenticated request is
    /// part of the contract these suites assert.
    pub fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
    ) -> reqwest::RequestBuilder {
        authorize(
            reqwest::Client::new().request(method, format!("http://{}{path}", self.address)),
            token,
        )
    }

    pub fn get(&self, path: &str, token: Option<&str>) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::GET, path, token)
    }

    pub fn post(&self, path: &str, token: Option<&str>) -> reqwest::RequestBuilder {
        self.request(reqwest::Method::POST, path, token)
    }

    pub async fn register(&self, name: &str) -> RegisteredAgent {
        self.post("/v1/agents", Some(&self.operator_token))
            .json(&CreateAgent {
                name: name.to_owned(),
                metadata: json!({}),
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

    pub async fn channel(&self, members: &[&str]) -> fleetd::model::Channel {
        self.post("/v1/channels", Some(&self.operator_token))
            .json(&CreateChannel {
                name: "auth-test".to_owned(),
                metadata: json!({}),
                member_ids: members.iter().map(|member| (*member).to_owned()).collect(),
                members: Vec::new(),
            })
            .send()
            .await
            .expect("channel request")
            .error_for_status()
            .expect("channel response")
            .json()
            .await
            .expect("channel body")
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.process.abort();
    }
}

pub async fn assert_resolution_authority(server: &Daemon, agent: &RegisteredAgent, block_id: i64) {
    let resolution = ResolveDeliveryBlock {
        resolution: BlockResolution::Requeue,
        retry_after_ms: 0,
        note: Some("verified safe to retry".to_owned()),
    };
    let resolution_path = format!("/v1/delivery-blocks/{block_id}/resolve");
    let agent_resolution = server
        .post(&resolution_path, Some(&agent.credential.token))
        .json(&resolution)
        .send()
        .await
        .expect("agent resolution response");
    assert_eq!(agent_resolution.status(), reqwest::StatusCode::FORBIDDEN);
    for label in ["first resolution", "resolution replay"] {
        let response = server
            .post(&resolution_path, Some(&server.operator_token))
            .json(&resolution)
            .send()
            .await
            .unwrap_or_else(|error| panic!("{label} response: {error}"));
        assert_eq!(
            response.status(),
            reqwest::StatusCode::NO_CONTENT,
            "{label}"
        );
    }
    let conflicting_resolution = server
        .post(&resolution_path, Some(&server.operator_token))
        .json(&ResolveDeliveryBlock {
            resolution: BlockResolution::Abandon,
            retry_after_ms: 0,
            note: Some("changed decision".to_owned()),
        })
        .send()
        .await
        .expect("conflicting resolution response");
    assert_eq!(
        conflicting_resolution.status(),
        reqwest::StatusCode::CONFLICT
    );

    let reclaimed: ClaimBatch = claim(server, &agent.agent.id, &agent.credential.token)
        .await
        .error_for_status()
        .expect("claim after resolution")
        .json()
        .await
        .expect("claim body after resolution");
    assert_eq!(reclaimed.deliveries.len(), 1);
    assert_eq!(reclaimed.deliveries[0].attempt, 2);
}

pub async fn send_message(
    server: &Daemon,
    channel_id: &str,
    sender: &RegisteredAgent,
    recipient_id: &str,
) -> Message {
    server
        .post(
            &format!("/v1/channels/{channel_id}/messages"),
            Some(&sender.credential.token),
        )
        .json(&SendMessage {
            idempotency_key: None,
            recipient_id: Some(recipient_id.to_owned()),
            kind: "review.requested/v1".to_owned(),
            payload: json!({ "commit": "4aa4cd1" }),
            correlation_id: Some("auth-test".to_owned()),
            causation_id: None,
        })
        .send()
        .await
        .expect("send request")
        .error_for_status()
        .expect("send response")
        .json()
        .await
        .expect("message body")
}

pub async fn post_message(
    server: &Daemon,
    channel_id: &str,
    token: &str,
    input: &SendMessage,
) -> reqwest::Response {
    server
        .post(&format!("/v1/channels/{channel_id}/messages"), Some(token))
        .json(input)
        .send()
        .await
        .expect("message response")
}

pub async fn claim(server: &Daemon, agent_id: &str, token: &str) -> reqwest::Response {
    server
        .post(
            &format!("/v1/agents/{agent_id}/deliveries/claim"),
            Some(token),
        )
        .json(&ClaimDeliveries {
            limit: 1,
            lease_duration_ms: 10_000,
        })
        .send()
        .await
        .expect("claim response")
}

fn authorize(request: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}
