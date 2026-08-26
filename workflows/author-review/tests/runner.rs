use std::{path::PathBuf, time::Duration};

use fleetd::{
    AppState, AuthService, ClaimBatch, ClaimDeliveries, CreateAgent, CreateChannel,
    CreateChannelMember, MembershipDeliveryMode, Message, MessagePage, SendMessage, Store, router,
};
use fleetd_author_review::runner::{
    AuthorReviewRunner, FleetdEndpoint, RunnerConfiguration, WorkflowPluginSpec,
};
use reqwest::StatusCode;
use serde_json::json;

const WORKFLOW_ID: &str = "FLEETD-RUNNER-001";

#[tokio::test]
async fn public_runner_replays_send_before_ack_without_duplicate_effects() {
    let fixture = WorkflowFixture::start().await;

    let mut first_runner = AuthorReviewRunner::start(fixture.configuration.clone())
        .await
        .expect("start first runner");
    let first = first_runner
        .evaluate_and_publish(&fixture.batch.deliveries[0])
        .await
        .expect("publish before simulated crash");
    assert_eq!(first.proposals.len(), 1);
    drop(first_runner);

    let mut replacement = AuthorReviewRunner::start(fixture.configuration.clone())
        .await
        .expect("start replacement runner");
    let replay = replacement
        .evaluate_and_publish(&fixture.batch.deliveries[0])
        .await
        .expect("replay after simulated crash");
    assert!(
        replay.proposals.is_empty(),
        "durable history lets the replacement suppress an already committed effect"
    );

    let observer_history = fixture.observer_history().await;
    let coordinator_assignments = observer_history
        .messages
        .iter()
        .filter(|message| {
            message.sender_id == fixture.runner_agent.agent.id
                && message.recipient_id.as_deref() == Some(&fixture.coordinator.agent.id)
                && message.kind == "work.requested"
                && message.payload["assignment"] == "coordinator"
        })
        .count();
    assert_eq!(coordinator_assignments, 1);
    assert_eq!(observer_history.messages.len(), 2);
    fixture.acknowledge_root().await;
}

struct WorkflowFixture {
    server: TestServer,
    runner_agent: fleetd::RegisteredAgent,
    coordinator: fleetd::RegisteredAgent,
    observer: fleetd::RegisteredAgent,
    channel_id: String,
    root: Message,
    batch: ClaimBatch,
    configuration: RunnerConfiguration,
}

impl WorkflowFixture {
    async fn start() -> Self {
        let server = TestServer::start().await;
        let human = server.register("workflow-human").await;
        let runner_agent = server.register("workflow-runner").await;
        let coordinator = server.register("workflow-coordinator").await;
        let author = server.register("workflow-author").await;
        let reviewer = server.register("workflow-reviewer").await;
        let observer = server.register("workflow-observer").await;
        let channel_id = create_workflow_channel(
            &server,
            [
                &human,
                &runner_agent,
                &coordinator,
                &author,
                &reviewer,
                &observer,
            ],
        )
        .await;
        let root = send_root(&server, &channel_id, &human, &runner_agent).await;
        let batch = claim_root(&server, &runner_agent).await;
        assert_eq!(batch.deliveries.len(), 1);
        assert_eq!(batch.deliveries[0].message, root);
        let credential_file = server.directory.path().join("workflow-runner.token");
        write_private(&credential_file, &runner_agent.credential.token);
        let configuration = runner_configuration(
            &server,
            &runner_agent,
            &coordinator,
            &author,
            &reviewer,
            credential_file,
        );
        Self {
            server,
            runner_agent,
            coordinator,
            observer,
            channel_id,
            root,
            batch,
            configuration,
        }
    }

    async fn observer_history(&self) -> MessagePage {
        self.server
            .http
            .get(format!(
                "{}/v1/channels/{}/messages?after=0&limit=100",
                self.server.origin, self.channel_id
            ))
            .bearer_auth(&self.observer.credential.token)
            .send()
            .await
            .expect("read observer history")
            .error_for_status()
            .expect("observer history status")
            .json()
            .await
            .expect("observer history body")
    }

    async fn acknowledge_root(&self) {
        let acknowledged = self
            .server
            .http
            .post(format!(
                "{}/v1/agents/{}/deliveries/{}/ack",
                self.server.origin, self.runner_agent.agent.id, self.root.id
            ))
            .bearer_auth(&self.runner_agent.credential.token)
            .json(&json!({"lease_token": self.batch.lease_token}))
            .send()
            .await
            .expect("ack replayed input");
        assert_eq!(acknowledged.status(), StatusCode::NO_CONTENT);
    }
}

async fn create_workflow_channel(
    server: &TestServer,
    agents: [&fleetd::RegisteredAgent; 6],
) -> String {
    let [human, runner, coordinator, author, reviewer, observer] = agents;
    server
        .store
        .create_channel(CreateChannel {
            name: "author-review-dogfood".to_owned(),
            metadata: json!({}),
            member_ids: Vec::new(),
            members: vec![
                member(&human.agent.id, MembershipDeliveryMode::StreamOnly),
                member(&runner.agent.id, MembershipDeliveryMode::Inbox),
                member(&coordinator.agent.id, MembershipDeliveryMode::Inbox),
                member(&author.agent.id, MembershipDeliveryMode::Inbox),
                member(&reviewer.agent.id, MembershipDeliveryMode::Inbox),
                member(&observer.agent.id, MembershipDeliveryMode::StreamOnly),
            ],
        })
        .await
        .expect("create workflow channel")
        .id
}

async fn send_root(
    server: &TestServer,
    channel_id: &str,
    human: &fleetd::RegisteredAgent,
    runner: &fleetd::RegisteredAgent,
) -> Message {
    server
        .http
        .post(format!(
            "{}/v1/channels/{channel_id}/messages",
            server.origin
        ))
        .bearer_auth(&human.credential.token)
        .json(&SendMessage {
            idempotency_key: Some("dogfood/root".to_owned()),
            recipient_id: Some(runner.agent.id.clone()),
            kind: "work.requested".to_owned(),
            payload: root_payload(),
            correlation_id: Some(WORKFLOW_ID.to_owned()),
            causation_id: None,
        })
        .send()
        .await
        .expect("send root request")
        .error_for_status()
        .expect("root request status")
        .json()
        .await
        .expect("root request body")
}

async fn claim_root(server: &TestServer, runner: &fleetd::RegisteredAgent) -> ClaimBatch {
    server
        .http
        .post(format!(
            "{}/v1/agents/{}/deliveries/claim",
            server.origin, runner.agent.id
        ))
        .bearer_auth(&runner.credential.token)
        .json(&ClaimDeliveries {
            limit: 1,
            lease_duration_ms: 15_000,
        })
        .send()
        .await
        .expect("claim workflow input")
        .error_for_status()
        .expect("claim status")
        .json()
        .await
        .expect("claim body")
}

fn root_payload() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "request_id": WORKFLOW_ID,
        "title": "Dogfood the author-review runner",
        "objective": "Prove public API replay across plugin replacement",
        "repository": {
            "path": "/Users/ngalluzzo/repos/fleetd",
            "base_revision": "fd32209"
        },
        "scope": ["external workflow runner"],
        "acceptance_criteria": ["one durable coordinator assignment"]
    })
}

fn runner_configuration(
    server: &TestServer,
    runner: &fleetd::RegisteredAgent,
    coordinator: &fleetd::RegisteredAgent,
    author: &fleetd::RegisteredAgent,
    reviewer: &fleetd::RegisteredAgent,
    credential_file: PathBuf,
) -> RunnerConfiguration {
    RunnerConfiguration {
        schema_version: 1,
        fleetd: FleetdEndpoint {
            origin: server.origin.clone(),
            agent_id: runner.agent.id.clone(),
            credential_file,
        },
        plugin: WorkflowPluginSpec {
            executable: PathBuf::from(env!("CARGO_BIN_EXE_fleetd-author-review-plugin")),
            args: Vec::new(),
            request_timeout_ms: 5_000,
        },
        plugin_configuration: json!({
            "schema_version": 1,
            "coordinator_agent_id": coordinator.agent.id,
            "author_agent_ids": [author.agent.id],
            "reviewer_agent_ids": [reviewer.agent.id],
            "max_children": 4,
            "max_revision_rounds": 2
        }),
        lease_duration_ms: 15_000,
        poll_interval_ms: 100,
    }
}

struct TestServer {
    directory: tempfile::TempDir,
    store: Store,
    origin: String,
    http: reqwest::Client,
    process: tokio::task::JoinHandle<()>,
}

impl TestServer {
    async fn start() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = Store::open(directory.path().join("fleetd.db"))
            .await
            .expect("open Fleetd store");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Fleetd");
        let address = listener.local_addr().expect("Fleetd address");
        let app = router(AppState::new(store.clone()));
        let process = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve Fleetd");
        });
        Self {
            directory,
            store,
            origin: format!("http://{address}"),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("HTTP client"),
            process,
        }
    }

    async fn register(&self, name: &str) -> fleetd::RegisteredAgent {
        AuthService::new(self.store.clone())
            .register_agent(CreateAgent {
                name: name.to_owned(),
                metadata: json!({}),
            })
            .await
            .expect("register agent")
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.process.abort();
    }
}

fn member(agent_id: &str, delivery_mode: MembershipDeliveryMode) -> CreateChannelMember {
    CreateChannelMember {
        agent_id: agent_id.to_owned(),
        delivery_mode,
    }
}

fn write_private(path: &std::path::Path, token: &str) {
    std::fs::write(path, token).expect("write credential");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("secure credential");
    }
}
