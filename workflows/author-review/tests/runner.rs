use std::{path::PathBuf, time::Duration};

use fleetd::{
    AppState, AuthService, ClaimBatch, ClaimDeliveries, CreateAgent, CreateChannel,
    CreateChannelMember, DeliveryState, MembershipDeliveryMode, Message, MessagePage,
    RetryDelivery, SendMessage, Store, router,
};
use fleetd_author_review::{
    protocol::{
        EVENT_KINDS, INTERFACE_ID, INTERFACE_VERSION, MAX_FRAME_BYTES, PLUGIN_ID, PLUGIN_VERSION,
    },
    runner::{
        AuthorReviewRunner, FleetdEndpoint, RunnerConfiguration, TickOutcome, WorkflowPluginSpec,
        load_configuration,
    },
};
use reqwest::StatusCode;
use serde_json::json;

const WORKFLOW_ID: &str = "FLEETD-RUNNER-001";

#[tokio::test]
async fn crash_replay_is_idempotent_and_divergent_replay_is_rejected() {
    let fixture = WorkflowFixture::start().await;
    let stable_plugin = fixture.write_plugin(
        "stable-plugin",
        &static_plugin_script(&proposal_result(
            &fixture.coordinator.agent.id,
            &json!({"artifact": "stable"}),
        )),
    );
    let mut stable_configuration = fixture.configuration.clone();
    stable_configuration.plugin.executable = stable_plugin;

    let mut first_runner = AuthorReviewRunner::start(stable_configuration.clone())
        .await
        .expect("start first runner");
    let first = first_runner
        .evaluate_and_publish(&fixture.batch.deliveries[0])
        .await
        .expect("publish before simulated crash");
    assert_eq!(first.proposals.len(), 1);
    drop(first_runner);

    let mut replacement = AuthorReviewRunner::start(stable_configuration)
        .await
        .expect("start replacement runner");
    let replay = replacement
        .evaluate_and_publish(&fixture.batch.deliveries[0])
        .await
        .expect("replay after simulated crash");
    assert_eq!(replay.proposals.len(), 1);

    let divergent_plugin = fixture.write_plugin(
        "divergent-plugin",
        &static_plugin_script(&proposal_result(
            &fixture.coordinator.agent.id,
            &json!({"artifact": "divergent"}),
        )),
    );
    let mut divergent_configuration = fixture.configuration.clone();
    divergent_configuration.plugin.executable = divergent_plugin;
    let mut divergent = AuthorReviewRunner::start(divergent_configuration)
        .await
        .expect("start divergent replacement");
    let error = divergent
        .evaluate_and_publish(&fixture.batch.deliveries[0])
        .await
        .expect_err("divergent replay must conflict");
    assert!(error.to_string().contains("divergent replay"));

    let observer_history = fixture.observer_history().await;
    let coordinator_assignments = observer_history
        .messages
        .iter()
        .filter(|message| {
            message.sender_id == fixture.runner_agent.agent.id
                && message.recipient_id.as_deref() == Some(&fixture.coordinator.agent.id)
                && message.kind == "work.requested"
                && message.payload["artifact"] == "stable"
        })
        .count();
    assert_eq!(coordinator_assignments, 1);
    assert_eq!(observer_history.messages.len(), 2);
    fixture.acknowledge_root().await;
}

#[tokio::test]
async fn permanent_plugin_failures_block_with_bounded_secret_free_diagnostics() {
    let secret = "DO_NOT_EXPOSE_PLUGIN_OR_AMBIENT_SECRET";
    let cases = vec![
        (
            "protocol",
            single_response_script("not-json"),
            "response decoding phase",
        ),
        (
            "identity",
            single_response_script(&description_response_with_interface("wrong.interface")),
            "description identity phase",
        ),
        (
            "framing",
            single_response_script(&"x".repeat(MAX_FRAME_BYTES + 1)),
            "response framing phase",
        ),
        (
            "decoding",
            static_plugin_script(&json!({"projection": {}})),
            "result decoding",
        ),
        (
            "semantic",
            static_plugin_script(&json!({
                "projection": {},
                "proposals": [{
                    "operation_id": "invalid-recipient",
                    "recipient_id": "not-a-member",
                    "kind": "work.requested",
                    "payload": {}
                }]
            })),
            "semantic-validation phase",
        ),
        (
            "rejection-redaction",
            plugin_script(
                &description_response_with_interface(INTERFACE_ID),
                &json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "error": {"code": -32001, "message": secret}
                })
                .to_string(),
            ),
            "evaluation phase",
        ),
    ];

    for (name, script, expected_phase) in cases {
        let fixture = WorkflowFixture::start().await;
        fixture.release_root().await;
        let mut configuration = fixture.configuration.clone();
        configuration.plugin.executable = fixture.write_plugin(name, &script);
        let mut runner = AuthorReviewRunner::start(configuration)
            .await
            .expect("configuration remains usable after a plugin probe failure");

        let outcome = runner.tick().await.expect("permanent failure is blocked");
        let TickOutcome::Blocked { diagnostic } = outcome else {
            panic!("{name} should block, got {outcome:?}");
        };
        assert!(
            diagnostic.contains(expected_phase),
            "{name} diagnostic was {diagnostic}"
        );
        assert!(diagnostic.contains("recovery:"));
        assert!(diagnostic.len() <= 4096);
        assert!(!diagnostic.contains(secret));
        assert!(!diagnostic.contains(&fixture.runner_agent.credential.token));
        assert!(!diagnostic.contains(&fixture.batch.lease_token));

        let blocked = fixture
            .server
            .store
            .list_deliveries(
                Some(&fixture.runner_agent.agent.id),
                Some(DeliveryState::Blocked),
                10,
            )
            .await
            .expect("list blocked delivery");
        assert_eq!(blocked.len(), 1, "{name} blocked the exact root");
        assert_eq!(blocked[0].message.id, fixture.root.id);
        assert_eq!(blocked[0].last_error.as_deref(), Some(diagnostic.as_str()));
    }
}

#[tokio::test]
async fn dead_child_is_replaced_and_delayed_input_does_not_starve_later_work() {
    let fixture = WorkflowFixture::start().await;
    fixture.release_root().await;
    let second = fixture
        .send_input("FLEETD-RUNNER-SECOND", "dogfood/second")
        .await;
    let marker = fixture
        .server
        .directory
        .path()
        .join("first-generation-failed");
    let script = replacement_plugin_script(&marker);
    let mut configuration = fixture.configuration.clone();
    configuration.plugin.executable = fixture.write_plugin("replacement-plugin", &script);
    configuration.retry_base_delay_ms = 200;
    configuration.retry_max_delay_ms = 800;
    let mut runner = AuthorReviewRunner::start(configuration)
        .await
        .expect("start runner");

    assert_eq!(runner.retry_delay_for_attempt(1), 200);
    assert_eq!(runner.retry_delay_for_attempt(2), 400);
    assert_eq!(runner.retry_delay_for_attempt(3), 800);
    assert_eq!(runner.retry_delay_for_attempt(100), 800);
    let first = runner.tick().await.expect("retry dead child");
    let TickOutcome::Retried {
        retry_after_ms,
        diagnostic,
    } = first
    else {
        panic!("dead child should be retried, got {first:?}");
    };
    assert_eq!(retry_after_ms, 400);
    assert!(diagnostic.contains("response read phase is unavailable"));
    assert!(marker.exists());

    assert_eq!(
        runner
            .tick()
            .await
            .expect("replacement processes later work"),
        TickOutcome::Acknowledged
    );
    let acknowledged = fixture
        .server
        .store
        .list_deliveries(
            Some(&fixture.runner_agent.agent.id),
            Some(DeliveryState::Acknowledged),
            10,
        )
        .await
        .expect("list acknowledged delivery");
    assert!(
        acknowledged
            .iter()
            .any(|record| record.message.id == second.id),
        "later claimable work progressed through the replacement child"
    );
    let immediately_claimable = claim_root(&fixture.server, &fixture.runner_agent).await;
    assert!(
        immediately_claimable.deliveries.is_empty(),
        "the transiently failing input remains delayed beyond the poll interval"
    );
}

#[tokio::test]
async fn divergent_idempotency_conflict_blocks_the_exact_delivery() {
    let fixture = WorkflowFixture::start().await;
    fixture.release_root().await;
    fixture.seed_divergent_root_effect().await;
    let mut runner = AuthorReviewRunner::start(fixture.configuration.clone())
        .await
        .expect("start real plugin runner");

    let outcome = runner
        .tick()
        .await
        .expect("conflict should settle as blocked");
    let TickOutcome::Blocked { diagnostic } = outcome else {
        panic!("divergent effect should block, got {outcome:?}");
    };
    assert!(diagnostic.contains("publication phase"));
    assert!(diagnostic.contains("divergent replay"));
    let blocked = fixture
        .server
        .store
        .list_deliveries(
            Some(&fixture.runner_agent.agent.id),
            Some(DeliveryState::Blocked),
            10,
        )
        .await
        .expect("list blocked delivery");
    assert_eq!(blocked.len(), 1);
    assert_eq!(blocked[0].message.id, fixture.root.id);
}

#[test]
fn retry_controls_reject_poll_interval_reclaim_and_unbounded_delays() {
    let directory = tempfile::tempdir().expect("temporary configuration directory");
    let path = directory.path().join("runner.json");
    let mut configuration = json!({
        "schema_version": 1,
        "fleetd": {
            "origin": "http://127.0.0.1:8787",
            "agent_id": "runner",
            "credential_file": directory.path().join("runner.token")
        },
        "plugin": {
            "executable": std::env::current_exe().expect("current test executable"),
            "args": [],
            "request_timeout_ms": 5000
        },
        "plugin_configuration": {},
        "lease_duration_ms": 15000,
        "poll_interval_ms": 100,
        "retry_base_delay_ms": 100,
        "retry_max_delay_ms": 60000
    });
    std::fs::write(
        &path,
        serde_json::to_vec(&configuration).expect("encode configuration"),
    )
    .expect("write configuration");
    let error = load_configuration(&path).expect_err("poll-rate retry must be rejected");
    assert!(error.to_string().contains("retry_base_delay_ms"));

    configuration["retry_base_delay_ms"] = json!(200);
    configuration["retry_max_delay_ms"] = json!(86_400_001_u64);
    std::fs::write(
        &path,
        serde_json::to_vec(&configuration).expect("encode configuration"),
    )
    .expect("rewrite configuration");
    let error = load_configuration(&path).expect_err("unbounded retry maximum must be rejected");
    assert!(error.to_string().contains("retry_max_delay_ms"));
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

    async fn release_root(&self) {
        let released = self
            .server
            .http
            .post(format!(
                "{}/v1/agents/{}/deliveries/{}/retry",
                self.server.origin, self.runner_agent.agent.id, self.root.id
            ))
            .bearer_auth(&self.runner_agent.credential.token)
            .json(&RetryDelivery {
                lease_token: self.batch.lease_token.clone(),
                retry_after_ms: 0,
                error: Some("test releases the setup lease".to_owned()),
            })
            .send()
            .await
            .expect("release setup lease");
        assert_eq!(released.status(), StatusCode::NO_CONTENT);
    }

    async fn send_input(&self, request_id: &str, idempotency_key: &str) -> Message {
        self.server
            .http
            .post(format!(
                "{}/v1/channels/{}/messages",
                self.server.origin, self.channel_id
            ))
            .bearer_auth(&self.observer.credential.token)
            .json(&SendMessage {
                idempotency_key: Some(idempotency_key.to_owned()),
                recipient_id: Some(self.runner_agent.agent.id.clone()),
                kind: "work.requested".to_owned(),
                payload: root_payload_for(request_id),
                correlation_id: Some(request_id.to_owned()),
                causation_id: None,
            })
            .send()
            .await
            .expect("send later workflow input")
            .error_for_status()
            .expect("later workflow input status")
            .json()
            .await
            .expect("later workflow input body")
    }

    async fn seed_divergent_root_effect(&self) {
        let operation_id = format!("assign-coordinator:{WORKFLOW_ID}");
        let response = self
            .server
            .http
            .post(format!(
                "{}/v1/channels/{}/messages",
                self.server.origin, self.channel_id
            ))
            .bearer_auth(&self.runner_agent.credential.token)
            .json(&SendMessage {
                idempotency_key: Some(format!("workflow/{}/{}", self.root.id, operation_id)),
                recipient_id: Some(self.coordinator.agent.id.clone()),
                kind: "work.requested".to_owned(),
                payload: json!({"divergent": true}),
                correlation_id: Some(WORKFLOW_ID.to_owned()),
                causation_id: Some(self.root.id.clone()),
            })
            .send()
            .await
            .expect("seed divergent effect");
        assert_eq!(response.status(), StatusCode::CREATED);
    }

    fn write_plugin(&self, name: &str, script: &str) -> PathBuf {
        let path = self.server.directory.path().join(name);
        std::fs::write(&path, script).expect("write test plugin");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                .expect("make test plugin executable");
        }
        path
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
    root_payload_for(WORKFLOW_ID)
}

fn root_payload_for(request_id: &str) -> serde_json::Value {
    json!({
        "schema_version": 1,
        "request_id": request_id,
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
        retry_base_delay_ms: 1_000,
        retry_max_delay_ms: 60_000,
    }
}

fn proposal_result(recipient_id: &str, payload: &serde_json::Value) -> serde_json::Value {
    json!({
        "projection": {},
        "proposals": [{
            "operation_id": "commit-effect",
            "recipient_id": recipient_id,
            "kind": "work.requested",
            "payload": payload
        }]
    })
}

fn description_response_with_interface(interface_id: &str) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "interface_id": interface_id,
            "interface_version": INTERFACE_VERSION,
            "plugin_id": PLUGIN_ID,
            "plugin_version": PLUGIN_VERSION,
            "roles": ["coordinator", "author", "reviewer"],
            "event_schemas": EVENT_KINDS.map(|kind| json!({"kind": kind, "schema": {}}))
        }
    })
    .to_string()
}

fn static_plugin_script(evaluation_result: &serde_json::Value) -> String {
    plugin_script(
        &description_response_with_interface(INTERFACE_ID),
        &json!({"jsonrpc": "2.0", "id": 2, "result": evaluation_result}).to_string(),
    )
}

fn single_response_script(response: &str) -> String {
    format!(
        "#!/bin/sh\nIFS= read -r _request || exit 1\nprintf '%s\\n' {}\n",
        shell_literal(response)
    )
}

fn plugin_script(description_response: &str, evaluation_response: &str) -> String {
    format!(
        "#!/bin/sh\nIFS= read -r _request || exit 1\nprintf '%s\\n' {}\nIFS= read -r _request || exit 1\nprintf '%s\\n' {}\n",
        shell_literal(description_response),
        shell_literal(evaluation_response)
    )
}

fn replacement_plugin_script(marker: &std::path::Path) -> String {
    let description = description_response_with_interface(INTERFACE_ID);
    let evaluation =
        json!({"jsonrpc": "2.0", "id": 2, "result": {"projection": {}, "proposals": []}})
            .to_string();
    format!(
        "#!/bin/sh\nIFS= read -r _request || exit 1\nprintf '%s\\n' {}\nIFS= read -r _request || exit 1\nif [ ! -e {} ]; then\n  : > {}\n  exit 17\nfi\nprintf '%s\\n' {}\n",
        shell_literal(&description),
        shell_literal(&marker.to_string_lossy()),
        shell_literal(&marker.to_string_lossy()),
        shell_literal(&evaluation)
    )
}

fn shell_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
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
