use fleetd::{
    AcquireSessionBinding, ArmInvocation, BlockDelivery, ClaimDeliveries, CreateAgent,
    CreateChannel, CreateMessage, SessionBindingState,
};
use serde_json::json;
use sqlx::{Connection, sqlite::SqliteConnectOptions};

#[tokio::test]
async fn an_m0_database_upgrades_without_losing_existing_data() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("fleetd.db");
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true);
    let mut connection = sqlx::SqliteConnection::connect_with(&options)
        .await
        .expect("open legacy database");
    sqlx::raw_sql(
        r#"
            CREATE TABLE agents (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                metadata_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            CREATE TABLE channels (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                metadata_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            CREATE TABLE channel_members (
                channel_id TEXT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
                agent_id TEXT NOT NULL REFERENCES agents(id) ON DELETE CASCADE,
                joined_at_ms INTEGER NOT NULL,
                PRIMARY KEY (channel_id, agent_id)
            );
            CREATE TABLE messages (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                channel_id TEXT NOT NULL REFERENCES channels(id),
                sender_id TEXT NOT NULL REFERENCES agents(id),
                recipient_id TEXT REFERENCES agents(id),
                kind TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                correlation_id TEXT,
                causation_id TEXT,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX messages_channel_seq ON messages(channel_id, seq);
            INSERT INTO agents (id, name, metadata_json, created_at_ms)
            VALUES ('legacy-agent', 'piler', '{"harness":"dsh"}', 1);
            "#,
    )
    .execute(&mut connection)
    .await
    .expect("create M0 schema");
    connection.close().await.expect("close legacy database");

    let store = fleetd::Store::open(&path).await.expect("migrate database");
    let agents = store.list_agents().await.expect("list migrated agents");
    assert_eq!(agents.len(), 1);
    assert_eq!(agents[0].id, "legacy-agent");
    assert_eq!(agents[0].metadata["harness"], "dsh");

    let recipient = store
        .create_agent(CreateAgent {
            name: "weaver".to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create recipient after migration");
    let channel = store
        .create_channel(CreateChannel {
            name: "migrated-channel".to_owned(),
            metadata: json!({}),
            member_ids: vec!["legacy-agent".to_owned(), recipient.id.clone()],
        })
        .await
        .expect("create channel after migration");
    let input = CreateMessage {
        sender_id: "legacy-agent".to_owned(),
        idempotency_key: Some("migration/result".to_owned()),
        recipient_id: Some(recipient.id.clone()),
        kind: "agent.output/v1".to_owned(),
        payload: json!({ "text": "survived" }),
        correlation_id: None,
        causation_id: None,
    };
    let created = store
        .append_message_idempotent(&channel.id, input.clone())
        .await
        .expect("append idempotent message after migration");
    let replayed = store
        .append_message_idempotent(&channel.id, input)
        .await
        .expect("replay idempotent message after migration");
    assert!(created.created);
    assert!(!replayed.created);
    assert_eq!(created.message, replayed.message);

    assert_managed_blocking_works_after_migration(&store, &recipient.id, &created.message.id).await;
    assert_session_bindings_work_after_migration(&store, &recipient.id).await;
}

async fn assert_session_bindings_work_after_migration(store: &fleetd::Store, agent_id: &str) {
    let acquired = store
        .acquire_session_binding(
            agent_id,
            AcquireSessionBinding {
                lane_policy: "per-agent".to_owned(),
                lane_key: "primary".to_owned(),
                owner_instance_id: "migration-controller".to_owned(),
                profile_digest: "sha256:migration-profile".to_owned(),
                compatibility_digest: "sha256:migration-driver".to_owned(),
                working_directory: env!("CARGO_MANIFEST_DIR").to_owned(),
                additional_directories: Vec::new(),
            },
        )
        .await
        .expect("acquire session after migration");
    assert_eq!(acquired.session.state, SessionBindingState::Opening);
    let ready = store
        .record_session_opened(agent_id, &acquired.session.binding, "migrated-session")
        .await
        .expect("persist session after migration");
    assert_eq!(ready.state, SessionBindingState::Ready);
}

async fn assert_managed_blocking_works_after_migration(
    store: &fleetd::Store,
    recipient_id: &str,
    message_id: &str,
) {
    let batch = store
        .reserve_invocations(
            recipient_id,
            ClaimDeliveries {
                limit: 1,
                lease_duration_ms: 10_000,
            },
        )
        .await
        .expect("reserve invocation after migration");
    let invocation = batch.invocations.first().expect("one invocation");
    store
        .arm_invocation(
            recipient_id,
            &invocation.id,
            ArmInvocation {
                lease_token: invocation.lease_token.clone(),
                fence_token: invocation.fence_token.clone(),
            },
        )
        .await
        .expect("arm invocation after migration");
    let (blocked, was_created) = store
        .block_delivery(
            recipient_id,
            message_id,
            BlockDelivery {
                lease_token: invocation.lease_token.clone(),
                reason: "migration smoke test".to_owned(),
            },
        )
        .await
        .expect("block delivery after migration");
    assert!(was_created);
    assert_eq!(
        store
            .list_blocked_deliveries(Some(recipient_id))
            .await
            .expect("list block after migration"),
        vec![blocked]
    );
}
