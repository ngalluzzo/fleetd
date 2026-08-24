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
}
