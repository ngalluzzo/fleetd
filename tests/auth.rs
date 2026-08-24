use std::fs;

use fleetd::{AuthService, CreateAgent, FleetError, Principal, Store};
use serde_json::json;

#[tokio::test]
async fn operator_bootstrap_is_private_digest_only_and_idempotent() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("fleetd.db");
    let token_path = directory.path().join("operator.token");
    let store = Store::open(&database_path).await.expect("open store");
    let auth = AuthService::new(store);

    let first = auth
        .ensure_operator_credential(&token_path)
        .await
        .expect("bootstrap operator");
    assert!(first.credential_rotated);
    let token = fs::read_to_string(&token_path)
        .expect("read token")
        .trim()
        .to_owned();
    assert!(matches!(
        auth.authenticate(&token)
            .await
            .expect("authenticate operator"),
        Principal::Operator { .. }
    ));
    let second = auth
        .ensure_operator_credential(&token_path)
        .await
        .expect("repeat bootstrap");
    assert!(!second.credential_rotated);
    assert_private_permissions(&token_path);

    for path in [&database_path, &database_path.with_extension("db-wal")] {
        if let Ok(bytes) = fs::read(path) {
            assert!(!contains(&bytes, token.as_bytes()));
        }
    }
}

#[tokio::test]
async fn agent_rotation_revokes_the_old_token_without_changing_identity() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let auth = AuthService::new(store);
    let registration = auth
        .register_agent(CreateAgent {
            name: "piler".to_owned(),
            metadata: json!({ "harness": "dsh" }),
        })
        .await
        .expect("register agent");
    let old_token = registration.credential.token.clone();
    assert!(!format!("{:?}", registration.credential).contains(&old_token));
    let replacement = auth
        .rotate_agent_credential(&registration.agent.id)
        .await
        .expect("rotate credential");

    assert!(matches!(
        auth.authenticate(&old_token).await,
        Err(FleetError::Unauthorized)
    ));
    let principal = auth
        .authenticate(&replacement.token)
        .await
        .expect("authenticate replacement");
    assert_eq!(principal.agent_id(), Some(registration.agent.id.as_str()));
}

#[tokio::test]
async fn a_revoked_operator_file_refuses_to_reconcile() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let store = Store::open(directory.path().join("fleetd.db"))
        .await
        .expect("open store");
    let auth = AuthService::new(store);
    let first_path = directory.path().join("first.token");
    let second_path = directory.path().join("second.token");
    auth.ensure_operator_credential(&first_path)
        .await
        .expect("first operator");
    auth.ensure_operator_credential(&second_path)
        .await
        .expect("second operator");
    let first = fs::read_to_string(&first_path).expect("read first token");
    let second = fs::read_to_string(&second_path).expect("read second token");

    match auth.ensure_operator_credential(&first_path).await {
        Err(FleetError::Credential(message)) => assert!(message.contains("revoked")),
        other => panic!("expected revoked credential error, got {other:?}"),
    }

    assert!(matches!(
        auth.authenticate(second.trim()).await,
        Ok(Principal::Operator { .. })
    ));
    assert!(matches!(
        auth.authenticate(first.trim()).await,
        Err(FleetError::Unauthorized)
    ));
}

#[tokio::test]
async fn a_fresh_database_adopts_an_existing_operator_file() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let token_path = directory.path().join("operator.token");
    let first_store = Store::open(directory.path().join("first.db"))
        .await
        .expect("open first store");
    AuthService::new(first_store)
        .ensure_operator_credential(&token_path)
        .await
        .expect("bootstrap operator");
    let token = fs::read_to_string(&token_path)
        .expect("read token")
        .trim()
        .to_owned();

    let second_store = Store::open(directory.path().join("second.db"))
        .await
        .expect("open second store");
    let adoption = AuthService::new(second_store.clone())
        .ensure_operator_credential(&token_path)
        .await
        .expect("adopt existing file");
    assert!(adoption.credential_rotated);
    assert!(matches!(
        AuthService::new(second_store)
            .authenticate(&token)
            .await
            .expect("authenticate adopted operator"),
        Principal::Operator { .. }
    ));
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[cfg(unix)]
fn assert_private_permissions(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path)
        .expect("token metadata")
        .permissions()
        .mode();
    assert_eq!(mode & 0o077, 0);
}

#[cfg(not(unix))]
fn assert_private_permissions(_path: &std::path::Path) {}
