#![cfg(unix)]

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use fleetd::plugin::{
    MacOsSeatbeltPosture, MacOsSeatbeltSandbox, PluginError, PluginInterface, PluginProcess,
    PluginSpec, SandboxNetwork, ShutdownOutcome,
};
use semver::Version;
use serde_json::json;

fn fixture_spec(mode: &str) -> PluginSpec {
    PluginSpec::new("mock.plugin", "/bin/sh")
        .with_arg(fixture_path())
        .with_arg(mode)
        .with_config(json!({ "secret": "must-not-appear-in-debug" }))
        .require_interface(PluginInterface::new(
            "fleetd.test.echo",
            Version::new(1, 0, 0),
        ))
        .with_initialize_timeout(Duration::from_millis(250))
        .with_request_timeout(Duration::from_millis(250))
        .with_shutdown_timeout(Duration::from_millis(100))
}

fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mock_plugin.sh")
}

async fn start_error(spec: PluginSpec, message: &str) -> PluginError {
    match PluginProcess::start(spec).await {
        Ok(plugin) => {
            let _shutdown = plugin.shutdown().await;
            panic!("{message}");
        }
        Err(error) => error,
    }
}

#[tokio::test]
async fn healthy_plugin_negotiates_interfaces_notifications_and_shutdown() {
    let spec = fixture_spec("healthy");
    assert!(!format!("{spec:?}").contains("must-not-appear-in-debug"));
    let mut plugin = PluginProcess::start(spec).await.expect("start plugin");
    assert_eq!(plugin.manifest().plugin.id, "mock.plugin");
    assert!(plugin.process_id().is_some());
    plugin.health().await.expect("second health check");
    let notification = plugin.try_notification().expect("ready notification");
    assert_eq!(notification.method, "mock.ready");
    assert_eq!(notification.params["ready"], true);
    let outcome = plugin.shutdown().await.expect("graceful shutdown");
    assert!(matches!(
        outcome,
        ShutdownOutcome::Graceful(exit) if exit.success && exit.code == Some(0)
    ));
}

#[tokio::test]
async fn identity_and_interface_mismatches_fail_closed() {
    let identity = start_error(fixture_spec("wrong-id"), "wrong identity must fail").await;
    assert!(matches!(identity, PluginError::IdentityMismatch { .. }));

    let interface = start_error(
        fixture_spec("missing-interface"),
        "missing interface must fail",
    )
    .await;
    assert!(matches!(interface, PluginError::MissingInterface { .. }));

    let duplicate = start_error(
        fixture_spec("duplicate-interface"),
        "duplicate interface must fail",
    )
    .await;
    assert!(matches!(duplicate, PluginError::InvalidManifest(_)));

    let protocol = start_error(
        fixture_spec("unsupported-protocol"),
        "unsupported lifecycle protocol must fail",
    )
    .await;
    assert!(matches!(protocol, PluginError::ProtocolVersion { .. }));
}

#[tokio::test]
async fn hung_and_malformed_plugins_fail_with_bounded_startup() {
    let timeout = start_error(fixture_spec("hang"), "hung plugin must time out").await;
    assert!(matches!(
        timeout,
        PluginError::Timeout { method, .. } if method == "fleetd.initialize"
    ));

    let malformed = start_error(fixture_spec("malformed"), "malformed protocol must fail").await;
    assert!(matches!(malformed, PluginError::Protocol(_)));

    let plugin_request = start_error(
        fixture_spec("plugin-request"),
        "plugin-initiated request must fail",
    )
    .await;
    assert!(matches!(plugin_request, PluginError::Protocol(_)));

    let unhealthy = start_error(fixture_spec("unhealthy"), "unhealthy plugin must fail").await;
    assert!(matches!(unhealthy, PluginError::Unhealthy(status) if status == "degraded"));

    let started = Instant::now();
    let blocked_write = start_error(
        fixture_spec("never-read")
            .with_config(json!({ "blob": "x".repeat(512 * 1024) }))
            .with_initialize_timeout(Duration::from_millis(100)),
        "plugin that never reads stdin must time out",
    )
    .await;
    assert!(matches!(
        blocked_write,
        PluginError::Timeout { method, .. } if method == "fleetd.initialize"
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn unexpected_exit_and_shutdown_overrun_produce_evidence() {
    let mut crashing = PluginProcess::start(fixture_spec("crash-after-health"))
        .await
        .expect("start crashing plugin");
    let exit = crashing.wait_for_exit().await.expect("wait for crash");
    assert!(!exit.success);
    assert_eq!(exit.code, Some(17));

    let forced = PluginProcess::start(fixture_spec("force-shutdown"))
        .await
        .expect("start stubborn plugin")
        .shutdown()
        .await
        .expect("force shutdown");
    assert!(matches!(forced, ShutdownOutcome::Forced(exit) if !exit.success));
}

#[tokio::test]
async fn dropping_plugin_kills_its_descendant_process_group() {
    let mut plugin = PluginProcess::start(fixture_spec("descendant"))
        .await
        .expect("start plugin with descendant");
    let ready = plugin.try_notification().expect("ready notification");
    assert_eq!(ready.method, "mock.ready");
    let descendant = plugin
        .try_notification()
        .expect("descendant PID notification");
    let descendant_pid = descendant.params["pid"]
        .as_i64()
        .and_then(|pid| i32::try_from(pid).ok())
        .expect("valid descendant PID");
    assert!(process_exists(descendant_pid));

    drop(plugin);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while process_exists(descendant_pid) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!process_exists(descendant_pid));
}

#[tokio::test]
async fn dropping_exited_plugin_still_kills_its_surviving_descendant() {
    let mut plugin = PluginProcess::start(fixture_spec("orphan-descendant"))
        .await
        .expect("start exiting plugin with descendant");
    let ready = plugin.try_notification().expect("ready notification");
    assert_eq!(ready.method, "mock.ready");
    let descendant = plugin
        .try_notification()
        .expect("descendant PID notification");
    let descendant_pid = descendant.params["pid"]
        .as_i64()
        .and_then(|pid| i32::try_from(pid).ok())
        .expect("valid descendant PID");
    let notification_deadline = tokio::time::Instant::now() + Duration::from_secs(1);
    let exiting = loop {
        if let Some(notification) = plugin.try_notification() {
            break notification;
        }
        assert!(
            tokio::time::Instant::now() < notification_deadline,
            "outer exit notification timed out"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(exiting.method, "mock.outer_exiting");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(process_exists(descendant_pid));

    drop(plugin);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while process_exists(descendant_pid) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(!process_exists(descendant_pid));
}

#[tokio::test]
async fn plugin_executables_must_be_absolute() {
    let error = start_error(
        PluginSpec::new("mock.plugin", "relative-plugin"),
        "relative executable must fail",
    )
    .await;
    assert!(matches!(error, PluginError::InvalidSpec(_)));

    let identifier = start_error(
        PluginSpec::new("invalid..plugin", "/bin/sh"),
        "malformed plugin identifier must fail",
    )
    .await;
    assert!(matches!(identifier, PluginError::InvalidSpec(_)));
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn seatbelt_allows_declared_writes_and_denies_sibling_escape() {
    let directory = tempfile::tempdir().expect("create sandbox fixture");
    let writable = directory.path().join("writable");
    std::fs::create_dir(&writable).expect("create writable root");
    let allowed = writable.join("allowed.txt");
    let denied = directory.path().join("denied.txt");
    let fixture = fixture_path();
    let sandbox = MacOsSeatbeltSandbox::new(
        [writable],
        [fixture
            .parent()
            .expect("fixture has a parent")
            .to_path_buf()],
        SandboxNetwork::Deny,
    )
    .expect("build Seatbelt policy");
    let spec = PluginSpec::new("mock.plugin", "/bin/sh")
        .with_arg(fixture)
        .with_arg("sandbox-write")
        .with_arg(&allowed)
        .with_arg(&denied)
        .with_config(json!({}))
        .require_interface(PluginInterface::new(
            "fleetd.test.echo",
            Version::new(1, 0, 0),
        ))
        .with_macos_seatbelt(sandbox)
        .with_initialize_timeout(Duration::from_secs(2));

    let plugin = PluginProcess::start(spec)
        .await
        .expect("sandboxed plugin starts after contained denial");
    assert_eq!(
        std::fs::read_to_string(allowed).expect("read allowed marker"),
        "allowed\n"
    );
    assert!(!denied.exists());
    plugin.shutdown().await.expect("shutdown sandboxed plugin");
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn write_scoped_reads_runtime_binds_loopback_and_confines_descendant_writes() {
    let directory = tempfile::tempdir().expect("create sandbox fixture");
    let workspace = directory.path().join("workspace");
    let state = directory.path().join("state");
    let temp = directory.path().join("temp");
    std::fs::create_dir(&workspace).expect("create workspace");
    std::fs::create_dir(&state).expect("create state root");
    std::fs::create_dir(&temp).expect("create temp root");
    let workspace_file = workspace.join("workspace.txt");
    let state_file = state.join("state.txt");
    let temp_file = temp.join("temp.txt");
    let descendant_file = workspace.join("descendant.txt");
    let denial_evidence = workspace.join("denial-evidence.txt");
    let denied = directory.path().join("denied.txt");
    let runtime_fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/write_scoped_runtime.txt");
    let sandbox = MacOsSeatbeltSandbox::write_scoped([workspace, state, temp])
        .expect("build write-scoped policy");
    assert_eq!(sandbox.posture(), MacOsSeatbeltPosture::WriteScoped);
    assert_eq!(
        sandbox.security_scope(),
        "writes_scoped_reads_and_network_unrestricted"
    );
    let digest = sandbox.profile_digest().to_owned();
    let spec = fixture_spec("sandbox-write-scoped")
        .with_arg(&workspace_file)
        .with_arg(&state_file)
        .with_arg(&temp_file)
        .with_arg(runtime_fixture)
        .with_arg(&denied)
        .with_arg(&descendant_file)
        .with_arg(&denial_evidence)
        .with_macos_seatbelt(sandbox);
    assert_eq!(
        spec.sandbox_posture(),
        Some(MacOsSeatbeltPosture::WriteScoped)
    );
    assert_eq!(spec.sandbox_profile_digest(), Some(digest.as_str()));

    let plugin = PluginProcess::start(spec)
        .await
        .expect("write-scoped plugin starts after bounded descendant denial and loopback bind");
    assert_eq!(
        std::fs::read_to_string(workspace_file).expect("read workspace marker"),
        "workspace\n"
    );
    assert_eq!(
        std::fs::read_to_string(state_file).expect("read state marker"),
        "state\n"
    );
    assert_eq!(
        std::fs::read_to_string(temp_file).expect("read temp marker"),
        "temp\n"
    );
    assert_eq!(
        std::fs::read_to_string(descendant_file).expect("read descendant marker"),
        "descendant\n"
    );
    assert_eq!(
        std::fs::read_to_string(denial_evidence).expect("read denial evidence"),
        "os.open flags=O_WRONLY|O_CREAT|O_TRUNC errno=1 name=EPERM message=Operation not permitted\n"
    );
    assert!(!denied.exists());
    plugin
        .shutdown()
        .await
        .expect("shutdown write-scoped plugin");
}

fn process_exists(pid: i32) -> bool {
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
}
