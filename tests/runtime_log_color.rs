use std::{net::SocketAddr, process::Stdio, time::Duration};

use tokio::{
    io::{AsyncBufReadExt as _, BufReader},
    process::Command,
};

const PROCESS_START_DEADLINE: Duration = Duration::from_secs(15);

/// An operator redirecting the daemon's logs to a file, and any tooling that
/// parses them, receives the record itself. The production binary is launched
/// in its default configuration so the qualification covers the wiring rather
/// than a test-only subscriber.
#[tokio::test]
async fn redirected_runtime_logs_carry_no_terminal_escape_sequences() {
    let directory = tempfile::tempdir().expect("temporary daemon directory");
    let database_path = directory.path().join("fleetd.db");
    let operator_token_path = directory.path().join("operator.token");
    let mut daemon = Command::new(env!("CARGO_BIN_EXE_fleetd"))
        .args([
            "serve",
            "--listen",
            "127.0.0.1:0",
            "--db",
            database_path.to_str().expect("UTF-8 database path"),
            "--operator-token-file",
            operator_token_path
                .to_str()
                .expect("UTF-8 operator token path"),
        ])
        .env("RUST_LOG", "fleetd=info")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("launch production fleetd binary");
    let stdout = daemon.stdout.take().expect("capture daemon stdout");
    let mut lines = BufReader::new(stdout).lines();

    let ready = tokio::time::timeout(PROCESS_START_DEADLINE, async {
        while let Some(line) = lines.next_line().await.expect("read daemon stdout") {
            assert!(
                !line.contains('\u{1b}'),
                "redirected runtime log carried an escape sequence: {line:?}"
            );
            if line.contains("fleetd ready") {
                return line;
            }
        }
        panic!("daemon exited before reporting readiness");
    })
    .await
    .expect("production daemon readiness timeout");

    // Every structured field stays addressable by a whitespace-splitting reader.
    let listen = ready
        .split_ascii_whitespace()
        .find_map(|field| field.strip_prefix("listen="))
        .expect("ready record exposes an unadorned listen field");
    listen
        .parse::<SocketAddr>()
        .expect("ready record exposes a parseable listen address");
}
