#![cfg(unix)]

use std::{
    net::SocketAddr,
    process::Stdio,
    sync::{Arc, Mutex},
    time::Duration,
};

use fleetd::{
    browser_stream_edge::{
        BROWSER_STREAM_PROTOCOL, BrowserStreamGrantIssueResponse,
        BrowserStreamRedemptionMessageType, BrowserStreamRedemptionRequest,
        BrowserStreamServerFrame,
    },
    model::{Channel, CreateAgent, CreateChannel, RegisteredAgent, SendMessage},
};
use futures_util::{SinkExt, StreamExt};
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use serde_json::json;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    task::JoinHandle,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message as WebSocketMessage,
        client::IntoClientRequest,
        http::{
            HeaderValue,
            header::{ORIGIN, SEC_WEBSOCKET_PROTOCOL},
        },
    },
};

const PROCESS_START_DEADLINE: Duration = Duration::from_secs(15);
const IO_DEADLINE: Duration = Duration::from_secs(5);

struct DaemonProcess {
    child: Child,
    stdout: Arc<Mutex<Vec<u8>>>,
    stderr: Arc<Mutex<Vec<u8>>>,
    stdout_capture: JoinHandle<()>,
    stderr_capture: JoinHandle<()>,
}

struct QualificationSecrets {
    agent_token: String,
    stream_grant: String,
}

#[tokio::test]
async fn production_runtime_logs_omit_exact_browser_stream_secrets() {
    let directory = tempfile::tempdir().expect("temporary daemon directory");
    let database_path = directory.path().join("fleetd.db");
    let operator_token_path = directory.path().join("operator.token");
    let mut daemon = spawn_daemon(&database_path, &operator_token_path);

    let address = wait_for_ready_address(&daemon.stdout, &daemon.stderr).await;
    let operator_token = std::fs::read_to_string(&operator_token_path)
        .expect("read generated operator credential")
        .trim()
        .to_owned();
    assert!(operator_token.starts_with("fl_op_"));
    let secrets = exercise_public_browser_surface(address, &operator_token).await;

    let status = terminate_cleanly(&mut daemon.child).await;
    assert!(status.success(), "daemon did not shut down cleanly");
    daemon
        .stdout_capture
        .await
        .expect("stdout capture completed");
    daemon
        .stderr_capture
        .await
        .expect("stderr capture completed");
    let stdout = daemon.stdout.lock().expect("stdout capture lock").clone();
    let stderr = daemon.stderr.lock().expect("stderr capture lock").clone();

    assert_secret_absent(
        &stdout,
        operator_token.as_bytes(),
        "stdout operator credential",
    );
    assert_secret_absent(
        &stderr,
        operator_token.as_bytes(),
        "stderr operator credential",
    );
    assert_secret_absent(
        &stdout,
        secrets.agent_token.as_bytes(),
        "stdout agent credential",
    );
    assert_secret_absent(
        &stderr,
        secrets.agent_token.as_bytes(),
        "stderr agent credential",
    );
    assert_secret_absent(
        &stdout,
        secrets.stream_grant.as_bytes(),
        "stdout stream grant",
    );
    assert_secret_absent(
        &stderr,
        secrets.stream_grant.as_bytes(),
        "stderr stream grant",
    );
}

async fn exercise_public_browser_surface(
    address: SocketAddr,
    operator_token: &str,
) -> QualificationSecrets {
    let client = reqwest::Client::new();
    let registered = register_sentinel_agent(&client, address, operator_token).await;
    let agent_token = registered.credential.token;
    assert!(agent_token.starts_with("fl_ag_"));
    let channel =
        create_sentinel_channel(&client, address, operator_token, registered.agent.id).await;
    let issued = issue_sentinel_grant(&client, address, &channel.id, &agent_token).await;
    let stream_grant = issued.grant.expose_secret().to_owned();
    assert!(stream_grant.starts_with("fl_sg_"));
    redeem_and_receive_live_message(&client, address, &channel.id, &agent_token, issued).await;
    QualificationSecrets {
        agent_token,
        stream_grant,
    }
}

async fn register_sentinel_agent(
    client: &reqwest::Client,
    address: SocketAddr,
    operator_token: &str,
) -> RegisteredAgent {
    client
        .post(format!("http://{address}/v1/agents"))
        .bearer_auth(operator_token)
        .json(&CreateAgent {
            name: "runtime-log-sentinel-agent".to_owned(),
            metadata: json!({"qualification": "runtime-log-sentinel"}),
        })
        .send()
        .await
        .expect("register sentinel agent")
        .error_for_status()
        .expect("successful sentinel agent registration")
        .json()
        .await
        .expect("sentinel agent response")
}

async fn create_sentinel_channel(
    client: &reqwest::Client,
    address: SocketAddr,
    operator_token: &str,
    agent_id: String,
) -> Channel {
    client
        .post(format!("http://{address}/v1/channels"))
        .bearer_auth(operator_token)
        .json(&CreateChannel {
            name: "runtime-log-sentinel-channel".to_owned(),
            metadata: json!({}),
            member_ids: vec![agent_id],
            members: Vec::new(),
        })
        .send()
        .await
        .expect("create sentinel channel")
        .error_for_status()
        .expect("successful sentinel channel creation")
        .json()
        .await
        .expect("sentinel channel response")
}

async fn issue_sentinel_grant(
    client: &reqwest::Client,
    address: SocketAddr,
    channel_id: &str,
    agent_token: &str,
) -> BrowserStreamGrantIssueResponse {
    client
        .post(format!(
            "http://{address}/v1/channels/{channel_id}/stream-grants"
        ))
        .bearer_auth(agent_token)
        .json(&json!({
            "after": 0,
            "protocol": BROWSER_STREAM_PROTOCOL
        }))
        .send()
        .await
        .expect("issue sentinel browser stream grant")
        .error_for_status()
        .expect("successful sentinel grant issuance")
        .json()
        .await
        .expect("sentinel grant response")
}

async fn redeem_and_receive_live_message(
    client: &reqwest::Client,
    address: SocketAddr,
    channel_id: &str,
    agent_token: &str,
    issued: BrowserStreamGrantIssueResponse,
) {
    let mut upgrade_request = format!("ws://{address}/v1/browser/channel-stream")
        .into_client_request()
        .expect("build browser stream request");
    upgrade_request.headers_mut().insert(
        ORIGIN,
        HeaderValue::from_str(&format!("http://{address}")).expect("origin header"),
    );
    upgrade_request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(BROWSER_STREAM_PROTOCOL),
    );
    let (mut socket, _) = connect_async(upgrade_request)
        .await
        .expect("upgrade sentinel browser stream");
    socket
        .send(WebSocketMessage::Text(
            serde_json::to_string(&BrowserStreamRedemptionRequest {
                message_type: BrowserStreamRedemptionMessageType::Redeem,
                grant: issued.grant,
            })
            .expect("serialize sentinel redemption")
            .into(),
        ))
        .await
        .expect("redeem sentinel browser stream grant");
    let ready = next_server_frame(&mut socket).await;
    assert!(matches!(ready, BrowserStreamServerFrame::Ready { .. }));

    client
        .post(format!(
            "http://{address}/v1/channels/{channel_id}/messages"
        ))
        .bearer_auth(agent_token)
        .json(&SendMessage {
            idempotency_key: Some("runtime-log-sentinel-message".to_owned()),
            recipient_id: None,
            kind: "qualification.runtime-log/v1".to_owned(),
            payload: json!({"sentinel": true}),
            correlation_id: None,
            causation_id: None,
        })
        .send()
        .await
        .expect("append sentinel message")
        .error_for_status()
        .expect("successful sentinel message append");
    let delivered = next_server_frame(&mut socket).await;
    assert!(matches!(
        delivered,
        BrowserStreamServerFrame::Message { .. }
    ));
    socket.close(None).await.expect("close browser stream");
}

fn spawn_daemon(
    database_path: &std::path::Path,
    operator_token_path: &std::path::Path,
) -> DaemonProcess {
    let stdout_capture_buffer = Arc::new(Mutex::new(Vec::new()));
    let stderr_capture_buffer = Arc::new(Mutex::new(Vec::new()));
    let mut command = Command::new(env!("CARGO_BIN_EXE_fleetd"));
    command
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
        .env(
            "RUST_LOG",
            "trace,tungstenite::protocol=trace,tungstenite::protocol::frame=trace",
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().expect("launch production fleetd binary");
    let stdout = child.stdout.take().expect("capture daemon stdout");
    let stderr = child.stderr.take().expect("capture daemon stderr");
    let stdout_capture = tokio::spawn(capture_output(stdout, Arc::clone(&stdout_capture_buffer)));
    let stderr_capture = tokio::spawn(capture_output(stderr, Arc::clone(&stderr_capture_buffer)));
    DaemonProcess {
        child,
        stdout: stdout_capture_buffer,
        stderr: stderr_capture_buffer,
        stdout_capture,
        stderr_capture,
    }
}

async fn capture_output(mut reader: impl AsyncRead + Unpin, output: Arc<Mutex<Vec<u8>>>) {
    let mut buffer = [0_u8; 8_192];
    loop {
        let count = reader
            .read(&mut buffer)
            .await
            .expect("capture daemon output");
        if count == 0 {
            return;
        }
        output
            .lock()
            .expect("capture buffer lock")
            .extend_from_slice(&buffer[..count]);
    }
}

async fn wait_for_ready_address(
    stdout: &Arc<Mutex<Vec<u8>>>,
    stderr: &Arc<Mutex<Vec<u8>>>,
) -> SocketAddr {
    tokio::time::timeout(PROCESS_START_DEADLINE, async {
        loop {
            let address = [stdout, stderr].into_iter().find_map(|output| {
                let output = output.lock().expect("capture buffer lock");
                parse_ready_address(&output)
            });
            if let Some(address) = address {
                return address;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("production daemon readiness timeout")
}

/// Removes ANSI escape sequences from captured runtime output.
///
/// The daemon's subscriber colorizes unconditionally, so the ready line arrives
/// as `\e[3mlisten\e[0m\e[2m=\e[0m127.0.0.1:0`. Parsing the decorated stream
/// keeps this qualification on the exact production log configuration instead of
/// forcing the daemon into a test-only rendering mode.
fn strip_ansi(output: &str) -> String {
    let mut plain = String::with_capacity(output.len());
    let mut characters = output.chars();
    while let Some(character) = characters.next() {
        if character != '\u{1b}' {
            plain.push(character);
            continue;
        }
        if characters.next() != Some('[') {
            continue;
        }
        // A CSI sequence ends at its first final byte in 0x40..=0x7e.
        for parameter in characters.by_ref() {
            if matches!(parameter, '\u{40}'..='\u{7e}') {
                break;
            }
        }
    }
    plain
}

fn parse_ready_address(output: &[u8]) -> Option<SocketAddr> {
    let output = std::str::from_utf8(output).ok()?;
    let output = strip_ansi(output);
    output.lines().find_map(|line| {
        line.contains("fleetd ready")
            .then(|| {
                line.split_ascii_whitespace()
                    .find_map(|field| field.strip_prefix("listen="))
                    .and_then(|address| address.parse().ok())
            })
            .flatten()
    })
}

async fn next_server_frame(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> BrowserStreamServerFrame {
    let frame = tokio::time::timeout(IO_DEADLINE, socket.next())
        .await
        .expect("browser server frame timeout")
        .expect("browser stream remained open")
        .expect("valid browser server frame");
    serde_json::from_str(frame.to_text().expect("browser text frame"))
        .expect("tagged browser server frame")
}

async fn terminate_cleanly(child: &mut Child) -> std::process::ExitStatus {
    let raw_pid = child.id().expect("running daemon process");
    let pid = i32::try_from(raw_pid).expect("daemon PID fits platform type");
    kill(Pid::from_raw(pid), Signal::SIGINT).expect("signal daemon shutdown");
    if let Ok(result) = tokio::time::timeout(IO_DEADLINE, child.wait()).await {
        return result.expect("wait for clean daemon shutdown");
    }
    child.start_kill().expect("force-stop stalled daemon");
    let _status = child.wait().await.expect("reap stalled daemon");
    panic!("daemon did not terminate after shutdown signal");
}

fn assert_secret_absent(surface: &[u8], secret: &[u8], surface_name: &'static str) {
    assert!(
        !surface.windows(secret.len()).any(|window| window == secret),
        "runtime log secret disclosure on {surface_name}"
    );
}
