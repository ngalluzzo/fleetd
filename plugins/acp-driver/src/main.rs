mod runtime;

use std::time::Duration;

use fleetd::{Capability, PluginIdentity, PluginManifest};
use futures_util::StreamExt;
use runtime::{DriverConfig, DriverRuntime};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::codec::{FramedRead, LinesCodec};

const MAX_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
struct RpcRequest {
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Deserialize)]
struct InitializeParams {
    protocol_version: u32,
    instance_id: String,
    host_version: String,
    config: DriverConfig,
}

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("--inner-launch") {
        inner_launch();
    }
    if let Err(error) = run().await {
        eprintln!("fleetd ACP driver failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(unix)]
fn inner_launch() -> ! {
    use std::os::unix::process::CommandExt as _;

    let mut arguments = std::env::args_os().skip(2);
    let process_group = arguments
        .next()
        .and_then(|value| value.to_string_lossy().parse::<i32>().ok())
        .unwrap_or_else(|| {
            eprintln!("inner launcher requires a valid parent process group");
            std::process::exit(2);
        });
    let executable = arguments.next().unwrap_or_else(|| {
        eprintln!("inner launcher requires an executable");
        std::process::exit(2);
    });
    if let Err(error) = nix::unistd::setpgid(
        nix::unistd::Pid::from_raw(0),
        nix::unistd::Pid::from_raw(process_group),
    ) {
        eprintln!("inner launcher failed to join parent process group: {error}");
        std::process::exit(2);
    }
    let error = std::process::Command::new(executable)
        .args(arguments)
        .exec();
    eprintln!("inner launcher failed to exec runtime: {error}");
    std::process::exit(2);
}

#[cfg(not(unix))]
fn inner_launch() -> ! {
    eprintln!("inner launcher is not implemented on this platform");
    std::process::exit(2);
}

async fn run() -> Result<(), runtime::DriverError> {
    let stdin = tokio::io::stdin();
    let mut frames = FramedRead::new(stdin, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    let stdout = tokio::io::stdout();
    let mut writer = BufWriter::new(stdout);

    let first = frames
        .next()
        .await
        .ok_or_else(|| runtime::DriverError::Protocol("host closed before initialize".to_owned()))?
        .map_err(|error| runtime::DriverError::Protocol(error.to_string()))?;
    let request = parse_request(&first)?;
    if request.method != "fleetd.initialize" {
        return Err(runtime::DriverError::Protocol(
            "first request must be fleetd.initialize".to_owned(),
        ));
    }
    let initialize: InitializeParams = serde_json::from_value(request.params.clone())?;
    if initialize.protocol_version != 1 {
        write_error(
            &mut writer,
            request.id,
            -32602,
            "unsupported lifecycle protocol",
            None,
        )
        .await?;
        return Ok(());
    }
    if initialize.instance_id.trim().is_empty() || initialize.host_version.trim().is_empty() {
        return Err(runtime::DriverError::Protocol(
            "initialize identity fields must not be empty".to_owned(),
        ));
    }

    let (mut runtime, mut notifications) = DriverRuntime::start(initialize.config).await?;
    let manifest = PluginManifest {
        protocol_version: 1,
        plugin: PluginIdentity {
            id: "fleetd.acp-driver".to_owned(),
            name: "fleetd ACP driver".to_owned(),
            version: env!("CARGO_PKG_VERSION").parse().expect("package semver"),
        },
        capabilities: vec![Capability {
            name: "harness.acp".to_owned(),
            version: 1,
        }],
    };
    write_result(&mut writer, request.id, serde_json::to_value(manifest)?).await?;

    loop {
        tokio::select! {
            notification = notifications.recv() => {
                let Some(notification) = notification else {
                    return Err(runtime::DriverError::Runtime(
                        "inner ACP notification channel closed".to_owned(),
                    ));
                };
                write_notification(&mut writer, notification.method, notification.params).await?;
            }
            frame = frames.next() => {
                let Some(frame) = frame else {
                    runtime.stop().await;
                    return Ok(());
                };
                let frame = frame.map_err(|error| runtime::DriverError::Protocol(error.to_string()))?;
                let request = parse_request(&frame)?;
                let id = request.id.clone();
                if request.method == "fleetd.shutdown" {
                    runtime.stop().await;
                    write_result(&mut writer, id, json!({"accepted": true})).await?;
                    return Ok(());
                }
                let response = tokio::time::timeout(
                    Duration::from_secs(30),
                    runtime.handle(&request.method, request.params),
                )
                .await;
                match response {
                    Ok(Ok(result)) => write_result(&mut writer, id, result).await?,
                    Ok(Err(error)) => {
                        write_error(&mut writer, id, error.code(), &error.to_string(), None).await?;
                    }
                    Err(_) => {
                        write_error(&mut writer, id, -32001, "driver operation timed out", None).await?;
                    }
                }
            }
        }
    }
}

fn parse_request(frame: &str) -> Result<RpcRequest, runtime::DriverError> {
    let request: RpcRequest = serde_json::from_str(frame)?;
    if request.jsonrpc != "2.0" || request.id.is_null() {
        return Err(runtime::DriverError::Protocol(
            "host frame is not a JSON-RPC 2.0 request".to_owned(),
        ));
    }
    Ok(request)
}

async fn write_result(
    writer: &mut BufWriter<tokio::io::Stdout>,
    id: Value,
    result: Value,
) -> Result<(), runtime::DriverError> {
    write_frame(
        writer,
        &json!({"jsonrpc": "2.0", "id": id, "result": result}),
    )
    .await
}

async fn write_error(
    writer: &mut BufWriter<tokio::io::Stdout>,
    id: Value,
    code: i64,
    message: &str,
    data: Option<Value>,
) -> Result<(), runtime::DriverError> {
    write_frame(
        writer,
        &json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": code, "message": message, "data": data}
        }),
    )
    .await
}

async fn write_notification(
    writer: &mut BufWriter<tokio::io::Stdout>,
    method: String,
    params: Value,
) -> Result<(), runtime::DriverError> {
    write_frame(
        writer,
        &json!({"jsonrpc": "2.0", "method": method, "params": params}),
    )
    .await
}

async fn write_frame(
    writer: &mut BufWriter<tokio::io::Stdout>,
    value: &Value,
) -> Result<(), runtime::DriverError> {
    let frame = serde_json::to_vec(value)?;
    if frame.len() > MAX_FRAME_BYTES {
        return Err(runtime::DriverError::Protocol(format!(
            "outbound frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    writer.write_all(&frame).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}
