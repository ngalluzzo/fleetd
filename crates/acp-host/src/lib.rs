//! Reusable typed host for ACP-backed fleetd harness plugins.
//!
//! This crate owns protocol translation and process containment. Harness
//! plugins own runtime identity, launch arguments, configuration, and the
//! environment names they deliberately grant.

mod runtime;

use std::time::Duration;

use fleetd::{PluginIdentity, PluginManifest, harness_acp_interface};
use futures_util::StreamExt;
use runtime::DriverRuntime;
pub use runtime::{DriverConfig, DriverError, RuntimeConfig};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::codec::{FramedRead, LinesCodec};

const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Vendor-owned definition for one ACP-backed fleetd harness plugin.
pub struct PluginDefinition {
    pub id: &'static str,
    pub name: &'static str,
    pub version: &'static str,
    pub allowed_environment: &'static [&'static str],
    pub prepare_config: fn(Value) -> Result<DriverConfig, DriverError>,
}

impl PluginDefinition {
    #[must_use]
    pub const fn new(
        id: &'static str,
        name: &'static str,
        version: &'static str,
        allowed_environment: &'static [&'static str],
        prepare_config: fn(Value) -> Result<DriverConfig, DriverError>,
    ) -> Self {
        Self {
            id,
            name,
            version,
            allowed_environment,
            prepare_config,
        }
    }
}

/// Serves one fleetd lifecycle connection until shutdown or failure.
///
/// # Errors
///
/// Returns protocol, configuration, runtime, JSON, or I/O failures observed by
/// the typed ACP host.
pub async fn serve(definition: PluginDefinition) -> Result<(), DriverError> {
    if std::env::args().nth(1).as_deref() == Some("--inner-launch") {
        inner_launch();
    }

    let stdin = tokio::io::stdin();
    let mut frames = FramedRead::new(stdin, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    let stdout = tokio::io::stdout();
    let mut writer = BufWriter::new(stdout);

    let first = frames
        .next()
        .await
        .ok_or_else(|| DriverError::Protocol("host closed before initialize".to_owned()))?
        .map_err(|error| DriverError::Protocol(error.to_string()))?;
    let request = parse_request(&first)?;
    if request.method != "fleetd.initialize" {
        return Err(DriverError::Protocol(
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
        return Err(DriverError::Protocol(
            "initialize identity fields must not be empty".to_owned(),
        ));
    }

    let config = (definition.prepare_config)(initialize.config)?;
    let (mut runtime, mut notifications) =
        DriverRuntime::start(config, definition.allowed_environment).await?;
    let manifest = PluginManifest {
        protocol_version: 1,
        plugin: PluginIdentity {
            id: definition.id.to_owned(),
            name: definition.name.to_owned(),
            version: definition.version.parse().map_err(|error| {
                DriverError::InvalidConfig(format!("plugin version is not valid SemVer: {error}"))
            })?,
        },
        interfaces: vec![harness_acp_interface()],
    };
    write_result(&mut writer, request.id, serde_json::to_value(manifest)?).await?;

    loop {
        tokio::select! {
            notification = notifications.recv() => {
                let Some(notification) = notification else {
                    return Err(DriverError::Runtime(
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
                let frame = frame.map_err(|error| DriverError::Protocol(error.to_string()))?;
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
    config: Value,
}

fn parse_request(frame: &str) -> Result<RpcRequest, DriverError> {
    let request: RpcRequest = serde_json::from_str(frame)?;
    if request.jsonrpc != "2.0" || request.id.is_null() {
        return Err(DriverError::Protocol(
            "host frame is not a JSON-RPC 2.0 request".to_owned(),
        ));
    }
    Ok(request)
}

async fn write_result(
    writer: &mut BufWriter<tokio::io::Stdout>,
    id: Value,
    result: Value,
) -> Result<(), DriverError> {
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
) -> Result<(), DriverError> {
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
) -> Result<(), DriverError> {
    write_frame(
        writer,
        &json!({"jsonrpc": "2.0", "method": method, "params": params}),
    )
    .await
}

async fn write_frame(
    writer: &mut BufWriter<tokio::io::Stdout>,
    value: &Value,
) -> Result<(), DriverError> {
    let frame = serde_json::to_vec(value)?;
    if frame.len() > MAX_FRAME_BYTES {
        return Err(DriverError::Protocol(format!(
            "outbound frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    writer.write_all(&frame).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
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
