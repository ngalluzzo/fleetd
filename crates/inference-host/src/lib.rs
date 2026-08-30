//! Reusable lifecycle host for process-backed inference plugins.
//!
//! Vendor plugins own strict configuration and translate it into one exact
//! process launch. This crate owns only lifecycle JSON-RPC, process containment,
//! version admission, and OpenAI-compatible readiness probes.

pub mod config;
mod runtime;

use std::time::Duration;

use fleetd_proto::{
    inference_openai::DescribeResult,
    plugin::{PluginIdentity, PluginManifest},
};
use futures_util::StreamExt;
pub use runtime::RuntimeConfig;
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio_util::codec::{FramedRead, LinesCodec};

use runtime::BackendRuntime;

const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Failures at the vendor configuration, process, HTTP, or protocol boundary.
#[derive(Debug, Error)]
pub enum BackendError {
    #[error("invalid backend configuration: {0}")]
    InvalidConfig(String),
    #[error("backend protocol error: {0}")]
    Protocol(String),
    #[error("backend runtime error: {0}")]
    Runtime(String),
    #[error("backend I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("backend HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("backend JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Fully resolved process and route assembled by one vendor plugin.
pub struct BackendLaunch {
    pub runtime: RuntimeConfig,
    pub description: DescribeResult,
    pub health_url: String,
    pub models_url: String,
    pub startup_timeout: Duration,
}

/// Static identity and policy supplied by one backend plugin executable.
pub struct PluginDefinition {
    pub id: &'static str,
    pub name: &'static str,
    pub version: &'static str,
    pub allowed_environment: &'static [&'static str],
    pub prepare_config: fn(Value) -> Result<BackendLaunch, BackendError>,
}

impl PluginDefinition {
    #[must_use]
    pub const fn new(
        id: &'static str,
        name: &'static str,
        version: &'static str,
        allowed_environment: &'static [&'static str],
        prepare_config: fn(Value) -> Result<BackendLaunch, BackendError>,
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

/// Serves one lifecycle connection and owns its model-server child.
///
/// # Errors
///
/// Returns an error for invalid plugin configuration, lifecycle protocol
/// failure, process failure, or an unavailable configured model route.
pub async fn serve(definition: PluginDefinition) -> Result<(), BackendError> {
    let stdin = tokio::io::stdin();
    let mut frames = FramedRead::new(stdin, LinesCodec::new_with_max_length(MAX_FRAME_BYTES));
    let stdout = tokio::io::stdout();
    let mut writer = BufWriter::new(stdout);
    let first = frames
        .next()
        .await
        .ok_or_else(|| BackendError::Protocol("host closed before initialize".to_owned()))?
        .map_err(|error| BackendError::Protocol(error.to_string()))?;
    let request = parse_request(&first)?;
    if request.method != "fleetd.initialize" {
        return Err(BackendError::Protocol(
            "first request must be fleetd.initialize".to_owned(),
        ));
    }
    let initialize: InitializeParams = serde_json::from_value(request.params)?;
    if initialize.protocol_version != 1 {
        write_error(
            &mut writer,
            request.id,
            -32602,
            "unsupported lifecycle protocol",
        )
        .await?;
        return Ok(());
    }
    if initialize.instance_id.trim().is_empty() || initialize.host_version.trim().is_empty() {
        return Err(BackendError::Protocol(
            "initialize identity fields must not be empty".to_owned(),
        ));
    }
    let launch = (definition.prepare_config)(initialize.config)?;
    validate_launch(&launch)?;
    let mut runtime = BackendRuntime::start(&launch, definition.allowed_environment).await?;
    let description = launch.description;
    let manifest = PluginManifest {
        protocol_version: 1,
        plugin: PluginIdentity {
            id: definition.id.to_owned(),
            name: definition.name.to_owned(),
            version: definition.version.parse().map_err(|error| {
                BackendError::InvalidConfig(format!("plugin version is not valid SemVer: {error}"))
            })?,
        },
        interfaces: vec![fleetd_proto::inference_openai::interface()],
    };
    write_result(&mut writer, request.id, serde_json::to_value(manifest)?).await?;

    while let Some(frame) = frames.next().await {
        let frame = frame.map_err(|error| BackendError::Protocol(error.to_string()))?;
        let request = parse_request(&frame)?;
        match request.method.as_str() {
            "fleetd.health" => {
                let status = if runtime.is_ready().await {
                    "ok"
                } else {
                    "unavailable"
                };
                write_result(&mut writer, request.id, json!({"status": status})).await?;
            }
            "inference.openai.describe" => {
                write_result(&mut writer, request.id, serde_json::to_value(&description)?).await?;
            }
            "fleetd.shutdown" => {
                runtime.stop().await;
                write_result(&mut writer, request.id, json!({"accepted": true})).await?;
                return Ok(());
            }
            _ => {
                write_error(&mut writer, request.id, -32601, "method not found").await?;
            }
        }
    }
    runtime.stop().await;
    Ok(())
}

fn validate_launch(launch: &BackendLaunch) -> Result<(), BackendError> {
    if launch.startup_timeout.is_zero() || launch.startup_timeout > Duration::from_mins(30) {
        return Err(BackendError::InvalidConfig(
            "startup timeout must contain between 1 millisecond and 30 minutes".to_owned(),
        ));
    }
    config::validate_loopback_url("inference health URL", &launch.health_url)?;
    config::validate_loopback_url("inference models URL", &launch.models_url)?;
    config::validate_loopback_url("inference base URL", &launch.description.endpoint.base_url)?;
    if let Some(observer) = &launch.description.observer {
        config::validate_loopback_url("inference observer URL", &observer.url)?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InitializeParams {
    protocol_version: u32,
    instance_id: String,
    host_version: String,
    config: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcRequest {
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

fn parse_request(frame: &str) -> Result<RpcRequest, BackendError> {
    let request: RpcRequest = serde_json::from_str(frame)?;
    if request.jsonrpc != "2.0" || request.id.is_null() {
        return Err(BackendError::Protocol(
            "host frame is not a JSON-RPC 2.0 request".to_owned(),
        ));
    }
    Ok(request)
}

async fn write_result(
    writer: &mut BufWriter<tokio::io::Stdout>,
    id: Value,
    result: Value,
) -> Result<(), BackendError> {
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
) -> Result<(), BackendError> {
    write_frame(
        writer,
        &json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}}),
    )
    .await
}

async fn write_frame(
    writer: &mut BufWriter<tokio::io::Stdout>,
    value: &Value,
) -> Result<(), BackendError> {
    let frame = serde_json::to_vec(value)?;
    if frame.len() > MAX_FRAME_BYTES {
        return Err(BackendError::Protocol(format!(
            "outbound frame exceeds {MAX_FRAME_BYTES} bytes"
        )));
    }
    writer.write_all(&frame).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}
