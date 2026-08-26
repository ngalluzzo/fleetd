use std::{ffi::OsString, fmt, path::PathBuf, process::Stdio, time::Duration};

use serde_json::{Value, json};
use thiserror::Error;
use tokio::process::{Child, Command};
use uuid::Uuid;

use crate::{
    protocol::{
        HealthResult, InitializeParams, LIFECYCLE_PROTOCOL_VERSION, PluginInterface,
        PluginManifest, PluginNotification, ShutdownResult, validate_identifier,
    },
    rpc::RpcPeer,
};

const DEFAULT_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Failures at the plugin process or lifecycle protocol boundary.
#[derive(Debug, Error)]
pub enum PluginError {
    #[error("invalid plugin specification: {0}")]
    InvalidSpec(String),
    #[error("invalid plugin manifest: {0}")]
    InvalidManifest(String),
    #[error("plugin identity mismatch: expected {expected}, received {actual}")]
    IdentityMismatch { expected: String, actual: String },
    #[error("plugin lifecycle version mismatch: expected {expected}, received {actual}")]
    ProtocolVersion { expected: u32, actual: u32 },
    #[error("plugin is missing required interface {interface}")]
    MissingInterface { interface: String },
    #[error("plugin protocol error: {0}")]
    Protocol(String),
    #[error("plugin transport error: {0}")]
    Transport(String),
    #[error("plugin call {method} timed out after {timeout:?}")]
    Timeout { method: String, timeout: Duration },
    #[error("plugin call {method} failed with JSON-RPC error {code}: {message}")]
    Remote {
        method: String,
        code: i64,
        message: String,
        data: Option<Box<Value>>,
    },
    #[error("plugin exited unexpectedly with code {code:?}")]
    Exited { code: Option<i32> },
    #[error("plugin reported unhealthy status: {0}")]
    Unhealthy(String),
    #[error("plugin rejected graceful shutdown")]
    ShutdownRejected,
    #[error("plugin I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("plugin JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Operator-supplied desired state for one plugin process.
#[derive(Clone)]
pub struct PluginSpec {
    id: String,
    executable: PathBuf,
    args: Vec<OsString>,
    config: Value,
    required_interfaces: Vec<PluginInterface>,
    initialize_timeout: Duration,
    request_timeout: Duration,
    shutdown_timeout: Duration,
}

impl PluginSpec {
    /// Creates a specification with bounded default lifecycle timeouts.
    #[must_use]
    pub fn new(id: impl Into<String>, executable: impl Into<PathBuf>) -> Self {
        Self {
            id: id.into(),
            executable: executable.into(),
            args: Vec::new(),
            config: json!({}),
            required_interfaces: Vec::new(),
            initialize_timeout: DEFAULT_INITIALIZE_TIMEOUT,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }

    /// Adds one literal process argument. No shell parsing is performed.
    #[must_use]
    pub fn with_arg(mut self, argument: impl Into<OsString>) -> Self {
        self.args.push(argument.into());
        self
    }

    /// Supplies opaque initialization configuration.
    #[must_use]
    pub fn with_config(mut self, config: Value) -> Self {
        self.config = config;
        self
    }

    /// Requires one exact operational interface before startup can succeed.
    #[must_use]
    pub fn require_interface(mut self, interface: PluginInterface) -> Self {
        self.required_interfaces.push(interface);
        self
    }

    /// Overrides the initialization deadline.
    #[must_use]
    pub const fn with_initialize_timeout(mut self, timeout: Duration) -> Self {
        self.initialize_timeout = timeout;
        self
    }

    /// Overrides ordinary lifecycle request deadlines.
    #[must_use]
    pub const fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Overrides the deadline for process exit after shutdown is accepted.
    #[must_use]
    pub const fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    fn validate(&self) -> Result<(), PluginError> {
        validate_identifier("plugin", &self.id).map_err(PluginError::InvalidSpec)?;
        if !self.executable.is_absolute() {
            return Err(PluginError::InvalidSpec(
                "plugin executable must be an absolute path".to_owned(),
            ));
        }
        if !self.executable.is_file() {
            return Err(PluginError::InvalidSpec(format!(
                "plugin executable does not exist: {}",
                self.executable.display()
            )));
        }
        if self.initialize_timeout.is_zero()
            || self.request_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
        {
            return Err(PluginError::InvalidSpec(
                "plugin lifecycle timeouts must be greater than zero".to_owned(),
            ));
        }
        for interface in &self.required_interfaces {
            interface.validate().map_err(PluginError::InvalidSpec)?;
        }
        Ok(())
    }
}

impl fmt::Debug for PluginSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PluginSpec")
            .field("id", &self.id)
            .field("executable", &self.executable)
            .field("args_count", &self.args.len())
            .field("config", &"[REDACTED]")
            .field("required_interfaces", &self.required_interfaces)
            .field("initialize_timeout", &self.initialize_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("shutdown_timeout", &self.shutdown_timeout)
            .finish()
    }
}

/// Evidence for a plugin process exit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginExit {
    pub success: bool,
    pub code: Option<i32>,
}

/// Result of a requested plugin shutdown.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShutdownOutcome {
    Graceful(PluginExit),
    Forced(PluginExit),
}

/// One initialized and healthy plugin child process.
pub struct PluginProcess {
    manifest: PluginManifest,
    child: Child,
    process_group: Option<i32>,
    rpc: RpcPeer,
    request_timeout: Duration,
    shutdown_timeout: Duration,
}

impl PluginProcess {
    /// Launches, initializes, validates, and health-checks a plugin.
    ///
    /// The child receives an empty environment, piped protocol stdio, and no
    /// fleetd credentials. A failed startup always terminates the child.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid specification, process failure, timeout,
    /// protocol violation, identity mismatch, or missing interface.
    pub async fn start(spec: PluginSpec) -> Result<Self, PluginError> {
        spec.validate()?;
        let mut command = Command::new(&spec.executable);
        command
            .args(&spec.args)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn()?;
        #[cfg(unix)]
        let process_group = child.id().and_then(|id| i32::try_from(id).ok());
        #[cfg(not(unix))]
        let process_group = None;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| PluginError::Transport("plugin stdin was not captured".to_owned()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| PluginError::Transport("plugin stdout was not captured".to_owned()))?;
        let rpc = RpcPeer::new(stdout, stdin);
        let result = initialize(&rpc, &spec).await;
        let manifest = match result {
            Ok(manifest) => manifest,
            Err(error) => {
                let _unused = terminate(&mut child, process_group).await;
                return Err(error);
            }
        };
        Ok(Self {
            manifest,
            child,
            process_group,
            rpc,
            request_timeout: spec.request_timeout,
            shutdown_timeout: spec.shutdown_timeout,
        })
    }

    /// Returns the negotiated manifest.
    #[must_use]
    pub const fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    /// Returns the operating-system process ID while available.
    #[must_use]
    pub fn process_id(&self) -> Option<u32> {
        self.child.id()
    }

    /// Performs an active readiness check.
    ///
    /// # Errors
    ///
    /// Returns an error when the process exited, times out, violates the
    /// protocol, or reports a status other than `ok`.
    pub async fn health(&mut self) -> Result<(), PluginError> {
        if let Some(status) = self.child.try_wait()? {
            kill_plugin_group(self.process_group);
            return Err(PluginError::Exited {
                code: status.code(),
            });
        }
        check_health(&self.rpc, self.request_timeout).await
    }

    /// Returns one queued plugin notification without blocking.
    pub fn try_notification(&mut self) -> Option<PluginNotification> {
        self.rpc.try_notification()
    }

    /// Converts this process into the typed experimental ACP harness client.
    ///
    /// # Errors
    ///
    /// Returns an error when the process did not negotiate `harness.acp` v1.
    pub fn into_harness_acp(self) -> Result<super::HarnessAcpClient, PluginError> {
        super::HarnessAcpClient::new(self)
    }

    pub(crate) async fn protocol_call<P, R>(
        &self,
        method: &str,
        params: &P,
    ) -> Result<R, PluginError>
    where
        P: serde::Serialize + ?Sized,
        R: serde::de::DeserializeOwned,
    {
        self.rpc.call(method, params, self.request_timeout).await
    }

    pub(crate) async fn next_notification(&mut self) -> Result<PluginNotification, PluginError> {
        self.rpc.next_notification().await
    }

    /// Waits for an unsolicited process exit and returns its evidence.
    ///
    /// # Errors
    ///
    /// Returns an error if waiting on the child process fails.
    pub async fn wait_for_exit(&mut self) -> Result<PluginExit, PluginError> {
        let status = self.child.wait().await?;
        kill_plugin_group(self.process_group);
        Ok(exit_evidence(status))
    }

    /// Requests graceful shutdown, then forcibly kills an overrun process.
    ///
    /// # Errors
    ///
    /// Returns an error when the shutdown request fails or is rejected. The
    /// process is still terminated before the error is returned.
    pub async fn shutdown(mut self) -> Result<ShutdownOutcome, PluginError> {
        let result: Result<ShutdownResult, PluginError> = self
            .rpc
            .call("fleetd.shutdown", &json!({}), self.request_timeout)
            .await;
        match result {
            Ok(result) if result.accepted => {}
            Ok(_) => {
                let _unused = terminate(&mut self.child, self.process_group).await;
                return Err(PluginError::ShutdownRejected);
            }
            Err(error) => {
                let _unused = terminate(&mut self.child, self.process_group).await;
                return Err(error);
            }
        }
        if let Ok(status) = tokio::time::timeout(self.shutdown_timeout, self.child.wait()).await {
            let status = status?;
            kill_plugin_group(self.process_group);
            Ok(ShutdownOutcome::Graceful(exit_evidence(status)))
        } else {
            let status = terminate(&mut self.child, self.process_group).await?;
            Ok(ShutdownOutcome::Forced(exit_evidence(status)))
        }
    }
}

impl Drop for PluginProcess {
    fn drop(&mut self) {
        let child_is_running = self.child.try_wait().ok().flatten().is_none();
        kill_plugin_group(self.process_group);
        if child_is_running {
            let _unused = self.child.start_kill();
        }
    }
}

async fn initialize(rpc: &RpcPeer, spec: &PluginSpec) -> Result<PluginManifest, PluginError> {
    let instance_id = Uuid::new_v4().to_string();
    let params = InitializeParams {
        protocol_version: LIFECYCLE_PROTOCOL_VERSION,
        instance_id: &instance_id,
        host_version: env!("CARGO_PKG_VERSION"),
        config: &spec.config,
    };
    let manifest: PluginManifest = rpc
        .call("fleetd.initialize", &params, spec.initialize_timeout)
        .await?;
    crate::protocol::negotiate(&manifest, &spec.id, &spec.required_interfaces)?;
    check_health(rpc, spec.request_timeout).await?;
    Ok(manifest)
}

async fn check_health(rpc: &RpcPeer, timeout: Duration) -> Result<(), PluginError> {
    let result: HealthResult = rpc.call("fleetd.health", &json!({}), timeout).await?;
    if result.status != "ok" {
        return Err(PluginError::Unhealthy(result.status));
    }
    Ok(())
}

async fn terminate(
    child: &mut Child,
    process_group: Option<i32>,
) -> Result<std::process::ExitStatus, std::io::Error> {
    if let Some(status) = child.try_wait()? {
        kill_plugin_group(process_group);
        return Ok(status);
    }
    kill_plugin_group(process_group);
    let _unused = child.start_kill();
    child.wait().await
}

fn kill_plugin_group(process_group: Option<i32>) {
    #[cfg(unix)]
    if let Some(process_group) = process_group {
        let _unused = nix::sys::signal::killpg(
            nix::unistd::Pid::from_raw(process_group),
            nix::sys::signal::Signal::SIGKILL,
        );
    }
    #[cfg(not(unix))]
    let _unused = process_group;
}

fn exit_evidence(status: std::process::ExitStatus) -> PluginExit {
    PluginExit {
        success: status.success(),
        code: status.code(),
    }
}
