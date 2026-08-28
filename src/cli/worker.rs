//! `fleetd worker` — desired state for one seat, and running it.

use std::{error::Error, fs, path::PathBuf};

use clap::{Args, Subcommand};
use fleetd_otlp::config::EgressRequest;

use fleetd::{
    execution::worker::{ContinuousHarnessWorker, ContinuousWorkerConfig, EnvelopeTurnAdapter},
    plugin::{PluginSpec, ToolBudget, TurnPolicy, harness_acp_interface},
    store::Store,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{print_json, shutdown_signal};

#[derive(Subcommand)]
pub(super) enum WorkerCommand {
    /// Continuously reserve and execute one agent's inbox.
    Run(WorkerRunArgs),
}

#[derive(Args)]
pub(super) struct WorkerRunArgs {
    /// Override the database named by the fleet configuration.
    #[arg(long, env = "FLEETD_DB")]
    db: Option<PathBuf>,
    /// JSON desired-state file for the worker and harness plugin.
    #[arg(long)]
    config: PathBuf,
    /// Stop after one completed or conservatively blocked turn.
    #[arg(long)]
    once: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerFileConfig {
    schema_version: u32,
    pub(super) agent_id: String,
    working_directory: PathBuf,
    #[serde(default)]
    additional_directories: Vec<PathBuf>,
    #[serde(default)]
    mcp_grants: Vec<String>,
    #[serde(default)]
    compatibility_digest: Option<String>,
    pub(super) plugin: WorkerPluginConfig,
    adapter: WorkerAdapterConfig,
    #[serde(default = "default_result_kind")]
    result_kind: String,
    #[serde(default = "default_worker_lease_ms")]
    lease_duration_ms: u64,
    #[serde(default = "default_poll_interval_ms")]
    poll_interval_ms: u64,
    #[serde(default = "default_restart_backoff_ms")]
    restart_backoff_ms: u64,
    #[serde(default = "default_pre_arm_retry_delay_ms")]
    pre_arm_retry_delay_ms: u64,
    #[serde(default)]
    turn: WorkerTurnConfig,
    /// Optional lossy trajectory egress. Absent means no exporter and no queue.
    #[serde(default)]
    egress: Option<WorkerEgressConfig>,
}

/// One seat's trajectory egress, exactly as written.
///
/// The rules live in `fleetd-otlp`, which is the mechanism that has to honour
/// them; this is only the shape the file is allowed to have.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerEgressConfig {
    schema_version: u32,
    kind: String,
    endpoint: String,
    #[serde(default)]
    headers_file: Option<String>,
    #[serde(default)]
    content: Option<String>,
    #[serde(default)]
    classifications: Option<Vec<String>>,
    #[serde(default)]
    resource_attributes: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    max_attribute_bytes: Option<usize>,
    #[serde(default)]
    queue_capacity: Option<usize>,
    #[serde(default)]
    export_timeout_ms: Option<u64>,
    #[serde(default)]
    shutdown_flush_ms: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum WorkerAdapterConfig {
    Envelope { inbound: InboundAcceptanceConfig },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct InboundAcceptanceConfig {
    schema_version: u32,
    message_kinds: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerPluginConfig {
    id: String,
    executable: PathBuf,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default = "empty_json_object")]
    config: Value,
    #[serde(default = "default_initialize_timeout_ms")]
    initialize_timeout_ms: u64,
    #[serde(default = "default_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_shutdown_timeout_ms")]
    shutdown_timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkerTurnConfig {
    #[serde(default = "default_idle_timeout_ms")]
    idle_timeout_ms: u64,
    #[serde(default = "default_wall_timeout_ms")]
    wall_timeout_ms: u64,
    #[serde(default = "default_cancel_drain_timeout_ms")]
    cancel_drain_timeout_ms: u64,
    #[serde(default = "default_captured_output_bytes")]
    max_captured_output_bytes: usize,
    #[serde(default = "default_tool_budget")]
    tool_budget: u64,
}

impl Default for WorkerTurnConfig {
    fn default() -> Self {
        Self {
            idle_timeout_ms: default_idle_timeout_ms(),
            wall_timeout_ms: default_wall_timeout_ms(),
            cancel_drain_timeout_ms: default_cancel_drain_timeout_ms(),
            max_captured_output_bytes: default_captured_output_bytes(),
            tool_budget: default_tool_budget(),
        }
    }
}

/// Reads and validates one worker desired-state file.
///
/// Shared by every command that takes one, so the schema version is refused in
/// exactly one place rather than once per caller.
pub(super) fn load_worker_config(path: &std::path::Path) -> MainResult<WorkerFileConfig> {
    let raw = fs::read(path)?;
    let value: Value = serde_json::from_slice(&raw).map_err(|error| {
        format!(
            "worker configuration {} is invalid: {error}",
            path.display()
        )
    })?;
    let schema_version = value.get("schema_version").and_then(Value::as_u64);
    if schema_version != Some(2) {
        let observed =
            schema_version.map_or_else(|| "missing".to_owned(), |value| value.to_string());
        return Err(format!(
            "unsupported worker configuration schema version {observed}; expected 2 with explicit inbound acceptance"
        )
        .into());
    }
    let desired: WorkerFileConfig = serde_json::from_value(value).map_err(|error| {
        format!(
            "worker configuration {} is invalid: {error}",
            path.display()
        )
    })?;
    debug_assert_eq!(desired.schema_version, 2);
    Ok(desired)
}

pub(super) async fn worker_command(
    command: WorkerCommand,
    fleet: &fleetd_fleet::ResolvedFleet,
) -> MainResult<()> {
    match command {
        WorkerCommand::Run(args) => run_worker(args, fleet).await,
    }
}

pub(super) async fn run_worker(
    args: WorkerRunArgs,
    fleet: &fleetd_fleet::ResolvedFleet,
) -> MainResult<()> {
    // The worker writes to the same authoritative database the daemon serves,
    // so it reads the same fleet configuration rather than defaulting to a
    // relative path in whatever directory it was launched from.
    let db = args.db.clone().unwrap_or_else(|| fleet.database.clone());
    let desired = load_worker_config(&args.config)?;
    if let Some(parent) = db.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let store = Store::open_with_message_commit_hints(&db).await?;
    let adapter = desired.turn_adapter()?;
    // Egress is a transport, so the binary provisions it for the same reason it
    // provisions the MCP endpoint below: the worker is handed a sink and never
    // learns that an exporter is a thing that can be started. Validation
    // happens here, before a plugin process exists, because a malformed block
    // is a configuration mistake rather than a runtime condition.
    let egress = desired.egress_request();
    let mut config = desired.into_runtime_config();
    if let Some(request) = egress {
        let validated = request
            .validate()
            .map_err(|error| format!("worker configuration egress block is invalid: {error}"))?;
        tracing::info!(
            endpoint = %validated.endpoint,
            content = validated.content.as_str(),
            "trajectory egress enabled for this seat"
        );
        config.trajectory_sink = Some(std::sync::Arc::new(
            fleetd_otlp::sink::TrajectoryEgress::start(validated)
                .map_err(|error| format!("trajectory egress could not start: {error}"))?,
        ));
    }
    // Whether a turn is offered an MCP endpoint is a deployment decision, so the
    // binary makes it. The worker is handed the result and never learns that an
    // endpoint is a thing that can be started.
    let broker = if config
        .mcp_grants
        .iter()
        .any(|grant| grant == fleetd::execution::message_grant::PUBLISH_DURABLE_MESSAGE_GRANT)
    {
        let broker = fleetd::mcp::MessageGrantBroker::start(store.clone()).await?;
        config.turn_grants.push(broker.turn_grant());
        Some(broker)
    } else {
        None
    };
    let worker = ContinuousHarnessWorker::new(&store, config, adapter)?;
    let cancellation = CancellationToken::new();
    let signal = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        shutdown_signal().await;
        signal.cancel();
    });
    tracing::info!(database = %db.display(), "continuous worker ready");
    let run = if args.once {
        worker.run_until(cancellation, Some(1)).await
    } else {
        worker.run(cancellation).await
    };
    signal_task.abort();
    if let Some(broker) = broker {
        broker.shutdown().await;
    }
    let report = run?;
    print_json(&report)
}

impl WorkerFileConfig {
    fn turn_adapter(&self) -> MainResult<EnvelopeTurnAdapter> {
        match &self.adapter {
            WorkerAdapterConfig::Envelope { inbound } => {
                if inbound.schema_version != 1 {
                    return Err(format!(
                        "unsupported inbound acceptance schema version {}; expected 1",
                        inbound.schema_version
                    )
                    .into());
                }
                EnvelopeTurnAdapter::new(self.result_kind.clone(), inbound.message_kinds.clone())
                    .map_err(|error| {
                        format!("worker adapter configuration is invalid: {error}").into()
                    })
            }
        }
    }

    /// Restates the egress block as the request `fleetd-otlp` validates.
    ///
    /// Read before `into_runtime_config` consumes the file, and separate from it
    /// because constructing the sink needs a runtime and can fail.
    fn egress_request(&self) -> Option<EgressRequest> {
        let egress = self.egress.as_ref()?;
        Some(EgressRequest {
            schema_version: egress.schema_version,
            kind: egress.kind.clone(),
            endpoint: egress.endpoint.clone(),
            headers_file: egress.headers_file.clone(),
            content: egress.content.clone(),
            classifications: egress.classifications.clone(),
            resource_attributes: egress.resource_attributes.clone(),
            max_attribute_bytes: egress.max_attribute_bytes,
            queue_capacity: egress.queue_capacity,
            export_timeout_ms: egress.export_timeout_ms,
            shutdown_flush_ms: egress.shutdown_flush_ms,
            agent_id: self.agent_id.clone(),
        })
    }

    fn into_runtime_config(self) -> ContinuousWorkerConfig {
        ContinuousWorkerConfig {
            agent_id: self.agent_id,
            plugin: self.plugin.into_spec(),
            working_directory: self.working_directory,
            additional_directories: self.additional_directories,
            mcp_grants: self.mcp_grants,
            turn_grants: Vec::new(),
            compatibility_digest: self.compatibility_digest,
            lease_duration: std::time::Duration::from_millis(self.lease_duration_ms),
            poll_interval: std::time::Duration::from_millis(self.poll_interval_ms),
            restart_backoff: std::time::Duration::from_millis(self.restart_backoff_ms),
            pre_arm_retry_delay: std::time::Duration::from_millis(self.pre_arm_retry_delay_ms),
            turn_policy: TurnPolicy {
                idle_timeout_ms: self.turn.idle_timeout_ms,
                wall_timeout_ms: self.turn.wall_timeout_ms,
                cancel_drain_timeout_ms: self.turn.cancel_drain_timeout_ms,
                max_captured_output_bytes: self.turn.max_captured_output_bytes,
                permission_policy: "controller".to_owned(),
                tool_budget: ToolBudget {
                    limit: self.turn.tool_budget,
                    required_enforcement: "observe_then_cancel".to_owned(),
                },
                token_budget: None,
            },
            trajectory_sink: None,
        }
    }
}

impl WorkerPluginConfig {
    pub(super) fn into_spec(self) -> PluginSpec {
        let mut spec = PluginSpec::new(self.id, self.executable)
            .with_config(self.config)
            .require_interface(harness_acp_interface())
            .with_initialize_timeout(std::time::Duration::from_millis(self.initialize_timeout_ms))
            .with_request_timeout(std::time::Duration::from_millis(self.request_timeout_ms))
            .with_shutdown_timeout(std::time::Duration::from_millis(self.shutdown_timeout_ms));
        for argument in self.args {
            spec = spec.with_arg(argument);
        }
        spec
    }
}

pub(super) const fn default_worker_lease_ms() -> u64 {
    900_000
}

pub(super) const fn default_poll_interval_ms() -> u64 {
    1_000
}

pub(super) const fn default_restart_backoff_ms() -> u64 {
    5_000
}

pub(super) const fn default_pre_arm_retry_delay_ms() -> u64 {
    5_000
}

pub(super) const fn default_initialize_timeout_ms() -> u64 {
    10_000
}

pub(super) const fn default_request_timeout_ms() -> u64 {
    30_000
}

pub(super) const fn default_shutdown_timeout_ms() -> u64 {
    5_000
}

pub(super) const fn default_idle_timeout_ms() -> u64 {
    120_000
}

pub(super) const fn default_wall_timeout_ms() -> u64 {
    600_000
}

pub(super) const fn default_cancel_drain_timeout_ms() -> u64 {
    15_000
}

pub(super) const fn default_captured_output_bytes() -> usize {
    512 * 1_024
}

pub(super) const fn default_tool_budget() -> u64 {
    64
}

pub(super) fn default_result_kind() -> String {
    "work.result/v1".to_owned()
}

pub(super) fn empty_json_object() -> Value {
    json!({})
}

#[cfg(test)]
mod tests {
    use fleetd::execution::worker::TurnAdapter;
    use serde_json::json;

    use super::WorkerFileConfig;

    /// A committed example is a template an operator copies, so it has to load.
    #[test]
    fn the_egress_example_parses_and_validates() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples/worker.acp.egress.example.json");
        let text = std::fs::read_to_string(&path).expect("read the egress example");
        let desired: WorkerFileConfig =
            serde_json::from_str(&text).expect("the egress example is a valid worker file");
        let validated = desired
            .egress_request()
            .expect("the example configures egress")
            .validate()
            .expect("the example satisfies the egress contract");
        assert_eq!(validated.content.as_str(), "metadata");
        assert!(validated.endpoint.starts_with("http://127.0.0.1:"));
    }

    #[test]
    fn a_seat_without_an_egress_block_provisions_no_sink() {
        let desired: WorkerFileConfig = serde_json::from_value(json!({
            "schema_version": 2,
            "agent_id": "agent-id",
            "working_directory": env!("CARGO_MANIFEST_DIR"),
            "adapter": {
                "kind": "envelope",
                "inbound": {"schema_version": 1, "message_kinds": ["work.request/v1"]}
            },
            "plugin": {"id": "mock.harness", "executable": "/usr/bin/python3"}
        }))
        .expect("parse without egress");
        assert!(desired.egress_request().is_none());
        assert!(desired.into_runtime_config().trajectory_sink.is_none());
    }

    /// An unknown field inside the egress block fails the seat rather than
    /// starting one that exports something other than what was written.
    #[test]
    fn an_unknown_egress_field_is_refused() {
        let value = json!({
            "schema_version": 2,
            "agent_id": "agent-id",
            "working_directory": env!("CARGO_MANIFEST_DIR"),
            "adapter": {
                "kind": "envelope",
                "inbound": {"schema_version": 1, "message_kinds": ["work.request/v1"]}
            },
            "plugin": {"id": "mock.harness", "executable": "/usr/bin/python3"},
            "egress": {
                "schema_version": 1,
                "kind": "otlp_http",
                "endpoint": "http://127.0.0.1:4318/v1/traces",
                "sampling": 0.5
            }
        });
        assert!(serde_json::from_value::<WorkerFileConfig>(value).is_err());
    }

    #[test]
    fn worker_config_defaults_are_bounded_and_inbound_acceptance_is_exact() {
        let value = json!({
            "schema_version": 2,
            "agent_id": "agent-id",
            "working_directory": env!("CARGO_MANIFEST_DIR"),
            "adapter": {
                "kind": "envelope",
                "inbound": {
                    "schema_version": 1,
                    "message_kinds": ["work.request/v1"]
                }
            },
            "plugin": {
                "id": "mock.harness",
                "executable": "/usr/bin/python3"
            }
        });
        let desired: WorkerFileConfig =
            serde_json::from_value(value.clone()).expect("parse minimal desired state");
        let adapter = desired.turn_adapter().expect("configure exact acceptance");
        assert_eq!(
            adapter.inbound_acceptance().message_kinds(),
            &std::collections::BTreeSet::from(["work.request/v1".to_owned()])
        );
        let runtime = desired.into_runtime_config();
        assert_eq!(runtime.lease_duration.as_millis(), 900_000);
        assert_eq!(runtime.turn_policy.wall_timeout_ms, 600_000);
        assert_eq!(runtime.turn_policy.tool_budget.limit, 64);

        let mut invalid = value;
        invalid["surprise"] = json!(true);
        assert!(serde_json::from_value::<WorkerFileConfig>(invalid).is_err());

        let duplicate = json!({
            "schema_version": 2,
            "agent_id": "agent-id",
            "working_directory": env!("CARGO_MANIFEST_DIR"),
            "adapter": {
                "kind": "envelope",
                "inbound": {
                    "schema_version": 1,
                    "message_kinds": ["work.request/v1", "work.request/v1"]
                }
            },
            "plugin": {
                "id": "mock.harness",
                "executable": "/usr/bin/python3"
            }
        });
        let duplicate: WorkerFileConfig =
            serde_json::from_value(duplicate).expect("parse duplicate acceptance shape");
        assert!(duplicate.turn_adapter().is_err());
    }
}
