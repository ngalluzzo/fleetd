use std::{
    error::Error,
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use clap::{Args, Parser, Subcommand, ValueEnum};
use fleetd_fleet::{base_url, validate_listen_address};

use fleetd::{
    auth::AuthService,
    execution::{
        invocation,
        worker::{ContinuousHarnessWorker, ContinuousWorkerConfig, EnvelopeTurnAdapter},
    },
    http::{AppState, router},
    model::{
        AckDelivery, AddMember, ArmInvocation, BlockDelivery, BlockResolution, ClaimDeliveries,
        CompleteInvocation, CreateAgent, CreateChannel, DeliveryState, IssuedCredential,
        MembershipDeliveryMode, MessagePage, RegisteredAgent, ResolveDeliveryBlock, RetryDelivery,
        SendMessage,
    },
    plugin::{PluginSpec, ToolBudget, TurnPolicy, harness_acp_interface},
    store::Store,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Flattens a typed error to its message.
///
/// `main` reports failures through `Debug` on a boxed error, which prints a
/// string error as its text but a typed error as its variant. Every message an
/// operator sees should read the same way.
fn flatten(error: impl std::fmt::Display) -> Box<dyn Error + Send + Sync> {
    error.to_string().into()
}

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Local fleet configuration, as created by `fleetd init`.
    #[arg(
        long = "fleet-config",
        env = "FLEETD_CONFIG",
        global = true,
        default_value = fleetd_fleet::DEFAULT_CONFIG_PATH
    )]
    fleet_config: PathBuf,
    /// Override the server named by the fleet configuration.
    #[arg(long, env = "FLEETD_SERVER", global = true)]
    server: Option<String>,
    #[arg(long, env = "FLEETD_TOKEN_FILE", global = true)]
    token_file: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Serve(ServeArgs),
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    Channel {
        #[command(subcommand)]
        command: ChannelCommand,
    },
    Message {
        #[command(subcommand)]
        command: MessageCommand,
    },
    Inbox {
        #[command(subcommand)]
        command: InboxCommand,
    },
    Invocation {
        #[command(subcommand)]
        command: InvocationCommand,
    },
    /// Run a local harness worker against fleetd's authoritative database.
    Worker {
        #[command(subcommand)]
        command: WorkerCommand,
    },
    /// Report what the fleet is doing now.
    Status(StatusArgs),
    /// Read durable delivery state across every agent.
    ///
    /// Settling one is `inbox retry` or `inbox resolve`, which already exist.
    Deliveries(DeliveriesArgs),
    /// Join one invocation to its session, plugin, and result evidence.
    Trace(TraceArgs),
    /// Create one local fleet: its directory, database, and operator credential.
    Init(InitArgs),
}

#[derive(Args)]
struct InitArgs {
    #[arg(long, default_value = "127.0.0.1:7419")]
    listen: SocketAddr,
}

#[derive(Args)]
struct StatusArgs {
    /// Limit the report to one agent ID.
    #[arg(long)]
    agent: Option<String>,
    /// Bound how many delivery rows the census reads.
    #[arg(long, default_value_t = 500)]
    delivery_limit: u32,
}

#[derive(Args)]
struct TraceArgs {
    /// Stable invocation ID.
    #[arg(long)]
    invocation: String,
}

#[derive(Args)]
struct DeliveriesArgs {
    /// Limit results to one agent ID.
    #[arg(long)]
    agent: Option<String>,
    /// Limit results to one durable delivery state.
    #[arg(long, value_enum)]
    state: Option<DeliveryStateArg>,
    /// Bound the returned read model.
    #[arg(long, default_value_t = 100)]
    limit: u32,
}

/// The delivery states an operator may filter on.
///
/// This mirrors `DeliveryState` for clap's sake only; the wire spelling comes
/// from the codec so the CLI never becomes a second source of the names.
#[derive(Clone, Copy, ValueEnum)]
enum DeliveryStateArg {
    Pending,
    Leased,
    Blocked,
    Acknowledged,
    Dead,
}

impl From<DeliveryStateArg> for DeliveryState {
    fn from(value: DeliveryStateArg) -> Self {
        match value {
            DeliveryStateArg::Pending => Self::Pending,
            DeliveryStateArg::Leased => Self::Leased,
            DeliveryStateArg::Blocked => Self::Blocked,
            DeliveryStateArg::Acknowledged => Self::Acknowledged,
            DeliveryStateArg::Dead => Self::Dead,
        }
    }
}

#[derive(Args)]
struct ServeArgs {
    /// Override the listen address named by the fleet configuration.
    #[arg(long, env = "FLEETD_LISTEN")]
    listen: Option<SocketAddr>,
    /// Override the database named by the fleet configuration.
    #[arg(long, env = "FLEETD_DB")]
    db: Option<PathBuf>,
    #[arg(long)]
    operator_token_file: Option<PathBuf>,
}

#[derive(Subcommand)]
enum AgentCommand {
    Add {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "{}")]
        metadata: String,
        #[arg(long)]
        credential_file: Option<PathBuf>,
    },
    List,
    RotateCredential {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        credential_file: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum ChannelCommand {
    Create {
        #[arg(long)]
        name: String,
        #[arg(long = "member")]
        member_ids: Vec<String>,
        #[arg(long, default_value = "{}")]
        metadata: String,
    },
    List,
    AddMember {
        #[arg(long)]
        channel: String,
        #[arg(long)]
        agent: String,
    },
}

#[derive(Subcommand)]
enum MessageCommand {
    Send {
        #[arg(long)]
        channel: String,
        #[arg(long)]
        idempotency_key: Option<String>,
        #[arg(long = "to")]
        recipient: Option<String>,
        #[arg(long, default_value = "text")]
        kind: String,
        #[arg(long, conflicts_with = "payload")]
        text: Option<String>,
        #[arg(long)]
        payload: Option<String>,
        #[arg(long)]
        correlation: Option<String>,
        #[arg(long)]
        causation: Option<String>,
    },
    List {
        #[arg(long)]
        channel: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    Watch {
        #[arg(long)]
        channel: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
}

#[derive(Subcommand)]
enum InboxCommand {
    Claim {
        #[arg(long)]
        agent: String,
        #[arg(long, default_value_t = 1)]
        limit: u32,
        #[arg(long, default_value_t = 300_000)]
        lease_ms: u64,
    },
    Ack {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        lease: String,
    },
    Retry {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        lease: String,
        #[arg(long, default_value_t = 0)]
        retry_after_ms: u64,
        #[arg(long)]
        error: Option<String>,
    },
    Block {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        message: String,
        #[arg(long)]
        lease: String,
        #[arg(long)]
        reason: String,
    },
    Blocked {
        #[arg(long)]
        agent: Option<String>,
    },
    Resolve {
        #[arg(long)]
        block: i64,
        #[arg(long, value_enum)]
        resolution: ResolutionArg,
        #[arg(long, default_value_t = 0)]
        retry_after_ms: u64,
        #[arg(long)]
        note: Option<String>,
    },
}

#[derive(Subcommand)]
enum InvocationCommand {
    Reserve {
        #[arg(long)]
        agent: String,
        #[arg(long, default_value_t = 1)]
        limit: u32,
        #[arg(long, default_value_t = 300_000)]
        lease_ms: u64,
    },
    Arm {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        invocation: String,
        #[arg(long)]
        lease: String,
        #[arg(long)]
        fence: String,
    },
    Complete {
        #[arg(long)]
        agent: String,
        #[arg(long)]
        invocation: String,
        #[arg(long)]
        lease: String,
        #[arg(long)]
        fence: String,
        #[arg(long, default_value = "text")]
        kind: String,
        #[arg(long, conflicts_with = "payload")]
        text: Option<String>,
        #[arg(long)]
        payload: Option<String>,
    },
    List {
        #[arg(long)]
        agent: Option<String>,
    },
}

#[derive(Subcommand)]
enum WorkerCommand {
    /// Continuously reserve and execute one agent's inbox.
    Run(WorkerRunArgs),
}

#[derive(Args)]
struct WorkerRunArgs {
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
struct WorkerFileConfig {
    schema_version: u32,
    agent_id: String,
    working_directory: PathBuf,
    #[serde(default)]
    additional_directories: Vec<PathBuf>,
    #[serde(default)]
    mcp_grants: Vec<String>,
    #[serde(default)]
    compatibility_digest: Option<String>,
    plugin: WorkerPluginConfig,
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
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum WorkerAdapterConfig {
    Envelope { inbound: InboundAcceptanceConfig },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InboundAcceptanceConfig {
    schema_version: u32,
    message_kinds: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerPluginConfig {
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
struct WorkerTurnConfig {
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

#[derive(Clone, Copy, ValueEnum)]
enum ResolutionArg {
    Requeue,
    Abandon,
}

impl From<ResolutionArg> for BlockResolution {
    fn from(value: ResolutionArg) -> Self {
        match value {
            ResolutionArg::Requeue => Self::Requeue,
            ResolutionArg::Abandon => Self::Abandon,
        }
    }
}

pub async fn run() -> MainResult<()> {
    let cli = Cli::parse();
    // One read of the fleet configuration supplies defaults for every command;
    // an explicit flag still wins over it.
    let fleet = fleetd_fleet::load(&cli.fleet_config).map_err(flatten)?;
    let server = cli.server.clone().unwrap_or_else(|| fleet.server.clone());
    let token_file = cli
        .token_file
        .clone()
        .or_else(|| Some(fleet.operator_token_file.clone()));
    match cli.command {
        Command::Init(args) => init_command(&cli.fleet_config, &args).await,
        Command::Serve(args) => serve(args, &fleet).await,
        Command::Agent { command } => {
            agent_command(&ApiClient::load(&server, token_file.as_deref())?, command).await
        }
        Command::Channel { command } => {
            channel_command(&ApiClient::load(&server, token_file.as_deref())?, command).await
        }
        Command::Message { command } => {
            message_command(&ApiClient::load(&server, token_file.as_deref())?, command).await
        }
        Command::Inbox { command } => {
            inbox_command(&ApiClient::load(&server, token_file.as_deref())?, command).await
        }
        Command::Invocation { command } => {
            invocation_command(&ApiClient::load(&server, token_file.as_deref())?, command).await
        }
        Command::Worker { command } => worker_command(command, &fleet).await,
        Command::Status(args) => {
            status_command(&ApiClient::load(&server, token_file.as_deref())?, args).await
        }
        Command::Deliveries(args) => {
            deliveries_command(&ApiClient::load(&server, token_file.as_deref())?, args).await
        }
        Command::Trace(args) => {
            trace_command(&ApiClient::load(&server, token_file.as_deref())?, args).await
        }
    }
}

/// Prints the fleet health report.
///
/// The report is composed by the daemon in one read, so this is a single
/// request and a print. It deliberately holds no rule about what "current" or
/// "active" means; see `fleetd_execution::health`.
async fn status_command(api: &ApiClient, args: StatusArgs) -> MainResult<()> {
    let mut parameters = vec![format!("delivery_limit={}", args.delivery_limit)];
    if let Some(agent) = args.agent {
        parameters.push(format!("agent={agent}"));
    }
    print_response(
        api.get(&format!("/v1/fleet-health?{}", parameters.join("&")))
            .send()
            .await?,
    )
    .await
}

async fn trace_command(api: &ApiClient, args: TraceArgs) -> MainResult<()> {
    print_response(
        api.get(&format!("/v1/invocations/{}/trace", args.invocation))
            .send()
            .await?,
    )
    .await
}

async fn deliveries_command(api: &ApiClient, args: DeliveriesArgs) -> MainResult<()> {
    let mut parameters = vec![format!("limit={}", args.limit)];
    if let Some(agent) = args.agent {
        parameters.push(format!("agent={agent}"));
    }
    if let Some(state) = args.state {
        parameters.push(format!("state={}", DeliveryState::from(state).as_str()));
    }
    print_response(
        api.get(&format!("/v1/deliveries?{}", parameters.join("&")))
            .send()
            .await?,
    )
    .await
}

async fn worker_command(
    command: WorkerCommand,
    fleet: &fleetd_fleet::ResolvedFleet,
) -> MainResult<()> {
    match command {
        WorkerCommand::Run(args) => run_worker(args, fleet).await,
    }
}

async fn run_worker(args: WorkerRunArgs, fleet: &fleetd_fleet::ResolvedFleet) -> MainResult<()> {
    // The worker writes to the same authoritative database the daemon serves,
    // so it reads the same fleet configuration rather than defaulting to a
    // relative path in whatever directory it was launched from.
    let db = args.db.clone().unwrap_or_else(|| fleet.database.clone());
    let raw = fs::read(&args.config)?;
    let value: Value = serde_json::from_slice(&raw).map_err(|error| {
        format!(
            "worker configuration {} is invalid: {error}",
            args.config.display()
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
            args.config.display()
        )
    })?;
    debug_assert_eq!(desired.schema_version, 2);
    if let Some(parent) = db.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let store = Store::open_with_message_commit_hints(&db).await?;
    let adapter = desired.turn_adapter()?;
    let mut config = desired.into_runtime_config();
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
        }
    }
}

impl WorkerPluginConfig {
    fn into_spec(self) -> PluginSpec {
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

const fn default_worker_lease_ms() -> u64 {
    900_000
}

const fn default_poll_interval_ms() -> u64 {
    1_000
}

const fn default_restart_backoff_ms() -> u64 {
    5_000
}

const fn default_pre_arm_retry_delay_ms() -> u64 {
    5_000
}

const fn default_initialize_timeout_ms() -> u64 {
    10_000
}

const fn default_request_timeout_ms() -> u64 {
    30_000
}

const fn default_shutdown_timeout_ms() -> u64 {
    5_000
}

const fn default_idle_timeout_ms() -> u64 {
    120_000
}

const fn default_wall_timeout_ms() -> u64 {
    600_000
}

const fn default_cancel_drain_timeout_ms() -> u64 {
    15_000
}

const fn default_captured_output_bytes() -> usize {
    512 * 1_024
}

const fn default_tool_budget() -> u64 {
    64
}

fn default_result_kind() -> String {
    "work.result/v1".to_owned()
}

fn empty_json_object() -> Value {
    json!({})
}

struct ApiClient {
    server: String,
    token: String,
    client: reqwest::Client,
}

impl ApiClient {
    fn load(server: &str, token_file: Option<&Path>) -> MainResult<Self> {
        Ok(Self {
            server: base_url(server).to_owned(),
            token: load_client_token(token_file)?,
            client: reqwest::Client::new(),
        })
    }

    fn get(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .get(format!("{}{path}", self.server))
            .bearer_auth(&self.token)
    }

    fn post(&self, path: &str) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}{path}", self.server))
            .bearer_auth(&self.token)
    }
}

/// Prints what `init` created.
///
/// The fleet layout itself is `fleetd_fleet`, so this is one call and a print.
async fn init_command(config_path: &Path, args: &InitArgs) -> MainResult<()> {
    let created = fleetd_fleet::create(config_path, args.listen)
        .await
        .map_err(flatten)?;
    print_json(&json!({
        "status": "initialized",
        "config": created.config_path.display().to_string(),
        "database": created.resolved.database.display().to_string(),
        "operator_token_file": created.operator_token_file.display().to_string(),
        "server": created.resolved.server,
        "next": [
            format!("fleetd --fleet-config {} serve", created.config_path.display()),
            format!("fleetd --fleet-config {} status", created.config_path.display()),
        ]
    }))
}

async fn serve(args: ServeArgs, fleet: &fleetd_fleet::ResolvedFleet) -> MainResult<()> {
    // A flag wins; otherwise the fleet configuration decides, so `fleetd serve`
    // after `fleetd init` needs no repeated arguments.
    let listen = args.listen.unwrap_or(fleet.listen);
    // An explicit `--db` keeps its credential beside itself, as it always has.
    // Only a database chosen by the fleet configuration takes the credential
    // path from that configuration too.
    let (db, configured_token) = match args.db.clone() {
        Some(db) => {
            let derived = default_operator_token_path(&db);
            (db, derived)
        }
        None => (fleet.database.clone(), fleet.operator_token_file.clone()),
    };
    validate_listen_address(listen).map_err(flatten)?;
    if let Some(parent) = db.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let store = Store::open(&db).await?;
    let token_path = args.operator_token_file.clone().unwrap_or(configured_token);
    let bootstrap = AuthService::new(store.clone())
        .ensure_operator_credential(&token_path)
        .await?;
    tracing::info!(
        path = %bootstrap.token_path.display(),
        rotated = bootstrap.credential_rotated,
        "operator credential ready"
    );
    let listener = tokio::net::TcpListener::bind(listen).await?;
    let listen_address = listener.local_addr()?;
    let recovery_store = store.clone();
    let state = AppState::new(store)
        .with_browser_stream_listener(listen_address)?
        .with_external_message_commit_hints(&db)?;
    // An attempt whose worker died leaves a leased delivery and an armed
    // invocation behind. Nothing else reclaims those: a worker only recovers the
    // agent it is running, so an agent with no worker stays stuck. The daemon
    // reconciles them for every agent instead.
    let recovery_cancellation = CancellationToken::new();
    let recovery_task = tokio::spawn(invocation::run_expired_invocation_reaper(
        recovery_store,
        recovery_cancellation.clone(),
        Duration::from_secs(1),
    ));
    tracing::info!(
        listen = %listen_address,
        browser_origin = state.browser_origin().expect("configured browser origin"),
        database = %db.display(),
        "fleetd ready"
    );
    let shutdown = recovery_cancellation.clone();
    let server = axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            shutdown.cancel();
        })
        .await;
    recovery_cancellation.cancel();
    recovery_task.await?;
    server?;
    Ok(())
}

async fn agent_command(api: &ApiClient, command: AgentCommand) -> MainResult<()> {
    match command {
        AgentCommand::Add {
            name,
            metadata,
            credential_file,
        } => {
            let registration: RegisteredAgent = api
                .post("/v1/agents")
                .json(&CreateAgent {
                    name,
                    metadata: parse_json(&metadata)?,
                })
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            print_registration(&registration, credential_file.as_deref())
        }
        AgentCommand::List => print_response(api.get("/v1/agents").send().await?).await,
        AgentCommand::RotateCredential {
            agent,
            credential_file,
        } => {
            let credential: IssuedCredential = api
                .post(&format!("/v1/agents/{agent}/credentials/rotate"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            print_credential(&credential, credential_file.as_deref())
        }
    }
}

async fn channel_command(api: &ApiClient, command: ChannelCommand) -> MainResult<()> {
    let response = match command {
        ChannelCommand::Create {
            name,
            member_ids,
            metadata,
        } => {
            api.post("/v1/channels")
                .json(&CreateChannel {
                    name,
                    metadata: parse_json(&metadata)?,
                    member_ids,
                    members: Vec::new(),
                })
                .send()
                .await?
        }
        ChannelCommand::List => api.get("/v1/channels").send().await?,
        ChannelCommand::AddMember { channel, agent } => {
            api.post(&format!("/v1/channels/{channel}/members"))
                .json(&AddMember {
                    agent_id: agent,
                    delivery_mode: MembershipDeliveryMode::Inbox,
                })
                .send()
                .await?
        }
    };
    print_response(response).await
}

async fn inbox_command(api: &ApiClient, command: InboxCommand) -> MainResult<()> {
    let response = match command {
        InboxCommand::Claim {
            agent,
            limit,
            lease_ms,
        } => {
            api.post(&format!("/v1/agents/{agent}/deliveries/claim"))
                .json(&ClaimDeliveries {
                    limit,
                    lease_duration_ms: lease_ms,
                })
                .send()
                .await?
        }
        InboxCommand::Ack {
            agent,
            message,
            lease,
        } => {
            api.post(&format!("/v1/agents/{agent}/deliveries/{message}/ack"))
                .json(&AckDelivery { lease_token: lease })
                .send()
                .await?
        }
        InboxCommand::Retry {
            agent,
            message,
            lease,
            retry_after_ms,
            error,
        } => {
            api.post(&format!("/v1/agents/{agent}/deliveries/{message}/retry"))
                .json(&RetryDelivery {
                    lease_token: lease,
                    retry_after_ms,
                    error,
                })
                .send()
                .await?
        }
        InboxCommand::Block {
            agent,
            message,
            lease,
            reason,
        } => {
            api.post(&format!("/v1/agents/{agent}/deliveries/{message}/block"))
                .json(&BlockDelivery {
                    lease_token: lease,
                    reason,
                })
                .send()
                .await?
        }
        InboxCommand::Blocked { agent } => {
            let query = agent.map_or_else(String::new, |agent| format!("?agent={agent}"));
            api.get(&format!("/v1/delivery-blocks{query}"))
                .send()
                .await?
        }
        InboxCommand::Resolve {
            block,
            resolution,
            retry_after_ms,
            note,
        } => {
            api.post(&format!("/v1/delivery-blocks/{block}/resolve"))
                .json(&ResolveDeliveryBlock {
                    resolution: resolution.into(),
                    retry_after_ms,
                    note,
                })
                .send()
                .await?
        }
    };
    print_response(response).await
}

async fn message_command(api: &ApiClient, command: MessageCommand) -> MainResult<()> {
    match command {
        MessageCommand::Send {
            channel,
            idempotency_key,
            recipient,
            kind,
            text,
            payload,
            correlation,
            causation,
        } => {
            let payload = match (text, payload) {
                (Some(text), None) => json!({ "text": text }),
                (None, Some(payload)) => parse_json(&payload)?,
                (None, None) => json!({}),
                (Some(_), Some(_)) => {
                    return Err("message text and payload are mutually exclusive".into());
                }
            };
            let response = api
                .post(&format!("/v1/channels/{channel}/messages"))
                .json(&SendMessage {
                    idempotency_key,
                    recipient_id: recipient,
                    kind,
                    payload,
                    correlation_id: correlation,
                    causation_id: causation,
                })
                .send()
                .await?;
            print_response(response).await
        }
        MessageCommand::List {
            channel,
            after,
            limit,
        } => {
            let page: MessagePage = api
                .get(&format!(
                    "/v1/channels/{channel}/messages?after={after}&limit={limit}"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            print_json(&page)
        }
        MessageCommand::Watch { channel, after } => watch(api, &channel, after).await,
    }
}

async fn invocation_command(api: &ApiClient, command: InvocationCommand) -> MainResult<()> {
    let response = match command {
        InvocationCommand::Reserve {
            agent,
            limit,
            lease_ms,
        } => {
            api.post(&format!("/v1/agents/{agent}/invocations/reserve"))
                .json(&ClaimDeliveries {
                    limit,
                    lease_duration_ms: lease_ms,
                })
                .send()
                .await?
        }
        InvocationCommand::Arm {
            agent,
            invocation,
            lease,
            fence,
        } => {
            api.post(&format!("/v1/agents/{agent}/invocations/{invocation}/arm"))
                .json(&ArmInvocation {
                    lease_token: lease,
                    fence_token: fence,
                })
                .send()
                .await?
        }
        InvocationCommand::Complete {
            agent,
            invocation,
            lease,
            fence,
            kind,
            text,
            payload,
        } => {
            let payload = match (text, payload) {
                (Some(text), None) => json!({ "text": text }),
                (None, Some(payload)) => parse_json(&payload)?,
                (None, None) => json!({}),
                (Some(_), Some(_)) => {
                    return Err("invocation result text and payload are mutually exclusive".into());
                }
            };
            api.post(&format!(
                "/v1/agents/{agent}/invocations/{invocation}/complete"
            ))
            .json(&CompleteInvocation {
                lease_token: lease,
                fence_token: fence,
                kind,
                payload,
            })
            .send()
            .await?
        }
        InvocationCommand::List { agent } => {
            let query = agent.map_or_else(String::new, |agent| format!("?agent={agent}"));
            api.get(&format!("/v1/invocations{query}")).send().await?
        }
    };
    print_response(response).await
}

async fn watch(api: &ApiClient, channel: &str, after: i64) -> MainResult<()> {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue};

    let socket_base = if let Some(rest) = api.server.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = api.server.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err("fleetd server URL must start with http:// or https://".into());
    };
    let url = format!("{socket_base}/v1/channels/{channel}/stream?after={after}");
    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", api.token))?,
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
    while let Some(frame) = socket.next().await {
        let frame = frame?;
        if frame.is_text() {
            println!("{}", frame.into_text()?);
        } else if frame.is_close() {
            break;
        }
    }
    Ok(())
}

async fn print_response(response: reqwest::Response) -> MainResult<()> {
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("fleetd returned {status}: {body}").into());
    }
    if !body.is_empty() {
        let value: Value = serde_json::from_str(&body)?;
        print_json(&value)?;
    }
    Ok(())
}

fn load_client_token(token_file: Option<&Path>) -> MainResult<String> {
    match std::env::var("FLEETD_TOKEN") {
        Ok(token) => validate_loaded_token(&token),
        Err(std::env::VarError::NotPresent) => {
            let path = token_file.unwrap_or_else(|| Path::new(".fleetd/operator.token"));
            validate_secret_file(path)?;
            validate_loaded_token(&fs::read_to_string(path)?)
        }
        Err(std::env::VarError::NotUnicode(_)) => Err("FLEETD_TOKEN is not valid Unicode".into()),
    }
}

fn print_registration(
    registration: &RegisteredAgent,
    credential_file: Option<&Path>,
) -> MainResult<()> {
    if let Some(path) = credential_file {
        persist_secret_file(path, &registration.credential.token)?;
        return print_json(&json!({
            "agent": registration.agent,
            "credential": {
                "id": registration.credential.id,
                "created_at_ms": registration.credential.created_at_ms,
                "token_file": path.display().to_string()
            }
        }));
    }
    print_json(&registration)
}

fn print_credential(
    credential: &IssuedCredential,
    credential_file: Option<&Path>,
) -> MainResult<()> {
    if let Some(path) = credential_file {
        replace_secret_file(path, &credential.token)?;
        return print_json(&json!({
            "id": credential.id,
            "created_at_ms": credential.created_at_ms,
            "token_file": path.display().to_string()
        }));
    }
    print_json(&credential)
}

fn persist_secret_file(path: &Path, token: &str) -> MainResult<()> {
    persist_secret_file_with_mode(path, token, false)
}

fn replace_secret_file(path: &Path, token: &str) -> MainResult<()> {
    persist_secret_file_with_mode(path, token, true)
}

fn persist_secret_file_with_mode(path: &Path, token: &str, replace: bool) -> MainResult<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    make_secret_file_private(temporary.path())?;
    writeln!(temporary, "{token}")?;
    temporary.as_file().sync_all()?;
    if replace {
        temporary.persist(path).map_err(|error| {
            format!(
                "could not replace credential file {}: {}",
                path.display(),
                error.error
            )
        })?;
    } else {
        temporary.persist_noclobber(path).map_err(|error| {
            format!(
                "could not persist credential file {}: {}",
                path.display(),
                error.error
            )
        })?;
    }
    Ok(())
}

#[cfg(unix)]
fn make_secret_file_private(path: &Path) -> MainResult<()> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn make_secret_file_private(_path: &Path) -> MainResult<()> {
    Err("secure credential files are not implemented on this platform".into())
}

fn validate_loaded_token(token: &str) -> MainResult<String> {
    let token = token.trim().to_owned();
    if token.is_empty() {
        return Err("fleetd credential is empty".into());
    }
    Ok(token)
}

#[cfg(unix)]
fn validate_secret_file(path: &Path) -> MainResult<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path)?.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(format!(
            "credential file {} must not be readable by group or others",
            path.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_secret_file(_path: &Path) -> MainResult<()> {
    Err("secure credential files are not implemented on this platform".into())
}

fn default_operator_token_path(database: &Path) -> PathBuf {
    database
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("operator.token")
}

async fn shutdown_signal() {
    let _unused = tokio::signal::ctrl_c().await;
}

fn parse_json(value: &str) -> MainResult<Value> {
    Ok(serde_json::from_str(value)?)
}

fn print_json(value: &impl Serialize) -> MainResult<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {

    use fleetd::execution::worker::TurnAdapter;
    use serde_json::json;

    // The loopback rule and its assertions moved to `fleetd-fleet`, which owns
    // the listen address every surface reads.
    use super::{WorkerFileConfig, persist_secret_file, replace_secret_file};

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

    #[test]
    #[cfg(unix)]
    fn credential_files_are_private_and_never_overwritten() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("agent.token");
        persist_secret_file(&path, "first").expect("persist token");
        let mode = std::fs::metadata(&path)
            .expect("token metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0);
        assert!(persist_secret_file(&path, "second").is_err());
        assert_eq!(
            std::fs::read_to_string(path).expect("read token"),
            "first\n"
        );
    }

    #[test]
    #[cfg(unix)]
    fn credential_rotation_atomically_replaces_the_private_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("agent.token");
        persist_secret_file(&path, "first").expect("persist initial token");
        replace_secret_file(&path, "second").expect("replace token");
        let metadata = std::fs::metadata(&path).expect("token metadata");
        assert_eq!(metadata.permissions().mode() & 0o077, 0);
        assert_eq!(
            std::fs::read_to_string(path).expect("read token"),
            "second\n"
        );
    }
}
