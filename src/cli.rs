use std::{
    error::Error,
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use clap::{Args, Parser, Subcommand, ValueEnum};
use fleetd::{
    AckDelivery, AddMember, AppState, ArmInvocation, AuthService, BlockDelivery, BlockResolution,
    ClaimDeliveries, CompleteInvocation, ContinuousHarnessWorker, ContinuousWorkerConfig,
    CreateAgent, CreateChannel, EnvelopeTurnAdapter, IssuedCredential, MessagePage, PluginSpec,
    RegisteredAgent, ResolveDeliveryBlock, RetryDelivery, SendMessage, Store, ToolBudget,
    TurnPolicy, harness_acp_interface, router,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, env = "FLEETD_SERVER", default_value = "http://127.0.0.1:7419")]
    server: String,
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
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, env = "FLEETD_LISTEN", default_value = "127.0.0.1:7419")]
    listen: SocketAddr,
    #[arg(long, env = "FLEETD_DB", default_value = "fleetd.db")]
    db: PathBuf,
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
    #[arg(long, env = "FLEETD_DB", default_value = "fleetd.db")]
    db: PathBuf,
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
    match cli.command {
        Command::Serve(args) => serve(args).await,
        Command::Agent { command } => {
            agent_command(
                &ApiClient::load(&cli.server, cli.token_file.as_deref())?,
                command,
            )
            .await
        }
        Command::Channel { command } => {
            channel_command(
                &ApiClient::load(&cli.server, cli.token_file.as_deref())?,
                command,
            )
            .await
        }
        Command::Message { command } => {
            message_command(
                &ApiClient::load(&cli.server, cli.token_file.as_deref())?,
                command,
            )
            .await
        }
        Command::Inbox { command } => {
            inbox_command(
                &ApiClient::load(&cli.server, cli.token_file.as_deref())?,
                command,
            )
            .await
        }
        Command::Invocation { command } => {
            invocation_command(
                &ApiClient::load(&cli.server, cli.token_file.as_deref())?,
                command,
            )
            .await
        }
        Command::Worker { command } => worker_command(command).await,
    }
}

async fn worker_command(command: WorkerCommand) -> MainResult<()> {
    match command {
        WorkerCommand::Run(args) => run_worker(args).await,
    }
}

async fn run_worker(args: WorkerRunArgs) -> MainResult<()> {
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
    if let Some(parent) = args.db.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let store = Store::open(&args.db).await?;
    let adapter = desired.turn_adapter()?;
    let worker = ContinuousHarnessWorker::new(&store, desired.into_runtime_config(), adapter)?;
    let cancellation = CancellationToken::new();
    let signal = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        shutdown_signal().await;
        signal.cancel();
    });
    tracing::info!(database = %args.db.display(), "continuous worker ready");
    let run = if args.once {
        worker.run_until(cancellation, Some(1)).await
    } else {
        worker.run(cancellation).await
    };
    signal_task.abort();
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

async fn serve(args: ServeArgs) -> MainResult<()> {
    validate_listen_address(args.listen)?;
    if let Some(parent) = args.db.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let store = Store::open(&args.db).await?;
    let token_path = args
        .operator_token_file
        .unwrap_or_else(|| default_operator_token_path(&args.db));
    let bootstrap = AuthService::new(store.clone())
        .ensure_operator_credential(&token_path)
        .await?;
    tracing::info!(
        path = %bootstrap.token_path.display(),
        rotated = bootstrap.credential_rotated,
        "operator credential ready"
    );
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(listen = %args.listen, database = %args.db.display(), "fleetd ready");
    axum::serve(listener, router(AppState::new(store)))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
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
                })
                .send()
                .await?
        }
        ChannelCommand::List => api.get("/v1/channels").send().await?,
        ChannelCommand::AddMember { channel, agent } => {
            api.post(&format!("/v1/channels/{channel}/members"))
                .json(&AddMember { agent_id: agent })
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

fn validate_listen_address(address: SocketAddr) -> MainResult<()> {
    if !address.ip().is_loopback() {
        return Err(
            "fleetd cannot listen beyond loopback until authenticated transport is configured"
                .into(),
        );
    }
    Ok(())
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

fn base_url(server: &str) -> &str {
    server.trim_end_matches('/')
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use fleetd::TurnAdapter;
    use serde_json::json;

    use super::{
        WorkerFileConfig, persist_secret_file, replace_secret_file, validate_listen_address,
    };

    #[test]
    fn loopback_listen_addresses_are_allowed() {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7419);
        assert!(validate_listen_address(address).is_ok());
    }

    #[test]
    fn non_loopback_listen_addresses_are_rejected() {
        let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7419);
        assert!(validate_listen_address(address).is_err());
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
