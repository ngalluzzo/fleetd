//! The command surface.
//!
//! Six of these modules mirror `HTTP_ROUTE_DOMAINS` one for one — agents,
//! channels, messages, inbox, invocations, operations — because the CLI and
//! HTTP are two ways to ask the same question. The rest are the binary's own:
//! running the daemon, running a seat, and the plumbing they share.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use clap::{Parser, Subcommand};

use serde_json::Value;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

mod agents;
mod channels;
mod client;
mod inbox;
mod init;
mod invocations;
mod messages;
mod operations;
mod secrets;
mod serve;
mod transcript;
mod worker;

use agents::{AgentCommand, agent_command};
use channels::{ChannelCommand, channel_command};
use client::{ApiClient, print_json, print_response};
use inbox::{InboxCommand, inbox_command};
use init::{InitArgs, init_command};
use invocations::{InvocationCommand, invocation_command};
use messages::{MessageCommand, message_command};
use operations::{
    DeliveriesArgs, StatusArgs, TraceArgs, deliveries_command, status_command, trace_command,
};
use secrets::{print_credential, print_registration};
use serve::{ServeArgs, serve};
use transcript::{TranscriptArgs, transcript_command};
use worker::{WorkerCommand, load_worker_config, worker_command};

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
    /// Replay one native harness session's stored conversation.
    ///
    /// Reads through a short-lived second plugin process, so a running worker
    /// keeps its own session untouched.
    Transcript(TranscriptArgs),
    /// Create one local fleet: its directory, database, and operator credential.
    Init(InitArgs),
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
        Command::Transcript(args) => transcript_command(args, &fleet).await,
        Command::Trace(args) => {
            trace_command(&ApiClient::load(&server, token_file.as_deref())?, args).await
        }
    }
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
