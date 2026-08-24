use std::{error::Error, net::SocketAddr, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use fleetd::{
    AckDelivery, AddMember, AppState, ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage,
    MessagePage, RetryDelivery, Store, router,
};
use futures_util::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use tracing_subscriber::EnvFilter;

type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, env = "FLEETD_SERVER", default_value = "http://127.0.0.1:7419")]
    server: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the local fleetd daemon.
    Serve(ServeArgs),
    /// Register or inspect agents.
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    /// Create or inspect communication channels.
    Channel {
        #[command(subcommand)]
        command: ChannelCommand,
    },
    /// Send, read, or watch immutable messages.
    Message {
        #[command(subcommand)]
        command: MessageCommand,
    },
    /// Lease and settle durable work addressed to an agent.
    Inbox {
        #[command(subcommand)]
        command: InboxCommand,
    },
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, env = "FLEETD_LISTEN", default_value = "127.0.0.1:7419")]
    listen: SocketAddr,
    #[arg(long, env = "FLEETD_DB", default_value = "fleetd.db")]
    db: PathBuf,
}

#[derive(Subcommand)]
enum AgentCommand {
    Add {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "{}")]
        metadata: String,
    },
    List,
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
        #[arg(long = "from")]
        sender: String,
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
}

#[tokio::main]
async fn main() -> MainResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "fleetd=info".into()))
        .init();
    let cli = Cli::parse();
    match cli.command {
        Command::Serve(args) => serve(args).await,
        Command::Agent { command } => agent_command(&cli.server, command).await,
        Command::Channel { command } => channel_command(&cli.server, command).await,
        Command::Message { command } => message_command(&cli.server, command).await,
        Command::Inbox { command } => inbox_command(&cli.server, command).await,
    }
}

async fn inbox_command(server: &str, command: InboxCommand) -> MainResult<()> {
    let client = reqwest::Client::new();
    let response = match command {
        InboxCommand::Claim {
            agent,
            limit,
            lease_ms,
        } => {
            let url = format!("{}/v1/agents/{agent}/deliveries/claim", base_url(server));
            client
                .post(url)
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
            let url = format!(
                "{}/v1/agents/{agent}/deliveries/{message}/ack",
                base_url(server)
            );
            client
                .post(url)
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
            let url = format!(
                "{}/v1/agents/{agent}/deliveries/{message}/retry",
                base_url(server)
            );
            client
                .post(url)
                .json(&RetryDelivery {
                    lease_token: lease,
                    retry_after_ms,
                    error,
                })
                .send()
                .await?
        }
    };
    print_response(response).await
}

async fn serve(args: ServeArgs) -> MainResult<()> {
    validate_listen_address(args.listen)?;
    if let Some(parent) = args.db.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let store = Store::open(&args.db).await?;
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(listen = %args.listen, database = %args.db.display(), "fleetd ready");
    axum::serve(listener, router(AppState::new(store)))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
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

async fn agent_command(server: &str, command: AgentCommand) -> MainResult<()> {
    let client = reqwest::Client::new();
    let url = format!("{}/v1/agents", base_url(server));
    let response = match command {
        AgentCommand::Add { name, metadata } => {
            let input = CreateAgent {
                name,
                metadata: parse_json(&metadata)?,
            };
            client.post(url).json(&input).send().await?
        }
        AgentCommand::List => client.get(url).send().await?,
    };
    print_response(response).await
}

async fn channel_command(server: &str, command: ChannelCommand) -> MainResult<()> {
    let client = reqwest::Client::new();
    let channels_url = format!("{}/v1/channels", base_url(server));
    let response = match command {
        ChannelCommand::Create {
            name,
            member_ids,
            metadata,
        } => {
            let input = CreateChannel {
                name,
                metadata: parse_json(&metadata)?,
                member_ids,
            };
            client.post(channels_url).json(&input).send().await?
        }
        ChannelCommand::List => client.get(channels_url).send().await?,
        ChannelCommand::AddMember { channel, agent } => {
            let url = format!("{}/v1/channels/{channel}/members", base_url(server));
            client
                .post(url)
                .json(&AddMember { agent_id: agent })
                .send()
                .await?
        }
    };
    print_response(response).await
}

async fn message_command(server: &str, command: MessageCommand) -> MainResult<()> {
    match command {
        MessageCommand::Send {
            channel,
            sender,
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
            let input = CreateMessage {
                sender_id: sender,
                recipient_id: recipient,
                kind,
                payload,
                correlation_id: correlation,
                causation_id: causation,
            };
            let url = format!("{}/v1/channels/{channel}/messages", base_url(server));
            let response = reqwest::Client::new().post(url).json(&input).send().await?;
            print_response(response).await
        }
        MessageCommand::List {
            channel,
            after,
            limit,
        } => {
            let url = format!(
                "{}/v1/channels/{channel}/messages?after={after}&limit={limit}",
                base_url(server)
            );
            let page: MessagePage = reqwest::Client::new()
                .get(url)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            print_json(&page)
        }
        MessageCommand::Watch { channel, after } => watch(server, &channel, after).await,
    }
}

async fn watch(server: &str, channel: &str, after: i64) -> MainResult<()> {
    let socket_base = if let Some(rest) = base_url(server).strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base_url(server).strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err("fleetd server URL must start with http:// or https://".into());
    };
    let url = format!("{socket_base}/v1/channels/{channel}/stream?after={after}");
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await?;
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

    use super::validate_listen_address;

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
}
