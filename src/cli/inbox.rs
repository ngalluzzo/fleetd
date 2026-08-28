//! `fleetd inbox` — leasing work and settling what became ambiguous.

use std::error::Error;

use clap::{Subcommand, ValueEnum};

use fleetd::model::{
    AckDelivery, BlockDelivery, BlockResolution, ClaimDeliveries, ResolveDeliveryBlock,
    RetryDelivery,
};

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{ApiClient, print_response};

#[derive(Subcommand)]
pub(super) enum InboxCommand {
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

#[derive(Clone, Copy, ValueEnum)]
pub(super) enum ResolutionArg {
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

pub(super) async fn inbox_command(api: &ApiClient, command: InboxCommand) -> MainResult<()> {
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
