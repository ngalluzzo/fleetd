//! `fleetd invocation` — the write-ahead fence, driven by hand.

use std::error::Error;

use clap::Subcommand;

use fleetd::model::{ArmInvocation, ClaimDeliveries, CompleteInvocation};
use serde_json::json;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{ApiClient, parse_json, print_response};

#[derive(Subcommand)]
pub(super) enum InvocationCommand {
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

pub(super) async fn invocation_command(
    api: &ApiClient,
    command: InvocationCommand,
) -> MainResult<()> {
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
