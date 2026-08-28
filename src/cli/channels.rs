//! `fleetd channel` — creating channels and adding members.

use std::error::Error;

use clap::Subcommand;

use fleetd::model::{AddMember, CreateChannel, MembershipDeliveryMode};

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{ApiClient, parse_json, print_response};

#[derive(Subcommand)]
pub(super) enum ChannelCommand {
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

pub(super) async fn channel_command(api: &ApiClient, command: ChannelCommand) -> MainResult<()> {
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
