//! `fleetd agent` — registering identities and rotating their credentials.

use std::{error::Error, path::PathBuf};

use clap::Subcommand;

use fleetd::model::{CreateAgent, IssuedCredential, RegisteredAgent};

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{ApiClient, parse_json, print_credential, print_registration, print_response};

#[derive(Subcommand)]
pub(super) enum AgentCommand {
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

pub(super) async fn agent_command(api: &ApiClient, command: AgentCommand) -> MainResult<()> {
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
