//! `fleetd trigger` — registering what may create work, and reporting a firing.
//!
//! Every subcommand but `fire` is an operator's. `fire` is the trigger's own,
//! and needs the trigger's credential rather than the operator's: point the
//! global `--token-file` at the file `trigger add --credential-file` wrote.
//! Having it here at all is so an operator can prove a registration works
//! before wiring a scheduler to it.

use std::{error::Error, path::PathBuf};

use clap::Subcommand;

use fleetd::{
    model::IssuedCredential,
    trigger::{RegisterTrigger, RegisteredTrigger, RetireTrigger, TriggerOccurrence},
};

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{ApiClient, parse_json, print_credential, print_registration, print_response};

#[derive(Subcommand)]
pub(super) enum TriggerCommand {
    /// Register something that creates work on its own.
    Add {
        #[arg(long)]
        name: String,
        /// The one channel this trigger may reach.
        #[arg(long)]
        channel: String,
        /// The existing agent its messages are attributed to.
        #[arg(long)]
        sender: String,
        /// A message kind it may create. Repeat for each one; the set is fixed
        /// at registration and changing it changes what the trigger is.
        #[arg(long = "kind", required = true)]
        kinds: Vec<String>,
        #[arg(long)]
        credential_file: Option<PathBuf>,
    },
    /// List registrations, with when each last created work.
    List {
        #[arg(long)]
        channel: Option<String>,
    },
    Show {
        #[arg(long)]
        trigger: String,
    },
    /// End a trigger's standing grant and revoke its credentials.
    Retire {
        #[arg(long)]
        trigger: String,
        /// Why it stopped. Recorded, and read by whoever finds it next.
        #[arg(long)]
        reason: String,
    },
    RotateCredential {
        #[arg(long)]
        trigger: String,
        #[arg(long)]
        credential_file: Option<PathBuf>,
    },
    /// Report an occurrence, using the trigger's own credential.
    Fire {
        #[arg(long)]
        trigger: String,
        /// This firing's name. Repeating one is absorbed exactly, so a
        /// scheduler that runs twice creates work once.
        #[arg(long)]
        occurrence: String,
        #[arg(long)]
        recipient: String,
        #[arg(long)]
        kind: String,
        #[arg(long, default_value = "{}")]
        payload: String,
    },
}

pub(super) async fn trigger_command(api: &ApiClient, command: TriggerCommand) -> MainResult<()> {
    match command {
        TriggerCommand::Add {
            name,
            channel,
            sender,
            kinds,
            credential_file,
        } => {
            let registered: RegisteredTrigger = api
                .post("/v1/triggers")
                .json(&RegisterTrigger {
                    name,
                    channel_id: channel,
                    sender_id: sender,
                    accepted_kinds: kinds,
                })
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            print_registration(
                "trigger",
                &registered.trigger,
                &registered.credential,
                credential_file.as_deref(),
            )
        }
        TriggerCommand::List { channel } => {
            let path = channel.map_or_else(
                || "/v1/triggers".to_owned(),
                |channel| format!("/v1/triggers?channel_id={channel}"),
            );
            print_response(api.get(&path).send().await?).await
        }
        TriggerCommand::Show { trigger } => {
            print_response(api.get(&format!("/v1/triggers/{trigger}")).send().await?).await
        }
        TriggerCommand::Retire { trigger, reason } => {
            print_response(
                api.post(&format!("/v1/triggers/{trigger}/retire"))
                    .json(&RetireTrigger { reason })
                    .send()
                    .await?,
            )
            .await
        }
        TriggerCommand::RotateCredential {
            trigger,
            credential_file,
        } => {
            let credential: IssuedCredential = api
                .post(&format!("/v1/triggers/{trigger}/credentials/rotate"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            print_credential(&credential, credential_file.as_deref())
        }
        TriggerCommand::Fire {
            trigger,
            occurrence,
            recipient,
            kind,
            payload,
        } => {
            print_response(
                api.post(&format!("/v1/triggers/{trigger}/occurrences"))
                    .json(&TriggerOccurrence {
                        occurrence_id: occurrence,
                        recipient_id: recipient,
                        kind,
                        payload: parse_json(&payload)?,
                    })
                    .send()
                    .await?,
            )
            .await
        }
    }
}
