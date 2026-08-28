//! `fleetd transcript` — replaying a stored native session.

use std::{error::Error, path::PathBuf};

use clap::Args;

use fleetd::{
    plugin::{
        HarnessAcpNotification, OpenSession, OpenSessionMode, PluginProcess, StartTranscript,
    },
    store::Store,
};
use serde_json::json;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{load_worker_config, print_json};

#[derive(Args)]
pub(super) struct TranscriptArgs {
    /// Worker desired-state file naming the harness plugin for this seat.
    #[arg(long)]
    config: PathBuf,
    /// Native session reference to replay, as `fleetd status` reports it.
    #[arg(long)]
    session: String,
    /// Database path. Defaults to the resolved fleet database.
    #[arg(long)]
    db: Option<PathBuf>,
}

/// Replays one stored native session through a short-lived plugin process.
///
/// This is retrieval, not work. It resumes the session to attach and then loads
/// it to read, which is the split ACP defines: only `session/load` replays. It
/// never closes the session, because a running worker owns that lane and
/// closing would retire its ownership.
pub(super) async fn transcript_command(
    args: TranscriptArgs,
    fleet: &fleetd_fleet::ResolvedFleet,
) -> MainResult<()> {
    let db = args.db.clone().unwrap_or_else(|| fleet.database.clone());
    let desired = load_worker_config(&args.config)?;
    let store = Store::open_with_message_commit_hints(&db).await?;

    // The binding comes from Fleetd's own durable record rather than being
    // invented here, so this can only read a session Fleetd actually owns and
    // an unknown reference fails with somewhere to look.
    let owner = fleetd::execution::session_binding::list_session_bindings(
        &store,
        Some(&desired.agent_id),
    )
    .await?
    .into_iter()
    .find(|binding| binding.session_ref.as_deref() == Some(args.session.as_str()))
    .ok_or_else(|| {
        format!(
            "agent {} owns no session {}; `fleetd status --agent {}` names its current session",
            desired.agent_id, args.session, desired.agent_id
        )
    })?;

    let spec = desired
        .plugin
        .into_spec()
        .require_interface(fleetd_proto::harness_acp::interface_v2());
    let process = PluginProcess::start(spec).await?;
    let mut harness = process.into_harness_acp()?;
    // The plugin's own profile digest, not the stored one: a replay performs no
    // turn, so a profile that drifted since the session was opened is not a
    // reason to refuse reading what it already said.
    let description = harness.describe().await?;
    harness
        .open_session(&OpenSession {
            binding: owner.binding.clone(),
            mode: OpenSessionMode::Resume {
                session_ref: args.session.clone(),
            },
            working_directory: owner.working_directory.clone(),
            additional_directories: owner.additional_directories.clone(),
            mcp_grants: Vec::new(),
            resolved_mcp_grants: Vec::new(),
            profile_digest: description.profile_digest.clone(),
        })
        .await?;
    harness
        .start_transcript(&StartTranscript {
            binding_id: owner.binding.binding_id.clone(),
            binding_generation: owner.binding.binding_generation,
            owner_epoch: owner.binding.owner_epoch,
            session_ref: args.session.clone(),
        })
        .await?;

    let mut entries = Vec::new();
    let complete = loop {
        match harness.next_notification().await? {
            HarnessAcpNotification::TranscriptEntry(entry) => entries.push(entry),
            HarnessAcpNotification::TranscriptComplete(complete) => break complete,
            other => {
                return Err(format!(
                    "harness sent an unexpected notification during a replay: {other:?}"
                )
                .into());
            }
        }
    };
    let _shutdown = harness.shutdown().await?;

    // One session serves a whole channel, so a replay covers every invocation on
    // the lane. Splitting it is a rule any surface would need, so it lives in
    // the wire crate rather than here.
    let turns = fleetd_proto::harness_acp::segment_transcript(entries);
    let attributed = turns
        .iter()
        .filter(|turn| turn.invocation_id.is_some())
        .count();
    print_json(&json!({
        "session_ref": args.session,
        "agent_id": desired.agent_id,
        "binding": owner.binding,
        "session_state": owner.state,
        // A replay is the conversation through the last settled entry: a turn
        // still in flight has stored no output yet, so this is stale rather than
        // torn. A turn with no `invocation_id` is one Fleetd did not dispatch,
        // or the session setup that precedes the first prompt.
        "turns": turns,
        "attributed_turns": attributed,
        "complete": complete,
    }))
}
