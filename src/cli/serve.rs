//! `fleetd serve` — running the daemon.

use std::{error::Error, net::SocketAddr, path::PathBuf};

use clap::Args;
use fleetd_fleet::validate_listen_address;

use fleetd::{
    auth::AuthService,
    execution::invocation,
    http::{AppState, router},
    store::Store,
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{default_operator_token_path, flatten, shutdown_signal};

#[derive(Args)]
pub(super) struct ServeArgs {
    /// Override the listen address named by the fleet configuration.
    #[arg(long, env = "FLEETD_LISTEN")]
    listen: Option<SocketAddr>,
    /// Override the database named by the fleet configuration.
    #[arg(long, env = "FLEETD_DB")]
    db: Option<PathBuf>,
    #[arg(long)]
    operator_token_file: Option<PathBuf>,
}

pub(super) async fn serve(args: ServeArgs, fleet: &fleetd_fleet::ResolvedFleet) -> MainResult<()> {
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
