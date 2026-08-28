//! `fleetd init` — creating one local fleet.

use std::{error::Error, net::SocketAddr, path::Path};

use clap::Args;

use serde_json::json;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{flatten, print_json};

#[derive(Args)]
pub(super) struct InitArgs {
    #[arg(long, default_value = "127.0.0.1:7419")]
    listen: SocketAddr,
}

/// Prints what `init` created.
///
/// The fleet layout itself is `fleetd_fleet`, so this is one call and a print.
pub(super) async fn init_command(config_path: &Path, args: &InitArgs) -> MainResult<()> {
    let created = fleetd_fleet::create(config_path, args.listen)
        .await
        .map_err(flatten)?;
    print_json(&json!({
        "status": "initialized",
        "config": created.config_path.display().to_string(),
        "database": created.resolved.database.display().to_string(),
        "operator_token_file": created.operator_token_file.display().to_string(),
        "server": created.resolved.server,
        "next": [
            format!("fleetd --fleet-config {} serve", created.config_path.display()),
            format!("fleetd --fleet-config {} status", created.config_path.display()),
        ]
    }))
}
