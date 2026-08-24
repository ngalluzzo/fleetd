mod cli;

use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> cli::MainResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "fleetd=info".into()))
        .init();
    cli::run().await
}
