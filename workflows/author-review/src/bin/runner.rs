use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use fleetd_author_review::runner::{AuthorReviewRunner, load_configuration};

#[derive(Debug, Parser)]
#[command(about = "Run the draft external Fleetd author-review workflow")]
struct Cli {
    /// Exact credential-free runner configuration.
    #[arg(long)]
    config: PathBuf,
    /// Process at most one currently eligible delivery and exit.
    #[arg(long)]
    once: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let configuration = load_configuration(&cli.config)?;
    let mut runner = AuthorReviewRunner::start(configuration).await?;
    if cli.once {
        runner.tick().await?;
        return Ok(());
    }
    loop {
        runner.tick().await?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return Ok(());
            }
            () = tokio::time::sleep(runner.poll_interval()) => {}
        }
    }
}
