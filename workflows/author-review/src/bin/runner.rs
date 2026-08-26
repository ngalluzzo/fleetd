use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use fleetd_author_review::runner::{AuthorReviewRunner, TickOutcome, load_configuration};

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
        report_outcome(&runner.tick().await?);
        return Ok(());
    }
    let mut consecutive_transient_failures = 0_i64;
    loop {
        let delay = match runner.tick().await {
            Ok(outcome) => {
                consecutive_transient_failures = 0;
                report_outcome(&outcome);
                post_tick_delay(&outcome, runner.poll_interval())
            }
            Err(error) if error.is_transient() => {
                consecutive_transient_failures = consecutive_transient_failures.saturating_add(1);
                let retry_after_ms = runner.retry_delay_for_attempt(consecutive_transient_failures);
                eprintln!(
                    "warning: {error}; runner recovery: retry the failed phase in {retry_after_ms}ms"
                );
                std::time::Duration::from_millis(retry_after_ms)
            }
            Err(error) => return Err(error.into()),
        };
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return Ok(());
            }
            () = tokio::time::sleep(delay) => {}
        }
    }
}

fn post_tick_delay(
    outcome: &TickOutcome,
    poll_interval: std::time::Duration,
) -> std::time::Duration {
    if matches!(outcome, TickOutcome::Retried { .. }) {
        std::time::Duration::ZERO
    } else {
        poll_interval
    }
}

fn report_outcome(outcome: &TickOutcome) {
    match outcome {
        TickOutcome::Retried {
            retry_after_ms,
            diagnostic,
        } => eprintln!(
            "warning: {diagnostic}; runner recovery: exact delivery scheduled after {retry_after_ms}ms"
        ),
        TickOutcome::Blocked { diagnostic } => {
            eprintln!("error: {diagnostic}");
        }
        TickOutcome::Idle | TickOutcome::Acknowledged => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_outcome_claims_other_work_before_sleeping() {
        let poll_interval = std::time::Duration::from_mins(1);
        let retried = TickOutcome::Retried {
            retry_after_ms: 71_000,
            diagnostic: "bounded retry".to_owned(),
        };

        assert_eq!(
            post_tick_delay(&retried, poll_interval),
            std::time::Duration::ZERO
        );
        assert_eq!(
            post_tick_delay(&TickOutcome::Acknowledged, poll_interval),
            poll_interval
        );
    }
}
