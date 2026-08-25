use std::{
    io::Write as _,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::Parser;
use fleetd_soak::{RunStatus, execute_plan, load_plan};

#[derive(Debug, Parser)]
#[command(about = "Run exact Fleetd workloads and preserve qualification evidence")]
struct Cli {
    /// Exact JSON workload plan.
    #[arg(long)]
    plan: PathBuf,
    /// New report path. Existing files are never overwritten.
    #[arg(long)]
    output: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(status) => status,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let (plan, digest) = load_plan(&cli.plan)?;
    let report = execute_plan(&plan, digest).await?;
    write_new_report(&cli.output, &report).await?;
    if report.status == RunStatus::Passed {
        Ok(ExitCode::SUCCESS)
    } else {
        Ok(ExitCode::FAILURE)
    }
}

async fn write_new_report(
    path: &Path,
    report: &fleetd_soak::SoakReport,
) -> Result<(), Box<dyn std::error::Error>> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(parent).await?;
    let bytes = serde_json::to_vec_pretty(report)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&bytes)?;
    temporary.write_all(b"\n")?;
    temporary.as_file().sync_all()?;
    temporary.persist_noclobber(path)?;
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}
