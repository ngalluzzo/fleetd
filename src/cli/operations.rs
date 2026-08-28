//! `fleetd status`, `trace`, and `deliveries` — the operator read models.

use std::error::Error;

use clap::{Args, ValueEnum};

use fleetd::model::DeliveryState;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{ApiClient, print_response};

#[derive(Args)]
pub(super) struct StatusArgs {
    /// Limit the report to one agent ID.
    #[arg(long)]
    agent: Option<String>,
    /// Bound how many delivery rows the census reads.
    #[arg(long, default_value_t = 500)]
    delivery_limit: u32,
}

#[derive(Args)]
pub(super) struct TraceArgs {
    /// Stable invocation ID.
    #[arg(long)]
    invocation: String,
}

#[derive(Args)]
pub(super) struct DeliveriesArgs {
    /// Limit results to one agent ID.
    #[arg(long)]
    agent: Option<String>,
    /// Limit results to one durable delivery state.
    #[arg(long, value_enum)]
    state: Option<DeliveryStateArg>,
    /// Bound the returned read model.
    #[arg(long, default_value_t = 100)]
    limit: u32,
}

/// The delivery states an operator may filter on.
///
/// This mirrors `DeliveryState` for clap's sake only; the wire spelling comes
/// from the codec so the CLI never becomes a second source of the names.
#[derive(Clone, Copy, ValueEnum)]
pub(super) enum DeliveryStateArg {
    Pending,
    Leased,
    Blocked,
    Acknowledged,
    Dead,
}

impl From<DeliveryStateArg> for DeliveryState {
    fn from(value: DeliveryStateArg) -> Self {
        match value {
            DeliveryStateArg::Pending => Self::Pending,
            DeliveryStateArg::Leased => Self::Leased,
            DeliveryStateArg::Blocked => Self::Blocked,
            DeliveryStateArg::Acknowledged => Self::Acknowledged,
            DeliveryStateArg::Dead => Self::Dead,
        }
    }
}

/// Prints the fleet health report.
///
/// The report is composed by the daemon in one read, so this is a single
/// request and a print. It deliberately holds no rule about what "current" or
/// "active" means; see `fleetd_execution::health`.
pub(super) async fn status_command(api: &ApiClient, args: StatusArgs) -> MainResult<()> {
    let mut parameters = vec![format!("delivery_limit={}", args.delivery_limit)];
    if let Some(agent) = args.agent {
        parameters.push(format!("agent={agent}"));
    }
    print_response(
        api.get(&format!("/v1/fleet-health?{}", parameters.join("&")))
            .send()
            .await?,
    )
    .await
}

pub(super) async fn trace_command(api: &ApiClient, args: TraceArgs) -> MainResult<()> {
    print_response(
        api.get(&format!("/v1/invocations/{}/trace", args.invocation))
            .send()
            .await?,
    )
    .await
}

pub(super) async fn deliveries_command(api: &ApiClient, args: DeliveriesArgs) -> MainResult<()> {
    let mut parameters = vec![format!("limit={}", args.limit)];
    if let Some(agent) = args.agent {
        parameters.push(format!("agent={agent}"));
    }
    if let Some(state) = args.state {
        parameters.push(format!("state={}", DeliveryState::from(state).as_str()));
    }
    print_response(
        api.get(&format!("/v1/deliveries?{}", parameters.join("&")))
            .send()
            .await?,
    )
    .await
}
