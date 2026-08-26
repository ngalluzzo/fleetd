mod cli;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _};

fn runtime_log_target_is_safe(target: &str) -> bool {
    !target.starts_with("tungstenite::protocol")
}

#[tokio::main]
async fn main() -> cli::MainResult<()> {
    let environment_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| "fleetd=info".into());
    let sensitive_transport_filter = tracing_subscriber::filter::filter_fn(|metadata| {
        // Tungstenite's protocol trace renders complete application frames.
        // The browser redemption frame contains a one-time credential, so no
        // environment directive may enable this target or one of its children.
        runtime_log_target_is_safe(metadata.target())
    });
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(environment_filter)
                .with_filter(sensitive_transport_filter),
        )
        .init();
    cli::run().await
}

#[cfg(test)]
mod tests {
    use super::runtime_log_target_is_safe;

    #[test]
    fn websocket_protocol_log_targets_are_unconditionally_rejected() {
        assert!(runtime_log_target_is_safe("fleetd::http"));
        assert!(!runtime_log_target_is_safe("tungstenite::protocol"));
        assert!(!runtime_log_target_is_safe("tungstenite::protocol::frame"));
    }
}
