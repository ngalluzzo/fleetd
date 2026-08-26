mod cli;

use std::ffi::OsStr;
use std::io::IsTerminal as _;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _};

fn runtime_log_target_is_safe(target: &str) -> bool {
    !target.starts_with("tungstenite::protocol")
}

/// Color is a courtesy to an operator watching an interactive terminal. A
/// redirected stream belongs to a log file or a parser, which must receive the
/// record itself and never an SGR escape sequence.
fn ansi_logging_enabled(log_stream_is_terminal: bool, no_color: Option<&OsStr>) -> bool {
    log_stream_is_terminal && no_color.is_none_or(OsStr::is_empty)
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
    // The formatting layer writes runtime logs to stdout, so stdout is the
    // stream whose interactivity settles the question.
    let no_color = std::env::var_os("NO_COLOR");
    let ansi = ansi_logging_enabled(std::io::stdout().is_terminal(), no_color.as_deref());
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(ansi)
                .with_filter(environment_filter)
                .with_filter(sensitive_transport_filter),
        )
        .init();
    cli::run().await
}

#[cfg(test)]
mod tests {
    use super::{ansi_logging_enabled, runtime_log_target_is_safe};
    use std::ffi::OsStr;

    #[test]
    fn websocket_protocol_log_targets_are_unconditionally_rejected() {
        assert!(runtime_log_target_is_safe("fleetd::api"));
        assert!(!runtime_log_target_is_safe("tungstenite::protocol"));
        assert!(!runtime_log_target_is_safe("tungstenite::protocol::frame"));
    }

    #[test]
    fn redirected_runtime_logs_are_never_colorized() {
        assert!(!ansi_logging_enabled(false, None));
        assert!(!ansi_logging_enabled(false, Some(OsStr::new(""))));
        assert!(!ansi_logging_enabled(false, Some(OsStr::new("1"))));
    }

    #[test]
    fn interactive_runtime_logs_observe_the_no_color_convention() {
        assert!(ansi_logging_enabled(true, None));
        // The convention reads an empty assignment as an unset variable.
        assert!(ansi_logging_enabled(true, Some(OsStr::new(""))));
        assert!(!ansi_logging_enabled(true, Some(OsStr::new("1"))));
    }
}
