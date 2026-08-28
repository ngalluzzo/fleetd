//! `fleetd message` — appending to the log and following it.

use std::error::Error;

use clap::Subcommand;

use fleetd::model::{MessagePage, SendMessage};
use futures_util::StreamExt;
use serde_json::json;

pub type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

use super::{ApiClient, parse_json, print_json, print_response};

#[derive(Subcommand)]
pub(super) enum MessageCommand {
    Send {
        #[arg(long)]
        channel: String,
        #[arg(long)]
        idempotency_key: Option<String>,
        #[arg(long = "to")]
        recipient: Option<String>,
        #[arg(long, default_value = "text")]
        kind: String,
        #[arg(long, conflicts_with = "payload")]
        text: Option<String>,
        #[arg(long)]
        payload: Option<String>,
        #[arg(long)]
        correlation: Option<String>,
        #[arg(long)]
        causation: Option<String>,
    },
    List {
        #[arg(long)]
        channel: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    Watch {
        #[arg(long)]
        channel: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
}

pub(super) async fn message_command(api: &ApiClient, command: MessageCommand) -> MainResult<()> {
    match command {
        MessageCommand::Send {
            channel,
            idempotency_key,
            recipient,
            kind,
            text,
            payload,
            correlation,
            causation,
        } => {
            let payload = match (text, payload) {
                (Some(text), None) => json!({ "text": text }),
                (None, Some(payload)) => parse_json(&payload)?,
                (None, None) => json!({}),
                (Some(_), Some(_)) => {
                    return Err("message text and payload are mutually exclusive".into());
                }
            };
            let response = api
                .post(&format!("/v1/channels/{channel}/messages"))
                .json(&SendMessage {
                    idempotency_key,
                    recipient_id: recipient,
                    kind,
                    payload,
                    correlation_id: correlation,
                    causation_id: causation,
                })
                .send()
                .await?;
            print_response(response).await
        }
        MessageCommand::List {
            channel,
            after,
            limit,
        } => {
            let page: MessagePage = api
                .get(&format!(
                    "/v1/channels/{channel}/messages?after={after}&limit={limit}"
                ))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            print_json(&page)
        }
        MessageCommand::Watch { channel, after } => watch(api, &channel, after).await,
    }
}

pub(super) async fn watch(api: &ApiClient, channel: &str, after: i64) -> MainResult<()> {
    use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderValue};

    let socket_base = if let Some(rest) = api.server.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = api.server.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        return Err("fleetd server URL must start with http:// or https://".into());
    };
    let url = format!("{socket_base}/v1/channels/{channel}/stream?after={after}");
    let mut request = url.into_client_request()?;
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", api.token))?,
    );
    let (mut socket, _) = tokio_tungstenite::connect_async(request).await?;
    while let Some(frame) = socket.next().await {
        let frame = frame?;
        if frame.is_text() {
            println!("{}", frame.into_text()?);
        } else if frame.is_close() {
            break;
        }
    }
    Ok(())
}
