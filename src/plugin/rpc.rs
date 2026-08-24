use std::{
    collections::HashMap,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use futures_util::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    process::{ChildStdin, ChildStdout},
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::codec::{FramedRead, LinesCodec};

use super::{protocol::PluginNotification, supervisor::PluginError};

const MAX_FRAME_LENGTH: usize = 1024 * 1024;
const NOTIFICATION_BUFFER: usize = 256;

type SharedState = Arc<Mutex<RpcState>>;

#[derive(Default)]
struct RpcState {
    pending: HashMap<u64, oneshot::Sender<Result<Value, ReplyError>>>,
    failure: Option<String>,
}

pub(crate) struct RpcPeer {
    writer: Arc<Mutex<BufWriter<ChildStdin>>>,
    state: SharedState,
    next_id: AtomicU64,
    notifications: mpsc::Receiver<PluginNotification>,
    reader_task: JoinHandle<()>,
}

impl RpcPeer {
    pub(crate) fn new(stdout: ChildStdout, stdin: ChildStdin) -> Self {
        let state = Arc::new(Mutex::new(RpcState::default()));
        let (notifications_tx, notifications) = mpsc::channel(NOTIFICATION_BUFFER);
        let reader_state = Arc::clone(&state);
        let reader_task = tokio::spawn(async move {
            read_responses(stdout, reader_state, notifications_tx).await;
        });
        Self {
            writer: Arc::new(Mutex::new(BufWriter::new(stdin))),
            state,
            next_id: AtomicU64::new(1),
            notifications,
            reader_task,
        }
    }

    pub(crate) async fn call<P, R>(
        &self,
        method: &str,
        params: &P,
        deadline: Duration,
    ) -> Result<R, PluginError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u64::MAX {
            return Err(PluginError::Protocol(
                "JSON-RPC request identifier space exhausted".to_owned(),
            ));
        }
        let request = serde_json::to_vec(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }))?;
        if request.len() > MAX_FRAME_LENGTH {
            return Err(PluginError::Protocol(format!(
                "outbound JSON-RPC frame exceeds {MAX_FRAME_LENGTH} bytes"
            )));
        }
        let (sender, receiver) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            if let Some(failure) = &state.failure {
                return Err(PluginError::Protocol(failure.clone()));
            }
            state.pending.insert(id, sender);
        }
        let exchange = async {
            self.write_frame(&request).await?;
            receiver
                .await
                .map_err(|_| PluginError::Transport("plugin response stream closed".to_owned()))
        };
        let reply = match tokio::time::timeout(deadline, exchange).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(error)) => {
                fail_all(
                    &self.state,
                    format!("plugin transport failed during {method}: {error}"),
                )
                .await;
                return Err(error);
            }
            Err(_) => {
                fail_all(
                    &self.state,
                    format!("plugin call {method} exceeded its deadline"),
                )
                .await;
                return Err(PluginError::Timeout {
                    method: method.to_owned(),
                    timeout: deadline,
                });
            }
        };
        let value = match reply {
            Ok(value) => value,
            Err(ReplyError::Remote {
                code,
                message,
                data,
            }) => {
                return Err(PluginError::Remote {
                    method: method.to_owned(),
                    code,
                    message,
                    data,
                });
            }
            Err(ReplyError::Protocol(message)) => return Err(PluginError::Protocol(message)),
        };
        Ok(serde_json::from_value(value)?)
    }

    pub(crate) fn try_notification(&mut self) -> Option<PluginNotification> {
        self.notifications.try_recv().ok()
    }

    async fn write_frame(&self, frame: &[u8]) -> Result<(), PluginError> {
        let mut writer = self.writer.lock().await;
        writer.write_all(frame).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;
        Ok(())
    }
}

impl Drop for RpcPeer {
    fn drop(&mut self) {
        self.reader_task.abort();
    }
}

#[derive(Debug)]
enum ReplyError {
    Remote {
        code: i64,
        message: String,
        data: Option<Value>,
    },
    Protocol(String),
}

async fn read_responses(
    stdout: ChildStdout,
    state: SharedState,
    notifications: mpsc::Sender<PluginNotification>,
) {
    let mut frames = FramedRead::new(stdout, LinesCodec::new_with_max_length(MAX_FRAME_LENGTH));
    while let Some(frame) = frames.next().await {
        let result = match frame {
            Ok(frame) => handle_frame(&frame, &state, &notifications).await,
            Err(error) => Err(format!("invalid plugin frame: {error}")),
        };
        if let Err(error) = result {
            fail_all(&state, error).await;
            return;
        }
    }
    fail_all(&state, "plugin stdout closed".to_owned()).await;
}

async fn handle_frame(
    frame: &str,
    state: &SharedState,
    notifications: &mpsc::Sender<PluginNotification>,
) -> Result<(), String> {
    let value: Value = serde_json::from_str(frame)
        .map_err(|error| format!("plugin emitted malformed JSON: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "plugin JSON-RPC frame is not an object".to_owned())?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err("plugin frame has an unsupported JSON-RPC version".to_owned());
    }
    if let Some(method) = object.get("method") {
        if object.contains_key("id") {
            return Err("plugin-initiated requests are unsupported in lifecycle v1".to_owned());
        }
        let method = method
            .as_str()
            .ok_or_else(|| "plugin notification method is not a string".to_owned())?;
        let notification = PluginNotification {
            method: method.to_owned(),
            params: object.get("params").cloned().unwrap_or(Value::Null),
        };
        notifications
            .try_send(notification)
            .map_err(|_| "plugin notification buffer exhausted".to_owned())?;
        return Ok(());
    }
    let id = object
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| "plugin response has an invalid request id".to_owned())?;
    let has_result = object.contains_key("result");
    let has_error = object.contains_key("error");
    if has_result == has_error {
        return Err("plugin response must contain exactly one result or error".to_owned());
    }
    let reply = if has_result {
        Ok(object.get("result").cloned().unwrap_or(Value::Null))
    } else {
        Err(parse_remote_error(object.get("error").ok_or_else(
            || "plugin response error is missing".to_owned(),
        )?)?)
    };
    let sender = state
        .lock()
        .await
        .pending
        .remove(&id)
        .ok_or_else(|| format!("plugin responded with unknown request id {id}"))?;
    let _unused = sender.send(reply);
    Ok(())
}

fn parse_remote_error(value: &Value) -> Result<ReplyError, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "plugin JSON-RPC error is not an object".to_owned())?;
    let code = object
        .get("code")
        .and_then(Value::as_i64)
        .ok_or_else(|| "plugin JSON-RPC error code is invalid".to_owned())?;
    let message = object
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| "plugin JSON-RPC error message is invalid".to_owned())?;
    Ok(ReplyError::Remote {
        code,
        message: message.to_owned(),
        data: object.get("data").cloned(),
    })
}

async fn fail_all(state: &SharedState, message: String) {
    let senders: Vec<_> = {
        let mut state = state.lock().await;
        state.failure = Some(message.clone());
        state.pending.drain().map(|(_, sender)| sender).collect()
    };
    for sender in senders {
        let _unused = sender.send(Err(ReplyError::Protocol(message.clone())));
    }
}
