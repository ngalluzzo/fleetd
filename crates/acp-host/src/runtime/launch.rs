//! Launching the inner ACP runtime and owning its process group.

use std::sync::Arc;

use agent_client_protocol::{AcpAgent, AcpAgentConfig, Agent, ConnectionTo};
use fleetd_proto::harness_acp::DescribeResult;
use serde_json::json;
use tokio::sync::{Mutex, mpsc, oneshot};

use super::{
    Command, DriverError, DriverNotification, RawPermissionRequest, RawResponse,
    RawSessionNotification, RuntimeConfig, SharedState, acp_error, handle_permission_request,
    handle_session_update, initialize_runtime, serve_commands,
};

pub(super) async fn run_acp(
    runtime: RuntimeConfig,
    executable_digest: String,
    profile_digest: String,
    shared: Arc<Mutex<SharedState>>,
    mut commands: mpsc::Receiver<Command>,
    notifications: mpsc::Sender<DriverNotification>,
    ready: oneshot::Sender<Result<DescribeResult, DriverError>>,
) -> Result<(), DriverError> {
    let agent_config = build_agent_config(&runtime)?;
    let agent = AcpAgent::new(agent_config);
    let update_shared = Arc::clone(&shared);
    let update_notifications = notifications.clone();
    let permission_shared = Arc::clone(&shared);
    let permission_notifications = notifications.clone();

    let connection = agent_client_protocol::Client
        .builder()
        .name("fleetd-acp-host")
        .on_receive_notification(
            async move |notification: RawSessionNotification, _connection| {
                handle_session_update(&update_shared, &update_notifications, notification.0)
                    .await
                    .map_err(acp_error)
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RawPermissionRequest, responder, _connection| {
                let response = handle_permission_request(
                    &permission_shared,
                    &permission_notifications,
                    request.0,
                )
                .await
                .unwrap_or_else(|_| json!({"outcome": {"outcome": "cancelled"}}));
                responder.respond(RawResponse(response))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(agent, |connection: ConnectionTo<Agent>| async move {
            let initialized =
                initialize_runtime(&connection, &runtime, executable_digest, profile_digest).await;
            let (description, adoption) = match initialized {
                Ok(initialized) => initialized,
                Err(error) => {
                    let _unused = ready.send(Err(error));
                    return Ok(());
                }
            };
            let _unused = ready.send(Ok(description));
            serve_commands(
                &connection,
                &shared,
                &notifications,
                &mut commands,
                adoption,
            )
            .await
        })
        .await;

    connection.map_err(|error| DriverError::Runtime(error.to_string()))
}

pub(super) fn build_agent_config(runtime: &RuntimeConfig) -> Result<AcpAgentConfig, DriverError> {
    let launcher = std::env::current_exe()?;
    let mut launcher_args = vec![
        "--inner-launch".to_owned(),
        parent_process_group()?,
        runtime.executable.to_string_lossy().into_owned(),
    ];
    launcher_args.extend(runtime.args.clone());
    Ok(AcpAgentConfig::new(launcher)
        .args(launcher_args)
        .envs(runtime.environment.clone()))
}

#[cfg(unix)]
pub(super) fn parent_process_group() -> Result<String, DriverError> {
    let process_group = nix::unistd::getpgrp().as_raw();
    if process_group <= 0 {
        return Err(DriverError::Runtime(
            "driver does not have a valid parent process group".to_owned(),
        ));
    }
    Ok(process_group.to_string())
}

#[cfg(not(unix))]
pub(super) fn parent_process_group() -> Result<String, DriverError> {
    Err(DriverError::InvalidConfig(
        "the ACP driver requires Unix process-group ownership".to_owned(),
    ))
}
