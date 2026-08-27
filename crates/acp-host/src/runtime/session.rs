//! Opening, adopting, and retiring one native session.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
};

use agent_client_protocol::{Agent, ConnectionTo};
use fleetd_proto::harness_acp::{
    CloseSession, CloseSessionResult, OpenSession, OpenSessionMode, OpenSessionResult,
    ResolvedMcpEndpoint,
};
use http::{HeaderName, HeaderValue};
use serde_json::{Value, json};
use tokio::sync::Mutex;
use url::Url;

use super::{
    AdoptionMethods, DriverError, MAX_FRAME_BYTES, RawLoadSessionRequest, RawNewSessionRequest,
    RawResumeSessionRequest, SessionState, SharedState, bound_json,
};

pub(super) async fn open_session(
    connection: &ConnectionTo<Agent>,
    shared: &Arc<Mutex<SharedState>>,
    request: OpenSession,
    adoption: AdoptionMethods,
) -> Result<OpenSessionResult, DriverError> {
    let mcp_servers = resolve_mcp_servers(&request)?;
    let directories = request
        .additional_directories
        .iter()
        .map(PathBuf::from)
        .collect::<Vec<_>>();
    let (session_ref, resumed, raw_result) = match &request.mode {
        OpenSessionMode::Create => {
            let raw_request = json!({
                "cwd": request.working_directory,
                "additionalDirectories": directories,
                "mcpServers": mcp_servers
            });
            let response = connection
                .send_request(RawNewSessionRequest(raw_request))
                .block_task()
                .await
                .map_err(|error| DriverError::Runtime(error.to_string()))?;
            let session_ref = response
                .0
                .get("sessionId")
                .and_then(Value::as_str)
                .ok_or_else(|| DriverError::Protocol("session/new omitted sessionId".to_owned()))?
                .to_owned();
            (session_ref, false, response.0)
        }
        OpenSessionMode::Resume { session_ref } => {
            let Some(method) = adoption.method() else {
                return Err(DriverError::Protocol(
                    "inner ACP runtime supports neither session/resume nor session/load".to_owned(),
                ));
            };
            let raw_request = json!({
                "sessionId": session_ref,
                "cwd": request.working_directory,
                "additionalDirectories": directories,
                "mcpServers": mcp_servers
            });
            let response = if method == "session/resume" {
                connection
                    .send_request(RawResumeSessionRequest(raw_request))
                    .block_task()
                    .await
            } else {
                connection
                    .send_request(RawLoadSessionRequest(raw_request))
                    .block_task()
                    .await
            }
            .map_err(|error| DriverError::Runtime(error.to_string()))?;
            (session_ref.clone(), true, response.0)
        }
    };
    let mut state = shared.lock().await;
    if let Some(existing) = state.sessions.get(&session_ref)
        && (existing.binding != request.binding
            || existing.cwd != request.working_directory
            || existing.additional_directories != request.additional_directories
            || existing.mcp_grants != request.mcp_grants)
    {
        return Err(DriverError::Protocol(
            "session reference already belongs to incompatible binding state".to_owned(),
        ));
    }
    let effective_working_directory = request.working_directory.clone();
    let effective_additional_directories = request.additional_directories.clone();
    let effective_mcp_grants = request.mcp_grants.clone();
    state.sessions.insert(
        session_ref.clone(),
        SessionState {
            binding: request.binding,
            cwd: request.working_directory,
            additional_directories: request.additional_directories,
            mcp_grants: request.mcp_grants,
            active: None,
            capturing: None,
        },
    );
    Ok(OpenSessionResult {
        session_ref,
        profile_digest: request.profile_digest,
        resumed,
        effective_config: json!({
            "working_directory": effective_working_directory,
            "additional_directories": effective_additional_directories,
            "mcp_grants": effective_mcp_grants,
        }),
        raw_session_result: bound_json(raw_result, MAX_FRAME_BYTES / 2),
    })
}

pub(super) fn resolve_mcp_servers(request: &OpenSession) -> Result<Vec<Value>, DriverError> {
    let mut requested = BTreeSet::new();
    for name in &request.mcp_grants {
        if name.trim().is_empty() || name.len() > 256 {
            return Err(DriverError::Protocol(
                "MCP grant names must contain between 1 and 256 bytes".to_owned(),
            ));
        }
        if !requested.insert(name.as_str()) {
            return Err(DriverError::Protocol(format!(
                "duplicate MCP grant name: {name}"
            )));
        }
    }
    let mut resolved = BTreeMap::new();
    for grant in &request.resolved_mcp_grants {
        if !requested.contains(grant.name.as_str()) {
            return Err(DriverError::Protocol(format!(
                "resolved MCP endpoint was not requested: {}",
                grant.name
            )));
        }
        if resolved.insert(grant.name.as_str(), grant).is_some() {
            return Err(DriverError::Protocol(format!(
                "duplicate resolved MCP grant: {}",
                grant.name
            )));
        }
    }
    if requested.len() != resolved.len() {
        return Err(DriverError::Protocol(
            "every requested MCP grant must have one resolved endpoint".to_owned(),
        ));
    }

    request
        .mcp_grants
        .iter()
        .map(|name| {
            let grant = resolved
                .get(name.as_str())
                .expect("resolved and requested MCP grant sets match");
            match &grant.endpoint {
                ResolvedMcpEndpoint::Http { url, headers } => {
                    validate_loopback_mcp_url(url)?;
                    if headers.len() > 16 {
                        return Err(DriverError::Protocol(
                            "resolved MCP HTTP endpoints may have at most 16 headers".to_owned(),
                        ));
                    }
                    let mut header_names = BTreeSet::new();
                    for header in headers {
                        let parsed_name =
                            HeaderName::from_bytes(header.name.as_bytes()).map_err(|_| {
                                DriverError::Protocol("invalid MCP HTTP header name".to_owned())
                            })?;
                        HeaderValue::from_str(&header.value).map_err(|_| {
                            DriverError::Protocol("invalid MCP HTTP header value".to_owned())
                        })?;
                        if header.value.len() > 4_096 {
                            return Err(DriverError::Protocol(
                                "MCP HTTP header values must not exceed 4,096 bytes".to_owned(),
                            ));
                        }
                        if !header_names.insert(parsed_name.as_str().to_owned()) {
                            return Err(DriverError::Protocol(
                                "duplicate MCP HTTP header name".to_owned(),
                            ));
                        }
                    }
                    Ok(json!({
                        "type": "http",
                        "name": name,
                        "url": url,
                        "headers": headers,
                    }))
                }
            }
        })
        .collect()
}

pub(super) fn validate_loopback_mcp_url(raw: &str) -> Result<(), DriverError> {
    let url = Url::parse(raw)
        .map_err(|_| DriverError::Protocol("resolved MCP URL is invalid".to_owned()))?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.port().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(DriverError::Protocol(
            "resolved MCP URL must be an explicit 127.0.0.1 HTTP endpoint without credentials, query, or fragment"
                .to_owned(),
        ));
    }
    Ok(())
}

pub(super) async fn close_session(
    shared: &Arc<Mutex<SharedState>>,
    request: CloseSession,
) -> Result<CloseSessionResult, DriverError> {
    let mut state = shared.lock().await;
    let session = state.sessions.get(&request.session_ref).ok_or_else(|| {
        DriverError::Protocol("session reference is not owned by driver".to_owned())
    })?;
    if session.active.is_some() {
        return Err(DriverError::Protocol(
            "cannot close a session with an active turn".to_owned(),
        ));
    }
    if session.binding.binding_id != request.binding_id
        || session.binding.binding_generation != request.binding_generation
        || session.binding.owner_epoch != request.owner_epoch
    {
        return Err(DriverError::Protocol(
            "session close fence does not match binding".to_owned(),
        ));
    }
    state.sessions.remove(&request.session_ref);
    Ok(CloseSessionResult {
        ownership_retired: true,
        native_resources_released: false,
    })
}

#[cfg(test)]
mod tests {
    use fleetd_proto::harness_acp::{
        Binding, OpenSession, OpenSessionMode, ResolvedMcpEndpoint, ResolvedMcpGrant,
        ResolvedMcpHttpHeader,
    };

    use super::resolve_mcp_servers;

    #[test]
    fn mcp_resolution_requires_an_exact_requested_loopback_endpoint() {
        let mut request = open_request();
        request.mcp_grants = vec!["fleet.messaging.send".to_owned()];
        assert!(resolve_mcp_servers(&request).is_err());

        request.resolved_mcp_grants = vec![ResolvedMcpGrant {
            name: "fleet.messaging.send".to_owned(),
            endpoint: ResolvedMcpEndpoint::Http {
                url: "https://example.com/mcp".to_owned(),
                headers: Vec::new(),
            },
        }];
        assert!(resolve_mcp_servers(&request).is_err());

        request.resolved_mcp_grants[0].endpoint = ResolvedMcpEndpoint::Http {
            url: "http://127.0.0.1:49152/mcp".to_owned(),
            headers: vec![ResolvedMcpHttpHeader {
                name: "x-fleetd-grant-token".to_owned(),
                value: "narrow-token".to_owned(),
            }],
        };
        let servers = resolve_mcp_servers(&request).expect("valid resolution");
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0]["type"], "http");
        assert_eq!(servers[0]["name"], "fleet.messaging.send");
    }

    #[test]
    fn mcp_resolution_rejects_duplicate_or_unrequested_grants() {
        let mut request = open_request();
        request.mcp_grants = vec![
            "fleet.messaging.send".to_owned(),
            "fleet.messaging.send".to_owned(),
        ];
        assert!(resolve_mcp_servers(&request).is_err());

        request.mcp_grants = vec!["fleet.messaging.send".to_owned()];
        request.resolved_mcp_grants = vec![ResolvedMcpGrant {
            name: "fleet.messaging.read".to_owned(),
            endpoint: ResolvedMcpEndpoint::Http {
                url: "http://127.0.0.1:49152/mcp".to_owned(),
                headers: Vec::new(),
            },
        }];
        assert!(resolve_mcp_servers(&request).is_err());
    }

    fn open_request() -> OpenSession {
        OpenSession {
            binding: Binding {
                binding_id: "binding".to_owned(),
                binding_generation: 1,
                owner_epoch: 1,
            },
            mode: OpenSessionMode::Create,
            working_directory: "/tmp".to_owned(),
            additional_directories: Vec::new(),
            mcp_grants: Vec::new(),
            resolved_mcp_grants: Vec::new(),
            profile_digest: "sha256:profile".to_owned(),
        }
    }
}
