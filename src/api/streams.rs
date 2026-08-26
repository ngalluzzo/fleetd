//! Live channel streaming for native and browser clients.
//!
//! Native clients authenticate the WebSocket with their bearer credential. A
//! browser redeems a single-use grant over the same durable log, so the two
//! transports share replay and never share a credential path.

use axum::{
    Extension, Json,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{CloseFrame, Message as WebSocketMessage, WebSocket},
    },
    http::{HeaderMap, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tokio::time::timeout;
use utoipa::IntoParams;
use utoipa_axum::{router::OpenApiRouter, routes};

use crate::{
    auth::Principal,
    browser_stream_edge::{
        APPLICATION_FRAME_SEND_DEADLINE, BROWSER_STREAM_PROTOCOL, BrowserStreamGrant,
        BrowserStreamGrantIssueRequest, BrowserStreamGrantIssueResponse, BrowserStreamPath,
        BrowserStreamProtocol, BrowserStreamRedemptionRequest, FIRST_FRAME_DEADLINE,
        MAX_REDEMPTION_FRAME_BYTES,
    },
    channel_stream::{
        AuthorizedChannelStream, run_browser_channel_stream, run_native_channel_stream,
    },
    error::{ErrorResponse, FleetError},
    store::now_ms,
    stream_grant_broker::StreamGrantBrokerError,
};

use super::{AppState, guard::require_channel_access};

pub(super) fn routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default()
        .routes(routes!(issue_browser_stream_grant))
        .routes(routes!(stream))
}

/// The browser edge is deliberately outside the bearer-authenticated router: a
/// browser presents a single-use grant instead of a credential.
pub(super) fn browser_routes() -> OpenApiRouter<AppState> {
    OpenApiRouter::default().routes(routes!(browser_channel_stream))
}

#[utoipa::path(
    post,
    path = "/v1/channels/{channel_id}/stream-grants",
    operation_id = "createBrowserChannelStreamGrant",
    tag = "channels",
    summary = "Mint a single-use browser channel-stream grant",
    description = "Operators or exact channel members. The grant is process-local, expires after 15 seconds, and is returned once with Cache-Control: no-store.",
    security(("bearerAuth" = [])),
    params(("channel_id" = String, Path, description = "Channel ID")),
    request_body = BrowserStreamGrantIssueRequest,
    responses(
        (status = 201, description = "Single-use browser stream grant", body = BrowserStreamGrantIssueResponse,
            headers(("Cache-Control" = String, description = "Always no-store"))
        ),
        (status = 400, description = "Invalid cursor or protocol", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Channel membership required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 429, description = "Unused grant capacity exhausted", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    )
)]
async fn issue_browser_stream_grant(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Json(input): Json<BrowserStreamGrantIssueRequest>,
) -> Result<(StatusCode, HeaderMap, Json<BrowserStreamGrantIssueResponse>), FleetError> {
    require_channel_access(&state, &principal, &channel_id).await?;
    state
        .store
        .list_messages(&channel_id, principal.agent_id(), input.after.get(), 1)
        .await?;
    let authorization =
        AuthorizedChannelStream::from_principal(channel_id, input.after.get(), &principal);
    let issued = state
        .stream_grants
        .issue(authorization, input.protocol.as_str())
        .map_err(|error| map_stream_grant_issue_error(&error))?;
    let (raw_grant, lifetime) = issued.into_parts();
    let grant = BrowserStreamGrant::parse(raw_grant)
        .map_err(|_| FleetError::Credential("generated stream grant was invalid".to_owned()))?;
    let lifetime_ms = i64::try_from(lifetime.as_millis()).unwrap_or(i64::MAX);
    let response = BrowserStreamGrantIssueResponse {
        grant,
        expires_at_ms: now_ms().saturating_add(lifetime_ms),
        websocket_path: BrowserStreamPath::ChannelStream,
        protocol: BrowserStreamProtocol::V1,
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        "no-store".parse().expect("static header value"),
    );
    Ok((StatusCode::CREATED, headers, Json(response)))
}

fn map_stream_grant_issue_error(error: &StreamGrantBrokerError) -> FleetError {
    match error {
        StreamGrantBrokerError::Capacity => {
            FleetError::ResourceExhausted("browser stream grant capacity".to_owned())
        }
        StreamGrantBrokerError::InvalidScope | StreamGrantBrokerError::Rejected => {
            FleetError::Invalid("invalid browser stream grant scope".to_owned())
        }
        StreamGrantBrokerError::Entropy | StreamGrantBrokerError::Revalidation(_) => {
            FleetError::Credential("browser stream grant issuance failed".to_owned())
        }
    }
}

#[derive(Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
struct StreamQuery {
    /// Exclusive global message sequence cursor to replay before live delivery.
    #[param(minimum = 0, default = 0)]
    #[serde(default)]
    after: i64,
}

#[utoipa::path(
    get,
    path = "/v1/channels/{channel_id}/stream",
    operation_id = "streamChannelMessages",
    tag = "channels",
    summary = "Replay and stream channel messages",
    description = "WebSocket upgrade for operators or channel members. Each server text frame is one Message JSON object. Reconnect with the highest durably processed seq as `after`. Client frames other than Close are ignored.",
    security(("bearerAuth" = [])),
    params(
        ("channel_id" = String, Path, description = "Channel ID"),
        StreamQuery
    ),
    responses(
        (status = 101, description = "WebSocket protocol switched"),
        (status = 400, description = "Invalid cursor or upgrade request", body = ErrorResponse),
        (status = 401, description = "Missing or invalid credential", body = ErrorResponse),
        (status = 403, description = "Channel membership required", body = ErrorResponse),
        (status = 404, description = "Channel not found", body = ErrorResponse),
        (status = 500, description = "Internal failure", body = ErrorResponse)
    ),
    extensions(
        ("x-fleetd-websocket" = json!({
            "direction": "server-to-client",
            "frameType": "text",
            "messageSchema": { "$ref": "#/components/schemas/Message" },
            "ordering": "ascending seq after replay cursor",
            "clientMessages": "ignored except Close"
        }))
    )
)]
async fn stream(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    Path(channel_id): Path<String>,
    Query(query): Query<StreamQuery>,
    upgrade: WebSocketUpgrade,
) -> Result<Response, FleetError> {
    require_channel_access(&state, &principal, &channel_id).await?;
    state
        .store
        .list_messages(&channel_id, principal.agent_id(), query.after, 1)
        .await?;
    let receiver = state.messages.subscribe();
    let authorization =
        AuthorizedChannelStream::from_principal(channel_id, query.after, &principal);
    debug_assert_eq!(authorization.credential_id(), principal.credential_id());
    Ok(upgrade
        .on_upgrade(move |socket| {
            run_native_channel_stream(socket, state.store, receiver, authorization)
        })
        .into_response())
}

#[utoipa::path(
    get,
    path = "/v1/browser/channel-stream",
    operation_id = "openBrowserChannelStream",
    tag = "channels",
    summary = "Redeem a browser channel-stream grant",
    description = "Same-origin WebSocket upgrade. No bearer or grant appears in the URI, headers, or subprotocol. The first application frame must redeem the single-use grant.",
    responses(
        (status = 101, description = "Browser WebSocket protocol switched",
            headers(("Sec-WebSocket-Protocol" = String, description = "fleetd.channel-stream.browser.v1"))
        ),
        (status = 400, description = "Invalid WebSocket upgrade", body = ErrorResponse),
        (status = 403, description = "Origin, authority, or protocol rejected", body = ErrorResponse),
        (status = 503, description = "Browser stream edge unavailable or at capacity", body = ErrorResponse)
    ),
    extensions(
        ("x-fleetd-websocket" = json!({
            "direction": "bidirectional-authentication-then-server-to-client",
            "frameType": "text",
            "subprotocol": BROWSER_STREAM_PROTOCOL,
            "firstClientMessageSchema": { "$ref": "#/components/schemas/BrowserStreamRedemptionRequest" },
            "serverMessageSchema": { "$ref": "#/components/schemas/BrowserStreamServerFrame" },
            "ordering": "ready, then ascending message.seq after the grant cursor",
            "clientMessagesAfterRedemption": "unsupported"
        }))
    )
)]
async fn browser_channel_stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Some(edge) = &state.browser_stream else {
        return browser_upgrade_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "browser stream edge is not configured",
        );
    };
    if edge.validate_upgrade_headers(&headers).is_err() {
        return browser_upgrade_error(StatusCode::FORBIDDEN, "browser stream upgrade rejected");
    }
    if !state.stream_grants.has_global_active_capacity() {
        return browser_upgrade_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "browser stream capacity exhausted",
        );
    }
    let Some(pre_authentication_slot) = edge.try_acquire_pre_authentication_slot() else {
        return browser_upgrade_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "browser stream capacity exhausted",
        );
    };
    upgrade
        .protocols([BROWSER_STREAM_PROTOCOL])
        .max_message_size(MAX_REDEMPTION_FRAME_BYTES)
        .max_frame_size(MAX_REDEMPTION_FRAME_BYTES)
        .on_upgrade(move |socket| {
            redeem_browser_channel_stream(socket, state, pre_authentication_slot)
        })
        .into_response()
}

fn browser_upgrade_error(status: StatusCode, message: &'static str) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.to_owned(),
        }),
    )
        .into_response()
}

async fn redeem_browser_channel_stream(
    mut socket: WebSocket,
    state: AppState,
    pre_authentication_slot: tokio::sync::OwnedSemaphorePermit,
) {
    let first_frame_deadline = tokio::time::Instant::now() + FIRST_FRAME_DEADLINE;
    let redemption = loop {
        match tokio::time::timeout_at(first_frame_deadline, socket.recv()).await {
            Err(_) => {
                close_browser_socket(&mut socket, 4_408, "grant_timeout").await;
                return;
            }
            Ok(Some(Ok(WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_)))) => {}
            Ok(Some(Ok(WebSocketMessage::Text(text)))) => {
                if let Ok(redemption) =
                    BrowserStreamRedemptionRequest::parse_text_frame(text.as_str())
                {
                    break redemption;
                }
                close_browser_socket(&mut socket, 4_400, "invalid_handshake").await;
                return;
            }
            Ok(_) => {
                close_browser_socket(&mut socket, 4_400, "invalid_handshake").await;
                return;
            }
        }
    };
    let redeemed = match state
        .stream_grants
        .redeem(
            redemption.grant.expose_secret(),
            BrowserStreamProtocol::V1.as_str(),
        )
        .await
    {
        Ok(redeemed) => redeemed,
        Err(StreamGrantBrokerError::Revalidation(_)) => {
            close_browser_socket(&mut socket, 1_011, "internal_error").await;
            return;
        }
        Err(_) => {
            close_browser_socket(&mut socket, 4_401, "grant_rejected").await;
            return;
        }
    };
    let (authorization, active_slot) = redeemed.into_parts();
    let principal = authorization.issuing_principal();
    let access = require_channel_access(&state, &principal, authorization.channel_id()).await;
    if let Err(error) = access {
        let (code, reason) = match error {
            FleetError::Database(_)
            | FleetError::Migration(_)
            | FleetError::Serialization(_)
            | FleetError::Credential(_)
            | FleetError::Io(_) => (1_011, "internal_error"),
            _ => (4_401, "grant_rejected"),
        };
        close_browser_socket(&mut socket, code, reason).await;
        return;
    }
    match state
        .store
        .list_messages(
            authorization.channel_id(),
            authorization.viewer_agent_id(),
            authorization.after(),
            1,
        )
        .await
    {
        Ok(_) => {}
        Err(FleetError::NotFound { .. } | FleetError::Invalid(_)) => {
            close_browser_socket(&mut socket, 4_401, "grant_rejected").await;
            return;
        }
        Err(_) => {
            close_browser_socket(&mut socket, 1_011, "internal_error").await;
            return;
        }
    }
    let receiver = state.messages.subscribe();
    drop(pre_authentication_slot);
    run_browser_channel_stream(
        socket,
        state.store,
        receiver,
        authorization,
        state.auth,
        active_slot,
    )
    .await;
}

async fn close_browser_socket(socket: &mut WebSocket, code: u16, reason: &'static str) {
    let close = socket.send(WebSocketMessage::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    })));
    let _ = timeout(APPLICATION_FRAME_SEND_DEADLINE, close).await;
}
