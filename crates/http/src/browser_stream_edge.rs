//! Wire types, origin policy, and pre-authentication capacity for the browser
//! channel-stream edge.

use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use axum::http::{
    HeaderMap, HeaderName,
    header::{HOST, ORIGIN, SEC_WEBSOCKET_PROTOCOL},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Deserializer, Serialize, de};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use utoipa::{OpenApi, ToSchema};

use fleetd_proto::model::Message;

pub const BROWSER_STREAM_PROTOCOL: &str = "fleetd.channel-stream.browser.v1";
pub const BROWSER_STREAM_PATH: &str = "/v1/browser/channel-stream";
pub const STREAM_GRANT_PREFIX: &str = "fl_sg_";
pub const STREAM_GRANT_ENTROPY_BYTES: usize = 32;
pub const STREAM_GRANT_REDEMPTION_LIFETIME: Duration = Duration::from_secs(15);
pub const FIRST_FRAME_DEADLINE: Duration = Duration::from_secs(5);
pub const MAX_REDEMPTION_FRAME_BYTES: usize = 1_024;
pub const MAX_UNUSED_GRANTS_PER_CREDENTIAL: usize = 8;
pub const MAX_UNUSED_GRANTS_PER_DAEMON: usize = 1_024;
pub const MAX_PRE_AUTHENTICATION_SOCKETS_PER_DAEMON: usize = 64;
pub const MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL: usize = 16;
pub const MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON: usize = 1_024;
pub const CREDENTIAL_REVALIDATION_INTERVAL: Duration = Duration::from_secs(30);
pub const APPLICATION_FRAME_SEND_DEADLINE: Duration = Duration::from_secs(10);

const STREAM_GRANT_ENCODED_ENTROPY_BYTES: usize = 43;

/// The schemas the browser edge contributes to the contract.
///
/// The edge speaks WebSocket frames rather than request bodies, so no route
/// signature mentions these types and nothing registers them implicitly. They
/// are declared here, beside the types themselves, rather than in the module
/// that composes the contract.
#[derive(OpenApi)]
#[openapi(components(schemas(
    BrowserStreamCursor,
    BrowserStreamGrant,
    BrowserStreamGrantIssueRequest,
    BrowserStreamGrantIssueResponse,
    BrowserStreamPath,
    BrowserStreamProtocol,
    BrowserStreamRedemptionMessageType,
    BrowserStreamRedemptionRequest,
    BrowserStreamServerFrame
)))]
pub(super) struct Schemas;

/// Exact browser stream protocol negotiated during upgrade and bound into a
/// grant. Representing the constant as a closed enum makes alternate values
/// fail during JSON decoding and constrains its generated schema.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub enum BrowserStreamProtocol {
    #[serde(rename = "fleetd.channel-stream.browser.v1")]
    V1,
}

impl BrowserStreamProtocol {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V1 => BROWSER_STREAM_PROTOCOL,
        }
    }
}

/// Exact path returned by successful grant issuance.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub enum BrowserStreamPath {
    #[serde(rename = "/v1/browser/channel-stream")]
    ChannelStream,
}

impl BrowserStreamPath {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ChannelStream => BROWSER_STREAM_PATH,
        }
    }
}

/// A valid exclusive cursor in Fleetd's signed global message sequence.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, ToSchema)]
#[serde(transparent)]
#[schema(value_type = i64)]
pub struct BrowserStreamCursor(i64);

impl BrowserStreamCursor {
    #[must_use]
    pub const fn new(value: i64) -> Option<Self> {
        if value < 0 { None } else { Some(Self(value)) }
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl<'de> Deserialize<'de> for BrowserStreamCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = i64::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| de::Error::custom("cursor must be non-negative"))
    }
}

/// One validated raw stream-grant token. Debug output is always redacted.
#[derive(Clone, Eq, PartialEq, Serialize, ToSchema)]
#[serde(transparent)]
#[schema(
    value_type = String,
    pattern = r"^fl_sg_[A-Za-z0-9_-]{43}$"
)]
pub struct BrowserStreamGrant(String);

impl BrowserStreamGrant {
    /// Parses one exact, canonically encoded stream grant.
    ///
    /// # Errors
    ///
    /// Returns an error when the prefix, encoded length, alphabet, decoded
    /// entropy length, or canonical base64url representation is invalid.
    pub fn parse(value: impl Into<String>) -> Result<Self, BrowserStreamGrantParseError> {
        let value = value.into();
        validate_grant_shape(&value)?;
        Ok(Self(value))
    }

    /// Exposes the one-time secret only to the issuance/redemption integration
    /// that must serialize or consume it. Callers must not log the result.
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BrowserStreamGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BrowserStreamGrant([REDACTED])")
    }
}

impl<'de> Deserialize<'de> for BrowserStreamGrant {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("invalid browser stream grant shape")]
pub struct BrowserStreamGrantParseError;

fn validate_grant_shape(value: &str) -> Result<(), BrowserStreamGrantParseError> {
    let Some(encoded) = value.strip_prefix(STREAM_GRANT_PREFIX) else {
        return Err(BrowserStreamGrantParseError);
    };
    if encoded.len() != STREAM_GRANT_ENCODED_ENTROPY_BYTES {
        return Err(BrowserStreamGrantParseError);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| BrowserStreamGrantParseError)?;
    if decoded.len() != STREAM_GRANT_ENTROPY_BYTES || URL_SAFE_NO_PAD.encode(decoded) != encoded {
        return Err(BrowserStreamGrantParseError);
    }
    Ok(())
}

/// Strict authenticated request to mint one browser stream grant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BrowserStreamGrantIssueRequest {
    pub after: BrowserStreamCursor,
    pub protocol: BrowserStreamProtocol,
}

/// One-time response to successful browser stream grant issuance.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BrowserStreamGrantIssueResponse {
    pub grant: BrowserStreamGrant,
    pub expires_at_ms: i64,
    pub websocket_path: BrowserStreamPath,
    pub protocol: BrowserStreamProtocol,
}

/// The only application message accepted before browser stream authority is
/// established. It carries no caller-selected channel, cursor, or principal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct BrowserStreamRedemptionRequest {
    #[serde(rename = "type")]
    pub message_type: BrowserStreamRedemptionMessageType,
    pub grant: BrowserStreamGrant,
}

impl BrowserStreamRedemptionRequest {
    /// Parses one already-reassembled text frame while enforcing the complete
    /// UTF-8 byte bound before JSON allocation or grant decoding.
    ///
    /// # Errors
    ///
    /// Returns an error when the frame exceeds the byte limit or is not the
    /// closed redemption request shape.
    pub fn parse_text_frame(value: &str) -> Result<Self, RedemptionFrameError> {
        if value.len() > MAX_REDEMPTION_FRAME_BYTES {
            return Err(RedemptionFrameError::Oversized);
        }
        serde_json::from_str(value).map_err(|_| RedemptionFrameError::Invalid)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub enum BrowserStreamRedemptionMessageType {
    #[serde(rename = "redeem")]
    Redeem,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum RedemptionFrameError {
    #[error("browser stream redemption frame exceeds its byte limit")]
    Oversized,
    #[error("invalid browser stream redemption frame")]
    Invalid,
}

/// Tagged server-to-browser application frames. The immutable message
/// envelope is nested without translation or field selection.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserStreamServerFrame {
    Ready {
        protocol: BrowserStreamProtocol,
        channel_id: String,
        after: BrowserStreamCursor,
    },
    Message {
        message: Box<Message>,
    },
}

impl BrowserStreamServerFrame {
    #[must_use]
    pub fn ready(channel_id: impl Into<String>, after: BrowserStreamCursor) -> Self {
        Self::Ready {
            protocol: BrowserStreamProtocol::V1,
            channel_id: channel_id.into(),
            after,
        }
    }

    #[must_use]
    pub fn message(message: Message) -> Self {
        Self::Message {
            message: Box::new(message),
        }
    }
}

/// Process-local policy and capacity for sockets that have upgraded but have
/// not yet redeemed a stream grant.
#[derive(Clone)]
pub(crate) struct BrowserStreamEdgeState {
    upgrade_policy: BrowserUpgradePolicy,
    pre_authentication_slots: Arc<Semaphore>,
}

impl BrowserStreamEdgeState {
    pub(crate) fn for_http_listener(
        listen_address: SocketAddr,
    ) -> Result<Self, BrowserUpgradePolicyError> {
        Ok(Self {
            upgrade_policy: BrowserUpgradePolicy::from_listen_address(
                BrowserOriginScheme::Http,
                listen_address,
            )?,
            pre_authentication_slots: Arc::new(Semaphore::new(
                MAX_PRE_AUTHENTICATION_SOCKETS_PER_DAEMON,
            )),
        })
    }

    pub(crate) fn canonical_origin(&self) -> &str {
        self.upgrade_policy.canonical_origin()
    }

    pub(crate) fn validate_upgrade_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<(), BrowserUpgradePolicyError> {
        self.upgrade_policy.validate_upgrade_headers(headers)
    }

    pub(crate) fn try_acquire_pre_authentication_slot(&self) -> Option<OwnedSemaphorePermit> {
        Arc::clone(&self.pre_authentication_slots)
            .try_acquire_owned()
            .ok()
    }
}

/// Scheme of the exact embedded-browser origin advertised by Fleetd.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BrowserOriginScheme {
    Http,
}

impl BrowserOriginScheme {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
        }
    }

    const fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
        }
    }
}

/// A startup-derived, exact allow policy for the unauthenticated WebSocket
/// upgrade. It never canonicalizes or reflects caller-controlled values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BrowserUpgradePolicy {
    origin: String,
    authority: String,
}

impl BrowserUpgradePolicy {
    pub(crate) fn from_listen_address(
        scheme: BrowserOriginScheme,
        listen_address: SocketAddr,
    ) -> Result<Self, BrowserUpgradePolicyError> {
        if !listen_address.ip().is_loopback() {
            return Err(BrowserUpgradePolicyError::NonLoopbackListenAddress);
        }
        if listen_address.port() == 0 {
            return Err(BrowserUpgradePolicyError::ZeroListenPort);
        }

        let host = format_ip_literal(listen_address.ip());
        let authority = if listen_address.port() == scheme.default_port() {
            host
        } else {
            format!("{host}:{}", listen_address.port())
        };
        let origin = format!("{}://{authority}", scheme.as_str());
        Ok(Self { origin, authority })
    }

    pub(crate) fn canonical_origin(&self) -> &str {
        &self.origin
    }

    #[cfg(test)]
    pub(crate) fn canonical_authority(&self) -> &str {
        &self.authority
    }

    /// Validates the exact raw headers needed before upgrade. Missing,
    /// duplicate, or non-text values fail closed. WebSocket syntax checks and
    /// capacity accounting remain integration responsibilities.
    pub(crate) fn validate_upgrade_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<(), BrowserUpgradePolicyError> {
        let origin = exactly_one_text_header(
            headers,
            &ORIGIN,
            BrowserUpgradePolicyError::MissingOrigin,
            BrowserUpgradePolicyError::MultipleOriginHeaders,
            BrowserUpgradePolicyError::InvalidOriginHeader,
        )?;
        if origin == "null" {
            return Err(BrowserUpgradePolicyError::NullOrigin);
        }
        if origin != self.origin {
            return Err(BrowserUpgradePolicyError::OriginMismatch);
        }

        let host = exactly_one_text_header(
            headers,
            &HOST,
            BrowserUpgradePolicyError::MissingHost,
            BrowserUpgradePolicyError::MultipleHostHeaders,
            BrowserUpgradePolicyError::InvalidHostHeader,
        )?;
        if host != self.authority {
            return Err(BrowserUpgradePolicyError::HostMismatch);
        }

        let subprotocol = exactly_one_text_header(
            headers,
            &SEC_WEBSOCKET_PROTOCOL,
            BrowserUpgradePolicyError::MissingBrowserStreamSubprotocol,
            BrowserUpgradePolicyError::MultipleBrowserStreamSubprotocolHeaders,
            BrowserUpgradePolicyError::InvalidBrowserStreamSubprotocolHeader,
        )?;
        if subprotocol != BROWSER_STREAM_PROTOCOL {
            return Err(BrowserUpgradePolicyError::BrowserStreamSubprotocolMismatch);
        }
        Ok(())
    }
}

fn exactly_one_text_header<'a>(
    headers: &'a HeaderMap,
    name: &HeaderName,
    missing: BrowserUpgradePolicyError,
    multiple: BrowserUpgradePolicyError,
    invalid: BrowserUpgradePolicyError,
) -> Result<&'a str, BrowserUpgradePolicyError> {
    let mut values = headers.get_all(name).iter();
    let value = values.next().ok_or(missing)?;
    if values.next().is_some() {
        return Err(multiple);
    }
    value.to_str().map_err(|_| invalid)
}

fn format_ip_literal(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(ip) => ip.to_string(),
        IpAddr::V6(ip) => format!("[{ip}]"),
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(crate) enum BrowserUpgradePolicyError {
    #[error("browser stream listen address must be an exact loopback IP")]
    NonLoopbackListenAddress,
    #[error("browser stream listen address must have a bound port")]
    ZeroListenPort,
    #[error("browser stream upgrade is missing Origin")]
    MissingOrigin,
    #[error("browser stream upgrade has multiple Origin headers")]
    MultipleOriginHeaders,
    #[error("browser stream upgrade has a non-text Origin header")]
    InvalidOriginHeader,
    #[error("browser stream upgrade has an opaque Origin")]
    NullOrigin,
    #[error("browser stream Origin does not match the configured origin")]
    OriginMismatch,
    #[error("browser stream upgrade is missing Host")]
    MissingHost,
    #[error("browser stream upgrade has multiple Host headers")]
    MultipleHostHeaders,
    #[error("browser stream upgrade has a non-text Host header")]
    InvalidHostHeader,
    #[error("browser stream Host does not match the configured authority")]
    HostMismatch,
    #[error("browser stream upgrade is missing the required subprotocol")]
    MissingBrowserStreamSubprotocol,
    #[error("browser stream upgrade has multiple subprotocol headers")]
    MultipleBrowserStreamSubprotocolHeaders,
    #[error("browser stream upgrade has a non-text subprotocol header")]
    InvalidBrowserStreamSubprotocolHeader,
    #[error("browser stream upgrade requested an unsupported subprotocol")]
    BrowserStreamSubprotocolMismatch,
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use axum::http::{
        HeaderMap, HeaderValue,
        header::{HOST, ORIGIN, SEC_WEBSOCKET_PROTOCOL},
    };
    use serde_json::{Value, json};

    use super::{
        APPLICATION_FRAME_SEND_DEADLINE, BROWSER_STREAM_PATH, BROWSER_STREAM_PROTOCOL,
        BrowserOriginScheme, BrowserStreamCursor, BrowserStreamGrant,
        BrowserStreamGrantIssueRequest, BrowserStreamGrantIssueResponse, BrowserStreamPath,
        BrowserStreamProtocol, BrowserStreamRedemptionRequest, BrowserStreamServerFrame,
        BrowserUpgradePolicy, BrowserUpgradePolicyError, CREDENTIAL_REVALIDATION_INTERVAL,
        FIRST_FRAME_DEADLINE, MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL,
        MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON, MAX_PRE_AUTHENTICATION_SOCKETS_PER_DAEMON,
        MAX_REDEMPTION_FRAME_BYTES, MAX_UNUSED_GRANTS_PER_CREDENTIAL, MAX_UNUSED_GRANTS_PER_DAEMON,
        RedemptionFrameError, STREAM_GRANT_ENTROPY_BYTES, STREAM_GRANT_REDEMPTION_LIFETIME,
    };
    use fleetd_proto::model::Message;

    const VALID_GRANT: &str = "fl_sg_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    fn valid_upgrade_headers(origin: &'static str, host: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static(origin));
        headers.insert(HOST, HeaderValue::from_static(host));
        headers.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static(BROWSER_STREAM_PROTOCOL),
        );
        headers
    }

    #[test]
    fn protocol_constants_and_operational_bounds_are_exact() {
        assert_eq!(
            BrowserStreamProtocol::V1.as_str(),
            "fleetd.channel-stream.browser.v1"
        );
        assert_eq!(
            BrowserStreamPath::ChannelStream.as_str(),
            "/v1/browser/channel-stream"
        );
        assert_eq!(BROWSER_STREAM_PROTOCOL, BrowserStreamProtocol::V1.as_str());
        assert_eq!(
            BROWSER_STREAM_PATH,
            BrowserStreamPath::ChannelStream.as_str()
        );
        assert_eq!(STREAM_GRANT_ENTROPY_BYTES, 32);
        assert_eq!(STREAM_GRANT_REDEMPTION_LIFETIME.as_secs(), 15);
        assert_eq!(FIRST_FRAME_DEADLINE.as_secs(), 5);
        assert_eq!(MAX_REDEMPTION_FRAME_BYTES, 1_024);
        assert_eq!(MAX_UNUSED_GRANTS_PER_CREDENTIAL, 8);
        assert_eq!(MAX_UNUSED_GRANTS_PER_DAEMON, 1_024);
        assert_eq!(MAX_PRE_AUTHENTICATION_SOCKETS_PER_DAEMON, 64);
        assert_eq!(MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL, 16);
        assert_eq!(MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON, 1_024);
        assert_eq!(CREDENTIAL_REVALIDATION_INTERVAL.as_secs(), 30);
        assert_eq!(APPLICATION_FRAME_SEND_DEADLINE.as_secs(), 10);
    }

    #[test]
    fn grant_issue_request_is_closed_and_bounded() {
        let parsed: BrowserStreamGrantIssueRequest = serde_json::from_value(json!({
            "after": 42,
            "protocol": BROWSER_STREAM_PROTOCOL
        }))
        .expect("valid issue request");
        assert_eq!(parsed.after.get(), 42);
        assert_eq!(parsed.protocol, BrowserStreamProtocol::V1);

        for rejected in [
            json!({"after": -1, "protocol": BROWSER_STREAM_PROTOCOL}),
            json!({"after": 0, "protocol": "fleetd.channel-stream.browser.v2"}),
            json!({"after": 0, "protocol": BROWSER_STREAM_PROTOCOL, "extra": true}),
            json!({"protocol": BROWSER_STREAM_PROTOCOL}),
            json!({"after": 0}),
        ] {
            assert!(serde_json::from_value::<BrowserStreamGrantIssueRequest>(rejected).is_err());
        }
    }

    #[test]
    fn issuance_response_has_exact_constants_and_redacts_its_grant() {
        let response = BrowserStreamGrantIssueResponse {
            grant: BrowserStreamGrant::parse(VALID_GRANT).expect("valid grant"),
            expires_at_ms: 1_787_666_400_000,
            websocket_path: BrowserStreamPath::ChannelStream,
            protocol: BrowserStreamProtocol::V1,
        };
        assert_eq!(
            serde_json::to_value(&response).expect("serialize response"),
            json!({
                "grant": VALID_GRANT,
                "expires_at_ms": 1_787_666_400_000_i64,
                "websocket_path": BROWSER_STREAM_PATH,
                "protocol": BROWSER_STREAM_PROTOCOL
            })
        );
        assert_eq!(response.grant.expose_secret(), VALID_GRANT);
        let debug = format!("{response:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains(VALID_GRANT));
    }

    #[test]
    fn redemption_frame_is_closed_bounded_and_scope_free() {
        let valid = format!(r#"{{"type":"redeem","grant":"{VALID_GRANT}"}}"#);
        let parsed = BrowserStreamRedemptionRequest::parse_text_frame(&valid)
            .expect("valid redemption frame");
        assert_eq!(parsed.grant.expose_secret(), VALID_GRANT);

        for rejected in [
            json!({"type": "other", "grant": VALID_GRANT}),
            json!({"type": "redeem"}),
            json!({"type": "redeem", "grant": VALID_GRANT, "channel_id": "caller-scope"}),
            json!({"type": "redeem", "grant": VALID_GRANT, "after": 0}),
        ] {
            let frame = serde_json::to_string(&rejected).expect("serialize fixture");
            assert_eq!(
                BrowserStreamRedemptionRequest::parse_text_frame(&frame),
                Err(RedemptionFrameError::Invalid)
            );
        }

        let exact_limit = format!(
            "{}{}",
            " ".repeat(MAX_REDEMPTION_FRAME_BYTES - valid.len()),
            valid
        );
        assert!(BrowserStreamRedemptionRequest::parse_text_frame(&exact_limit).is_ok());
        let oversized = format!(" {exact_limit}");
        assert_eq!(oversized.len(), MAX_REDEMPTION_FRAME_BYTES + 1);
        assert_eq!(
            BrowserStreamRedemptionRequest::parse_text_frame(&oversized),
            Err(RedemptionFrameError::Oversized)
        );
    }

    #[test]
    fn grant_shape_is_exact_and_errors_never_echo_the_candidate() {
        assert!(BrowserStreamGrant::parse(VALID_GRANT).is_ok());
        for rejected in [
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "fl_sg_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "fl_sg_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "fl_sg_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "fl_sg_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA+",
            "fl_sg_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/",
            "fl_sg_éAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ] {
            let error = BrowserStreamGrant::parse(rejected).expect_err("invalid grant");
            assert_eq!(error.to_string(), "invalid browser stream grant shape");
            assert!(!error.to_string().contains(rejected));
        }
    }

    #[test]
    fn tagged_server_frames_are_exact_and_preserve_the_message() {
        let ready = BrowserStreamServerFrame::ready(
            "channel-id",
            BrowserStreamCursor::new(42).expect("valid cursor"),
        );
        assert_eq!(
            serde_json::to_value(&ready).expect("serialize ready"),
            json!({
                "type": "ready",
                "protocol": BROWSER_STREAM_PROTOCOL,
                "channel_id": "channel-id",
                "after": 42
            })
        );

        let message = Message {
            seq: 43,
            id: "message-id".to_owned(),
            channel_id: "channel-id".to_owned(),
            sender_id: "sender-id".to_owned(),
            recipient_id: None,
            kind: "unknown-contract/v7".to_owned(),
            payload: json!({"extension": {"nested": true}}),
            correlation_id: None,
            causation_id: None,
            created_at_ms: 1_787_666_400_001,
        };
        let frame = BrowserStreamServerFrame::message(message.clone());
        let encoded = serde_json::to_value(&frame).expect("serialize message frame");
        assert_eq!(encoded["type"], "message");
        assert_eq!(encoded["message"], serde_json::to_value(&message).unwrap());

        let mut unknown = serde_json::to_value(ready).expect("serialize ready fixture");
        unknown
            .as_object_mut()
            .expect("object frame")
            .insert("unexpected".to_owned(), Value::Bool(true));
        assert!(serde_json::from_value::<BrowserStreamServerFrame>(unknown).is_err());
    }

    #[test]
    fn ipv4_policy_accepts_only_exact_origin_host_and_protocol() {
        let policy = BrowserUpgradePolicy::from_listen_address(
            BrowserOriginScheme::Http,
            SocketAddr::from(([127, 0, 0, 1], 7419)),
        )
        .expect("loopback policy");
        assert_eq!(policy.canonical_origin(), "http://127.0.0.1:7419");
        assert_eq!(policy.canonical_authority(), "127.0.0.1:7419");
        let valid = valid_upgrade_headers("http://127.0.0.1:7419", "127.0.0.1:7419");
        assert_eq!(policy.validate_upgrade_headers(&valid), Ok(()));

        let mut missing_origin = valid.clone();
        missing_origin.remove(ORIGIN);
        assert_eq!(
            policy.validate_upgrade_headers(&missing_origin),
            Err(BrowserUpgradePolicyError::MissingOrigin)
        );

        let mut null_origin = valid.clone();
        null_origin.insert(ORIGIN, HeaderValue::from_static("null"));
        assert_eq!(
            policy.validate_upgrade_headers(&null_origin),
            Err(BrowserUpgradePolicyError::NullOrigin)
        );

        let mut alias_origin = valid.clone();
        alias_origin.insert(ORIGIN, HeaderValue::from_static("http://localhost:7419"));
        assert_eq!(
            policy.validate_upgrade_headers(&alias_origin),
            Err(BrowserUpgradePolicyError::OriginMismatch)
        );

        let mut wildcard_origin = valid.clone();
        wildcard_origin.insert(ORIGIN, HeaderValue::from_static("*"));
        assert_eq!(
            policy.validate_upgrade_headers(&wildcard_origin),
            Err(BrowserUpgradePolicyError::OriginMismatch)
        );

        let mut missing_host = valid.clone();
        missing_host.remove(HOST);
        assert_eq!(
            policy.validate_upgrade_headers(&missing_host),
            Err(BrowserUpgradePolicyError::MissingHost)
        );

        let mut alias_host = valid.clone();
        alias_host.insert(HOST, HeaderValue::from_static("localhost:7419"));
        assert_eq!(
            policy.validate_upgrade_headers(&alias_host),
            Err(BrowserUpgradePolicyError::HostMismatch)
        );

        let mut missing_subprotocol = valid.clone();
        missing_subprotocol.remove(SEC_WEBSOCKET_PROTOCOL);
        assert_eq!(
            policy.validate_upgrade_headers(&missing_subprotocol),
            Err(BrowserUpgradePolicyError::MissingBrowserStreamSubprotocol)
        );

        let mut multiple_subprotocols = valid.clone();
        multiple_subprotocols.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("fleetd.channel-stream.browser.v1, other"),
        );
        assert_eq!(
            policy.validate_upgrade_headers(&multiple_subprotocols),
            Err(BrowserUpgradePolicyError::BrowserStreamSubprotocolMismatch)
        );
    }

    #[test]
    fn aliases_and_caller_canonicalization_are_rejected() {
        let policy = BrowserUpgradePolicy::from_listen_address(
            BrowserOriginScheme::Http,
            SocketAddr::from(([127, 0, 0, 1], 7419)),
        )
        .expect("loopback policy");
        for origin in [
            "HTTP://127.0.0.1:7419",
            "http://127.0.0.1:7419/",
            "http://127.000.000.001:7419",
            "http://127.0.0.1.evil:7419",
            "http://0.0.0.0:7419",
            "http://[::1]:7419",
        ] {
            let headers = valid_upgrade_headers(origin, "127.0.0.1:7419");
            assert_eq!(
                policy.validate_upgrade_headers(&headers),
                Err(BrowserUpgradePolicyError::OriginMismatch)
            );
        }
    }

    #[test]
    fn duplicate_and_non_text_upgrade_headers_fail_closed() {
        let policy = BrowserUpgradePolicy::from_listen_address(
            BrowserOriginScheme::Http,
            SocketAddr::from(([127, 0, 0, 1], 7419)),
        )
        .expect("loopback policy");
        let valid = valid_upgrade_headers("http://127.0.0.1:7419", "127.0.0.1:7419");
        let cases = [
            (
                ORIGIN,
                BrowserUpgradePolicyError::MultipleOriginHeaders,
                BrowserUpgradePolicyError::InvalidOriginHeader,
            ),
            (
                HOST,
                BrowserUpgradePolicyError::MultipleHostHeaders,
                BrowserUpgradePolicyError::InvalidHostHeader,
            ),
            (
                SEC_WEBSOCKET_PROTOCOL,
                BrowserUpgradePolicyError::MultipleBrowserStreamSubprotocolHeaders,
                BrowserUpgradePolicyError::InvalidBrowserStreamSubprotocolHeader,
            ),
        ];

        for (name, multiple_error, invalid_error) in cases {
            let mut duplicate = valid.clone();
            duplicate.append(name.clone(), HeaderValue::from_static("duplicate"));
            assert_eq!(
                policy.validate_upgrade_headers(&duplicate),
                Err(multiple_error)
            );

            let mut invalid = valid.clone();
            invalid.insert(
                name,
                HeaderValue::from_bytes(b"\xff").expect("opaque header fixture"),
            );
            assert_eq!(
                policy.validate_upgrade_headers(&invalid),
                Err(invalid_error)
            );
        }
    }

    #[test]
    fn ipv6_and_default_ports_use_browser_canonical_authorities() {
        let ipv6 = BrowserUpgradePolicy::from_listen_address(
            BrowserOriginScheme::Http,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8443),
        )
        .expect("IPv6 loopback policy");
        assert_eq!(ipv6.canonical_origin(), "http://[::1]:8443");
        assert_eq!(ipv6.canonical_authority(), "[::1]:8443");
        let ipv6_headers = valid_upgrade_headers("http://[::1]:8443", "[::1]:8443");
        assert!(ipv6.validate_upgrade_headers(&ipv6_headers).is_ok());

        let default_http = BrowserUpgradePolicy::from_listen_address(
            BrowserOriginScheme::Http,
            SocketAddr::from(([127, 0, 0, 1], 80)),
        )
        .expect("default HTTP policy");
        assert_eq!(default_http.canonical_origin(), "http://127.0.0.1");
        assert_eq!(default_http.canonical_authority(), "127.0.0.1");
    }

    #[test]
    fn policy_derivation_rejects_non_loopback_and_unbound_addresses() {
        let non_loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7419);
        assert_eq!(
            BrowserUpgradePolicy::from_listen_address(BrowserOriginScheme::Http, non_loopback),
            Err(BrowserUpgradePolicyError::NonLoopbackListenAddress)
        );

        let unbound = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
        assert_eq!(
            BrowserUpgradePolicy::from_listen_address(BrowserOriginScheme::Http, unbound),
            Err(BrowserUpgradePolicyError::ZeroListenPort)
        );
    }
}
