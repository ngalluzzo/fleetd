//! The versioned HTTP and WebSocket contract.
//!
//! This module owns composition only: shared state, the route graph, the
//! generated document, and the authentication layer every protected route sits
//! behind. One module per domain owns its own handlers and declares its own
//! routes, so two domains can change without touching the same file.

use axum::{
    Router,
    extract::{Request, State},
    http::header::AUTHORIZATION,
    middleware::{self, Next},
    response::Response,
};
use tokio::sync::broadcast;
use utoipa::{Modify, OpenApi};
use utoipa_axum::router::OpenApiRouter;

use crate::{browser_stream_edge::BrowserStreamEdgeState, stream_grant_broker::StreamGrantBroker};
use fleetd_kernel::{
    auth::AuthService,
    error::FleetError,
    message_commit_hint::{MessageCommitHintBridge, MessageCommitWake},
    store::Store,
};

use error::ApiError;

/// Declares the authenticated route domains, once.
///
/// A domain has to be both declared as a module and merged into the contract,
/// and keeping those in two lists is exactly how a domain ends up declared but
/// unreachable. This expands one list into both, so adding a domain is a
/// one-line change and the two can never disagree.
///
/// The order is significant: it fixes the order operations are registered in,
/// and therefore the order they appear in the generated document. Append rather
/// than insert, or the contract churns for every reader.
macro_rules! route_domains {
    ($($domain:ident),+ $(,)?) => {
        $(mod $domain;)+

        fn protected_contract() -> OpenApiRouter<AppState> {
            OpenApiRouter::default()$(.merge($domain::routes()))+
        }
    };
}

route_domains!(
    agents,
    channels,
    messages,
    streams,
    deliveries,
    invocations,
    operations,
);

// Shared by those domains.
pub mod error;
mod guard;
mod meta;

// Transport beneath the routes.
pub mod browser_stream_edge;
mod channel_stream;
mod stream_grant_broker;
mod surface;

const BEARER_AUTH: &str = "bearerAuth";

#[derive(OpenApi)]
#[openapi(
    info(
        title = "fleetd API",
        version = "1.5.0",
        description = "Versioned control-plane contract for cooperating software agents."
    ),
    tags(
        (name = "system", description = "Process health and API discovery"),
        (name = "agents", description = "Agent identity and credential administration"),
        (name = "channels", description = "Channel membership and durable messaging"),
        (name = "deliveries", description = "Leased agent inbox delivery"),
        (name = "invocations", description = "Crash-safe managed invocation fencing"),
        (name = "operations", description = "Operator-visible worker and harness evidence")
    ),
    modifiers(&SecurityAddon)
)]
struct ApiDoc;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        use utoipa::openapi::security::{Http, HttpAuthScheme, SecurityScheme};

        openapi
            .components
            .get_or_insert_with(Default::default)
            .add_security_scheme(
                BEARER_AUTH,
                SecurityScheme::Http(Http::new(HttpAuthScheme::Bearer)),
            );
    }
}

/// Shared dependencies for the HTTP and WebSocket interfaces.
pub struct AppState {
    store: Store,
    auth: AuthService,
    messages: broadcast::Sender<MessageCommitWake>,
    stream_grants: StreamGrantBroker,
    browser_stream: Option<BrowserStreamEdgeState>,
    message_commit_hints: Option<std::sync::Arc<MessageCommitHintBridge>>,
}

impl Clone for AppState {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            auth: self.auth.clone(),
            messages: self.messages.clone(),
            stream_grants: self.stream_grants.clone(),
            browser_stream: self.browser_stream.clone(),
            message_commit_hints: self.message_commit_hints.clone(),
        }
    }
}

impl AppState {
    /// Creates application state over the supplied durable store.
    #[must_use]
    pub fn new(store: Store) -> Self {
        let (messages, _) = broadcast::channel(1_024);
        let auth = AuthService::new(store.clone());
        Self {
            stream_grants: StreamGrantBroker::new(auth.clone()),
            auth,
            store,
            messages,
            browser_stream: None,
            message_commit_hints: None,
        }
    }

    /// Enables content-free wakeups for durable message commits made by local
    /// writer processes using the same database.
    ///
    /// # Errors
    ///
    /// Returns an error when the private local datagram cannot be bound or is
    /// already owned by another daemon for the same database.
    pub fn with_external_message_commit_hints(
        mut self,
        database_path: impl AsRef<std::path::Path>,
    ) -> Result<Self, FleetError> {
        self.message_commit_hints = Some(std::sync::Arc::new(MessageCommitHintBridge::bind(
            database_path.as_ref(),
            self.messages.clone(),
        )?));
        Ok(self)
    }

    /// Enables the origin-bound browser stream edge for the exact bound HTTP
    /// listener. Call this only after an ephemeral port has been resolved.
    ///
    /// # Errors
    ///
    /// Returns an error for an unbound or non-loopback address.
    pub fn with_browser_stream_listener(
        mut self,
        listen_address: std::net::SocketAddr,
    ) -> Result<Self, FleetError> {
        self.browser_stream = Some(
            BrowserStreamEdgeState::for_http_listener(listen_address)
                .map_err(|error| FleetError::Invalid(error.to_string()))?,
        );
        Ok(self)
    }

    /// Returns the exact browser origin accepted by this daemon, when enabled.
    #[must_use]
    pub fn browser_origin(&self) -> Option<&str> {
        self.browser_stream
            .as_ref()
            .map(BrowserStreamEdgeState::canonical_origin)
    }
}

/// Builds fleetd's versioned API.
pub fn router(state: AppState) -> Router {
    let protected: Router<AppState> = protected_contract().into();
    let protected =
        protected.route_layer(middleware::from_fn_with_state(state.clone(), authenticate));
    let public: Router<AppState> = public_contract().into();
    let browser: Router<AppState> = browser_contract().into();
    public
        .merge(surface::operator::routes())
        .merge(surface::conversation::routes())
        .merge(browser)
        .merge(protected)
        .with_state(state)
}

/// Returns the exact `OpenAPI` document collected from the registered handlers.
#[must_use]
pub fn openapi_document() -> utoipa::openapi::OpenApi {
    public_contract()
        .merge(browser_contract())
        .merge(protected_contract())
        .into_openapi()
}

fn public_contract() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi()).merge(meta::routes())
}

fn browser_contract() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(browser_stream_edge::Schemas::openapi())
        .merge(streams::browser_routes())
}

async fn authenticate(
    State(state): State<AppState>,
    mut request: Request,
    next: Next,
) -> Result<Response, ApiError> {
    let header = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(FleetError::Unauthorized)?;
    let token = parse_bearer_token(header).ok_or(FleetError::Unauthorized)?;
    let principal = state.auth.authenticate(token).await?;
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

fn parse_bearer_token(header: &str) -> Option<&str> {
    let (scheme, token) = header.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer")
        || token.is_empty()
        || token.chars().any(char::is_whitespace)
    {
        return None;
    }
    Some(token)
}
