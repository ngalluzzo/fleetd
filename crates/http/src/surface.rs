//! Static presentation adapters.
//!
//! Each target is one checked-in artifact served from the same origin as the
//! API it calls. They introduce no product semantics into the daemon: a target
//! is HTML, CSS, a script, and the exact contract it was built against.

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, HeaderValue, header},
    response::{Redirect, Response},
    routing::get,
};

use super::AppState;

const CONTENT_SECURITY_POLICY: &str = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

fn asset(media_type: &'static str, content: &'static str) -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(media_type));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    let mut response = Response::new(Body::from(content));
    *response.headers_mut() = headers;
    response
}

/// The operator target: blocked-work resolution over public endpoints.
pub mod operator {
    use super::{AppState, Redirect, Response, Router, asset, get};

    pub fn routes() -> Router<AppState> {
        Router::new()
            .route(
                "/operator",
                get(|| async { Redirect::permanent("/operator/") }),
            )
            .route("/operator/", get(index))
            .route("/operator/operator.css", get(styles))
            .route("/operator/operator.js", get(script))
            .route("/operator/contract.json", get(contract))
    }

    async fn index() -> Response {
        asset(
            "text/html; charset=utf-8",
            include_str!("../../../web/operator/index.html"),
        )
    }

    async fn styles() -> Response {
        asset(
            "text/css; charset=utf-8",
            include_str!("../../../web/operator/operator.css"),
        )
    }

    async fn script() -> Response {
        asset(
            "text/javascript; charset=utf-8",
            include_str!("../../../web/operator/operator.js"),
        )
    }

    async fn contract() -> Response {
        asset(
            "application/json",
            include_str!("../../../web/operator/contract.json"),
        )
    }
}

/// The conversation target: the reusable human-to-agent client.
pub mod conversation {
    use super::{AppState, Redirect, Response, Router, asset, get};

    pub fn routes() -> Router<AppState> {
        Router::new()
            .route(
                "/conversation",
                get(|| async { Redirect::permanent("/conversation/") }),
            )
            .route("/conversation/", get(index))
            .route("/conversation/conversation.css", get(styles))
            .route("/conversation/conversation.js", get(script))
            .route("/conversation/contract.json", get(contract))
    }

    async fn index() -> Response {
        asset(
            "text/html; charset=utf-8",
            include_str!("../../../web/conversation/index.html"),
        )
    }

    async fn styles() -> Response {
        asset(
            "text/css; charset=utf-8",
            include_str!("../../../web/conversation/conversation.css"),
        )
    }

    async fn script() -> Response {
        asset(
            "text/javascript; charset=utf-8",
            include_str!("../../../web/conversation/conversation.js"),
        )
    }

    async fn contract() -> Response {
        asset(
            "application/json",
            include_str!("../../../web/conversation/contract.json"),
        )
    }
}
