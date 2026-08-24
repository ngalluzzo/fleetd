//! Static adapter from the exact Fleetd web target contract to a browser UI.

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, HeaderValue, header},
    response::{Redirect, Response},
    routing::get,
};

use crate::AppState;

const CONTENT_SECURITY_POLICY: &str = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";

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
        include_str!("../web/operator/index.html"),
    )
}

async fn styles() -> Response {
    asset(
        "text/css; charset=utf-8",
        include_str!("../web/operator/operator.css"),
    )
}

async fn script() -> Response {
    asset(
        "text/javascript; charset=utf-8",
        include_str!("../web/operator/operator.js"),
    )
}

async fn contract() -> Response {
    asset(
        "application/json",
        include_str!("../web/operator/contract.json"),
    )
}

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
