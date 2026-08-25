//! Static adapter from the exact Fleetd web target contract to a browser UI.

use axum::{
    Router,
    response::{Redirect, Response},
    routing::get,
};

use crate::{AppState, web_surface::asset};

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
