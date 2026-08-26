//! Static adapter for the reusable human-to-agent conversation client.

use axum::{
    Router,
    response::{Redirect, Response},
    routing::get,
};

use crate::{api::AppState, web_surface::asset};

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
        include_str!("../web/conversation/index.html"),
    )
}

async fn styles() -> Response {
    asset(
        "text/css; charset=utf-8",
        include_str!("../web/conversation/conversation.css"),
    )
}

async fn script() -> Response {
    asset(
        "text/javascript; charset=utf-8",
        include_str!("../web/conversation/conversation.js"),
    )
}

async fn contract() -> Response {
    asset(
        "application/json",
        include_str!("../web/conversation/contract.json"),
    )
}
