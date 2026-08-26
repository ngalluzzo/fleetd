//! The opening moves every integration suite shares.
//!
//! Each file under `tests/` is its own crate, so anything they have in common
//! has to live in a module they each include. Every suite that needs durable
//! state starts the same way: a temporary directory, a migrated store inside
//! it, an operator credential on disk, a loopback listener, a served router.
//! Those steps are what this module owns. What a suite asserts afterwards, and
//! the vocabulary it wraps around these primitives, stays with the suite.

// Cargo compiles this whole module into every test binary that includes it, so
// a helper only one suite needs looks unused from all the others.
#![allow(dead_code)]

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
};

use fleetd::{
    auth::AuthService,
    http::{AppState, router},
    store::Store,
};
use tokio::{net::TcpListener, task::JoinHandle};

/// A temporary directory and the store opened inside it.
///
/// Hold the whole value for as long as the store is needed: dropping it removes
/// the directory. `database_path` is exposed because reopening the same file is
/// how the suites test that state survives a restart.
pub struct TempStore {
    pub directory: tempfile::TempDir,
    pub database_path: PathBuf,
    pub store: Store,
}

/// Opens a freshly migrated store in a new temporary directory.
pub async fn temp_store() -> TempStore {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database_path = directory.path().join("fleetd.db");
    let store = Store::open(&database_path).await.expect("open store");
    TempStore {
        directory,
        database_path,
        store,
    }
}

/// Provisions the operator credential and returns the exact token from disk.
///
/// The file is authoritative, so the token is read back rather than assumed:
/// this is the same path the daemon takes on startup.
pub async fn bootstrap_operator(store: &Store, directory: &Path) -> String {
    let token_path = directory.join("operator.token");
    AuthService::new(store.clone())
        .ensure_operator_credential(&token_path)
        .await
        .expect("bootstrap operator");
    std::fs::read_to_string(&token_path)
        .expect("read operator token")
        .trim()
        .to_owned()
}

/// Binds a listener on an arbitrary free loopback port.
///
/// Binding is separate from serving because some state has to be built around
/// the address it will be reached on -- the browser stream edge is configured
/// with its own origin -- so the address must be known before the router is.
pub async fn bind_loopback() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let address = listener.local_addr().expect("bound listener address");
    (listener, address)
}

/// Serves one router on an already-bound listener.
pub fn spawn_server(listener: TcpListener, state: AppState) -> JoinHandle<()> {
    tokio::spawn(async move {
        axum::serve(listener, router(state))
            .await
            .expect("serve API");
    })
}

/// Binds and serves state that does not need to know its own address.
pub async fn serve(state: AppState) -> (SocketAddr, JoinHandle<()>) {
    let (listener, address) = bind_loopback().await;
    (address, spawn_server(listener, state))
}
