//! Durable, local-first coordination primitives for cooperating software agents.
//!
//! The directory tree is the architecture. Below this crate sit `fleetd-proto`
//! (wire types), `fleetd-kernel` (the six concepts and the only connection
//! pool), and `fleetd-plugin-host` (process lifecycle), each re-exported here
//! under the name it is known by. Above them, [`execution`] decides what
//! happens to durable state and [`http`] exposes it.
//!
//! Types are reached through the module that owns them — `model::Message`,
//! `store::Store`, `execution::worker::WorkerReport`. There is deliberately no
//! flat re-export at the crate root: an import should say which boundary it
//! depends on, and adding a type should not mean editing a list every other
//! change also edits.

pub use fleetd_kernel::{auth, delivery, error, message_commit_hint, store};
pub use fleetd_plugin_host as plugin;
pub use fleetd_proto::model;

pub mod execution;
pub mod http;
