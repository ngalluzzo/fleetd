//! Durable, local-first coordination primitives for cooperating software agents.
//!
//! Types are reached through the module that owns them — `model::Message`,
//! `store::Store`, `plugin::PluginProcess`. There is deliberately no flat
//! re-export at the crate root: an import should say which boundary it depends
//! on, and adding a type should not mean editing a list every other change
//! also edits.

pub use fleetd_plugin_host as plugin;
pub use fleetd_proto::model;

pub mod api;
pub mod auth;
pub mod browser_stream_edge;
mod channel_stream;
pub mod controller;
mod conversation_surface;
mod delivery;
pub mod error;
pub mod invocation;
mod message_commit_hint;
pub mod message_grant_broker;
pub mod operations;
mod operator_surface;
pub mod session_binding;
pub mod settlement;
pub mod store;
mod stream_grant_broker;
mod web_surface;
pub mod worker;
