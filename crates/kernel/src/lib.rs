//! The fleetd kernel: the six concepts every other layer is built from.
//!
//! An **agent** is an addressable participant, a **channel** is a durable
//! bounded conversation, **membership** is permission to send or receive in
//! one, a **message** is an immutable envelope in a globally ordered sequence,
//! a **delivery** is a recipient snapshot and its durable processing state, and
//! a **principal** is an operator or one authenticated agent identity.
//!
//! The kernel does not know what a harness, a task, a workflow, or a semantic
//! capability is. It owns the authoritative `SQLite` store and every transition
//! its own rows can make, and it commits none of them on another layer's
//! behalf: [`Store::begin_immediate`] lets a caller enlist its own work in a
//! kernel transaction so both commit together.

pub mod auth;
pub mod delivery;
pub mod error;
pub mod message_commit_hint;
pub mod store;
