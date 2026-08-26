//! Everything the daemon does above the kernel.
//!
//! The kernel stores agents, channels, membership, messages, and deliveries.
//! This layer decides what happens to them: which delivery a worker reserves,
//! how a turn is fenced before dispatch, which harness session owns it, and
//! what bounded evidence is kept afterwards.
//!
//! It composes over `&Store` and never extends it, so the direction of the
//! dependency is visible at every call site.

pub mod controller;
pub mod invocation;
pub mod message_grant_broker;
pub mod operations;
pub mod session_binding;
pub mod settlement;
pub mod worker;
