//! Everything the daemon does above the kernel.
//!
//! The kernel stores agents, channels, membership, messages, and deliveries.
//! This layer decides what happens to them: which delivery a worker reserves,
//! how a turn is fenced before dispatch, which harness session owns it, and
//! what bounded evidence is kept afterwards.
//!
//! It composes over `&Store` and never extends it, so the direction of the
//! dependency is visible at every call site. The orphan rule now enforces that:
//! `Store` belongs to another crate, so a method on it cannot be written here.
//!
//! Nothing in this crate exposes anything. A surface provisions transports and
//! hands the worker a [`worker::TurnGrant`], which is why no web framework
//! appears in this crate's dependencies.

pub mod controller;
pub mod health;
pub mod invocation;
pub mod message_grant;
pub mod operations;
pub mod session_binding;
pub mod settlement;
pub mod trajectory;
pub mod trigger;
pub mod worker;
