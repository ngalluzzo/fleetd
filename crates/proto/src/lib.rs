//! Wire types for every fleetd boundary.
//!
//! This crate is what a harness vendor, an external tool, or a generated client
//! compiles instead of the daemon. It describes the exact frames that cross a
//! boundary and nothing else: no persistence, no transport, no runtime.
//!
//! Modules are addressed by path rather than re-exported at the root, so a
//! consumer's imports say which boundary they depend on.

pub mod error;
pub mod harness_acp;
pub mod inference_openai;
pub mod model;
pub mod operations;
pub mod plugin;
pub mod session;
pub mod trigger;
