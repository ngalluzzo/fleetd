//! Draft author-review workflow and its external Fleetd runner.
//!
//! This package deliberately keeps the first workflow interface coupled to one
//! real integration. It is dogfood, not a stabilized workflow SDK.

pub mod plugin;
pub mod protocol;
pub mod runner;
