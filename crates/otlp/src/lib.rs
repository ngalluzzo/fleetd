//! OpenTelemetry egress for trajectory the durable record does not keep.
//!
//! One mechanism, the way `http` and `mcp` are one mechanism each. ADR 0028
//! splits observability into two sinks with different loss tolerance: the
//! durable evidence rows are projected by an external collector tailing their
//! public cursors, and the in-flight harness trajectory -- reasoning, tool
//! arguments, intermediate plans -- is exported from here, lossily, because it
//! exists nowhere else.
//!
//! Nothing in this crate is evidence. It implements
//! [`fleetd_execution::trajectory::TrajectorySink`], so the dependency points
//! this way: deciding what happens to durable state must not mean knowing that
//! an exporter exists.
//!
//! This is where the OpenTelemetry crates are allowed to be. Their traces
//! signal and OTLP trace exporter are still Beta and every crate is pre-1.0, so
//! confining them to a leaf crate is what keeps a breaking minor release away
//! from the daemon.

pub mod config;
mod projection;
pub mod sink;
