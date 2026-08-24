//! Out-of-process plugin lifecycle and capability negotiation.

mod protocol;
mod rpc;
mod supervisor;

pub use protocol::{Capability, PluginIdentity, PluginManifest, PluginNotification};
pub use supervisor::{PluginError, PluginExit, PluginProcess, PluginSpec, ShutdownOutcome};
