//! Out-of-process plugin lifecycle and capability negotiation.

mod harness_acp;
mod protocol;
mod rpc;
mod supervisor;

pub use harness_acp::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, CloseSession, CloseSessionResult,
    DescribeResult, DriverIdentity, EffectiveEnforcement, ExecutionFence, HarnessAcpClient,
    HarnessAcpNotification, HarnessExecutionCertainty, HarnessLimits, OpenSession, OpenSessionMode,
    OpenSessionResult, PermissionOutcome, PermissionRequested, PermissionResolution, PromptBlock,
    RuntimeIdentity, SessionPersistence, StartTurn, StartTurnResult, ToolBudget, TurnEvent,
    TurnPolicy, TurnSource, TurnTerminal, capability as harness_acp_capability,
};
pub use protocol::{Capability, PluginIdentity, PluginManifest, PluginNotification};
pub use supervisor::{PluginError, PluginExit, PluginProcess, PluginSpec, ShutdownOutcome};
