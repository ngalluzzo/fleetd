//! Out-of-process plugin lifecycle and operational interface negotiation.
//!
//! The host launches an absolute executable without a shell, clears its
//! environment, bounds frames and deadlines, negotiates exact identity and
//! interfaces, and terminates the complete process group. It owns no durable
//! state: what a turn means, and what to record about it, belong to the caller.

mod harness_acp;
mod protocol;
mod rpc;
mod supervisor;

pub use harness_acp::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, CloseSession, CloseSessionResult,
    DescribeResult, DriverIdentity, EffectiveEnforcement, ExecutionFence, HarnessAcpClient,
    HarnessAcpNotification, HarnessExecutionCertainty, HarnessLimits, OpenSession, OpenSessionMode,
    OpenSessionResult, PermissionOutcome, PermissionRequested, PermissionResolution, PromptBlock,
    ResolvedMcpEndpoint, ResolvedMcpGrant, ResolvedMcpHttpHeader, RuntimeIdentity,
    SessionPersistence, StartTurn, StartTurnResult, ToolBudget, TurnEvent, TurnPolicy, TurnSource,
    TurnTerminal, interface as harness_acp_interface,
};
pub use protocol::{PluginIdentity, PluginInterface, PluginManifest, PluginNotification};
pub use supervisor::{PluginError, PluginExit, PluginProcess, PluginSpec, ShutdownOutcome};
