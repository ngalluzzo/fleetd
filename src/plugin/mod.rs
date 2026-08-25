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
    ResolvedMcpEndpoint, ResolvedMcpGrant, ResolvedMcpHttpHeader, RuntimeIdentity,
    SessionPersistence, StartTurn, StartTurnResult, ToolBudget, TurnEvent, TurnPolicy, TurnSource,
    TurnTerminal, capabilities as harness_acp_capabilities, offer_set as harness_acp_offer_set,
};
pub use protocol::{PluginIdentity, PluginManifest, PluginNotification};
pub use supervisor::{PluginError, PluginExit, PluginProcess, PluginSpec, ShutdownOutcome};
