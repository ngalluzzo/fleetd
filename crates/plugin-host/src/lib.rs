//! Out-of-process plugin lifecycle and operational interface negotiation.
//!
//! The host launches an absolute executable without a shell, clears its
//! environment, bounds frames and deadlines, negotiates exact identity and
//! interfaces, and terminates the complete process group. It owns no durable
//! state: what a turn or backend means, and what to record about it, belong to
//! the caller.

mod harness_acp;
mod inference_openai;
mod protocol;
mod rpc;
mod sandbox;
mod supervisor;

pub use harness_acp::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, CloseSession, CloseSessionResult,
    DescribeResult, DriverIdentity, EffectiveEnforcement, ExecutionFence, HarnessAcpClient,
    HarnessAcpNotification, HarnessExecutionCertainty, HarnessLimits, OpenSession, OpenSessionMode,
    OpenSessionResult, PermissionOutcome, PermissionRequested, PermissionResolution, PromptBlock,
    ResolvedMcpEndpoint, ResolvedMcpGrant, ResolvedMcpHttpHeader, RuntimeIdentity,
    SessionPersistence, StartTranscript, StartTranscriptResult, StartTurn, StartTurnResult,
    ToolBudget, TranscriptComplete, TranscriptEntry, TurnEvent, TurnPolicy, TurnSource,
    TurnTerminal, interface as harness_acp_interface,
};
pub use inference_openai::{
    BackendIdentity as InferenceBackendIdentity, DescribeResult as InferenceDescribeResult,
    Endpoint as InferenceEndpoint, InferenceOpenAiClient, ModelRoute as InferenceModelRoute,
    ObserverEndpoint as InferenceObserverEndpoint, interface as inference_openai_interface,
};
pub use protocol::{PluginIdentity, PluginInterface, PluginManifest, PluginNotification};
pub use sandbox::{MacOsSeatbeltPosture, MacOsSeatbeltSandbox, SandboxNetwork};
pub use supervisor::{PluginError, PluginExit, PluginProcess, PluginSpec, ShutdownOutcome};
