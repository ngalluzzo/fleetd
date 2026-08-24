//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
pub mod auth;
pub mod controller;
mod delivery;
pub mod error;
mod invocation;
pub mod model;
pub mod plugin;
pub mod session_binding;
pub mod store;

pub use api::{AppState, openapi_document, router};
pub use auth::{AuthService, OperatorBootstrap, Principal};
pub use controller::{ManagedHarnessController, ManagedTurn, ManagedTurnError, ManagedTurnOutcome};
pub use error::{ErrorResponse, FleetError};
pub use model::{
    AckDelivery, AddMember, Agent, ArmInvocation, BlockDelivery, BlockResolution, BlockedDelivery,
    Channel, ClaimBatch, ClaimDeliveries, CompleteInvocation, CreateAgent, CreateChannel,
    CreateMessage, Delivery, ExecutionCertainty, Invocation, InvocationBatch, InvocationCompletion,
    InvocationState, IssuedCredential, Message, MessagePage, RegisteredAgent, ResolveDeliveryBlock,
    RetryDelivery, SendMessage,
};
pub use plugin::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, Capability, CloseSession,
    CloseSessionResult, DescribeResult, DriverIdentity, EffectiveEnforcement, ExecutionFence,
    HarnessAcpClient, HarnessAcpNotification, HarnessExecutionCertainty, HarnessLimits,
    OpenSession, OpenSessionMode, OpenSessionResult, PermissionOutcome, PermissionRequested,
    PermissionResolution, PluginError, PluginExit, PluginIdentity, PluginManifest,
    PluginNotification, PluginProcess, PluginSpec, PromptBlock, RuntimeIdentity,
    SessionPersistence, ShutdownOutcome, StartTurn, StartTurnResult, ToolBudget, TurnEvent,
    TurnPolicy, TurnSource, TurnTerminal, harness_acp_capability,
};
pub use session_binding::{
    AcquireSessionBinding, BoundInvocation, SessionAcquisition, SessionAcquisitionMode,
    SessionBinding, SessionBindingState,
};
pub use store::{AppendMessageResult, Store};
