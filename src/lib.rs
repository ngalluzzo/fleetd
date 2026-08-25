//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
pub mod auth;
pub mod controller;
mod delivery;
pub mod error;
mod invocation;
pub mod message_grant_broker;
pub mod model;
pub mod operations;
mod operator_surface;
pub mod plugin;
pub mod session_binding;
pub mod store;
pub mod worker;

pub use api::{AppState, openapi_document, router};
pub use auth::{AuthService, OperatorBootstrap, Principal};
pub use controller::{
    ManagedHarnessController, ManagedTurn, ManagedTurnError, ManagedTurnGrant, ManagedTurnOutcome,
    TurnResultCapture,
};
pub use error::{ErrorResponse, FleetError};
pub use message_grant_broker::{
    MessageGrantBroker, MessageGrantBrokerError, PUBLISH_DURABLE_MESSAGE_GRANT,
};
pub use model::{
    AckDelivery, AddMember, Agent, ArmInvocation, BlockDelivery, BlockResolution, BlockedDelivery,
    Channel, ChannelMember, ClaimBatch, ClaimDeliveries, CompleteInvocation, CreateAgent,
    CreateChannel, CreateChannelMember, CreateMessage, Delivery, ExecutionCertainty, Invocation,
    InvocationBatch, InvocationCompletion, InvocationState, IssuedCredential,
    MembershipDeliveryMode, Message, MessagePage, RegisteredAgent, ResolveDeliveryBlock,
    RetryDelivery, SendMessage,
};
pub use operations::{
    InvocationEventCounts, InvocationObservation, NewPluginGeneration, ObservedPluginInterface,
    PluginGeneration, PluginGenerationDisposition, PluginGenerationHealth, PluginGenerationState,
    PluginShutdownOutcome, StopPluginGeneration,
};
pub use plugin::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, CloseSession, CloseSessionResult,
    DescribeResult, DriverIdentity, EffectiveEnforcement, ExecutionFence, HarnessAcpClient,
    HarnessAcpNotification, HarnessExecutionCertainty, HarnessLimits, OpenSession, OpenSessionMode,
    OpenSessionResult, PermissionOutcome, PermissionRequested, PermissionResolution, PluginError,
    PluginExit, PluginIdentity, PluginInterface, PluginManifest, PluginNotification, PluginProcess,
    PluginSpec, PromptBlock, ResolvedMcpEndpoint, ResolvedMcpGrant, ResolvedMcpHttpHeader,
    RuntimeIdentity, SessionPersistence, ShutdownOutcome, StartTurn, StartTurnResult, ToolBudget,
    TurnEvent, TurnPolicy, TurnSource, TurnTerminal, harness_acp_interface,
};
pub use session_binding::{
    AcquireSessionBinding, BoundInvocation, SessionAcquisition, SessionAcquisitionMode,
    SessionBinding, SessionBindingState,
};
pub use store::{AppendMessageResult, Store};
pub use worker::{
    ContinuousHarnessWorker, ContinuousWorkerConfig, ContinuousWorkerError, EnvelopeTurnAdapter,
    InboundAcceptance, PreparedTurn, TurnAdapter, WorkerReport,
};
