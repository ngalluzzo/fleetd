//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
pub mod auth;
pub mod capability_broker;
pub mod controller;
mod delivery;
pub mod error;
pub mod gooir;
mod invocation;
pub mod model;
mod operator_surface;
pub mod plugin;
pub mod session_binding;
pub mod store;
pub mod worker;

pub use api::{AppState, openapi_document, router};
pub use auth::{AuthService, OperatorBootstrap, Principal};
pub use capability_broker::{
    CapabilityBrokerError, MessageCapabilityBroker, PUBLISH_DURABLE_MESSAGE_GRANT,
};
pub use controller::{
    ManagedHarnessController, ManagedTurn, ManagedTurnCapability, ManagedTurnError,
    ManagedTurnOutcome, TurnResultCapture,
};
pub use error::{ErrorResponse, FleetError};
pub use gooir::{
    BoundFact, CAPABILITY_CANDIDATE_PROTOCOL, CAPABILITY_INVOCATION_KIND,
    CAPABILITY_INVOCATION_PROTOCOL, CAPABILITY_OFFERS_PROTOCOL, CAPABILITY_RESULT_KIND,
    CAPABILITY_RESULT_PROTOCOL, CapabilityCandidate, CapabilityCandidateBody, CapabilityInvocation,
    CapabilityInvocationBody, CapabilityOffer, CapabilityOfferSet, CapabilityResult, EvidenceRef,
    ExactIdentity, FactAcceptance, FactCoverage, FactRequirement, GooirError, ProducedFact,
    candidate_from_result_message, durable_message_evidence,
};
pub use model::{
    AckDelivery, AddMember, Agent, ArmInvocation, BlockDelivery, BlockResolution, BlockedDelivery,
    Channel, ClaimBatch, ClaimDeliveries, CompleteInvocation, CreateAgent, CreateChannel,
    CreateMessage, Delivery, ExecutionCertainty, Invocation, InvocationBatch, InvocationCompletion,
    InvocationState, IssuedCredential, Message, MessagePage, RegisteredAgent, ResolveDeliveryBlock,
    RetryDelivery, SendMessage,
};
pub use plugin::{
    AcceptedResult, AssistantMessage, Binding, CancelTurn, CloseSession, CloseSessionResult,
    DescribeResult, DriverIdentity, EffectiveEnforcement, ExecutionFence, HarnessAcpClient,
    HarnessAcpNotification, HarnessExecutionCertainty, HarnessLimits, OpenSession, OpenSessionMode,
    OpenSessionResult, PermissionOutcome, PermissionRequested, PermissionResolution, PluginError,
    PluginExit, PluginIdentity, PluginManifest, PluginNotification, PluginProcess, PluginSpec,
    PromptBlock, ResolvedMcpEndpoint, ResolvedMcpGrant, ResolvedMcpHttpHeader, RuntimeIdentity,
    SessionPersistence, ShutdownOutcome, StartTurn, StartTurnResult, ToolBudget, TurnEvent,
    TurnPolicy, TurnSource, TurnTerminal, harness_acp_capabilities, harness_acp_offer_set,
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
