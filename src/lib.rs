//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
pub mod auth;
pub mod capability_broker;
pub mod controller;
mod delivery;
pub mod error;
mod invocation;
pub mod model;
mod operator_surface;
pub mod plugin;
mod repository_git;
pub mod repository_inspection;
pub mod repository_patch;
pub mod session_binding;
pub mod store;
pub mod work_contract;
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
    PluginNotification, PluginProcess, PluginSpec, PromptBlock, ResolvedMcpEndpoint,
    ResolvedMcpGrant, ResolvedMcpHttpHeader, RuntimeIdentity, SessionPersistence, ShutdownOutcome,
    StartTurn, StartTurnResult, ToolBudget, TurnEvent, TurnPolicy, TurnSource, TurnTerminal,
    harness_acp_capability,
};
pub use repository_git::RepositoryGitError;
pub use repository_inspection::{
    InspectionDisposition, REPOSITORY_INSPECTION_SUITE, RepositoryInspectionAnswer,
    RepositoryInspectionBrief, RepositoryInspectionError, RepositoryInspectionEvidence,
    RepositoryInspectionQuestion, RepositoryInspectionReport, RepositoryInspectionTurnAdapter,
    bind_repository_inspection, conform_repository_inspection, inspection_brief,
    repository_inspection_brief_fact, repository_inspection_capability,
    repository_inspection_report_fact,
};
pub use repository_patch::{
    ConformedRepositoryPatch, REPOSITORY_PATCH_SUITE, RepositoryChangeBrief, RepositoryPatchError,
    RepositoryPatchProposal, RepositoryPatchTurnAdapter, bind_repository_patch,
    conform_repository_patch, repository_change_brief, repository_change_brief_fact,
    repository_patch_artifact_fact, repository_patch_capability,
};
pub use session_binding::{
    AcquireSessionBinding, BoundInvocation, SessionAcquisition, SessionAcquisitionMode,
    SessionBinding, SessionBindingState,
};
pub use store::{AppendMessageResult, Store};
pub use work_contract::{
    AttemptEvidence, BoundFact, CAPABILITY_WORK_ATTEMPT_KIND, CAPABILITY_WORK_ATTEMPT_V2_KIND,
    CAPABILITY_WORK_CANDIDATE_KIND, CAPABILITY_WORK_REQUEST_KIND, CandidateFact,
    CapabilityAttemptProjection, CapabilityCandidate, CapabilityCandidateBody,
    CapabilityProviderDescriptor, CapabilityUnable, CapabilityWorkBody, CapabilityWorkRequest,
    ExactIdentity, FactAcceptance, FactCoverage, FactRequirement, WorkContractError,
    capability_attempt_context, extract_capability_attempt, extract_capability_attempt_v2,
    extract_capability_message,
};
pub use worker::{
    CapabilityWorkTurnAdapter, ContinuousHarnessWorker, ContinuousWorkerConfig,
    ContinuousWorkerError, EnvelopeTurnAdapter, InboundAcceptance, PreparedTurn, TurnAdapter,
    WorkerReport,
};
