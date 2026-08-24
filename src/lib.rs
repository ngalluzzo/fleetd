//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
pub mod auth;
mod delivery;
pub mod error;
mod invocation;
pub mod model;
pub mod plugin;
pub mod store;

pub use api::{AppState, router};
pub use auth::{AuthService, OperatorBootstrap, Principal};
pub use error::FleetError;
pub use model::{
    AckDelivery, AddMember, Agent, ArmInvocation, BlockDelivery, BlockResolution, BlockedDelivery,
    Channel, ClaimBatch, ClaimDeliveries, CompleteInvocation, CreateAgent, CreateChannel,
    CreateMessage, Delivery, ExecutionCertainty, Invocation, InvocationBatch, InvocationCompletion,
    InvocationState, IssuedCredential, Message, MessagePage, RegisteredAgent, ResolveDeliveryBlock,
    RetryDelivery, SendMessage,
};
pub use plugin::{
    Capability, PluginError, PluginExit, PluginIdentity, PluginManifest, PluginNotification,
    PluginProcess, PluginSpec, ShutdownOutcome,
};
pub use store::{AppendMessageResult, Store};
