//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
pub mod auth;
mod delivery;
pub mod error;
pub mod model;
pub mod plugin;
pub mod store;

pub use api::{AppState, router};
pub use auth::{AuthService, OperatorBootstrap, Principal};
pub use error::FleetError;
pub use model::{
    AckDelivery, AddMember, Agent, BlockDelivery, BlockResolution, BlockedDelivery, Channel,
    ClaimBatch, ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage, Delivery,
    IssuedCredential, Message, MessagePage, RegisteredAgent, ResolveDeliveryBlock, RetryDelivery,
    SendMessage,
};
pub use plugin::{
    Capability, PluginError, PluginExit, PluginIdentity, PluginManifest, PluginNotification,
    PluginProcess, PluginSpec, ShutdownOutcome,
};
pub use store::{AppendMessageResult, Store};
