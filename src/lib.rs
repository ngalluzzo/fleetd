//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
pub mod auth;
mod delivery;
pub mod error;
pub mod model;
pub mod store;

pub use api::{AppState, router};
pub use auth::{AuthService, OperatorBootstrap, Principal};
pub use error::FleetError;
pub use model::{
    AckDelivery, AddMember, Agent, Channel, ClaimBatch, ClaimDeliveries, CreateAgent,
    CreateChannel, CreateMessage, Delivery, IssuedCredential, Message, MessagePage,
    RegisteredAgent, RetryDelivery, SendMessage,
};
pub use store::Store;
