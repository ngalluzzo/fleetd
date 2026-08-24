//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
mod delivery;
pub mod error;
pub mod model;
pub mod store;

pub use api::{AppState, router};
pub use error::FleetError;
pub use model::{
    AckDelivery, AddMember, Agent, Channel, ClaimBatch, ClaimDeliveries, CreateAgent,
    CreateChannel, CreateMessage, Delivery, Message, MessagePage, RetryDelivery,
};
pub use store::Store;
