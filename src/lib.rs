//! Durable, local-first coordination primitives for cooperating software agents.

pub mod api;
pub mod error;
pub mod model;
pub mod store;

pub use api::{AppState, router};
pub use error::FleetError;
pub use model::{
    AddMember, Agent, Channel, CreateAgent, CreateChannel, CreateMessage, Message, MessagePage,
};
pub use store::Store;
