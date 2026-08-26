//! Authorization checks applied inside a handler.
//!
//! Authentication resolves which principal made the request; these decide what
//! that principal may reach.

use fleetd_kernel::{auth::Principal, error::FleetError};

use super::AppState;

pub(super) fn require_operator(principal: &Principal) -> Result<(), FleetError> {
    if principal.is_operator() {
        return Ok(());
    }
    Err(FleetError::Forbidden(
        "operator credential required".to_owned(),
    ))
}

pub(super) fn require_agent(principal: &Principal) -> Result<&str, FleetError> {
    principal
        .agent_id()
        .ok_or_else(|| FleetError::Forbidden("agent credential required".to_owned()))
}

pub(super) fn require_bound_agent(
    principal: &Principal,
    expected_agent_id: &str,
) -> Result<(), FleetError> {
    if require_agent(principal)? == expected_agent_id {
        return Ok(());
    }
    Err(FleetError::Forbidden(
        "credential is bound to another agent".to_owned(),
    ))
}

pub(super) async fn require_channel_access(
    state: &AppState,
    principal: &Principal,
    channel_id: &str,
) -> Result<(), FleetError> {
    if principal.is_operator() {
        return Ok(());
    }
    let agent_id = require_agent(principal)?;
    if state.store.is_member(channel_id, agent_id).await? {
        return Ok(());
    }
    Err(FleetError::Forbidden(
        "agent is not a member of this channel".to_owned(),
    ))
}
