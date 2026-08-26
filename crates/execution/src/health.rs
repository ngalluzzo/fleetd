//! What a fleet is doing right now, composed from the durable read models.
//!
//! Every other module here owns a table and answers questions about it. This
//! one owns nothing and answers the question an operator actually asks: is the
//! fleet healthy? It is deliberately a composition, so a surface never has to
//! assemble the answer itself -- the CLI, HTTP, and any later surface all read
//! the same report rather than each reimplementing "current" and "active".

use std::collections::BTreeSet;

use fleetd_kernel::{error::FleetError, store::Store};
use fleetd_proto::model::{DeliveryState, InvocationState};

pub use fleetd_proto::operations::{DeliveryCensus, FleetHealth};

use crate::{
    invocation::list_invocations, operations::list_plugin_generations,
    session_binding::list_session_bindings,
};

/// Reads one bounded fleet health report, optionally narrowed to one agent.
///
/// `delivery_limit` bounds how many delivery rows the census reads, and
/// `DeliveryCensus::inspected` reports how many it actually saw, so a capped
/// read is visible as a cap rather than as a healthy fleet.
///
/// # Errors
///
/// Returns an error for a `delivery_limit` outside its bounds, or when any
/// underlying read model cannot be read or decoded.
pub async fn fleet_health(
    store: &Store,
    agent_id: Option<&str>,
    delivery_limit: u32,
) -> Result<FleetHealth, FleetError> {
    let generations = list_plugin_generations(store, agent_id).await?;
    let sessions = list_session_bindings(store, agent_id).await?;
    let invocations = list_invocations(store, agent_id).await?;
    let delivery_records = store
        .list_deliveries(agent_id, None, delivery_limit)
        .await?;

    Ok(FleetHealth {
        agent_id: agent_id.map(ToOwned::to_owned),
        current_plugin_generations: newest_per(generations, |generation| {
            generation.agent_id.clone()
        }),
        current_session_bindings: newest_per(sessions, |session| {
            session.binding.binding_id.clone()
        }),
        active_invocations: invocations
            .into_iter()
            .filter(|invocation| invocation.state != InvocationState::Terminal)
            .collect(),
        deliveries: census(&delivery_records),
        delivery_records,
    })
}

/// Keeps the first row for each key and drops the rest.
///
/// Both lists this is applied to are ordered newest-first, so the first row per
/// key is the current one. That ordering is the contract being relied on: a
/// list that stopped returning newest-first would silently change what
/// "current" means here, which is why this rule lives beside the report rather
/// than inside a caller.
fn newest_per<T, K: Ord>(rows: Vec<T>, key: impl Fn(&T) -> K) -> Vec<T> {
    let mut seen = BTreeSet::new();
    rows.into_iter()
        .filter(|row| seen.insert(key(row)))
        .collect()
}

fn census(records: &[fleetd_proto::model::DeliveryRecord]) -> DeliveryCensus {
    let mut census = DeliveryCensus {
        inspected: records.len(),
        ..DeliveryCensus::default()
    };
    for record in records {
        match record.state {
            DeliveryState::Pending => census.pending += 1,
            DeliveryState::Leased => {
                census.leased += 1;
                if record.lease_expired {
                    census.expired_leases += 1;
                }
            }
            DeliveryState::Blocked => census.blocked += 1,
            DeliveryState::Acknowledged => census.acknowledged += 1,
            DeliveryState::Dead => census.dead += 1,
        }
    }
    census
}

#[cfg(test)]
mod tests {
    use super::newest_per;

    #[test]
    fn newest_per_keeps_the_first_row_for_each_key() {
        let rows = vec![
            ("agent-a", "newest"),
            ("agent-a", "older"),
            ("agent-b", "only"),
            ("agent-a", "oldest"),
        ];
        assert_eq!(
            newest_per(rows, |row| row.0),
            vec![("agent-a", "newest"), ("agent-b", "only")]
        );
    }

    #[test]
    fn newest_per_preserves_the_order_it_was_given() {
        let rows = vec![("b", 1), ("a", 2), ("b", 3), ("c", 4)];
        assert_eq!(
            newest_per(rows, |row| row.0),
            vec![("b", 1), ("a", 2), ("c", 4)]
        );
    }
}
