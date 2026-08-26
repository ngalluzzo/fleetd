//! The fleet health report, read directly from durable state.
//!
//! These assertions go through no surface at all. The report is composed below
//! HTTP, MCP, and the CLI, so it is provable without starting any of them.

use fleetd::execution::{health, settlement};
use fleetd::{
    model::{BlockDelivery, ClaimDeliveries, CreateAgent, CreateChannel, CreateMessage},
    store::Store,
};
use serde_json::json;

mod common;

fn claim(limit: u32, lease_duration_ms: u64) -> ClaimDeliveries {
    ClaimDeliveries {
        limit,
        lease_duration_ms,
    }
}

#[tokio::test]
async fn the_census_counts_every_delivery_state_and_notices_a_lapsed_lease() {
    let common::TempStore {
        directory: _directory,
        store,
        ..
    } = common::temp_store().await;
    let sender = agent(&store, "census-sender").await;
    let worker = agent(&store, "census-worker").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "census".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), worker.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");

    for index in 0..5 {
        send(&store, &channel.id, &sender.id, &worker.id, index).await;
    }

    // Oldest first: a lease that will lapse, a lease that will not, one
    // acknowledged, one blocked, and one left pending.
    let lapsing = settlement::claim_deliveries(&store, &worker.id, claim(1, 30))
        .await
        .expect("claim the lease that will lapse");
    assert_eq!(lapsing.deliveries.len(), 1);
    let held = settlement::claim_deliveries(&store, &worker.id, claim(1, 60_000))
        .await
        .expect("claim the lease that will hold");
    assert_eq!(held.deliveries.len(), 1);
    let settled = settlement::claim_deliveries(&store, &worker.id, claim(1, 60_000))
        .await
        .expect("claim the one to acknowledge");
    settlement::acknowledge_delivery(
        &store,
        &worker.id,
        &settled.deliveries[0].message.id,
        &settled.lease_token,
    )
    .await
    .expect("acknowledge");
    let to_block = settlement::claim_deliveries(&store, &worker.id, claim(1, 60_000))
        .await
        .expect("claim the one to block");
    settlement::block_delivery(
        &store,
        &worker.id,
        &to_block.deliveries[0].message.id,
        BlockDelivery {
            lease_token: to_block.lease_token.clone(),
            reason: "needs an operator".to_owned(),
        },
    )
    .await
    .expect("block");

    tokio::time::sleep(std::time::Duration::from_millis(60)).await;

    let report = health::fleet_health(&store, Some(&worker.id), 500)
        .await
        .expect("read fleet health");

    assert_eq!(report.agent_id.as_deref(), Some(worker.id.as_str()));
    assert_eq!(report.deliveries.inspected, 5);
    assert_eq!(report.deliveries.pending, 1);
    assert_eq!(report.deliveries.leased, 2);
    assert_eq!(report.deliveries.expired_leases, 1);
    assert_eq!(report.deliveries.blocked, 1);
    assert_eq!(report.deliveries.acknowledged, 1);
    assert_eq!(report.deliveries.dead, 0);
    assert_eq!(report.delivery_records.len(), 5);

    // The lapsed lease is named, not merely counted.
    let expired = report
        .delivery_records
        .iter()
        .filter(|record| record.lease_expired)
        .collect::<Vec<_>>();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].message.id, lapsing.deliveries[0].message.id);
    assert!(
        report
            .delivery_records
            .iter()
            .any(|record| record.message.id == held.deliveries[0].message.id
                && !record.lease_expired),
        "the live lease must not be reported as lapsed"
    );

    // Nothing has run, so there is no harness or turn to report on.
    assert!(report.current_plugin_generations.is_empty());
    assert!(report.current_session_bindings.is_empty());
    assert!(report.active_invocations.is_empty());
}

#[tokio::test]
async fn a_capped_census_reports_the_cap_rather_than_a_healthy_fleet() {
    let common::TempStore {
        directory: _directory,
        store,
        ..
    } = common::temp_store().await;
    let sender = agent(&store, "cap-sender").await;
    let worker = agent(&store, "cap-worker").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "cap".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), worker.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");
    for index in 0..4 {
        send(&store, &channel.id, &sender.id, &worker.id, index).await;
    }

    let capped = health::fleet_health(&store, Some(&worker.id), 2)
        .await
        .expect("read a capped report");
    assert_eq!(capped.deliveries.inspected, 2);
    assert_eq!(capped.deliveries.pending, 2);
    assert_eq!(capped.delivery_records.len(), 2);

    let full = health::fleet_health(&store, Some(&worker.id), 500)
        .await
        .expect("read the full report");
    assert_eq!(full.deliveries.inspected, 4);

    let rejected = health::fleet_health(&store, Some(&worker.id), 0).await;
    assert!(rejected.is_err(), "a zero limit must be refused");
}

#[tokio::test]
async fn health_scopes_to_one_agent_and_reports_an_idle_fleet_as_empty() {
    let common::TempStore {
        directory: _directory,
        store,
        ..
    } = common::temp_store().await;
    let sender = agent(&store, "scope-sender").await;
    let mine = agent(&store, "scope-mine").await;
    let theirs = agent(&store, "scope-theirs").await;
    let channel = store
        .create_channel(CreateChannel {
            name: "scope".to_owned(),
            metadata: json!({}),
            member_ids: vec![sender.id.clone(), mine.id.clone(), theirs.id.clone()],
            members: Vec::new(),
        })
        .await
        .expect("create channel");
    send(&store, &channel.id, &sender.id, &mine.id, 0).await;
    send(&store, &channel.id, &sender.id, &theirs.id, 1).await;

    let scoped = health::fleet_health(&store, Some(&mine.id), 500)
        .await
        .expect("scoped report");
    assert_eq!(scoped.deliveries.inspected, 1);

    let whole_fleet = health::fleet_health(&store, None, 500)
        .await
        .expect("fleet-wide report");
    assert_eq!(whole_fleet.agent_id, None);
    assert_eq!(whole_fleet.deliveries.inspected, 2);

    let idle = agent(&store, "scope-idle").await;
    let quiet = health::fleet_health(&store, Some(&idle.id), 500)
        .await
        .expect("idle report");
    assert_eq!(quiet.deliveries, health::DeliveryCensus::default());
    assert!(quiet.delivery_records.is_empty());
}

async fn agent(store: &Store, name: &str) -> fleetd::model::Agent {
    store
        .create_agent(CreateAgent {
            name: name.to_owned(),
            metadata: json!({}),
        })
        .await
        .expect("create agent")
}

async fn send(
    store: &Store,
    channel_id: &str,
    sender_id: &str,
    recipient_id: &str,
    index: usize,
) -> fleetd::model::Message {
    store
        .append_message(
            channel_id,
            CreateMessage {
                sender_id: sender_id.to_owned(),
                idempotency_key: None,
                recipient_id: Some(recipient_id.to_owned()),
                kind: "work.requested".to_owned(),
                payload: json!({ "index": index }),
                correlation_id: None,
                causation_id: None,
            },
        )
        .await
        .expect("append message")
}
