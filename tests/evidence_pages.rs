//! Cursor-addressed reads of the durable evidence listings.
//!
//! These assertions go through no surface at all. Paging is decided below
//! HTTP, MCP, and the CLI, so a collector's guarantee -- every row seen once
//! per change, in one total order -- is provable without starting a server.
//!
//! The assertions are written as walk properties rather than as fixed expected
//! orders. Rows recorded in the same millisecond tie on the change clock, and
//! whether any two of them do is a timing accident; that a walk still visits
//! each of them exactly once is the contract.

use fleetd::execution::operations::{
    self, EvidenceCursor, EvidenceOrder, EvidencePage, NewPluginGeneration,
    PluginGenerationDisposition, PluginGenerationState, PluginShutdownOutcome,
    StopPluginGeneration,
};
use fleetd::{
    error::FleetError,
    model::CreateAgent,
    plugin::{
        DescribeResult, DriverIdentity, HarnessLimits, PluginIdentity, RuntimeIdentity,
        harness_acp_interface,
    },
    store::Store,
};
use semver::Version;
use serde_json::json;

mod common;

/// Walks one listing a page at a time, following the cursor it hands back.
///
/// The page size is deliberately one: a collector that reads a single row per
/// request is the case most likely to skip or repeat a row at a page boundary,
/// so it is the case worth walking.
async fn walk(store: &Store, agent_id: &str, order: EvidenceOrder, settled: bool) -> Vec<String> {
    let mut visited = Vec::new();
    let mut after: Option<EvidenceCursor> = None;
    loop {
        let page = operations::list_plugin_generations(
            store,
            &EvidencePage {
                agent_id: Some(agent_id),
                after: after.as_ref(),
                limit: 1,
                settled,
                order,
            },
        )
        .await
        .expect("read one evidence page");
        let Some(generation) = page.last() else {
            return visited;
        };
        assert_eq!(page.len(), 1, "a page must honour its requested size");
        after = Some(EvidenceCursor {
            changed_at_ms: generation.last_heartbeat_at_ms,
            id: generation.id.clone(),
        });
        visited.push(generation.id.clone());
        assert!(
            visited.len() <= 32,
            "walking {order:?} did not terminate: {visited:?}"
        );
    }
}

#[tokio::test]
async fn a_cursor_walk_visits_every_generation_exactly_once_in_both_directions() {
    let common::TempStore {
        directory: _directory,
        store,
        ..
    } = common::temp_store().await;
    let agent = agent(&store, "evidence-walker").await;

    let mut recorded = Vec::new();
    for ordinal in 0..5 {
        recorded.push(generation(&store, &agent.id, ordinal).await);
        // Distinct milliseconds for most rows, so the walk is exercised across
        // change-clock values and not only across the ID tiebreak.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let oldest = walk(&store, &agent.id, EvidenceOrder::Oldest, false).await;
    let mut seen = oldest.clone();
    seen.sort();
    seen.dedup();
    assert_eq!(
        seen.len(),
        recorded.len(),
        "walking oldest skipped or repeated a generation"
    );

    let newest = walk(&store, &agent.id, EvidenceOrder::Newest, false).await;
    let reversed: Vec<_> = oldest.iter().rev().cloned().collect();
    assert_eq!(
        newest, reversed,
        "the two directions disagreed about the total order"
    );
}

#[tokio::test]
async fn a_settled_page_reports_only_evidence_that_can_no_longer_change() {
    let common::TempStore {
        directory: _directory,
        store,
        ..
    } = common::temp_store().await;
    let agent = agent(&store, "evidence-settler").await;

    let retired = generation(&store, &agent.id, 0).await;
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let running = generation(&store, &agent.id, 1).await;
    // Retire strictly after the live generation was recorded, so the settled
    // row's clock is provably behind it before retirement and ahead of it
    // after -- which is the movement this test is about.
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    let stopped = operations::stop_plugin_generation(
        &store,
        &retired,
        StopPluginGeneration {
            disposition: PluginGenerationDisposition::Stopped,
            reason: "operator stopped the worker".to_owned(),
            shutdown_outcome: PluginShutdownOutcome::Graceful,
            shutdown_exit_code: Some(0),
        },
    )
    .await
    .expect("retire the first generation");

    let settled = walk(&store, &agent.id, EvidenceOrder::Oldest, true).await;
    assert_eq!(settled, vec![retired.clone()]);

    // Retirement moves the change clock, so the settled row now sorts after
    // the live one. A collector that ordered settled rows by start time would
    // have walked past this generation before it ever settled.
    let unsettled = walk(&store, &agent.id, EvidenceOrder::Oldest, false).await;
    assert_eq!(unsettled, vec![running, retired]);
    assert_eq!(stopped.state, PluginGenerationState::Stopped);
    assert_eq!(stopped.last_heartbeat_at_ms, stopped.stopped_at_ms.unwrap());
}

#[tokio::test]
async fn a_cursor_addresses_a_position_rather_than_a_millisecond() {
    let common::TempStore {
        directory: _directory,
        store,
        ..
    } = common::temp_store().await;
    let agent = agent(&store, "evidence-boundary").await;
    let first = generation(&store, &agent.id, 0).await;
    let oldest = operations::list_plugin_generations(
        &store,
        &EvidencePage {
            agent_id: Some(&agent.id),
            after: None,
            limit: 500,
            settled: false,
            order: EvidenceOrder::Oldest,
        },
    )
    .await
    .expect("read the whole listing");
    assert_eq!(oldest.len(), 1);

    // A cursor at the row's own position excludes it: the position is
    // exclusive, so re-reading from a stored cursor cannot replay the last row
    // a collector already archived.
    let after_first = operations::list_plugin_generations(
        &store,
        &EvidencePage {
            agent_id: Some(&agent.id),
            after: Some(&EvidenceCursor {
                changed_at_ms: oldest[0].last_heartbeat_at_ms,
                id: first,
            }),
            limit: 500,
            settled: false,
            order: EvidenceOrder::Oldest,
        },
    )
    .await
    .expect("read past the last row");
    assert!(after_first.is_empty());
}

#[tokio::test]
async fn a_negative_cursor_is_rejected_and_an_oversized_limit_is_bounded() {
    let common::TempStore {
        directory: _directory,
        store,
        ..
    } = common::temp_store().await;
    let agent = agent(&store, "evidence-bounds").await;
    generation(&store, &agent.id, 0).await;

    let rejected = operations::list_invocation_observations(
        &store,
        &EvidencePage {
            agent_id: None,
            after: Some(&EvidenceCursor {
                changed_at_ms: -1,
                id: String::new(),
            }),
            limit: 10,
            settled: false,
            order: EvidenceOrder::Oldest,
        },
    )
    .await
    .expect_err("a negative cursor must not read from the beginning");
    assert!(matches!(rejected, FleetError::Invalid(_)));

    // An oversized limit is bounded rather than refused, so a caller asking
    // for everything receives a bounded page instead of an error.
    let bounded = operations::list_plugin_generations(
        &store,
        &EvidencePage {
            agent_id: Some(&agent.id),
            after: None,
            limit: u32::MAX,
            settled: false,
            order: EvidenceOrder::Newest,
        },
    )
    .await
    .expect("an oversized limit is clamped");
    assert_eq!(bounded.len(), 1);
}

async fn generation(store: &Store, agent_id: &str, ordinal: u32) -> String {
    let id = format!("evidence-generation-{ordinal}");
    operations::record_plugin_generation(
        store,
        NewPluginGeneration {
            id: id.clone(),
            agent_id: agent_id.to_owned(),
            plugin: PluginIdentity {
                id: "test.harness".to_owned(),
                name: "Test harness".to_owned(),
                version: Version::new(0, 1, 0),
            },
            interfaces: vec![harness_acp_interface()],
            process_id: Some(42),
            description: DescribeResult {
                driver: DriverIdentity {
                    version: "0.1.0".to_owned(),
                    acp_sdk_version: "2.0.0".to_owned(),
                    acp_protocol_version: 1,
                },
                runtime: RuntimeIdentity {
                    name: "test-runtime".to_owned(),
                    version: "1.0.0".to_owned(),
                    executable_digest: "sha256:test-runtime".to_owned(),
                },
                agent_capabilities: json!({}),
                limits: HarnessLimits {
                    max_concurrent_turns: 1,
                    max_frame_bytes: 1_048_576,
                },
                profile_digest: "sha256:test-profile".to_owned(),
                raw_initialize_result: json!({}),
            },
            compatibility_digest: "sha256:test-compatibility".to_owned(),
            heartbeat_interval_ms: 5_000,
        },
    )
    .await
    .expect("record plugin generation");
    id
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
