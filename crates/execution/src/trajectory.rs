//! Optional lossy egress of in-flight harness trajectory.
//!
//! The bounded observation this layer already records keeps counters, byte
//! totals, and a chain digest. Reasoning, tool arguments, and intermediate
//! plans exist only while a turn is draining, and ADR 0028 exports them through
//! a sink that is permitted to lose them rather than by widening the durable
//! record.
//!
//! This module holds the offer, not the transport. A sink opens sockets, so it
//! lives in a crate named for its mechanism and arrives here as a trait
//! object -- the same shape as [`crate::controller::ManagedTurnGrant`], and for
//! the same reason: this layer decides what happens to durable state and does
//! not provision transports.
//!
//! Nothing here may fail a turn. Every offer returns `()`, so a sink has no way
//! to report an error into the control path, and one that cannot keep up is
//! required to drop rather than to block.

use futures_util::future::BoxFuture;
use serde_json::Value;

use fleetd_proto::model::ExecutionCertainty;

/// Identity of one managed invocation, offered when its trajectory opens.
///
/// `correlation_id` is carried because it, not a transport header, is how
/// Fleetd already identifies one causal run across seats. A sink that derives
/// trace identity from it joins an A -> B -> A loop into one trace without any
/// change to the immutable envelope.
#[derive(Clone, Copy, Debug)]
pub struct TrajectoryTurn<'a> {
    pub invocation_id: &'a str,
    pub agent_id: &'a str,
    pub channel_id: &'a str,
    pub source_message_id: &'a str,
    pub generation_id: &'a str,
    pub binding_id: &'a str,
    pub binding_generation: u64,
    pub owner_epoch: u64,
    pub correlation_id: Option<&'a str>,
    pub causation_id: Option<&'a str>,
    pub opened_at_ms: i64,
}

/// One observed harness update, offered before the durable fold discards its
/// content.
///
/// `raw_update` is already bounded by the turn policy's captured-output cap. It
/// is offered whole because selecting fields out of it is the sink's redaction
/// decision, not this layer's.
#[derive(Clone, Copy, Debug)]
pub struct TrajectoryUpdate<'a> {
    pub invocation_id: &'a str,
    pub event_seq: u64,
    pub observed_at_ms: i64,
    pub classification: &'a str,
    pub raw_update: &'a Value,
}

/// How one trajectory ended.
#[derive(Clone, Debug)]
pub struct TrajectoryOutcome<'a> {
    pub invocation_id: &'a str,
    pub closed_at_ms: i64,
    pub close: TrajectoryClose<'a>,
}

/// A known terminal, or ambiguity that was parked instead of repeated.
///
/// The two are kept apart because they are the two different things an operator
/// looks for. A parked turn has no result message to find it by, which is why a
/// sink is expected to mark it as failed rather than merely unfinished.
#[derive(Clone, Debug)]
pub enum TrajectoryClose<'a> {
    Terminal {
        stop_reason: &'a str,
        runtime_stop_reason: Option<&'a str>,
        certainty: ExecutionCertainty,
        session_quiescent: bool,
        usage: &'a Value,
    },
    Parked {
        reason: &'a str,
    },
}

/// An optional destination for trajectory that the durable record does not
/// keep.
///
/// Implementations must not block. `open`, `observe`, and `close` are called
/// from the turn's own task, between an armed dispatch and its settlement, so a
/// sink that waits on a socket delays a settlement and an unreachable collector
/// would become a control-plane failure. Queue and drop instead.
pub trait TrajectorySink: Send + Sync {
    /// Offers the identity of a turn whose dispatch has just been armed.
    fn open(&self, turn: &TrajectoryTurn<'_>);

    /// Offers one observed update. Called after the durable fold has committed,
    /// so a dropped offer never costs evidence.
    fn observe(&self, update: &TrajectoryUpdate<'_>);

    /// Offers the outcome of a turn that will not produce further updates.
    fn close(&self, outcome: &TrajectoryOutcome<'_>);

    /// Best-effort flush, bounded by the sink's own deadline.
    ///
    /// Called once when a plugin generation retires. Shutdown evidence must not
    /// depend on a collector being reachable, so this may return having sent
    /// nothing.
    fn flush(&self) -> BoxFuture<'_, ()>;
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        TrajectoryClose, TrajectoryOutcome, TrajectorySink, TrajectoryTurn, TrajectoryUpdate,
    };
    use futures_util::future::BoxFuture;
    use serde_json::{Value, json};

    /// A sink that records offers instead of exporting them.
    #[derive(Default)]
    struct RecordingSink {
        offers: Mutex<Vec<String>>,
    }

    impl TrajectorySink for RecordingSink {
        fn open(&self, turn: &TrajectoryTurn<'_>) {
            self.offers
                .lock()
                .expect("offers")
                .push(format!("open {}", turn.invocation_id));
        }

        fn observe(&self, update: &TrajectoryUpdate<'_>) {
            self.offers
                .lock()
                .expect("offers")
                .push(format!("{} {}", update.classification, update.event_seq));
        }

        fn close(&self, outcome: &TrajectoryOutcome<'_>) {
            let label = match &outcome.close {
                TrajectoryClose::Terminal { stop_reason, .. } => {
                    format!("terminal {stop_reason}")
                }
                TrajectoryClose::Parked { reason } => format!("parked {reason}"),
            };
            self.offers.lock().expect("offers").push(label);
        }

        fn flush(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    #[test]
    fn a_sink_is_usable_as_a_trait_object_without_a_runtime() {
        let sink = RecordingSink::default();
        let raw = json!({"sessionUpdate": "agent_thought_chunk"});
        let dynamic: &dyn TrajectorySink = &sink;
        dynamic.open(&TrajectoryTurn {
            invocation_id: "inv-1",
            agent_id: "agent-1",
            channel_id: "channel-1",
            source_message_id: "message-1",
            generation_id: "generation-1",
            binding_id: "binding-1",
            binding_generation: 2,
            owner_epoch: 3,
            correlation_id: Some("correlation-1"),
            causation_id: None,
            opened_at_ms: 10,
        });
        dynamic.observe(&TrajectoryUpdate {
            invocation_id: "inv-1",
            event_seq: 1,
            observed_at_ms: 11,
            classification: "reasoning",
            raw_update: &raw,
        });
        dynamic.close(&TrajectoryOutcome {
            invocation_id: "inv-1",
            closed_at_ms: 12,
            close: TrajectoryClose::Parked {
                reason: "harness outcome unknown",
            },
        });

        assert_eq!(
            *sink.offers.lock().expect("offers"),
            vec![
                "open inv-1".to_owned(),
                "reasoning 1".to_owned(),
                "parked harness outcome unknown".to_owned(),
            ]
        );
    }

    #[test]
    fn an_update_offers_the_whole_raw_value_for_the_sink_to_redact() {
        let sink = RecordingSink::default();
        let raw: Value = json!({"sessionUpdate": "tool_call", "title": "read file"});
        sink.observe(&TrajectoryUpdate {
            invocation_id: "inv-1",
            event_seq: 4,
            observed_at_ms: 20,
            classification: "tool",
            raw_update: &raw,
        });

        assert_eq!(raw["title"], json!("read file"));
        assert_eq!(*sink.offers.lock().expect("offers"), vec!["tool 4"]);
    }
}
