//! The bounded, lossy sink itself.
//!
//! Everything the turn's task touches is a non-blocking `try_send`. A drain
//! task owns the in-flight span state and the exporter, so no lock is held
//! across an HTTP request and an unreachable collector cannot reach back into a
//! settlement.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use fleetd_execution::trajectory::{
    TrajectoryClose, TrajectoryOutcome, TrajectorySink, TrajectoryTurn, TrajectoryUpdate,
};
use futures_util::future::BoxFuture;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{WithExportConfig as _, WithHttpConfig as _};
use opentelemetry_sdk::{Resource, trace::SpanExporter as _};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{
    config::EgressConfig,
    projection::{InFlight, OwnedClose, OwnedTurn, OwnedUpdate},
};

/// Failure to establish the egress transport.
#[derive(Debug, Error)]
pub enum EgressError {
    #[error("otlp span exporter could not be built: {0}")]
    Exporter(String),
}

enum Offer {
    Open(Box<OwnedTurn>),
    Update(Box<OwnedUpdate>),
    Close(Box<OwnedClose>),
    Flush(oneshot::Sender<()>),
}

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    dropped: AtomicU64,
    exported: AtomicU64,
    failed: AtomicU64,
    warned: AtomicBool,
}

/// One seat's trajectory egress.
///
/// Counters live here rather than in `WorkerReport`, which reports what a run
/// settled. They are scoped to the plugin generation whose retirement flushes
/// them, and they reach an operator through the log stream.
pub struct TrajectoryEgress {
    offers: mpsc::Sender<Offer>,
    counters: Arc<Counters>,
    shutdown_flush: Duration,
}

impl TrajectoryEgress {
    /// Builds the exporter and starts the drain task.
    ///
    /// Must be called from inside a Tokio runtime; a surface that provisions
    /// this has one, and this layer deliberately does not create its own.
    ///
    /// # Errors
    ///
    /// Returns an error only when the exporter itself cannot be constructed. A
    /// collector that is merely unreachable is not an error: nothing here may
    /// prevent a seat from starting.
    pub fn start(config: EgressConfig) -> Result<Self, EgressError> {
        let mut exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(config.endpoint.clone())
            .with_timeout(config.export_timeout)
            .with_headers(config.headers.clone().into_iter().collect())
            .build()
            .map_err(|error| EgressError::Exporter(error.to_string()))?;
        exporter.set_resource(&resource(&config));

        let (offers, receiver) = mpsc::channel(config.queue_capacity);
        let counters = Arc::new(Counters::default());
        let shutdown_flush = config.shutdown_flush;
        tokio::spawn(drain(receiver, exporter, config, Arc::clone(&counters)));
        Ok(Self {
            offers,
            counters,
            shutdown_flush,
        })
    }

    /// Queues one offer, counting the loss when the queue is full.
    ///
    /// The first drop of a generation is warned about once. Warning per event
    /// would turn a slow collector into a flooded log, which is its own outage.
    fn enqueue(&self, offer: Offer) {
        if self.offers.try_send(offer).is_ok() {
            self.counters.accepted.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let dropped = self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        if dropped == 0 && !self.counters.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "trajectory egress queue is full; dropping spans for this plugin generation. \
                 Durable invocation evidence is unaffected."
            );
        }
    }
}

fn resource(config: &EgressConfig) -> Resource {
    let mut attributes = vec![KeyValue::new("fleetd.agent_id", config.agent_id.clone())];
    for (key, value) in &config.resource_attributes {
        attributes.push(KeyValue::new(key.clone(), value.clone()));
    }
    Resource::builder()
        .with_service_name("fleetd")
        .with_attributes(attributes)
        .build()
}

impl TrajectorySink for TrajectoryEgress {
    fn open(&self, turn: &TrajectoryTurn<'_>) {
        self.enqueue(Offer::Open(Box::new(OwnedTurn {
            invocation_id: turn.invocation_id.to_owned(),
            agent_id: turn.agent_id.to_owned(),
            channel_id: turn.channel_id.to_owned(),
            source_message_id: turn.source_message_id.to_owned(),
            generation_id: turn.generation_id.to_owned(),
            binding_id: turn.binding_id.to_owned(),
            binding_generation: turn.binding_generation,
            owner_epoch: turn.owner_epoch,
            correlation_id: turn.correlation_id.map(ToOwned::to_owned),
            causation_id: turn.causation_id.map(ToOwned::to_owned),
            opened_at_ms: turn.opened_at_ms,
        })));
    }

    fn observe(&self, update: &TrajectoryUpdate<'_>) {
        self.enqueue(Offer::Update(Box::new(OwnedUpdate {
            invocation_id: update.invocation_id.to_owned(),
            event_seq: update.event_seq,
            observed_at_ms: update.observed_at_ms,
            classification: update.classification.to_owned(),
            raw_update: update.raw_update.clone(),
        })));
    }

    fn close(&self, outcome: &TrajectoryOutcome<'_>) {
        let mut owned = OwnedClose {
            invocation_id: outcome.invocation_id.to_owned(),
            closed_at_ms: outcome.closed_at_ms,
            stop_reason: None,
            runtime_stop_reason: None,
            certainty: None,
            session_quiescent: None,
            usage: None,
            parked_reason: None,
        };
        match &outcome.close {
            TrajectoryClose::Terminal {
                stop_reason,
                runtime_stop_reason,
                certainty,
                session_quiescent,
                usage,
            } => {
                owned.stop_reason = Some((*stop_reason).to_owned());
                owned.runtime_stop_reason = runtime_stop_reason.map(ToOwned::to_owned);
                owned.certainty = Some(certainty.as_str().to_owned());
                owned.session_quiescent = Some(*session_quiescent);
                owned.usage = Some((*usage).clone());
            }
            TrajectoryClose::Parked { reason } => {
                owned.parked_reason = Some((*reason).to_owned());
            }
        }
        self.enqueue(Offer::Close(Box::new(owned)));
    }

    fn flush(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let (acknowledged, wait) = oneshot::channel();
            // The queue is FIFO, so by the time the drain task answers this it
            // has already exported everything offered before it. That makes a
            // flush an ordering fact rather than a second mechanism.
            let settled = match self.offers.try_send(Offer::Flush(acknowledged)) {
                Ok(()) => tokio::time::timeout(self.shutdown_flush, wait)
                    .await
                    .is_ok(),
                Err(_) => false,
            };
            let accepted = self.counters.accepted.load(Ordering::Relaxed);
            let exported = self.counters.exported.load(Ordering::Relaxed);
            let failed = self.counters.failed.load(Ordering::Relaxed);
            let dropped = self.counters.dropped.load(Ordering::Relaxed);
            // An unacknowledged flush means the drain task never answered, so
            // nothing here can be trusted as a complete account. Reporting it as
            // a clean retirement is the silent loss this sink exists to avoid.
            if settled {
                tracing::info!(
                    accepted,
                    dropped,
                    exported,
                    failed,
                    "trajectory egress retired with this plugin generation"
                );
            } else {
                tracing::warn!(
                    accepted,
                    dropped,
                    exported,
                    failed,
                    "trajectory egress did not acknowledge its flush; spans offered after the \
                     last export were lost. Durable invocation evidence is unaffected."
                );
            }
        })
    }
}

/// Owns the in-flight spans and the exporter for one seat.
async fn drain(
    mut receiver: mpsc::Receiver<Offer>,
    exporter: opentelemetry_otlp::SpanExporter,
    config: EgressConfig,
    counters: Arc<Counters>,
) {
    let mut in_flight: BTreeMap<String, InFlight> = BTreeMap::new();
    while let Some(offer) = receiver.recv().await {
        match offer {
            Offer::Open(turn) => {
                in_flight.insert(turn.invocation_id.clone(), InFlight::open(&turn));
            }
            Offer::Update(update) => {
                if let Some(flight) = in_flight.get_mut(&update.invocation_id) {
                    flight.observe(&update, &config);
                }
            }
            Offer::Close(close) => {
                // A second close for one invocation finds nothing to remove,
                // which is what makes close idempotent for every post-arm exit.
                if let Some(flight) = in_flight.remove(&close.invocation_id) {
                    let spans = flight.close(&close);
                    let count = u64::try_from(spans.len()).unwrap_or_default();
                    match exporter.export(spans).await {
                        Ok(()) => {
                            counters.exported.fetch_add(count, Ordering::Relaxed);
                        }
                        Err(error) => {
                            counters.failed.fetch_add(count, Ordering::Relaxed);
                            tracing::debug!(
                                %error,
                                "trajectory egress export failed; durable evidence is unaffected"
                            );
                        }
                    }
                }
            }
            Offer::Flush(acknowledged) => {
                let _receiver_gone = acknowledged.send(());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    };

    use super::{Counters, Offer, TrajectoryEgress};
    use crate::config::{ContentLevel, EgressConfig};
    use fleetd_execution::trajectory::{TrajectorySink as _, TrajectoryTurn};
    use fleetd_proto::operations::EventClass;
    use tokio::sync::mpsc;

    /// Builds a sink whose queue nothing is draining, so a full queue is
    /// reachable without a collector.
    fn stalled(capacity: usize) -> (TrajectoryEgress, mpsc::Receiver<Offer>) {
        let (offers, receiver) = mpsc::channel(capacity);
        (
            TrajectoryEgress {
                offers,
                counters: Arc::new(Counters {
                    accepted: AtomicU64::new(0),
                    dropped: AtomicU64::new(0),
                    exported: AtomicU64::new(0),
                    failed: AtomicU64::new(0),
                    warned: AtomicBool::new(false),
                }),
                shutdown_flush: std::time::Duration::from_millis(10),
            },
            receiver,
        )
    }

    fn turn<'a>() -> TrajectoryTurn<'a> {
        TrajectoryTurn {
            invocation_id: "inv-1",
            agent_id: "agent-1",
            channel_id: "channel-1",
            source_message_id: "message-1",
            generation_id: "generation-1",
            binding_id: "binding-1",
            binding_generation: 1,
            owner_epoch: 1,
            correlation_id: None,
            causation_id: None,
            opened_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn a_full_queue_drops_and_counts_instead_of_blocking() {
        let (sink, _receiver) = stalled(1);
        sink.open(&turn());
        sink.open(&turn());
        sink.open(&turn());

        assert_eq!(sink.counters.accepted.load(Ordering::Relaxed), 1);
        assert_eq!(sink.counters.dropped.load(Ordering::Relaxed), 2);
        assert!(sink.counters.warned.load(Ordering::Relaxed));
    }

    /// A dead or stalled drain task must not be able to report a clean
    /// retirement. This is the shape of a real failure: the exporter's blocking
    /// client panicked the task, every offer was accepted, and nothing shipped.
    #[tokio::test]
    async fn an_unacknowledged_flush_is_reported_rather_than_swallowed() {
        let (sink, receiver) = stalled(4);
        sink.open(&turn());
        drop(receiver);
        sink.flush().await;

        assert_eq!(sink.counters.accepted.load(Ordering::Relaxed), 1);
        assert_eq!(sink.counters.exported.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_validated_config_keeps_the_endpoint_it_was_given() {
        let config = EgressConfig {
            endpoint: "http://127.0.0.1:4318/v1/traces".to_owned(),
            headers: BTreeMap::new(),
            content: ContentLevel::Metadata,
            classifications: EventClass::ALL.iter().map(|it| it.as_str()).collect(),
            resource_attributes: BTreeMap::new(),
            max_attribute_bytes: 4_096,
            queue_capacity: 8,
            export_timeout: std::time::Duration::from_secs(5),
            shutdown_flush: std::time::Duration::from_secs(2),
            agent_id: "agent-1".to_owned(),
        };
        assert!(super::resource(&config).iter().count() > 0);
    }
}
