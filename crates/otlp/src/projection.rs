//! Projection of one turn's offers into OpenTelemetry spans.
//!
//! Spans are built by hand rather than through a tracer. A tracer instruments
//! the code it runs inside; this is a projection of records that already
//! happened, so it needs externally supplied identity and explicit start and
//! end timestamps. `SpanData` is the honest representation of that, and it also
//! keeps the queue and its drop accounting ours rather than a batch processor's.

use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use fleetd_proto::operations::EventClass;
use opentelemetry::{
    InstrumentationScope, KeyValue,
    trace::{Event, SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceId, TraceState},
};
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanLinks};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{ContentLevel, EgressConfig};

/// Identity of one turn, owned so it can cross the queue.
#[derive(Clone, Debug)]
pub(crate) struct OwnedTurn {
    pub invocation_id: String,
    pub agent_id: String,
    pub channel_id: String,
    pub source_message_id: String,
    pub generation_id: String,
    pub binding_id: String,
    pub binding_generation: u64,
    pub owner_epoch: u64,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub opened_at_ms: i64,
}

/// One observed update, owned so it can cross the queue.
#[derive(Clone, Debug)]
pub(crate) struct OwnedUpdate {
    pub invocation_id: String,
    pub event_seq: u64,
    pub observed_at_ms: i64,
    pub classification: String,
    pub raw_update: Value,
}

/// How one turn ended, owned so it can cross the queue.
#[derive(Clone, Debug)]
pub(crate) struct OwnedClose {
    pub invocation_id: String,
    pub closed_at_ms: i64,
    pub stop_reason: Option<String>,
    pub runtime_stop_reason: Option<String>,
    pub certainty: Option<String>,
    pub session_quiescent: Option<bool>,
    pub usage: Option<Value>,
    pub parked_reason: Option<String>,
}

/// One tool call, open until its last update.
struct ToolSpan {
    span_id: SpanId,
    start: SystemTime,
    end: SystemTime,
    attributes: Vec<KeyValue>,
    events: Vec<Event>,
}

/// One turn whose span has opened and not yet closed.
pub(crate) struct InFlight {
    trace_id: TraceId,
    span_id: SpanId,
    start: SystemTime,
    attributes: Vec<KeyValue>,
    events: Vec<Event>,
    tools: BTreeMap<String, ToolSpan>,
    observed: u64,
}

impl InFlight {
    pub(crate) fn open(turn: &OwnedTurn) -> Self {
        // Trace identity comes from the correlation the envelope already
        // carries, so an A -> B -> A run is one trace and an external projection
        // of the durable rows derives the same ids from the same evidence.
        let correlation = turn
            .correlation_id
            .as_deref()
            .unwrap_or(&turn.source_message_id);
        let mut attributes = vec![
            KeyValue::new("gen_ai.operation.name", "invoke_agent"),
            KeyValue::new("fleetd.invocation_id", turn.invocation_id.clone()),
            KeyValue::new("fleetd.agent_id", turn.agent_id.clone()),
            KeyValue::new("fleetd.channel_id", turn.channel_id.clone()),
            KeyValue::new("fleetd.source_message_id", turn.source_message_id.clone()),
            KeyValue::new("fleetd.generation_id", turn.generation_id.clone()),
            KeyValue::new("fleetd.binding_id", turn.binding_id.clone()),
            KeyValue::new(
                "fleetd.binding_generation",
                i64::try_from(turn.binding_generation).unwrap_or(i64::MAX),
            ),
            KeyValue::new(
                "fleetd.owner_epoch",
                i64::try_from(turn.owner_epoch).unwrap_or(i64::MAX),
            ),
        ];
        if let Some(correlation_id) = &turn.correlation_id {
            attributes.push(KeyValue::new(
                "fleetd.correlation_id",
                correlation_id.clone(),
            ));
        }
        if let Some(causation_id) = &turn.causation_id {
            attributes.push(KeyValue::new("fleetd.causation_id", causation_id.clone()));
        }
        Self {
            trace_id: derive_trace_id(correlation),
            span_id: derive_span_id("invocation", &turn.invocation_id),
            start: system_time(turn.opened_at_ms),
            attributes,
            events: Vec::new(),
            tools: BTreeMap::new(),
            observed: 0,
        }
    }

    /// Folds one update into either a tool child span or an event on the parent.
    ///
    /// Assistant, reasoning, and plan updates are chunks of one operation, so
    /// they are events. A tool call is an operation with a beginning and an end,
    /// so it is a span.
    pub(crate) fn observe(&mut self, update: &OwnedUpdate, config: &EgressConfig) {
        let class = EventClass::parse(&update.classification);
        if !config.classifications.contains(class.as_str()) {
            return;
        }
        self.observed = self.observed.saturating_add(1);
        let at = system_time(update.observed_at_ms);
        if class == EventClass::Tool {
            self.observe_tool(update, config, at);
            return;
        }
        let mut attributes = vec![KeyValue::new(
            "fleetd.event_seq",
            i64::try_from(update.event_seq).unwrap_or(i64::MAX),
        )];
        attributes.extend(event_attributes(class, &update.raw_update, config));
        self.events
            .push(Event::new(class.as_str().to_owned(), at, attributes, 0));
    }

    fn observe_tool(&mut self, update: &OwnedUpdate, config: &EgressConfig, at: SystemTime) {
        let Some(tool_call_id) = tool_call_id(&update.raw_update) else {
            // A tool update without an id cannot be grouped, so it stays an
            // event on the parent rather than being silently dropped.
            self.events.push(Event::new(
                "tool".to_owned(),
                at,
                vec![KeyValue::new(
                    "fleetd.event_seq",
                    i64::try_from(update.event_seq).unwrap_or(i64::MAX),
                )],
                0,
            ));
            return;
        };
        let span_id = derive_span_id(&format!("tool:{tool_call_id}"), &update.invocation_id);
        let tool = self.tools.entry(tool_call_id.clone()).or_insert(ToolSpan {
            span_id,
            start: at,
            end: at,
            attributes: vec![
                KeyValue::new("gen_ai.operation.name", "execute_tool"),
                KeyValue::new("fleetd.tool_call_id", tool_call_id),
            ],
            events: Vec::new(),
        });
        tool.end = at;
        tool.attributes
            .extend(tool_attributes(&update.raw_update, config));
        tool.events.push(Event::new(
            update.classification.clone(),
            at,
            vec![KeyValue::new(
                "fleetd.event_seq",
                i64::try_from(update.event_seq).unwrap_or(i64::MAX),
            )],
            0,
        ));
    }

    /// Finishes the parent span and every tool span it still holds open.
    pub(crate) fn close(mut self, close: &OwnedClose) -> Vec<SpanData> {
        let end = system_time(close.closed_at_ms);
        let mut spans = Vec::with_capacity(self.tools.len().saturating_add(1));
        for (_, tool) in std::mem::take(&mut self.tools) {
            spans.push(SpanData {
                span_context: SpanContext::new(
                    self.trace_id,
                    tool.span_id,
                    TraceFlags::SAMPLED,
                    false,
                    TraceState::default(),
                ),
                parent_span_id: self.span_id,
                parent_span_is_remote: false,
                span_kind: SpanKind::Internal,
                name: "execute_tool".into(),
                start_time: tool.start,
                end_time: tool.end,
                attributes: tool.attributes,
                dropped_attributes_count: 0,
                events: span_events(tool.events),
                links: SpanLinks::default(),
                status: Status::Unset,
                instrumentation_scope: scope(),
            });
        }

        let mut attributes = self.attributes;
        attributes.push(KeyValue::new(
            "fleetd.observed_event_count",
            i64::try_from(self.observed).unwrap_or(i64::MAX),
        ));
        let status = match (&close.parked_reason, &close.certainty) {
            (Some(reason), _) => Status::error(reason.clone()),
            (None, Some(certainty)) if certainty == "outcome_unknown" => {
                Status::error("harness outcome unknown")
            }
            _ => Status::Ok,
        };
        if let Some(stop_reason) = &close.stop_reason {
            attributes.push(KeyValue::new("fleetd.stop_reason", stop_reason.clone()));
        }
        if let Some(runtime_stop_reason) = &close.runtime_stop_reason {
            attributes.push(KeyValue::new(
                "fleetd.runtime_stop_reason",
                runtime_stop_reason.clone(),
            ));
        }
        if let Some(certainty) = &close.certainty {
            attributes.push(KeyValue::new(
                "fleetd.execution_certainty",
                certainty.clone(),
            ));
        }
        if let Some(quiescent) = close.session_quiescent {
            attributes.push(KeyValue::new("fleetd.session_quiescent", quiescent));
        }
        if let Some(usage) = &close.usage {
            attributes.extend(usage_attributes(usage));
        }

        spans.push(SpanData {
            span_context: SpanContext::new(
                self.trace_id,
                self.span_id,
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            parent_span_id: SpanId::INVALID,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: "invoke_agent".into(),
            start_time: self.start,
            end_time: end,
            attributes,
            dropped_attributes_count: 0,
            events: span_events(self.events),
            links: SpanLinks::default(),
            status,
            instrumentation_scope: scope(),
        });
        spans
    }
}

fn scope() -> InstrumentationScope {
    InstrumentationScope::builder("fleetd.trajectory")
        .with_version(env!("CARGO_PKG_VERSION"))
        .build()
}

fn span_events(events: Vec<Event>) -> SpanEvents {
    let mut span_events = SpanEvents::default();
    span_events.events = events;
    span_events
}

/// Content selected from a non-tool update, at the configured level.
fn event_attributes(class: EventClass, raw: &Value, config: &EgressConfig) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    match config.content {
        ContentLevel::None => return attributes,
        ContentLevel::Metadata | ContentLevel::Full => {}
    }
    if class == EventClass::Plan
        && let Some(entries) = raw.get("entries").and_then(Value::as_array)
    {
        attributes.push(KeyValue::new(
            "fleetd.plan_entries",
            i64::try_from(entries.len()).unwrap_or(i64::MAX),
        ));
    }
    if config.content == ContentLevel::Full {
        if let Some(text) = raw
            .get("content")
            .and_then(|content| content.get("text"))
            .and_then(Value::as_str)
        {
            attributes.push(KeyValue::new(
                "fleetd.content",
                truncate(text, config.max_attribute_bytes),
            ));
        } else if class == EventClass::Unknown {
            attributes.push(KeyValue::new(
                "fleetd.raw_update",
                truncate(&raw.to_string(), config.max_attribute_bytes),
            ));
        }
    }
    attributes
}

/// Content selected from a tool update, at the configured level.
///
/// `kind` is an ACP enumeration, so it is the tool's name at the metadata level.
/// `title` is agent-authored prose and can name a path or quote a request, so it
/// only appears at `full`.
fn tool_attributes(raw: &Value, config: &EgressConfig) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    match config.content {
        ContentLevel::None => return attributes,
        ContentLevel::Metadata | ContentLevel::Full => {}
    }
    if let Some(kind) = raw.get("kind").and_then(Value::as_str) {
        attributes.push(KeyValue::new("gen_ai.tool.name", kind.to_owned()));
    }
    if let Some(status) = raw.get("status").and_then(Value::as_str) {
        attributes.push(KeyValue::new("fleetd.tool_status", status.to_owned()));
    }
    if config.content == ContentLevel::Full {
        if let Some(title) = raw.get("title").and_then(Value::as_str) {
            attributes.push(KeyValue::new(
                "fleetd.tool_title",
                truncate(title, config.max_attribute_bytes),
            ));
        }
        for (field, key) in [
            ("rawInput", "fleetd.tool_input"),
            ("rawOutput", "fleetd.tool_output"),
        ] {
            if let Some(value) = raw.get(field) {
                attributes.push(KeyValue::new(
                    key,
                    truncate(&value.to_string(), config.max_attribute_bytes),
                ));
            }
        }
    }
    attributes
}

/// Token usage, read from the same evidence the durable observation stores.
fn usage_attributes(usage: &Value) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    for (field, key) in [
        ("inputTokens", "gen_ai.usage.input_tokens"),
        ("input_tokens", "gen_ai.usage.input_tokens"),
        ("outputTokens", "gen_ai.usage.output_tokens"),
        ("output_tokens", "gen_ai.usage.output_tokens"),
    ] {
        if attributes
            .iter()
            .any(|kv: &KeyValue| kv.key.as_str() == key)
        {
            continue;
        }
        if let Some(count) = usage.get(field).and_then(Value::as_i64) {
            attributes.push(KeyValue::new(key, count));
        }
    }
    attributes
}

fn tool_call_id(raw: &Value) -> Option<String> {
    raw.get("toolCallId")
        .or_else(|| raw.get("tool_call_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

/// Truncates on a character boundary, because a span attribute is text.
fn truncate(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_owned();
    }
    let mut end = limit;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_owned()
}

fn system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(u64::try_from(ms).unwrap_or_default())
}

/// Derives a trace id from the run's own correlation identity.
///
/// Deterministic so the same evidence always produces the same trace, and
/// domain-separated from span derivation so one identity cannot collide with
/// itself across the two.
fn derive_trace_id(correlation: &str) -> TraceId {
    let mut digest = Sha256::new();
    digest.update(b"fleetd-trajectory-trace-v1\0");
    digest.update(correlation.as_bytes());
    let bytes = digest.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&bytes[..16]);
    if id == [0; 16] {
        id[15] = 1;
    }
    TraceId::from_bytes(id)
}

fn derive_span_id(role: &str, identity: &str) -> SpanId {
    let mut digest = Sha256::new();
    digest.update(b"fleetd-trajectory-span-v1\0");
    digest.update(role.as_bytes());
    digest.update(b"\0");
    digest.update(identity.as_bytes());
    let bytes = digest.finalize();
    let mut id = [0_u8; 8];
    id.copy_from_slice(&bytes[..8]);
    if id == [0; 8] {
        id[7] = 1;
    }
    SpanId::from_bytes(id)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use fleetd_proto::operations::EventClass;
    use serde_json::json;

    use super::{InFlight, OwnedClose, OwnedTurn, OwnedUpdate, derive_trace_id, truncate};
    use crate::config::{ContentLevel, EgressConfig};

    fn config(content: ContentLevel) -> EgressConfig {
        EgressConfig {
            endpoint: "http://127.0.0.1:4318/v1/traces".to_owned(),
            headers: BTreeMap::new(),
            content,
            classifications: EventClass::ALL.iter().map(|it| it.as_str()).collect(),
            resource_attributes: BTreeMap::new(),
            max_attribute_bytes: 32,
            queue_capacity: 8,
            export_timeout: std::time::Duration::from_secs(5),
            shutdown_flush: std::time::Duration::from_secs(2),
            agent_id: "agent-1".to_owned(),
        }
    }

    fn turn() -> OwnedTurn {
        OwnedTurn {
            invocation_id: "inv-1".to_owned(),
            agent_id: "agent-1".to_owned(),
            channel_id: "channel-1".to_owned(),
            source_message_id: "message-1".to_owned(),
            generation_id: "generation-1".to_owned(),
            binding_id: "binding-1".to_owned(),
            binding_generation: 1,
            owner_epoch: 2,
            correlation_id: Some("correlation-1".to_owned()),
            causation_id: None,
            opened_at_ms: 1_000,
        }
    }

    fn update(seq: u64, classification: &str, raw: serde_json::Value) -> OwnedUpdate {
        OwnedUpdate {
            invocation_id: "inv-1".to_owned(),
            event_seq: seq,
            observed_at_ms: 1_000 + i64::try_from(seq).unwrap_or_default(),
            classification: classification.to_owned(),
            raw_update: raw,
        }
    }

    fn terminal() -> OwnedClose {
        OwnedClose {
            invocation_id: "inv-1".to_owned(),
            closed_at_ms: 2_000,
            stop_reason: Some("end_turn".to_owned()),
            runtime_stop_reason: None,
            certainty: Some("outcome_known".to_owned()),
            session_quiescent: Some(true),
            usage: Some(json!({"inputTokens": 11, "outputTokens": 22})),
            parked_reason: None,
        }
    }

    #[test]
    fn one_tool_call_becomes_one_child_span_across_its_updates() {
        let config = config(ContentLevel::Metadata);
        let mut flight = InFlight::open(&turn());
        flight.observe(
            &update(
                1,
                "tool_call",
                json!({"toolCallId": "call-1", "kind": "read", "status": "pending"}),
            ),
            &config,
        );
        flight.observe(
            &update(
                2,
                "tool_call_update",
                json!({"toolCallId": "call-1", "status": "completed"}),
            ),
            &config,
        );
        flight.observe(&update(3, "reasoning_content", json!({})), &config);

        let spans = flight.close(&terminal());
        assert_eq!(spans.len(), 2, "one tool child plus the parent");
        let tool = &spans[0];
        assert_eq!(tool.name, "execute_tool");
        assert_eq!(tool.events.events.len(), 2, "both updates land on one span");
        assert_eq!(tool.parent_span_id, spans[1].span_context.span_id());
        assert_eq!(
            tool.span_context.trace_id(),
            spans[1].span_context.trace_id()
        );
    }

    #[test]
    fn a_parked_turn_is_a_failed_span() {
        let flight = InFlight::open(&turn());
        let parked = OwnedClose {
            parked_reason: Some("outcome could not be proven".to_owned()),
            ..terminal()
        };
        let spans = flight.close(&parked);
        assert!(matches!(
            spans[0].status,
            opentelemetry::trace::Status::Error { .. }
        ));
    }

    #[test]
    fn metadata_carries_the_tool_kind_and_never_its_prose() {
        let config = config(ContentLevel::Metadata);
        let mut flight = InFlight::open(&turn());
        flight.observe(
            &update(
                1,
                "tool_call",
                json!({
                    "toolCallId": "call-1",
                    "kind": "read",
                    "title": "read /home/someone/secrets.txt",
                    "rawInput": {"path": "/home/someone/secrets.txt"}
                }),
            ),
            &config,
        );
        let spans = flight.close(&terminal());
        let keys: Vec<&str> = spans[0]
            .attributes
            .iter()
            .map(|kv| kv.key.as_str())
            .collect();
        assert!(keys.contains(&"gen_ai.tool.name"));
        assert!(!keys.contains(&"fleetd.tool_title"));
        assert!(!keys.contains(&"fleetd.tool_input"));
    }

    #[test]
    fn none_lifts_nothing_out_of_an_update() {
        let config = config(ContentLevel::None);
        let mut flight = InFlight::open(&turn());
        flight.observe(
            &update(
                1,
                "agent_message_content",
                json!({"content": {"type": "text", "text": "secret plan"}}),
            ),
            &config,
        );
        let spans = flight.close(&terminal());
        let parent = spans.last().expect("parent span");
        let rendered = format!("{:?}", parent.events.events);
        assert!(!rendered.contains("secret plan"), "{rendered}");
    }

    #[test]
    fn a_deselected_classification_never_reaches_a_span() {
        let mut config = config(ContentLevel::Full);
        config.classifications = BTreeSet::from(["tool"]);
        let mut flight = InFlight::open(&turn());
        flight.observe(
            &update(
                1,
                "reasoning_content",
                json!({"content": {"type": "text", "text": "thinking out loud"}}),
            ),
            &config,
        );
        let spans = flight.close(&terminal());
        let parent = spans.last().expect("parent span");
        assert!(parent.events.events.is_empty());
        let rendered = format!("{:?}", parent.events.events);
        assert!(!rendered.contains("thinking out loud"));
    }

    #[test]
    fn one_correlation_identity_is_one_trace() {
        assert_eq!(
            derive_trace_id("correlation-1"),
            derive_trace_id("correlation-1")
        );
        assert_ne!(
            derive_trace_id("correlation-1"),
            derive_trace_id("correlation-2")
        );
    }

    #[test]
    fn truncation_stops_on_a_character_boundary() {
        assert_eq!(truncate("dogs", 10), "dogs");
        assert_eq!(truncate("aaaa", 2), "aa");
        // Three bytes per character, so a two-byte limit keeps none of them.
        assert_eq!(truncate("日本語", 2), "");
    }
}
