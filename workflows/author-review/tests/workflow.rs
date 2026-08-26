use fleetd_author_review::{
    plugin::{AuthorReviewError, describe, evaluate},
    protocol::{
        ARTIFACT_PROPOSED, EvaluateParams, REVIEW_COMPLETED, REVIEW_REQUESTED, REVISION_REQUESTED,
        WORK_BLOCKED, WORK_COMPLETED, WORK_REQUESTED, WorkflowMember, WorkflowMessage,
    },
};
use serde_json::{Value, json};

const RUNNER: &str = "runner";
const HUMAN: &str = "human";
const COORDINATOR: &str = "coordinator";
const AUTHOR_A: &str = "author-a";
const AUTHOR_B: &str = "author-b";
const REVIEWER: &str = "reviewer";
const CHANNEL: &str = "channel";
const WORKFLOW: &str = "FLEETD-001";

#[test]
fn description_owns_the_small_vocabulary_and_payload_schemas() {
    let description = describe();
    assert_eq!(description.interface_id, "fleetd.workflow-draft");
    assert_eq!(description.interface_version, "0.0.1");
    assert_eq!(description.plugin_id, "fleetd.workflow.author-review");
    assert_eq!(description.event_schemas.len(), 7);
    for contract in &description.event_schemas {
        assert!(contract.schema.is_object());
        assert_eq!(contract.kind.split('.').count(), 2);
        assert_eq!(
            contract.schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        assert_closed_and_bounded_schema(&contract.schema);
    }
    assert_eq!(
        schema_for(&description, WORK_REQUESTED)["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        schema_for(&description, ARTIFACT_PROPOSED)["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        schema_for(&description, REVIEW_COMPLETED)["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        schema_for(&description, WORK_COMPLETED)["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        schema_for(&description, WORK_BLOCKED)["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn one_parent_fans_out_revises_and_completes_deterministically() {
    let mut history = vec![message(HUMAN, Some(RUNNER), WORK_REQUESTED, root_request())];

    let root = run(&history);
    assert_eq!(root.proposals.len(), 1);
    assert_eq!(root.proposals[0].recipient_id, COORDINATOR);
    assert_eq!(root.proposals[0].kind, WORK_REQUESTED);
    append_proposals(&mut history, &root.proposals);

    history.push(message(
        COORDINATOR,
        Some(RUNNER),
        ARTIFACT_PROPOSED,
        json!({
            "schema_version": 1,
            "request_id": WORKFLOW,
            "artifact_type": "decomposition",
            "children": [
                {
                    "request_id": "FLEETD-001-A",
                    "title": "Add projection",
                    "objective": "Implement the projection",
                    "scope": ["workflow projection"],
                    "acceptance_criteria": ["projection test passes"]
                },
                {
                    "request_id": "FLEETD-001-B",
                    "title": "Add runner",
                    "objective": "Implement durable runner effects",
                    "scope": ["workflow runner"],
                    "acceptance_criteria": ["crash replay test passes"]
                }
            ]
        }),
    ));
    let fanout = run(&history);
    assert_eq!(fanout.proposals.len(), 2);
    assert_eq!(fanout.proposals[0].recipient_id, AUTHOR_A);
    assert_eq!(fanout.proposals[1].recipient_id, AUTHOR_B);
    append_proposals(&mut history, &fanout.proposals);

    history.push(change_artifact(AUTHOR_A, "FLEETD-001-A", 0, "a0"));
    let review_a0 = run(&history);
    assert_eq!(review_a0.proposals.len(), 1);
    assert_eq!(review_a0.proposals[0].kind, REVIEW_REQUESTED);
    assert_eq!(review_a0.proposals[0].recipient_id, REVIEWER);
    append_proposals(&mut history, &review_a0.proposals);

    history.push(message(
        REVIEWER,
        Some(RUNNER),
        REVIEW_COMPLETED,
        json!({
            "schema_version": 1,
            "request_id": "FLEETD-001-A",
            "revision": 0,
            "decision": "revise",
            "summary": "One correction is required",
            "findings": ["Add the missing restart assertion"]
        }),
    ));
    let revision = run(&history);
    assert_eq!(revision.proposals.len(), 1);
    assert_eq!(revision.proposals[0].kind, REVISION_REQUESTED);
    assert_eq!(revision.proposals[0].recipient_id, AUTHOR_A);
    assert_eq!(revision.proposals[0].payload["revision"], 1);
    append_proposals(&mut history, &revision.proposals);

    history.push(change_artifact(AUTHOR_A, "FLEETD-001-A", 1, "a1"));
    let review_a1 = run(&history);
    append_proposals(&mut history, &review_a1.proposals);
    history.push(approved_review("FLEETD-001-A", 1));
    let complete_a = run(&history);
    assert_eq!(complete_a.proposals.len(), 1);
    assert_eq!(complete_a.proposals[0].kind, WORK_COMPLETED);
    assert_eq!(
        complete_a.proposals[0].payload["request_id"],
        "FLEETD-001-A"
    );
    append_proposals(&mut history, &complete_a.proposals);

    history.push(change_artifact(AUTHOR_B, "FLEETD-001-B", 0, "b0"));
    let review_b = run(&history);
    append_proposals(&mut history, &review_b.proposals);
    history.push(approved_review("FLEETD-001-B", 0));
    let final_review = history.last().expect("final review").clone();
    let complete = run(&history);
    assert_eq!(complete.proposals.len(), 2);
    assert_eq!(complete.proposals[0].payload["request_id"], "FLEETD-001-B");
    assert_eq!(complete.proposals[1].payload["request_id"], WORKFLOW);
    assert_eq!(complete.projection["phase"], "completed");

    append_proposals(&mut history, &complete.proposals);
    let replay = evaluate(&params_with_input(&history, final_review)).expect("replay workflow");
    assert!(replay.proposals.is_empty());
}

#[test]
fn an_unassigned_agent_cannot_submit_an_artifact() {
    let mut history = vec![message(HUMAN, Some(RUNNER), WORK_REQUESTED, root_request())];
    let root = run(&history);
    append_proposals(&mut history, &root.proposals);
    history.push(message(
        AUTHOR_A,
        Some(RUNNER),
        ARTIFACT_PROPOSED,
        json!({
            "schema_version": 1,
            "request_id": WORKFLOW,
            "artifact_type": "decomposition",
            "children": [{
                "request_id": "FLEETD-001-A",
                "title": "Wrong sender",
                "objective": "Must fail attribution",
                "scope": ["test"],
                "acceptance_criteria": ["rejected"]
            }]
        }),
    ));
    let error = evaluate(&params(&history)).expect_err("unassigned coordinator");
    assert!(matches!(error, AuthorReviewError::Evaluation(_)));
    assert!(error.to_string().contains("assigned coordinator"));
}

#[test]
fn role_selection_and_sender_attribution_are_deterministic() {
    let mut history = vec![message(HUMAN, Some(RUNNER), WORK_REQUESTED, root_request())];
    let root = run(&history);
    assert_eq!(root.proposals[0].recipient_id, COORDINATOR);
    append_proposals(&mut history, &root.proposals);

    history.push(message(
        COORDINATOR,
        Some(RUNNER),
        ARTIFACT_PROPOSED,
        decomposition(&["FLEETD-001-A", "FLEETD-001-B"]),
    ));
    let fanout = run(&history);
    assert_eq!(
        fanout
            .proposals
            .iter()
            .map(|proposal| proposal.recipient_id.as_str())
            .collect::<Vec<_>>(),
        [AUTHOR_A, AUTHOR_B]
    );
    append_proposals(&mut history, &fanout.proposals);

    history.push(change_artifact(AUTHOR_B, "FLEETD-001-A", 0, "wrong"));
    let error = evaluate(&params(&history)).expect_err("artifact from wrong author");
    assert!(error.to_string().contains("assigned author"));
    history.pop();

    history.push(change_artifact(AUTHOR_A, "FLEETD-001-A", 0, "right"));
    let review = run(&history);
    assert_eq!(review.proposals[0].recipient_id, REVIEWER);
    append_proposals(&mut history, &review.proposals);

    history.push(completed_review(AUTHOR_B, "FLEETD-001-A", 0, "approve"));
    let error = evaluate(&params(&history)).expect_err("review from wrong reviewer");
    assert!(error.to_string().contains("assigned reviewer"));
}

#[test]
fn correlated_replay_suppresses_committed_effects_but_keeps_missing_effects() {
    let mut history = one_child_history();
    history.push(change_artifact(AUTHOR_A, "FLEETD-001-A", 0, "a0"));
    let request_review = run(&history);
    append_proposals(&mut history, &request_review.proposals);
    let review = approved_review("FLEETD-001-A", 0);
    history.push(review.clone());

    let completion = run(&history);
    assert_eq!(completion.proposals.len(), 2);
    append_proposals_for_input(&mut history, &completion.proposals[..1], &review.id);

    let replay = evaluate(&params_with_input(&history, review.clone())).expect("partial replay");
    assert_eq!(replay.proposals.len(), 1);
    assert_eq!(replay.proposals[0].payload["request_id"], WORKFLOW);
    append_proposals_for_input(&mut history, &replay.proposals, &review.id);

    let fully_committed = evaluate(&params_with_input(&history, review)).expect("complete replay");
    assert!(fully_committed.proposals.is_empty());
}

#[test]
fn revision_round_configuration_and_transition_boundaries_are_consistent() {
    let root_history = vec![message(HUMAN, Some(RUNNER), WORK_REQUESTED, root_request())];
    assert!(evaluate(&params_with_max_rounds(&root_history, 0)).is_ok());
    assert!(evaluate(&params_with_max_rounds(&root_history, 8)).is_ok());
    let error =
        evaluate(&params_with_max_rounds(&root_history, 9)).expect_err("rounds above bound");
    assert!(error.to_string().contains("between 0 and 8"));

    let mut zero_rounds = one_child_history();
    zero_rounds.push(change_artifact(AUTHOR_A, "FLEETD-001-A", 0, "a0"));
    let request_review = run_with_max_rounds(&zero_rounds, 0);
    append_proposals(&mut zero_rounds, &request_review.proposals);
    zero_rounds.push(completed_review(REVIEWER, "FLEETD-001-A", 0, "revise"));
    let blocked = run_with_max_rounds(&zero_rounds, 0);
    assert_eq!(blocked.proposals.len(), 2);
    assert!(
        blocked
            .proposals
            .iter()
            .all(|proposal| proposal.kind == WORK_BLOCKED)
    );

    let mut eight_rounds = one_child_history();
    eight_rounds.push(revision_assignment(7));
    eight_rounds.push(change_artifact(AUTHOR_A, "FLEETD-001-A", 7, "a7"));
    let request_review = run_with_max_rounds(&eight_rounds, 8);
    append_proposals(&mut eight_rounds, &request_review.proposals);
    eight_rounds.push(completed_review(REVIEWER, "FLEETD-001-A", 7, "revise"));
    let final_revision = run_with_max_rounds(&eight_rounds, 8);
    assert_eq!(final_revision.proposals.len(), 1);
    assert_eq!(final_revision.proposals[0].kind, REVISION_REQUESTED);
    assert_eq!(final_revision.proposals[0].payload["revision"], 8);
    append_proposals(&mut eight_rounds, &final_revision.proposals);

    eight_rounds.push(change_artifact(AUTHOR_A, "FLEETD-001-A", 8, "a8"));
    let request_review = run_with_max_rounds(&eight_rounds, 8);
    append_proposals(&mut eight_rounds, &request_review.proposals);
    eight_rounds.push(completed_review(REVIEWER, "FLEETD-001-A", 8, "revise"));
    let blocked = run_with_max_rounds(&eight_rounds, 8);
    assert_eq!(blocked.proposals.len(), 2);
    assert!(
        blocked
            .proposals
            .iter()
            .all(|proposal| proposal.kind == WORK_BLOCKED)
    );
}

fn run(history: &[WorkflowMessage]) -> fleetd_author_review::protocol::EvaluateResult {
    evaluate(&params(history)).expect("evaluate workflow")
}

fn run_with_max_rounds(
    history: &[WorkflowMessage],
    max_revision_rounds: u32,
) -> fleetd_author_review::protocol::EvaluateResult {
    evaluate(&params_with_max_rounds(history, max_revision_rounds)).expect("evaluate workflow")
}

fn params(history: &[WorkflowMessage]) -> EvaluateParams {
    params_with_input(history, history.last().expect("input message").clone())
}

fn params_with_input(history: &[WorkflowMessage], input: WorkflowMessage) -> EvaluateParams {
    params_with_input_and_max_rounds(history, input, 2)
}

fn params_with_max_rounds(history: &[WorkflowMessage], max_revision_rounds: u32) -> EvaluateParams {
    params_with_input_and_max_rounds(
        history,
        history.last().expect("input message").clone(),
        max_revision_rounds,
    )
}

fn params_with_input_and_max_rounds(
    history: &[WorkflowMessage],
    input: WorkflowMessage,
    max_revision_rounds: u32,
) -> EvaluateParams {
    EvaluateParams {
        configuration: json!({
            "schema_version": 1,
            "coordinator_agent_id": COORDINATOR,
            "author_agent_ids": [AUTHOR_A, AUTHOR_B],
            "reviewer_agent_ids": [REVIEWER],
            "max_children": 4,
            "max_revision_rounds": max_revision_rounds
        }),
        runner_agent_id: RUNNER.to_owned(),
        workflow_id: WORKFLOW.to_owned(),
        input,
        history: history.to_vec(),
        members: vec![
            member(RUNNER, "inbox"),
            member(HUMAN, "stream_only"),
            member(COORDINATOR, "inbox"),
            member(AUTHOR_A, "inbox"),
            member(AUTHOR_B, "inbox"),
            member(REVIEWER, "inbox"),
        ],
    }
}

fn root_request() -> Value {
    json!({
        "schema_version": 1,
        "request_id": WORKFLOW,
        "title": "Dogfood author-review",
        "objective": "Run Fleetd development through its own visible workflow",
        "repository": {
            "path": "/Users/ngalluzzo/repos/fleetd",
            "base_revision": "fd32209"
        },
        "scope": ["external workflow package"],
        "acceptance_criteria": ["fan-out is visible", "restart replay is idempotent"]
    })
}

fn decomposition(request_ids: &[&str]) -> Value {
    json!({
        "schema_version": 1,
        "request_id": WORKFLOW,
        "artifact_type": "decomposition",
        "children": request_ids
            .iter()
            .map(|request_id| json!({
                "request_id": request_id,
                "title": format!("Implement {request_id}"),
                "objective": format!("Complete {request_id}"),
                "scope": ["workflow package"],
                "acceptance_criteria": ["workflow test passes"]
            }))
            .collect::<Vec<_>>()
    })
}

fn one_child_history() -> Vec<WorkflowMessage> {
    let mut history = vec![message(HUMAN, Some(RUNNER), WORK_REQUESTED, root_request())];
    let root = run(&history);
    append_proposals(&mut history, &root.proposals);
    history.push(message(
        COORDINATOR,
        Some(RUNNER),
        ARTIFACT_PROPOSED,
        decomposition(&["FLEETD-001-A"]),
    ));
    let fanout = run(&history);
    append_proposals(&mut history, &fanout.proposals);
    history
}

fn change_artifact(sender: &str, request_id: &str, revision: u32, suffix: &str) -> WorkflowMessage {
    message(
        sender,
        Some(RUNNER),
        ARTIFACT_PROPOSED,
        json!({
            "schema_version": 1,
            "request_id": request_id,
            "artifact_type": "change",
            "revision": revision,
            "summary": format!("implemented {request_id}"),
            "artifacts": [{
                "kind": "git_commit",
                "uri": format!("git:commit:{suffix}"),
                "revision": suffix
            }],
            "evidence": ["cargo test passed"]
        }),
    )
}

fn approved_review(request_id: &str, revision: u32) -> WorkflowMessage {
    completed_review(REVIEWER, request_id, revision, "approve")
}

fn completed_review(
    sender: &str,
    request_id: &str,
    revision: u32,
    decision: &str,
) -> WorkflowMessage {
    message(
        sender,
        Some(RUNNER),
        REVIEW_COMPLETED,
        json!({
            "schema_version": 1,
            "request_id": request_id,
            "revision": revision,
            "decision": decision,
            "summary": if decision == "approve" { "Approved against exact evidence" } else { "Material correction required" },
            "findings": if decision == "revise" { vec!["Address the material finding"] } else { Vec::<&str>::new() }
        }),
    )
}

fn revision_assignment(revision: u32) -> WorkflowMessage {
    message(
        RUNNER,
        Some(AUTHOR_A),
        REVISION_REQUESTED,
        json!({
            "schema_version": 1,
            "request_id": "FLEETD-001-A",
            "parent_request_id": WORKFLOW,
            "assignment": "author",
            "revision": revision,
            "prior_proposal_message_id": format!("proposal-{revision}"),
            "review_message_id": format!("review-{revision}"),
            "findings": ["Address the material finding"],
            "expected_output": {
                "kind": ARTIFACT_PROPOSED,
                "artifact_type": "change",
                "revision": revision,
                "instruction": "Address every finding, then return only one JSON object matching the change artifact schema as the final assistant message."
            }
        }),
    )
}

fn append_proposals(
    history: &mut Vec<WorkflowMessage>,
    proposals: &[fleetd_author_review::protocol::ProposedMessage],
) {
    let input_id = history.last().expect("proposal input").id.clone();
    append_proposals_for_input(history, proposals, &input_id);
}

fn append_proposals_for_input(
    history: &mut Vec<WorkflowMessage>,
    proposals: &[fleetd_author_review::protocol::ProposedMessage],
    input_id: &str,
) {
    for proposal in proposals {
        let mut committed = message(
            RUNNER,
            Some(&proposal.recipient_id),
            &proposal.kind,
            proposal.payload.clone(),
        );
        committed.causation_id = Some(input_id.to_owned());
        history.push(committed);
    }
}

fn message(sender: &str, recipient: Option<&str>, kind: &str, payload: Value) -> WorkflowMessage {
    let seq = next_sequence();
    WorkflowMessage {
        seq,
        id: format!("message-{seq}"),
        channel_id: CHANNEL.to_owned(),
        sender_id: sender.to_owned(),
        recipient_id: recipient.map(str::to_owned),
        kind: kind.to_owned(),
        payload,
        correlation_id: Some(WORKFLOW.to_owned()),
        causation_id: None,
        created_at_ms: seq,
    }
}

fn next_sequence() -> i64 {
    use std::sync::atomic::{AtomicI64, Ordering};
    static NEXT: AtomicI64 = AtomicI64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn member(agent_id: &str, delivery_mode: &str) -> WorkflowMember {
    WorkflowMember {
        agent_id: agent_id.to_owned(),
        agent_name: agent_id.to_owned(),
        delivery_mode: delivery_mode.to_owned(),
        joined_at_ms: 1,
    }
}

fn schema_for<'a>(
    description: &'a fleetd_author_review::protocol::DescribeResult,
    kind: &str,
) -> &'a Value {
    &description
        .event_schemas
        .iter()
        .find(|contract| contract.kind == kind)
        .expect("declared event schema")
        .schema
}

fn assert_closed_and_bounded_schema(value: &Value) {
    match value {
        Value::Object(object) => {
            if object.get("type") == Some(&Value::String("object".to_owned())) {
                assert_eq!(
                    object.get("additionalProperties"),
                    Some(&Value::Bool(false))
                );
                let properties = object["properties"].as_object().expect("object properties");
                let mut required = object["required"]
                    .as_array()
                    .expect("object required fields")
                    .iter()
                    .map(|field| field.as_str().expect("required field name"))
                    .collect::<Vec<_>>();
                let mut property_names = properties.keys().map(String::as_str).collect::<Vec<_>>();
                required.sort_unstable();
                property_names.sort_unstable();
                assert_eq!(
                    required, property_names,
                    "every object field is exact and required"
                );
            }
            if object.get("type") == Some(&Value::String("string".to_owned())) {
                assert_eq!(object.get("minLength"), Some(&json!(1)));
                assert!(object.get("maxLength").and_then(Value::as_u64).is_some());
                assert_eq!(object.get("pattern"), Some(&json!("\\S")));
                assert_eq!(object.get("x-fleetd-maxBytes"), object.get("maxLength"));
            }
            if object.get("type") == Some(&Value::String("array".to_owned())) {
                assert!(object.get("minItems").and_then(Value::as_u64).is_some());
                assert!(object.get("maxItems").and_then(Value::as_u64).is_some());
                assert!(object.contains_key("items"));
            }
            if object.get("type") == Some(&Value::String("integer".to_owned())) {
                assert!(object.get("minimum").and_then(Value::as_u64).is_some());
                assert_eq!(object.get("maximum"), Some(&json!(8)));
            }
            for nested in object.values() {
                assert_closed_and_bounded_schema(nested);
            }
        }
        Value::Array(values) => {
            for nested in values {
                assert_closed_and_bounded_schema(nested);
            }
        }
        _ => {}
    }
}
