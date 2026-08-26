use fleetd_author_review::{
    plugin::{AuthorReviewError, describe, evaluate},
    protocol::{
        ARTIFACT_PROPOSED, EvaluateParams, REVIEW_COMPLETED, REVIEW_REQUESTED, REVISION_REQUESTED,
        WORK_COMPLETED, WORK_REQUESTED, WorkflowMember, WorkflowMessage,
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
    assert!(
        description.event_schemas.iter().all(|contract| {
            contract.schema.is_object() && contract.kind.split('.').count() == 2
        })
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

fn run(history: &[WorkflowMessage]) -> fleetd_author_review::protocol::EvaluateResult {
    evaluate(&params(history)).expect("evaluate workflow")
}

fn params(history: &[WorkflowMessage]) -> EvaluateParams {
    params_with_input(history, history.last().expect("input message").clone())
}

fn params_with_input(history: &[WorkflowMessage], input: WorkflowMessage) -> EvaluateParams {
    EvaluateParams {
        configuration: json!({
            "schema_version": 1,
            "coordinator_agent_id": COORDINATOR,
            "author_agent_ids": [AUTHOR_A, AUTHOR_B],
            "reviewer_agent_ids": [REVIEWER],
            "max_children": 4,
            "max_revision_rounds": 2
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
    message(
        REVIEWER,
        Some(RUNNER),
        REVIEW_COMPLETED,
        json!({
            "schema_version": 1,
            "request_id": request_id,
            "revision": revision,
            "decision": "approve",
            "summary": "Approved against exact evidence",
            "findings": []
        }),
    )
}

fn append_proposals(
    history: &mut Vec<WorkflowMessage>,
    proposals: &[fleetd_author_review::protocol::ProposedMessage],
) {
    for proposal in proposals {
        history.push(message(
            RUNNER,
            Some(&proposal.recipient_id),
            &proposal.kind,
            proposal.payload.clone(),
        ));
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
