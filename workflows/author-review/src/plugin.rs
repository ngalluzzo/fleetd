//! Deterministic author-review policy behind the draft workflow interface.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use thiserror::Error;

use crate::protocol::{
    ARTIFACT_PROPOSED, DescribeResult, EVENT_KINDS, EvaluateParams, EvaluateResult, EventSchema,
    INTERFACE_ID, INTERFACE_VERSION, MAX_HISTORY_MESSAGES, MAX_MEMBERS, MAX_PROPOSALS,
    MAX_REVISION_ROUNDS, MIN_REVISION_ROUNDS, PLUGIN_ID, PLUGIN_VERSION, ProposedMessage,
    REVIEW_COMPLETED, REVIEW_REQUESTED, REVISION_REQUESTED, WORK_BLOCKED, WORK_COMPLETED,
    WORK_REQUESTED, WorkflowMessage,
};

const MAX_ID_BYTES: usize = 128;
const MAX_TEXT_BYTES: usize = 8 * 1024;
const MAX_LIST_ITEMS: usize = 64;
const MAX_CHILDREN: usize = 16;

#[derive(Debug, Error)]
pub enum AuthorReviewError {
    #[error("invalid author-review configuration: {0}")]
    Configuration(String),
    #[error("invalid workflow evaluation: {0}")]
    Evaluation(String),
    #[error("invalid {kind} payload: {reason}")]
    Payload { kind: String, reason: String },
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorReviewConfiguration {
    schema_version: u32,
    coordinator_agent_id: String,
    author_agent_ids: Vec<String>,
    reviewer_agent_ids: Vec<String>,
    max_children: usize,
    max_revision_rounds: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RepositoryTarget {
    path: String,
    base_revision: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ChangeRequest {
    schema_version: u32,
    request_id: String,
    title: String,
    objective: String,
    repository: RepositoryTarget,
    scope: Vec<String>,
    acceptance_criteria: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ChildRequest {
    request_id: String,
    title: String,
    objective: String,
    scope: Vec<String>,
    acceptance_criteria: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, tag = "artifact_type", rename_all = "snake_case")]
enum ProposedArtifact {
    Decomposition {
        schema_version: u32,
        request_id: String,
        children: Vec<ChildRequest>,
    },
    Change {
        schema_version: u32,
        request_id: String,
        revision: u32,
        summary: String,
        artifacts: Vec<ArtifactReference>,
        evidence: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactReference {
    kind: String,
    uri: String,
    revision: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ReviewDecision {
    Approve,
    Revise,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CompletedReview {
    schema_version: u32,
    request_id: String,
    revision: u32,
    decision: ReviewDecision,
    summary: String,
    findings: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct AssignmentIdentity {
    request_id: String,
    #[serde(default, rename = "parent_request_id")]
    _parent_request_id: Option<String>,
    assignment: String,
    #[serde(default)]
    revision: u32,
    #[serde(default)]
    proposal_message_id: Option<String>,
}

#[must_use]
pub fn describe() -> DescribeResult {
    DescribeResult {
        interface_id: INTERFACE_ID.to_owned(),
        interface_version: INTERFACE_VERSION.to_owned(),
        plugin_id: PLUGIN_ID.to_owned(),
        plugin_version: PLUGIN_VERSION.to_owned(),
        roles: vec![
            "coordinator".to_owned(),
            "author".to_owned(),
            "reviewer".to_owned(),
        ],
        event_schemas: EVENT_KINDS
            .iter()
            .map(|kind| EventSchema {
                kind: (*kind).to_owned(),
                schema: event_schema(kind),
            })
            .collect(),
    }
}

/// Projects exact workflow history and returns only missing idempotent effects.
///
/// # Errors
///
/// Returns an error when configuration, history, membership, attribution, or a
/// semantic payload is invalid. The credential-owning runner decides how that
/// leased input is retried or blocked.
pub fn evaluate(params: &EvaluateParams) -> Result<EvaluateResult, AuthorReviewError> {
    validate_evaluation(params)?;
    let configuration = validate_configuration(params)?;
    let mut result = match params.input.kind.as_str() {
        WORK_REQUESTED => evaluate_root_request(params, &configuration)?,
        ARTIFACT_PROPOSED => evaluate_artifact(params, &configuration)?,
        REVIEW_COMPLETED => evaluate_review(params, &configuration)?,
        other => {
            return Err(AuthorReviewError::Evaluation(format!(
                "runner received unsupported input kind {other}"
            )));
        }
    };
    if result.proposals.len() > MAX_PROPOSALS {
        return Err(AuthorReviewError::Evaluation(format!(
            "plugin proposed more than {MAX_PROPOSALS} effects"
        )));
    }
    let mut operation_ids = HashSet::new();
    for proposal in &result.proposals {
        validate_identifier("proposal operation_id", &proposal.operation_id)?;
        if !operation_ids.insert(proposal.operation_id.as_str()) {
            return Err(AuthorReviewError::Evaluation(format!(
                "duplicate proposal operation_id {}",
                proposal.operation_id
            )));
        }
        if !EVENT_KINDS.contains(&proposal.kind.as_str()) {
            return Err(AuthorReviewError::Evaluation(format!(
                "proposal has undeclared kind {}",
                proposal.kind
            )));
        }
    }
    result.proposals.shrink_to_fit();
    Ok(result)
}

fn evaluate_root_request(
    params: &EvaluateParams,
    configuration: &AuthorReviewConfiguration,
) -> Result<EvaluateResult, AuthorReviewError> {
    let request: ChangeRequest = decode_direct_payload(&params.input)?;
    validate_change_request(&request, &params.workflow_id)?;
    if params.input.sender_id == params.runner_agent_id {
        return Err(AuthorReviewError::Evaluation(
            "root request must originate outside the workflow runner".to_owned(),
        ));
    }
    let proposal = ProposedMessage {
        operation_id: format!("assign-coordinator:{}", request.request_id),
        recipient_id: configuration.coordinator_agent_id.clone(),
        kind: WORK_REQUESTED.to_owned(),
        payload: json!({
            "schema_version": 1,
            "request_id": request.request_id,
            "parent_request_id": null,
            "assignment": "coordinator",
            "revision": 0,
            "change_request": request,
            "expected_output": {
                "kind": ARTIFACT_PROPOSED,
                "artifact_type": "decomposition",
                "instruction": "Return only one JSON object matching the decomposition artifact schema as the final assistant message."
            }
        }),
    };
    let proposals = missing_effect(params, proposal).into_iter().collect();
    Ok(EvaluateResult {
        projection: json!({
            "workflow_id": params.workflow_id,
            "root_request_id": request.request_id,
            "phase": "awaiting_decomposition",
            "child_count": 0,
            "completed_children": 0,
            "blocked_children": 0
        }),
        proposals,
    })
}

fn evaluate_artifact(
    params: &EvaluateParams,
    configuration: &AuthorReviewConfiguration,
) -> Result<EvaluateResult, AuthorReviewError> {
    let value = extract_semantic_payload(&params.input)?;
    let artifact: ProposedArtifact = decode_value(ARTIFACT_PROPOSED, value)?;
    match artifact {
        ProposedArtifact::Decomposition {
            schema_version,
            request_id,
            children,
        } => evaluate_decomposition(
            params,
            configuration,
            schema_version,
            &request_id,
            &children,
        ),
        ProposedArtifact::Change {
            schema_version,
            request_id,
            revision,
            summary,
            artifacts,
            evidence,
        } => evaluate_change_artifact(
            params,
            configuration,
            ChangeArtifactInput {
                schema_version,
                request_id: &request_id,
                revision,
                summary: &summary,
                artifacts: &artifacts,
                evidence: &evidence,
            },
        ),
    }
}

fn evaluate_decomposition(
    params: &EvaluateParams,
    configuration: &AuthorReviewConfiguration,
    schema_version: u32,
    request_id: &str,
    children: &[ChildRequest],
) -> Result<EvaluateResult, AuthorReviewError> {
    require_schema_one(schema_version, ARTIFACT_PROPOSED)?;
    if request_id != params.workflow_id {
        return Err(payload_error(
            ARTIFACT_PROPOSED,
            "decomposition request_id does not match workflow_id",
        ));
    }
    let coordinator_assignment = find_assignment(
        &params.history,
        &params.runner_agent_id,
        WORK_REQUESTED,
        request_id,
        "coordinator",
        0,
    )
    .ok_or_else(|| {
        AuthorReviewError::Evaluation("decomposition has no coordinator assignment".to_owned())
    })?;
    if coordinator_assignment.recipient_id.as_deref() != Some(&params.input.sender_id) {
        return Err(AuthorReviewError::Evaluation(
            "decomposition sender is not the assigned coordinator".to_owned(),
        ));
    }
    if children.is_empty() || children.len() > configuration.max_children {
        return Err(payload_error(
            ARTIFACT_PROPOSED,
            format!(
                "decomposition must contain between 1 and {} children",
                configuration.max_children
            ),
        ));
    }
    let mut child_ids = HashSet::new();
    for child in children {
        validate_child_request(child)?;
        if child.request_id == request_id || !child_ids.insert(child.request_id.as_str()) {
            return Err(payload_error(
                ARTIFACT_PROPOSED,
                "child request IDs must be unique and differ from the parent",
            ));
        }
    }
    let root = find_root_request(&params.history, request_id)?;
    let mut proposals = Vec::new();
    for (index, child) in children.iter().enumerate() {
        let author = &configuration.author_agent_ids[index % configuration.author_agent_ids.len()];
        let proposal = ProposedMessage {
            operation_id: format!("assign-author:{}:0", child.request_id),
            recipient_id: author.clone(),
            kind: WORK_REQUESTED.to_owned(),
            payload: json!({
                "schema_version": 1,
                "request_id": child.request_id,
                "parent_request_id": request_id,
                "assignment": "author",
                "revision": 0,
                "repository": root.repository,
                "change_request": child,
                "expected_output": {
                    "kind": ARTIFACT_PROPOSED,
                    "artifact_type": "change",
                    "instruction": "Implement the bounded change, then return only one JSON object matching the change artifact schema as the final assistant message."
                }
            }),
        };
        proposals.extend(missing_effect(params, proposal));
    }
    Ok(EvaluateResult {
        projection: projection(params, request_id, "awaiting_child_artifacts", children),
        proposals,
    })
}

#[derive(Clone, Copy)]
struct ChangeArtifactInput<'a> {
    schema_version: u32,
    request_id: &'a str,
    revision: u32,
    summary: &'a str,
    artifacts: &'a [ArtifactReference],
    evidence: &'a [String],
}

fn evaluate_change_artifact(
    params: &EvaluateParams,
    configuration: &AuthorReviewConfiguration,
    artifact: ChangeArtifactInput<'_>,
) -> Result<EvaluateResult, AuthorReviewError> {
    require_schema_one(artifact.schema_version, ARTIFACT_PROPOSED)?;
    validate_identifier("change request_id", artifact.request_id)?;
    validate_text("change summary", artifact.summary)?;
    validate_bounded_list("change artifacts", artifact.artifacts)?;
    validate_bounded_list("change evidence", artifact.evidence)?;
    if artifact.artifacts.is_empty() {
        return Err(payload_error(
            ARTIFACT_PROPOSED,
            "change artifact must identify at least one bounded output",
        ));
    }
    for reference in artifact.artifacts {
        validate_identifier("artifact kind", &reference.kind)?;
        validate_text("artifact URI", &reference.uri)?;
        validate_identifier("artifact revision", &reference.revision)?;
    }
    for evidence in artifact.evidence {
        validate_text("artifact evidence", evidence)?;
    }
    let author_assignment = find_assignment(
        &params.history,
        &params.runner_agent_id,
        if artifact.revision == 0 {
            WORK_REQUESTED
        } else {
            REVISION_REQUESTED
        },
        artifact.request_id,
        "author",
        artifact.revision,
    )
    .ok_or_else(|| {
        AuthorReviewError::Evaluation(format!(
            "change {} revision {} has no author assignment",
            artifact.request_id, artifact.revision
        ))
    })?;
    if author_assignment.recipient_id.as_deref() != Some(&params.input.sender_id) {
        return Err(AuthorReviewError::Evaluation(
            "change artifact sender is not its assigned author".to_owned(),
        ));
    }
    let reviewer = select_reviewer(configuration, &params.input.sender_id, artifact.request_id)?;
    let proposal = ProposedMessage {
        operation_id: format!(
            "request-review:{}:{}:{}",
            artifact.request_id, artifact.revision, params.input.id
        ),
        recipient_id: reviewer.to_owned(),
        kind: REVIEW_REQUESTED.to_owned(),
        payload: json!({
            "schema_version": 1,
            "request_id": artifact.request_id,
            "parent_request_id": params.workflow_id,
            "assignment": "reviewer",
            "revision": artifact.revision,
            "proposal_message_id": params.input.id,
            "proposal": {
                "summary": artifact.summary,
                "artifacts": artifact.artifacts,
                "evidence": artifact.evidence
            },
            "expected_output": {
                "kind": REVIEW_COMPLETED,
                "instruction": "Review the exact proposed artifacts and evidence, then return only one JSON object matching the completed review schema as the final assistant message."
            }
        }),
    };
    let proposals = missing_effect(params, proposal).into_iter().collect();
    let children = find_children(&params.history, &params.workflow_id)?;
    Ok(EvaluateResult {
        projection: projection(params, &params.workflow_id, "awaiting_review", &children),
        proposals,
    })
}

fn evaluate_review(
    params: &EvaluateParams,
    configuration: &AuthorReviewConfiguration,
) -> Result<EvaluateResult, AuthorReviewError> {
    let value = extract_semantic_payload(&params.input)?;
    let review: CompletedReview = decode_value(REVIEW_COMPLETED, value)?;
    require_schema_one(review.schema_version, REVIEW_COMPLETED)?;
    validate_identifier("review request_id", &review.request_id)?;
    validate_text("review summary", &review.summary)?;
    validate_bounded_list("review findings", &review.findings)?;
    for finding in &review.findings {
        validate_text("review finding", finding)?;
    }
    let review_request = find_assignment(
        &params.history,
        &params.runner_agent_id,
        REVIEW_REQUESTED,
        &review.request_id,
        "reviewer",
        review.revision,
    )
    .ok_or_else(|| {
        AuthorReviewError::Evaluation(format!(
            "review {} revision {} has no review assignment",
            review.request_id, review.revision
        ))
    })?;
    if review_request.recipient_id.as_deref() != Some(&params.input.sender_id) {
        return Err(AuthorReviewError::Evaluation(
            "completed review sender is not its assigned reviewer".to_owned(),
        ));
    }
    let proposal_message_id = assignment_identity(review_request)
        .and_then(|identity| identity.proposal_message_id)
        .ok_or_else(|| {
            AuthorReviewError::Evaluation("review request lacks proposal identity".to_owned())
        })?;
    let root = find_root_request(&params.history, &params.workflow_id)?;
    let children = find_children(&params.history, &params.workflow_id)?;
    if !children
        .iter()
        .any(|child| child.request_id == review.request_id)
    {
        return Err(AuthorReviewError::Evaluation(
            "completed review does not belong to the workflow decomposition".to_owned(),
        ));
    }
    let proposals = match review.decision {
        ReviewDecision::Approve => {
            approve_review(params, &review, &proposal_message_id, &root, &children)?
        }
        ReviewDecision::Revise => {
            revise_review(params, configuration, &review, &proposal_message_id, &root)?
        }
    };
    let phase = match review.decision {
        ReviewDecision::Approve
            if parent_will_be_complete(params, &review.request_id, &children) =>
        {
            "completed"
        }
        ReviewDecision::Approve => "awaiting_children",
        ReviewDecision::Revise if review.revision >= configuration.max_revision_rounds => "blocked",
        ReviewDecision::Revise => "awaiting_revision",
    };
    Ok(EvaluateResult {
        projection: projection(params, &params.workflow_id, phase, &children),
        proposals,
    })
}

fn approve_review(
    params: &EvaluateParams,
    review: &CompletedReview,
    proposal_message_id: &str,
    root: &ChangeRequest,
    children: &[ChildRequest],
) -> Result<Vec<ProposedMessage>, AuthorReviewError> {
    let mut proposals = Vec::new();
    let complete_child = ProposedMessage {
        operation_id: format!("complete-child:{}:{}", review.request_id, review.revision),
        recipient_id: root_requester(&params.history, &params.workflow_id)?.to_owned(),
        kind: WORK_COMPLETED.to_owned(),
        payload: json!({
            "schema_version": 1,
            "request_id": review.request_id,
            "parent_request_id": params.workflow_id,
            "revision": review.revision,
            "proposal_message_id": proposal_message_id,
            "review_message_id": params.input.id,
            "summary": review.summary
        }),
    };
    proposals.extend(missing_effect(params, complete_child));
    if parent_will_be_complete(params, &review.request_id, children) {
        let complete_parent = ProposedMessage {
            operation_id: format!("complete-parent:{}", params.workflow_id),
            recipient_id: root_requester(&params.history, &params.workflow_id)?.to_owned(),
            kind: WORK_COMPLETED.to_owned(),
            payload: json!({
                "schema_version": 1,
                "request_id": params.workflow_id,
                "parent_request_id": null,
                "title": root.title,
                "children": children.iter().map(|child| child.request_id.clone()).collect::<Vec<_>>(),
                "summary": "Every child change request completed its author-review policy."
            }),
        };
        proposals.extend(missing_effect(params, complete_parent));
    }
    Ok(proposals)
}

fn revise_review(
    params: &EvaluateParams,
    configuration: &AuthorReviewConfiguration,
    review: &CompletedReview,
    proposal_message_id: &str,
    root: &ChangeRequest,
) -> Result<Vec<ProposedMessage>, AuthorReviewError> {
    if review.findings.is_empty() {
        return Err(payload_error(
            REVIEW_COMPLETED,
            "revision decision requires at least one finding",
        ));
    }
    let requester = root_requester(&params.history, &params.workflow_id)?;
    if review.revision >= configuration.max_revision_rounds {
        let mut proposals = Vec::new();
        let block_child = ProposedMessage {
            operation_id: format!("block-child:{}", review.request_id),
            recipient_id: requester.to_owned(),
            kind: WORK_BLOCKED.to_owned(),
            payload: json!({
                "schema_version": 1,
                "request_id": review.request_id,
                "parent_request_id": params.workflow_id,
                "reason": "revision_limit_reached",
                "revision": review.revision,
                "findings": review.findings
            }),
        };
        proposals.extend(missing_effect(params, block_child));
        let block_parent = ProposedMessage {
            operation_id: format!("block-parent:{}", params.workflow_id),
            recipient_id: requester.to_owned(),
            kind: WORK_BLOCKED.to_owned(),
            payload: json!({
                "schema_version": 1,
                "request_id": params.workflow_id,
                "parent_request_id": null,
                "title": root.title,
                "reason": "child_revision_limit_reached",
                "blocked_child_id": review.request_id
            }),
        };
        proposals.extend(missing_effect(params, block_parent));
        return Ok(proposals);
    }
    let next_revision = review.revision + 1;
    let original_author = find_assignment(
        &params.history,
        &params.runner_agent_id,
        WORK_REQUESTED,
        &review.request_id,
        "author",
        0,
    )
    .and_then(|message| message.recipient_id.as_deref())
    .ok_or_else(|| AuthorReviewError::Evaluation("child has no original author".to_owned()))?;
    let proposal = ProposedMessage {
        operation_id: format!("request-revision:{}:{next_revision}", review.request_id),
        recipient_id: original_author.to_owned(),
        kind: REVISION_REQUESTED.to_owned(),
        payload: json!({
            "schema_version": 1,
            "request_id": review.request_id,
            "parent_request_id": params.workflow_id,
            "assignment": "author",
            "revision": next_revision,
            "prior_proposal_message_id": proposal_message_id,
            "review_message_id": params.input.id,
            "findings": review.findings,
            "expected_output": {
                "kind": ARTIFACT_PROPOSED,
                "artifact_type": "change",
                "revision": next_revision,
                "instruction": "Address every finding, then return only one JSON object matching the change artifact schema as the final assistant message."
            }
        }),
    };
    Ok(missing_effect(params, proposal).into_iter().collect())
}

fn validate_evaluation(params: &EvaluateParams) -> Result<(), AuthorReviewError> {
    validate_identifier("runner_agent_id", &params.runner_agent_id)?;
    validate_identifier("workflow_id", &params.workflow_id)?;
    if params.history.is_empty() || params.history.len() > MAX_HISTORY_MESSAGES {
        return Err(AuthorReviewError::Evaluation(format!(
            "history must contain between 1 and {MAX_HISTORY_MESSAGES} messages"
        )));
    }
    if params.members.is_empty() || params.members.len() > MAX_MEMBERS {
        return Err(AuthorReviewError::Evaluation(format!(
            "membership must contain between 1 and {MAX_MEMBERS} members"
        )));
    }
    if params.input.recipient_id.as_deref() != Some(&params.runner_agent_id) {
        return Err(AuthorReviewError::Evaluation(
            "leased input is not addressed to the workflow runner".to_owned(),
        ));
    }
    let member_ids = params
        .members
        .iter()
        .map(|member| member.agent_id.as_str())
        .collect::<HashSet<_>>();
    if !member_ids.contains(params.runner_agent_id.as_str())
        || !member_ids.contains(params.input.sender_id.as_str())
    {
        return Err(AuthorReviewError::Evaluation(
            "runner and input sender must be channel members".to_owned(),
        ));
    }
    let mut previous_seq = 0;
    let mut message_ids = HashSet::new();
    let mut found_input = false;
    for message in &params.history {
        if message.channel_id != params.input.channel_id
            || message.correlation_id.as_deref() != Some(params.workflow_id.as_str())
            || message.seq <= previous_seq
            || !message_ids.insert(message.id.as_str())
        {
            return Err(AuthorReviewError::Evaluation(
                "history must be one strictly ordered channel log".to_owned(),
            ));
        }
        previous_seq = message.seq;
        found_input |= message.id == params.input.id;
    }
    if !found_input {
        return Err(AuthorReviewError::Evaluation(
            "history does not contain the leased input".to_owned(),
        ));
    }
    Ok(())
}

fn validate_configuration(
    params: &EvaluateParams,
) -> Result<AuthorReviewConfiguration, AuthorReviewError> {
    let configuration: AuthorReviewConfiguration =
        serde_json::from_value(params.configuration.clone())
            .map_err(|error| AuthorReviewError::Configuration(error.to_string()))?;
    if configuration.schema_version != 1 {
        return Err(AuthorReviewError::Configuration(
            "schema_version must equal 1".to_owned(),
        ));
    }
    if configuration.author_agent_ids.is_empty()
        || configuration.reviewer_agent_ids.is_empty()
        || configuration.author_agent_ids.len() > MAX_MEMBERS
        || configuration.reviewer_agent_ids.len() > MAX_MEMBERS
    {
        return Err(AuthorReviewError::Configuration(
            "author and reviewer candidate sets must be non-empty and bounded".to_owned(),
        ));
    }
    if configuration.max_children == 0 || configuration.max_children > MAX_CHILDREN {
        return Err(AuthorReviewError::Configuration(format!(
            "max_children must be between 1 and {MAX_CHILDREN}"
        )));
    }
    if !(MIN_REVISION_ROUNDS..=MAX_REVISION_ROUNDS).contains(&configuration.max_revision_rounds) {
        return Err(AuthorReviewError::Configuration(format!(
            "max_revision_rounds must be between {MIN_REVISION_ROUNDS} and {MAX_REVISION_ROUNDS}"
        )));
    }
    let members = params
        .members
        .iter()
        .map(|member| (member.agent_id.as_str(), member.delivery_mode.as_str()))
        .collect::<HashMap<_, _>>();
    for (role, agent_id) in
        std::iter::once(("coordinator", configuration.coordinator_agent_id.as_str()))
            .chain(
                configuration
                    .author_agent_ids
                    .iter()
                    .map(|id| ("author", id.as_str())),
            )
            .chain(
                configuration
                    .reviewer_agent_ids
                    .iter()
                    .map(|id| ("reviewer", id.as_str())),
            )
    {
        validate_identifier(&format!("{role} agent ID"), agent_id)?;
        if members.get(agent_id) != Some(&"inbox") {
            return Err(AuthorReviewError::Configuration(format!(
                "{role} agent {agent_id} must be an inbox member of the channel"
            )));
        }
    }
    if !configuration.reviewer_agent_ids.iter().any(|reviewer| {
        configuration
            .author_agent_ids
            .iter()
            .any(|author| author != reviewer)
    }) {
        return Err(AuthorReviewError::Configuration(
            "at least one author/reviewer pair must use distinct agents".to_owned(),
        ));
    }
    Ok(configuration)
}

fn validate_change_request(
    request: &ChangeRequest,
    workflow_id: &str,
) -> Result<(), AuthorReviewError> {
    require_schema_one(request.schema_version, WORK_REQUESTED)?;
    if request.request_id != workflow_id {
        return Err(payload_error(
            WORK_REQUESTED,
            "request_id must equal the durable workflow correlation",
        ));
    }
    validate_identifier("change request ID", &request.request_id)?;
    validate_text("change title", &request.title)?;
    validate_text("change objective", &request.objective)?;
    validate_text("repository path", &request.repository.path)?;
    validate_identifier("base revision", &request.repository.base_revision)?;
    validate_nonempty_text_list("scope", &request.scope)?;
    validate_nonempty_text_list("acceptance_criteria", &request.acceptance_criteria)
}

fn validate_child_request(child: &ChildRequest) -> Result<(), AuthorReviewError> {
    validate_identifier("child request ID", &child.request_id)?;
    validate_text("child title", &child.title)?;
    validate_text("child objective", &child.objective)?;
    validate_nonempty_text_list("child scope", &child.scope)?;
    validate_nonempty_text_list("child acceptance_criteria", &child.acceptance_criteria)
}

fn validate_nonempty_text_list(label: &str, values: &[String]) -> Result<(), AuthorReviewError> {
    if values.is_empty() || values.len() > MAX_LIST_ITEMS {
        return Err(AuthorReviewError::Evaluation(format!(
            "{label} must contain between 1 and {MAX_LIST_ITEMS} entries"
        )));
    }
    for value in values {
        validate_text(label, value)?;
    }
    Ok(())
}

fn validate_bounded_list<T>(label: &str, values: &[T]) -> Result<(), AuthorReviewError> {
    if values.len() > MAX_LIST_ITEMS {
        return Err(AuthorReviewError::Evaluation(format!(
            "{label} exceeds {MAX_LIST_ITEMS} entries"
        )));
    }
    Ok(())
}

fn validate_identifier(label: &str, value: &str) -> Result<(), AuthorReviewError> {
    if value.trim().is_empty() || value.len() > MAX_ID_BYTES {
        return Err(AuthorReviewError::Evaluation(format!(
            "{label} must contain between 1 and {MAX_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_text(label: &str, value: &str) -> Result<(), AuthorReviewError> {
    if value.trim().is_empty() || value.len() > MAX_TEXT_BYTES {
        return Err(AuthorReviewError::Evaluation(format!(
            "{label} must contain between 1 and {MAX_TEXT_BYTES} bytes"
        )));
    }
    Ok(())
}

fn require_schema_one(version: u32, kind: &str) -> Result<(), AuthorReviewError> {
    if version != 1 {
        return Err(payload_error(kind, "schema_version must equal 1"));
    }
    Ok(())
}

fn decode_direct_payload<T: DeserializeOwned>(
    message: &WorkflowMessage,
) -> Result<T, AuthorReviewError> {
    decode_value(&message.kind, message.payload.clone())
}

fn decode_value<T: DeserializeOwned>(kind: &str, value: Value) -> Result<T, AuthorReviewError> {
    serde_json::from_value(value).map_err(|error| payload_error(kind, error.to_string()))
}

fn payload_error(kind: &str, reason: impl Into<String>) -> AuthorReviewError {
    AuthorReviewError::Payload {
        kind: kind.to_owned(),
        reason: reason.into(),
    }
}

fn extract_semantic_payload(message: &WorkflowMessage) -> Result<Value, AuthorReviewError> {
    if message.payload.get("schema_version").is_some() {
        return Ok(message.payload.clone());
    }
    let payload = message
        .payload
        .as_object()
        .ok_or_else(|| payload_error(&message.kind, "payload must be an object"))?;
    if payload.get("status").and_then(Value::as_str) != Some("completed")
        || payload.get("output_complete").and_then(Value::as_bool) != Some(true)
    {
        return Err(payload_error(
            &message.kind,
            "managed result must be completed with complete output",
        ));
    }
    let messages = payload
        .get("assistant_messages")
        .and_then(Value::as_array)
        .ok_or_else(|| payload_error(&message.kind, "assistant_messages must be an array"))?;
    let final_message = messages
        .last()
        .and_then(Value::as_object)
        .ok_or_else(|| payload_error(&message.kind, "assistant output is empty"))?;
    if final_message.get("complete").and_then(Value::as_bool) != Some(true) {
        return Err(payload_error(
            &message.kind,
            "final assistant message is incomplete",
        ));
    }
    let mut previous_last = 0;
    for assistant in messages {
        let assistant = assistant
            .as_object()
            .ok_or_else(|| payload_error(&message.kind, "assistant message must be an object"))?;
        let first = assistant
            .get("first_event_seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| payload_error(&message.kind, "invalid assistant event bounds"))?;
        let last = assistant
            .get("last_event_seq")
            .and_then(Value::as_u64)
            .ok_or_else(|| payload_error(&message.kind, "invalid assistant event bounds"))?;
        if first == 0 || first > last || first <= previous_last {
            return Err(payload_error(
                &message.kind,
                "assistant event bounds are not strictly ordered",
            ));
        }
        previous_last = last;
    }
    let content = final_message
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(|| payload_error(&message.kind, "final content must be an array"))?;
    let mut text = String::new();
    for block in content {
        if block.get("type").and_then(Value::as_str) != Some("text") {
            return Err(payload_error(
                &message.kind,
                "final content supports only text blocks",
            ));
        }
        let fragment = block
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| payload_error(&message.kind, "text block lacks text"))?;
        if text.len().saturating_add(fragment.len()) > MAX_TEXT_BYTES * 8 {
            return Err(payload_error(
                &message.kind,
                "final semantic output exceeds its bound",
            ));
        }
        text.push_str(fragment);
    }
    serde_json::from_str(&text).map_err(|error| payload_error(&message.kind, error.to_string()))
}

fn missing_effect(params: &EvaluateParams, proposal: ProposedMessage) -> Option<ProposedMessage> {
    let committed = params.history.iter().any(|message| {
        message.sender_id == params.runner_agent_id
            && message.recipient_id.as_deref() == Some(proposal.recipient_id.as_str())
            && message.kind == proposal.kind
            && message.payload == proposal.payload
            && message.correlation_id.as_deref() == Some(params.workflow_id.as_str())
            && message.causation_id.as_deref() == Some(params.input.id.as_str())
    });
    (!committed).then_some(proposal)
}

fn find_assignment<'a>(
    history: &'a [WorkflowMessage],
    runner_agent_id: &str,
    kind: &str,
    request_id: &str,
    assignment: &str,
    revision: u32,
) -> Option<&'a WorkflowMessage> {
    history.iter().find(|message| {
        message.sender_id == runner_agent_id
            && message.kind == kind
            && assignment_identity(message).is_some_and(|identity| {
                identity.request_id == request_id
                    && identity.assignment == assignment
                    && identity.revision == revision
            })
    })
}

fn assignment_identity(message: &WorkflowMessage) -> Option<AssignmentIdentity> {
    serde_json::from_value(message.payload.clone()).ok()
}

fn find_root_request(
    history: &[WorkflowMessage],
    workflow_id: &str,
) -> Result<ChangeRequest, AuthorReviewError> {
    history
        .iter()
        .find(|message| {
            message.kind == WORK_REQUESTED
                && message.sender_id != message.recipient_id.as_deref().unwrap_or_default()
                && message.payload.get("request_id").and_then(Value::as_str) == Some(workflow_id)
                && message.payload.get("assignment").is_none()
        })
        .ok_or_else(|| AuthorReviewError::Evaluation("root request is absent".to_owned()))
        .and_then(decode_direct_payload)
}

fn root_requester<'a>(
    history: &'a [WorkflowMessage],
    workflow_id: &str,
) -> Result<&'a str, AuthorReviewError> {
    history
        .iter()
        .find(|message| {
            message.kind == WORK_REQUESTED
                && message.payload.get("request_id").and_then(Value::as_str) == Some(workflow_id)
                && message.payload.get("assignment").is_none()
        })
        .map(|message| message.sender_id.as_str())
        .ok_or_else(|| AuthorReviewError::Evaluation("root requester is absent".to_owned()))
}

fn find_children(
    history: &[WorkflowMessage],
    workflow_id: &str,
) -> Result<Vec<ChildRequest>, AuthorReviewError> {
    for message in history {
        if message.kind != ARTIFACT_PROPOSED {
            continue;
        }
        let Ok(value) = extract_semantic_payload(message) else {
            continue;
        };
        let Ok(ProposedArtifact::Decomposition {
            request_id,
            children,
            ..
        }) = serde_json::from_value(value)
        else {
            continue;
        };
        if request_id == workflow_id {
            return Ok(children);
        }
    }
    Err(AuthorReviewError::Evaluation(
        "workflow decomposition is absent".to_owned(),
    ))
}

fn has_terminal_event(
    history: &[WorkflowMessage],
    runner_agent_id: &str,
    kind: &str,
    request_id: &str,
) -> bool {
    history.iter().any(|message| {
        message.sender_id == runner_agent_id
            && message.kind == kind
            && message.payload.get("request_id").and_then(Value::as_str) == Some(request_id)
    })
}

fn parent_will_be_complete(
    params: &EvaluateParams,
    approving_child_id: &str,
    children: &[ChildRequest],
) -> bool {
    children.iter().all(|child| {
        child.request_id == approving_child_id
            || has_terminal_event(
                &params.history,
                &params.runner_agent_id,
                WORK_COMPLETED,
                &child.request_id,
            )
    })
}

fn select_reviewer<'a>(
    configuration: &'a AuthorReviewConfiguration,
    author_id: &str,
    request_id: &str,
) -> Result<&'a str, AuthorReviewError> {
    let start = request_id
        .bytes()
        .fold(0_usize, |accumulator, byte| accumulator + usize::from(byte))
        % configuration.reviewer_agent_ids.len();
    for offset in 0..configuration.reviewer_agent_ids.len() {
        let reviewer = &configuration.reviewer_agent_ids
            [(start + offset) % configuration.reviewer_agent_ids.len()];
        if reviewer != author_id {
            return Ok(reviewer);
        }
    }
    Err(AuthorReviewError::Configuration(format!(
        "no reviewer distinct from author {author_id}"
    )))
}

fn projection(
    params: &EvaluateParams,
    root_request_id: &str,
    phase: &str,
    children: &[ChildRequest],
) -> Value {
    let completed_children = children
        .iter()
        .filter(|child| {
            has_terminal_event(
                &params.history,
                &params.runner_agent_id,
                WORK_COMPLETED,
                &child.request_id,
            )
        })
        .count();
    let blocked_children = children
        .iter()
        .filter(|child| {
            has_terminal_event(
                &params.history,
                &params.runner_agent_id,
                WORK_BLOCKED,
                &child.request_id,
            )
        })
        .count();
    json!({
        "workflow_id": params.workflow_id,
        "root_request_id": root_request_id,
        "phase": phase,
        "child_count": children.len(),
        "completed_children": completed_children,
        "blocked_children": blocked_children
    })
}

fn event_schema(kind: &str) -> Value {
    match kind {
        WORK_REQUESTED => work_requested_schema(),
        ARTIFACT_PROPOSED => artifact_proposed_schema(),
        REVIEW_REQUESTED => review_requested_schema(),
        REVIEW_COMPLETED => review_completed_schema(),
        REVISION_REQUESTED => revision_requested_schema(),
        WORK_COMPLETED => work_completed_schema(),
        WORK_BLOCKED => work_blocked_schema(),
        _ => Value::Null,
    }
}

fn work_requested_schema() -> Value {
    schema_root(json!({
        "oneOf": [
            change_request_schema(),
            object_schema(
                &["schema_version", "request_id", "parent_request_id", "assignment", "revision", "change_request", "expected_output"],
                json!({
                    "schema_version": {"const": 1},
                    "request_id": identifier_schema(),
                    "parent_request_id": {"const": null},
                    "assignment": {"const": "coordinator"},
                    "revision": {"const": 0},
                    "change_request": change_request_schema(),
                    "expected_output": decomposition_output_schema()
                })
            ),
            object_schema(
                &["schema_version", "request_id", "parent_request_id", "assignment", "revision", "repository", "change_request", "expected_output"],
                json!({
                    "schema_version": {"const": 1},
                    "request_id": identifier_schema(),
                    "parent_request_id": identifier_schema(),
                    "assignment": {"const": "author"},
                    "revision": {"const": 0},
                    "repository": repository_schema(),
                    "change_request": child_request_schema(),
                    "expected_output": change_output_schema(false)
                })
            )
        ]
    }))
}

fn artifact_proposed_schema() -> Value {
    schema_root(json!({
        "oneOf": [
            object_schema(
                &["schema_version", "request_id", "artifact_type", "children"],
                json!({
                    "schema_version": {"const": 1},
                    "request_id": identifier_schema(),
                    "artifact_type": {"const": "decomposition"},
                    "children": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_CHILDREN,
                        "items": child_request_schema()
                    }
                })
            ),
            object_schema(
                &["schema_version", "request_id", "artifact_type", "revision", "summary", "artifacts", "evidence"],
                json!({
                    "schema_version": {"const": 1},
                    "request_id": identifier_schema(),
                    "artifact_type": {"const": "change"},
                    "revision": revision_schema(0),
                    "summary": text_schema(),
                    "artifacts": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_LIST_ITEMS,
                        "items": artifact_reference_schema()
                    },
                    "evidence": text_list_schema(0)
                })
            )
        ]
    }))
}

fn review_requested_schema() -> Value {
    schema_root(object_schema(
        &[
            "schema_version",
            "request_id",
            "parent_request_id",
            "assignment",
            "revision",
            "proposal_message_id",
            "proposal",
            "expected_output",
        ],
        json!({
            "schema_version": {"const": 1},
            "request_id": identifier_schema(),
            "parent_request_id": identifier_schema(),
            "assignment": {"const": "reviewer"},
            "revision": revision_schema(0),
            "proposal_message_id": identifier_schema(),
            "proposal": object_schema(
                &["summary", "artifacts", "evidence"],
                json!({
                    "summary": text_schema(),
                    "artifacts": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_LIST_ITEMS,
                        "items": artifact_reference_schema()
                    },
                    "evidence": text_list_schema(0)
                })
            ),
            "expected_output": review_output_schema()
        }),
    ))
}

fn review_completed_schema() -> Value {
    schema_root(json!({
        "oneOf": [
            completed_review_variant("approve", 0),
            completed_review_variant("revise", 1)
        ]
    }))
}

fn revision_requested_schema() -> Value {
    schema_root(object_schema(
        &[
            "schema_version",
            "request_id",
            "parent_request_id",
            "assignment",
            "revision",
            "prior_proposal_message_id",
            "review_message_id",
            "findings",
            "expected_output",
        ],
        json!({
            "schema_version": {"const": 1},
            "request_id": identifier_schema(),
            "parent_request_id": identifier_schema(),
            "assignment": {"const": "author"},
            "revision": revision_schema(1),
            "prior_proposal_message_id": identifier_schema(),
            "review_message_id": identifier_schema(),
            "findings": text_list_schema(1),
            "expected_output": change_output_schema(true)
        }),
    ))
}

fn work_completed_schema() -> Value {
    schema_root(json!({
        "oneOf": [
            object_schema(
                &["schema_version", "request_id", "parent_request_id", "revision", "proposal_message_id", "review_message_id", "summary"],
                json!({
                    "schema_version": {"const": 1},
                    "request_id": identifier_schema(),
                    "parent_request_id": identifier_schema(),
                    "revision": revision_schema(0),
                    "proposal_message_id": identifier_schema(),
                    "review_message_id": identifier_schema(),
                    "summary": text_schema()
                })
            ),
            object_schema(
                &["schema_version", "request_id", "parent_request_id", "title", "children", "summary"],
                json!({
                    "schema_version": {"const": 1},
                    "request_id": identifier_schema(),
                    "parent_request_id": {"const": null},
                    "title": text_schema(),
                    "children": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_CHILDREN,
                        "uniqueItems": true,
                        "items": identifier_schema()
                    },
                    "summary": text_schema()
                })
            )
        ]
    }))
}

fn work_blocked_schema() -> Value {
    schema_root(json!({
        "oneOf": [
            object_schema(
                &["schema_version", "request_id", "parent_request_id", "reason", "revision", "findings"],
                json!({
                    "schema_version": {"const": 1},
                    "request_id": identifier_schema(),
                    "parent_request_id": identifier_schema(),
                    "reason": {"const": "revision_limit_reached"},
                    "revision": revision_schema(0),
                    "findings": text_list_schema(1)
                })
            ),
            object_schema(
                &["schema_version", "request_id", "parent_request_id", "title", "reason", "blocked_child_id"],
                json!({
                    "schema_version": {"const": 1},
                    "request_id": identifier_schema(),
                    "parent_request_id": {"const": null},
                    "title": text_schema(),
                    "reason": {"const": "child_revision_limit_reached"},
                    "blocked_child_id": identifier_schema()
                })
            )
        ]
    }))
}

fn schema_root(mut schema: Value) -> Value {
    schema
        .as_object_mut()
        .expect("event schemas are objects")
        .insert(
            "$schema".to_owned(),
            Value::String("https://json-schema.org/draft/2020-12/schema".to_owned()),
        );
    schema
}

fn object_schema(required: &[&str], properties: Value) -> Value {
    let mut schema = json!({
        "type": "object",
        "additionalProperties": false,
        "required": required
    });
    schema
        .as_object_mut()
        .expect("object schemas are objects")
        .insert("properties".to_owned(), properties);
    schema
}

fn identifier_schema() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": MAX_ID_BYTES,
        "pattern": "\\S",
        "x-fleetd-maxBytes": MAX_ID_BYTES
    })
}

fn text_schema() -> Value {
    json!({
        "type": "string",
        "minLength": 1,
        "maxLength": MAX_TEXT_BYTES,
        "pattern": "\\S",
        "x-fleetd-maxBytes": MAX_TEXT_BYTES
    })
}

fn revision_schema(minimum: u32) -> Value {
    json!({
        "type": "integer",
        "minimum": minimum,
        "maximum": MAX_REVISION_ROUNDS
    })
}

fn text_list_schema(min_items: usize) -> Value {
    json!({
        "type": "array",
        "minItems": min_items,
        "maxItems": MAX_LIST_ITEMS,
        "items": text_schema()
    })
}

fn repository_schema() -> Value {
    object_schema(
        &["path", "base_revision"],
        json!({
            "path": text_schema(),
            "base_revision": identifier_schema()
        }),
    )
}

fn change_request_schema() -> Value {
    object_schema(
        &[
            "schema_version",
            "request_id",
            "title",
            "objective",
            "repository",
            "scope",
            "acceptance_criteria",
        ],
        json!({
            "schema_version": {"const": 1},
            "request_id": identifier_schema(),
            "title": text_schema(),
            "objective": text_schema(),
            "repository": repository_schema(),
            "scope": text_list_schema(1),
            "acceptance_criteria": text_list_schema(1)
        }),
    )
}

fn child_request_schema() -> Value {
    object_schema(
        &[
            "request_id",
            "title",
            "objective",
            "scope",
            "acceptance_criteria",
        ],
        json!({
            "request_id": identifier_schema(),
            "title": text_schema(),
            "objective": text_schema(),
            "scope": text_list_schema(1),
            "acceptance_criteria": text_list_schema(1)
        }),
    )
}

fn artifact_reference_schema() -> Value {
    object_schema(
        &["kind", "uri", "revision"],
        json!({
            "kind": identifier_schema(),
            "uri": text_schema(),
            "revision": identifier_schema()
        }),
    )
}

fn decomposition_output_schema() -> Value {
    object_schema(
        &["kind", "artifact_type", "instruction"],
        json!({
            "kind": {"const": ARTIFACT_PROPOSED},
            "artifact_type": {"const": "decomposition"},
            "instruction": {"const": "Return only one JSON object matching the decomposition artifact schema as the final assistant message."}
        }),
    )
}

fn change_output_schema(includes_revision: bool) -> Value {
    if includes_revision {
        object_schema(
            &["kind", "artifact_type", "revision", "instruction"],
            json!({
                "kind": {"const": ARTIFACT_PROPOSED},
                "artifact_type": {"const": "change"},
                "revision": revision_schema(1),
                "instruction": {"const": "Address every finding, then return only one JSON object matching the change artifact schema as the final assistant message."}
            }),
        )
    } else {
        object_schema(
            &["kind", "artifact_type", "instruction"],
            json!({
                "kind": {"const": ARTIFACT_PROPOSED},
                "artifact_type": {"const": "change"},
                "instruction": {"const": "Implement the bounded change, then return only one JSON object matching the change artifact schema as the final assistant message."}
            }),
        )
    }
}

fn review_output_schema() -> Value {
    object_schema(
        &["kind", "instruction"],
        json!({
            "kind": {"const": REVIEW_COMPLETED},
            "instruction": {"const": "Review the exact proposed artifacts and evidence, then return only one JSON object matching the completed review schema as the final assistant message."}
        }),
    )
}

fn completed_review_variant(decision: &str, minimum_findings: usize) -> Value {
    object_schema(
        &[
            "schema_version",
            "request_id",
            "revision",
            "decision",
            "summary",
            "findings",
        ],
        json!({
            "schema_version": {"const": 1},
            "request_id": identifier_schema(),
            "revision": revision_schema(0),
            "decision": {"const": decision},
            "summary": text_schema(),
            "findings": text_list_schema(minimum_findings)
        }),
    )
}
