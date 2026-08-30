//! Controller-owned decisions for harness permission requests.
//!
//! The evaluator consumes typed ACP option semantics. It deliberately does not
//! parse a model-authored title, command string, or tool input to infer safety.

use fleetd_plugin_host::{PermissionOutcome, PermissionRequested};
use serde_json::{Value, json};

/// Permission authority granted to one managed worker seat.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PermissionPolicy {
    /// Refuse every harness permission request.
    #[default]
    Deny,
    /// Select exactly one option whose ACP semantic kind is `allow_once`.
    AllowOnce,
}

impl PermissionPolicy {
    /// Stable desired-state vocabulary used in evidence and configuration.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::AllowOnce => "allow_once",
        }
    }
}

/// One decision plus the bounded explanation folded into turn evidence.
#[derive(Clone, Debug, PartialEq)]
pub struct PermissionDecision {
    pub outcome: PermissionOutcome,
    pub evidence: Value,
}

/// Resolves a request without assigning meaning to its model-authored tool
/// description.
#[must_use]
pub fn decide(policy: PermissionPolicy, request: &PermissionRequested) -> PermissionDecision {
    if policy == PermissionPolicy::Deny {
        return cancelled(policy, "policy_denied");
    }

    let mut allow_once = request.options.iter().filter_map(|option| {
        if option.get("kind").and_then(Value::as_str) != Some("allow_once") {
            return None;
        }
        option
            .get("optionId")
            .and_then(Value::as_str)
            .filter(|option_id| !option_id.trim().is_empty())
            .map(str::to_owned)
    });
    let Some(option_id) = allow_once.next() else {
        return cancelled(policy, "allow_once_option_missing");
    };
    if allow_once.next().is_some() {
        return cancelled(policy, "allow_once_option_ambiguous");
    }
    PermissionDecision {
        outcome: PermissionOutcome::Selected {
            option_id: option_id.clone(),
        },
        evidence: json!({
            "policy": policy.as_str(),
            "decision": "selected",
            "option_kind": "allow_once",
            "option_id": option_id,
        }),
    }
}

fn cancelled(policy: PermissionPolicy, reason: &str) -> PermissionDecision {
    PermissionDecision {
        outcome: PermissionOutcome::Cancelled,
        evidence: json!({
            "policy": policy.as_str(),
            "decision": "cancelled",
            "reason": reason,
        }),
    }
}

#[cfg(test)]
mod tests {
    use fleetd_plugin_host::{ExecutionFence, PermissionOutcome, PermissionRequested};
    use serde_json::json;

    use super::{PermissionPolicy, decide};

    fn request(options: Vec<serde_json::Value>) -> PermissionRequested {
        PermissionRequested {
            fence: ExecutionFence {
                binding_id: "binding".to_owned(),
                binding_generation: 1,
                owner_epoch: 1,
                invocation_id: "invocation".to_owned(),
                fence_token: "fence".to_owned(),
            },
            permission_id: "permission".to_owned(),
            event_seq: 1,
            tool_call: json!({"title": "untrusted description"}),
            options,
            expires_at_ms: 1,
        }
    }

    #[test]
    fn deny_never_selects_an_option() {
        let decision = decide(
            PermissionPolicy::Deny,
            &request(vec![json!({"kind": "allow_once", "optionId": "allow"})]),
        );
        assert_eq!(decision.outcome, PermissionOutcome::Cancelled);
        assert_eq!(decision.evidence["reason"], "policy_denied");
    }

    #[test]
    fn allow_once_selects_by_semantic_kind_not_adapter_specific_id() {
        let decision = decide(
            PermissionPolicy::AllowOnce,
            &request(vec![
                json!({"kind": "allow_always", "optionId": "persist"}),
                json!({"kind": "allow_once", "optionId": "vendor-specific"}),
                json!({"kind": "reject_once", "optionId": "reject"}),
            ]),
        );
        assert_eq!(
            decision.outcome,
            PermissionOutcome::Selected {
                option_id: "vendor-specific".to_owned()
            }
        );
        assert_eq!(decision.evidence["option_kind"], "allow_once");
    }

    #[test]
    fn missing_or_ambiguous_allow_once_fails_closed() {
        let missing = decide(
            PermissionPolicy::AllowOnce,
            &request(vec![json!({"kind": "allow_always", "optionId": "persist"})]),
        );
        assert_eq!(missing.outcome, PermissionOutcome::Cancelled);
        assert_eq!(missing.evidence["reason"], "allow_once_option_missing");

        let ambiguous = decide(
            PermissionPolicy::AllowOnce,
            &request(vec![
                json!({"kind": "allow_once", "optionId": "one"}),
                json!({"kind": "allow_once", "optionId": "two"}),
            ]),
        );
        assert_eq!(ambiguous.outcome, PermissionOutcome::Cancelled);
        assert_eq!(ambiguous.evidence["reason"], "allow_once_option_ambiguous");
    }
}
