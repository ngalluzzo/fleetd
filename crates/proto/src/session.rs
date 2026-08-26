//! Durable harness session lanes and their ownership fences.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::harness_acp::{Binding, SessionPersistence};

/// Durable lifecycle state for one native harness session generation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SessionBindingState {
    Opening,
    Ready,
    Active,
    Uncertain,
    Retired,
}

impl SessionBindingState {
    /// Every variant, so `parse` can invert `as_str` without a second table.
    ///
    /// A new variant has to appear here to survive a storage round trip; the
    /// tests at the end of this module fail while it is missing.
    pub const ALL: [Self; 5] = [
        Self::Opening,
        Self::Ready,
        Self::Active,
        Self::Uncertain,
        Self::Retired,
    ];

    /// Returns the exact stored representation of this variant.
    ///
    /// `Serialize` produces the same spelling, and a test pins the two
    /// together: a durable row and a wire frame carry one vocabulary, not two.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::Ready => "ready",
            Self::Active => "active",
            Self::Uncertain => "uncertain",
            Self::Retired => "retired",
        }
    }

    /// Reads back the representation `as_str` produced.
    ///
    /// Returns `None` for anything else, leaving the caller to say what an
    /// unreadable stored value means to it.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|variant| variant.as_str() == value)
    }
}

/// Exact desired lane and runtime compatibility used to acquire ownership.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AcquireSessionBinding {
    pub lane_policy: String,
    pub lane_key: String,
    pub owner_instance_id: String,
    pub profile_digest: String,
    pub compatibility_digest: String,
    pub working_directory: String,
    #[serde(default)]
    pub additional_directories: Vec<String>,
}

/// Harness operation required after durable lane acquisition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionAcquisitionMode {
    Create,
    Resume { session_ref: String },
}

/// One durable native-session generation and its current owner fence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct SessionBinding {
    pub binding: Binding,
    pub agent_id: String,
    pub lane_policy: String,
    pub lane_key: String,
    pub owner_instance_id: String,
    pub profile_digest: String,
    pub compatibility_digest: String,
    pub working_directory: String,
    pub additional_directories: Vec<String>,
    pub session_ref: Option<String>,
    pub state: SessionBindingState,
    pub active_invocation_id: Option<String>,
    pub last_quiescent_invocation_id: Option<String>,
    pub session_persistence: Option<SessionPersistence>,
    pub uncertain_reason: Option<String>,
    pub retired_reason: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub opened_at_ms: Option<i64>,
    pub retired_at_ms: Option<i64>,
}

/// Result of acquiring one logical session lane.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionAcquisition {
    pub session: SessionBinding,
    pub mode: SessionAcquisitionMode,
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::SessionBindingState;

    #[test]
    fn stored_spelling_matches_the_wire_spelling() {
        for variant in SessionBindingState::ALL {
            assert_eq!(
                serde_json::to_value(variant).expect("serialize session state"),
                Value::String(variant.as_str().to_owned()),
                "the stored and wire spellings of {variant:?} diverged",
            );
            assert_eq!(SessionBindingState::parse(variant.as_str()), Some(variant));
        }
    }

    #[test]
    fn unreadable_values_do_not_parse() {
        assert_eq!(SessionBindingState::parse("Opening"), None);
        assert_eq!(SessionBindingState::parse("quiescent"), None);
        assert_eq!(SessionBindingState::parse(""), None);
    }

    #[test]
    fn all_lists_every_variant() {
        // Adding a variant makes this match non-exhaustive, and the count below
        // then fails until `ALL` learns about it too.
        for variant in SessionBindingState::ALL {
            match variant {
                SessionBindingState::Opening
                | SessionBindingState::Ready
                | SessionBindingState::Active
                | SessionBindingState::Uncertain
                | SessionBindingState::Retired => {}
            }
        }
        assert_eq!(SessionBindingState::ALL.len(), 5);
    }
}
