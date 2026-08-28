//! The authenticated identity attached to one request.

/// Who is acting, and the exact credential that established it.
///
/// The variants are the system's authority categories, and there are
/// deliberately few. An operator holds standing authority over everything; an
/// agent holds it over itself; a trigger holds exactly what its registration
/// declared, and nothing about the fleet it cannot already see.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Principal {
    Operator {
        credential_id: String,
    },
    Agent {
        credential_id: String,
        agent_id: String,
    },
    Trigger {
        credential_id: String,
        trigger_id: String,
    },
}

impl Principal {
    /// Returns whether this principal has operator authority.
    #[must_use]
    pub const fn is_operator(&self) -> bool {
        matches!(self, Self::Operator { .. })
    }

    /// Returns the bound agent ID for an agent credential.
    ///
    /// A trigger has a sender, but it is derived from the registration rather
    /// than carried by the credential, so a trigger is not an agent here. That
    /// is what keeps a trigger from reaching anything an agent may reach.
    #[must_use]
    pub fn agent_id(&self) -> Option<&str> {
        match self {
            Self::Operator { .. } | Self::Trigger { .. } => None,
            Self::Agent { agent_id, .. } => Some(agent_id),
        }
    }

    /// Returns the bound trigger ID for a trigger credential.
    #[must_use]
    pub fn trigger_id(&self) -> Option<&str> {
        match self {
            Self::Operator { .. } | Self::Agent { .. } => None,
            Self::Trigger { trigger_id, .. } => Some(trigger_id),
        }
    }

    /// Returns the exact credential that authenticated this principal.
    #[must_use]
    pub fn credential_id(&self) -> &str {
        match self {
            Self::Operator { credential_id }
            | Self::Agent { credential_id, .. }
            | Self::Trigger { credential_id, .. } => credential_id,
        }
    }
}
