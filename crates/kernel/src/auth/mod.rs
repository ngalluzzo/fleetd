//! Credential issuance and authentication over a durable store.
//!
//! Verification is one path and issuance is several, which is the split this
//! module is arranged around. Every credential is looked up the same way -- a
//! digest against active rows -- so [`AuthService::authenticate`] lives here and
//! is the only place a raw token becomes a [`Principal`]. How a credential comes
//! to exist differs by what it authenticates, so each authority category owns
//! its own file and adds its own `impl AuthService` block.
//!
//! Raw token material never leaves this module tree's `token` file. Everything
//! durable holds a digest, and nothing in fleetd can recover a token it issued.

mod agent;
mod operator;
mod principal;
mod token;

use std::fmt;

use sqlx::Row;

pub use operator::OperatorBootstrap;
pub use principal::Principal;

use crate::{error::FleetError, store::Store};

use token::{MAX_TOKEN_LENGTH, token_digest};

/// Credential issuance and authentication over a durable store.
#[derive(Clone)]
pub struct AuthService {
    store: Store,
}

impl fmt::Debug for AuthService {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthService")
            .finish_non_exhaustive()
    }
}

impl AuthService {
    /// Creates an authentication service over the supplied store.
    #[must_use]
    pub const fn new(store: Store) -> Self {
        Self { store }
    }

    /// Authenticates a raw bearer token against active credential digests.
    ///
    /// # Errors
    ///
    /// Returns [`FleetError::Unauthorized`] for malformed, unknown, or revoked
    /// tokens and a persistence error if credential lookup fails.
    pub async fn authenticate(&self, token: &str) -> Result<Principal, FleetError> {
        if token.is_empty() || token.len() > MAX_TOKEN_LENGTH {
            return Err(FleetError::Unauthorized);
        }
        let digest = token_digest(token);
        let row = sqlx::query(
            r"
            SELECT id, principal_kind, agent_id
            FROM auth_credentials
            WHERE token_digest = ? AND revoked_at_ms IS NULL
            ",
        )
        .bind(&digest[..])
        .fetch_optional(self.store.pool())
        .await?
        .ok_or(FleetError::Unauthorized)?;
        let credential_id: String = row.try_get("id")?;
        let principal_kind: String = row.try_get("principal_kind")?;
        let agent_id: Option<String> = row.try_get("agent_id")?;
        principal_from_row(&credential_id, &principal_kind, agent_id)
    }

    /// Revalidates an exact credential-bound principal without accepting or
    /// returning raw credential material.
    ///
    /// # Errors
    ///
    /// Returns an error when the credential can no longer be read.
    pub async fn revalidate_principal(&self, expected: &Principal) -> Result<bool, FleetError> {
        let row = sqlx::query(
            r"
            SELECT principal_kind, agent_id
            FROM auth_credentials
            WHERE id = ? AND revoked_at_ms IS NULL
            ",
        )
        .bind(expected.credential_id())
        .fetch_optional(self.store.pool())
        .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let principal_kind: String = row.try_get("principal_kind")?;
        let agent_id: Option<String> = row.try_get("agent_id")?;
        let actual = principal_from_row(expected.credential_id(), &principal_kind, agent_id)?;
        Ok(&actual == expected)
    }
}

/// Reads a stored credential row back as the principal it authenticates.
///
/// The stored shape and the enum are checked against each other rather than
/// assumed to agree: the row's `CHECK` constraints hold the same invariant, and
/// a row that satisfies neither is refused instead of being read as the weaker
/// of the two.
fn principal_from_row(
    credential_id: &str,
    principal_kind: &str,
    agent_id: Option<String>,
) -> Result<Principal, FleetError> {
    match (principal_kind, agent_id) {
        ("operator", None) => Ok(Principal::Operator {
            credential_id: credential_id.to_owned(),
        }),
        ("agent", Some(agent_id)) => Ok(Principal::Agent {
            credential_id: credential_id.to_owned(),
            agent_id,
        }),
        _ => Err(FleetError::Credential(
            "credential principal invariant is invalid".to_owned(),
        )),
    }
}
