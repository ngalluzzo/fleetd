use std::{
    fmt, fs,
    io::Write,
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};
use sqlx::Row;
use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::{
    error::FleetError,
    store::{Store, map_unique_conflict, now_ms, validate_name},
};
use fleetd_proto::model::{Agent, CreateAgent, IssuedCredential, RegisteredAgent};

const OPERATOR_TOKEN_PREFIX: &str = "fl_op_";
const AGENT_TOKEN_PREFIX: &str = "fl_ag_";
const TOKEN_BYTES: usize = 32;
const MAX_TOKEN_LENGTH: usize = 128;

/// The authenticated identity attached to one API request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Principal {
    Operator {
        credential_id: String,
    },
    Agent {
        credential_id: String,
        agent_id: String,
    },
}

impl Principal {
    /// Returns whether this principal has operator authority.
    #[must_use]
    pub const fn is_operator(&self) -> bool {
        matches!(self, Self::Operator { .. })
    }

    /// Returns the bound agent ID for an agent credential.
    #[must_use]
    pub fn agent_id(&self) -> Option<&str> {
        match self {
            Self::Operator { .. } => None,
            Self::Agent { agent_id, .. } => Some(agent_id),
        }
    }

    /// Returns the exact credential that authenticated this principal.
    #[must_use]
    pub fn credential_id(&self) -> &str {
        match self {
            Self::Operator { credential_id } | Self::Agent { credential_id, .. } => credential_id,
        }
    }
}

/// Result of reconciling the operator token file with credential storage.
#[derive(Clone, Debug)]
pub struct OperatorBootstrap {
    pub token_path: PathBuf,
    pub credential_rotated: bool,
}

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

    /// Reconciles a private operator token file with its database digest.
    ///
    /// The local file is authoritative. An unchanged file is a no-op, a new or
    /// unknown digest revokes every prior operator credential transactionally,
    /// and a file holding a previously revoked credential fails closed rather
    /// than resurrecting it.
    ///
    /// # Errors
    ///
    /// Returns an error when the token file cannot be secured, entropy is
    /// unavailable, the file holds a revoked credential, or credential state
    /// cannot be persisted.
    pub async fn ensure_operator_credential(
        &self,
        token_path: impl AsRef<Path>,
    ) -> Result<OperatorBootstrap, FleetError> {
        let token_path = token_path.as_ref();
        let token = ensure_token_file(token_path)?;
        validate_token(&token, OPERATOR_TOKEN_PREFIX)?;
        let digest = token_digest(&token);
        let revoked_at_ms: Option<Option<i64>> = sqlx::query_scalar(
            r"
            SELECT revoked_at_ms
            FROM auth_credentials
            WHERE principal_kind = 'operator'
              AND token_digest = ?
            ",
        )
        .bind(&digest[..])
        .fetch_optional(&self.store.pool)
        .await?;
        let credential_rotated = match revoked_at_ms {
            Some(None) => false,
            Some(Some(_)) => {
                return Err(FleetError::Credential(format!(
                    "operator token file {} holds a credential that was explicitly \
                     revoked; delete the file to provision a replacement",
                    token_path.display()
                )));
            }
            None => {
                self.rotate_operator_digest(&digest).await?;
                true
            }
        };
        Ok(OperatorBootstrap {
            token_path: token_path.to_owned(),
            credential_rotated,
        })
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
        .fetch_optional(&self.store.pool)
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
        .fetch_optional(&self.store.pool)
        .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let principal_kind: String = row.try_get("principal_kind")?;
        let agent_id: Option<String> = row.try_get("agent_id")?;
        let actual = principal_from_row(expected.credential_id(), &principal_kind, agent_id)?;
        Ok(&actual == expected)
    }

    /// Registers an agent and its first credential in one transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid or duplicate agent input, unavailable
    /// entropy, serialization failure, or persistence failure.
    pub async fn register_agent(&self, input: CreateAgent) -> Result<RegisteredAgent, FleetError> {
        validate_name("agent", &input.name)?;
        let issued = issue_credential(AGENT_TOKEN_PREFIX)?;
        let digest = token_digest(&issued.token);
        let agent = Agent {
            id: Uuid::new_v4().to_string(),
            name: input.name,
            metadata: input.metadata,
            created_at_ms: issued.created_at_ms,
        };
        let metadata_json = serde_json::to_string(&agent.metadata)?;
        let mut transaction = self.store.pool.begin().await?;
        let result = sqlx::query(
            "INSERT INTO agents (id, name, metadata_json, created_at_ms) VALUES (?, ?, ?, ?)",
        )
        .bind(&agent.id)
        .bind(&agent.name)
        .bind(metadata_json)
        .bind(agent.created_at_ms)
        .execute(&mut *transaction)
        .await;
        map_unique_conflict(result, "agent name")?;
        insert_agent_credential(&mut transaction, &agent.id, &issued, &digest).await?;
        transaction.commit().await?;
        Ok(RegisteredAgent {
            agent,
            credential: issued,
        })
    }

    /// Revokes every active credential for an agent and returns a replacement.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown agent, unavailable entropy, or a
    /// persistence failure.
    pub async fn rotate_agent_credential(
        &self,
        agent_id: &str,
    ) -> Result<IssuedCredential, FleetError> {
        let issued = issue_credential(AGENT_TOKEN_PREFIX)?;
        let digest = token_digest(&issued.token);
        let mut transaction = self.store.pool.begin().await?;
        let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM agents WHERE id = ?")
            .bind(agent_id)
            .fetch_one(&mut *transaction)
            .await?;
        if exists == 0 {
            return Err(FleetError::NotFound {
                entity: "agent",
                id: agent_id.to_owned(),
            });
        }
        sqlx::query(
            r"
            UPDATE auth_credentials
            SET revoked_at_ms = ?
            WHERE principal_kind = 'agent'
              AND agent_id = ?
              AND revoked_at_ms IS NULL
            ",
        )
        .bind(issued.created_at_ms)
        .bind(agent_id)
        .execute(&mut *transaction)
        .await?;
        insert_agent_credential(&mut transaction, agent_id, &issued, &digest).await?;
        transaction.commit().await?;
        Ok(issued)
    }

    async fn rotate_operator_digest(&self, digest: &[u8; 32]) -> Result<(), FleetError> {
        let now = now_ms();
        let mut transaction = self.store.pool.begin().await?;
        sqlx::query(
            r"
            UPDATE auth_credentials
            SET revoked_at_ms = ?
            WHERE principal_kind = 'operator' AND revoked_at_ms IS NULL
            ",
        )
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r"
            INSERT INTO auth_credentials (
                id, principal_kind, token_digest, created_at_ms
            ) VALUES (?, 'operator', ?, ?)
            ",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(&digest[..])
        .bind(now)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }
}

async fn insert_agent_credential(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    agent_id: &str,
    issued: &IssuedCredential,
    digest: &[u8; 32],
) -> Result<(), FleetError> {
    sqlx::query(
        r"
        INSERT INTO auth_credentials (
            id, principal_kind, agent_id, token_digest, created_at_ms
        ) VALUES (?, 'agent', ?, ?, ?)
        ",
    )
    .bind(&issued.id)
    .bind(agent_id)
    .bind(&digest[..])
    .bind(issued.created_at_ms)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

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

fn issue_credential(prefix: &str) -> Result<IssuedCredential, FleetError> {
    Ok(IssuedCredential {
        id: Uuid::new_v4().to_string(),
        token: generate_token(prefix)?,
        created_at_ms: now_ms(),
    })
}

fn generate_token(prefix: &str) -> Result<String, FleetError> {
    let mut bytes = [0_u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes)
        .map_err(|error| FleetError::Credential(format!("secure entropy unavailable: {error}")))?;
    Ok(format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

fn token_digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn validate_token(token: &str, prefix: &str) -> Result<(), FleetError> {
    if token.len() > MAX_TOKEN_LENGTH || !token.starts_with(prefix) {
        return Err(FleetError::Credential(
            "operator token file contains an invalid credential".to_owned(),
        ));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(&token[prefix.len()..])
        .map_err(|_| {
            FleetError::Credential("operator token file contains invalid encoding".to_owned())
        })?;
    if decoded.len() != TOKEN_BYTES {
        return Err(FleetError::Credential(
            "operator token file contains invalid entropy".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_token_file(path: &Path) -> Result<String, FleetError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    if !path.exists() {
        persist_new_token(path, &generate_token(OPERATOR_TOKEN_PREFIX)?)?;
    }
    secure_file_permissions(path)?;
    let token = fs::read_to_string(path)?;
    Ok(token.trim().to_owned())
}

fn persist_new_token(path: &Path, token: &str) -> Result<(), FleetError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let mut temporary = NamedTempFile::new_in(parent)?;
    secure_file_permissions(temporary.path())?;
    writeln!(temporary, "{token}")?;
    temporary.as_file().sync_all()?;
    match temporary.persist_noclobber(path) {
        Ok(_) => Ok(()),
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(FleetError::Io(error.error)),
    }
}

#[cfg(unix)]
fn secure_file_permissions(path: &Path) -> Result<(), FleetError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn secure_file_permissions(_path: &Path) -> Result<(), FleetError> {
    Err(FleetError::Credential(
        "secure operator token files are not implemented on this platform".to_owned(),
    ))
}
