//! Operator authority, and the private file that holds it.
//!
//! An operator credential is the one fleetd provisions rather than issues on
//! request, because the first operator has nobody to ask. The local file is
//! authoritative and the database follows it, which is what lets an operator
//! recover from a lost token by deleting a file they own.

use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
};

use tempfile::NamedTempFile;
use uuid::Uuid;

use crate::{error::FleetError, store::now_ms};

use super::{
    AuthService,
    token::{OPERATOR_TOKEN_PREFIX, generate_token, token_digest, validate_token},
};

/// Result of reconciling the operator token file with credential storage.
#[derive(Clone, Debug)]
pub struct OperatorBootstrap {
    pub token_path: PathBuf,
    pub credential_rotated: bool,
}

impl AuthService {
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
        validate_token(&token, OPERATOR_TOKEN_PREFIX, "operator token file")?;
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
        .fetch_optional(self.store.pool())
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

    async fn rotate_operator_digest(&self, digest: &[u8; 32]) -> Result<(), FleetError> {
        let now = now_ms();
        let mut transaction = self.store.pool().begin().await?;
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
