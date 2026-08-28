//! Credentials bound to one inbound trigger.
//!
//! This is the third authority category, and the narrowest standing one. An
//! operator credential may do anything; an agent credential may act as that
//! agent. A trigger credential may only fire the trigger it names, and what that
//! trigger may create was fixed when it was registered.
//!
//! Registration issues the credential in the same transaction, because a trigger
//! holding none is inert and a credential naming a trigger that does not exist
//! is authority over nothing. Retirement revokes in the same transaction for the
//! same reason: a standing grant that outlived its registration is exactly the
//! failure a standing grant is worth worrying about.

use crate::{
    error::FleetError,
    store::{
        now_ms,
        trigger::{insert_trigger, new_trigger, retire_trigger_row},
    },
};
use fleetd_proto::{
    model::IssuedCredential,
    trigger::{RegisterTrigger, RegisteredTrigger, Trigger},
};

use super::{
    AuthService,
    token::{TRIGGER_TOKEN_PREFIX, issue_credential, token_digest},
};

/// A reason nobody can read is the same as no reason recorded.
const MAX_RETIREMENT_REASON_BYTES: usize = 512;

impl AuthService {
    /// Registers a trigger and the credential that lets it fire, in one
    /// transaction.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid or duplicate declaration, an unknown
    /// channel or sender, unavailable entropy, or a persistence failure.
    pub async fn register_trigger(
        &self,
        input: RegisterTrigger,
    ) -> Result<RegisteredTrigger, FleetError> {
        let issued = issue_credential(TRIGGER_TOKEN_PREFIX)?;
        let digest = token_digest(&issued.token);
        let trigger = new_trigger(input, issued.created_at_ms)?;
        let mut transaction = self.store.begin_immediate().await?;
        insert_trigger(&mut transaction, &trigger).await?;
        sqlx::query(
            r"
            INSERT INTO auth_credentials (
                id, principal_kind, trigger_id, token_digest, created_at_ms
            ) VALUES (?, 'trigger', ?, ?, ?)
            ",
        )
        .bind(&issued.id)
        .bind(&trigger.id)
        .bind(&digest[..])
        .bind(issued.created_at_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(RegisteredTrigger {
            trigger,
            credential: issued,
        })
    }

    /// Retires a trigger and revokes every credential that could fire it.
    ///
    /// Idempotent: retiring an already-retired trigger reports the row as it
    /// stands, keeping the reason recorded the first time, because that is the
    /// one describing why it stopped.
    ///
    /// # Errors
    ///
    /// Returns not found for an unknown trigger, or a persistence failure.
    pub async fn retire_trigger(
        &self,
        trigger_id: &str,
        reason: &str,
    ) -> Result<Trigger, FleetError> {
        if reason.trim().is_empty() || reason.len() > MAX_RETIREMENT_REASON_BYTES {
            return Err(FleetError::Invalid(format!(
                "a retirement reason must contain between 1 and \
                 {MAX_RETIREMENT_REASON_BYTES} bytes"
            )));
        }
        let now = now_ms();
        let mut transaction = self.store.begin_immediate().await?;
        if retire_trigger_row(&mut transaction, trigger_id, reason, now).await? {
            revoke_trigger_credentials(&mut transaction, trigger_id, now).await?;
        }
        transaction.commit().await?;
        self.store.get_trigger(trigger_id).await
    }

    /// Revokes every active credential for a trigger and returns a replacement.
    ///
    /// A retired trigger has no replacement to issue: its authority is over, and
    /// rotating one back into existence would make retirement reversible by
    /// anyone who could rotate.
    ///
    /// # Errors
    ///
    /// Returns not found for an unknown trigger, a conflict for a retired one,
    /// unavailable entropy, or a persistence failure.
    pub async fn rotate_trigger_credential(
        &self,
        trigger_id: &str,
    ) -> Result<IssuedCredential, FleetError> {
        let issued = issue_credential(TRIGGER_TOKEN_PREFIX)?;
        let digest = token_digest(&issued.token);
        let trigger = self.store.get_trigger(trigger_id).await?;
        if trigger.state != fleetd_proto::trigger::TriggerState::Active {
            return Err(FleetError::Conflict(format!(
                "trigger {trigger_id} is retired and cannot hold a credential"
            )));
        }
        let mut transaction = self.store.begin_immediate().await?;
        revoke_trigger_credentials(&mut transaction, trigger_id, issued.created_at_ms).await?;
        sqlx::query(
            r"
            INSERT INTO auth_credentials (
                id, principal_kind, trigger_id, token_digest, created_at_ms
            ) VALUES (?, 'trigger', ?, ?, ?)
            ",
        )
        .bind(&issued.id)
        .bind(trigger_id)
        .bind(&digest[..])
        .bind(issued.created_at_ms)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(issued)
    }
}

async fn revoke_trigger_credentials(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    trigger_id: &str,
    now_ms: i64,
) -> Result<(), FleetError> {
    sqlx::query(
        r"
        UPDATE auth_credentials
        SET revoked_at_ms = ?
        WHERE principal_kind = 'trigger'
          AND trigger_id = ?
          AND revoked_at_ms IS NULL
        ",
    )
    .bind(now_ms)
    .bind(trigger_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}
