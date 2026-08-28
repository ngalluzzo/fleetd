//! Bearer token material: how one is made, and how it is reduced to a digest.
//!
//! Nothing here knows what kind of principal a token will authenticate. That is
//! the point of keeping it separate: issuance is the same operation for every
//! credential, and only the prefix distinguishes them for a human reading a
//! token out of a file.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{error::FleetError, store::now_ms};
use fleetd_proto::model::IssuedCredential;

pub(super) const OPERATOR_TOKEN_PREFIX: &str = "fl_op_";
pub(super) const AGENT_TOKEN_PREFIX: &str = "fl_ag_";
pub(super) const TOKEN_BYTES: usize = 32;
pub(super) const MAX_TOKEN_LENGTH: usize = 128;

/// Mints one credential: an identity, its raw token, and when it was issued.
///
/// The raw token exists in memory exactly once, here. Everything durable holds
/// only [`token_digest`] of it.
pub(super) fn issue_credential(prefix: &str) -> Result<IssuedCredential, FleetError> {
    Ok(IssuedCredential {
        id: Uuid::new_v4().to_string(),
        token: generate_token(prefix)?,
        created_at_ms: now_ms(),
    })
}

pub(super) fn generate_token(prefix: &str) -> Result<String, FleetError> {
    let mut bytes = [0_u8; TOKEN_BYTES];
    getrandom::fill(&mut bytes)
        .map_err(|error| FleetError::Credential(format!("secure entropy unavailable: {error}")))?;
    Ok(format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

pub(super) fn token_digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// Checks that a token read back from outside fleetd is shaped like one fleetd
/// issued, before it is used to look anything up.
pub(super) fn validate_token(token: &str, prefix: &str, source: &str) -> Result<(), FleetError> {
    if token.len() > MAX_TOKEN_LENGTH || !token.starts_with(prefix) {
        return Err(FleetError::Credential(format!(
            "{source} contains an invalid credential"
        )));
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(&token[prefix.len()..])
        .map_err(|_| FleetError::Credential(format!("{source} contains invalid encoding")))?;
    if decoded.len() != TOKEN_BYTES {
        return Err(FleetError::Credential(format!(
            "{source} contains invalid entropy"
        )));
    }
    Ok(())
}
