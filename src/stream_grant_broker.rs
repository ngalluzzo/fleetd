use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::browser_stream_edge::{
    MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL, MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON,
    MAX_UNUSED_GRANTS_PER_CREDENTIAL, MAX_UNUSED_GRANTS_PER_DAEMON, STREAM_GRANT_ENTROPY_BYTES,
    STREAM_GRANT_PREFIX, STREAM_GRANT_REDEMPTION_LIFETIME,
};
use crate::{auth::AuthService, channel_stream::AuthorizedChannelStream, error::FleetError};

const STREAM_GRANT_ENCODED_LENGTH: usize = 43;
const MAX_PROTOCOL_LENGTH: usize = 128;
const MAX_ENTROPY_ATTEMPTS: usize = 4;

/// One-time stream-grant authority broker owned by one daemon process.
#[derive(Clone)]
pub(crate) struct StreamGrantBroker {
    shared: Arc<BrokerShared>,
}

struct BrokerShared {
    state: Mutex<BrokerState>,
    limits: BrokerLimits,
    auth: AuthService,
}

#[derive(Clone, Copy)]
struct BrokerLimits {
    grant_lifetime: Duration,
    unused_per_credential: usize,
    unused_total: usize,
    active_per_credential: usize,
    active_total: usize,
}

impl Default for BrokerLimits {
    fn default() -> Self {
        Self {
            grant_lifetime: STREAM_GRANT_REDEMPTION_LIFETIME,
            unused_per_credential: MAX_UNUSED_GRANTS_PER_CREDENTIAL,
            unused_total: MAX_UNUSED_GRANTS_PER_DAEMON,
            active_per_credential: MAX_ACTIVE_BROWSER_STREAMS_PER_CREDENTIAL,
            active_total: MAX_ACTIVE_BROWSER_STREAMS_PER_DAEMON,
        }
    }
}

#[derive(Default)]
struct BrokerState {
    unused: HashMap<[u8; 32], GrantRecord>,
    unused_by_credential: HashMap<String, usize>,
    active_by_credential: HashMap<String, usize>,
    active_total: usize,
}

struct GrantRecord {
    authorization: AuthorizedChannelStream,
    protocol: String,
    expires_at: Instant,
}

/// A raw stream grant returned exactly once to the internal issuance caller.
pub(crate) struct IssuedStreamGrant {
    grant: String,
    lifetime: Duration,
}

impl IssuedStreamGrant {
    #[cfg(test)]
    pub(crate) fn expose(&self) -> &str {
        &self.grant
    }

    #[cfg(test)]
    pub(crate) const fn lifetime(&self) -> Duration {
        self.lifetime
    }

    pub(crate) fn into_parts(self) -> (String, Duration) {
        (self.grant, self.lifetime)
    }
}

impl fmt::Debug for IssuedStreamGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedStreamGrant")
            .field("grant", &"[REDACTED]")
            .field("lifetime", &self.lifetime)
            .finish()
    }
}

/// An authorized stream plus the active-capacity slot held for its lifetime.
pub(crate) struct RedeemedStreamGrant {
    authorization: AuthorizedChannelStream,
    active_slot: ActiveStreamSlot,
}

impl RedeemedStreamGrant {
    #[cfg(test)]
    pub(crate) const fn authorization(&self) -> &AuthorizedChannelStream {
        &self.authorization
    }

    pub(crate) fn into_parts(self) -> (AuthorizedChannelStream, ActiveStreamSlot) {
        (self.authorization, self.active_slot)
    }
}

impl fmt::Debug for RedeemedStreamGrant {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedeemedStreamGrant")
            .field("authorization", &self.authorization)
            .field("active_slot", &self.active_slot)
            .finish()
    }
}

/// RAII ownership of one active browser-stream capacity slot.
pub(crate) struct ActiveStreamSlot {
    shared: Arc<BrokerShared>,
    credential_id: String,
}

impl fmt::Debug for ActiveStreamSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActiveStreamSlot")
            .field("credential_id", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl Drop for ActiveStreamSlot {
    fn drop(&mut self) {
        let mut state = lock_state(&self.shared);
        decrement_count(&mut state.active_by_credential, &self.credential_id);
        debug_assert!(state.active_total > 0, "active slot count underflow");
        state.active_total -= 1;
    }
}

/// Fixed failure classes that never contain raw grant material.
#[derive(Debug, Error)]
pub(crate) enum StreamGrantBrokerError {
    #[error("stream grant capacity exhausted")]
    Capacity,
    #[error("invalid stream grant scope")]
    InvalidScope,
    #[error("stream grant rejected")]
    Rejected,
    #[error("secure stream grant entropy unavailable")]
    Entropy,
    #[error("stream grant credential revalidation failed")]
    Revalidation(#[source] FleetError),
}

impl fmt::Debug for StreamGrantBroker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamGrantBroker")
            .finish_non_exhaustive()
    }
}

impl StreamGrantBroker {
    pub(crate) fn new(auth: AuthService) -> Self {
        Self::with_limits(auth, BrokerLimits::default())
    }

    fn with_limits(auth: AuthService, limits: BrokerLimits) -> Self {
        Self {
            shared: Arc::new(BrokerShared {
                state: Mutex::new(BrokerState::default()),
                limits,
                auth,
            }),
        }
    }

    /// Issues one digest-only, process-local grant for an already-authorized
    /// exact stream scope.
    pub(crate) fn issue(
        &self,
        authorization: AuthorizedChannelStream,
        protocol: &str,
    ) -> Result<IssuedStreamGrant, StreamGrantBrokerError> {
        if authorization.after() < 0 || protocol.is_empty() || protocol.len() > MAX_PROTOCOL_LENGTH
        {
            return Err(StreamGrantBrokerError::InvalidScope);
        }

        let now = Instant::now();
        let credential_id = authorization.credential_id().to_owned();
        let mut state = lock_state(&self.shared);
        prune_expired(&mut state, now);
        if state.unused.len() >= self.shared.limits.unused_total
            || state
                .unused_by_credential
                .get(&credential_id)
                .copied()
                .unwrap_or_default()
                >= self.shared.limits.unused_per_credential
        {
            return Err(StreamGrantBrokerError::Capacity);
        }

        let (grant, digest) = generate_unique_grant(&state.unused)?;
        state.unused.insert(
            digest,
            GrantRecord {
                authorization,
                protocol: protocol.to_owned(),
                expires_at: now + self.shared.limits.grant_lifetime,
            },
        );
        increment_count(&mut state.unused_by_credential, &credential_id);
        Ok(IssuedStreamGrant {
            grant,
            lifetime: self.shared.limits.grant_lifetime,
        })
    }

    /// Atomically consumes a grant, revalidates its issuing credential, and
    /// reserves bounded active-stream capacity.
    pub(crate) async fn redeem(
        &self,
        grant: &str,
        protocol: &str,
    ) -> Result<RedeemedStreamGrant, StreamGrantBrokerError> {
        let digest = grant_digest(grant).ok_or(StreamGrantBrokerError::Rejected)?;
        let record = {
            let mut state = lock_state(&self.shared);
            let record = state
                .unused
                .remove(&digest)
                .ok_or(StreamGrantBrokerError::Rejected)?;
            decrement_count(
                &mut state.unused_by_credential,
                record.authorization.credential_id(),
            );
            record
        };

        if Instant::now() >= record.expires_at || record.protocol != protocol {
            return Err(StreamGrantBrokerError::Rejected);
        }
        let principal = record.authorization.issuing_principal();
        if !self
            .shared
            .auth
            .revalidate_principal(&principal)
            .await
            .map_err(StreamGrantBrokerError::Revalidation)?
        {
            return Err(StreamGrantBrokerError::Rejected);
        }
        let active_slot = self.reserve_active(record.authorization.credential_id())?;
        Ok(RedeemedStreamGrant {
            authorization: record.authorization,
            active_slot,
        })
    }

    /// Returns whether an upgrade can proceed without already exceeding the
    /// daemon-wide active-stream bound. Redemption still performs the atomic
    /// capacity reservation because multiple pre-authentication sockets may
    /// race after this advisory check.
    pub(crate) fn has_global_active_capacity(&self) -> bool {
        let state = lock_state(&self.shared);
        state.active_total < self.shared.limits.active_total
    }

    fn reserve_active(
        &self,
        credential_id: &str,
    ) -> Result<ActiveStreamSlot, StreamGrantBrokerError> {
        let mut state = lock_state(&self.shared);
        let credential_active = state
            .active_by_credential
            .get(credential_id)
            .copied()
            .unwrap_or_default();
        if state.active_total >= self.shared.limits.active_total
            || credential_active >= self.shared.limits.active_per_credential
        {
            return Err(StreamGrantBrokerError::Rejected);
        }
        state.active_total += 1;
        increment_count(&mut state.active_by_credential, credential_id);
        Ok(ActiveStreamSlot {
            shared: Arc::clone(&self.shared),
            credential_id: credential_id.to_owned(),
        })
    }
}

fn lock_state(shared: &BrokerShared) -> MutexGuard<'_, BrokerState> {
    shared
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn generate_unique_grant(
    existing: &HashMap<[u8; 32], GrantRecord>,
) -> Result<(String, [u8; 32]), StreamGrantBrokerError> {
    for _ in 0..MAX_ENTROPY_ATTEMPTS {
        let mut entropy = [0_u8; STREAM_GRANT_ENTROPY_BYTES];
        getrandom::fill(&mut entropy).map_err(|_| StreamGrantBrokerError::Entropy)?;
        let grant = format!("{STREAM_GRANT_PREFIX}{}", URL_SAFE_NO_PAD.encode(entropy));
        let digest: [u8; 32] = Sha256::digest(grant.as_bytes()).into();
        if !existing.contains_key(&digest) {
            return Ok((grant, digest));
        }
    }
    Err(StreamGrantBrokerError::Entropy)
}

fn grant_digest(grant: &str) -> Option<[u8; 32]> {
    let encoded = grant.strip_prefix(STREAM_GRANT_PREFIX)?;
    if encoded.len() != STREAM_GRANT_ENCODED_LENGTH {
        return None;
    }
    let entropy = URL_SAFE_NO_PAD.decode(encoded).ok()?;
    if entropy.len() != STREAM_GRANT_ENTROPY_BYTES {
        return None;
    }
    Some(Sha256::digest(grant.as_bytes()).into())
}

fn prune_expired(state: &mut BrokerState, now: Instant) {
    let expired: Vec<_> = state
        .unused
        .iter()
        .filter_map(|(digest, record)| (now >= record.expires_at).then_some(*digest))
        .collect();
    for digest in expired {
        if let Some(record) = state.unused.remove(&digest) {
            decrement_count(
                &mut state.unused_by_credential,
                record.authorization.credential_id(),
            );
        }
    }
}

fn increment_count(counts: &mut HashMap<String, usize>, credential_id: &str) {
    *counts.entry(credential_id.to_owned()).or_default() += 1;
}

fn decrement_count(counts: &mut HashMap<String, usize>, credential_id: &str) {
    let count = counts
        .get_mut(credential_id)
        .expect("broker count must exist before decrement");
    debug_assert!(*count > 0, "broker count underflow");
    *count -= 1;
    if *count == 0 {
        counts.remove(credential_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::{auth::Principal, model::CreateAgent, store::Store};

    const PROTOCOL: &str = "fleetd.channel-stream.browser.v1";

    struct Fixture {
        directory: tempfile::TempDir,
        auth: AuthService,
        principal: Principal,
    }

    async fn fixture(name: &str) -> Fixture {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = Store::open(directory.path().join("fleetd.db"))
            .await
            .expect("open store");
        let auth = AuthService::new(store);
        let registration = auth
            .register_agent(CreateAgent {
                name: name.to_owned(),
                metadata: json!({}),
            })
            .await
            .expect("register agent");
        let principal = auth
            .authenticate(&registration.credential.token)
            .await
            .expect("authenticate agent");
        Fixture {
            directory,
            auth,
            principal,
        }
    }

    fn authorization(principal: &Principal, channel: &str, after: i64) -> AuthorizedChannelStream {
        AuthorizedChannelStream::from_principal(channel.to_owned(), after, principal)
    }

    fn limits(
        lifetime: Duration,
        unused_per_credential: usize,
        unused_total: usize,
        active_per_credential: usize,
        active_total: usize,
    ) -> BrokerLimits {
        BrokerLimits {
            grant_lifetime: lifetime,
            unused_per_credential,
            unused_total,
            active_per_credential,
            active_total,
        }
    }

    #[tokio::test]
    async fn grant_has_256_bits_and_is_digest_only_and_debug_redacted() {
        let fixture = fixture("redacted").await;
        let broker = StreamGrantBroker::new(fixture.auth.clone());
        let issued = broker
            .issue(authorization(&fixture.principal, "channel", 42), PROTOCOL)
            .expect("issue grant");
        let raw = issued.expose().to_owned();
        let entropy = URL_SAFE_NO_PAD
            .decode(raw.strip_prefix(STREAM_GRANT_PREFIX).expect("grant prefix"))
            .expect("grant encoding");
        assert_eq!(entropy.len(), STREAM_GRANT_ENTROPY_BYTES);
        assert_eq!(issued.lifetime(), Duration::from_secs(15));
        assert!(!format!("{issued:?}").contains(&raw));
        assert!(!format!("{broker:?}").contains(&raw));

        let state = lock_state(&broker.shared);
        let digest: [u8; 32] = Sha256::digest(raw.as_bytes()).into();
        assert!(state.unused.contains_key(&digest));
    }

    #[tokio::test]
    async fn redemption_preserves_exact_scope_and_is_single_use() {
        let fixture = fixture("single-use").await;
        let broker = StreamGrantBroker::new(fixture.auth.clone());
        let issued = broker
            .issue(
                authorization(&fixture.principal, "exact-channel", 42),
                PROTOCOL,
            )
            .expect("issue grant");
        let raw = issued.expose().to_owned();
        let redeemed = broker.redeem(&raw, PROTOCOL).await.expect("redeem grant");
        assert_eq!(redeemed.authorization().channel_id(), "exact-channel");
        assert_eq!(redeemed.authorization().after(), 42);
        assert_eq!(
            redeemed.authorization().issuing_principal(),
            fixture.principal
        );
        assert!(matches!(
            broker.redeem(&raw, PROTOCOL).await,
            Err(StreamGrantBrokerError::Rejected)
        ));
    }

    #[tokio::test]
    async fn operator_principal_shape_is_bound_and_revalidated() {
        let fixture = fixture("operator-fixture-agent").await;
        let token_path = fixture.directory.path().join("operator.token");
        fixture
            .auth
            .ensure_operator_credential(&token_path)
            .await
            .expect("provision operator credential");
        let token = std::fs::read_to_string(token_path).expect("read operator token");
        let operator = fixture
            .auth
            .authenticate(token.trim())
            .await
            .expect("authenticate operator");
        assert!(operator.is_operator());

        let broker = StreamGrantBroker::new(fixture.auth.clone());
        let issued = broker
            .issue(authorization(&operator, "operator-channel", 7), PROTOCOL)
            .expect("issue operator grant");
        let redeemed = broker
            .redeem(issued.expose(), PROTOCOL)
            .await
            .expect("redeem operator grant");
        assert_eq!(redeemed.authorization().issuing_principal(), operator);
    }

    #[tokio::test]
    async fn concurrent_redemption_has_exactly_one_winner() {
        let fixture = fixture("race").await;
        let broker = Arc::new(StreamGrantBroker::new(fixture.auth.clone()));
        let raw = broker
            .issue(authorization(&fixture.principal, "channel", 0), PROTOCOL)
            .expect("issue grant")
            .expose()
            .to_owned();
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let broker = Arc::clone(&broker);
            let raw = raw.clone();
            tasks.push(tokio::spawn(async move {
                broker.redeem(&raw, PROTOCOL).await.is_ok()
            }));
        }
        let mut winners = 0;
        for task in tasks {
            winners += usize::from(task.await.expect("redemption task"));
        }
        assert_eq!(winners, 1);
    }

    #[tokio::test]
    async fn expiry_is_monotonic_and_expired_capacity_is_pruned() {
        let fixture = fixture("expiry").await;
        let broker = StreamGrantBroker::with_limits(
            fixture.auth.clone(),
            limits(Duration::from_millis(1), 1, 1, 1, 1),
        );
        let expired = broker
            .issue(authorization(&fixture.principal, "channel", 0), PROTOCOL)
            .expect("issue expiring grant")
            .expose()
            .to_owned();
        tokio::time::sleep(Duration::from_millis(10)).await;
        broker
            .issue(authorization(&fixture.principal, "channel", 1), PROTOCOL)
            .expect("expired grant is pruned before capacity check");
        assert!(matches!(
            broker.redeem(&expired, PROTOCOL).await,
            Err(StreamGrantBrokerError::Rejected)
        ));
    }

    #[tokio::test]
    async fn unused_and_active_bounds_recover_capacity() {
        let fixture = fixture("bounds").await;
        let broker = StreamGrantBroker::with_limits(
            fixture.auth.clone(),
            limits(Duration::from_secs(15), 1, 1, 1, 1),
        );
        let first = broker
            .issue(authorization(&fixture.principal, "channel", 0), PROTOCOL)
            .expect("first grant");
        assert!(matches!(
            broker.issue(authorization(&fixture.principal, "channel", 0), PROTOCOL),
            Err(StreamGrantBrokerError::Capacity)
        ));
        let active = broker
            .redeem(first.expose(), PROTOCOL)
            .await
            .expect("first active stream");
        let second = broker
            .issue(authorization(&fixture.principal, "channel", 1), PROTOCOL)
            .expect("redemption releases unused capacity");
        assert!(matches!(
            broker.redeem(second.expose(), PROTOCOL).await,
            Err(StreamGrantBrokerError::Rejected)
        ));
        drop(active);
        let third = broker
            .issue(authorization(&fixture.principal, "channel", 2), PROTOCOL)
            .expect("issue after active release");
        let redeemed = broker
            .redeem(third.expose(), PROTOCOL)
            .await
            .expect("RAII release restores active capacity");
        let (_, slot) = redeemed.into_parts();
        drop(slot);
    }

    #[tokio::test]
    async fn global_unused_and_active_bounds_apply_across_credentials() {
        let first = fixture("global-first").await;
        let second_registration = first
            .auth
            .register_agent(CreateAgent {
                name: "global-second".to_owned(),
                metadata: json!({}),
            })
            .await
            .expect("register second agent");
        let second_principal = first
            .auth
            .authenticate(&second_registration.credential.token)
            .await
            .expect("authenticate second agent");
        let broker = StreamGrantBroker::with_limits(
            first.auth.clone(),
            limits(Duration::from_secs(15), 2, 1, 2, 1),
        );
        let first_grant = broker
            .issue(authorization(&first.principal, "channel", 0), PROTOCOL)
            .expect("first grant");
        assert!(matches!(
            broker.issue(authorization(&second_principal, "channel", 0), PROTOCOL),
            Err(StreamGrantBrokerError::Capacity)
        ));
        let first_active = broker
            .redeem(first_grant.expose(), PROTOCOL)
            .await
            .expect("first active stream");
        let second_grant = broker
            .issue(authorization(&second_principal, "channel", 0), PROTOCOL)
            .expect("unused global capacity released");
        assert!(matches!(
            broker.redeem(second_grant.expose(), PROTOCOL).await,
            Err(StreamGrantBrokerError::Rejected)
        ));
        drop(first_active);
        let replacement = broker
            .issue(authorization(&second_principal, "channel", 1), PROTOCOL)
            .expect("issue replacement grant");
        broker
            .redeem(replacement.expose(), PROTOCOL)
            .await
            .expect("active global capacity released");
    }

    #[tokio::test]
    async fn protocol_mismatch_and_other_broker_consume_no_authority() {
        let fixture = fixture("scope").await;
        let issuer = StreamGrantBroker::new(fixture.auth.clone());
        let other_process = StreamGrantBroker::new(fixture.auth.clone());
        let raw = issuer
            .issue(authorization(&fixture.principal, "channel", 0), PROTOCOL)
            .expect("issue grant")
            .expose()
            .to_owned();
        assert!(matches!(
            other_process.redeem(&raw, PROTOCOL).await,
            Err(StreamGrantBrokerError::Rejected)
        ));
        assert!(matches!(
            issuer.redeem(&raw, "another-protocol").await,
            Err(StreamGrantBrokerError::Rejected)
        ));
        assert!(matches!(
            issuer.redeem(&raw, PROTOCOL).await,
            Err(StreamGrantBrokerError::Rejected)
        ));
    }

    #[tokio::test]
    async fn credential_revocation_after_issuance_rejects_redemption() {
        let fixture = fixture("revoked").await;
        let broker = StreamGrantBroker::new(fixture.auth.clone());
        let raw = broker
            .issue(authorization(&fixture.principal, "channel", 0), PROTOCOL)
            .expect("issue grant")
            .expose()
            .to_owned();
        let agent_id = fixture.principal.agent_id().expect("agent principal");
        fixture
            .auth
            .rotate_agent_credential(agent_id)
            .await
            .expect("rotate credential");
        assert!(matches!(
            broker.redeem(&raw, PROTOCOL).await,
            Err(StreamGrantBrokerError::Rejected)
        ));
    }
}
