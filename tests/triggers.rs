//! The durable registration behind an inbound trigger, and the credential that
//! lets it fire.
//!
//! Registering and being able to fire are one act, so these tests treat them as
//! one: that the declaration is normalised into a single representation, that
//! the references are real, that the credential authenticates as the third
//! authority category and nothing else, and that retiring a trigger actually
//! ends its standing grant rather than only relabelling the row.

use fleetd::{
    auth::{AuthService, Principal},
    error::FleetError,
    model::{CreateAgent, CreateChannel},
    store::Store,
    trigger::{RegisterTrigger, RegisteredTrigger, TriggerState},
};
use serde_json::json;

mod common;

struct Registry {
    _directory: tempfile::TempDir,
    store: Store,
    auth: AuthService,
    channel_id: String,
    sender_id: String,
}

impl Registry {
    async fn open() -> Self {
        let common::TempStore {
            directory, store, ..
        } = common::temp_store().await;
        let auth = AuthService::new(store.clone());
        let sender = store
            .create_agent(CreateAgent {
                name: "nightly-scheduler".to_owned(),
                metadata: json!({}),
            })
            .await
            .expect("create the trigger's sender");
        let channel = store
            .create_channel(CreateChannel {
                name: "nightly".to_owned(),
                metadata: json!({}),
                member_ids: Vec::new(),
                members: Vec::new(),
            })
            .await
            .expect("create the trigger's channel");
        Self {
            _directory: directory,
            store,
            auth,
            channel_id: channel.id,
            sender_id: sender.id,
        }
    }

    fn declaring(&self, name: &str, kinds: &[&str]) -> RegisterTrigger {
        RegisterTrigger {
            name: name.to_owned(),
            channel_id: self.channel_id.clone(),
            sender_id: self.sender_id.clone(),
            accepted_kinds: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
        }
    }

    async fn register(&self, name: &str, kinds: &[&str]) -> RegisteredTrigger {
        self.auth
            .register_trigger(self.declaring(name, kinds))
            .await
            .expect("register a trigger")
    }
}

/// A trigger that has never fired must read as exactly that, rather than as one
/// whose history is merely unknown. The distinction is the reason to register a
/// trigger at all: an idle fleet and a broken scheduler look identical until the
/// record can tell them apart.
#[tokio::test]
async fn a_registered_trigger_starts_active_and_having_fired_nothing() {
    let registry = Registry::open().await;
    let registered = registry.register("nightly-sweep", &["task.request"]).await;
    let trigger = registered.trigger;

    assert_eq!(trigger.state, TriggerState::Active);
    assert_eq!(trigger.accepted_occurrences, 0);
    assert_eq!(trigger.last_occurrence_id, None);
    assert_eq!(trigger.last_fired_at_ms, None);
    assert_eq!(trigger.retired_at_ms, None);

    let read_back = registry
        .store
        .get_trigger(&trigger.id)
        .await
        .expect("read the registration back");
    assert_eq!(read_back, trigger);
}

/// The credential is the third authority category, and it must resolve as
/// itself. Reading a trigger as an agent would hand it everything that agent's
/// membership reaches, which is the whole thing a standing grant has to avoid.
#[tokio::test]
async fn a_trigger_credential_authenticates_as_a_trigger_and_not_as_its_sender() {
    let registry = Registry::open().await;
    let registered = registry.register("nightly-sweep", &["task.request"]).await;

    let principal = registry
        .auth
        .authenticate(&registered.credential.token)
        .await
        .expect("authenticate the trigger credential");
    assert_eq!(
        principal,
        Principal::Trigger {
            credential_id: registered.credential.id.clone(),
            trigger_id: registered.trigger.id.clone(),
        }
    );
    assert!(!principal.is_operator());
    assert_eq!(principal.agent_id(), None);
    assert_eq!(principal.trigger_id(), Some(registered.trigger.id.as_str()));
    assert!(
        registry
            .auth
            .revalidate_principal(&principal)
            .await
            .expect("revalidate")
    );
}

/// Declared kinds participate in trigger identity, so the stored form has to be
/// one representation of one set. Two operators writing the same set in a
/// different order have registered the same authority.
#[tokio::test]
async fn the_declared_kinds_are_stored_sorted() {
    let registry = Registry::open().await;
    let registered = registry
        .register(
            "unsorted",
            &["task.request", "note.append", "review.request"],
        )
        .await;
    assert_eq!(
        registered.trigger.accepted_kinds,
        ["note.append", "review.request", "task.request"]
    );
    assert_eq!(
        registry
            .store
            .get_trigger(&registered.trigger.id)
            .await
            .expect("read back")
            .accepted_kinds,
        registered.trigger.accepted_kinds
    );
}

/// A trigger declaring nothing could create nothing, and a duplicated kind means
/// the operator does not know what they granted. Both are refused at the door
/// rather than normalised away.
#[tokio::test]
async fn a_meaningless_declaration_is_refused() {
    let registry = Registry::open().await;

    for (label, declaration) in [
        ("empty", registry.declaring("empty", &[])),
        (
            "duplicated",
            registry.declaring("duplicated", &["task.request", "task.request"]),
        ),
        ("unnamed", registry.declaring("  ", &["task.request"])),
    ] {
        let refused = registry.auth.register_trigger(declaration).await;
        assert!(
            matches!(refused, Err(FleetError::Invalid(_))),
            "{label}: {refused:?}"
        );
    }

    assert!(
        registry
            .store
            .list_triggers(None)
            .await
            .expect("list triggers")
            .is_empty(),
        "a refused declaration left a registration behind"
    );
}

/// A trigger's authority is expressed as a channel and a sender that already
/// exist. Registering against either one absent would leave a standing grant
/// pointing at nothing, discovered at the first firing rather than at the moment
/// it was granted.
#[tokio::test]
async fn a_trigger_cannot_be_registered_against_something_absent() {
    let registry = Registry::open().await;

    let mut absent_channel = registry.declaring("absent-channel", &["task.request"]);
    absent_channel.channel_id = "channel-that-does-not-exist".to_owned();
    let refused = registry.auth.register_trigger(absent_channel).await;
    assert!(refused.is_err(), "{refused:?}");

    let mut absent_sender = registry.declaring("absent-sender", &["task.request"]);
    absent_sender.sender_id = "agent-that-does-not-exist".to_owned();
    let refused = registry.auth.register_trigger(absent_sender).await;
    assert!(refused.is_err(), "{refused:?}");

    // The credential and the registration commit together, so a refused
    // registration must not leave a credential authorising nothing.
    assert_eq!(active_trigger_credentials(&registry.store).await, 0);
}

/// A trigger's name is how an operator refers to it, so two triggers cannot
/// share one.
#[tokio::test]
async fn a_trigger_name_is_claimed_once() {
    let registry = Registry::open().await;
    registry.register("nightly-sweep", &["task.request"]).await;
    let second = registry
        .auth
        .register_trigger(registry.declaring("nightly-sweep", &["note.append"]))
        .await;
    assert!(matches!(second, Err(FleetError::Conflict(_))), "{second:?}");
    assert_eq!(active_trigger_credentials(&registry.store).await, 1);
}

/// Retiring is how an operator ends a standing grant, and a grant that outlives
/// its registration is exactly the failure a standing grant is worth worrying
/// about. The reason recorded is the first one, because that is the one that
/// describes why it stopped.
#[tokio::test]
async fn retiring_ends_the_grant_and_is_idempotent() {
    let registry = Registry::open().await;
    let registered = registry.register("retire-me", &["task.request"]).await;

    let retired = registry
        .auth
        .retire_trigger(
            &registered.trigger.id,
            "the deploy it watched was decommissioned",
        )
        .await
        .expect("retire the trigger");
    assert_eq!(retired.state, TriggerState::Retired);
    assert_eq!(
        retired.retired_reason.as_deref(),
        Some("the deploy it watched was decommissioned")
    );
    assert!(retired.retired_at_ms.is_some());

    assert!(matches!(
        registry
            .auth
            .authenticate(&registered.credential.token)
            .await,
        Err(FleetError::Unauthorized)
    ));
    assert_eq!(active_trigger_credentials(&registry.store).await, 0);

    let again = registry
        .auth
        .retire_trigger(&registered.trigger.id, "a second operator, later")
        .await
        .expect("retire the trigger again");
    assert_eq!(again, retired);
}

/// Rotating replaces the token without disturbing what the trigger may do, and
/// the credential it replaces stops working immediately.
#[tokio::test]
async fn rotating_replaces_exactly_one_credential() {
    let registry = Registry::open().await;
    let registered = registry.register("rotate-me", &["task.request"]).await;

    let replacement = registry
        .auth
        .rotate_trigger_credential(&registered.trigger.id)
        .await
        .expect("rotate the credential");
    assert_ne!(replacement.token, registered.credential.token);

    assert!(matches!(
        registry
            .auth
            .authenticate(&registered.credential.token)
            .await,
        Err(FleetError::Unauthorized)
    ));
    assert_eq!(
        registry
            .auth
            .authenticate(&replacement.token)
            .await
            .expect("authenticate the replacement")
            .trigger_id(),
        Some(registered.trigger.id.as_str())
    );
    assert_eq!(active_trigger_credentials(&registry.store).await, 1);
}

/// Retirement has to be final. A rotation that reissued authority for a retired
/// trigger would make stopping one reversible by anyone who can rotate.
#[tokio::test]
async fn a_retired_trigger_cannot_be_handed_a_new_credential() {
    let registry = Registry::open().await;
    let registered = registry.register("retire-me", &["task.request"]).await;
    registry
        .auth
        .retire_trigger(&registered.trigger.id, "no longer needed")
        .await
        .expect("retire the trigger");

    let refused = registry
        .auth
        .rotate_trigger_credential(&registered.trigger.id)
        .await;
    assert!(
        matches!(refused, Err(FleetError::Conflict(_))),
        "{refused:?}"
    );
    assert_eq!(active_trigger_credentials(&registry.store).await, 0);
}

#[tokio::test]
async fn an_unknown_trigger_is_not_found() {
    let registry = Registry::open().await;
    for outcome in [
        registry.store.get_trigger("nope").await,
        registry.auth.retire_trigger("nope", "because").await,
        registry
            .auth
            .rotate_trigger_credential("nope")
            .await
            .map(|_| unreachable!("an unknown trigger has no credential to rotate")),
    ] {
        assert!(matches!(
            outcome,
            Err(FleetError::NotFound {
                entity: "trigger",
                ..
            })
        ));
    }
}

/// Scoping is what makes the list answerable: a trigger registered against
/// another channel is not this channel's business. Retiring does not remove a
/// registration -- a standing grant that was withdrawn is a fact worth keeping,
/// not an absence.
#[tokio::test]
async fn listing_is_scoped_by_channel_and_keeps_retired_registrations() {
    let registry = Registry::open().await;
    let other_channel = registry
        .store
        .create_channel(CreateChannel {
            name: "weekly".to_owned(),
            metadata: json!({}),
            member_ids: Vec::new(),
            members: Vec::new(),
        })
        .await
        .expect("create a second channel");

    registry.register("first", &["task.request"]).await;
    registry.register("second", &["task.request"]).await;
    let mut elsewhere = registry.declaring("elsewhere", &["task.request"]);
    elsewhere.channel_id = other_channel.id.clone();
    let elsewhere = registry
        .auth
        .register_trigger(elsewhere)
        .await
        .expect("register a trigger on the other channel");

    let mut scoped: Vec<String> = registry
        .store
        .list_triggers(Some(&registry.channel_id))
        .await
        .expect("list the channel's triggers")
        .into_iter()
        .map(|trigger| trigger.name)
        .collect();
    scoped.sort();
    assert_eq!(scoped, ["first", "second"]);

    registry
        .auth
        .retire_trigger(&elsewhere.trigger.id, "no longer needed")
        .await
        .expect("retire it");
    assert_eq!(
        registry
            .store
            .list_triggers(Some(&other_channel.id))
            .await
            .expect("list again")
            .len(),
        1
    );
    assert_eq!(
        registry
            .store
            .list_triggers(None)
            .await
            .expect("list every trigger")
            .len(),
        3
    );
}

/// Read directly, because "the grant ended" is a claim about stored rows rather
/// than about what the service reports.
async fn active_trigger_credentials(store: &Store) -> i64 {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM auth_credentials \
         WHERE principal_kind = 'trigger' AND revoked_at_ms IS NULL",
    )
    .fetch_one(store.pool())
    .await
    .expect("count active trigger credentials")
}
