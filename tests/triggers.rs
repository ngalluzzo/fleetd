//! The durable registration behind an inbound trigger.
//!
//! What a trigger may do is declared once and stored, so these tests are about
//! the row rather than about firing: that the declaration is normalised into one
//! representation, that the references are real, and that retirement is a state
//! an operator can reach twice without being told they made a mistake.

use fleetd::{
    error::FleetError,
    model::{CreateAgent, CreateChannel},
    store::Store,
    trigger::{RegisterTrigger, TriggerState},
};
use serde_json::json;

mod common;

async fn registry() -> (tempfile::TempDir, Store, String, String) {
    let common::TempStore {
        directory, store, ..
    } = common::temp_store().await;
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
    (directory, store, channel.id, sender.id)
}

fn registration(name: &str, channel_id: &str, sender_id: &str, kinds: &[&str]) -> RegisterTrigger {
    RegisterTrigger {
        name: name.to_owned(),
        channel_id: channel_id.to_owned(),
        sender_id: sender_id.to_owned(),
        accepted_kinds: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
    }
}

/// A trigger that has never fired must read as exactly that, rather than as one
/// whose history is merely unknown. The distinction is the reason to register a
/// trigger at all: an idle fleet and a broken scheduler look identical until the
/// record can tell them apart.
#[tokio::test]
async fn a_registered_trigger_starts_active_and_having_fired_nothing() {
    let (_directory, store, channel_id, sender_id) = registry().await;
    let trigger = store
        .register_trigger(registration(
            "nightly-sweep",
            &channel_id,
            &sender_id,
            &["task.request"],
        ))
        .await
        .expect("register a trigger");

    assert_eq!(trigger.state, TriggerState::Active);
    assert_eq!(trigger.accepted_occurrences, 0);
    assert_eq!(trigger.last_occurrence_id, None);
    assert_eq!(trigger.last_fired_at_ms, None);
    assert_eq!(trigger.retired_at_ms, None);

    let read_back = store
        .get_trigger(&trigger.id)
        .await
        .expect("read the registration back");
    assert_eq!(read_back, trigger);
}

/// Declared kinds participate in trigger identity, so the stored form has to be
/// one representation of one set. Two operators writing the same set in a
/// different order have registered the same authority.
#[tokio::test]
async fn the_declared_kinds_are_stored_sorted() {
    let (_directory, store, channel_id, sender_id) = registry().await;
    let trigger = store
        .register_trigger(registration(
            "unsorted",
            &channel_id,
            &sender_id,
            &["task.request", "note.append", "review.request"],
        ))
        .await
        .expect("register a trigger");
    assert_eq!(
        trigger.accepted_kinds,
        ["note.append", "review.request", "task.request"]
    );
    assert_eq!(
        store
            .get_trigger(&trigger.id)
            .await
            .expect("read back")
            .accepted_kinds,
        trigger.accepted_kinds
    );
}

/// A trigger declaring nothing could create nothing, and a duplicated kind means
/// the operator does not know what they granted. Both are refused at the door
/// rather than normalised away.
#[tokio::test]
async fn a_meaningless_declaration_is_refused() {
    let (_directory, store, channel_id, sender_id) = registry().await;

    let empty = store
        .register_trigger(registration("empty", &channel_id, &sender_id, &[]))
        .await;
    assert!(matches!(empty, Err(FleetError::Invalid(_))), "{empty:?}");

    let duplicated = store
        .register_trigger(registration(
            "duplicated",
            &channel_id,
            &sender_id,
            &["task.request", "task.request"],
        ))
        .await;
    assert!(
        matches!(duplicated, Err(FleetError::Invalid(_))),
        "{duplicated:?}"
    );

    let unnamed = store
        .register_trigger(registration(
            "  ",
            &channel_id,
            &sender_id,
            &["task.request"],
        ))
        .await;
    assert!(
        matches!(unnamed, Err(FleetError::Invalid(_))),
        "{unnamed:?}"
    );
}

/// A trigger's authority is expressed as a channel and a sender that already
/// exist. Registering against either one absent would leave a standing grant
/// pointing at nothing, which is discovered at the first firing rather than at
/// the moment it was granted.
#[tokio::test]
async fn a_trigger_cannot_be_registered_against_something_absent() {
    let (_directory, store, channel_id, sender_id) = registry().await;

    let absent_channel = store
        .register_trigger(registration(
            "absent-channel",
            "channel-that-does-not-exist",
            &sender_id,
            &["task.request"],
        ))
        .await;
    assert!(absent_channel.is_err(), "{absent_channel:?}");

    let absent_sender = store
        .register_trigger(registration(
            "absent-sender",
            &channel_id,
            "agent-that-does-not-exist",
            &["task.request"],
        ))
        .await;
    assert!(absent_sender.is_err(), "{absent_sender:?}");
}

/// A trigger's name is how an operator refers to it, so two triggers cannot
/// share one.
#[tokio::test]
async fn a_trigger_name_is_claimed_once() {
    let (_directory, store, channel_id, sender_id) = registry().await;
    store
        .register_trigger(registration(
            "nightly-sweep",
            &channel_id,
            &sender_id,
            &["task.request"],
        ))
        .await
        .expect("register the first trigger");
    let second = store
        .register_trigger(registration(
            "nightly-sweep",
            &channel_id,
            &sender_id,
            &["note.append"],
        ))
        .await;
    assert!(matches!(second, Err(FleetError::Conflict(_))), "{second:?}");
}

/// Retiring is how an operator stops a standing grant, and stopping something
/// twice is not a mistake. The reason recorded is the first one, because that is
/// the one that describes why it stopped.
#[tokio::test]
async fn retiring_is_idempotent_and_keeps_the_first_reason() {
    let (_directory, store, channel_id, sender_id) = registry().await;
    let trigger = store
        .register_trigger(registration(
            "retire-me",
            &channel_id,
            &sender_id,
            &["task.request"],
        ))
        .await
        .expect("register a trigger");

    let retired = store
        .retire_trigger(&trigger.id, "the deploy it watched was decommissioned")
        .await
        .expect("retire the trigger");
    assert_eq!(retired.state, TriggerState::Retired);
    assert_eq!(
        retired.retired_reason.as_deref(),
        Some("the deploy it watched was decommissioned")
    );
    assert!(retired.retired_at_ms.is_some());

    let again = store
        .retire_trigger(&trigger.id, "a second operator, later")
        .await
        .expect("retire the trigger again");
    assert_eq!(again, retired);
}

#[tokio::test]
async fn an_unknown_trigger_is_not_found() {
    let (_directory, store, _channel_id, _sender_id) = registry().await;
    assert!(matches!(
        store.get_trigger("nope").await,
        Err(FleetError::NotFound {
            entity: "trigger",
            ..
        })
    ));
    assert!(matches!(
        store.retire_trigger("nope", "because").await,
        Err(FleetError::NotFound {
            entity: "trigger",
            ..
        })
    ));
}

/// Scoping is what makes the list answerable: a trigger registered against
/// another channel is not this channel's business.
#[tokio::test]
async fn listing_is_scoped_by_channel() {
    let (_directory, store, channel_id, sender_id) = registry().await;
    let other_channel = store
        .create_channel(CreateChannel {
            name: "weekly".to_owned(),
            metadata: json!({}),
            member_ids: Vec::new(),
            members: Vec::new(),
        })
        .await
        .expect("create a second channel");

    for name in ["first", "second"] {
        store
            .register_trigger(registration(
                name,
                &channel_id,
                &sender_id,
                &["task.request"],
            ))
            .await
            .expect("register a trigger on the first channel");
    }
    store
        .register_trigger(registration(
            "elsewhere",
            &other_channel.id,
            &sender_id,
            &["task.request"],
        ))
        .await
        .expect("register a trigger on the other channel");

    let mut scoped: Vec<String> = store
        .list_triggers(Some(&channel_id))
        .await
        .expect("list the channel's triggers")
        .into_iter()
        .map(|trigger| trigger.name)
        .collect();
    scoped.sort();
    assert_eq!(scoped, ["first", "second"]);

    // Retiring does not remove the registration: a standing grant that was
    // withdrawn is a fact worth keeping, not an absence.
    let retired = store
        .list_triggers(Some(&other_channel.id))
        .await
        .expect("list the other channel's triggers");
    assert_eq!(retired.len(), 1);
    store
        .retire_trigger(&retired[0].id, "no longer needed")
        .await
        .expect("retire it");
    assert_eq!(
        store
            .list_triggers(Some(&other_channel.id))
            .await
            .expect("list again")
            .len(),
        1
    );

    assert_eq!(
        store
            .list_triggers(None)
            .await
            .expect("list every trigger")
            .len(),
        3
    );
}
