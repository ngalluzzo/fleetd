//! What a trigger credential reaches over HTTP, and what it does not.
//!
//! The registration is the authority, so the surface is where that has to hold
//! against a real credential rather than against a caller who intends to behave.
//! These suites drive the same token an operator would hand a scheduler, and ask
//! it for everything it should not have.

use fleetd::{
    model::{Message, MessagePage, SendMessage},
    trigger::{
        RegisterTrigger, RegisteredTrigger, RetireTrigger, Trigger, TriggerFired, TriggerOccurrence,
    },
};
use serde_json::json;

mod common;

use common::api::Daemon;

/// One channel, a sender the trigger speaks as, and a worker it addresses.
struct Fleet {
    server: Daemon,
    channel_id: String,
    sender_id: String,
    worker_id: String,
    worker_token: String,
}

impl Fleet {
    async fn start() -> Self {
        let server = Daemon::start().await;
        let sender = server.register("nightly-scheduler").await;
        let worker = server.register("nightly-worker").await;
        let channel = server.channel(&[&sender.agent.id, &worker.agent.id]).await;
        Self {
            channel_id: channel.id,
            sender_id: sender.agent.id,
            worker_id: worker.agent.id,
            worker_token: worker.credential.token,
            server,
        }
    }

    async fn register(&self, name: &str, kinds: &[&str]) -> RegisteredTrigger {
        self.server
            .post("/v1/triggers", Some(&self.server.operator_token))
            .json(&RegisterTrigger {
                name: name.to_owned(),
                channel_id: self.channel_id.clone(),
                sender_id: self.sender_id.clone(),
                accepted_kinds: kinds.iter().map(|kind| (*kind).to_owned()).collect(),
            })
            .send()
            .await
            .expect("register request")
            .error_for_status()
            .expect("register response")
            .json()
            .await
            .expect("registration body")
    }

    fn occurrence(&self, occurrence_id: &str, kind: &str) -> TriggerOccurrence {
        TriggerOccurrence {
            occurrence_id: occurrence_id.to_owned(),
            recipient_id: self.worker_id.clone(),
            kind: kind.to_owned(),
            payload: json!({ "sweep": "nightly" }),
        }
    }

    async fn fire(
        &self,
        trigger_id: &str,
        token: &str,
        occurrence: &TriggerOccurrence,
    ) -> reqwest::Response {
        self.server
            .post(
                &format!("/v1/triggers/{trigger_id}/occurrences"),
                Some(token),
            )
            .json(occurrence)
            .send()
            .await
            .expect("fire request")
    }

    async fn channel_messages(&self) -> Vec<Message> {
        let page: MessagePage = self
            .server
            .get(
                &format!("/v1/channels/{}/messages", self.channel_id),
                Some(&self.server.operator_token),
            )
            .send()
            .await
            .expect("messages request")
            .error_for_status()
            .expect("messages response")
            .json()
            .await
            .expect("messages body");
        page.messages
    }
}

/// A trigger credential creates the work its registration describes, and the
/// message is attributed to the registration's sender rather than to anything
/// the occurrence chose.
#[tokio::test]
async fn a_trigger_credential_creates_the_work_its_registration_declared() {
    let fleet = Fleet::start().await;
    let registered = fleet.register("nightly-sweep", &["task.request"]).await;

    let response = fleet
        .fire(
            &registered.trigger.id,
            &registered.credential.token,
            &fleet.occurrence("2026-08-27T02:00", "task.request"),
        )
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let fired: TriggerFired = response.json().await.expect("fired body");
    assert!(fired.created);

    let messages = fleet.channel_messages().await;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].id, fired.message_id);
    assert_eq!(messages[0].sender_id, fleet.sender_id);
    assert_eq!(messages[0].kind, "task.request");
}

/// The declared set is the whole of a trigger's authority over content, and the
/// surface is where that has to survive a caller who simply asks for more.
#[tokio::test]
async fn a_trigger_credential_cannot_create_an_undeclared_kind() {
    let fleet = Fleet::start().await;
    let registered = fleet.register("nightly-sweep", &["task.request"]).await;

    let response = fleet
        .fire(
            &registered.trigger.id,
            &registered.credential.token,
            &fleet.occurrence("2026-08-27T02:00", "review.request"),
        )
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(fleet.channel_messages().await.is_empty());
}

/// A trigger's authority is over one registration. Holding a token is not the
/// same as being the trigger the path names.
#[tokio::test]
async fn a_trigger_credential_cannot_fire_another_trigger() {
    let fleet = Fleet::start().await;
    let first = fleet.register("first-sweep", &["task.request"]).await;
    let second = fleet.register("second-sweep", &["task.request"]).await;

    let response = fleet
        .fire(
            &second.trigger.id,
            &first.credential.token,
            &fleet.occurrence("2026-08-27T02:00", "task.request"),
        )
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(fleet.channel_messages().await.is_empty());
}

/// Firing is not an operator action. An operator who wants to create work can
/// append a message as themselves; firing someone else's trigger would put an
/// occurrence in its record that it never produced.
#[tokio::test]
async fn nobody_else_can_fire_a_trigger() {
    let fleet = Fleet::start().await;
    let registered = fleet.register("nightly-sweep", &["task.request"]).await;
    let occurrence = fleet.occurrence("2026-08-27T02:00", "task.request");

    for (label, token) in [
        ("operator", fleet.server.operator_token.clone()),
        ("agent", fleet.worker_token.clone()),
    ] {
        let response = fleet
            .fire(&registered.trigger.id, &token, &occurrence)
            .await;
        assert_eq!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN,
            "{label} fired a trigger"
        );
    }
    assert!(fleet.channel_messages().await.is_empty());
}

/// A trigger creates work and never observes it. Every route that would let one
/// read the fleet has to refuse it, or the no-back-channel rule is a comment
/// rather than a boundary.
#[tokio::test]
async fn a_trigger_credential_reaches_nothing_else() {
    let fleet = Fleet::start().await;
    let registered = fleet.register("nightly-sweep", &["task.request"]).await;
    let token = Some(registered.credential.token.as_str());

    for path in [
        "/v1/agents".to_owned(),
        "/v1/triggers".to_owned(),
        format!("/v1/triggers/{}", registered.trigger.id),
        format!("/v1/channels/{}/messages", fleet.channel_id),
        format!("/v1/channels/{}/members", fleet.channel_id),
    ] {
        let response = fleet
            .server
            .get(&path, token)
            .send()
            .await
            .unwrap_or_else(|error| panic!("GET {path}: {error}"));
        assert_eq!(
            response.status(),
            reqwest::StatusCode::FORBIDDEN,
            "a trigger credential reached GET {path}"
        );
    }

    // Nor may it speak through the ordinary message route, which would bypass
    // the declared kinds entirely. The server derives the sender from the
    // credential, so there is no field here for a trigger to name one.
    let response = fleet
        .server
        .post(
            &format!("/v1/channels/{}/messages", fleet.channel_id),
            token,
        )
        .json(&SendMessage {
            idempotency_key: None,
            recipient_id: Some(fleet.worker_id.clone()),
            kind: "review.request".to_owned(),
            payload: json!({}),
            correlation_id: None,
            causation_id: None,
        })
        .send()
        .await
        .expect("message request");
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    assert!(fleet.channel_messages().await.is_empty());
}

/// Registration and administration stay operator-only, and a retirement over
/// the wire actually ends the grant the credential carries.
#[tokio::test]
async fn retiring_over_the_wire_ends_the_grant() {
    let fleet = Fleet::start().await;
    let registered = fleet.register("nightly-sweep", &["task.request"]).await;

    let agent_attempt = fleet
        .server
        .post("/v1/triggers", Some(&fleet.worker_token))
        .json(&RegisterTrigger {
            name: "unauthorized".to_owned(),
            channel_id: fleet.channel_id.clone(),
            sender_id: fleet.sender_id.clone(),
            accepted_kinds: vec!["task.request".to_owned()],
        })
        .send()
        .await
        .expect("agent registration response");
    assert_eq!(agent_attempt.status(), reqwest::StatusCode::FORBIDDEN);

    let retired: Trigger = fleet
        .server
        .post(
            &format!("/v1/triggers/{}/retire", registered.trigger.id),
            Some(&fleet.server.operator_token),
        )
        .json(&RetireTrigger {
            reason: "the deploy it watched was decommissioned".to_owned(),
        })
        .send()
        .await
        .expect("retire request")
        .error_for_status()
        .expect("retire response")
        .json()
        .await
        .expect("retired body");
    assert_eq!(
        retired.retired_reason.as_deref(),
        Some("the deploy it watched was decommissioned")
    );

    let response = fleet
        .fire(
            &registered.trigger.id,
            &registered.credential.token,
            &fleet.occurrence("2026-08-27T02:00", "task.request"),
        )
        .await;
    assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    assert!(fleet.channel_messages().await.is_empty());
}

/// The listing is what turns "this trigger has fired nothing since Tuesday"
/// into something an operator can read.
#[tokio::test]
async fn the_listing_carries_what_each_trigger_last_created() {
    let fleet = Fleet::start().await;
    let registered = fleet.register("nightly-sweep", &["task.request"]).await;
    fleet
        .fire(
            &registered.trigger.id,
            &registered.credential.token,
            &fleet.occurrence("2026-08-27T02:00", "task.request"),
        )
        .await
        .error_for_status()
        .expect("fire response");

    let listed: Vec<Trigger> = fleet
        .server
        .get(
            &format!("/v1/triggers?channel_id={}", fleet.channel_id),
            Some(&fleet.server.operator_token),
        )
        .send()
        .await
        .expect("list request")
        .error_for_status()
        .expect("list response")
        .json()
        .await
        .expect("list body");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].accepted_occurrences, 1);
    assert_eq!(
        listed[0].last_occurrence_id.as_deref(),
        Some("2026-08-27T02:00")
    );
    assert!(listed[0].last_fired_at_ms.is_some());
}
