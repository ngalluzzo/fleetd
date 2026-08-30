//! Authorization and idempotency for desired agent execution.

use fleetd::operations::{AgentSeatConfiguration, AgentSeatDesiredState, ConfigureAgentSeat};

mod common;

use common::api::Daemon;

#[tokio::test]
async fn an_operator_can_configure_stop_and_restart_one_agent() {
    let server = Daemon::start().await;
    let agent = server.register("builder").await;
    let path = format!("/v1/agents/{}/seat-configuration", agent.agent.id);
    let running = ConfigureAgentSeat {
        profile_id: "opencode.glm".to_owned(),
        instructions: "Build changes and converse with peers.".to_owned(),
        desired_state: AgentSeatDesiredState::Running,
    };

    let forbidden = server
        .request(reqwest::Method::PUT, &path, Some(&agent.credential.token))
        .json(&running)
        .send()
        .await
        .expect("agent configuration response");
    assert_eq!(forbidden.status(), reqwest::StatusCode::FORBIDDEN);

    let first = server
        .request(reqwest::Method::PUT, &path, Some(&server.operator_token))
        .json(&running)
        .send()
        .await
        .expect("operator configuration response")
        .error_for_status()
        .expect("operator configuration status")
        .json::<AgentSeatConfiguration>()
        .await
        .expect("configuration body");
    let replay = server
        .request(reqwest::Method::PUT, &path, Some(&server.operator_token))
        .json(&running)
        .send()
        .await
        .expect("configuration replay")
        .error_for_status()
        .expect("configuration replay status")
        .json::<AgentSeatConfiguration>()
        .await
        .expect("configuration replay body");
    assert_eq!(first.revision, 1);
    assert_eq!(replay.revision, 1);

    let restarted = server
        .post(
            &format!("/v1/agents/{}/seat-restart", agent.agent.id),
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("restart response")
        .error_for_status()
        .expect("restart status")
        .json::<AgentSeatConfiguration>()
        .await
        .expect("restart body");
    assert_eq!(restarted.revision, 2);

    let listed = server
        .get(
            "/v1/agent-seat-configurations",
            Some(&server.operator_token),
        )
        .send()
        .await
        .expect("list response")
        .error_for_status()
        .expect("list status")
        .json::<Vec<AgentSeatConfiguration>>()
        .await
        .expect("list body");
    assert_eq!(listed, vec![restarted]);
}

#[tokio::test]
async fn seat_configuration_refuses_unknown_agents_and_unapproved_profile_shapes() {
    let server = Daemon::start().await;
    let request = ConfigureAgentSeat {
        profile_id: "../../bin/sh".to_owned(),
        instructions: String::new(),
        desired_state: AgentSeatDesiredState::Running,
    };
    let invalid = server
        .request(
            reqwest::Method::PUT,
            "/v1/agents/missing/seat-configuration",
            Some(&server.operator_token),
        )
        .json(&request)
        .send()
        .await
        .expect("invalid profile response");
    assert_eq!(invalid.status(), reqwest::StatusCode::BAD_REQUEST);

    let missing = server
        .request(
            reqwest::Method::PUT,
            "/v1/agents/missing/seat-configuration",
            Some(&server.operator_token),
        )
        .json(&ConfigureAgentSeat {
            profile_id: "approved-profile".to_owned(),
            instructions: String::new(),
            desired_state: AgentSeatDesiredState::Stopped,
        })
        .send()
        .await
        .expect("missing agent response");
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);
}
