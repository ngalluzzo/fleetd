//! Initialize handshake, runtime identity, and which adoption methods exist.

use agent_client_protocol::{
    Agent, ConnectionTo,
    schema::{
        ProtocolVersion,
        v1::{AgentCapabilities, Implementation, InitializeRequest, InitializeResponse},
    },
};
use fleetd_proto::harness_acp::{DescribeResult, DriverIdentity, HarnessLimits, RuntimeIdentity};

use super::{DriverError, MAX_FRAME_BYTES, RawInitializeRequest, RuntimeConfig, bound_json};

pub(super) const ACP_SDK_VERSION: &str = "2.0.0";

/// Which session-adoption methods the inner runtime advertised.
///
/// ACP requires `session/load` to replay the entire conversation as
/// `session/update` notifications before it answers, and requires
/// `session/resume` not to. Adoption wants the session back rather than its
/// transcript, so it prefers `resume`; `load` remains the fallback for a
/// runtime that predates it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AdoptionMethods {
    pub(super) load: bool,
    pub(super) resume: bool,
}

impl AdoptionMethods {
    pub(super) fn from_capabilities(capabilities: &AgentCapabilities) -> Self {
        Self {
            load: capabilities.load_session,
            resume: capabilities.session_capabilities.resume.is_some(),
        }
    }

    /// Which ACP method adopts an existing session, or `None` when the runtime
    /// advertises neither.
    ///
    /// `resume` wins wherever it exists: both restore the session, and only
    /// `load` is obliged to replay the entire conversation first.
    pub(super) const fn method(self) -> Option<&'static str> {
        if self.resume {
            Some("session/resume")
        } else if self.load {
            Some("session/load")
        } else {
            None
        }
    }
}

pub(super) async fn initialize_runtime(
    connection: &ConnectionTo<Agent>,
    runtime: &RuntimeConfig,
    executable_digest: String,
    profile_digest: String,
) -> Result<(DescribeResult, AdoptionMethods), DriverError> {
    let request = InitializeRequest::new(ProtocolVersion::V1).client_info(Implementation::new(
        "fleetd-acp-host",
        env!("CARGO_PKG_VERSION"),
    ));
    let raw_initialize = connection
        .send_request(RawInitializeRequest(serde_json::to_value(request)?))
        .block_task()
        .await
        .map_err(|error| DriverError::Runtime(error.to_string()))?;
    let parsed: InitializeResponse = serde_json::from_value(raw_initialize.0.clone())?;
    let agent_info = parsed.agent_info.clone().ok_or_else(|| {
        DriverError::Runtime("inner ACP runtime did not report agentInfo".to_owned())
    })?;
    if agent_info.name != runtime.expected_name || agent_info.version != runtime.expected_version {
        return Err(DriverError::Runtime(format!(
            "runtime identity mismatch: expected {} {}, received {} {}",
            runtime.expected_name, runtime.expected_version, agent_info.name, agent_info.version
        )));
    }
    let adoption = AdoptionMethods::from_capabilities(&parsed.agent_capabilities);
    let description = DescribeResult {
        driver: DriverIdentity {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            acp_sdk_version: ACP_SDK_VERSION.to_owned(),
            acp_protocol_version: 1,
        },
        runtime: RuntimeIdentity {
            name: agent_info.name,
            version: agent_info.version,
            executable_digest,
        },
        agent_capabilities: serde_json::to_value(&parsed.agent_capabilities)?,
        limits: HarnessLimits {
            max_concurrent_turns: 1,
            max_frame_bytes: MAX_FRAME_BYTES,
        },
        profile_digest,
        raw_initialize_result: bound_json(raw_initialize.0, MAX_FRAME_BYTES / 2),
    };
    Ok((description, adoption))
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::schema::v1::AgentCapabilities;
    use serde_json::json;

    use super::AdoptionMethods;

    /// Adoption wants the session back, not its transcript. ACP obliges
    /// `session/load` to replay the entire conversation before it answers and
    /// obliges `session/resume` not to, so resume wins wherever it exists.
    #[test]
    fn adoption_prefers_resume_and_falls_back_to_load() {
        assert_eq!(
            AdoptionMethods {
                load: true,
                resume: true
            }
            .method(),
            Some("session/resume")
        );
        assert_eq!(
            AdoptionMethods {
                load: true,
                resume: false
            }
            .method(),
            Some("session/load")
        );
        assert_eq!(
            AdoptionMethods {
                load: false,
                resume: true
            }
            .method(),
            Some("session/resume")
        );
        assert_eq!(
            AdoptionMethods {
                load: false,
                resume: false
            }
            .method(),
            None,
            "a runtime advertising neither cannot be adopted, and must not \
             silently open a fresh session instead"
        );
    }

    /// The capability shape is read from the runtime's own initialize response,
    /// so a runtime that omits `sessionCapabilities` entirely is load-only
    /// rather than unadoptable.
    #[test]
    fn adoption_methods_are_read_from_advertised_capabilities() {
        let load_only: AgentCapabilities = serde_json::from_value(json!({"loadSession": true}))
            .expect("capabilities without sessionCapabilities parse");
        assert_eq!(
            AdoptionMethods::from_capabilities(&load_only).method(),
            Some("session/load")
        );

        let resumable: AgentCapabilities = serde_json::from_value(
            json!({"loadSession": true, "sessionCapabilities": {"resume": {}}}),
        )
        .expect("capabilities with resume parse");
        assert_eq!(
            AdoptionMethods::from_capabilities(&resumable).method(),
            Some("session/resume")
        );

        let fresh_only: AgentCapabilities =
            serde_json::from_value(json!({})).expect("empty capabilities parse");
        assert_eq!(
            AdoptionMethods::from_capabilities(&fresh_only).method(),
            None
        );
    }
}
