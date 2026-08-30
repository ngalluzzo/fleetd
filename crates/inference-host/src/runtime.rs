//! Contained model-server process and active route probes.

use std::{collections::BTreeMap, path::PathBuf, process::Stdio, time::Duration};

use serde::Deserialize;
use tokio::process::{Child, Command};

use crate::{BackendError, BackendLaunch};

const VERSION_TIMEOUT: Duration = Duration::from_secs(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_INTERVAL: Duration = Duration::from_millis(250);
const MAX_VERSION_OUTPUT_BYTES: usize = 128 * 1_024;

/// Exact model-server process launch assembled by one vendor integration.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub executable: PathBuf,
    pub version_args: Vec<String>,
    pub expected_version: String,
    pub args: Vec<String>,
    pub environment: BTreeMap<String, String>,
}

pub(crate) struct BackendRuntime {
    child: Child,
    client: reqwest::Client,
    health_url: String,
    models_url: String,
    model_id: String,
}

impl BackendRuntime {
    pub(crate) async fn start(
        launch: &BackendLaunch,
        allowed_environment: &[&str],
    ) -> Result<Self, BackendError> {
        validate_environment(&launch.runtime.environment, allowed_environment)?;
        verify_version(&launch.runtime).await?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(PROBE_TIMEOUT)
            .build()?;
        let mut command = Command::new(&launch.runtime.executable);
        command
            .args(&launch.runtime.args)
            .env_clear()
            .envs(&launch.runtime.environment)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let child = command.spawn()?;
        let mut runtime = Self {
            child,
            client,
            health_url: launch.health_url.clone(),
            models_url: launch.models_url.clone(),
            model_id: launch.description.endpoint.model.id.clone(),
        };
        runtime.wait_ready(launch.startup_timeout).await?;
        Ok(runtime)
    }

    pub(crate) async fn is_ready(&mut self) -> bool {
        if self.child.try_wait().ok().flatten().is_some() {
            return false;
        }
        self.probe().await.is_ok()
    }

    pub(crate) async fn stop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _unused = self.child.start_kill();
        }
        let _unused = tokio::time::timeout(Duration::from_secs(5), self.child.wait()).await;
    }

    async fn wait_ready(&mut self, timeout: Duration) -> Result<(), BackendError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Err(BackendError::Runtime(format!(
                    "backend exited before readiness with code {:?}",
                    status.code()
                )));
            }
            let probe_error = match self.probe().await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if tokio::time::Instant::now() >= deadline {
                self.stop().await;
                return Err(BackendError::Runtime(format!(
                    "backend did not become ready before {timeout:?}: {probe_error}"
                )));
            }
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
    }

    async fn probe(&self) -> Result<(), BackendError> {
        let health = self.client.get(&self.health_url).send().await?;
        if !health.status().is_success() {
            return Err(BackendError::Runtime(format!(
                "health probe returned {}",
                health.status()
            )));
        }
        let models = self.client.get(&self.models_url).send().await?;
        if !models.status().is_success() {
            return Err(BackendError::Runtime(format!(
                "model probe returned {}",
                models.status()
            )));
        }
        let models: ModelsResponse = models.json().await?;
        if !models.data.iter().any(|model| model.id == self.model_id) {
            return Err(BackendError::Runtime(format!(
                "backend does not expose configured model ID {}",
                self.model_id
            )));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct ModelsResponse {
    data: Vec<ModelRecord>,
}

#[derive(Deserialize)]
struct ModelRecord {
    id: String,
}

async fn verify_version(runtime: &RuntimeConfig) -> Result<(), BackendError> {
    let mut command = Command::new(&runtime.executable);
    command
        .args(&runtime.version_args)
        .env_clear()
        .envs(&runtime.environment)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let output = tokio::time::timeout(VERSION_TIMEOUT, command.output())
        .await
        .map_err(|_| BackendError::Runtime("backend version probe timed out".to_owned()))??;
    if output.stdout.len() + output.stderr.len() > MAX_VERSION_OUTPUT_BYTES {
        return Err(BackendError::Runtime(
            "backend version probe exceeded its output bound".to_owned(),
        ));
    }
    let mut observed = String::from_utf8_lossy(&output.stdout).into_owned();
    observed.push_str(&String::from_utf8_lossy(&output.stderr));
    if !output.status.success() {
        let diagnostic = observed
            .chars()
            .filter(|character| !character.is_control() || character.is_whitespace())
            .take(2_048)
            .collect::<String>();
        return Err(BackendError::Runtime(format!(
            "backend version probe exited with code {:?}: {}",
            output.status.code(),
            diagnostic.trim()
        )));
    }
    if !observed.contains(&runtime.expected_version) {
        return Err(BackendError::Runtime(format!(
            "backend version did not contain expected identity {}",
            runtime.expected_version
        )));
    }
    Ok(())
}

fn validate_environment(
    environment: &BTreeMap<String, String>,
    allowed: &[&str],
) -> Result<(), BackendError> {
    for name in environment.keys() {
        if !allowed.contains(&name.as_str()) {
            return Err(BackendError::InvalidConfig(format!(
                "backend environment name {name} is not allowlisted by its plugin"
            )));
        }
    }
    Ok(())
}
