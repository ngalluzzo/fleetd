//! Machine-local reconciliation of desired agent execution.
//!
//! The HTTP boundary stores only a profile reference. This command is the
//! trusted local half that resolves that reference against a private catalog,
//! provisions transports, and owns worker lifetimes.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use clap::Args;
use fleetd::{
    execution::seat_configuration,
    operations::{AgentSeatConfiguration, AgentSeatDesiredState},
    plugin::{
        InferenceDescribeResult, InferenceOpenAiClient, PluginProcess, PluginSpec,
        inference_openai_interface,
    },
    store::Store,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{
    shutdown_signal,
    worker::{MainResult, WorkerFileConfig, parse_worker_config_value, run_loaded_worker},
};

const MAX_CATALOG_BYTES: u64 = 1024 * 1024;
const MAX_PROFILES: usize = 128;
const MAX_INFERENCE_BACKENDS: usize = 32;
const MAX_PROFILE_ID_BYTES: usize = 128;
const MAX_PROFILE_LABEL_BYTES: usize = 256;
const MAX_PROFILE_DESCRIPTION_BYTES: usize = 2_048;
const BACKEND_RESTART_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Args)]
pub(super) struct WorkerSuperviseArgs {
    /// Override the database named by the fleet configuration.
    #[arg(long, env = "FLEETD_DB")]
    db: Option<PathBuf>,
    /// Private JSON catalog of approved worker runtime profiles.
    #[arg(long)]
    profiles: PathBuf,
    /// How often durable desired state is reconciled.
    #[arg(long, default_value_t = 1_000)]
    poll_interval_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileCatalogFile {
    schema_version: u32,
    #[serde(default)]
    inference_backends: Vec<InferenceBackendFile>,
    profiles: Vec<RuntimeProfileFile>,
}

struct ProfileCatalog {
    profiles: BTreeMap<String, RuntimeProfileFile>,
    inference_backends: BTreeMap<String, InferenceBackendFile>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InferenceBackendFile {
    id: String,
    label: String,
    plugin: InferencePluginFile,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InferencePluginFile {
    id: String,
    executable: PathBuf,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default = "empty_json_object")]
    config: Value,
    #[serde(default = "default_backend_initialize_timeout_ms")]
    initialize_timeout_ms: u64,
    #[serde(default = "default_backend_request_timeout_ms")]
    request_timeout_ms: u64,
    #[serde(default = "default_backend_shutdown_timeout_ms")]
    shutdown_timeout_ms: u64,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeProfileFile {
    id: String,
    label: String,
    description: String,
    #[serde(default)]
    inference_backend: Option<String>,
    worker: Value,
}

struct RunningSeat {
    revision: u64,
    inference_backend: Option<String>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

struct RunningBackend {
    client: InferenceOpenAiClient,
    description: InferenceDescribeResult,
}

/// Reconciles durable desired execution until the process is asked to stop.
pub(super) async fn supervise_workers(
    args: WorkerSuperviseArgs,
    fleet: &fleetd_fleet::ResolvedFleet,
) -> MainResult<()> {
    if args.poll_interval_ms == 0 || args.poll_interval_ms > 60_000 {
        return Err("poll_interval_ms must contain between 1 and 60000 milliseconds".into());
    }
    let database = args.db.unwrap_or_else(|| fleet.database.clone());
    if let Some(parent) = database.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await?;
    }
    let catalog = load_profile_catalog(&args.profiles)?;
    let _lock = acquire_supervisor_lock(&database)?;
    let store = Store::open_with_message_commit_hints(&database).await?;
    let cancellation = CancellationToken::new();
    let signal = cancellation.clone();
    let signal_task = tokio::spawn(async move {
        shutdown_signal().await;
        signal.cancel();
    });
    tracing::info!(
        database = %database.display(),
        profiles = catalog.profiles.len(),
        inference_backends = catalog.inference_backends.len(),
        "agent execution supervisor ready"
    );

    let mut running = BTreeMap::<String, RunningSeat>::new();
    let mut running_backends = BTreeMap::<String, RunningBackend>::new();
    let mut backend_retry_after = BTreeMap::<String, Instant>::new();
    let mut unresolved = BTreeSet::<(String, String)>::new();
    let interval = Duration::from_millis(args.poll_interval_ms);
    loop {
        reconcile(
            &store,
            &catalog,
            &mut running,
            &mut running_backends,
            &mut backend_retry_after,
            &mut unresolved,
        )
        .await?;
        tokio::select! {
            () = cancellation.cancelled() => break,
            () = tokio::time::sleep(interval) => {}
        }
    }
    signal_task.abort();
    for (_, seat) in running {
        stop_seat(seat).await;
    }
    for (backend_id, backend) in running_backends {
        stop_backend(&backend_id, backend).await;
    }
    Ok(())
}

async fn reconcile(
    store: &Store,
    catalog: &ProfileCatalog,
    running: &mut BTreeMap<String, RunningSeat>,
    running_backends: &mut BTreeMap<String, RunningBackend>,
    backend_retry_after: &mut BTreeMap<String, Instant>,
    unresolved: &mut BTreeSet<(String, String)>,
) -> MainResult<()> {
    let configurations = seat_configuration::list(store).await?;
    let desired_backends = desired_backend_ids(&configurations, catalog);
    reconcile_backends(
        catalog,
        &desired_backends,
        running,
        running_backends,
        backend_retry_after,
    )
    .await;
    reconcile_seats(
        store,
        catalog,
        &configurations,
        running,
        running_backends,
        unresolved,
    )
    .await?;
    stop_unused_backends(&desired_backends, running_backends, backend_retry_after).await;
    Ok(())
}

fn desired_backend_ids(
    configurations: &[AgentSeatConfiguration],
    catalog: &ProfileCatalog,
) -> BTreeSet<String> {
    configurations
        .iter()
        .filter(|configuration| configuration.desired_state == AgentSeatDesiredState::Running)
        .filter_map(|configuration| catalog.profiles.get(&configuration.profile_id))
        .filter_map(|profile| profile.inference_backend.clone())
        .collect()
}

async fn reconcile_backends(
    catalog: &ProfileCatalog,
    desired_backends: &BTreeSet<String>,
    running: &mut BTreeMap<String, RunningSeat>,
    running_backends: &mut BTreeMap<String, RunningBackend>,
    backend_retry_after: &mut BTreeMap<String, Instant>,
) {
    let backend_ids = running_backends.keys().cloned().collect::<Vec<_>>();
    for backend_id in backend_ids {
        if !desired_backends.contains(&backend_id) {
            continue;
        }
        let healthy = match running_backends.get_mut(&backend_id) {
            Some(backend) => backend.client.health().await,
            None => continue,
        };
        if let Err(error) = healthy {
            tracing::error!(%backend_id, %error, "inference backend became unavailable");
            stop_seats_using_backend(running, &backend_id).await;
            if let Some(backend) = running_backends.remove(&backend_id) {
                stop_backend(&backend_id, backend).await;
            }
            backend_retry_after.insert(backend_id, Instant::now() + BACKEND_RESTART_BACKOFF);
        }
    }

    for backend_id in desired_backends {
        if running_backends.contains_key(backend_id)
            || backend_retry_after
                .get(backend_id)
                .is_some_and(|retry_after| *retry_after > Instant::now())
        {
            continue;
        }
        let backend = catalog
            .inference_backends
            .get(backend_id)
            .expect("catalog validation resolved backend reference");
        tracing::info!(%backend_id, backend_label = %backend.label, "starting shared inference backend");
        match start_backend(backend).await {
            Ok(running_backend) => {
                tracing::info!(
                    %backend_id,
                    backend_name = %running_backend.description.backend.name,
                    backend_version = %running_backend.description.backend.version,
                    model_id = %running_backend.description.endpoint.model.id,
                    "shared inference backend ready"
                );
                running_backends.insert(backend_id.clone(), running_backend);
                backend_retry_after.remove(backend_id);
            }
            Err(error) => {
                tracing::error!(%backend_id, %error, "shared inference backend failed to start");
                backend_retry_after
                    .insert(backend_id.clone(), Instant::now() + BACKEND_RESTART_BACKOFF);
            }
        }
    }
}

async fn reconcile_seats(
    store: &Store,
    catalog: &ProfileCatalog,
    configurations: &[AgentSeatConfiguration],
    running: &mut BTreeMap<String, RunningSeat>,
    running_backends: &BTreeMap<String, RunningBackend>,
    unresolved: &mut BTreeSet<(String, String)>,
) -> MainResult<()> {
    stop_unconfigured_seats(configurations, running).await;

    for configuration in configurations {
        if configuration.desired_state == AgentSeatDesiredState::Stopped {
            if let Some(seat) = running.remove(&configuration.agent_id) {
                tracing::info!(agent_id = %configuration.agent_id, "stopping configured agent");
                stop_seat(seat).await;
            }
            continue;
        }

        let Some(profile) = catalog.profiles.get(&configuration.profile_id) else {
            if let Some(seat) = running.remove(&configuration.agent_id) {
                stop_seat(seat).await;
            }
            let key = (
                configuration.agent_id.clone(),
                configuration.profile_id.clone(),
            );
            if unresolved.insert(key) {
                tracing::error!(
                    agent_id = %configuration.agent_id,
                    profile_id = %configuration.profile_id,
                    "configured runtime profile is not approved on this machine"
                );
            }
            continue;
        };
        unresolved.remove(&(
            configuration.agent_id.clone(),
            configuration.profile_id.clone(),
        ));
        let inference = match &profile.inference_backend {
            Some(backend_id) => {
                let Some(backend) = running_backends.get(backend_id) else {
                    if let Some(seat) = running.remove(&configuration.agent_id) {
                        stop_seat(seat).await;
                    }
                    continue;
                };
                Some(&backend.description)
            }
            None => None,
        };
        let current_matches = running.get(&configuration.agent_id).is_some_and(|seat| {
            seat.revision == configuration.revision
                && seat.inference_backend == profile.inference_backend
                && !seat.task.is_finished()
        });
        if current_matches {
            continue;
        }
        if let Some(seat) = running.remove(&configuration.agent_id) {
            stop_seat(seat).await;
        }
        let desired = instantiate_profile(
            profile,
            &configuration.agent_id,
            &configuration.instructions,
            inference,
        )?;
        let child = CancellationToken::new();
        let run_cancellation = child.clone();
        let run_store = store.clone();
        let agent_id = configuration.agent_id.clone();
        let revision = configuration.revision;
        tracing::info!(
            %agent_id,
            profile_id = %configuration.profile_id,
            profile_label = %profile.label,
            %revision,
            "starting configured agent"
        );
        let task_agent_id = agent_id.clone();
        let task = tokio::spawn(async move {
            if let Err(error) = run_loaded_worker(run_store, desired, run_cancellation, None).await
            {
                tracing::error!(agent_id = %task_agent_id, %revision, %error, "configured agent stopped with an error");
            }
        });
        running.insert(
            agent_id,
            RunningSeat {
                revision,
                inference_backend: profile.inference_backend.clone(),
                cancellation: child,
                task,
            },
        );
    }
    Ok(())
}

async fn stop_unconfigured_seats(
    configurations: &[AgentSeatConfiguration],
    running: &mut BTreeMap<String, RunningSeat>,
) {
    let desired_ids = configurations
        .iter()
        .map(|configuration| configuration.agent_id.as_str())
        .collect::<BTreeSet<_>>();
    let retired = running
        .keys()
        .filter(|agent_id| !desired_ids.contains(agent_id.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    for agent_id in retired {
        if let Some(seat) = running.remove(&agent_id) {
            stop_seat(seat).await;
        }
    }
}

async fn stop_unused_backends(
    desired_backends: &BTreeSet<String>,
    running_backends: &mut BTreeMap<String, RunningBackend>,
    backend_retry_after: &mut BTreeMap<String, Instant>,
) {
    let unused_backends = running_backends
        .keys()
        .filter(|backend_id| !desired_backends.contains(*backend_id))
        .cloned()
        .collect::<Vec<_>>();
    for backend_id in unused_backends {
        if let Some(backend) = running_backends.remove(&backend_id) {
            stop_backend(&backend_id, backend).await;
        }
        backend_retry_after.remove(&backend_id);
    }
}

async fn start_backend(backend: &InferenceBackendFile) -> MainResult<RunningBackend> {
    let process = PluginProcess::start(backend.plugin.spec()).await?;
    let client = process.into_inference_openai()?;
    let description = client.describe().await?;
    Ok(RunningBackend {
        client,
        description,
    })
}

async fn stop_seats_using_backend(running: &mut BTreeMap<String, RunningSeat>, backend_id: &str) {
    let agent_ids = running
        .iter()
        .filter(|(_, seat)| seat.inference_backend.as_deref() == Some(backend_id))
        .map(|(agent_id, _)| agent_id.clone())
        .collect::<Vec<_>>();
    for agent_id in agent_ids {
        if let Some(seat) = running.remove(&agent_id) {
            stop_seat(seat).await;
        }
    }
}

async fn stop_backend(backend_id: &str, backend: RunningBackend) {
    tracing::info!(%backend_id, "stopping shared inference backend");
    if let Err(error) = backend.client.shutdown().await {
        tracing::warn!(%backend_id, %error, "inference backend did not stop cleanly");
    }
}

async fn stop_seat(seat: RunningSeat) {
    seat.cancellation.cancel();
    if let Err(error) = seat.task.await {
        tracing::warn!(%error, "configured agent task did not join cleanly");
    }
}

fn load_profile_catalog(path: &Path) -> MainResult<ProfileCatalog> {
    require_private_regular_file(path, "worker profile catalog")?;
    let metadata = fs::metadata(path)?;
    if metadata.len() > MAX_CATALOG_BYTES {
        return Err(format!("worker profile catalog exceeds {MAX_CATALOG_BYTES} bytes").into());
    }
    let source = fs::read(path)?;
    let file: ProfileCatalogFile = serde_json::from_slice(&source)
        .map_err(|error| format!("worker profile catalog is invalid: {error}"))?;
    if !matches!(file.schema_version, 1 | 2) {
        return Err(format!(
            "unsupported worker profile catalog schema version {}; expected 1 or 2",
            file.schema_version
        )
        .into());
    }
    if file.schema_version == 1 && !file.inference_backends.is_empty() {
        return Err("worker profile catalog schema 1 cannot declare inference backends".into());
    }
    if file.profiles.is_empty() || file.profiles.len() > MAX_PROFILES {
        return Err(format!(
            "worker profile catalog must contain between 1 and {MAX_PROFILES} profiles"
        )
        .into());
    }
    if file.inference_backends.len() > MAX_INFERENCE_BACKENDS {
        return Err(format!(
            "worker profile catalog may contain at most {MAX_INFERENCE_BACKENDS} inference backends"
        )
        .into());
    }
    let mut inference_backends = BTreeMap::new();
    for backend in file.inference_backends {
        validate_backend(&backend)?;
        let backend_id = backend.id.clone();
        if inference_backends
            .insert(backend_id.clone(), backend)
            .is_some()
        {
            return Err(format!("duplicate inference backend ID {backend_id}").into());
        }
    }
    let mut profiles = BTreeMap::new();
    for profile in file.profiles {
        validate_profile(&profile)?;
        if file.schema_version == 1 && profile.inference_backend.is_some() {
            return Err(
                "worker profile catalog schema 1 cannot reference an inference backend".into(),
            );
        }
        if let Some(backend_id) = &profile.inference_backend
            && !inference_backends.contains_key(backend_id)
        {
            return Err(format!(
                "worker profile {} references unknown inference backend {backend_id}",
                profile.id
            )
            .into());
        }
        // Instantiate with bounded sentinels so every private worker block is
        // rejected before the supervisor begins reconciliation.
        instantiate_profile(&profile, "catalog-validation", "", None)?;
        let profile_id = profile.id.clone();
        if profiles.insert(profile_id.clone(), profile).is_some() {
            return Err(format!("duplicate worker profile ID {profile_id}").into());
        }
    }
    Ok(ProfileCatalog {
        profiles,
        inference_backends,
    })
}

fn instantiate_profile(
    profile: &RuntimeProfileFile,
    agent_id: &str,
    instructions: &str,
    inference: Option<&InferenceDescribeResult>,
) -> MainResult<WorkerFileConfig> {
    let mut worker = profile
        .worker
        .as_object()
        .cloned()
        .ok_or("worker profile worker must be a JSON object")?;
    if worker.contains_key("agent_id") || worker.contains_key("instructions") {
        return Err(format!(
            "worker profile {} must not set agent_id or instructions",
            profile.id
        )
        .into());
    }
    worker.insert("agent_id".to_owned(), Value::String(agent_id.to_owned()));
    worker.insert(
        "instructions".to_owned(),
        Value::String(instructions.to_owned()),
    );
    let plugin = worker
        .get_mut("plugin")
        .and_then(Value::as_object_mut)
        .ok_or("worker profile plugin must be a JSON object")?;
    let config = plugin
        .entry("config")
        .or_insert_with(empty_json_object)
        .as_object_mut()
        .ok_or("worker profile plugin config must be a JSON object")?;
    if config.contains_key("inference") {
        return Err(format!(
            "worker profile {} must not pre-resolve an inference route",
            profile.id
        )
        .into());
    }
    if let Some(inference) = inference {
        config.insert("inference".to_owned(), serde_json::to_value(inference)?);
    }
    parse_worker_config_value(Value::Object(worker))
}

fn validate_backend(backend: &InferenceBackendFile) -> MainResult<()> {
    if !valid_profile_id(&backend.id) {
        return Err(format!(
            "inference backend ID must contain 1 to {MAX_PROFILE_ID_BYTES} ASCII letters, digits, dots, dashes, or underscores"
        )
        .into());
    }
    if backend.label.trim().is_empty() || backend.label.len() > MAX_PROFILE_LABEL_BYTES {
        return Err(format!(
            "inference backend label must contain between 1 and {MAX_PROFILE_LABEL_BYTES} bytes"
        )
        .into());
    }
    Ok(())
}

fn validate_profile(profile: &RuntimeProfileFile) -> MainResult<()> {
    if !valid_profile_id(&profile.id) {
        return Err(format!(
            "profile ID must contain 1 to {MAX_PROFILE_ID_BYTES} ASCII letters, digits, dots, dashes, or underscores"
        )
        .into());
    }
    for (label, value, maximum) in [
        (
            "profile label",
            profile.label.as_str(),
            MAX_PROFILE_LABEL_BYTES,
        ),
        (
            "profile description",
            profile.description.as_str(),
            MAX_PROFILE_DESCRIPTION_BYTES,
        ),
    ] {
        if value.trim().is_empty() || value.len() > maximum {
            return Err(format!("{label} must contain between 1 and {maximum} bytes").into());
        }
    }
    Ok(())
}

impl InferencePluginFile {
    fn spec(&self) -> PluginSpec {
        let mut spec = PluginSpec::new(self.id.clone(), self.executable.clone())
            .with_config(self.config.clone())
            .require_interface(inference_openai_interface())
            .with_initialize_timeout(Duration::from_millis(self.initialize_timeout_ms))
            .with_request_timeout(Duration::from_millis(self.request_timeout_ms))
            .with_shutdown_timeout(Duration::from_millis(self.shutdown_timeout_ms));
        for argument in &self.args {
            spec = spec.with_arg(argument);
        }
        spec
    }
}

fn empty_json_object() -> Value {
    serde_json::json!({})
}

const fn default_backend_initialize_timeout_ms() -> u64 {
    16 * 60 * 1_000
}

const fn default_backend_request_timeout_ms() -> u64 {
    10_000
}

const fn default_backend_shutdown_timeout_ms() -> u64 {
    10_000
}

fn valid_profile_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PROFILE_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn acquire_supervisor_lock(database: &Path) -> MainResult<File> {
    let lock_path = PathBuf::from(format!("{}.worker-supervisor.lock", database.display()));
    let file = private_lock_file(&lock_path)?;
    file.try_lock()
        .map_err(|error| -> Box<dyn Error + Send + Sync> {
            format!(
                "another worker supervisor already owns {}: {error}",
                lock_path.display()
            )
            .into()
        })?;
    Ok(file)
}

fn private_lock_file(path: &Path) -> Result<File, Box<dyn Error + Send + Sync>> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

fn require_private_regular_file(path: &Path, label: &str) -> MainResult<()> {
    if !path.is_absolute() {
        return Err(format!("{label} path must be absolute").into());
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(format!("{label} must be a regular file, not a link").into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(format!("{label} must not grant group or other permissions").into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use fleetd::plugin::{
        InferenceBackendIdentity, InferenceDescribeResult, InferenceEndpoint, InferenceModelRoute,
    };
    use serde_json::json;

    use super::{RuntimeProfileFile, instantiate_profile, validate_profile};

    fn profile() -> RuntimeProfileFile {
        RuntimeProfileFile {
            id: "opencode.glm".to_owned(),
            label: "OpenCode · GLM".to_owned(),
            description: "Approved local coding runtime".to_owned(),
            inference_backend: None,
            worker: json!({
                "schema_version": 2,
                "working_directory": env!("CARGO_MANIFEST_DIR"),
                "adapter": {
                    "kind": "envelope",
                    "inbound": {"schema_version": 1, "message_kinds": ["work.request/v1"]}
                },
                "plugin": {"id": "mock.harness", "executable": "/usr/bin/false"}
            }),
        }
    }

    #[test]
    fn a_profile_supplies_execution_while_identity_is_injected() {
        let profile = profile();
        validate_profile(&profile).expect("profile");
        let desired = instantiate_profile(&profile, "agent-1", "Review carefully.", None)
            .expect("instantiated worker");
        assert_eq!(desired.agent_id, "agent-1");
    }

    #[test]
    fn a_profile_cannot_preempt_operator_selected_identity() {
        let mut profile = profile();
        profile.worker["agent_id"] = json!("attacker-selected");
        assert!(instantiate_profile(&profile, "agent-1", "", None).is_err());
    }

    #[test]
    fn a_ready_backend_is_injected_without_exposing_launch_configuration() {
        let mut profile = profile();
        profile.inference_backend = Some("qwen-local".to_owned());
        let inference = InferenceDescribeResult {
            backend: InferenceBackendIdentity {
                name: "MLX-VLM".to_owned(),
                version: "0.6.15".to_owned(),
                executable_digest: format!("sha256:{}", "a".repeat(64)),
            },
            endpoint: InferenceEndpoint {
                base_url: "http://127.0.0.1:18082/v1".to_owned(),
                model: InferenceModelRoute {
                    id: "qwen".to_owned(),
                    name: "Qwen".to_owned(),
                    revision: None,
                },
            },
            profile_digest: format!("sha256:{}", "b".repeat(64)),
            observer: None,
        };
        let desired = instantiate_profile(&profile, "agent-1", "", Some(&inference))
            .expect("instantiated worker");
        assert_eq!(
            desired.plugin.config["inference"]["endpoint"]["model"]["id"],
            "qwen"
        );
        assert!(desired.plugin.config.get("executable").is_none());
    }
}
