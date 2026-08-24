use fleetd_acp_host::{DriverConfig, DriverError, PluginDefinition, serve};
use serde_json::Value;

#[tokio::main]
async fn main() {
    let definition = PluginDefinition::new(
        "fleetd.acp-reference",
        "fleetd ACP reference plugin",
        env!("CARGO_PKG_VERSION"),
        &["HOME", "PATH", "TERM", "TMPDIR"],
        prepare_config,
    );
    if let Err(error) = serve(definition).await {
        eprintln!("fleetd ACP reference plugin failed: {error}");
        std::process::exit(1);
    }
}

fn prepare_config(value: Value) -> Result<DriverConfig, DriverError> {
    serde_json::from_value(value).map_err(DriverError::Json)
}
