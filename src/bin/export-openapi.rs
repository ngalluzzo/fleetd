//! Writes fleetd's collected `OpenAPI` contract to a deterministic JSON artifact.

use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = env::args_os().nth(1).map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("openapi/fleetd-v1.json"),
        PathBuf::from,
    );
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut encoded = serde_json::to_string_pretty(&fleetd::api::openapi_document())?;
    encoded.push('\n');
    fs::write(&output, encoded)?;
    println!("wrote {}", output.display());
    Ok(())
}
