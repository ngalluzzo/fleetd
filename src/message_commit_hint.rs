//! Lossy local wakeups for message commits made outside the daemon process.
//!
//! `SQLite` remains the only authority. A hint contains no message data and may
//! be duplicated or lost; every stream responds by replaying its own durable
//! cursor from `SQLite`.

use std::{path::Path, sync::Arc};

use sha2::{Digest, Sha256};
use tokio::{sync::broadcast, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{error::FleetError, model::Message};

const HINT_BYTE: u8 = 1;

/// One process-local or cross-process reason to reconcile durable messages.
#[derive(Clone, Debug)]
pub(crate) enum MessageCommitWake {
    Committed(Box<Message>),
    External,
}

/// Best-effort sender used by a separate local writer process.
#[derive(Clone)]
pub(crate) struct MessageCommitNotifier {
    #[cfg(unix)]
    socket: Arc<std::os::unix::net::UnixDatagram>,
    #[cfg(unix)]
    address: Arc<std::path::PathBuf>,
}

impl MessageCommitNotifier {
    pub(crate) fn for_database(path: &Path) -> Result<Self, FleetError> {
        #[cfg(unix)]
        {
            let address = hint_address(path)?;
            let socket = std::os::unix::net::UnixDatagram::unbound()?;
            socket.set_nonblocking(true)?;
            Ok(Self {
                socket: Arc::new(socket),
                address: Arc::new(address),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Ok(Self {})
        }
    }

    /// Sends one content-free wakeup. Missing or congested listeners are
    /// intentionally ignored because cursor replay is the recovery path.
    pub(crate) fn notify(&self) {
        #[cfg(unix)]
        {
            let _unused = self.socket.send_to(&[HINT_BYTE], self.address.as_path());
        }
    }
}

/// Daemon-owned bridge from a private local datagram to the in-process bus.
pub(crate) struct MessageCommitHintBridge {
    #[cfg(unix)]
    cancellation: CancellationToken,
    #[cfg(unix)]
    task: JoinHandle<()>,
    #[cfg(unix)]
    address: std::path::PathBuf,
}

impl MessageCommitHintBridge {
    pub(crate) fn bind(
        database_path: &Path,
        wakes: broadcast::Sender<MessageCommitWake>,
    ) -> Result<Self, FleetError> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            let address = hint_address(database_path)?;
            remove_stale_socket(&address)?;
            let socket = tokio::net::UnixDatagram::bind(&address)?;
            std::fs::set_permissions(&address, std::fs::Permissions::from_mode(0o600))?;
            let cancellation = CancellationToken::new();
            let task_cancellation = cancellation.clone();
            let task = tokio::spawn(async move {
                let mut byte = [0_u8; 1];
                loop {
                    tokio::select! {
                        () = task_cancellation.cancelled() => return,
                        received = socket.recv(&mut byte) => match received {
                            Ok(_) => {
                                let _unused = wakes.send(MessageCommitWake::External);
                            }
                            Err(error) => {
                                tracing::warn!(%error, "message commit hint listener stopped");
                                return;
                            }
                        }
                    }
                }
            });
            Ok(Self {
                cancellation,
                task,
                address,
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (database_path, wakes);
            Ok(Self {})
        }
    }
}

impl Drop for MessageCommitHintBridge {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            self.cancellation.cancel();
            self.task.abort();
            let _unused = std::fs::remove_file(&self.address);
        }
    }
}

#[cfg(unix)]
fn hint_address(database_path: &Path) -> Result<std::path::PathBuf, FleetError> {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::fs::MetadataExt as _;

    let canonical = std::fs::canonicalize(database_path)?;
    let owner = std::fs::metadata(&canonical)?.uid();
    let directory = std::path::PathBuf::from(format!("/tmp/fleetd-message-hints-{owner}"));
    ensure_private_directory(&directory, owner)?;
    let digest = format!("{:x}", Sha256::digest(canonical.as_os_str().as_bytes()));
    Ok(directory.join(format!("{}.sock", &digest[..32])))
}

#[cfg(unix)]
fn ensure_private_directory(path: &Path, owner: u32) -> Result<(), FleetError> {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.uid() != owner
                || metadata.mode() & 0o777 != 0o700
            {
                return Err(FleetError::Conflict(
                    "message commit hint directory is not private to the database owner".to_owned(),
                ));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(path)?;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
            let metadata = std::fs::symlink_metadata(path)?;
            if metadata.uid() != owner || metadata.mode() & 0o777 != 0o700 {
                return Err(FleetError::Conflict(
                    "message commit hint directory could not be made private to the database owner"
                        .to_owned(),
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

#[cfg(unix)]
fn remove_stale_socket(path: &Path) -> Result<(), FleetError> {
    if !path.exists() {
        return Ok(());
    }
    let probe = std::os::unix::net::UnixDatagram::unbound()?;
    if probe.connect(path).is_ok() && probe.send(&[HINT_BYTE]).is_ok() {
        return Err(FleetError::Conflict(
            "another daemon owns the message commit hint listener for this database".to_owned(),
        ));
    }
    std::fs::remove_file(path)?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn content_free_hint_wakes_the_exact_database_bridge() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let database_path = directory.path().join("fleetd.db");
        std::fs::write(&database_path, []).expect("create database identity");
        let (wakes, mut receiver) = broadcast::channel(8);
        let bridge =
            MessageCommitHintBridge::bind(&database_path, wakes).expect("bind hint bridge");
        let notifier =
            MessageCommitNotifier::for_database(&database_path).expect("create commit notifier");

        notifier.notify();
        let wake = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .expect("bounded wake")
            .expect("open bridge");
        assert!(matches!(wake, MessageCommitWake::External));
        drop(bridge);
    }
}
