//! Crate-wide error type and Result alias.

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("containerd socket not found at '{}'. Is containerd running? (hint: start Colima or set CONTAINERD_SOCKET)", path.display())]
    SocketNotFound { path: PathBuf },

    #[error("connection refused to containerd socket at '{}'. Is containerd running?", path.display())]
    ConnectionRefused { path: PathBuf },

    #[error("transport error")]
    Transport(#[source] containerd_client::tonic::transport::Error),

    /// A containerd gRPC call failed. `op` names the operation; `source`
    /// preserves the original `tonic::Status` for callers that need the code.
    #[error("containerd {op} failed")]
    Containerd {
        op: &'static str,
        #[source]
        source: containerd_client::tonic::Status,
    },

    #[error("io error: {context}")]
    Io {
        context: &'static str,
        #[source]
        source: std::io::Error,
    },

    #[error("image not found: {0}")]
    ImageNotFound(String),

    #[error("container already exists: {0}")]
    ContainerAlreadyExists(String),

    #[error("container not found: {0}")]
    ContainerNotFound(String),

    #[error("task already exists for container: {0}")]
    TaskAlreadyExists(String),

    #[error("task not found: {0}")]
    TaskNotFound(String),

    #[error("task exited before becoming ready: {0}")]
    TaskExited(String),

    #[error("snapshot already exists: {0}")]
    SnapshotAlreadyExists(String),

    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),

    #[error("snapshot '{name}' was not created by containerd-manager (missing managed label)")]
    SnapshotNotManaged { name: String },

    /// A `Mutex` guarding shared client state was poisoned (a thread holding
    /// the lock panicked). Operations that depend on the lock cannot proceed.
    #[error("internal client state poisoned: {0}")]
    StatePoisoned(&'static str),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("port forwarding error: {0}")]
    PortForward(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A finite resource (bridge IPs, host ports, etc.) is exhausted.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    #[error("not implemented")]
    NotImplemented,
}

pub type Result<T> = std::result::Result<T, Error>;
