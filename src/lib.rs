//! containerd-manager: Rust library for managing containerd (containers, tasks, images).
//!
//! # Example
//!
//! ```ignore
//! use containerd_manager::connect;
//!
//! let client = connect(None)?;              // defaults to "default" namespace
//! client.pull_image("mongodb/mongodb-atlas-local:latest").await?;
//! ```

// `Error` is large because `Error::Containerd` carries a `tonic::Status`
// (~192 bytes). Boxing would dirty every error path; we accept slightly
// larger `Result` values instead.
#![allow(clippy::result_large_err)]

// `bridge` is pub(crate) for hook-script unit tests in subordinate modules.
pub(crate) mod bridge;
mod client;
mod clone;
mod connect;
mod consts;
mod container;
mod error;
mod exec;
mod image;
mod inspect;
mod list;
mod log_tailer;
mod logs;
mod port_forward;
mod readiness;
mod snapshot;
mod snapshot_util;
mod task;
mod types;
mod util;

/// Maps the Rust target architecture to the OCI platform architecture name.
pub(crate) fn oci_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

pub use client::Client;
pub use clone::CloneContainerOpts;
pub use connect::connect;
pub use container::{CreateContainerOpts, Mount, NetworkMode, NetworkOpts};
pub use error::{Error, Result};
pub use exec::{ExecOpts, ExecOutput, DEFAULT_EXEC_MAX_OUTPUT_BYTES};
pub use logs::{LogEntry, LogFollower, LogStream, LogsFilter};
pub use port_forward::{PortForwardHandle, PortForwardOpts};
pub use readiness::{parse_healthcheck, HealthCheck, HealthStatus, ReadinessStrategy};
pub use snapshot::{
    ResetToSnapshotOpts, RestoreContainerOpts, SnapshotContainerOpts, SnapshotInfo,
};
pub use task::TaskId;
pub use types::{ContainerId, ContainerInfo, TaskInfo, TaskStatus};
