//! Data types returned by inspect / list operations, plus the
//! [`ContainerId`] newtype.

use std::collections::HashMap;
use std::fmt;

use containerd_client::types::v1::Status;

use crate::error::Result;

/// Opaque, content-anchored container identifier. Minted by
/// [`Client::create_container`](crate::Client::create_container) /
/// `clone_container` / `restore_container`; treat as opaque.
///
/// IDs are globally unique and stable across restarts: deleting and
/// recreating a container with the same human name yields a new
/// `ContainerId`, so stale references (port-forward handles, log files,
/// snapshot keys) tied to the old ID safely fail rather than silently
/// targeting the new instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerId(String);

impl ContainerId {
    /// Generate a fresh UUIDv4-backed ID. Used by `create_container`.
    pub(crate) fn generate() -> Self {
        Self(uuid::Uuid::new_v4().simple().to_string())
    }

    /// Adopt an existing containerd container ID string. Used by
    /// `inspect` / `list` when reading records back, and by callers
    /// reloading a persisted ID. Validates length + charset so a corrupt
    /// record can't smuggle in shell metacharacters.
    pub fn from_existing(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        crate::util::validate_identifier(&id)?;
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for ContainerId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContainerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone)]
pub struct ContainerInfo {
    /// Opaque hash ID. Stable identifier across the container's lifetime.
    pub id: ContainerId,
    /// Human-readable name supplied at create time. Falls back to the ID
    /// for foreign containers (created via ctr/nerdctl without our label).
    pub name: String,
    pub image: String,
    pub labels: HashMap<String, String>,
    pub env: HashMap<String, String>,
    /// `(source, destination)` pairs.
    pub mounts: Vec<(String, String)>,
    pub task: Option<TaskInfo>,
    /// `(host_port, container_port)` pairs.
    pub port_forwards: Vec<(u16, u16)>,
}

#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub pid: u32,
    pub status: TaskStatus,
    /// Set only when `status == Stopped`. `None` for running, paused, or
    /// unknown tasks.
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    Stopped,
    Paused,
    Unknown,
}

impl From<i32> for TaskStatus {
    fn from(status: i32) -> Self {
        match status {
            x if x == Status::Running as i32 => TaskStatus::Running,
            x if x == Status::Stopped as i32 => TaskStatus::Stopped,
            x if x == Status::Paused as i32 => TaskStatus::Paused,
            x if x == Status::Pausing as i32 => TaskStatus::Paused,
            // `Created` is transient pre-running; surface as Running.
            x if x == Status::Created as i32 => TaskStatus::Running,
            _ => TaskStatus::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_status_from_running() {
        assert_eq!(
            TaskStatus::from(Status::Running as i32),
            TaskStatus::Running
        );
    }

    #[test]
    fn task_status_from_stopped() {
        assert_eq!(
            TaskStatus::from(Status::Stopped as i32),
            TaskStatus::Stopped
        );
    }

    #[test]
    fn task_status_from_paused() {
        assert_eq!(TaskStatus::from(Status::Paused as i32), TaskStatus::Paused);
    }

    #[test]
    fn task_status_from_unknown() {
        assert_eq!(TaskStatus::from(99), TaskStatus::Unknown);
    }

    #[test]
    fn container_id_generate_is_unique_and_hex() {
        let a = ContainerId::generate();
        let b = ContainerId::generate();
        assert_ne!(a, b);
        assert_eq!(a.as_str().len(), 32);
        assert!(a.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }
}
