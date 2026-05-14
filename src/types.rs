//! Data types returned by inspect / list operations, plus the
//! [`ContainerId`] newtype with built-in validation.

use std::collections::HashMap;
use std::fmt;

use containerd_client::types::v1::Status;

use crate::error::{Error, Result};
use crate::util::validate_identifier;

/// Validated containerd identifier. Construction is the only way to obtain
/// one, and it enforces containerd's identifier rules (see
/// [`crate::util::validate_identifier`]).
///
/// The newtype prevents shell injection / path traversal at the API boundary:
/// the crate interpolates container IDs into OCI hook shell scripts and into
/// `~/.containerd-manager/` filesystem paths.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerId(String);

impl ContainerId {
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        validate_identifier(&id)?;
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

impl TryFrom<&str> for ContainerId {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

impl TryFrom<String> for ContainerId {
    type Error = Error;
    fn try_from(s: String) -> Result<Self> {
        Self::new(s)
    }
}

#[derive(Debug, Clone)]
pub struct ContainerInfo {
    pub id: ContainerId,
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
            // `Created` is a transient pre-running state; surface it as
            // Running so callers don't need to handle it separately.
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
    fn container_id_round_trips() {
        let id = ContainerId::new("my-container").unwrap();
        assert_eq!(id.as_str(), "my-container");
        assert_eq!(format!("{}", id), "my-container");
    }

    #[test]
    fn container_id_rejects_invalid() {
        assert!(ContainerId::new("").is_err());
        assert!(ContainerId::new("../foo").is_err());
        assert!(ContainerId::new("foo;ls").is_err());
        assert!(ContainerId::new("foo bar").is_err());
    }

    #[test]
    fn container_id_try_from() {
        let id: ContainerId = "abc".try_into().unwrap();
        assert_eq!(id.as_str(), "abc");
        let owned: ContainerId = String::from("abc").try_into().unwrap();
        assert_eq!(owned.as_str(), "abc");
    }
}
