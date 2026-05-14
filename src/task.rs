//! Create/start/stop/delete Task; pause/unpause; delete container.

use containerd_client::services::v1::snapshots::{MountsRequest, RemoveSnapshotRequest};
use containerd_client::services::v1::{
    CreateTaskRequest, DeleteContainerRequest, DeleteTaskRequest, GetContainerRequest, KillRequest,
    PauseTaskRequest, ResumeTaskRequest, StartRequest,
};

const SIGTERM: u32 = 15;
const SIGKILL: u32 = 9;

use crate::client::Client;
use crate::consts::DEFAULT_SNAPSHOTTER;
use crate::error::{Error, Result};
use crate::util::{poll_until, StatusExt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskId {
    container_id: String,
    pid: u32,
}

impl TaskId {
    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }
}

fn build_create_task_request(
    container_id: &str,
    rootfs: Vec<containerd_client::types::Mount>,
    stdout: String,
    stderr: String,
) -> CreateTaskRequest {
    CreateTaskRequest {
        container_id: container_id.to_string(),
        rootfs,
        stdin: String::new(),
        stdout,
        stderr,
        terminal: false,
        checkpoint: None,
        options: None,
        runtime_path: String::new(),
    }
}


pub(crate) async fn start_container(client: &Client, container_id: &str) -> Result<TaskId> {
    let get_req = client.ns_req(GetContainerRequest {
        id: container_id.to_string(),
    });
    let container_resp = client
        .containers_client()
        .get(get_req)
        .await
        .map_err(|e| {
            if e.code() == containerd_client::tonic::Code::NotFound {
                Error::ContainerNotFound(container_id.to_string())
            } else {
                e.into_crate_error("get_container")
            }
        })?;

    let container = container_resp
        .into_inner()
        .container
        .ok_or_else(|| Error::ContainerNotFound(container_id.to_string()))?;

    let mounts_req = client.ns_req(MountsRequest {
        snapshotter: container.snapshotter.clone(),
        key: container.snapshot_key.clone(),
    });
    let mounts_resp = client
        .snapshots()
        .mounts(mounts_req)
        .await
        .map_err(|e| e.into_crate_error("snapshot_mounts"))?;
    let rootfs = mounts_resp.into_inner().mounts;

    // Log files must exist before task create so the shim can open them.
    let (stdout_path, stderr_path) = crate::logs::prepare_log_files(container_id)?;

    let mut tasks_client = client.tasks();
    let create_req = build_create_task_request(
        container_id,
        rootfs,
        stdout_path.to_string_lossy().to_string(),
        stderr_path.to_string_lossy().to_string(),
    );
    let create_resp = tasks_client
        .create(client.ns_req(create_req))
        .await
        .map_err(|e| {
            if e.code() == containerd_client::tonic::Code::AlreadyExists {
                Error::TaskAlreadyExists(container_id.to_string())
            } else {
                e.into_crate_error("create_task")
            }
        })?;
    let pid = create_resp.into_inner().pid;

    let start_req = StartRequest {
        container_id: container_id.to_string(),
        exec_id: String::new(),
    };
    tasks_client
        .start(client.ns_req(start_req))
        .await
        .map_err(|e| e.into_crate_error("start_task"))?;

    client.record_task_start(container_id);

    Ok(TaskId {
        container_id: container_id.to_string(),
        pid,
    })
}


/// Sends SIGTERM. Idempotent on already-stopped tasks.
pub(crate) async fn stop_task(client: &Client, container_id: &str) -> Result<()> {
    let req = client.ns_req(KillRequest {
        container_id: container_id.to_string(),
        exec_id: String::new(),
        signal: SIGTERM,
        all: true,
    });
    match client.tasks().kill(req).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.code() == containerd_client::tonic::Code::NotFound {
                Err(Error::TaskNotFound(container_id.to_string()))
            } else if e.code() == containerd_client::tonic::Code::FailedPrecondition {
                // Task already stopped.
                Ok(())
            } else {
                Err(e.into_crate_error("kill_task"))
            }
        }
    }
}

/// Sends an arbitrary signal. Idempotent on missing or stopped tasks.
async fn kill_task(client: &Client, container_id: &str, signal: u32) -> Result<()> {
    let req = client.ns_req(KillRequest {
        container_id: container_id.to_string(),
        exec_id: String::new(),
        signal,
        all: true,
    });
    match client.tasks().kill(req).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.code() == containerd_client::tonic::Code::NotFound
                || e.code() == containerd_client::tonic::Code::FailedPrecondition
            {
                Ok(())
            } else {
                Err(e.into_crate_error("kill_task"))
            }
        }
    }
}

async fn is_task_running(client: &Client, container_id: &str) -> bool {
    use crate::inspect::get_task_info;
    use crate::types::TaskStatus;
    get_task_info(client, container_id)
        .await
        .is_some_and(|t| t.status == TaskStatus::Running)
}

/// Polls until task reaches `Stopped` (or is gone). Treats a missing
/// container as already-stopped.
pub(crate) async fn wait_for_task_stop(
    client: &Client,
    container_id: &str,
    timeout: std::time::Duration,
) -> Result<()> {
    use crate::types::TaskStatus;

    poll_until(
        std::time::Instant::now(),
        timeout,
        std::time::Duration::from_millis(100),
        format!("task {} did not stop within {:?}", container_id, timeout),
        || async {
            match crate::inspect::inspect_container(client, container_id).await {
                // No task entry = stopped (or never started); same for missing container.
                Ok(info) => info
                    .task
                    .as_ref()
                    .is_none_or(|t| t.status == TaskStatus::Stopped),
                Err(Error::ContainerNotFound(_)) => true,
                Err(_) => false,
            }
        },
    )
    .await
}

/// Polls until the task reaches `Stopped`, then returns its exit code.
/// Errors if the container/task is missing, or if the deadline elapses.
///
/// Captures the exit code from the same inspect call that first observed
/// `Stopped`. A previous version re-fetched after the predicate returned
/// true, which could race with concurrent `delete_container` and surface a
/// spurious `TaskNotFound` even though we'd just seen the task exit.
pub(crate) async fn wait_for_exit(
    client: &Client,
    container_id: &str,
    timeout: std::time::Duration,
) -> Result<i32> {
    use crate::types::TaskStatus;
    let start = std::time::Instant::now();
    let interval = std::time::Duration::from_millis(100);
    loop {
        if start.elapsed() > timeout {
            return Err(Error::Timeout(format!(
                "task {} did not exit within {:?}",
                container_id, timeout
            )));
        }
        match crate::inspect::inspect_container(client, container_id).await {
            Ok(info) => {
                if let Some(task) = info.task {
                    if task.status == TaskStatus::Stopped {
                        return task.exit_code.ok_or_else(|| {
                            Error::TaskExited(format!(
                                "task {} stopped but no exit code reported",
                                container_id
                            ))
                        });
                    }
                }
            }
            Err(Error::ContainerNotFound(_)) => {
                return Err(Error::ContainerNotFound(container_id.to_string()));
            }
            Err(_) => {} // transient; retry
        }
        tokio::time::sleep(interval).await;
    }
}

/// Polls `delete_task` for up to `timeout`. Tolerates poststop hooks that
/// take a moment to settle (returning `signal: terminated` etc); the next
/// attempt typically succeeds within ~10ms.
/// Classify a `Containerd` error as worth retrying. Retries are useful for
/// transient races (e.g. poststop hooks still running, momentary
/// unavailability) but not for definitive failures like authn/authz issues
/// or malformed args, which won't change between retries.
fn is_retryable_delete_error(err: &Error) -> bool {
    use containerd_client::tonic::Code;
    match err {
        Error::Containerd { source, .. } => matches!(
            source.code(),
            Code::FailedPrecondition | Code::Unavailable | Code::Internal | Code::Aborted | Code::DeadlineExceeded
        ),
        _ => false,
    }
}

pub(crate) async fn retry_delete_task(
    client: &Client,
    container_id: &str,
    timeout: std::time::Duration,
) -> Result<()> {
    use std::time::Instant;
    let start = Instant::now();
    let mut attempts = 0;
    loop {
        match delete_task(client, container_id).await {
            Ok(()) => {
                if attempts > 0 {
                    tracing::debug!(container_id, attempts, "delete_task succeeded after retries");
                }
                return Ok(());
            }
            Err(e) => {
                attempts += 1;
                if !is_retryable_delete_error(&e) {
                    tracing::warn!(container_id, attempts, error = %e, "delete_task: non-retryable failure");
                    return Err(e);
                }
                if start.elapsed() >= timeout {
                    tracing::warn!(container_id, attempts, error = %e, "delete_task gave up after timeout");
                    return Err(e);
                }
                tracing::trace!(container_id, attempts, error = %e, "delete_task transient error; retrying");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }
}

/// Task must be stopped first. Idempotent if the task is already gone.
///
/// Also evicts the task's start time from the client-side tracker so
/// `probe_health` doesn't mistakenly apply the old run's start_period grace
/// to a future probe. This is the single chokepoint for "the task is gone":
/// `remove_container` calls this transitively, and so do the various
/// stop/kill paths.
pub(crate) async fn delete_task(client: &Client, container_id: &str) -> Result<()> {
    let req = client.ns_req(DeleteTaskRequest {
        container_id: container_id.to_string(),
    });
    let result = match client.tasks().delete(req).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.code() == containerd_client::tonic::Code::NotFound {
                Ok(())
            } else {
                Err(e.into_crate_error("delete_task"))
            }
        }
    };
    // Evict only on success/NotFound; real errors leave state uncertain.
    if result.is_ok() {
        client.forget_task_start(container_id);
    }
    result
}

/// Low-level: removes the container record, snapshot, and log files.
/// Task (if any) must already be deleted. Idempotent.
pub(crate) async fn remove_container(client: &Client, container_id: &str) -> Result<()> {
    let mut containers_client = client.containers_client();

    // Fetch the snapshotter name first; if the record is already gone, fall
    // back to the default so we can still attempt snapshot cleanup.
    let snapshotter = {
        let get_req = client.ns_req(GetContainerRequest {
            id: container_id.to_string(),
        });
        containers_client
            .get(get_req)
            .await
            .ok()
            .and_then(|r| r.into_inner().container)
            .map(|c| c.snapshotter)
            .unwrap_or_else(|| {
                // Debug, not warn: normal during delete_container retry loop.
                tracing::debug!(
                    container_id,
                    fallback = DEFAULT_SNAPSHOTTER,
                    "remove_container: container record gone; using default snapshotter for cleanup"
                );
                DEFAULT_SNAPSHOTTER.to_string()
            })
    };

    let delete_req = client.ns_req(DeleteContainerRequest {
        id: container_id.to_string(),
    });
    let result = match containers_client.delete(delete_req).await {
        Ok(_) => Ok(()),
        Err(e) => {
            if e.code() == containerd_client::tonic::Code::NotFound {
                Ok(())
            } else {
                Err(e.into_crate_error("delete_container"))
            }
        }
    };

    let snapshot_key = crate::container::snapshot_key_for(container_id);
    let remove_req = client.ns_req(RemoveSnapshotRequest {
        snapshotter,
        key: snapshot_key.clone(),
    });
    if let Err(e) = client.snapshots().remove(remove_req).await {
        if e.code() != containerd_client::tonic::Code::NotFound {
            // NotFound is fine - the snapshot may have been pruned already.
            // Anything else is an actual leak that operators want to see.
            tracing::warn!(container_id, key = %snapshot_key, error = %e, "snapshot leak: remove failed during container cleanup");
        }
    }

    crate::logs::cleanup_log_files(container_id);
    // `forget_task_start` happens in `delete_task` (the chokepoint).
    result
}

/// SIGTERM, wait, then SIGKILL. Leaves the container record intact.
pub(crate) async fn stop_container(
    client: &Client,
    container_id: &str,
    timeout: std::time::Duration,
) -> Result<()> {
    // Cap SIGTERM grace at 5s but never more than half the budget - SIGKILL
    // must always have time to run too.
    let sigterm_grace = (timeout / 2).min(std::time::Duration::from_secs(5));

    if let Err(e) = stop_task(client, container_id).await {
        tracing::debug!(container_id, error = %e, "stop_container: SIGTERM step failed; continuing");
    }
    if let Err(e) = wait_for_task_stop(client, container_id, sigterm_grace).await {
        tracing::debug!(container_id, error = %e, "stop_container: wait-after-SIGTERM failed; will try SIGKILL");
    }

    if is_task_running(client, container_id).await {
        if let Err(e) = kill_task(client, container_id, SIGKILL).await {
            tracing::warn!(container_id, error = %e, "stop_container: SIGKILL failed; task may be stuck");
        }
        let remaining = timeout.saturating_sub(sigterm_grace);
        wait_for_task_stop(client, container_id, remaining).await?;
    }

    Ok(())
}

/// Full cleanup: stop, delete task, remove container + snapshot. Idempotent.
pub(crate) async fn delete_container(
    client: &Client,
    container_id: &str,
    timeout: std::time::Duration,
) -> Result<()> {
    use crate::types::TaskStatus;
    use std::time::Instant;

    // Per-phase timeout (worst case 3×timeout). Shared deadline would
    // mis-attribute slow-phase-1 timeouts to phases 2/3.
    let poll_interval = std::time::Duration::from_millis(100);

    let _ = stop_container(client, container_id, timeout).await;

    let phase_start = Instant::now();
    poll_until(
        phase_start,
        timeout,
        poll_interval,
        format!("delete_container[phase=delete_task] timed out for {}", container_id),
        || async { delete_task(client, container_id).await.is_ok() },
    )
    .await?;

    // Wait for the task record to disappear; retry delete on stopped tasks
    // that haven't been cleaned up yet.
    let phase_start = Instant::now();
    poll_until(
        phase_start,
        timeout,
        poll_interval,
        format!(
            "delete_container[phase=task_record_gone] timed out for {} within {:?}",
            container_id, timeout
        ),
        || async {
            match crate::inspect::inspect_container(client, container_id).await {
                Ok(info) => {
                    if let Some(task) = &info.task {
                        if task.status == TaskStatus::Stopped {
                            let _ = delete_task(client, container_id).await;
                        }
                    }
                    info.task.is_none()
                }
                _ => true, // container gone or inspect failed - done
            }
        },
    )
    .await?;

    let _ = remove_container(client, container_id).await;

    let phase_start = Instant::now();
    poll_until(
        phase_start,
        timeout,
        poll_interval,
        format!(
            "delete_container[phase=container_record_gone] timed out for {} within {:?}",
            container_id, timeout
        ),
        || async {
            match crate::inspect::inspect_container(client, container_id).await {
                Err(Error::ContainerNotFound(_)) => true,
                Ok(_) => {
                    let _ = remove_container(client, container_id).await;
                    false
                }
                Err(_) => false,
            }
        },
    )
    .await?;

    Ok(())
}

pub(crate) async fn pause_container(client: &Client, container_id: &str) -> Result<()> {
    let req = client.ns_req(PauseTaskRequest {
        container_id: container_id.to_string(),
    });
    match client.tasks().pause(req).await {
        Ok(_) => Ok(()),
        Err(e) if e.code() == containerd_client::tonic::Code::NotFound => {
            Err(Error::TaskNotFound(container_id.to_string()))
        }
        Err(e) => Err(e.into_crate_error("pause_task")),
    }
}

pub(crate) async fn unpause_container(client: &Client, container_id: &str) -> Result<()> {
    let req = client.ns_req(ResumeTaskRequest {
        container_id: container_id.to_string(),
    });
    match client.tasks().resume(req).await {
        Ok(_) => Ok(()),
        Err(e) if e.code() == containerd_client::tonic::Code::NotFound => {
            Err(Error::TaskNotFound(container_id.to_string()))
        }
        Err(e) => Err(e.into_crate_error("unpause_task")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_create_task_request_sets_container_id() {
        let req = build_create_task_request(
            "test-container",
            vec![],
            "/logs/stdout".into(),
            "/logs/stderr".into(),
        );
        assert_eq!(req.container_id, "test-container");
        assert!(!req.terminal);
        assert!(req.stdin.is_empty());
        assert_eq!(req.stdout, "/logs/stdout");
        assert_eq!(req.stderr, "/logs/stderr");
    }

    #[test]
    fn build_create_task_request_includes_rootfs() {
        let mount = containerd_client::types::Mount {
            r#type: "overlay".to_string(),
            source: "/some/path".to_string(),
            target: String::new(),
            options: vec!["lowerdir=/a".to_string(), "upperdir=/b".to_string()],
        };
        let req =
            build_create_task_request("test-container", vec![mount], String::new(), String::new());
        assert_eq!(req.rootfs.len(), 1);
        assert_eq!(req.rootfs[0].r#type, "overlay");
    }


}
