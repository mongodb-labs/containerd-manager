//! Named snapshot + restore of container rootfs.
//!
//! - `snapshot_container`: freezes the writable layer into a named base.
//! - `restore_container`: instantiates a new container from a base.
//! - `reset_to_snapshot`: rewinds an existing container's rootfs in place,
//!   preserving network identity (bridge IP, host ports) and the OCI spec.
//! - `list_snapshots` / `delete_snapshot`: lifecycle.
//!
//! Compared to [`crate::clone::clone_container`]: clone bundles freeze and
//! instantiate into one call with an implementation-managed base name.
//! Snapshot/restore decouples them: name is user-chosen and persists across
//! src deletion.
//!
//! Snapshots carry [`MANAGED_LABEL`] + [`IMAGE_LABEL`]. Restore, reset, and
//! delete reject snapshots missing these so callers can't accidentally
//! touch image-layer or third-party snapshots.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use containerd_client::services::v1::snapshots::snapshots_client::SnapshotsClient;
use containerd_client::services::v1::snapshots::{
    CommitSnapshotRequest, ListSnapshotsRequest, PrepareSnapshotRequest, RemoveSnapshotRequest,
    StatSnapshotRequest,
};
use containerd_client::tonic::Code;

use crate::client::Client;
use crate::consts::SNAPSHOT_OP_STOP_TIMEOUT;
use crate::container::{CreateContainerOpts, NetworkMode};
use crate::error::{Error, Result};
use crate::snapshot_util::fetch_container_record;
use crate::types::{ContainerId, TaskStatus};
use crate::util::StatusExt;

/// Source image ref. Restore reads this so callers needn't remember it.
pub(crate) const IMAGE_LABEL: &str = "containerd-manager.snapshot.image";
/// Marker filtering our snapshots from image-layer / runtime / stargz ones.
pub(crate) const MANAGED_LABEL: &str = "containerd-manager.snapshot.managed";
/// Free-form description surfaced by `list_snapshots`.
pub(crate) const DESCRIPTION_LABEL: &str = "containerd-manager.snapshot.description";

#[derive(Debug, Clone, Default, typed_builder::TypedBuilder)]
#[builder(doc)]
pub struct SnapshotContainerOpts {
    /// Restart the source after committing its writable layer. Default
    /// `false` — the most common workflow is "prime, snapshot, leave
    /// stopped, restore N times".
    #[builder(default = false)]
    pub restart_src: bool,
    /// Free-form description stored alongside the snapshot, surfaced by
    /// [`Client::list_snapshots`].
    #[builder(default, setter(strip_option, into))]
    pub description: Option<String>,
}


#[derive(Debug, Clone, Default, typed_builder::TypedBuilder)]
#[builder(doc, mutators(
    /// Add an env var. May be called repeatedly.
    pub fn env(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.env.insert(key.into(), value.into());
    }
    /// Add a label. May be called repeatedly.
    pub fn label(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.labels.insert(key.into(), value.into());
    }
))]
pub struct RestoreContainerOpts {
    #[builder(default, setter(strip_option))]
    pub network: Option<NetworkMode>,
    /// Container ports to expose on the restored container. Host ports are
    /// always allocated fresh. Empty = no port bindings.
    #[builder(default)]
    pub port_bindings: Vec<u16>,
    #[builder(via_mutators)]
    pub env: HashMap<String, String>,
    #[builder(via_mutators)]
    pub labels: HashMap<String, String>,
}


#[derive(Debug, Clone, Default, typed_builder::TypedBuilder)]
#[builder(doc)]
pub struct ResetToSnapshotOpts {
    /// Restart the container after re-preparing its writable layer. Default
    /// `false` — caller drives the next `start_container` so they can run
    /// fixture-loading code or change labels between reset and start.
    #[builder(default = false)]
    pub restart: bool,
}


#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub name: String,
    pub image: Option<String>,
    pub description: Option<String>,
    pub created_at: Option<SystemTime>,
    pub parent: String,
}

pub(crate) async fn snapshot_container(
    client: &Client,
    src: &ContainerId,
    name: &str,
    opts: SnapshotContainerOpts,
) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidArgument("snapshot name must not be empty".into()));
    }
    tracing::info!(
        src = %src,
        snapshot = %name,
        restart_src = opts.restart_src,
        "snapshot_container: begin"
    );

    let src_info = crate::inspect::inspect_container(client, src.as_str()).await?;
    let was_running = matches!(
        src_info.task.as_ref().map(|t| t.status),
        Some(TaskStatus::Running)
    );

    if was_running {
        crate::task::stop_container(client, src.as_str(), SNAPSHOT_OP_STOP_TIMEOUT).await?;
        crate::task::retry_delete_task(client, src.as_str(), SNAPSHOT_OP_STOP_TIMEOUT).await?;
    }

    let record = fetch_container_record(client, src.as_str()).await?;
    let snapshotter = record.snapshotter().to_string();
    let snapshot_key = record.snapshot_key().to_string();
    let image = record.image().to_string();

    let mut labels = HashMap::new();
    labels.insert(IMAGE_LABEL.to_string(), image);
    labels.insert(MANAGED_LABEL.to_string(), "1".to_string());
    if let Some(ref desc) = opts.description {
        labels.insert(DESCRIPTION_LABEL.to_string(), desc.clone());
    }

    let mut snap = client.snapshots();

    // Commit src active → named snapshot. Race-tolerant: AlreadyExists from
    // the commit RPC is the loser's signal; no pre-stat needed (would race).
    if let Err(e) = snap
        .commit(client.ns_req(CommitSnapshotRequest {
            snapshotter: snapshotter.clone(),
            name: name.to_string(),
            key: snapshot_key.clone(),
            labels,
        }))
        .await
    {
        let mapped = if e.code() == Code::AlreadyExists {
            Error::SnapshotAlreadyExists(name.to_string())
        } else {
            e.into_crate_error("snapshot_commit")
        };
        // Commit failed after we stopped+deleted the task; restart so the
        // caller's container isn't stranded.
        if was_running && opts.restart_src {
            tracing::warn!(
                src = %src,
                error = %mapped,
                "snapshot_container: commit failed; attempting src restart"
            );
            if let Err(restart_err) = client.start_container(src).await {
                tracing::warn!(src = %src, error = %restart_err, "snapshot_container: src restart after commit failure also failed");
            }
        }
        return Err(mapped);
    }

    // Re-prepare src's active layer rooted at the new snapshot. Failure
    // here bricks the src (no active layer); roll back the snapshot.
    if let Err(e) = snap
        .prepare(client.ns_req(PrepareSnapshotRequest {
            snapshotter: snapshotter.clone(),
            key: snapshot_key,
            parent: name.to_string(),
            labels: HashMap::new(),
        }))
        .await
    {
        tracing::warn!(
            snapshot = %name,
            error = %e,
            "snapshot_container: reprepare failed; rolling back committed snapshot"
        );
        let _ = snap
            .remove(client.ns_req(RemoveSnapshotRequest {
                snapshotter,
                key: name.to_string(),
            }))
            .await;
        return Err(e.into_crate_error("snapshot_reprepare_src"));
    }

    if was_running && opts.restart_src {
        // Best-effort: snapshot already succeeded; warn on restart failure
        // rather than throwing away the snapshot.
        if let Err(e) = client.start_container(src).await {
            tracing::warn!(
                src = %src,
                snapshot = %name,
                error = %e,
                "snapshot_container: snapshot committed but src restart failed"
            );
        }
    }

    tracing::info!(snapshot = %name, "snapshot_container: done");
    Ok(())
}

pub(crate) async fn restore_container(
    client: &Client,
    snapshot_name: &str,
    dst: &ContainerId,
    opts: RestoreContainerOpts,
) -> Result<()> {
    if snapshot_name.is_empty() {
        return Err(Error::InvalidArgument("snapshot name must not be empty".into()));
    }
    tracing::info!(snapshot = %snapshot_name, dst = %dst, "restore_container: begin");

    let snapshotter = crate::consts::DEFAULT_SNAPSHOTTER.to_string();
    let mut snap = client.snapshots();

    let image = stat_managed_snapshot(client, &mut snap, &snapshotter, snapshot_name).await?;

    let dst_snapshot_key = crate::container::snapshot_key_for(dst.as_str());
    let prep_req = client.ns_req(PrepareSnapshotRequest {
        snapshotter: snapshotter.clone(),
        key: dst_snapshot_key.clone(),
        parent: snapshot_name.to_string(),
        labels: HashMap::new(),
    });
    snap.prepare(prep_req).await.map_err(|e| {
        if e.code() == Code::AlreadyExists {
            Error::ContainerAlreadyExists(dst.as_str().to_string())
        } else {
            e.into_crate_error("restore_prepare_dst")
        }
    })?;

    let network_mode = opts.network.unwrap_or(NetworkMode::Bridge);
    // Build via direct field mutation; see clone.rs for rationale.
    let mut create_opts = CreateContainerOpts::default();
    create_opts.network.mode = network_mode;
    create_opts.from_existing_snapshot = Some(dst_snapshot_key.clone());
    for (k, v) in &opts.env {
        create_opts.env.insert(k.clone(), v.clone());
    }
    for (k, v) in &opts.labels {
        create_opts.labels.insert(k.clone(), v.clone());
    }
    for &cp in &opts.port_bindings {
        create_opts.port_bindings.push((0, cp));
    }

    if let Err(e) =
        crate::container::create_container(client, dst.as_str(), &image, create_opts).await
    {
        tracing::warn!(
            dst = %dst,
            error = %e,
            "restore_container: dst create failed, removing orphan snapshot"
        );
        let _ = snap
            .remove(client.ns_req(RemoveSnapshotRequest {
                snapshotter: snapshotter.clone(),
                key: dst_snapshot_key,
            }))
            .await;
        return Err(e);
    }

    tracing::info!(snapshot = %snapshot_name, dst = %dst, "restore_container: done");
    Ok(())
}

/// Resets an existing container's rootfs back to a named snapshot. Discards
/// the current writable layer and re-prepares a fresh one parented at
/// `snapshot_name`. Network identity (bridge IP, host port allocations) and
/// the OCI spec are preserved — only on-disk data changes.
///
/// The task is stopped and its record dropped. If
/// [`ResetToSnapshotOpts::restart`] is `true` the container is started again
/// before this returns; otherwise the caller drives the next start.
///
/// # Errors
/// - [`Error::SnapshotNotFound`] / [`Error::SnapshotNotManaged`] if the
///   snapshot doesn't exist or wasn't created by this crate.
/// - [`Error::InvalidArgument`] if the snapshot's image doesn't match the
///   container's image.
pub(crate) async fn reset_to_snapshot(
    client: &Client,
    container_id: &ContainerId,
    snapshot_name: &str,
    opts: ResetToSnapshotOpts,
) -> Result<()> {
    if snapshot_name.is_empty() {
        return Err(Error::InvalidArgument("snapshot name must not be empty".into()));
    }
    tracing::info!(
        container = %container_id,
        snapshot = %snapshot_name,
        restart = opts.restart,
        "reset_to_snapshot: begin"
    );

    let record = fetch_container_record(client, container_id.as_str()).await?;
    let snapshotter = record.snapshotter().to_string();
    let snapshot_key = record.snapshot_key().to_string();
    let image = record.image().to_string();

    let mut snap = client.snapshots();

    let snapshot_image = stat_managed_snapshot(client, &mut snap, &snapshotter, snapshot_name).await?;
    if snapshot_image != image {
        return Err(Error::InvalidArgument(format!(
            "snapshot image '{}' does not match container image '{}'",
            snapshot_image, image
        )));
    }

    // Drop task + forwards so the snapshot can be mutated and restart binds
    // fresh ports. Stop failure usually = already stopped; log, don't fail.
    if let Err(e) = crate::task::stop_container(client, container_id.as_str(), SNAPSHOT_OP_STOP_TIMEOUT).await {
        tracing::debug!(
            container = %container_id,
            error = %e,
            "reset_to_snapshot: stop_container failed; proceeding to delete_task"
        );
    }
    crate::task::retry_delete_task(client, container_id.as_str(), SNAPSHOT_OP_STOP_TIMEOUT).await?;
    client.stop_managed_forwards(container_id.as_str()).await;

    // Same key name keeps the container record's snapshot_key valid.
    let remove_req = client.ns_req(RemoveSnapshotRequest {
        snapshotter: snapshotter.clone(),
        key: snapshot_key.clone(),
    });
    if let Err(e) = snap.remove(remove_req).await {
        if e.code() != Code::NotFound {
            return Err(e.into_crate_error("reset_remove_active"));
        }
    }

    // remove+prepare not atomic: prepare-fail leaves container's snapshot_key
    // pointing at nothing. Retry briefly for transient containerd errors;
    // on persistent failure recover via delete_container.
    let mut last_err = None;
    for attempt in 0..5 {
        let prep_req = client.ns_req(PrepareSnapshotRequest {
            snapshotter: snapshotter.clone(),
            key: snapshot_key.clone(),
            parent: snapshot_name.to_string(),
            labels: HashMap::new(),
        });
        match snap.prepare(prep_req).await {
            Ok(_) => {
                last_err = None;
                break;
            }
            Err(e) => {
                tracing::warn!(
                    container = %container_id,
                    attempt = attempt + 1,
                    error = %e,
                    "reset_to_snapshot: prepare failed; retrying"
                );
                last_err = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt as u64 + 1))).await;
            }
        }
    }
    if let Some(e) = last_err {
        tracing::error!(
            container = %container_id,
            error = %e,
            "reset_to_snapshot: prepare failed after retries; container's snapshot_key now points at nothing. \
             Recover via delete_container."
        );
        return Err(e.into_crate_error("reset_prepare_active"));
    }

    if opts.restart {
        client.start_container(container_id).await?;
    }

    tracing::info!(container = %container_id, snapshot = %snapshot_name, "reset_to_snapshot: done");
    Ok(())
}

pub(crate) async fn list_snapshots(client: &Client) -> Result<Vec<SnapshotInfo>> {
    let snapshotter = crate::consts::DEFAULT_SNAPSHOTTER.to_string();
    let mut snap = client.snapshots();

    let req = client.ns_req(ListSnapshotsRequest {
        snapshotter,
        filters: vec![format!("labels.\"{}\"==1", MANAGED_LABEL)],
    });
    let mut stream = snap
        .list(req)
        .await
        .map_err(|e| e.into_crate_error("snapshot_list"))?
        .into_inner();
    let mut out = Vec::new();
    while let Some(msg) = stream
        .message()
        .await
        .map_err(|e| e.into_crate_error("snapshot_list_stream"))?
    {
        for info in msg.info {
            out.push(SnapshotInfo {
                name: info.name,
                image: info.labels.get(IMAGE_LABEL).cloned(),
                description: info.labels.get(DESCRIPTION_LABEL).cloned(),
                created_at: info.created_at.and_then(timestamp_to_systemtime),
                parent: info.parent,
            });
        }
    }
    Ok(out)
}

pub(crate) async fn delete_snapshot(client: &Client, name: &str) -> Result<()> {
    tracing::info!(snapshot = %name, "delete_snapshot");
    let snapshotter = crate::consts::DEFAULT_SNAPSHOTTER.to_string();
    let mut snap = client.snapshots();
    // Refuse to remove snapshots we didn't create (image layers etc).
    stat_managed_snapshot(client, &mut snap, &snapshotter, name).await?;

    let req = client.ns_req(RemoveSnapshotRequest {
        snapshotter,
        key: name.to_string(),
    });
    snap.remove(req).await.map_err(|e| {
        if e.code() == Code::NotFound {
            Error::SnapshotNotFound(name.to_string())
        } else {
            e.into_crate_error("snapshot_delete")
        }
    })?;
    Ok(())
}

/// Stats a snapshot, verifies it carries [`MANAGED_LABEL`] + [`IMAGE_LABEL`].
/// Returns the image label value (the only field callers consume).
async fn stat_managed_snapshot(
    client: &Client,
    snap: &mut SnapshotsClient<containerd_client::tonic::transport::Channel>,
    snapshotter: &str,
    name: &str,
) -> Result<String> {
    let stat_req = client.ns_req(StatSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: name.to_string(),
    });
    let stat = snap.stat(stat_req).await.map_err(|e| {
        if e.code() == Code::NotFound {
            Error::SnapshotNotFound(name.to_string())
        } else {
            e.into_crate_error("snapshot_stat")
        }
    })?;
    let info = stat
        .into_inner()
        .info
        .ok_or_else(|| Error::SnapshotNotFound(name.to_string()))?;
    if info.labels.get(MANAGED_LABEL).map(String::as_str) != Some("1") {
        return Err(Error::SnapshotNotManaged {
            name: name.to_string(),
        });
    }
    info.labels.get(IMAGE_LABEL).cloned().ok_or_else(|| {
        Error::InvalidArgument(format!(
            "managed snapshot '{name}' missing required {IMAGE_LABEL} label"
        ))
    })
}

fn timestamp_to_systemtime(ts: prost_types::Timestamp) -> Option<SystemTime> {
    let secs = u64::try_from(ts.seconds).ok()?;
    let nanos = u32::try_from(ts.nanos).ok()?;
    Some(UNIX_EPOCH + Duration::new(secs, nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Snapshot label keys are persisted on every managed snapshot. Renames
    // would orphan list_snapshots filters + restore.
    #[test]
    fn snapshot_label_keys_pinned() {
        assert_eq!(IMAGE_LABEL, "containerd-manager.snapshot.image");
        assert_eq!(MANAGED_LABEL, "containerd-manager.snapshot.managed");
        assert_eq!(DESCRIPTION_LABEL, "containerd-manager.snapshot.description");
    }

    #[test]
    fn timestamp_zero_is_epoch() {
        let t = prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        };
        assert_eq!(timestamp_to_systemtime(t), Some(UNIX_EPOCH));
    }

    #[test]
    fn timestamp_negative_seconds_returns_none() {
        let t = prost_types::Timestamp {
            seconds: -1,
            nanos: 0,
        };
        assert_eq!(timestamp_to_systemtime(t), None);
    }

    #[test]
    fn timestamp_negative_nanos_returns_none() {
        let t = prost_types::Timestamp {
            seconds: 1,
            nanos: -5,
        };
        assert_eq!(timestamp_to_systemtime(t), None);
    }

    #[test]
    fn label_constants_are_under_our_namespace() {
        for k in [IMAGE_LABEL, MANAGED_LABEL, DESCRIPTION_LABEL] {
            assert!(
                k.starts_with("containerd-manager."),
                "label {k} must be under our namespace"
            );
        }
    }
}
