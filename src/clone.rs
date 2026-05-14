//! CoW clone of a container via the containerd snapshotter API.
//!
//! 1. Stop src (snapshotter rejects commit on a mounted active key).
//! 2. First clone: commit src's writable layer to `<src>-clone-base`, then
//!    re-prepare src's active rooted at that base. Later clones reuse it.
//! 3. Prepare dst's active rooted at the base. Sub-second CoW.
//! 4. Build the dst container record via `create_container` with
//!    `from_existing_snapshot`, which re-runs bridge IP + port allocation.

use std::collections::HashMap;

use containerd_client::services::v1::snapshots::{
    CommitSnapshotRequest, PrepareSnapshotRequest, RemoveSnapshotRequest, StatSnapshotRequest,
};
use containerd_client::tonic::Code;

use crate::client::Client;
use crate::consts::SNAPSHOT_OP_STOP_TIMEOUT;
use crate::container::{CreateContainerOpts, NetworkMode};
use crate::error::{Error, Result};
use crate::snapshot_util::{fetch_container_record, is_managed_label, network_from_labels};
use crate::types::{ContainerId, TaskStatus};
use crate::util::StatusExt;

const CLONE_BASE_SUFFIX: &str = "-clone-base";

/// Options for [`Client::clone_container_with_opts`].
///
/// The default workflow is "prime once, clone N times, run clones in parallel".
/// Set `restart_src(true)` to keep the source running alongside its clones.
#[derive(Debug, Clone, typed_builder::TypedBuilder)]
#[builder(doc, mutators(
    /// Add an env var on top of src's env. May be called repeatedly.
    pub fn env(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.extra_env.insert(key.into(), value.into());
    }
    /// Add a label override. May be called repeatedly. Managed labels
    /// (network mode, bridge IP, port bindings) cannot be overridden and
    /// will produce an error at clone time.
    pub fn label(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.label_overrides.insert(key.into(), value.into());
    }
))]
pub struct CloneContainerOpts {
    /// Restart the source if it was running before the clone.
    #[builder(default = false)]
    pub restart_src: bool,
    /// If true (the default), copy src's container ports onto the clone (with
    /// freshly allocated host ports). Set to false when you want to override
    /// the clone's exposed ports via `port_bindings`.
    #[builder(default = true)]
    pub inherit_src_ports: bool,
    /// Explicit container ports to expose on the clone. Only consulted when
    /// `inherit_src_ports` is false; the clone always gets freshly allocated
    /// host ports.
    #[builder(default)]
    pub port_bindings: Vec<u16>,
    /// Override network mode. `None` inherits from src.
    #[builder(default, setter(strip_option))]
    pub network: Option<NetworkMode>,
    /// Env vars to set on the clone *on top of* src's env.
    #[builder(via_mutators)]
    pub extra_env: HashMap<String, String>,
    /// Labels to overwrite/add on the clone after src's user labels are
    /// copied. Managed labels (network mode, bridge IP, port bindings)
    /// cannot be overridden.
    #[builder(via_mutators)]
    pub label_overrides: HashMap<String, String>,
}

impl Default for CloneContainerOpts {
    fn default() -> Self {
        // Inherit-src-ports defaults to true (matching the builder), so a
        // plain `default()` produces the same behaviour as `.builder().build()`.
        Self {
            restart_src: false,
            inherit_src_ports: true,
            port_bindings: Vec::new(),
            network: None,
            extra_env: HashMap::new(),
            label_overrides: HashMap::new(),
        }
    }
}


/// Clones a container.
///
/// # Idempotency
/// - Cloning a src that already has a `<src>-clone-base` reuses it.
/// - Concurrent clones of the same src are tolerated; the race winner's
///   commit succeeds and losers reuse the resulting base.
/// - The destination must not already exist.
///
/// # Errors
/// - [`Error::ContainerNotFound`] if `src` doesn't exist.
/// - [`Error::ContainerAlreadyExists`] if `dst` already exists.
/// - [`Error::Containerd`] for unexpected snapshotter / containers RPC errors.
pub(crate) async fn clone_container(
    client: &Client,
    src: &ContainerId,
    dst: &ContainerId,
    opts: CloneContainerOpts,
) -> Result<()> {
    tracing::info!(src = %src, dst = %dst, restart_src = opts.restart_src, "clone_container: begin");
    let src_info = crate::inspect::inspect_container(client, src.as_str()).await?;
    let was_running = matches!(
        src_info.task.as_ref().map(|t| t.status),
        Some(TaskStatus::Running)
    );

    if was_running {
        crate::task::stop_container(client, src.as_str(), SNAPSHOT_OP_STOP_TIMEOUT).await?;
        // stop_container leaves the task record; drop it so the eventual restart
        // can recreate. Retry covers transient poststop-hook errors.
        crate::task::retry_delete_task(client, src.as_str(), SNAPSHOT_OP_STOP_TIMEOUT).await?;
    }

    // Restart src on any error path: clone_inner may fail after we already
    // stopped + deleted the src task.
    let result = clone_inner(client, src, dst, &src_info, &opts).await;

    if was_running && opts.restart_src {
        if let Err(e) = client.start_container(src).await {
            tracing::warn!(
                src = %src,
                error = %e,
                "clone_container: src restart failed (clone outcome: {})",
                if result.is_ok() { "succeeded" } else { "failed" },
            );
        }
    }

    result?;
    tracing::info!(src = %src, dst = %dst, "clone_container: done");
    Ok(())
}

async fn clone_inner(
    client: &Client,
    src: &ContainerId,
    dst: &ContainerId,
    src_info: &crate::types::ContainerInfo,
    opts: &CloneContainerOpts,
) -> Result<()> {
    let record = fetch_container_record(client, src.as_str()).await?;
    let snapshotter = record.snapshotter().to_string();
    let src_snapshot_key = record.snapshot_key().to_string();
    let base_key = format!("{}{}", src.as_str(), CLONE_BASE_SUFFIX);
    let dst_snapshot_key = crate::container::snapshot_key_for(dst.as_str());

    tracing::debug!(base = %base_key, src_active = %src_snapshot_key, "ensuring clone base");
    ensure_clone_base(client, &snapshotter, &src_snapshot_key, &base_key).await?;

    prepare_clone_snapshot(
        client,
        &snapshotter,
        &dst_snapshot_key,
        &base_key,
        dst.as_str(),
    )
    .await?;

    // Clean up the orphan snapshot if container record creation fails, else
    // retries hit AlreadyExists on the snapshotter.
    if let Err(e) = create_clone_record(client, dst, src_info, &dst_snapshot_key, opts).await {
        tracing::warn!(dst = %dst, error = %e, "clone_container: dst create failed, removing orphan snapshot");
        let _ = remove_snapshot(client, &snapshotter, &dst_snapshot_key).await;
        return Err(e);
    }
    Ok(())
}

/// Idempotent and race-tolerant: concurrent callers may both miss the Stat;
/// the Commit loser sees AlreadyExists and treats it as success.
async fn ensure_clone_base(
    client: &Client,
    snapshotter: &str,
    src_active_key: &str,
    base_key: &str,
) -> Result<()> {
    let mut snap = client.snapshots();

    let stat_req = client.ns_req(StatSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: base_key.to_string(),
    });
    match snap.stat(stat_req).await {
        Ok(_) => return Ok(()),
        Err(e) if e.code() == Code::NotFound => {}
        Err(e) => return Err(e.into_crate_error("clone_stat_base")),
    }

    let commit_req = client.ns_req(CommitSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        name: base_key.to_string(),
        key: src_active_key.to_string(),
        labels: HashMap::new(),
    });
    match snap.commit(commit_req).await {
        Ok(_) => tracing::debug!(base = %base_key, "committed clone-base"),
        // Race winner already did the commit + reprep; nothing more to do.
        Err(e) if e.code() == Code::AlreadyExists => {
            tracing::debug!(base = %base_key, "clone-base already present (race winner did the commit)");
            return Ok(());
        }
        Err(e) => return Err(e.into_crate_error("clone_commit_src")),
    }

    let reprep_req = client.ns_req(PrepareSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: src_active_key.to_string(),
        parent: base_key.to_string(),
        labels: HashMap::new(),
    });
    match snap.prepare(reprep_req).await {
        Ok(_) => Ok(()),
        Err(e) if e.code() == Code::AlreadyExists => Ok(()),
        Err(e) => Err(e.into_crate_error("clone_reprepare_src")),
    }
}

async fn prepare_clone_snapshot(
    client: &Client,
    snapshotter: &str,
    dst_key: &str,
    parent_key: &str,
    dst_id: &str,
) -> Result<()> {
    let prep_req = client.ns_req(PrepareSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: dst_key.to_string(),
        parent: parent_key.to_string(),
        labels: HashMap::new(),
    });
    // AlreadyExists here means either dst was previously created or an orphan
    // snapshot from a failed clone remains. Both surface as "dst exists".
    client.snapshots().prepare(prep_req).await.map_err(|e| {
        if e.code() == Code::AlreadyExists {
            Error::ContainerAlreadyExists(dst_id.to_string())
        } else {
            e.into_crate_error("clone_prepare_dst")
        }
    })?;
    Ok(())
}

async fn remove_snapshot(client: &Client, snapshotter: &str, key: &str) -> Result<()> {
    let req = client.ns_req(RemoveSnapshotRequest {
        snapshotter: snapshotter.to_string(),
        key: key.to_string(),
    });
    client
        .snapshots()
        .remove(req)
        .await
        .map_err(|e| e.into_crate_error("clone_remove_dst_snapshot"))?;
    Ok(())
}

async fn create_clone_record(
    client: &Client,
    dst: &ContainerId,
    src_info: &crate::types::ContainerInfo,
    dst_snapshot_key: &str,
    opts: &CloneContainerOpts,
) -> Result<()> {
    let network_mode = opts
        .network
        .clone()
        .unwrap_or_else(|| network_from_labels(&src_info.labels));

    let container_ports: Vec<u16> = if opts.inherit_src_ports {
        src_info
            .port_forwards
            .iter()
            .map(|&(_host, ctr)| ctr)
            .collect()
    } else {
        opts.port_bindings.clone()
    };

    // Direct field mutation: iterator accumulation doesn't fit the
    // builder's consume-self chain.
    let mut create_opts = CreateContainerOpts::default();
    create_opts.network.mode = network_mode;
    create_opts.from_existing_snapshot = Some(dst_snapshot_key.to_string());
    for (k, v) in &src_info.env {
        create_opts.env.insert(k.clone(), v.clone());
    }
    for (k, v) in &opts.extra_env {
        create_opts.env.insert(k.clone(), v.clone());
    }
    for (k, v) in &src_info.labels {
        if !is_managed_label(k) {
            create_opts.labels.insert(k.clone(), v.clone());
        }
    }
    // Overrides applied last; managed labels rejected.
    for (k, v) in &opts.label_overrides {
        if is_managed_label(k) {
            return Err(Error::InvalidArgument(format!(
                "label '{k}' is managed by containerd-manager and cannot be overridden"
            )));
        }
        create_opts.labels.insert(k.clone(), v.clone());
    }
    for cp in container_ports {
        create_opts.port_bindings.push((0, cp));
    }

    crate::container::create_container(client, dst.as_str(), &src_info.image, create_opts).await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Manual `Default` impl required because `inherit_src_ports` defaults
    /// to `true`, not bool::default()'s `false`. Worth a guard so a future
    /// derive-Default doesn't silently flip clone behavior.
    #[test]
    fn default_inherits_src_ports() {
        assert!(CloneContainerOpts::default().inherit_src_ports);
    }

    #[test]
    fn clone_base_suffix_stable() {
        // Snapshot keys are user-visible via ctr snapshots ls; locking in the
        // current format so changes are intentional.
        let base = format!("primed{CLONE_BASE_SUFFIX}");
        assert_eq!(base, "primed-clone-base");
    }
}
