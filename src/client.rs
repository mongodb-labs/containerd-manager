//! Client: holds connection state; delegates to domain modules.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use containerd_client::services::v1::containers_client::ContainersClient;
use containerd_client::services::v1::content_client::ContentClient;
use containerd_client::services::v1::images_client::ImagesClient;
use containerd_client::services::v1::snapshots::snapshots_client::SnapshotsClient;
use containerd_client::services::v1::tasks_client::TasksClient;
use containerd_client::services::v1::transfer_client::TransferClient;
use containerd_client::tonic::transport::Channel;
use containerd_client::tonic::Request;

use crate::clone::CloneContainerOpts;
use crate::consts::NETWORK_MODE_LABEL;
use crate::container::{CreateContainerOpts, ImageMetadata};
use crate::snapshot::{
    ResetToSnapshotOpts, RestoreContainerOpts, SnapshotContainerOpts, SnapshotInfo,
};
use crate::error::{Error, Result};
use crate::exec::ExecOutput;
use crate::logs::{LogEntry, LogFollower};
use crate::port_forward::{PortForwardHandle, PortForwardOpts};
use crate::readiness::{HealthCheck, HealthStatus, ReadinessStrategy};
use crate::task::TaskId;
use crate::types::{ContainerId, ContainerInfo};
use crate::util::StatusExt;

/// Client for containerd operations. Constructed by [`crate::connect()`],
/// defaults to the `"default"` namespace.
pub struct Client {
    /// Underlying gRPC connection. Accessed only through this module's
    /// helpers (`channel()`, `snapshots()`, `containers_client()`, etc.) so
    /// the rest of the crate doesn't need to know about
    /// `containerd_client::Client`.
    containerd_client: containerd_client::Client,
    /// Active managed port-forward handles. **Invariant**: the `std::sync::Mutex`
    /// here is never held across an `.await` point. Each access takes the lock,
    /// mutates the Vec, and drops the guard before any subsequent async work.
    /// Holding it across `.await` would risk deadlocking with the runtime.
    pub(crate) managed_handles: Arc<Mutex<Vec<PortForwardHandle>>>,
    /// Per-container log tailers writing events.log + rotating. Keyed by
    /// `<namespace>/<container_id>`. Stopped on `delete_container`.
    pub(crate) managed_tailers: Arc<DashMap<String, crate::log_tailer::LogTailer>>,
    pub(crate) namespace: String,
    /// Cached image metadata keyed by manifest digest. Shared across clones
    /// (incl. `with_namespace`) - same gRPC connection, same content store.
    pub(crate) image_metadata_cache: Arc<DashMap<String, Arc<ImageMetadata>>>,
    /// Task start times keyed by `<namespace>/<container_id>`. Recorded on
    /// `start_container`, evicted on `remove_container`. Used by `probe_health`
    /// to honor the image's HEALTHCHECK `start_period` (a failing check inside
    /// the grace window reports `Starting`, not `Unhealthy`).
    pub(crate) task_start_times: Arc<DashMap<String, Instant>>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            containerd_client: containerd_client::Client::from(self.channel()),
            managed_handles: self.managed_handles.clone(),
            managed_tailers: self.managed_tailers.clone(),
            namespace: self.namespace.clone(),
            image_metadata_cache: self.image_metadata_cache.clone(),
            task_start_times: self.task_start_times.clone(),
        }
    }
}

impl Client {
    /// Internal constructor used by `connect` and the dummy-client test helper.
    /// Keeps the `containerd_client` field private to this module.
    pub(crate) fn from_parts(containerd_client: containerd_client::Client) -> Self {
        Self {
            containerd_client,
            managed_handles: Arc::new(Mutex::new(Vec::new())),
            managed_tailers: Arc::new(DashMap::new()),
            namespace: "default".to_string(),
            image_metadata_cache: Arc::new(DashMap::new()),
            task_start_times: Arc::new(DashMap::new()),
        }
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns a new Client pointing at a different namespace. The underlying
    /// connection and port-forward state are shared with the original.
    ///
    /// Panics on an invalid namespace (one that can't be encoded as a gRPC
    /// metadata header value). For user-supplied input prefer
    /// [`try_with_namespace`](Self::try_with_namespace), which returns an
    /// error instead of panicking.
    pub fn with_namespace(&self, namespace: impl Into<String>) -> Self {
        self.try_with_namespace(namespace).expect(
            "namespace not valid as gRPC metadata; \
             use try_with_namespace for user-supplied input",
        )
    }

    /// Fallible variant of [`with_namespace`](Self::with_namespace). Returns
    /// `Error::InvalidArgument` when the namespace can't be encoded as a
    /// gRPC metadata header value (must be ASCII visible chars).
    pub fn try_with_namespace(&self, namespace: impl Into<String>) -> Result<Self> {
        let namespace = namespace.into();
        if namespace
            .parse::<containerd_client::tonic::metadata::MetadataValue<_>>()
            .is_err()
        {
            return Err(Error::InvalidArgument(format!(
                "namespace {:?} is not valid as a gRPC metadata header value \
                 (ASCII visible chars only)",
                namespace
            )));
        }
        let mut cloned = self.clone();
        cloned.namespace = namespace;
        Ok(cloned)
    }

    /// Wraps a gRPC request body in a `tonic::Request` and attaches the
    /// containerd-namespace metadata header. The namespace is validated by
    /// `with_namespace` / `connect`, so the parse here always succeeds.
    pub(crate) fn ns_req<T>(&self, inner: T) -> Request<T> {
        let mut req = Request::new(inner);
        let val = self.namespace.parse().expect(
            "namespace invariant violated: not parseable as gRPC metadata. \
             Construct Client via connect() or with_namespace() to validate up front.",
        );
        req.metadata_mut().insert("containerd-namespace", val);
        req
    }

    pub(crate) fn channel(&self) -> Channel {
        self.containerd_client.channel()
    }

    pub(crate) fn transfer(&self) -> TransferClient<Channel> {
        TransferClient::new(self.channel())
    }

    pub(crate) fn snapshots(&self) -> SnapshotsClient<Channel> {
        SnapshotsClient::new(self.channel())
    }

    pub(crate) fn containers_client(&self) -> ContainersClient<Channel> {
        ContainersClient::new(self.channel())
    }

    pub(crate) fn tasks(&self) -> TasksClient<Channel> {
        TasksClient::new(self.channel())
    }

    pub(crate) fn images_client(&self) -> ImagesClient<Channel> {
        ImagesClient::new(self.channel())
    }

    pub(crate) fn content_client(&self) -> ContentClient<Channel> {
        ContentClient::new(self.channel())
    }

    pub async fn server_version(&self) -> Result<String> {
        let mut version_client = self.containerd_client.version();
        let resp = version_client
            .version(())
            .await
            .map_err(|e| e.into_crate_error("server_version"))?;
        Ok(resp.into_inner().version)
    }

    #[tracing::instrument(skip(self), fields(namespace = %self.namespace))]
    pub async fn pull_image(&self, image: &str) -> Result<()> {
        crate::image::pull_image(self, image).await
    }

    // Image-metadata cache: lock-free via DashMap. Cache key is the manifest
    // digest, so values are content-addressed and safe to keep across pulls.

    /// Drops every cached image-metadata entry. Mostly useful for tests.
    pub fn clear_image_cache(&self) {
        self.image_metadata_cache.clear();
    }

    pub fn image_cache_len(&self) -> usize {
        self.image_metadata_cache.len()
    }

    pub(crate) fn lookup_image_metadata(
        &self,
        manifest_digest: &str,
    ) -> Option<Arc<ImageMetadata>> {
        self.image_metadata_cache
            .get(manifest_digest)
            .map(|r| r.value().clone())
    }

    pub(crate) fn store_image_metadata(
        &self,
        manifest_digest: String,
        metadata: Arc<ImageMetadata>,
    ) {
        self.image_metadata_cache.insert(manifest_digest, metadata);
    }

    /// Returns the image's `HEALTHCHECK` definition (command, interval,
    /// timeout, retries, start-period), or `None` if the image has none.
    pub async fn image_healthcheck(&self, image: &str) -> Result<Option<HealthCheck>> {
        let raw = crate::container::get_raw_image_config(self, image).await?;
        Ok(crate::readiness::parse_healthcheck(&raw))
    }

    #[tracing::instrument(skip(self, opts), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn create_container(
        &self,
        container_id: &ContainerId,
        image: &str,
        opts: CreateContainerOpts,
    ) -> Result<()> {
        crate::container::create_container(self, container_id.as_str(), image, opts).await
    }

    /// Starts the task and, for non-host-network containers, auto-starts a
    /// port-forward proxy for every declared port binding. Host-network
    /// containers skip the proxy because the kernel already exposes their
    /// ports on the host loopback.
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn start_container(&self, container_id: &ContainerId) -> Result<TaskId> {
        let task_id = crate::task::start_container(self, container_id.as_str()).await?;

        // Background tailer drains stdout/stderr into events.log so
        // `container_logs` works with timestamps + history filters.
        self.start_managed_tailer(container_id.as_str()).await?;

        let info = crate::inspect::inspect_container(self, container_id.as_str()).await?;
        if is_bridge_mode(&info) {
            for (host_port, container_port) in info.port_forwards {
                self.start_managed_port_forward(
                    container_id,
                    host_port,
                    container_port,
                    PortForwardOpts::default(),
                )?;
            }
        }

        Ok(task_id)
    }

    /// Idempotent: succeeds if the container doesn't exist.
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn delete_container(
        &self,
        container_id: &ContainerId,
        timeout: Duration,
    ) -> Result<()> {
        let result = crate::task::delete_container(self, container_id.as_str(), timeout).await;
        self.stop_managed_forwards(container_id.as_str()).await;
        self.stop_managed_tailer(container_id.as_str()).await;
        result
    }

    /// SIGTERM, wait, then SIGKILL. Leaves the container record intact.
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn stop_container(
        &self,
        container_id: &ContainerId,
        timeout: Duration,
    ) -> Result<()> {
        crate::task::stop_container(self, container_id.as_str(), timeout).await
    }

    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn inspect_container(&self, container_id: &ContainerId) -> Result<ContainerInfo> {
        crate::inspect::inspect_container(self, container_id.as_str()).await
    }

    /// Each filter is `"key=value"`; all must match (AND).
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace))]
    pub async fn list_containers(&self, label_filters: &[&str]) -> Result<Vec<ContainerInfo>> {
        crate::list::list_containers(self, label_filters).await
    }

    /// Returns a handle that stops forwarding when dropped.
    pub fn start_port_forward(
        &self,
        container_id: &ContainerId,
        host_port: u16,
        container_port: u16,
        opts: PortForwardOpts,
    ) -> Result<PortForwardHandle> {
        crate::port_forward::start_port_forward(
            container_id.as_str(),
            host_port,
            container_port,
            opts,
        )
    }

    /// Like [`start_port_forward`](Self::start_port_forward), but stores the
    /// handle inside the Client so forwarding stays active as long as the
    /// Client (or any clone) lives.
    pub fn start_managed_port_forward(
        &self,
        container_id: &ContainerId,
        host_port: u16,
        container_port: u16,
        opts: PortForwardOpts,
    ) -> Result<()> {
        let handle = self.start_port_forward(container_id, host_port, container_port, opts)?;
        self.managed_handles
            .lock()
            .map_err(|_| Error::StatePoisoned("managed_handles"))?
            .push(handle);
        Ok(())
    }

    pub(crate) async fn stop_managed_forwards(&self, container_id: &str) {
        let to_stop: Vec<PortForwardHandle> = {
            let mut handles = match self.managed_handles.lock() {
                Ok(h) => h,
                Err(poisoned) => {
                    // Poisoned (panic mid-update) is exactly when skipping
                    // would leak forwards. Recover the inner Vec and proceed.
                    tracing::error!(
                        container_id,
                        "managed_handles mutex poisoned; attempting forward cleanup anyway"
                    );
                    poisoned.into_inner()
                }
            };
            let (matching, remaining) = std::mem::take(&mut *handles)
                .into_iter()
                .partition(|h| h.container_id() == container_id);
            *handles = remaining;
            matching
        };
        for handle in to_stop {
            handle.stop().await;
        }
    }

    /// Builds a `<namespace>/<container_id>` key for per-container client-side
    /// state (managed tailers, task start times). The namespace prefix
    /// prevents the same container id across namespaces from clobbering.
    fn ns_key(&self, container_id: &str) -> String {
        format!("{}/{container_id}", self.namespace)
    }

    pub(crate) fn record_task_start(&self, container_id: &str) {
        self.task_start_times
            .insert(self.ns_key(container_id), Instant::now());
    }

    pub(crate) fn forget_task_start(&self, container_id: &str) {
        self.task_start_times.remove(&self.ns_key(container_id));
    }

    pub(crate) fn task_uptime(&self, container_id: &str) -> Option<Duration> {
        self.task_start_times
            .get(&self.ns_key(container_id))
            .map(|entry| entry.elapsed())
    }

    pub(crate) async fn start_managed_tailer(&self, container_id: &str) -> Result<()> {
        let tailer = crate::log_tailer::LogTailer::start(container_id)?;
        let key = self.ns_key(container_id);
        // Restart-without-delete: graceful stop the prior tailer so pending
        // writes drain (Drop would abort mid-pass).
        let old = self.managed_tailers.insert(key, tailer);
        if let Some(old) = old {
            old.stop().await;
        }
        Ok(())
    }

    pub(crate) async fn stop_managed_tailer(&self, container_id: &str) {
        let key = self.ns_key(container_id);
        // remove returns (k, v); destructure to take ownership of the tailer.
        if let Some((_, tailer)) = self.managed_tailers.remove(&key) {
            tailer.stop().await;
        }
    }

    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn probe_health(&self, container_id: &ContainerId) -> Result<HealthStatus> {
        crate::readiness::probe_health(self, container_id.as_str()).await
    }

    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn exec(&self, container_id: &ContainerId, command: &[&str]) -> Result<ExecOutput> {
        crate::exec::exec(self, container_id.as_str(), command).await
    }

    pub fn container_logs(&self, container_id: &ContainerId) -> Result<Vec<LogEntry>> {
        crate::logs::container_logs(container_id.as_str())
    }

    /// Reads accumulated events with filters (`tail`, `since`, `until`).
    /// Events are timestamped at observation time by the background tailer.
    pub fn container_logs_filtered(
        &self,
        container_id: &ContainerId,
        filter: &crate::logs::LogsFilter,
    ) -> Result<Vec<LogEntry>> {
        crate::logs::container_logs_filtered(container_id.as_str(), filter)
    }

    /// Streams stdout + stderr live, reading the tailer-managed events.log
    /// so timestamps + ordering match what `container_logs_filtered` returns.
    /// Caller polls [`LogFollower::recv`] until `None`. Drop the follower to stop.
    pub fn container_logs_stream(&self, container_id: &ContainerId) -> Result<LogFollower> {
        let events_path = crate::logs::events_path_for(container_id.as_str())?;
        Ok(LogFollower::start(events_path))
    }

    /// Like [`container_logs_stream`](Self::container_logs_stream) but ends
    /// the stream once the task exits (or the container disappears). A
    /// background watcher polls task state every 250ms; on `Stopped` the
    /// follower does one final drain and `recv` returns `None`.
    ///
    /// Dropping the returned `LogFollower` also stops the watcher: the
    /// follower owns the stop-signal receiver, so dropping it closes the
    /// oneshot and the watcher detects this via `stop_tx.closed()` and
    /// exits without further polling.
    pub fn container_logs_stream_until_exit(
        &self,
        container_id: &ContainerId,
    ) -> Result<LogFollower> {
        let events_path = crate::logs::events_path_for(container_id.as_str())?;
        let (mut stop_tx, stop_rx) = tokio::sync::oneshot::channel();

        let watcher_client = self.clone();
        let watcher_id = container_id.as_str().to_string();
        tokio::spawn(async move {
            use crate::types::TaskStatus;
            const BASE_INTERVAL: Duration = Duration::from_millis(250);
            const MAX_INTERVAL: Duration = Duration::from_secs(5);
            const MAX_CONSECUTIVE_ERRORS: u32 = 60; // ~15s of failures at base rate
            let mut interval = BASE_INTERVAL;
            let mut consecutive_errors: u32 = 0;
            loop {
                tokio::select! {
                    biased;
                    // Follower dropped before the task exited: nothing left
                    // to signal, just stop polling.
                    _ = stop_tx.closed() => return,
                    _ = tokio::time::sleep(interval) => {}
                }
                let done =
                    match crate::inspect::inspect_container(&watcher_client, &watcher_id).await {
                        Ok(info) => {
                            consecutive_errors = 0;
                            interval = BASE_INTERVAL;
                            matches!(
                                info.task.as_ref().map(|t| t.status),
                                Some(TaskStatus::Stopped)
                            )
                        }
                        Err(Error::ContainerNotFound(_)) => true,
                        Err(e) => {
                            consecutive_errors += 1;
                            // Back off on persistent failures.
                            interval = (interval * 2).min(MAX_INTERVAL);
                            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                                tracing::warn!(
                                    container_id = %watcher_id,
                                    consecutive_errors,
                                    error = %e,
                                    "until_exit watcher: too many inspect failures; giving up"
                                );
                                let _ = stop_tx.send(());
                                return;
                            }
                            false
                        }
                    };
                if done {
                    let _ = stop_tx.send(());
                    return;
                }
            }
        });

        Ok(LogFollower::start_with_stop(events_path, stop_rx))
    }

    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn pause_container(&self, container_id: &ContainerId) -> Result<()> {
        crate::task::pause_container(self, container_id.as_str()).await
    }

    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id))]
    pub async fn unpause_container(&self, container_id: &ContainerId) -> Result<()> {
        crate::task::unpause_container(self, container_id.as_str()).await
    }

    #[tracing::instrument(skip(self, strategy), fields(namespace = %self.namespace, id = %container_id, timeout_ms = timeout.as_millis() as u64))]
    pub async fn wait_ready(
        &self,
        container_id: &ContainerId,
        timeout: Duration,
        strategy: ReadinessStrategy,
    ) -> Result<()> {
        crate::readiness::wait_ready(self, container_id.as_str(), timeout, strategy).await
    }

    /// Waits for the task to exit, returns its exit code. Errors if the
    /// container or task is missing, or the deadline elapses.
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id, timeout_ms = timeout.as_millis() as u64))]
    pub async fn wait_for_exit(
        &self,
        container_id: &ContainerId,
        timeout: Duration,
    ) -> Result<i32> {
        crate::task::wait_for_exit(self, container_id.as_str(), timeout).await
    }

    /// CoW-clones a container including its rootfs data. The source is briefly
    /// stopped to commit its writable layer, then restarted (override via
    /// [`CloneContainerOpts::restart_src`]). The clone is created in the
    /// `Created` state with a fresh bridge IP and fresh host ports, so it can
    /// run in parallel with the source. Subsequent clones of the same source
    /// reuse the frozen base snapshot — only the first clone pays the
    /// commit cost.
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, src = %src, dst = %dst))]
    pub async fn clone_container(
        &self,
        src: &ContainerId,
        dst: &ContainerId,
    ) -> Result<()> {
        crate::clone::clone_container(self, src, dst, CloneContainerOpts::default()).await
    }

    /// Like [`clone_container`](Self::clone_container) with a custom
    /// [`CloneContainerOpts`].
    pub async fn clone_container_with_opts(
        &self,
        src: &ContainerId,
        dst: &ContainerId,
        opts: CloneContainerOpts,
    ) -> Result<()> {
        crate::clone::clone_container(self, src, dst, opts).await
    }

    /// Freezes a container's rootfs into a named base snapshot. The source is
    /// briefly stopped to commit its writable layer; pass
    /// [`SnapshotContainerOpts::restart_src`] = `true` to restart it
    /// afterwards. Image ref + description are persisted as snapshot labels
    /// so [`restore_container`](Self::restore_container) is self-contained.
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, src = %src, snapshot = %name))]
    pub async fn snapshot_container(
        &self,
        src: &ContainerId,
        name: &str,
    ) -> Result<()> {
        crate::snapshot::snapshot_container(self, src, name, SnapshotContainerOpts::default())
            .await
    }

    /// Like [`snapshot_container`](Self::snapshot_container) with a custom
    /// [`SnapshotContainerOpts`].
    pub async fn snapshot_container_with_opts(
        &self,
        src: &ContainerId,
        name: &str,
        opts: SnapshotContainerOpts,
    ) -> Result<()> {
        crate::snapshot::snapshot_container(self, src, name, opts).await
    }

    /// Instantiates a fresh container from a named snapshot. Image ref and
    /// rootfs come from the snapshot; network, ports, and labels come from
    /// [`RestoreContainerOpts`].
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, snapshot = %snapshot_name, dst = %dst))]
    pub async fn restore_container(
        &self,
        snapshot_name: &str,
        dst: &ContainerId,
    ) -> Result<()> {
        crate::snapshot::restore_container(self, snapshot_name, dst, RestoreContainerOpts::default())
            .await
    }

    /// Like [`restore_container`](Self::restore_container) with a custom
    /// [`RestoreContainerOpts`].
    pub async fn restore_container_with_opts(
        &self,
        snapshot_name: &str,
        dst: &ContainerId,
        opts: RestoreContainerOpts,
    ) -> Result<()> {
        crate::snapshot::restore_container(self, snapshot_name, dst, opts).await
    }

    /// Resets an existing container's rootfs back to a named snapshot.
    /// Network identity, labels, and OCI spec are preserved — only on-disk
    /// data changes. The task is stopped; caller is responsible for
    /// restarting via [`start_container`](Self::start_container).
    ///
    /// Use case: same-container test isolation. Snapshot once, reset between
    /// tests instead of full teardown/setup.
    ///
    /// # Errors
    /// - [`Error::SnapshotNotFound`] / [`Error::SnapshotNotManaged`] for
    ///   missing or third-party snapshots.
    /// - [`Error::InvalidArgument`] if the snapshot's image doesn't match
    ///   the container's image.
    #[tracing::instrument(skip(self), fields(namespace = %self.namespace, id = %container_id, snapshot = %snapshot_name))]
    pub async fn reset_to_snapshot(
        &self,
        container_id: &ContainerId,
        snapshot_name: &str,
    ) -> Result<()> {
        crate::snapshot::reset_to_snapshot(
            self,
            container_id,
            snapshot_name,
            ResetToSnapshotOpts::default(),
        )
        .await
    }

    /// Like [`reset_to_snapshot`](Self::reset_to_snapshot) with a custom
    /// [`ResetToSnapshotOpts`]. Use `.restart(true)` to start the container
    /// again as part of the call.
    pub async fn reset_to_snapshot_with_opts(
        &self,
        container_id: &ContainerId,
        snapshot_name: &str,
        opts: ResetToSnapshotOpts,
    ) -> Result<()> {
        crate::snapshot::reset_to_snapshot(self, container_id, snapshot_name, opts).await
    }

    /// Lists named snapshots created via [`snapshot_container`](Self::snapshot_container).
    /// Image-layer snapshots and stargz/CRI snapshots are filtered out by the
    /// managed-by label.
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>> {
        crate::snapshot::list_snapshots(self).await
    }

    /// Deletes a named snapshot. Fails if any active snapshots are still
    /// parented at it.
    pub async fn delete_snapshot(&self, name: &str) -> Result<()> {
        crate::snapshot::delete_snapshot(self, name).await
    }
}

/// Only bridge mode has user-space port-forward proxies. Host shares the
/// host loopback; isolated has no external connectivity.
fn is_bridge_mode(info: &ContainerInfo) -> bool {
    info.labels
        .get(NETWORK_MODE_LABEL)
        .is_some_and(|v| v == "bridge")
}

#[cfg(test)]
mod tests {
    use super::*;
    use containerd_client::tonic::transport::Endpoint;
    use tower::service_fn;

    /// A `Client` with a lazy, never-actually-connected gRPC channel. Good
    /// enough for testing the cache helpers since none of them talk to
    /// containerd.
    fn dummy_client() -> Client {
        let channel = Endpoint::try_from("http://[::]")
            .unwrap()
            .connect_with_connector_lazy(service_fn(|_| async {
                Err::<hyper_util::rt::TokioIo<tokio::net::TcpStream>, std::io::Error>(
                    std::io::Error::other("dummy"),
                )
            }));
        Client::from_parts(containerd_client::Client::from(channel))
    }

    fn dummy_metadata() -> Arc<ImageMetadata> {
        use oci_spec::image::{ImageConfiguration, RootFsBuilder};
        let rootfs = RootFsBuilder::default()
            .typ("layers".to_string())
            .diff_ids(vec!["sha256:abc".to_string()])
            .build()
            .unwrap();
        let config = ImageConfiguration::default();
        Arc::new(ImageMetadata {
            chain_id: "sha256:abc".to_string(),
            config: oci_spec::image::ImageConfigurationBuilder::default()
                .rootfs(rootfs)
                .build()
                .unwrap_or(config),
            raw_config: b"{}".to_vec(),
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn image_cache_starts_empty() {
        let c = dummy_client();
        assert_eq!(c.image_cache_len(), 0);
        assert!(c.lookup_image_metadata("sha256:abc").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn image_cache_store_then_lookup() {
        let c = dummy_client();
        c.store_image_metadata("sha256:abc".into(), dummy_metadata());
        assert_eq!(c.image_cache_len(), 1);

        let hit = c.lookup_image_metadata("sha256:abc").unwrap();
        assert_eq!(hit.chain_id, "sha256:abc");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn image_cache_miss_for_other_digest() {
        let c = dummy_client();
        c.store_image_metadata("sha256:abc".into(), dummy_metadata());
        assert!(c.lookup_image_metadata("sha256:def").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn image_cache_clear_drops_all_entries() {
        let c = dummy_client();
        c.store_image_metadata("sha256:abc".into(), dummy_metadata());
        c.store_image_metadata("sha256:def".into(), dummy_metadata());
        assert_eq!(c.image_cache_len(), 2);
        c.clear_image_cache();
        assert_eq!(c.image_cache_len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn image_cache_shared_with_clone_and_namespace() {
        let c = dummy_client();
        let other = c.with_namespace("other");
        c.store_image_metadata("sha256:abc".into(), dummy_metadata());
        // The clone sees the same cache.
        assert_eq!(other.image_cache_len(), 1);
        assert!(other.lookup_image_metadata("sha256:abc").is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn task_uptime_none_until_recorded() {
        let c = dummy_client();
        assert!(c.task_uptime("c1").is_none());
        c.record_task_start("c1");
        assert!(c.task_uptime("c1").is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn task_uptime_evicted_by_forget() {
        let c = dummy_client();
        c.record_task_start("c1");
        c.forget_task_start("c1");
        assert!(c.task_uptime("c1").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn task_uptime_keyed_per_namespace() {
        let c = dummy_client();
        c.record_task_start("c1");
        let other = c.with_namespace("other");
        // Same container id, different namespace - independent entry.
        assert!(other.task_uptime("c1").is_none());
        other.record_task_start("c1");
        assert!(other.task_uptime("c1").is_some());
        // Original namespace still has its entry.
        assert!(c.task_uptime("c1").is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ns_req_attaches_containerd_namespace_metadata() {
        let c = dummy_client().with_namespace("attached-ns");
        let req = c.ns_req(());
        let value = req
            .metadata()
            .get("containerd-namespace")
            .expect("ns_req must attach containerd-namespace metadata");
        assert_eq!(value.to_str().unwrap(), "attached-ns");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn try_with_namespace_rejects_invalid() {
        let c = dummy_client();
        let bad = c.try_with_namespace("contains\nnewline");
        assert!(bad.is_err(), "newline must be rejected for header value");
    }

    #[tokio::test(flavor = "current_thread")]
    #[should_panic(expected = "namespace not valid")]
    async fn with_namespace_panics_on_invalid() {
        let c = dummy_client();
        let _ = c.with_namespace("contains\nnewline");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn with_namespace_shares_image_cache() {
        let c = dummy_client();
        c.store_image_metadata("sha256:abc".into(), dummy_metadata());
        let other = c.with_namespace("other");
        // Image cache content-addressed, namespace-independent.
        assert_eq!(other.image_cache_len(), 1);
        assert!(other.lookup_image_metadata("sha256:abc").is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn with_namespace_shares_task_start_times_arc_but_keyed_per_ns() {
        // Same `Arc<DashMap>`, but the `ns_key` includes namespace so entries
        // don't collide across namespaces.
        let c = dummy_client();
        c.record_task_start("c1");
        let other = c.with_namespace("other");
        assert!(other.task_uptime("c1").is_none());
        assert!(c.task_uptime("c1").is_some());
    }
}
