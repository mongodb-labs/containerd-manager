//! User-space TCP port forwarding. Port bindings declared via
//! `CreateContainerOpts::port_binding` are persisted as container labels so
//! they survive across process restarts.

use std::collections::HashMap;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use crate::consts::PORT_BINDING_LABEL_PREFIX;
use crate::error::{Error, Result};

/// `(host_port, container_port)` for every well-formed binding label.
pub(crate) fn parse_port_binding_labels(labels: &HashMap<String, String>) -> Vec<(u16, u16)> {
    labels
        .iter()
        .filter_map(|(key, value)| {
            let container_port: u16 = key.strip_prefix(PORT_BINDING_LABEL_PREFIX)?.parse().ok()?;
            let host_port: u16 = value.parse().ok()?;
            Some((host_port, container_port))
        })
        .collect()
}

/// Default `target_addr` is `127.0.0.1` (works for host networking). For
/// containers with their own IP, set it to the container address.
#[derive(Debug, Clone, typed_builder::TypedBuilder)]
#[builder(doc)]
pub struct PortForwardOpts {
    #[builder(default = "127.0.0.1".to_string(), setter(into))]
    pub target_addr: String,
}

impl Default for PortForwardOpts {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Stops forwarding when dropped. Use [`stop`](Self::stop) to also await full
/// task exit (guarantees the OS port is released).
pub struct PortForwardHandle {
    container_id: String,
    host_port: u16,
    container_port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl PortForwardHandle {
    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    pub fn host_port(&self) -> u16 {
        self.host_port
    }

    pub fn container_port(&self) -> u16 {
        self.container_port
    }

    /// Signals shutdown and awaits the listener task. Unlike `Drop` (which
    /// returns immediately), this guarantees the OS port is released.
    ///
    /// `stop()` consumes the handle; calling it again is a compile error.
    /// Drop after `stop()` is a no-op because both `shutdown_tx` and
    /// `join_handle` have been taken.
    pub async fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(jh) = self.join_handle.take() {
            let _ = jh.await;
        }
    }
}

impl Drop for PortForwardHandle {
    /// Best-effort shutdown. Aborts the listener task (which transitively
    /// aborts in-flight per-conn proxy tasks via its `JoinSet`). In-flight
    /// `tokio::io::copy_bidirectional` calls are cancelled mid-stream — any
    /// buffered bytes not yet flushed to the peer are lost. Use
    /// [`stop`](Self::stop) for the graceful path that awaits the listener
    /// task and OS port release.
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(jh) = self.join_handle.take() {
            jh.abort();
        }
    }
}

/// `copy_bidirectional` handles half-close correctly so the remaining bytes
/// drain after one side closes its write half.
async fn proxy(mut client: TcpStream, mut target: TcpStream) {
    let _ = tokio::io::copy_bidirectional(&mut client, &mut target).await;
}

pub(crate) fn start_port_forward(
    container_id: &str,
    host_port: u16,
    container_port: u16,
    opts: PortForwardOpts,
) -> Result<PortForwardHandle> {
    let container_id = container_id.to_string();
    let target_addr = opts.target_addr;

    if host_port == 0 {
        return Err(Error::PortForward("host_port cannot be 0".to_string()));
    }
    if container_port == 0 {
        return Err(Error::PortForward("container_port cannot be 0".to_string()));
    }

    // Bind eagerly so callers see the error here, not from a background task.
    let std_listener = std::net::TcpListener::bind(format!("127.0.0.1:{}", host_port))
        .map_err(|e| Error::PortForward(format!("failed to bind port {}: {}", host_port, e)))?;
    std_listener.set_nonblocking(true).map_err(|e| {
        Error::PortForward(format!(
            "failed to set non-blocking on port {}: {}",
            host_port, e
        ))
    })?;
    // Convert to tokio before spawn - surfaces conversion errors as Err
    // instead of swallowing them in the background task.
    let listener = TcpListener::from_std(std_listener)
        .map_err(|e| Error::PortForward(format!("failed to create tokio listener: {}", e)))?;

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let target = format!("{}:{}", target_addr, container_port);

    tracing::debug!(
        container_id = %container_id,
        host_port,
        container_port,
        target = %target,
        "port_forward: listening"
    );

    let join_handle = tokio::spawn(async move {
        // Track in-flight per-conn tasks so shutdown aborts them too. A slow
        // upstream connect would otherwise outlive the forwarder.
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                // Shutdown takes priority: under heavy connection rate the
                // unbiased default could starve the stop signal indefinitely.
                biased;
                _ = &mut shutdown_rx => {
                    tracing::debug!(host_port, "port_forward: shutdown");
                    connections.abort_all();
                    break;
                }
                result = listener.accept() => {
                    match result {
                        Ok((client_stream, peer)) => {
                            let target_clone = target.clone();
                            // Spawn connect + proxy together so a stalled
                            // upstream doesn't block the accept loop.
                            connections.spawn(async move {
                                let target_stream = match TcpStream::connect(&target_clone).await {
                                    Ok(s) => s,
                                    Err(e) => {
                                        tracing::trace!(target = %target_clone, error = %e, "port_forward: upstream connect failed");
                                        return;
                                    }
                                };
                                tracing::trace!(peer = ?peer, "port_forward: accepted");
                                proxy(client_stream, target_stream).await;
                            });
                        }
                        Err(e) => {
                            tracing::trace!(error = %e, "port_forward: accept error");
                            continue;
                        }
                    }
                }
                // Reap completed per-conn tasks so the JoinSet doesn't grow
                // without bound under steady-state traffic.
                Some(_) = connections.join_next() => {}
            }
        }
    });

    Ok(PortForwardHandle {
        container_id,
        host_port,
        container_port,
        shutdown_tx: Some(shutdown_tx),
        join_handle: Some(join_handle),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_port_binding_labels_parses_valid_entries() {
        let mut labels = HashMap::new();
        labels.insert(
            "containerd-manager.port.27017".to_string(),
            "50123".to_string(),
        );
        labels.insert(
            "containerd-manager.port.443".to_string(),
            "8443".to_string(),
        );
        labels.insert("unrelated-label".to_string(), "value".to_string());

        let mut forwards = parse_port_binding_labels(&labels);
        forwards.sort();
        assert_eq!(forwards, vec![(8443, 443), (50123, 27017)]);
    }

    #[test]
    fn parse_port_binding_labels_empty() {
        assert!(parse_port_binding_labels(&HashMap::new()).is_empty());
    }

    #[test]
    fn parse_port_binding_labels_ignores_malformed() {
        let mut labels = HashMap::new();
        labels.insert(
            "containerd-manager.port.notanumber".to_string(),
            "50123".to_string(),
        );
        labels.insert(
            "containerd-manager.port.27017".to_string(),
            "notanumber".to_string(),
        );
        labels.insert("containerd-manager.port.".to_string(), "50123".to_string());

        assert!(parse_port_binding_labels(&labels).is_empty());
    }

    #[test]
    fn start_port_forward_fails_if_port_in_use() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let blocker = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = blocker.local_addr().unwrap().port();

            let result = start_port_forward("test", port, 80, PortForwardOpts::default());
            match result {
                Err(Error::PortForward(_)) => {}
                Err(other) => panic!("expected Error::PortForward, got {other:?}"),
                Ok(_) => panic!("expected error when port is in use"),
            }
        });
    }

    /// 127.0.0.1 is the default because host networking + bridge proxy both
    /// route through loopback. Lock so a refactor doesn't silently change it.
    #[test]
    fn port_forward_default_target_addr_is_loopback() {
        assert_eq!(PortForwardOpts::default().target_addr, "127.0.0.1");
    }
}
