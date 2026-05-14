//! Crate-wide constants. All container labels owned by this crate live under
//! the `containerd-manager.*` namespace - keep them here so the namespace
//! stays auditable.

pub(crate) const DEFAULT_SNAPSHOTTER: &str = "overlayfs";
pub(crate) const DEFAULT_RUNTIME: &str = "io.containerd.runc.v2";

pub(crate) const SOCKET_ENV_VAR: &str = "CONTAINERD_SOCKET";

/// Default grace period when an op (clone, snapshot, reset) needs to stop
/// the source task to mutate its snapshot.
pub(crate) const SNAPSHOT_OP_STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// `containerd-manager.port.<container_port> = <host_port>`
pub(crate) const PORT_BINDING_LABEL_PREFIX: &str = "containerd-manager.port.";

/// Bridge-mode container IP allocated by `alloc_bridge_ip()`.
pub(crate) const BRIDGE_IP_LABEL: &str = "containerd-manager.bridge.ip";

/// `NetworkMode` recorded at create time. Value: `"bridge"`, `"host"`, `"isolated"`.
pub(crate) const NETWORK_MODE_LABEL: &str = "containerd-manager.network.mode";

pub(crate) const BRIDGE_NAME: &str = "cni0";
pub(crate) const BRIDGE_GATEWAY: &str = "10.88.0.1";
/// `/24` subnet - IPs `10.88.1.2`-`10.88.1.254`.
pub(crate) const CONTAINER_SUBNET_PREFIX: &str = "10.88.1";

/// OCI runtime spec version we generate.
pub(crate) const OCI_SPEC_VERSION: &str = "1.0.2";

/// Path inside the bundle where the runtime spec mounts the rootfs.
pub(crate) const OCI_ROOTFS_PATH: &str = "rootfs";

/// `Any.type_url` for the OCI runtime spec payload attached to a container.
pub(crate) const OCI_SPEC_TYPE_URL: &str = "types.containerd.io/opencontainers/runtime-spec/1/Spec";

/// PATH provided to OCI prestart/poststart/poststop hooks. Hooks run with a
/// near-empty environment by default; tools like `ip`, `socat`, `nohup`
/// must be reachable.
pub(crate) const HOOK_PATH: &str =
    "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

#[cfg(test)]
mod tests {
    use super::*;

    // Pinned: persisted on every container record; silent renames orphan
    // label-keyed lookups on upgrade.
    #[test]
    fn label_keys_pinned() {
        assert_eq!(PORT_BINDING_LABEL_PREFIX, "containerd-manager.port.");
        assert_eq!(BRIDGE_IP_LABEL, "containerd-manager.bridge.ip");
        assert_eq!(NETWORK_MODE_LABEL, "containerd-manager.network.mode");
    }

    #[test]
    fn oci_constants_pinned() {
        assert_eq!(OCI_SPEC_VERSION, "1.0.2");
        assert_eq!(
            OCI_SPEC_TYPE_URL,
            "types.containerd.io/opencontainers/runtime-spec/1/Spec"
        );
        assert_eq!(OCI_ROOTFS_PATH, "rootfs");
    }

    #[test]
    fn snapshotter_pinned() {
        // The snapshotter name is embedded in every container record; changing
        // it would break snapshot lookups on existing containers.
        assert_eq!(DEFAULT_SNAPSHOTTER, "overlayfs");
    }
}
