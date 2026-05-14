//! Create containerd Container from an image.

use std::collections::HashMap;

use containerd_client::services::v1::content_client::ContentClient;
use containerd_client::services::v1::snapshots::{PrepareSnapshotRequest, RemoveSnapshotRequest};
use containerd_client::services::v1::{
    Container, CreateContainerRequest, GetImageRequest, ReadContentRequest,
};
use containerd_client::tonic::Request;
use containerd_client::with_namespace;
use oci_spec::image::ImageConfiguration;
use oci_spec::runtime::{
    get_default_namespaces, Capability, Hook, HookBuilder, Hooks, HooksBuilder, LinuxBuilder,
    LinuxCapabilities, LinuxCapabilitiesBuilder, LinuxNamespaceType, Mount as OciMount,
    ProcessBuilder, RootBuilder, Spec as OciSpec, SpecBuilder,
};
use prost_types::Any;
use sha2::{Digest, Sha256};

use crate::client::Client;
use crate::consts::{
    BRIDGE_IP_LABEL, DEFAULT_RUNTIME, DEFAULT_SNAPSHOTTER, HOOK_PATH, NETWORK_MODE_LABEL,
    OCI_ROOTFS_PATH, OCI_SPEC_TYPE_URL, OCI_SPEC_VERSION, PORT_BINDING_LABEL_PREFIX,
};
use crate::error::{Error, Result};
use crate::util::{map_image_status, StatusExt};

/// Snapshot key for a container. Must be consistent between create_container
/// (prepare) and remove_container (remove). Keyed off the opaque hash ID so
/// delete+recreate with the same human name yields a fresh snapshot key.
pub(crate) fn snapshot_key_for(container_id: &str) -> String {
    format!("{}-snapshot", container_id)
}

#[derive(Debug, Clone, Default)]
pub struct Mount {
    pub source: String,
    pub destination: String,
    /// Mount options (e.g., `"ro"`, `"rw"`).
    pub options: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum NetworkMode {
    /// No external connectivity.
    Isolated,
    /// Share the host's network namespace.
    Host,
    /// CNI-style bridge via OCI hooks.
    #[default]
    Bridge,
}

impl NetworkMode {
    /// String form stored in `NETWORK_MODE_LABEL`.
    pub(crate) fn label_value(&self) -> &'static str {
        match self {
            NetworkMode::Bridge => "bridge",
            NetworkMode::Host => "host",
            NetworkMode::Isolated => "isolated",
        }
    }
}

#[derive(Debug, Clone, Default, typed_builder::TypedBuilder)]
pub struct NetworkOpts {
    #[builder(default)]
    pub mode: NetworkMode,
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
    /// Add a read-write bind mount.
    pub fn mount(&mut self, source: impl Into<String>, destination: impl Into<String>) {
        self.mounts.push(Mount {
            source: source.into(),
            destination: destination.into(),
            options: vec!["rbind".to_string(), "rw".to_string()],
        });
    }
    /// Add a read-only bind mount.
    pub fn mount_ro(&mut self, source: impl Into<String>, destination: impl Into<String>) {
        self.mounts.push(Mount {
            source: source.into(),
            destination: destination.into(),
            options: vec!["rbind".to_string(), "ro".to_string()],
        });
    }
    /// Add a port binding. `(host_port, container_port)`. May be called
    /// repeatedly. Persisted as container labels so they survive restarts;
    /// `start_container` auto-starts proxies for them.
    pub fn port_binding(&mut self, host_port: u16, container_port: u16) {
        self.port_bindings.push((host_port, container_port));
    }
))]
pub struct CreateContainerOpts {
    #[builder(via_mutators)]
    pub env: HashMap<String, String>,
    #[builder(via_mutators)]
    pub labels: HashMap<String, String>,
    #[builder(via_mutators)]
    pub mounts: Vec<Mount>,
    #[builder(default)]
    pub network: NetworkOpts,
    /// Replaces image entrypoint + cmd. Accepts any iterator of stringy
    /// items: `.cmd(["sleep", "infinity"])`.
    #[builder(default, setter(transform = |args: impl IntoIterator<Item = impl Into<String>>| Some(args.into_iter().map(Into::into).collect::<Vec<_>>())))]
    pub cmd: Option<Vec<String>>,
    /// `(host_port, container_port)` pairs.
    #[builder(via_mutators)]
    pub port_bindings: Vec<(u16, u16)>,
    /// Adopt an existing snapshot key instead of preparing a fresh one from
    /// the image's chain id. Caller is responsible for ensuring the snapshot
    /// is rooted at a parent that contains the image's layers, otherwise the
    /// container task will fail to mount its rootfs.
    ///
    /// Internal coupling: this is used by `clone_container` /
    /// `restore_container` to thread their already-prepared snapshot through
    /// `create_container`. External users should prefer those high-level APIs
    /// instead of constructing this field directly.
    #[doc(hidden)]
    #[builder(default, setter(strip_option, into))]
    pub from_existing_snapshot: Option<String>,
}

/// Image labels first, then user labels override. Port-binding labels are
/// emitted by the caller after bridge allocation runs (or directly from
/// `opts.port_bindings` for host/isolated mode).
fn build_labels(
    image_config: &ImageConfiguration,
    opts: &CreateContainerOpts,
) -> HashMap<String, String> {
    let mut labels = HashMap::new();
    if let Some(cfg) = image_config.config() {
        if let Some(img_labels) = cfg.labels() {
            labels.extend(img_labels.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
    }
    labels.extend(opts.labels.iter().map(|(k, v)| (k.clone(), v.clone())));
    labels
}

fn build_mounts(opts: &CreateContainerOpts) -> Vec<OciMount> {
    opts.mounts
        .iter()
        .map(|m| {
            let mut mount = OciMount::default();
            mount.set_source(Some(m.source.clone().into()));
            mount.set_destination(m.destination.clone().into());
            mount.set_typ(Some("bind".to_string()));
            mount.set_options(Some(m.options.clone()));
            mount
        })
        .collect()
}

/// `cmd` override replaces image entrypoint+cmd entirely; otherwise we
/// concatenate image entrypoint+cmd and fall back to `/bin/sh`.
fn build_args(image_config: &ImageConfiguration, opts: &CreateContainerOpts) -> Vec<String> {
    if let Some(cmd) = opts.cmd.as_ref() {
        return cmd.clone();
    }
    let mut args: Vec<String> = image_config
        .config()
        .as_ref()
        .map(|c| {
            c.entrypoint()
                .iter()
                .flatten()
                .chain(c.cmd().iter().flatten())
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    if args.is_empty() {
        args.push("/bin/sh".to_string());
    }
    args
}

fn build_cwd(image_config: &ImageConfiguration) -> String {
    image_config
        .config()
        .as_ref()
        .and_then(|c| c.working_dir().as_ref())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "/".to_string())
}

/// Image env first, then user opts override (last write wins).
fn build_merged_env(image_config: &ImageConfiguration, opts: &CreateContainerOpts) -> Vec<String> {
    let mut env_map: HashMap<String, String> = HashMap::new();
    if let Some(config) = image_config.config() {
        if let Some(config_env) = config.env() {
            for entry in config_env {
                if let Some((key, value)) = entry.split_once('=') {
                    env_map.insert(key.to_string(), value.to_string());
                }
            }
        }
    }
    env_map.extend(opts.env.iter().map(|(k, v)| (k.clone(), v.clone())));
    env_map
        .into_iter()
        .map(|(k, v)| format!("{}={}", k, v))
        .collect()
}

fn build_namespaces(network_mode: &NetworkMode) -> Vec<oci_spec::runtime::LinuxNamespace> {
    let mut namespaces = get_default_namespaces();
    // Host mode shares the host net namespace.
    if matches!(network_mode, NetworkMode::Host) {
        namespaces.retain(|ns| ns.typ() != LinuxNamespaceType::Network);
    }
    namespaces
}

/// An OCI hook that runs `/bin/sh -c <script>` in the VM.
fn sh_hook(script: &str) -> Result<Hook> {
    let path = std::path::PathBuf::from("/bin/sh");
    HookBuilder::default()
        .path(path)
        .args(vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            script.to_string(),
        ])
        .env(vec![HOOK_PATH.to_string()])
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build hook: {e}")))
}

/// prestart = veth+bridge+routes; poststart = socat proxy; poststop = cleanup.
fn build_bridge_hooks(
    container_id: &str,
    container_ip: &str,
    namespace: &str,
    ports: &[(u16, u16)],
    host_container_pairs: &[(u16, u16)],
) -> Result<Hooks> {
    let prestart = sh_hook(&crate::bridge::prestart_hook_script(
        container_ip,
        container_id,
        namespace,
        host_container_pairs,
    ))?;
    let poststart = sh_hook(&crate::bridge::poststart_hook_script(
        container_id,
        container_ip,
        ports,
    ))?;
    let poststop = sh_hook(&crate::bridge::poststop_hook_script(
        container_id,
        namespace,
        ports,
    ))?;

    // `prestart` is deprecated in favour of `create_runtime` but still widely supported.
    #[allow(deprecated)]
    HooksBuilder::default()
        .prestart(vec![prestart])
        .poststart(vec![poststart])
        .poststop(vec![poststop])
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build hooks: {e}")))
}

/// Same set as `build_default_capabilities`, exposed for `exec` processes so
/// they share the container's primary-process privileges.
pub(crate) fn default_exec_capabilities() -> Result<LinuxCapabilities> {
    build_default_capabilities()
}

/// Docker-equivalent default capability set.
fn build_default_capabilities() -> Result<LinuxCapabilities> {
    use std::collections::HashSet;

    let caps: HashSet<Capability> = [
        Capability::Chown,
        Capability::DacOverride,
        Capability::Fsetid,
        Capability::Fowner,
        Capability::Mknod,
        Capability::NetRaw,
        Capability::Setgid,
        Capability::Setuid,
        Capability::Setfcap,
        Capability::Setpcap,
        Capability::NetBindService,
        Capability::SysChroot,
        Capability::Kill,
        Capability::AuditWrite,
    ]
    .into_iter()
    .collect();

    LinuxCapabilitiesBuilder::default()
        .bounding(caps.clone())
        .effective(caps.clone())
        .inheritable(caps.clone())
        .permitted(caps.clone())
        .ambient(caps)
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build capabilities: {}", e)))
}

fn build_oci_spec(
    image_config: &ImageConfiguration,
    opts: &CreateContainerOpts,
    hooks: Option<Hooks>,
) -> Result<OciSpec> {
    let args = build_args(image_config, opts);
    let cwd = build_cwd(image_config);
    let env = build_merged_env(image_config, opts);
    let mounts = build_mounts(opts);
    let namespaces = build_namespaces(&opts.network.mode);

    let capabilities = build_default_capabilities()?;
    let user = oci_spec::runtime::UserBuilder::default()
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build user spec: {}", e)))?;
    let process = ProcessBuilder::default()
        .terminal(false)
        .user(user)
        .args(args)
        .env(env)
        .cwd(cwd)
        .capabilities(capabilities)
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build process spec: {}", e)))?;

    let root = RootBuilder::default()
        .path(OCI_ROOTFS_PATH.to_string())
        .readonly(false)
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build root spec: {}", e)))?;

    let linux = LinuxBuilder::default()
        .namespaces(namespaces)
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build linux spec: {}", e)))?;

    let mut spec_builder = SpecBuilder::default()
        .version(OCI_SPEC_VERSION.to_string())
        .process(process)
        .root(root)
        .linux(linux);

    if !mounts.is_empty() {
        spec_builder = spec_builder.mounts(mounts);
    }

    if let Some(h) = hooks {
        spec_builder = spec_builder.hooks(h);
    }

    spec_builder
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build OCI spec: {}", e)))
}

/// Serializes the OCI spec to a prost Any.
fn spec_to_any(spec: &OciSpec) -> Result<Any> {
    let json = serde_json::to_vec(spec)
        .map_err(|e| Error::InvalidArgument(format!("serialize OCI spec: {}", e)))?;

    Ok(Any {
        type_url: OCI_SPEC_TYPE_URL.to_string(),
        value: json,
    })
}

/// For one layer, chain ID = diff ID. For N>1,
/// chain_N = sha256(chain_{N-1} + " " + diff_N).
fn compute_chain_id(diff_ids: &[String]) -> String {
    if diff_ids.is_empty() {
        return String::new();
    }

    let mut chain_id = diff_ids[0].clone();

    for diff_id in &diff_ids[1..] {
        let input = format!("{} {}", chain_id, diff_id);
        let mut hasher = Sha256::new();
        hasher.update(input.as_bytes());
        let result = hasher.finalize();
        chain_id = format!("sha256:{}", hex::encode(result));
    }

    chain_id
}

async fn read_content(
    content_client: &mut ContentClient<containerd_client::tonic::transport::Channel>,
    digest: &str,
    namespace: &str,
) -> Result<Vec<u8>> {
    let req = with_namespace!(
        ReadContentRequest {
            digest: digest.to_string(),
            offset: 0,
            size: 0, // Read entire blob
        },
        namespace
    );

    let mut stream = content_client
        .read(req)
        .await
        .map_err(|e| e.into_crate_error("read_content"))?
        .into_inner();

    let mut data = Vec::new();
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|e| e.into_crate_error("read_content_chunk"))?
    {
        data.extend(chunk.data);
    }

    Ok(data)
}

pub(crate) struct ImageMetadata {
    pub(crate) chain_id: String,
    pub(crate) config: ImageConfiguration,
    pub(crate) raw_config: Vec<u8>,
}

/// Walks `manifest -> [sub-manifest for our arch] -> config`, caching by
/// manifest digest on the [`Client`]. The digest is the cache key (not the
/// image name) so retagging a name to a different content doesn't return
/// stale data.
async fn get_image_metadata(
    client: &Client,
    content_client: &mut ContentClient<containerd_client::tonic::transport::Channel>,
    manifest_digest: &str,
    namespace: &str,
) -> Result<std::sync::Arc<ImageMetadata>> {
    if let Some(cached) = client.lookup_image_metadata(manifest_digest) {
        return Ok(cached);
    }

    let metadata = fetch_image_metadata(content_client, manifest_digest, namespace).await?;
    let arc = std::sync::Arc::new(metadata);
    client.store_image_metadata(manifest_digest.to_string(), arc.clone());
    Ok(arc)
}

/// Pure fetch - no caching. Split from [`get_image_metadata`] so the cache
/// wrapper stays trivial.
async fn fetch_image_metadata(
    content_client: &mut ContentClient<containerd_client::tonic::transport::Channel>,
    manifest_digest: &str,
    namespace: &str,
) -> Result<ImageMetadata> {
    let manifest_data = read_content(content_client, manifest_digest, namespace).await?;

    let manifest: serde_json::Value = serde_json::from_slice(&manifest_data)
        .map_err(|e| Error::InvalidArgument(format!("parse manifest: {}", e)))?;

    let config_digest = if let Some(config) = manifest.get("config") {
        config
            .get("digest")
            .and_then(|d| d.as_str())
            .ok_or_else(|| Error::InvalidArgument("manifest missing config digest".to_string()))?
            .to_string()
    } else if let Some(manifests) = manifest.get("manifests") {
        // Manifest list / OCI index - pick the sub-manifest for our arch.
        let target_arch = crate::oci_arch();

        let sub_manifest_digest = manifests
            .as_array()
            .ok_or_else(|| Error::InvalidArgument("invalid manifests array".to_string()))?
            .iter()
            .find(|m| {
                m.get("platform")
                    .and_then(|p| p.get("architecture"))
                    .and_then(|a| a.as_str())
                    == Some(target_arch)
            })
            .and_then(|m| m.get("digest"))
            .and_then(|d| d.as_str())
            .ok_or_else(|| {
                Error::InvalidArgument(format!(
                    "no manifest found for architecture {}",
                    target_arch
                ))
            })?;

        let sub_manifest_data =
            read_content(content_client, sub_manifest_digest, namespace).await?;
        let sub_manifest: serde_json::Value = serde_json::from_slice(&sub_manifest_data)
            .map_err(|e| Error::InvalidArgument(format!("parse sub-manifest: {}", e)))?;

        sub_manifest
            .get("config")
            .and_then(|c| c.get("digest"))
            .and_then(|d| d.as_str())
            .ok_or_else(|| {
                Error::InvalidArgument("sub-manifest missing config digest".to_string())
            })?
            .to_string()
    } else {
        return Err(Error::InvalidArgument(
            "manifest has neither config nor manifests".to_string(),
        ));
    };

    let config_data = read_content(content_client, &config_digest, namespace).await?;

    let config: ImageConfiguration = serde_json::from_slice(&config_data)
        .map_err(|e| Error::InvalidArgument(format!("parse image config: {}", e)))?;

    let diff_ids: Vec<String> = config
        .rootfs()
        .diff_ids()
        .iter()
        .map(|d| d.to_string())
        .collect();

    if diff_ids.is_empty() {
        return Err(Error::InvalidArgument(
            "image config has no diff_ids".to_string(),
        ));
    }

    Ok(ImageMetadata {
        chain_id: compute_chain_id(&diff_ids),
        config,
        raw_config: config_data,
    })
}

/// Raw image config JSON. Used internally to read Docker-specific extensions
/// (HEALTHCHECK, etc.) that aren't in the typed `ImageConfiguration`.
pub(crate) async fn get_raw_image_config(client: &Client, image: &str) -> Result<Vec<u8>> {
    let get_image_req = client.ns_req(GetImageRequest {
        name: image.to_string(),
    });
    let image_resp = client
        .images_client()
        .get(get_image_req)
        .await
        .map_err(|e| map_image_status("get_image", image, e))?;

    let image_info = image_resp
        .into_inner()
        .image
        .ok_or_else(|| Error::ImageNotFound(image.to_string()))?;

    let manifest_digest = image_info
        .target
        .as_ref()
        .map(|t| t.digest.clone())
        .ok_or_else(|| Error::ImageNotFound(format!("{}: no target descriptor", image)))?;

    let mut content_client = client.content_client();
    let metadata = get_image_metadata(
        client,
        &mut content_client,
        &manifest_digest,
        client.namespace(),
    )
    .await?;
    Ok(metadata.raw_config.clone())
}

/// Creates the container record only. The task is started separately via
/// [`crate::task::start_container`]. Returns the freshly-minted opaque
/// [`ContainerId`] (a UUIDv4 hex). The `name` is stored as a label for
/// reverse lookup; the container's containerd ID is the hash. `name` is
/// validated against containerd's identifier rules.
pub(crate) async fn create_container(
    client: &Client,
    name: &str,
    image: &str,
    opts_owned: CreateContainerOpts,
) -> Result<crate::types::ContainerId> {
    crate::util::validate_identifier(name)?;
    create_container_with_id(
        client,
        crate::types::ContainerId::generate(),
        name,
        image,
        opts_owned,
    )
    .await
}

/// Internal variant: caller supplies the [`ContainerId`]. Used by clone /
/// restore so the snapshot key can be computed upfront from the same ID
/// that `create_container` writes into the containerd record.
pub(crate) async fn create_container_with_id(
    client: &Client,
    container_id: crate::types::ContainerId,
    name: &str,
    image: &str,
    opts_owned: CreateContainerOpts,
) -> Result<crate::types::ContainerId> {
    crate::util::validate_identifier(name)?;
    tracing::info!(
        name = %name,
        container_id = %container_id,
        image,
        from_existing_snapshot = opts_owned.from_existing_snapshot.is_some(),
        "create_container"
    );
    let opts = &opts_owned;
    let container_id_str = container_id.as_str();

    // Reject duplicate names in this namespace upfront. Without this the
    // user gets `ContainerAlreadyExists` from a snapshot key collision,
    // which is confusing. Names are unique-per-namespace by contract.
    let dupe_filter = format!("labels.\"{}\"=={}", crate::consts::NAME_LABEL, name);
    let existing = client
        .containers_client()
        .list(
            client.ns_req(containerd_client::services::v1::ListContainersRequest {
                filters: vec![dupe_filter],
            }),
        )
        .await
        .map_err(|e| e.into_crate_error("list_containers_for_name_check"))?
        .into_inner()
        .containers;
    if !existing.is_empty() {
        return Err(Error::ContainerAlreadyExists(name.to_string()));
    }

    let get_image_req = client.ns_req(GetImageRequest {
        name: image.to_string(),
    });
    let image_resp = client
        .images_client()
        .get(get_image_req)
        .await
        .map_err(|e| map_image_status("get_image", image, e))?;

    let image_info = image_resp
        .into_inner()
        .image
        .ok_or_else(|| Error::ImageNotFound(image.to_string()))?;

    let manifest_digest = image_info
        .target
        .as_ref()
        .map(|t| t.digest.clone())
        .ok_or_else(|| Error::InvalidArgument("image has no target".to_string()))?;

    let mut content_client = client.content_client();
    let image_metadata = get_image_metadata(
        client,
        &mut content_client,
        &manifest_digest,
        client.namespace(),
    )
    .await?;

    // Adopt pre-prepared (clone path) or prepare from image chain id.
    // `we_prepared` gates rollback: clone-path snapshots aren't ours to remove.
    let (snapshot_key, we_prepared) = match opts_owned.from_existing_snapshot.as_deref() {
        Some(key) => (key.to_string(), false),
        None => {
            let key = snapshot_key_for(container_id_str);
            let prepare_req = client.ns_req(PrepareSnapshotRequest {
                snapshotter: DEFAULT_SNAPSHOTTER.to_string(),
                key: key.clone(),
                parent: image_metadata.chain_id.clone(),
                labels: HashMap::new(),
            });
            client.snapshots().prepare(prepare_req).await.map_err(|e| {
                if e.code() == containerd_client::tonic::Code::AlreadyExists {
                    Error::ContainerAlreadyExists(name.to_string())
                } else {
                    e.into_crate_error("prepare_snapshot")
                }
            })?;
            (key, true)
        }
    };

    let network_mode = opts.network.mode.clone();

    // Bridge: alloc IP + host-loopback port per container port. Default to
    // 27017 (Atlas-Local). Guard held until containers.create commits the
    // label so a parallel alloc can't pick the same octet.
    type BridgeConfig = Option<(String, Vec<(u16, u16)>)>;
    let (bridge_config, _alloc_guard): (BridgeConfig, _) =
        if matches!(network_mode, NetworkMode::Bridge) {
            let (container_ip, guard) = crate::bridge::alloc_bridge_ip(client).await?;
            let container_ports: Vec<u16> = if opts.port_bindings.is_empty() {
                vec![27017]
            } else {
                opts.port_bindings.iter().map(|&(_, cp)| cp).collect()
            };
            let ports: Vec<(u16, u16)> = container_ports
                .into_iter()
                .map(|cp| crate::bridge::pick_free_port().map(|hp| (cp, hp)))
                .collect::<Result<Vec<_>>>()?;
            (Some((container_ip, ports)), Some(guard))
        } else {
            (None, None)
        };

    // Compute (host_port, container_port) pairs up front: needed by both the
    // bridge prestart hook (to write nerdctl's network-config.json) and the
    // label emission below.
    let host_container_pairs: Vec<(u16, u16)> = match bridge_config {
        Some((_, ref ports)) => ports.iter().map(|&(cp, hp)| (hp, cp)).collect(),
        None => opts.port_bindings.clone(),
    };

    let hooks: Option<Hooks> = if let Some((ref ip, ref ports)) = bridge_config {
        Some(build_bridge_hooks(
            container_id_str,
            ip,
            client.namespace(),
            ports,
            &host_container_pairs,
        )?)
    } else {
        None
    };

    let oci_spec = build_oci_spec(&image_metadata.config, opts, hooks)?;
    let spec_any = spec_to_any(&oci_spec)?;

    let mut labels = build_labels(&image_metadata.config, opts);
    labels.insert(
        NETWORK_MODE_LABEL.to_string(),
        network_mode.label_value().to_string(),
    );
    if let Some((ref ip, _)) = bridge_config {
        labels.insert(BRIDGE_IP_LABEL.to_string(), ip.clone());
    }
    for &(host_port, container_port) in &host_container_pairs {
        labels.insert(
            format!("{}{}", PORT_BINDING_LABEL_PREFIX, container_port),
            host_port.to_string(),
        );
    }
    // Our own name label for fast reverse lookup (name → ID).
    labels.insert(crate::consts::NAME_LABEL.to_string(), name.to_string());
    // nerdctl-compatible labels so `nerdctl ps` / `inspect` recognise our
    // containers. Port info goes in nerdctl's per-container
    // network-config.json (written by the bridge prestart hook), not in a
    // `nerdctl/ports` label — nerdctl 2.x warns about the legacy label form.
    labels.insert("nerdctl/name".to_string(), name.to_string());
    labels.insert(
        "nerdctl/namespace".to_string(),
        client.namespace().to_string(),
    );

    let container = Container {
        id: container_id_str.to_string(),
        image: image.to_string(),
        runtime: Some(containerd_client::services::v1::container::Runtime {
            name: DEFAULT_RUNTIME.to_string(),
            options: None,
        }),
        spec: Some(spec_any),
        snapshotter: DEFAULT_SNAPSHOTTER.to_string(),
        snapshot_key: snapshot_key.clone(),
        labels,
        ..Default::default()
    };

    let create_req = client.ns_req(CreateContainerRequest {
        container: Some(container),
    });

    if let Err(e) = client.containers_client().create(create_req).await {
        let err = if e.code() == containerd_client::tonic::Code::AlreadyExists {
            Error::ContainerAlreadyExists(name.to_string())
        } else {
            e.into_crate_error("create_container")
        };
        // Rollback the active snapshot we just prepared; otherwise leaks.
        if we_prepared {
            let _ = client
                .snapshots()
                .remove(client.ns_req(RemoveSnapshotRequest {
                    snapshotter: DEFAULT_SNAPSHOTTER.to_string(),
                    key: snapshot_key.clone(),
                }))
                .await;
        }
        return Err(err);
    }

    Ok(container_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oci_spec::image::{ConfigBuilder, ImageConfigurationBuilder, RootFsBuilder};

    fn test_image_config() -> ImageConfiguration {
        let rootfs = RootFsBuilder::default()
            .typ("layers".to_string())
            .diff_ids(vec!["sha256:abc123".to_string()])
            .build()
            .unwrap();
        ImageConfigurationBuilder::default()
            .rootfs(rootfs)
            .build()
            .unwrap()
    }

    fn test_image_config_with_entrypoint() -> ImageConfiguration {
        let rootfs = RootFsBuilder::default()
            .typ("layers".to_string())
            .diff_ids(vec!["sha256:abc123".to_string()])
            .build()
            .unwrap();
        let config = ConfigBuilder::default()
            .entrypoint(vec!["/entrypoint.sh".to_string()])
            .cmd(vec!["--flag".to_string()])
            .working_dir("/app".to_string())
            .env(vec!["PATH=/usr/bin".to_string(), "HOME=/root".to_string()])
            .build()
            .unwrap();
        ImageConfigurationBuilder::default()
            .rootfs(rootfs)
            .config(config)
            .build()
            .unwrap()
    }

    #[test]
    fn build_labels_creates_hashmap() {
        let config = test_image_config();
        let opts = CreateContainerOpts::builder()
            .label("app", "test")
            .label("version", "1.0")
            .build();

        let labels = build_labels(&config, &opts);
        assert_eq!(labels.get("app"), Some(&"test".to_string()));
        assert_eq!(labels.get("version"), Some(&"1.0".to_string()));
    }

    #[test]
    fn build_mounts_creates_oci_mounts() {
        let opts = CreateContainerOpts::builder()
            .mount("/host/data", "/container/data")
            .mount_ro("/host/config", "/container/config")
            .build();

        let mounts = build_mounts(&opts);
        assert_eq!(mounts.len(), 2);

        assert_eq!(
            mounts[0].source().as_ref().map(|p| p.to_string_lossy()),
            Some("/host/data".into())
        );
        assert_eq!(mounts[0].destination().to_string_lossy(), "/container/data");

        assert_eq!(
            mounts[1].source().as_ref().map(|p| p.to_string_lossy()),
            Some("/host/config".into())
        );
        assert!(mounts[1]
            .options()
            .as_ref()
            .unwrap()
            .contains(&"ro".to_string()));
    }

    #[test]
    fn build_args_uses_entrypoint_and_cmd() {
        let config = test_image_config_with_entrypoint();
        let args = build_args(&config, &CreateContainerOpts::default());
        assert_eq!(args, vec!["/entrypoint.sh", "--flag"]);
    }

    #[test]
    fn build_args_falls_back_to_shell() {
        let config = test_image_config();
        let args = build_args(&config, &CreateContainerOpts::default());
        assert_eq!(args, vec!["/bin/sh"]);
    }

    #[test]
    fn build_args_cmd_override_replaces_image_args() {
        let config = test_image_config_with_entrypoint();
        let opts = CreateContainerOpts::builder()
            .cmd(["sleep", "infinity"])
            .build();
        let args = build_args(&config, &opts);
        assert_eq!(args, vec!["sleep", "infinity"]);
    }

    #[test]
    fn build_args_cmd_override_on_empty_image() {
        let config = test_image_config();
        let opts = CreateContainerOpts::builder()
            .cmd(["/bin/my-app", "--verbose"])
            .build();
        let args = build_args(&config, &opts);
        assert_eq!(args, vec!["/bin/my-app", "--verbose"]);
    }

    #[test]
    fn build_args_opts_without_cmd_uses_image() {
        let config = test_image_config_with_entrypoint();
        let opts = CreateContainerOpts::builder().label("app", "test").build();
        let args = build_args(&config, &opts);
        assert_eq!(args, vec!["/entrypoint.sh", "--flag"]);
    }

    #[test]
    fn create_container_opts_cmd_builder() {
        let opts = CreateContainerOpts::builder().cmd(["sleep", "300"]).build();
        assert_eq!(opts.cmd, Some(vec!["sleep".to_string(), "300".to_string()]));
    }

    #[test]
    fn build_cwd_uses_image_working_dir() {
        let config = test_image_config_with_entrypoint();
        let cwd = build_cwd(&config);
        assert_eq!(cwd, "/app");
    }

    #[test]
    fn build_cwd_defaults_to_root() {
        let config = test_image_config();
        let cwd = build_cwd(&config);
        assert_eq!(cwd, "/");
    }

    #[test]
    fn build_merged_env_combines_image_and_opts() {
        let config = test_image_config_with_entrypoint();
        let opts = CreateContainerOpts::builder()
            .env("MY_VAR", "my_value")
            .env("PATH", "/custom")
            .build();
        let env = build_merged_env(&config, &opts);

        assert!(env.contains(&"MY_VAR=my_value".to_string()));
        assert!(env.contains(&"HOME=/root".to_string())); // From image
                                                          // PATH is overridden: exactly one PATH= entry, value from opts.
        let path_entries: Vec<&String> = env.iter().filter(|e| e.starts_with("PATH=")).collect();
        assert_eq!(
            path_entries.len(),
            1,
            "expected exactly one PATH= entry, got {:?}",
            path_entries
        );
        assert_eq!(path_entries[0], "PATH=/custom");
    }

    #[test]
    fn build_oci_spec_succeeds() {
        let config = test_image_config();
        let opts = CreateContainerOpts::builder().env("TEST", "value").build();

        let spec = build_oci_spec(&config, &opts, None).expect("should build spec");
        assert_eq!(spec.version(), "1.0.2");

        let process = spec.process().as_ref().expect("should have process");
        let env = process.env().as_ref().expect("should have env");
        assert!(env.contains(&"TEST=value".to_string()));
    }

    #[test]
    fn build_oci_spec_uses_image_entrypoint() {
        let config = test_image_config_with_entrypoint();
        let spec = build_oci_spec(&config, &CreateContainerOpts::default(), None)
            .expect("should build spec");

        let process = spec.process().as_ref().expect("should have process");
        let args = process.args().as_ref().expect("should have args");
        assert_eq!(
            args,
            &vec!["/entrypoint.sh".to_string(), "--flag".to_string()]
        );
        assert_eq!(process.cwd().to_string_lossy(), "/app");
    }

    #[test]
    fn spec_to_any_serializes_correctly() {
        let config = test_image_config();
        let spec = build_oci_spec(&config, &CreateContainerOpts::default(), None)
            .expect("should build spec");
        let any = spec_to_any(&spec).expect("should serialize");

        assert_eq!(
            any.type_url,
            "types.containerd.io/opencontainers/runtime-spec/1/Spec"
        );
        assert!(!any.value.is_empty());
    }

    #[test]
    fn create_container_opts_builder_works() {
        let opts = CreateContainerOpts::builder()
            .env("KEY", "value")
            .label("name", "test")
            .mount("/src", "/dst")
            .build();

        assert_eq!(opts.env.len(), 1);
        assert_eq!(opts.labels.len(), 1);
        assert_eq!(opts.mounts.len(), 1);
    }

    #[test]
    fn compute_chain_id_single_layer() {
        let diff_ids = vec![
            "sha256:45f3ea5848e8a25ca27718b640a21ffd8c8745d342a24e1d4ddfc8c449b0a724".to_string(),
        ];
        let chain_id = compute_chain_id(&diff_ids);
        assert_eq!(chain_id, diff_ids[0]);
    }

    #[test]
    fn compute_chain_id_multiple_layers() {
        let diff_ids = vec!["sha256:aaaa".to_string(), "sha256:bbbb".to_string()];
        let chain_id = compute_chain_id(&diff_ids);
        // The chain ID should be sha256 of "sha256:aaaa sha256:bbbb"
        assert!(chain_id.starts_with("sha256:"));
        assert_ne!(chain_id, diff_ids[0]);
        assert_ne!(chain_id, diff_ids[1]);
    }

    #[test]
    fn compute_chain_id_empty() {
        let diff_ids: Vec<String> = vec![];
        let chain_id = compute_chain_id(&diff_ids);
        assert!(chain_id.is_empty());
    }

    #[test]
    fn network_opts_default_is_bridge() {
        let opts = NetworkOpts::default();
        assert!(matches!(opts.mode, NetworkMode::Bridge));
    }

    #[test]
    fn network_mode_host_removes_network_namespace() {
        let namespaces = build_namespaces(&NetworkMode::Host);
        // Host mode should NOT have a network namespace
        assert!(!namespaces
            .iter()
            .any(|ns| ns.typ() == oci_spec::runtime::LinuxNamespaceType::Network));
    }

    #[test]
    fn network_mode_bridge_has_network_namespace() {
        let namespaces = build_namespaces(&NetworkMode::Bridge);
        // Bridge mode should have a network namespace
        assert!(namespaces
            .iter()
            .any(|ns| ns.typ() == oci_spec::runtime::LinuxNamespaceType::Network));
    }

    #[test]
    fn network_mode_isolated_has_network_namespace() {
        let namespaces = build_namespaces(&NetworkMode::Isolated);
        // Isolated mode keeps its own network namespace (no external connectivity)
        assert!(namespaces
            .iter()
            .any(|ns| ns.typ() == oci_spec::runtime::LinuxNamespaceType::Network));
    }

    #[test]
    fn create_container_opts_host_network_builder() {
        let opts = CreateContainerOpts::builder()
            .network(NetworkOpts {
                mode: NetworkMode::Host,
            })
            .build();
        assert!(matches!(opts.network.mode, NetworkMode::Host));
    }

    #[test]
    fn create_container_opts_network_mode_builder() {
        let opts = CreateContainerOpts::builder()
            .network(NetworkOpts {
                mode: NetworkMode::Isolated,
            })
            .build();
        assert!(matches!(opts.network.mode, NetworkMode::Isolated));
    }

    #[test]
    fn build_oci_spec_host_network_omits_netns() {
        let config = test_image_config();
        let opts = CreateContainerOpts::builder()
            .network(NetworkOpts {
                mode: NetworkMode::Host,
            })
            .build();

        let spec = build_oci_spec(&config, &opts, None).expect("should build spec");
        let linux = spec.linux().as_ref().expect("should have linux");
        let namespaces = linux.namespaces().as_ref().expect("should have namespaces");

        // Verify network namespace is NOT present
        assert!(!namespaces
            .iter()
            .any(|ns| ns.typ() == oci_spec::runtime::LinuxNamespaceType::Network));
    }
}
