//! Bridge networking for containerd containers via OCI hooks.
//!
//! Each container with [`NetworkMode::Bridge`] gets an isolated network
//! namespace connected to a shared bridge (`cni0`, 10.88.0.1/16).  A socat
//! proxy in the VM's root network namespace forwards a unique host-loopback
//! port to the container's port 27017, making it accessible from the Mac host
//! via colima's VZ network.
//!
//! # How it works
//!
//! 1. **prestart hook** - runs inside the colima VM before the init process
//!    starts.  Creates a veth pair, attaches the host end to the `cni0` bridge,
//!    moves the container end into the container's new network namespace, and
//!    configures addresses + default route.
//!
//! 2. **poststart hook** - runs after the init process starts.  Launches a
//!    background `socat` process that listens on the VM's loopback at
//!    `HOST_PORT` and proxies to `CONTAINER_IP:27017`.  The VZ network then
//!    exposes this loopback port to the Mac host.
//!
//! 3. **poststop hook** - runs when the container stops.  Kills the socat
//!    process and deletes the veth host end.
//!
//! # Concurrency model
//!
//! IP allocation is serialised process-wide via [`ALLOC_LOCK`] so two
//! parallel `alloc_bridge_ip` calls in the same `containerd-manager`
//! process can't pick the same octet. **Cross-process** races (two
//! independent processes each running their own `containerd-manager`)
//! are still possible: the listing scan persists labels via
//! `containers.create`, so the loser sees the winner's label on its next
//! scan and picks differently, but a tight race window (~10s of ms) exists.
//! Treat this as a single-process tool.
//!
//! # Port allocation
//!
//! `pick_free_port` binds + drops on the host. In colima setups the host
//! port maps to a VM-side socat, so the binding only confirms the port
//! is free *on the host*; the VM-side port universe is independent. See
//! [`pick_free_port`] for details.

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::str::FromStr;

use containerd_client::services::v1::ListContainersRequest;
use rand::seq::IteratorRandom;

use crate::client::Client;
use crate::consts::{BRIDGE_GATEWAY, BRIDGE_IP_LABEL, BRIDGE_NAME, CONTAINER_SUBNET_PREFIX};
use crate::error::{Error, Result};
use crate::util::StatusExt;

/// Process-wide mutex serialising IP allocation + container-create. Two
/// concurrent `alloc_bridge_ip` calls would otherwise scan the same set of
/// labels, both pick the same free octet, and both `containers.create`
/// commit, leaving two containers with the same bridge IP. Wrapped in an
/// `Arc` so we can hand out `OwnedMutexGuard` to the caller. Cross-process
/// races (a separate process picking the same octet between our scan and
/// create) are still possible but rare; the listing scan rejects octets
/// already persisted in containerd.
static ALLOC_LOCK: std::sync::LazyLock<std::sync::Arc<tokio::sync::Mutex<()>>> =
    std::sync::LazyLock::new(|| std::sync::Arc::new(tokio::sync::Mutex::new(())));

/// Held by `alloc_bridge_ip` and released by the caller after the
/// `containers.create` RPC that persists the bridge IP label.
pub(crate) struct BridgeAllocGuard(#[allow(dead_code)] tokio::sync::OwnedMutexGuard<()>);

/// Picks a `10.88.1.x` octet not currently used by any container in the
/// namespace. Returns a guard the caller must hold until after the matching
/// `containers.create` completes.
pub(crate) async fn alloc_bridge_ip(client: &Client) -> Result<(String, BridgeAllocGuard)> {
    let guard = ALLOC_LOCK.clone().lock_owned().await;
    let used = used_octets(client).await?;
    let octet = pick_free_octet(&used)?;
    let ip = format!("{CONTAINER_SUBNET_PREFIX}.{octet}");
    tracing::debug!(ip = %ip, used_count = used.len(), "alloc_bridge_ip");
    Ok((ip, BridgeAllocGuard(guard)))
}

/// Random unused octet in `2..=254`, or `Err` if all are taken.
fn pick_free_octet(used: &HashSet<u8>) -> Result<u8> {
    (2u8..=254)
        .filter(|o| !used.contains(o))
        .choose(&mut rand::rng())
        .ok_or_else(|| {
            Error::ResourceExhausted(format!(
                "no free bridge IPs in {CONTAINER_SUBNET_PREFIX}.0/24 ({} octets in use)",
                used.len()
            ))
        })
}

/// Last octet of a well-formed IPv4 string, or `None` if it isn't one.
fn parse_octet(ip: &str) -> Option<u8> {
    Ipv4Addr::from_str(ip).ok().map(|a| a.octets()[3])
}

/// Reads `BRIDGE_IP_LABEL` from every container in the namespace and returns
/// the set of last-octet values found. Goes direct to containerd's
/// ListContainers - avoids the per-container task fetch that
/// `list::list_containers` does, since we only want labels.
async fn used_octets(client: &Client) -> Result<HashSet<u8>> {
    let req = client.ns_req(ListContainersRequest { filters: vec![] });
    let resp = client
        .containers_client()
        .list(req)
        .await
        .map_err(|e| e.into_crate_error("list_containers"))?;

    Ok(resp
        .into_inner()
        .containers
        .iter()
        .filter_map(|c| c.labels.get(BRIDGE_IP_LABEL))
        .filter_map(|ip| parse_octet(ip))
        .collect())
}

/// 15-char veth name (Linux limit) derived via FNV-1a hash so common prefixes
/// (e.g. `foo` vs `foo-2`) don't collide.
pub(crate) fn veth_name(container_id: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in container_id.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // 13 hex chars = 52 bits; collision probability is negligible for our scale.
    format!("cm{:013x}", hash & 0x000f_ffff_ffff_ffff)
}

/// Inherent TOCTOU between bind+drop and the consumer's bind - acceptable for
/// dev tooling. Returns `Err` on OS exhaustion instead of silently falling
/// back to a random port that may already be in use.
pub(crate) fn pick_free_port() -> Result<u16> {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").map_err(|e| {
        Error::ResourceExhausted(format!("could not bind ephemeral host port: {e}"))
    })?;
    let port = listener
        .local_addr()
        .map_err(|e| {
            Error::ResourceExhausted(format!("could not read ephemeral port local_addr: {e}"))
        })?
        .port();
    Ok(port)
}

pub(crate) fn prestart_hook_script(container_ip: &str, container_id: &str) -> String {
    let veth = veth_name(container_id);
    format!(
        r#"
STATE=$(cat)
CONTAINER_PID=$(echo "$STATE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('pid',0))")
HOST_VETH="{veth}"
BRIDGE="{BRIDGE_NAME}"
GATEWAY="{BRIDGE_GATEWAY}"
CONTAINER_IP="{container_ip}"

ip link show "$BRIDGE" >/dev/null 2>&1 || ip link add "$BRIDGE" type bridge
ip addr add "$GATEWAY/16" dev "$BRIDGE" 2>/dev/null || true
ip link set "$BRIDGE" up

# `netns` option places the peer in the container's netns directly, so 'eth0'
# never appears in the host namespace (which would conflict with the VM NIC).
ip link add "$HOST_VETH" type veth peer name eth0 netns /proc/$CONTAINER_PID/ns/net
ip link set "$HOST_VETH" master "$BRIDGE"
ip link set "$HOST_VETH" up

nsenter --net=/proc/$CONTAINER_PID/ns/net ip addr add "$CONTAINER_IP/16" dev eth0
nsenter --net=/proc/$CONTAINER_PID/ns/net ip link set eth0 up
nsenter --net=/proc/$CONTAINER_PID/ns/net ip link set lo up
nsenter --net=/proc/$CONTAINER_PID/ns/net ip route add default via "$GATEWAY" 2>/dev/null || true

sysctl -w net.ipv4.ip_forward=1 >/dev/null
iptables -t nat -C POSTROUTING -s 10.88.0.0/16 ! -d 10.88.0.0/16 -j MASQUERADE 2>/dev/null || \
  iptables -t nat -A POSTROUTING -s 10.88.0.0/16 ! -d 10.88.0.0/16 -j MASQUERADE
"#
    )
}

/// One `socat` per binding. Inputs are `(container_port, host_port)` — the
/// reverse of the public `(host_port, container_port)` shape used in
/// `CreateContainerOpts` / `ContainerInfo`, because the hooks were written
/// thinking container-first. Callers do the swap; not unifying here would
/// rewrite every hook test.
///
/// After backgrounding,
/// verify the pid is still alive — if socat failed to exec (missing binary,
/// permission denied) the pid file would otherwise reference a dead process
/// and silently break forwarding.
pub(crate) fn poststart_hook_script(
    container_id: &str,
    container_ip: &str,
    ports: &[(u16, u16)],
) -> String {
    let mut script = String::from("\n");
    for &(container_port, host_port) in ports {
        let pid_file = socat_pid_file(container_id, container_port);
        let log_file = socat_log_file(container_id, container_port);
        script.push_str(&format!(
            "nohup /usr/bin/socat TCP-LISTEN:{host_port},fork,reuseaddr TCP:{container_ip}:{container_port} \\\n  >{log_file} 2>&1 &\n\
             SOCAT_PID=$!\n\
             echo $SOCAT_PID > {pid_file}\n\
             # Give socat a moment to fail-fast on exec / bind errors.\n\
             sleep 0.1\n\
             if ! kill -0 $SOCAT_PID 2>/dev/null; then\n\
               echo \"poststart: socat $SOCAT_PID exited (port {host_port})\" >&2\n\
               cat {log_file} >&2 2>/dev/null || true\n\
               rm -f {pid_file}\n\
               exit 1\n\
             fi\n",
        ));
    }
    script
}

/// Kill the socat we started (by pid only — scoping by host_port via `pkill`
/// would also kill an unrelated container's forwarder that picked the same
/// port after our pid file was lost), remove pid + log files, then drop the
/// veth.
pub(crate) fn poststop_hook_script(container_id: &str, ports: &[(u16, u16)]) -> String {
    let veth = veth_name(container_id);
    let mut script = String::from("\n");
    for &(container_port, _host_port) in ports {
        let pid_file = socat_pid_file(container_id, container_port);
        let log_file = socat_log_file(container_id, container_port);
        // SIGTERM, brief grace, SIGKILL. socat blocked mid-transfer
        // doesn't always respond to SIGTERM; without the SIGKILL fallback
        // the poststop hook hangs and prevents container deletion.
        script.push_str(&format!(
            "SOCAT_PID=$(cat {pid_file} 2>/dev/null || echo)\n\
             if [ -n \"$SOCAT_PID\" ]; then\n\
               kill $SOCAT_PID 2>/dev/null || true\n\
               for i in 1 2 3 4 5; do\n\
                 kill -0 $SOCAT_PID 2>/dev/null || break\n\
                 sleep 0.1\n\
               done\n\
               kill -9 $SOCAT_PID 2>/dev/null || true\n\
             fi\n\
             rm -f {pid_file} {log_file}\n",
        ));
    }
    script.push_str(&format!("ip link delete \"{veth}\" 2>/dev/null || true\n"));
    script
}

/// Safe to interpolate `container_id` unquoted: `ContainerId::new` rejects
/// path separators + shell metachars (`/`, `$`, `;`, backticks, etc. — see
/// `util::validate_identifier`). The allowed set is `[A-Za-z0-9._-]`.
fn socat_pid_file(container_id: &str, container_port: u16) -> String {
    format!("/tmp/socat-{container_id}-{container_port}.pid")
}

fn socat_log_file(container_id: &str, container_port: u16) -> String {
    format!("/tmp/socat-{container_id}-{container_port}.log")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_octet_last_segment() {
        assert_eq!(parse_octet("10.88.1.42"), Some(42));
        assert_eq!(parse_octet("10.88.1.2"), Some(2));
        assert_eq!(parse_octet("10.88.1.254"), Some(254));
    }

    #[test]
    fn parse_octet_rejects_garbage() {
        assert_eq!(parse_octet(""), None);
        assert_eq!(parse_octet("not-an-ip"), None);
        assert_eq!(parse_octet("10.88.1.999"), None);
        assert_eq!(parse_octet("10.88.1.-1"), None);
        // Stricter than the old rsplit-last impl: requires four octets.
        assert_eq!(parse_octet("42"), None);
        assert_eq!(parse_octet("foo.42"), None);
    }

    #[test]
    fn pick_free_octet_returns_in_range() {
        let used = HashSet::new();
        for _ in 0..50 {
            let o = pick_free_octet(&used).unwrap();
            assert!((2..=254).contains(&o));
        }
    }

    #[test]
    fn pick_free_octet_avoids_used() {
        let used: HashSet<u8> = (2..=253).collect();
        // Only 254 is free.
        let o = pick_free_octet(&used).unwrap();
        assert_eq!(o, 254);
    }

    #[test]
    fn pick_free_octet_errors_on_exhaustion() {
        let used: HashSet<u8> = (2..=254).collect();
        let err = pick_free_octet(&used).unwrap_err();
        assert!(matches!(err, Error::ResourceExhausted(_)));
    }

    #[test]
    fn veth_name_is_15_chars_and_stable() {
        let a = veth_name("my-container");
        let b = veth_name("my-container");
        assert_eq!(a, b);
        assert_eq!(a.len(), 15);
        assert!(a.starts_with("cm"));
    }

    #[test]
    fn veth_name_distinguishes_prefix_siblings() {
        assert_ne!(veth_name("foo"), veth_name("foo-2"));
        assert_ne!(veth_name("containerd-demo"), veth_name("containerd-demo-2"));
    }

    #[test]
    fn poststart_one_port_emits_one_socat() {
        let script = poststart_hook_script("cid", "10.88.1.5", &[(27017, 50123)]);
        assert_eq!(script.matches("socat TCP-LISTEN").count(), 1);
        assert!(script.contains("TCP-LISTEN:50123"));
        assert!(script.contains("TCP:10.88.1.5:27017"));
        assert!(script.contains("/tmp/socat-cid-27017.pid"));
    }

    #[test]
    fn poststart_multi_port_emits_one_socat_per_binding() {
        let script = poststart_hook_script("cid", "10.88.1.5", &[(27017, 50001), (8080, 50002)]);
        assert_eq!(script.matches("socat TCP-LISTEN").count(), 2);
        assert!(script.contains("TCP-LISTEN:50001"));
        assert!(script.contains("TCP-LISTEN:50002"));
        assert!(script.contains("TCP:10.88.1.5:27017"));
        assert!(script.contains("TCP:10.88.1.5:8080"));
        // Per-port pid files prevent socats from clobbering each other.
        assert!(script.contains("/tmp/socat-cid-27017.pid"));
        assert!(script.contains("/tmp/socat-cid-8080.pid"));
    }

    #[test]
    fn poststart_no_ports_emits_no_socat() {
        let script = poststart_hook_script("cid", "10.88.1.5", &[]);
        assert!(!script.contains("socat"));
    }

    #[test]
    fn poststop_kills_each_socat_then_deletes_veth() {
        let script = poststop_hook_script("cid", &[(27017, 50001), (8080, 50002)]);
        // Each binding loads pid then SIGTERM then SIGKILL.
        assert_eq!(script.matches("SOCAT_PID=$(cat").count(), 2);
        assert_eq!(script.matches("kill -9 $SOCAT_PID").count(), 2);
        assert!(script.contains("/tmp/socat-cid-27017.pid"));
        assert!(script.contains("/tmp/socat-cid-8080.pid"));
        // host_port no longer appears - scoping by host_port via pkill -f
        // would risk killing an unrelated forwarder that reused the port.
        assert!(!script.contains("TCP-LISTEN"));
        assert!(script.contains("ip link delete"));
        // Veth delete happens once, after all kills.
        let veth_idx = script.find("ip link delete").unwrap();
        let last_kill_idx = script.rfind("kill -9 $SOCAT_PID").unwrap();
        assert!(veth_idx > last_kill_idx);
    }
}
