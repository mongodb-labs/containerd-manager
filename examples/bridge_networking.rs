//! Bridge networking demo: run nginx in bridge mode, connect to it through
//! the auto-started socat proxy.
//!
//! Exercises the path that is the entire reason this crate exists:
//!   * `NetworkMode::Bridge` (default) wires OCI prestart/poststart/poststop
//!     hooks that build a veth pair into the cni0 bridge and run a socat
//!     proxy in the VM's root namespace.
//!   * `port_binding(host_port, container_port)` persists as a container
//!     label; `start_container` auto-starts a user-space proxy from the
//!     host loopback to the bound port.
//!
//! After the container is up the example issues an HTTP GET to confirm
//! traffic flows host -> proxy -> socat (in VM) -> container.
//!
//! Run with: cargo run --example bridge_networking
//!
//! Preconditions:
//!   * containerd reachable (Colima profile on macOS, native daemon on Linux)
//!   * On Linux: the OCI hooks need socat, iproute2, iptables, util-linux,
//!     python3 installed and CAP_NET_ADMIN available to the runc/shim.

use std::time::Duration;

use containerd_manager::{ContainerId, CreateContainerOpts, ReadinessStrategy};

const IMAGE: &str = "docker.io/library/nginx:alpine";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = containerd_manager::connect(None)?.with_namespace("bridge-demo");
    let id = ContainerId::new("nginx-bridge-demo")?;
    let host_port: u16 = 18080;
    let container_port: u16 = 80;

    // Clean up any leftover from a previous run.
    let _ = client.delete_container(&id, Duration::from_secs(10)).await;

    println!("pulling {IMAGE}");
    client.pull_image(IMAGE).await?;

    // Bridge mode is the default; spelled out here for clarity. The
    // port_binding is what triggers the auto-started host-side proxy on
    // start_container.
    let opts = CreateContainerOpts::builder()
        .label("app", "bridge-demo")
        .port_binding(host_port, container_port).build();

    println!("creating container {id} (bridge mode, {host_port} -> :{container_port})");
    client.create_container(&id, IMAGE, opts).await?;

    println!("starting container");
    let task_id = client.start_container(&id).await?;
    println!("pid {}", task_id.pid());

    // Wait for nginx to bind its port inside the container.
    println!("waiting for tcp ready on host loopback :{host_port}");
    client
        .wait_ready(
            &id,
            Duration::from_secs(30),
            ReadinessStrategy::TcpPort(host_port),
        )
        .await?;

    // Talk to the container via the auto-started proxy.
    println!("\nGET http://127.0.0.1:{host_port}/");
    let body = http_get(host_port).await?;
    let preview: String = body.lines().take(3).collect::<Vec<_>>().join("\n");
    println!("--- response preview ---");
    println!("{preview}");

    println!("\ndeleting container (also tears down proxy + veth)");
    client
        .delete_container(&id, Duration::from_secs(10))
        .await?;
    Ok(())
}

/// Bare-bones HTTP/1.1 GET so we don't pull in reqwest just for a demo.
async fn http_get(port: u16) -> Result<String, Box<dyn std::error::Error>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await?;

    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}
