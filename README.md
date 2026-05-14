# containerd-manager

[![CI](https://github.com/mongodb-labs/containerd-manager/actions/workflows/ci.yml/badge.svg)](https://github.com/mongodb-labs/containerd-manager/actions/workflows/ci.yml)

Rust async library for managing **containerd** (containers, tasks, images) over gRPC. An alternative to Docker/Bollard for container lifecycle management without a Docker daemon.

> [!WARNING]
> Work in progress. Public API may change.

**Platform support:**

| Platform | State |
|---|---|
| Linux native | Works. CI tested. |
| macOS + Colima | Works. Primary development target. |
| Windows + WSL2 | Works as Linux from inside the WSL2 distro. |
| Windows native | Not supported. `connect()` returns an error pointing to WSL2. |

## Prerequisites

containerd-manager requires a running **containerd** daemon. Docker Desktop is **not** required - containerd runs independently.

### macOS (Colima)

[Colima](https://github.com/abiosoft/colima) runs a Linux VM with containerd on macOS.
containerd-manager talks to that VM's containerd socket directly.

#### Why a non-`default` profile

Colima's `default` profile is provisioned with the docker runtime: it runs a
Docker daemon inside the VM, and the containerd socket there is the one
Docker itself uses. To avoid stepping on Docker, our auto-discovery (and
this guide) uses a **separate, non-`default` colima profile** dedicated to
containerd. You can run Docker Desktop or `colima start` (the default
docker profile) alongside without conflict.

#### Fresh install

```bash
brew install colima

# Create a dedicated containerd profile. `--runtime` is honoured only at
# profile-creation time; recreate the profile if you need to change it.
colima start --profile containerd --runtime containerd
```

The socket lands at `~/.colima/containerd/containerd.sock`.

#### Verify the setup

```bash
colima status --profile containerd     # should say: runtime: containerd
ls ~/.colima/containerd/containerd.sock
```

#### Inspecting containers from the terminal

`ctr` is containerd's built-in CLI and is available inside the Colima VM.
Use `colima ssh` to run it:

```bash
# List all namespaces
colima ssh --profile containerd -- sudo ctr namespaces ls

# List containers in a namespace (e.g. "my-app")
colima ssh --profile containerd -- sudo ctr -n my-app containers ls

# List running tasks (processes) in a namespace
colima ssh --profile containerd -- sudo ctr -n my-app tasks ls
```

> **Note:** containerd isolates resources by namespace. Containers created under
> `"my-app"` are invisible to the `"default"` namespace and to Docker.

#### Switching an existing profile to containerd

If you previously did `colima start --runtime containerd` (without `--profile`)
the runtime is locked onto the `default` profile but our auto-discovery
ignores `default`. Recreate as a non-default profile:

```bash
colima stop && colima delete --force
colima start --profile containerd --runtime containerd
```

### Linux

containerd is typically available as a system package.

```bash
# Debian / Ubuntu
sudo apt-get install containerd

# Start the daemon
sudo systemctl start containerd
sudo systemctl enable containerd
```

The default socket path is `/run/containerd/containerd.sock`.

### Windows (WSL2)

Run containerd inside a WSL2 Linux distribution.

```bash
# Inside your WSL2 distro (e.g. Ubuntu)
sudo apt-get install containerd
sudo containerd &
```

The socket path inside WSL2 is `/run/containerd/containerd.sock`. If connecting from a Windows host, map the WSL2 path or set the `CONTAINERD_SOCKET` environment variable.

## Adding as a dependency

```toml
# Path (local development)
containerd-manager = { path = "../containerd-manager" }

# Git
containerd-manager = { git = "https://github.com/mongodb-labs/containerd-manager" }
```

## Quick start

```rust,ignore
use std::time::Duration;
use containerd_manager::{ContainerId, CreateContainerOpts, ReadinessStrategy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to containerd (auto-detects a Colima socket on macOS).
    let client = containerd_manager::connect(None)?.with_namespace("my-app");

    let image = "docker.io/library/alpine:latest";
    let id = ContainerId::new("example-1")?;

    // Pull. Idempotent: skips if already present in the namespace.
    client.pull_image(image).await?;

    let opts = CreateContainerOpts::new()
        .label("app", "example")
        .host_network()
        .cmd(["sleep", "300"]);
    client.create_container(&id, image, opts).await?;
    client.start_container(&id).await?;

    // Wait until ready. ProcessRunning is the simplest strategy; use
    // ImageHealthcheck for Docker-compatible health-aware waiting.
    client
        .wait_ready(&id, Duration::from_secs(60), ReadinessStrategy::ProcessRunning)
        .await?;

    let output = client.exec(&id, &["echo", "hello"]).await?;
    println!("{}", output.stdout_str().trim());

    // Stream logs live (use container_logs for a one-shot bulk read).
    let mut follower = client.container_logs_stream(&id)?;
    if let Some(Ok(entry)) = follower.recv().await {
        print!("{}", String::from_utf8_lossy(&entry.data));
    }

    client.delete_container(&id, Duration::from_secs(30)).await?;
    Ok(())
}
```

A complete end-to-end example (MongoDB Atlas Local) is in [`examples/atlas_local.rs`](examples/atlas_local.rs):

```bash
cargo run --example atlas_local
```

## Public API

- **`connect(socket_path: Option<PathBuf>) -> Result<Client>`**: synchronous; connect to containerd. Defaults to the `"default"` containerd namespace.
- **`Client`**: all container operations, scoped to the namespace set at construction.
  - **Namespace:** `namespace`, `with_namespace`
  - **Image:** `pull_image`, `image_healthcheck`
  - **Lifecycle:** `create_container`, `start_container`, `stop_container`, `delete_container`, `pause_container`, `unpause_container`
  - **Query:** `inspect_container`, `list_containers`, `probe_health`
  - **I/O:** `exec`, `container_logs`, `container_logs_stream`
  - **Port forwarding:** `start_port_forward`, `start_managed_port_forward`
  - **Readiness:** `wait_ready`
  - **Server:** `server_version`
  - **Cache:** `image_cache_len`, `clear_image_cache`
- **Types:** `Error`, `Result`, `ContainerId`, `TaskId`, `CreateContainerOpts`, `Mount`, `NetworkMode`, `NetworkOpts`, `PortForwardHandle`, `PortForwardOpts`, `ContainerInfo`, `TaskInfo`, `TaskStatus`, `ExecOutput`, `LogEntry`, `LogStream`, `LogFollower`, `ReadinessStrategy`, `HealthCheck`, `HealthStatus`.

`ContainerId` is a validated newtype (containerd's identifier rules); construct with `ContainerId::new("foo-1")?`. Blocks shell injection and path traversal at the API boundary.

## Examples

```bash
cargo run --example atlas_local            # full MongoDB Atlas Local lifecycle + live log tail
cargo run --example follow_logs            # docker-logs-f style demo with Ctrl-C cleanup
cargo run --example bridge_networking      # bridge mode + port_binding (veth + socat)
cargo run --example atlas_local_bollard    # side-by-side via Bollard (Docker API), for comparison
```

## Testing

Three suites, gated by feature flags. See [TESTING.md](TESTING.md) for the full table.

```bash
cargo test                                              # unit tests, no setup
cargo test --features e2e-tests                         # needs colima running
cargo test --features e2e-colima-profiles -- --test-threads=1 --nocapture   # destructive
```

## Configuration

### Socket path

The containerd socket path is resolved in this order:

| Priority | Source | Example |
|----------|--------|---------|
| 1 | Explicit argument to `connect()` | `connect(Some("/custom/path.sock".into()))` |
| 2 | `CONTAINERD_SOCKET` environment variable | `export CONTAINERD_SOCKET=~/.colima/containerd/containerd.sock` |
| 3 | Platform default | macOS: scans `~/.colima/<profile>/` (skips `default`), Linux: `/run/containerd/containerd.sock` |

### Namespaces

containerd uses **namespaces** to isolate resources. The namespace is set on the `Client` and applies to all operations:

```rust,ignore
// Default namespace
let client = containerd_manager::connect(None)?;
assert_eq!(client.namespace(), "default");

// Custom namespace
let client = client.with_namespace("my-app");
```

Different namespaces see different containers, images, and tasks. `with_namespace` returns a new `Client` that shares the underlying connection.

## Troubleshooting

### Socket not found

```
Error: containerd socket not found at '~/.colima/containerd/containerd.sock'
```

**Cause:** containerd is not running, or the socket is at a different path.

**Fix:**

1. Start containerd:
   - macOS: `colima start --profile containerd --runtime containerd`
   - Linux: `sudo systemctl start containerd`
   - WSL2: `sudo containerd &`
2. If the socket is at a non-default path, set the env var:
   ```bash
   export CONTAINERD_SOCKET=/path/to/containerd.sock
   ```
3. Verify the socket exists: `ls -la /path/to/containerd.sock`

### Connection refused

```
Error: connection refused to containerd socket at '...'
```

**Cause:** The socket file exists but nothing is listening (e.g. containerd crashed or was stopped uncleanly).

**Fix:**

1. Check if containerd is running:
   - macOS: `colima status --profile containerd`
   - Linux: `systemctl status containerd`
2. Restart it:
   - macOS: `colima stop --profile containerd && colima start --profile containerd`
   - Linux: `sudo systemctl restart containerd`
3. If the socket is stale, remove and restart:
   ```bash
   sudo rm /path/to/containerd.sock
   # Then start containerd again
   ```

### Port already in use

```
Error: port forwarding error: ...
```

**Cause:** The host port requested for `start_port_forward` is already bound by another process.

**Fix:**

1. Find what's using the port:
   ```bash
   lsof -i :27017    # macOS / Linux
   ```
2. Stop the conflicting process, or choose a different host port.
3. If a previous run left a stale forward, ensure you dropped the `PortForwardHandle` or deleted the container.

### Colima VM won't start (macOS)

```
FATA: error starting vm: error at 'starting': exit status 1
```

**Cause:** Stale VM state from a previous unclean shutdown, or the disk was provisioned for a different runtime.

**Fix:**

```bash
# Delete the existing instance and start fresh
colima delete --profile containerd --force
colima start --profile containerd --runtime containerd

# If the error mentions "runtime disk provisioned for docker runtime":
colima delete --profile containerd --data
colima start --profile containerd --runtime containerd
```

### Image pull is slow or fails

**Cause:** Large images (like MongoDB Atlas Local) take time on first pull. Failures are usually network-related.

**Fix:**

1. Verify network connectivity inside the VM:
   - macOS: `colima ssh -- ping -c1 registry-1.docker.io`
2. Check if the image reference is correct (include the registry prefix):
   ```
   docker.io/mongodb/mongodb-atlas-local:latest   # correct
   mongodb/mongodb-atlas-local:latest              # may not resolve
   ```
3. Subsequent pulls of the same image are fast (content is cached in the store).

## MSRV

`rust-version = "1.88"`

## License

Apache-2.0
