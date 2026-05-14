//! Integration tests against a real containerd. Gated by the `e2e-tests`
//! feature so the default `cargo test` skips them.
//!
//! Preconditions:
//!   * Colima is running (containerd socket reachable).
//!   * Docker daemon is NOT running (avoids accidentally testing against the
//!     Docker-shim containerd instance).
//!
//! Run: `cargo test --features e2e-tests`
#![cfg(feature = "e2e-tests")]

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use containerd_manager::{
    ContainerId, CreateContainerOpts, Error, HealthStatus, LogStream, PortForwardOpts,
    ReadinessStrategy, TaskStatus,
};

const ALPINE: &str = "docker.io/library/alpine:latest";
const BUSYBOX: &str = "docker.io/library/busybox:latest";
const NAMESPACE: &str = "e2e-tests";
const TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Preflight: verify the environment before any test touches it.
// ---------------------------------------------------------------------------

/// Cached preflight result. Using `OnceLock<Result>` rather than `Once`
/// means we surface the *same* diagnostic on every test instead of leaving
/// 22 of them showing "Once instance has previously been poisoned" after
/// the first panic.
static PREFLIGHT: OnceLock<Result<(), String>> = OnceLock::new();

fn preflight() {
    let status = PREFLIGHT.get_or_init(check_env);
    if let Err(reason) = status {
        panic!(
            "e2e-tests preflight failed: {reason}\n\
             \n\
             needs a reachable containerd socket. macOS: `colima start --profile \
             containerd --runtime containerd`. Linux: install + start containerd \
             (e.g. `sudo systemctl start containerd`), then either chmod the \
             socket or set CONTAINERD_SOCKET. See TESTING.md."
        );
    }
}

/// Probe order:
///   1. `CONTAINERD_SOCKET` env var if set
///   2. macOS: scan `~/.colima/<profile>/containerd.sock`
///   3. Linux/other: `/run/containerd/containerd.sock`
fn check_env() -> Result<(), String> {
    if let Ok(env_path) = std::env::var("CONTAINERD_SOCKET") {
        let p = PathBuf::from(&env_path);
        return reachable_sock(&p)
            .then_some(())
            .ok_or_else(|| format!("CONTAINERD_SOCKET={env_path} not reachable"));
    }

    #[cfg(target_os = "macos")]
    {
        let home = PathBuf::from(std::env::var("HOME").map_err(|_| "$HOME not set".to_string())?);
        let colima_dir = home.join(".colima");
        return first_existing_colima_sock(&colima_dir)
            .map(|_| ())
            .ok_or_else(|| {
                format!(
                    "no live colima containerd socket under {}",
                    colima_dir.display()
                )
            });
    }

    #[cfg(not(target_os = "macos"))]
    {
        let default = PathBuf::from("/run/containerd/containerd.sock");
        if reachable_sock(&default) {
            return Ok(());
        }
        Err(format!(
            "no live containerd socket at {}",
            default.display()
        ))
    }
}

fn reachable_sock(p: &Path) -> bool {
    p.exists() && std::os::unix::net::UnixStream::connect(p).is_ok()
}

#[cfg(target_os = "macos")]
fn first_existing_colima_sock(colima_dir: &Path) -> Option<PathBuf> {
    std::fs::read_dir(colima_dir).ok()?.flatten().find_map(|e| {
        let sock = e.path().join("containerd.sock");
        reachable_sock(&sock).then_some(sock)
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn connect() -> containerd_manager::Client {
    preflight();
    containerd_manager::connect(None)
        .expect("failed to connect to containerd")
        .with_namespace(NAMESPACE)
}

/// Returns a Client pinned to a namespace derived from `test_tag` so concurrent
/// tests don't see each other's containers. Use this in any test that calls
/// `list_containers` or otherwise observes shared namespace state.
fn connect_isolated(test_tag: &str) -> containerd_manager::Client {
    preflight();
    let ns = format!("e2e-{test_tag}");
    containerd_manager::connect(None)
        .expect("failed to connect to containerd")
        .with_namespace(ns)
}

/// Bind/drop on the loopback to pick a free host port. Inherent TOCTOU vs.
/// the test's later bind, but on a developer machine the window is small
/// and beats hardcoding ports that collide between parallel runs.
fn pick_free_host_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind loopback")
        .local_addr()
        .expect("local_addr")
        .port()
}

/// Poll `predicate` every 50ms up to `deadline`. Replaces hardcoded sleeps
/// for "wait a bit then assert". Returns `true` if the predicate succeeded,
/// `false` if the deadline elapsed.
async fn poll_until<F, Fut>(deadline: Duration, mut predicate: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if predicate().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

fn cid(s: &str) -> ContainerId {
    ContainerId::new(s).expect("valid identifier")
}

async fn cleanup(client: &containerd_manager::Client, container_id: &ContainerId) {
    let _ = client.delete_container(container_id, TIMEOUT).await;
}

// ---------------------------------------------------------------------------
// Smoke
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_connect() {
    let client = connect();
    let version = client.server_version().await.unwrap();
    assert!(!version.is_empty(), "server version should not be empty");
}

#[tokio::test]
async fn test_pull_image() {
    let client = connect();
    client.pull_image(ALPINE).await.unwrap();
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_create_container() {
    let client = connect();
    let id = cid("create-test");

    cleanup(&client, &id).await;
    client.pull_image(ALPINE).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .env("MY_VAR", "hello")
        .label("app", "test").build();
    client
        .create_container(&id, ALPINE, opts.clone())
        .await
        .unwrap();

    // Duplicate should fail.
    let err = client.create_container(&id, ALPINE, opts).await;
    assert!(
        matches!(err, Err(Error::ContainerAlreadyExists(_))),
        "expected ContainerAlreadyExists, got {:?}",
        err
    );

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_start_and_delete_container() {
    let client = connect();
    let id = cid("lifecycle-test");

    cleanup(&client, &id).await;
    client.pull_image(ALPINE).await.unwrap();

    let opts = CreateContainerOpts::builder().label("test", "lifecycle").build();
    client.create_container(&id, ALPINE, opts).await.unwrap();

    let task_id = client.start_container(&id).await.unwrap();
    assert_eq!(task_id.container_id(), id.as_str());
    assert!(task_id.pid() > 0, "pid should be positive");

    client.delete_container(&id, TIMEOUT).await.unwrap();
    // Idempotent.
    client.delete_container(&id, TIMEOUT).await.unwrap();
}

#[tokio::test]
async fn test_inspect_container() {
    let client = connect();
    let id = cid("inspect-test");

    cleanup(&client, &id).await;
    client.pull_image(ALPINE).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("app", "inspect-test")
        .label("version", "1.0")
        .env("MY_VAR", "hello")
        .env("ANOTHER", "world")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, ALPINE, opts).await.unwrap();

    let info = client.inspect_container(&id).await.unwrap();
    assert_eq!(info.id, id);
    assert!(info.task.is_none(), "task should be None before start");
    assert_eq!(
        info.labels.get("app").map(String::as_str),
        Some("inspect-test")
    );
    assert_eq!(info.env.get("MY_VAR").map(String::as_str), Some("hello"));

    let task_id = client.start_container(&id).await.unwrap();
    let info = client.inspect_container(&id).await.unwrap();
    let task = info.task.as_ref().expect("task present after start");
    assert_eq!(task.status, TaskStatus::Running);
    assert_eq!(task.pid, task_id.pid());

    let nonexistent = cid("does-not-exist");
    let err = client.inspect_container(&nonexistent).await;
    assert!(
        matches!(err, Err(Error::ContainerNotFound(_))),
        "expected ContainerNotFound, got {:?}",
        err
    );

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_list_containers() {
    // Isolated namespace so a leftover container from a prior run can't
    // hide a regression here (list_containers(&[]).len() >= 2 would pass
    // silently with stale state).
    let client = connect_isolated("list-containers");
    let id1 = cid("list-test-1");
    let id2 = cid("list-test-2");

    cleanup(&client, &id1).await;
    cleanup(&client, &id2).await;
    client.pull_image(ALPINE).await.unwrap();

    let opts1 = CreateContainerOpts::builder()
        .label("app", "frontend")
        .label("list-group", "e2e").build();
    client.create_container(&id1, ALPINE, opts1).await.unwrap();

    let opts2 = CreateContainerOpts::builder()
        .label("app", "backend")
        .label("list-group", "e2e").build();
    client.create_container(&id2, ALPINE, opts2).await.unwrap();

    let all = client.list_containers(&[]).await.unwrap();
    // Exact count in an isolated namespace.
    assert_eq!(all.len(), 2);

    let frontend = client.list_containers(&["app=frontend"]).await.unwrap();
    assert_eq!(frontend.len(), 1);
    assert_eq!(frontend[0].id, id1);

    let backend = client.list_containers(&["app=backend"]).await.unwrap();
    assert_eq!(backend.len(), 1);
    assert_eq!(backend[0].id, id2);

    let both = client.list_containers(&["list-group=e2e"]).await.unwrap();
    assert_eq!(both.len(), 2);

    let multi = client
        .list_containers(&["app=frontend", "list-group=e2e"])
        .await
        .unwrap();
    assert_eq!(multi.len(), 1);
    assert_eq!(multi[0].id, id1);

    let empty_client = client.with_namespace("does-not-exist");
    let empty = empty_client.list_containers(&[]).await.unwrap();
    assert!(empty.is_empty());

    cleanup(&client, &id1).await;
    cleanup(&client, &id2).await;
}

#[tokio::test]
async fn test_container_logs() {
    let client = connect();
    let id = cid("logs-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder().label("test", "logs").cmd([
        "sh",
        "-c",
        "echo hello-from-logs && sleep 300",
    ]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let appeared = poll_until(Duration::from_secs(10), || async {
        client
            .container_logs(&id)
            .map(|e| !e.is_empty())
            .unwrap_or(false)
    })
    .await;
    assert!(appeared, "expected log output from echo within 10s");

    let entries = client.container_logs(&id).unwrap();
    assert!(entries
        .iter()
        .all(|e| matches!(e.stream, LogStream::Stdout | LogStream::Stderr)));

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_container_logs_stream() {
    let client = connect();
    let id = cid("logs-stream-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    // Container prints a numbered line every 100ms.
    let opts = CreateContainerOpts::builder()
        .label("test", "logs-stream")
        .cmd([
            "sh",
            "-c",
            "i=0; while :; do echo line-$i; i=$((i+1)); sleep 0.1; done",
        ]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let mut follower = client.container_logs_stream(&id).unwrap();

    let mut got = Vec::new();
    for _ in 0..5 {
        let entry = tokio::time::timeout(Duration::from_secs(5), follower.recv())
            .await
            .expect("follower recv timed out")
            .expect("follower closed early")
            .expect("follower yielded I/O error");
        got.push(String::from_utf8_lossy(&entry.data).to_string());
    }

    assert_eq!(got.len(), 5);
    for (i, line) in got.iter().enumerate() {
        assert_eq!(line, &format!("line-{i}\n"), "unexpected line ordering");
    }

    drop(follower);
    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_wait_for_exit() {
    let client = connect();
    let id = cid("wait-exit-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    // Container exits with code 42 after ~200ms.
    let opts = CreateContainerOpts::builder().label("test", "wait-exit").cmd([
        "sh",
        "-c",
        "sleep 0.2; exit 42",
    ]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let code = client
        .wait_for_exit(&id, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(code, 42);

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_wait_for_exit_timeout() {
    let client = connect();
    let id = cid("wait-exit-timeout-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    // Long-running container; should hit the 1s timeout.
    let opts = CreateContainerOpts::builder().cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let result = client.wait_for_exit(&id, Duration::from_secs(1)).await;
    assert!(
        matches!(result, Err(Error::Timeout(_))),
        "expected Timeout, got {:?}",
        result
    );

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_container_logs_stream_until_exit() {
    let client = connect();
    let id = cid("logs-stream-until-exit-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    // Short-lived container prints three lines then exits.
    let opts =
        CreateContainerOpts::builder().cmd(["sh", "-c", "echo one; echo two; echo three; sleep 0.5"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let mut follower = client.container_logs_stream_until_exit(&id).unwrap();
    let mut got = Vec::new();
    while let Some(result) = tokio::time::timeout(Duration::from_secs(10), follower.recv())
        .await
        .expect("recv timed out")
    {
        got.push(String::from_utf8_lossy(&result.unwrap().data).to_string());
    }

    // Stream auto-ended when the task exited. All three lines delivered.
    assert!(got.contains(&"one\n".to_string()), "got: {:?}", got);
    assert!(got.contains(&"two\n".to_string()));
    assert!(got.contains(&"three\n".to_string()));

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_exec() {
    let client = connect();
    let id = cid("exec-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "exec")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let output = client.exec(&id, &["echo", "hello"]).await.unwrap();
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "hello");
    assert_eq!(output.exit_code, 0);

    let output = client.exec(&id, &["false"]).await.unwrap();
    assert_ne!(output.exit_code, 0);

    let output = client
        .exec(&id, &["sh", "-c", "echo err_msg >&2"])
        .await
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&output.stderr).trim(), "err_msg");
    assert_eq!(output.exit_code, 0);

    cleanup(&client, &id).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_port_forward() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    let client = connect();
    let id = cid("port-fwd-test");

    let server_port = pick_free_host_port();
    let forward_port = pick_free_host_port();

    let server_listener = TcpListener::bind(format!("127.0.0.1:{}", server_port))
        .await
        .unwrap();
    let server_handle = tokio::spawn(async move {
        if let Ok((mut stream, _)) = server_listener.accept().await {
            let mut buf = vec![0u8; 1024];
            if let Ok(n) = stream.read(&mut buf).await {
                if n > 0 {
                    let response = format!("ECHO: {}", String::from_utf8_lossy(&buf[..n]));
                    let _ = stream.write_all(response.as_bytes()).await;
                }
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let opts = PortForwardOpts::builder().target_addr("127.0.0.1").build();
    let handle = client
        .start_port_forward(&id, forward_port, server_port, opts)
        .unwrap();
    assert_eq!(handle.host_port(), forward_port);

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", forward_port))
        .await
        .unwrap();
    let message = "Hello through the port forward!";
    stream.write_all(message.as_bytes()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut response = vec![0u8; 1024];
    let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut response))
        .await
        .expect("read timed out")
        .expect("read failed");

    let response_str = String::from_utf8_lossy(&response[..n]);
    assert!(
        response_str.contains("ECHO:") && response_str.contains(message),
        "expected echo response, got: {}",
        response_str
    );

    drop(handle);
    drop(stream);
    let _ = tokio::time::timeout(Duration::from_secs(1), server_handle).await;
}

// ---------------------------------------------------------------------------
// stop / pause / unpause
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_stop_container() {
    let client = connect();
    let id = cid("stop-only-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "stop-only")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let info = client.inspect_container(&id).await.unwrap();
    assert_eq!(info.task.as_ref().unwrap().status, TaskStatus::Running);

    client.stop_container(&id, TIMEOUT).await.unwrap();

    let info = client.inspect_container(&id).await.unwrap();
    assert_eq!(info.id, id, "container record should survive stop");
    assert_eq!(
        info.task.as_ref().unwrap().status,
        TaskStatus::Stopped,
        "task should be Stopped after stop_container"
    );

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_pause_and_unpause() {
    let client = connect();
    let id = cid("pause-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "pause")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let info = client.inspect_container(&id).await.unwrap();
    assert_eq!(info.task.as_ref().unwrap().status, TaskStatus::Running);

    client.pause_container(&id).await.unwrap();
    let info = client.inspect_container(&id).await.unwrap();
    assert_eq!(info.task.as_ref().unwrap().status, TaskStatus::Paused);

    client.unpause_container(&id).await.unwrap();
    let info = client.inspect_container(&id).await.unwrap();
    assert_eq!(info.task.as_ref().unwrap().status, TaskStatus::Running);

    cleanup(&client, &id).await;
}

// ---------------------------------------------------------------------------
// wait_ready
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_wait_ready_process_running() {
    let client = connect();
    let id = cid("ready-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "ready")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    client
        .wait_ready(
            &id,
            Duration::from_secs(10),
            ReadinessStrategy::ProcessRunning,
        )
        .await
        .unwrap();

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_wait_ready_exec_strategy() {
    let client = connect();
    let id = cid("ready-exec-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "ready-exec")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let strategy = ReadinessStrategy::Exec(vec!["true".to_string()]);
    client
        .wait_ready(&id, Duration::from_secs(10), strategy)
        .await
        .unwrap();

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_wait_ready_timeout() {
    let client = connect();
    let id = cid("ready-timeout-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "ready-timeout")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let strategy = ReadinessStrategy::Exec(vec!["false".to_string()]);
    let result = client
        .wait_ready(&id, Duration::from_secs(2), strategy)
        .await;
    assert!(
        matches!(result, Err(Error::Timeout(_))),
        "expected Timeout, got {:?}",
        result
    );

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_wait_ready_image_healthcheck_fallback() {
    let client = connect();
    let id = cid("ready-imghc-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "ready-imghc")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    // busybox has no HEALTHCHECK → falls back to ProcessRunning.
    client
        .wait_ready(
            &id,
            Duration::from_secs(10),
            ReadinessStrategy::ImageHealthcheck,
        )
        .await
        .unwrap();

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_wait_ready_task_exited() {
    let client = connect();
    let id = cid("ready-exit-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "ready-exit")
        .cmd(["true"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    let result = client
        .wait_ready(
            &id,
            Duration::from_secs(5),
            ReadinessStrategy::ProcessRunning,
        )
        .await;
    assert!(
        matches!(result, Err(Error::TaskExited(_))),
        "expected TaskExited, got {:?}",
        result
    );

    cleanup(&client, &id).await;
}

#[tokio::test]
async fn test_wait_ready_tcp_port() {
    let client = connect();
    let id = cid("ready-tcp-test");
    let port = pick_free_host_port();

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let cmd = format!("nc -l -p {} & sleep 300", port);
    use containerd_manager::{NetworkMode, NetworkOpts};
    let opts = CreateContainerOpts::builder()
        .label("test", "ready-tcp")
        .network(NetworkOpts { mode: NetworkMode::Host })
        .cmd(["sh", "-c", &cmd]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    client
        .wait_ready(
            &id,
            Duration::from_secs(15),
            ReadinessStrategy::TcpPort(port),
        )
        .await
        .unwrap();

    cleanup(&client, &id).await;
}

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_start_nonexistent_container_errors() {
    let client = connect();
    let result = client.start_container(&cid("does-not-exist-xyz")).await;
    assert!(
        matches!(result, Err(Error::ContainerNotFound(_))),
        "expected ContainerNotFound, got {:?}",
        result
    );
}

// ---------------------------------------------------------------------------
// probe_health
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_probe_health_no_healthcheck() {
    let client = connect();
    let id = cid("probe-health-test");

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .label("test", "probe-health")
        .cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    let status = client.probe_health(&id).await.unwrap();
    assert_eq!(status, HealthStatus::NoHealthcheck);

    cleanup(&client, &id).await;
}

// ---------------------------------------------------------------------------
// image_healthcheck
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_image_healthcheck_busybox_has_none() {
    let client = connect();
    client.pull_image(BUSYBOX).await.unwrap();
    let hc = client.image_healthcheck(BUSYBOX).await.unwrap();
    assert!(hc.is_none(), "busybox has no HEALTHCHECK");
}

// ---------------------------------------------------------------------------
// Image metadata cache
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_image_cache_populates_on_create() {
    let client = connect();
    let id = cid("img-cache-test");

    cleanup(&client, &id).await;
    client.clear_image_cache();
    assert_eq!(client.image_cache_len(), 0);

    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder().cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    assert_eq!(
        client.image_cache_len(),
        1,
        "create_container should populate the metadata cache"
    );

    cleanup(&client, &id).await;
}

// ---------------------------------------------------------------------------
// Managed port forward cleanup
// ---------------------------------------------------------------------------

/// Declared port bindings (via .port_binding()) are stored as labels, auto-
/// started by start_container, visible via inspect, and cleaned up on delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_declared_port_binding_lifecycle() {
    let client = connect();
    let id = cid("declared-fwd-test");
    let host_port = pick_free_host_port();

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder()
        .cmd(["sleep", "300"])
        .port_binding(host_port, 8080).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();

    let info = client.inspect_container(&id).await.unwrap();
    assert!(
        info.port_forwards.iter().any(|&(_, cp)| cp == 8080),
        "declared binding should be visible via inspect before start; got {:?}",
        info.port_forwards
    );

    client.start_container(&id).await.unwrap();

    client.delete_container(&id, TIMEOUT).await.unwrap();

    assert!(
        std::net::TcpListener::bind(format!("127.0.0.1:{}", host_port)).is_ok(),
        "host port should be free after delete_container"
    );
}

/// Dynamic forwards (start_managed_port_forward) are also cleaned up on
/// delete_container even though they don't appear in inspect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dynamic_port_forward_dropped_on_delete() {
    let client = connect();
    let id = cid("dynamic-fwd-test");
    let host_port = pick_free_host_port();

    cleanup(&client, &id).await;
    client.pull_image(BUSYBOX).await.unwrap();

    let opts = CreateContainerOpts::builder().cmd(["sleep", "300"]).build();
    client.create_container(&id, BUSYBOX, opts).await.unwrap();
    client.start_container(&id).await.unwrap();

    client
        .start_managed_port_forward(&id, host_port, 8080, PortForwardOpts::default())
        .unwrap();

    let info = client.inspect_container(&id).await.unwrap();
    assert!(
        !info.port_forwards.iter().any(|&(h, _)| h == host_port),
        "dynamic forward should not appear in inspect (no label written)"
    );

    client.delete_container(&id, TIMEOUT).await.unwrap();

    assert!(
        std::net::TcpListener::bind(format!("127.0.0.1:{}", host_port)).is_ok(),
        "host port should be free after delete_container"
    );
}
