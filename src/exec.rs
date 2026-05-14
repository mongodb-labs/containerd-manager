//! Execute a command inside a running task.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use containerd_client::services::v1::{
    DeleteProcessRequest, ExecProcessRequest, StartRequest, WaitRequest,
};
use oci_spec::runtime::ProcessBuilder;
use prost_types::Any;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::util::StatusExt;

static EXEC_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Default per-stream cap on captured output. A runaway process (megabytes/s
/// of stdout) would otherwise OOM the caller. 16 MiB comfortably holds
/// healthcheck mongosh output and most diagnostic commands; raise via
/// `ExecOpts::max_output_bytes` for log-dump-style use cases.
pub const DEFAULT_EXEC_MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, typed_builder::TypedBuilder)]
#[builder(doc)]
pub struct ExecOpts {
    /// Per-stream cap on captured bytes; reads beyond this are dropped and
    /// `truncated_stdout` / `truncated_stderr` flagged on the output.
    #[builder(default = DEFAULT_EXEC_MAX_OUTPUT_BYTES)]
    pub max_output_bytes: usize,
}

impl Default for ExecOpts {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Debug)]
pub struct ExecOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_code: i32,
    /// True if stdout was truncated at `ExecOpts::max_output_bytes`.
    pub truncated_stdout: bool,
    /// True if stderr was truncated at `ExecOpts::max_output_bytes`.
    pub truncated_stderr: bool,
}

impl ExecOutput {
    pub fn stdout_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }

    pub fn stderr_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stderr)
    }
}

fn generate_exec_id() -> String {
    let count = EXEC_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("exec-{}-{}", std::process::id(), count)
}

/// RAII cleanup for the per-exec I/O directory.
struct IoDir {
    path: PathBuf,
}

impl Drop for IoDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Uses `~/.containerd-manager/exec-io/` so the paths are visible from both
/// macOS host and the Colima Linux VM (the home dir is virtiofs-mounted but
/// `/var/folders` is not).
///
/// Regular files, not FIFOs: FIFO pipe semantics don't survive virtiofs. The
/// shim's `OpenFifo` helper opens pre-existing files without type checks, so
/// regular files work as a drop-in replacement.
fn create_io_dir() -> Result<(IoDir, PathBuf, PathBuf)> {
    #[allow(deprecated)] // std::env::home_dir re-stabilised in 1.86.
    let home = std::env::home_dir().ok_or_else(|| Error::Io {
        context: "home directory",
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "no home directory"),
    })?;
    let base = home.join(".containerd-manager").join("exec-io");
    std::fs::create_dir_all(&base).map_err(|e| Error::Io {
        context: "create exec-io base dir",
        source: e,
    })?;

    let dir = base.join(generate_exec_id());
    std::fs::create_dir_all(&dir).map_err(|e| Error::Io {
        context: "create exec-io dir",
        source: e,
    })?;

    let stdout_path = dir.join("stdout");
    let stderr_path = dir.join("stderr");

    std::fs::write(&stdout_path, b"").map_err(|e| Error::Io {
        context: "create exec stdout file",
        source: e,
    })?;
    std::fs::write(&stderr_path, b"").map_err(|e| Error::Io {
        context: "create exec stderr file",
        source: e,
    })?;

    Ok((IoDir { path: dir }, stdout_path, stderr_path))
}

fn build_exec_process_spec(command: &[&str], env: Vec<String>) -> Result<Any> {
    // Match primary-process capabilities so exec'd helpers (mongosh, healthcheck)
    // don't hit EACCES on paths the image restricts (e.g. /root at 0550).
    let capabilities = crate::container::default_exec_capabilities()?;
    let process = ProcessBuilder::default()
        .terminal(false)
        .args(command.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .env(env)
        .cwd("/")
        .capabilities(capabilities)
        .build()
        .map_err(|e| Error::InvalidArgument(format!("build process spec: {}", e)))?;

    let json = serde_json::to_vec(&process)
        .map_err(|e| Error::InvalidArgument(format!("serialize process spec: {}", e)))?;

    Ok(Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Process".to_string(),
        value: json,
    })
}

fn build_exec_request(
    container_id: &str,
    exec_id: &str,
    stdout_path: &Path,
    stderr_path: &Path,
    spec: Any,
) -> ExecProcessRequest {
    ExecProcessRequest {
        container_id: container_id.to_string(),
        stdin: String::new(),
        stdout: stdout_path.to_string_lossy().to_string(),
        stderr: stderr_path.to_string_lossy().to_string(),
        terminal: false,
        spec: Some(spec),
        exec_id: exec_id.to_string(),
    }
}

pub(crate) async fn exec(
    client: &Client,
    container_id: &str,
    command: &[&str],
) -> Result<ExecOutput> {
    exec_with_opts(client, container_id, command, ExecOpts::default()).await
}

pub(crate) async fn exec_with_opts(
    client: &Client,
    container_id: &str,
    command: &[&str],
    opts: ExecOpts,
) -> Result<ExecOutput> {
    if command.is_empty() {
        return Err(Error::InvalidArgument("command must not be empty".into()));
    }

    let exec_id = generate_exec_id();
    tracing::debug!(container_id, exec_id = %exec_id, cmd = ?command, "exec: begin");

    let (_io_guard, stdout_path, stderr_path) = create_io_dir()?;

    // Inherit container env; containerd doesn't auto-merge it into the exec
    // process spec, so healthcheck binaries would otherwise miss auth vars.
    let info = crate::inspect::inspect_container(client, container_id).await?;
    let env: Vec<String> = info.env.iter().map(|(k, v)| format!("{k}={v}")).collect();

    let spec = build_exec_process_spec(command, env)?;
    let exec_req = build_exec_request(container_id, &exec_id, &stdout_path, &stderr_path, spec);

    let mut tasks_client = client.tasks();
    tasks_client
        .exec(client.ns_req(exec_req))
        .await
        .map_err(|e| e.into_crate_error("exec_create"))?;

    tasks_client
        .start(client.ns_req(StartRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.clone(),
        }))
        .await
        .map_err(|e| e.into_crate_error("exec_start"))?;

    let wait_resp = tasks_client
        .wait(client.ns_req(WaitRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.clone(),
        }))
        .await
        .map_err(|e| e.into_crate_error("exec_wait"))?;

    let exit_code = wait_resp.into_inner().exit_status as i32;
    tracing::debug!(container_id, exec_id = %exec_id, exit_code, "exec: done");

    // Process has exited; output files are fully flushed.
    let (stdout, truncated_stdout) = read_capped(&stdout_path, opts.max_output_bytes).await?;
    let (stderr, truncated_stderr) = read_capped(&stderr_path, opts.max_output_bytes).await?;

    if let Err(e) = tasks_client
        .delete_process(client.ns_req(DeleteProcessRequest {
            container_id: container_id.to_string(),
            exec_id: exec_id.clone(),
        }))
        .await
    {
        // Not fatal — the process is already gone — but a leak of exec
        // records in containerd is observable here.
        tracing::warn!(container_id, exec_id = %exec_id, error = %e, "exec: delete_process failed");
    }

    Ok(ExecOutput {
        stdout,
        stderr,
        exit_code,
        truncated_stdout,
        truncated_stderr,
    })
}

/// Reads `path` up to `cap` bytes. Returns `(bytes, truncated)`. `truncated`
/// is true if the file was larger than `cap` (bytes beyond are dropped).
async fn read_capped(path: &Path, cap: usize) -> Result<(Vec<u8>, bool)> {
    use tokio::io::AsyncReadExt;
    let file = tokio::fs::File::open(path).await.map_err(|e| Error::Io {
        context: "read exec output: open",
        source: e,
    })?;
    let total_len = file
        .metadata()
        .await
        .map_err(|e| Error::Io {
            context: "read exec output: stat",
            source: e,
        })?
        .len();
    let cap_u64 = cap as u64;
    let truncated = total_len > cap_u64;
    let take = total_len.min(cap_u64);
    let mut buf = Vec::with_capacity(take as usize);
    file.take(take)
        .read_to_end(&mut buf)
        .await
        .map_err(|e| Error::Io {
            context: "read exec output",
            source: e,
        })?;
    Ok((buf, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_output_str_lossy_on_non_utf8() {
        // The only `ExecOutput` behavior worth a unit test: lossy UTF-8
        // decode preserves valid prefix + replaces invalid sequences.
        let output = ExecOutput {
            stdout: b"ok\xff\xfemore".to_vec(),
            stderr: Vec::new(),
            exit_code: 0,
            truncated_stdout: false,
            truncated_stderr: false,
        };
        let s = output.stdout_str();
        assert!(s.starts_with("ok"));
        assert!(s.ends_with("more"));
        assert!(s.contains('\u{FFFD}'));
    }

    #[tokio::test]
    async fn read_capped_truncates_oversize() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("big");
        std::fs::write(&path, vec![0u8; 1024]).unwrap();
        let (buf, truncated) = read_capped(&path, 100).await.unwrap();
        assert_eq!(buf.len(), 100);
        assert!(truncated);
    }

    #[tokio::test]
    async fn read_capped_passthrough_when_under_cap() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("small");
        std::fs::write(&path, b"hello").unwrap();
        let (buf, truncated) = read_capped(&path, 100).await.unwrap();
        assert_eq!(buf, b"hello");
        assert!(!truncated);
    }

    #[test]
    fn generate_exec_id_is_unique() {
        let id1 = generate_exec_id();
        let id2 = generate_exec_id();
        assert_ne!(id1, id2);
        assert!(id1.starts_with("exec-"));
        assert!(id2.starts_with("exec-"));
    }

    #[test]
    fn build_exec_process_spec_sets_args() {
        let any = build_exec_process_spec(&["echo", "hello"], vec![]).expect("should build spec");
        assert_eq!(
            any.type_url,
            "types.containerd.io/opencontainers/runtime-spec/1/Process"
        );
        assert!(!any.value.is_empty());

        let parsed: serde_json::Value =
            serde_json::from_slice(&any.value).expect("should be valid json");
        let args = parsed["args"].as_array().expect("should have args");
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], "echo");
        assert_eq!(args[1], "hello");
        assert_eq!(parsed["cwd"], "/");
    }

    #[test]
    fn build_exec_process_spec_single_arg() {
        let any = build_exec_process_spec(&["ls"], vec![]).expect("should build spec");
        let parsed: serde_json::Value = serde_json::from_slice(&any.value).unwrap();
        let args = parsed["args"].as_array().unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], "ls");
    }

    #[test]
    fn build_exec_request_sets_fields() {
        let spec = Any {
            type_url: "test".to_string(),
            value: vec![1, 2, 3],
        };
        let req = build_exec_request(
            "my-container",
            "exec-1",
            Path::new("/tmp/stdout"),
            Path::new("/tmp/stderr"),
            spec,
        );
        assert_eq!(req.container_id, "my-container");
        assert_eq!(req.exec_id, "exec-1");
        assert_eq!(req.stdout, "/tmp/stdout");
        assert_eq!(req.stderr, "/tmp/stderr");
        assert!(req.stdin.is_empty());
        assert!(!req.terminal);
        assert!(req.spec.is_some());
    }

    #[test]
    fn build_exec_request_empty_stdin() {
        let spec = Any {
            type_url: String::new(),
            value: Vec::new(),
        };
        let req = build_exec_request(
            "c1",
            "e1",
            Path::new("/a/stdout"),
            Path::new("/a/stderr"),
            spec,
        );
        assert!(req.stdin.is_empty());
    }

    #[test]
    fn create_io_dir_creates_both_files() {
        let (_guard, stdout, stderr) = create_io_dir().expect("should create io dir");
        assert!(stdout.exists());
        assert!(stderr.exists());
        assert!(stdout.is_file());
        assert!(stderr.is_file());
    }

    #[test]
    fn io_dir_guard_cleans_up_on_drop() {
        let dir_path;
        {
            let (guard, stdout, stderr) = create_io_dir().expect("should create io dir");
            dir_path = guard.path.clone();
            assert!(stdout.exists());
            assert!(stderr.exists());
        }
        assert!(!dir_path.exists(), "io dir should be cleaned up after drop");
    }
}
