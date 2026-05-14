//! Destructive opt-in tests that exercise the real colima auto-discovery
//! path. Each test starts a uniquely-named colima profile, verifies our
//! library finds and connects to it, and tears it down. Gated by the
//! `e2e-colima-profiles` feature so they don't run by default.
//!
//! Preconditions:
//!   * The `colima` CLI is installed and on PATH.
//!   * No colima profiles are currently running (these tests would otherwise
//!     pick the user's existing profile instead of the one they start).
//!
//! Each colima start takes 30-60s, so run with `--test-threads=1`. See
//! TESTING.md.
//!
//!   cargo test --features e2e-colima-profiles -- --test-threads=1 --nocapture
#![cfg(feature = "e2e-colima-profiles")]

use std::process::{Command, Output};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

/// Serialises real `colima start`s. Recover from poison so one failing test
/// doesn't make the rest panic with `PoisonError` and bury the real cause.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial_lock() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Cached precondition result. Same diagnostic on every test, instead of one
/// clear failure + N `PoisonError`s.
static PREFLIGHT: OnceLock<Result<(), String>> = OnceLock::new();

fn preflight() {
    let status = PREFLIGHT.get_or_init(check_env);
    if let Err(reason) = status {
        panic!(
            "e2e-colima-profiles preflight failed: {reason}\n\
             \n\
             needs: `colima` on PATH, and no colima profile currently running. \
             Stop any with: `colima stop --profile <name>`. See TESTING.md."
        );
    }
}

fn check_env() -> Result<(), String> {
    if Command::new("colima").arg("version").output().is_err() {
        return Err("`colima` CLI not on PATH".into());
    }
    let out = colima(&["list", "--json"]);
    let stdout = String::from_utf8_lossy(&out.stdout);
    for line in stdout.lines() {
        if line.contains("\"status\":\"Running\"") {
            return Err(format!("colima profile already running: {line}"));
        }
    }
    Ok(())
}

fn colima(args: &[&str]) -> Output {
    eprintln!("[colima_profiles] colima {}", args.join(" "));
    Command::new("colima")
        .args(args)
        .output()
        .expect("failed to invoke `colima` - is it installed and on PATH?")
}

fn delete_profile(profile: &str) {
    let _ = colima(&["stop", "--profile", profile]);
    let _ = colima(&["delete", "--profile", profile, "--force"]);
}

/// RAII guard that overrides `$HOME` for the current process and restores it
/// on drop. Both `colima` (child process, inherits env) and our own
/// `home::home_dir()` call read `$HOME` per invocation, so flipping it
/// here sandboxes everything to a tempdir.
struct HomeGuard {
    original: Option<std::ffi::OsString>,
}

impl HomeGuard {
    fn new(home: &std::path::Path) -> Self {
        let original = std::env::var_os("HOME");
        std::env::set_var("HOME", home);
        Self { original }
    }
}

impl Drop for HomeGuard {
    fn drop(&mut self) {
        match &self.original {
            Some(orig) => std::env::set_var("HOME", orig),
            None => std::env::remove_var("HOME"),
        }
    }
}

/// Wraps the body so cleanup runs even on panic.
fn with_profile<F: FnOnce() + std::panic::UnwindSafe>(profile: &str, body: F) {
    preflight();
    let _lock = serial_lock();

    // Make sure no stale state under our test name.
    delete_profile(profile);

    let start = colima(&[
        "start",
        "--profile",
        profile,
        "--runtime",
        "containerd",
        "--cpu",
        "2",
        "--memory",
        "2",
    ]);
    assert!(
        start.status.success(),
        "colima start --profile {} failed:\nstdout: {}\nstderr: {}",
        profile,
        String::from_utf8_lossy(&start.stdout),
        String::from_utf8_lossy(&start.stderr)
    );

    let result = std::panic::catch_unwind(body);

    delete_profile(profile);

    if let Err(p) = result {
        std::panic::resume_unwind(p);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Builds a runtime, enters its context (so `connect()`'s lazy gRPC channel
/// can construct a hyper IO), then hands the runtime to the body.
fn run_in_rt<F: FnOnce(&tokio::runtime::Runtime)>(body: F) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let _guard = rt.enter();
    body(&rt);
}

/// Auto-discovery picks a non-default profile and the library can talk to
/// the containerd inside it.
#[test]
fn discovers_non_default_profile_and_connects() {
    with_profile("cm-test-custom", || {
        run_in_rt(|rt| {
            let client = containerd_manager::connect(None)
                .expect("auto-discovery failed to find the colima socket");
            // server_version is the smallest RPC that proves we are talking
            // to a real containerd, not a stale socket file.
            let version = rt
                .block_on(client.server_version())
                .expect("server_version failed");
            assert!(!version.is_empty(), "got empty version");
        });
    });
}

/// Smoke that the library can do more than just `server_version` against a
/// real profile: pull a small image (twice - second hits the existence cache).
/// Catches anything the unit-test mocks miss in the pull → unpack path.
#[test]
fn end_to_end_pull_on_real_profile() {
    with_profile("cm-test-pull", || {
        run_in_rt(|rt| {
            let client = containerd_manager::connect(None)
                .expect("connect")
                .with_namespace("e2e-profile");

            rt.block_on(client.pull_image("docker.io/library/busybox:latest"))
                .expect("pull_image failed");
            rt.block_on(client.pull_image("docker.io/library/busybox:latest"))
                .expect("second pull_image failed");

            std::thread::sleep(Duration::from_millis(200));
        });
    });
}

/// When only a profile literally named "default" exists, auto-discovery
/// should still find it (documented fallback). Runs inside a `$HOME`
/// sandbox so the user's real `default` profile is untouched: colima
/// writes its state under `tempdir/.colima`, and our `home::home_dir()`
/// reads `$HOME` per call so it scans the same place.
#[test]
fn falls_back_to_default_profile_when_only_one() {
    preflight();
    let _lock = serial_lock();

    // /tmp keeps the path short - colima/lima append a long ssh-socket
    // suffix and macOS caps Unix socket paths at 104 chars. The system
    // tempdir (/var/folders/...) is already ~50 chars before our suffix
    // and blows the limit.
    let tmp = tempfile::Builder::new()
        .prefix("cm")
        .tempdir_in("/tmp")
        .expect("create tempdir");
    let _home = HomeGuard::new(tmp.path());

    let start = colima(&[
        "start",
        "--profile",
        "default",
        "--runtime",
        "containerd",
        "--cpu",
        "2",
        "--memory",
        "2",
    ]);
    assert!(
        start.status.success(),
        "colima start failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&start.stdout),
        String::from_utf8_lossy(&start.stderr)
    );

    let result = std::panic::catch_unwind(|| {
        run_in_rt(|rt| {
            let client = containerd_manager::connect(None)
                .expect("auto-discovery should fall back to `default`");
            let version = rt
                .block_on(client.server_version())
                .expect("server_version failed");
            assert!(!version.is_empty(), "got empty version");
        });
    });

    let _ = colima(&["stop", "--profile", "default"]);
    let _ = colima(&["delete", "--profile", "default", "--force"]);

    if let Err(p) = result {
        std::panic::resume_unwind(p);
    }
}
