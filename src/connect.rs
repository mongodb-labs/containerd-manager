//! Synchronous connect to containerd.

use std::path::PathBuf;

use containerd_client::tonic::transport::Endpoint;
use tower::service_fn;

use crate::client::Client;
use crate::consts::SOCKET_ENV_VAR;
use crate::error::{Error, Result};

/// Precedence: explicit arg → `CONTAINERD_SOCKET` env var → platform default.
fn resolve_socket_path(socket_path: Option<PathBuf>) -> PathBuf {
    let env_val = std::env::var(SOCKET_ENV_VAR).ok();
    resolve_socket_path_with(socket_path, env_val.as_deref())
}

/// Pure version of `resolve_socket_path` for testing.
fn resolve_socket_path_with(socket_path: Option<PathBuf>, env_val: Option<&str>) -> PathBuf {
    if let Some(path) = socket_path {
        return path;
    }
    if let Some(val) = env_val {
        if !val.is_empty() {
            return PathBuf::from(val);
        }
    }
    default_socket_path()
}

fn default_socket_path() -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        if let Some(path) = find_colima_socket() {
            return path;
        }
        PathBuf::from("/var/run/containerd/containerd.sock")
    }
    #[cfg(not(target_os = "macos"))]
    {
        PathBuf::from("/run/containerd/containerd.sock")
    }
}

#[cfg(target_os = "macos")]
fn find_colima_socket() -> Option<PathBuf> {
    #[allow(deprecated)]
    let home = std::env::home_dir()?;
    find_colima_socket_in(&home.join(".colima"))
}

/// Scans `<colima_dir>/*/containerd.sock`. The `default` profile typically
/// hosts Docker and its containerd socket serves the daemon's internal
/// namespace, so non-default profiles are preferred; `default` is only used
/// as a fallback. Non-default candidates are sorted by profile name so the
/// selection is deterministic across runs (directory iteration order is
/// otherwise filesystem-dependent).
#[cfg(any(target_os = "macos", test))]
fn find_colima_socket_in(colima_dir: &std::path::Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(colima_dir).ok()?;
    let mut candidates: Vec<(String, PathBuf)> = entries
        .flatten()
        .filter(|e| e.path().metadata().map(|m| m.is_dir()).unwrap_or(false))
        .filter_map(|e| {
            let sock = e.path().join("containerd.sock");
            if !sock.exists() {
                return None;
            }
            let name = e.file_name().to_string_lossy().into_owned();
            Some((name, sock))
        })
        .collect();
    candidates.sort_by(|a, b| a.0.cmp(&b.0));

    let mut default_sock: Option<PathBuf> = None;
    for (name, sock) in candidates {
        if name == "default" {
            default_sock = Some(sock);
        } else {
            return Some(sock);
        }
    }
    default_sock
}

/// **Synchronous** (matches the shape of `Docker::connect_with_socket()`).
/// `socket_path = None` uses the platform default.
pub fn connect(socket_path: Option<PathBuf>) -> Result<Client> {
    // Windows native has no Unix-socket support in std, and containerd's
    // wire protocol is a UDS gRPC. WSL2 distros are real Linux from the
    // crate's perspective, so build inside WSL.
    #[cfg(not(unix))]
    {
        let _ = socket_path;
        return Err(Error::InvalidArgument(
            "containerd-manager requires a Unix-socket-capable target. \
             On Windows, build and run inside WSL2."
                .into(),
        ));
    }

    #[cfg(unix)]
    {
        let path = resolve_socket_path(socket_path);
        connect_unix(path)
    }
}

#[cfg(unix)]
fn connect_unix(path: PathBuf) -> Result<Client> {
    tracing::debug!(socket = %path.display(), "connect: resolving socket");
    if !path.exists() {
        return Err(Error::SocketNotFound { path });
    }

    // Eagerly probe the socket so we surface dead-socket errors before any
    // gRPC call.
    if let Err(e) = std::os::unix::net::UnixStream::connect(&path) {
        if e.kind() == std::io::ErrorKind::ConnectionRefused {
            return Err(Error::ConnectionRefused { path });
        } else {
            return Err(Error::Io {
                context: "connect socket",
                source: e,
            });
        }
    }

    let path_clone = path.clone();
    let channel = match Endpoint::try_from("http://[::]") {
        Ok(endpoint) => endpoint.connect_with_connector_lazy(service_fn(move |_| {
            let path = path_clone.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    tokio::net::UnixStream::connect(path).await?,
                ))
            }
        })),
        Err(e) => return Err(Error::Transport(e)),
    };

    let containerd_client = containerd_client::Client::from(channel);
    Ok(Client::from_parts(containerd_client))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_explicit_path_takes_precedence() {
        let path = resolve_socket_path_with(Some(PathBuf::from("/my/socket.sock")), None);
        assert_eq!(path, PathBuf::from("/my/socket.sock"));
    }

    #[test]
    fn resolve_env_var_overrides_default() {
        let path = resolve_socket_path_with(None, Some("/from/env.sock"));
        assert_eq!(path, PathBuf::from("/from/env.sock"));
    }

    #[test]
    fn resolve_empty_env_var_falls_through() {
        let path = resolve_socket_path_with(None, Some(""));
        assert_eq!(path, default_socket_path());
    }

    #[test]
    fn resolve_unset_env_var_falls_through() {
        let path = resolve_socket_path_with(None, None);
        assert_eq!(path, default_socket_path());
    }

    #[test]
    fn resolve_explicit_beats_env_var() {
        let path = resolve_socket_path_with(
            Some(PathBuf::from("/explicit.sock")),
            Some("/from/env.sock"),
        );
        assert_eq!(path, PathBuf::from("/explicit.sock"));
    }

    fn make_colima_profile(colima_dir: &std::path::Path, name: &str, with_sock: bool) {
        let profile_dir = colima_dir.join(name);
        std::fs::create_dir_all(&profile_dir).unwrap();
        if with_sock {
            // The socket is just a regular file in the test fixture - we only
            // ever check `.exists()`, never connect.
            std::fs::write(profile_dir.join("containerd.sock"), b"").unwrap();
        }
    }

    #[test]
    fn find_colima_socket_returns_none_for_missing_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist");
        assert!(find_colima_socket_in(&missing).is_none());
    }

    #[test]
    fn find_colima_socket_returns_none_when_empty() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(find_colima_socket_in(tmp.path()).is_none());
    }

    #[test]
    fn find_colima_socket_returns_none_when_no_profile_has_sock() {
        let tmp = tempfile::tempdir().unwrap();
        make_colima_profile(tmp.path(), "default", false);
        make_colima_profile(tmp.path(), "custom", false);
        assert!(find_colima_socket_in(tmp.path()).is_none());
    }

    #[test]
    fn find_colima_socket_prefers_non_default_profile() {
        let tmp = tempfile::tempdir().unwrap();
        make_colima_profile(tmp.path(), "default", true);
        make_colima_profile(tmp.path(), "containerd-dev", true);
        let sock = find_colima_socket_in(tmp.path()).unwrap();
        // Must NOT have picked the "default" profile.
        assert!(!sock.starts_with(tmp.path().join("default")));
        assert!(sock.ends_with("containerd.sock"));
    }

    #[test]
    fn find_colima_socket_falls_back_to_default_when_only_option() {
        let tmp = tempfile::tempdir().unwrap();
        make_colima_profile(tmp.path(), "default", true);
        let sock = find_colima_socket_in(tmp.path()).unwrap();
        assert_eq!(sock, tmp.path().join("default").join("containerd.sock"));
    }

    #[test]
    fn find_colima_socket_skips_profile_without_sock() {
        let tmp = tempfile::tempdir().unwrap();
        // "custom" has no sock - should be skipped. "default" should win as
        // the fallback even though it's the less-preferred name.
        make_colima_profile(tmp.path(), "custom", false);
        make_colima_profile(tmp.path(), "default", true);
        let sock = find_colima_socket_in(tmp.path()).unwrap();
        assert_eq!(sock, tmp.path().join("default").join("containerd.sock"));
    }

    // Windows returns InvalidArgument (no Unix-socket support); SocketNotFound
    // only applies to Unix targets.
    #[cfg(unix)]
    #[test]
    fn connect_socket_not_found() {
        let result = connect(Some(PathBuf::from("/path/to/nowhere.sock")));
        match result {
            Err(Error::SocketNotFound { path }) => {
                assert_eq!(path, PathBuf::from("/path/to/nowhere.sock"))
            }
            other => panic!(
                "Expected SocketNotFound error, got: {:?}",
                other.map(|_| "Client")
            ),
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn connect_happy_path() {
        use std::os::unix::net::UnixListener;
        use std::thread;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let sock_path = dir.path().join("containerd.sock");

        let sock_path_clone = sock_path.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        let handle = thread::spawn(move || {
            let listener = UnixListener::bind(&sock_path_clone).unwrap();
            tx.send(()).unwrap();
            let _ = listener.accept();
        });

        rx.recv().unwrap();

        let result = connect(Some(sock_path));
        assert!(
            result.is_ok(),
            "Expected connection to succeed, got: {:?}",
            result.err()
        );
        // No actual gRPC call here - the dummy listener doesn't speak gRPC.

        handle.join().unwrap();
    }

    /// Relies on Linux/macOS Unix-socket semantics: a socket file with no
    /// listener returns ECONNREFUSED on connect. Other Unixen may return
    /// ENOENT or similar; gate accordingly if the suite ever targets one.
    #[cfg(unix)]
    #[tokio::test]
    async fn connect_connection_refused() {
        use std::os::unix::net::UnixListener;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let sock_path = dir.path().join("containerd.sock");

        {
            // Bind to create the socket file, then drop. The socket file
            // remains but nobody is listening.
            let _listener = UnixListener::bind(&sock_path).unwrap();
        }

        let result = connect(Some(sock_path.clone()));
        match result {
            Err(Error::ConnectionRefused { path }) => assert_eq!(path, sock_path),
            other => panic!(
                "Expected ConnectionRefused error, got: {:?}",
                other.map(|_| "Client")
            ),
        }
    }
}
