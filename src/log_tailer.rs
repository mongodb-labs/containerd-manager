//! Background tailer: drains the shim's stdout/stderr files into a single
//! timestamped + rotated `events.log` per container.
//!
//! Spawned at `start_container`, aborted at `delete_container`. Requires a
//! tokio runtime active when [`LogTailer::start`] is called.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::oneshot;

use crate::error::{Error, Result};
use crate::logs::LogStream;
use crate::logs::{
    encode_record_into, events_path_for, log_paths_for, EVENTS_ROTATE_BYTES, MAX_ROTATED_FILES,
};

const TAILER_INTERVAL: Duration = Duration::from_millis(100);

/// Consecutive drain errors before the tailer gives up and exits. Without
/// this a disk-full / permissions failure would spin silently forever.
const MAX_CONSECUTIVE_ERRORS: u32 = 50;

/// Maximum bytes buffered while waiting for a newline. A container writing
/// MB of data with no `\n` would otherwise force unbounded `leftover` growth.
/// On overflow, the buffer is flushed as a single record so progress isn't
/// blocked by an absent terminator.
const MAX_LEFTOVER_BYTES: usize = 1 << 20; // 1 MB

/// Maximum bytes read from a raw stdout/stderr file in a single drain pass.
/// If the file grew by more than this between polls (e.g. paused tailer
/// resuming), we read in chunks instead of allocating one giant buffer.
const MAX_READ_CHUNK: u64 = 4 * 1024 * 1024;

/// Background tailer handle. Invariant: at most one tailer per
/// (namespace, container_id). The Client's `managed_tailers` DashMap
/// enforces this — inserting a new tailer drops any existing one, whose
/// `Drop` aborts the prior task. Two tailers writing the same events.log
/// would interleave records arbitrarily.
pub(crate) struct LogTailer {
    // `Option` so `stop()` can take ownership of `task` before its `Drop`
    // runs; otherwise `Drop` would abort the task we just awaited.
    stop_tx: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl LogTailer {
    /// Begins a tailer for `container_id`. Returns immediately; the
    /// background task takes responsibility for events.log + rotation.
    pub(crate) fn start(container_id: &str) -> Result<Self> {
        let (stdout_path, stderr_path) = log_paths_for(container_id)?;
        let events_path = events_path_for(container_id)?;
        let (stop_tx, stop_rx) = oneshot::channel();
        let task = tokio::spawn(tailer_loop(stdout_path, stderr_path, events_path, stop_rx));
        Ok(Self {
            stop_tx: Some(stop_tx),
            task: Some(task),
        })
    }

    /// Signals the tailer to drain once and exit. Returns when it's gone.
    /// Prefer this over relying on Drop when you need the final drain — Drop
    /// can't await so it just aborts mid-pass.
    pub(crate) async fn stop(mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        // After this, Drop runs with both fields = None: no abort.
    }
}

impl Drop for LogTailer {
    fn drop(&mut self) {
        // Abort-only; `stop().await` is the graceful path.
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Per-raw-stream state. Holds the open file handle so we don't reopen on
/// every poll; on truncation / NotFound the handle is dropped and reopened
/// on the next pass.
#[derive(Default)]
struct TailState {
    pos: u64,
    leftover: Vec<u8>,
    file: Option<fs::File>,
}

async fn tailer_loop(
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    events_path: PathBuf,
    mut stop_rx: oneshot::Receiver<()>,
) {
    let mut stdout_state = TailState::default();
    let mut stderr_state = TailState::default();
    // Per-stream counters; either hitting the cap aborts the tailer.
    let mut stdout_errors: u32 = 0;
    let mut stderr_errors: u32 = 0;

    loop {
        let r1 = drain_into_events(
            &stdout_path,
            LogStream::Stdout,
            &mut stdout_state,
            &events_path,
        )
        .await;
        let r2 = drain_into_events(
            &stderr_path,
            LogStream::Stderr,
            &mut stderr_state,
            &events_path,
        )
        .await;

        match &r1 {
            Ok(()) => stdout_errors = 0,
            Err(e) => {
                stdout_errors += 1;
                tracing::debug!(error = %e, ?events_path, stream = "stdout", "log tailer: drain error");
            }
        }
        match &r2 {
            Ok(()) => stderr_errors = 0,
            Err(e) => {
                stderr_errors += 1;
                tracing::debug!(error = %e, ?events_path, stream = "stderr", "log tailer: drain error");
            }
        }
        if stdout_errors >= MAX_CONSECUTIVE_ERRORS || stderr_errors >= MAX_CONSECUTIVE_ERRORS {
            tracing::error!(
                ?events_path,
                stdout_errors,
                stderr_errors,
                stdout_last_err = ?r1.err().map(|e| e.to_string()),
                stderr_last_err = ?r2.err().map(|e| e.to_string()),
                "log tailer aborting: persistent drain failures \
                 (followers will see no new records)"
            );
            return;
        }

        tokio::select! {
            _ = tokio::time::sleep(TAILER_INTERVAL) => {}
            _ = &mut stop_rx => {
                // Final drain after stop so writes between the signal and
                // shutdown still land in events.log.
                let _ = drain_into_events(&stdout_path, LogStream::Stdout, &mut stdout_state, &events_path).await;
                let _ = drain_into_events(&stderr_path, LogStream::Stderr, &mut stderr_state, &events_path).await;
                return;
            }
        }
    }
}

async fn ensure_file_open(state: &mut TailState, raw_path: &Path) -> Result<bool> {
    if state.file.is_some() {
        return Ok(true);
    }
    match fs::File::open(raw_path).await {
        Ok(f) => {
            state.file = Some(f);
            Ok(true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(Error::Io {
            context: "tailer: open raw",
            source: e,
        }),
    }
}

/// Reads new bytes from `raw_path` since `state.pos`, line-splits with the
/// state's leftover buffer, and appends each completed line to events.log
/// as a timestamped record. Holds a long-lived file handle in `state.file`;
/// drops + reopens it only on rotation/truncation.
async fn drain_into_events(
    raw_path: &Path,
    stream: LogStream,
    state: &mut TailState,
    events_path: &Path,
) -> Result<()> {
    if !ensure_file_open(state, raw_path).await? {
        return Ok(());
    }
    let file = state
        .file
        .as_mut()
        .expect("file present after ensure_file_open");

    let len = file
        .metadata()
        .await
        .map_err(|e| Error::Io {
            context: "tailer: stat raw",
            source: e,
        })?
        .len();

    // Truncation: reset position + drop the open handle so the next pass
    // reopens the file at offset 0.
    if len < state.pos {
        state.pos = 0;
        state.leftover.clear();
        state.file = None;
        return Ok(());
    }
    if len == state.pos {
        return Ok(());
    }

    file.seek(std::io::SeekFrom::Start(state.pos))
        .await
        .map_err(|e| Error::Io {
            context: "tailer: seek raw",
            source: e,
        })?;

    // Cap per-pass read; multi-pass catches gigabyte growth without OOM.
    let want = (len - state.pos).min(MAX_READ_CHUNK) as usize;
    let mut combined = std::mem::take(&mut state.leftover);
    combined.reserve(want);
    let read_n = (&mut *file)
        .take(want as u64)
        .read_to_end(&mut combined)
        .await
        .map_err(|e| Error::Io {
            context: "tailer: read raw",
            source: e,
        })?;
    state.pos += read_n as u64;

    // One timestamp per batch; matches 100ms tailer granularity and avoids
    // a syscall per line under bursty output.
    let batch_ts = SystemTime::now();
    let mut batch: Vec<u8> = Vec::new();
    let mut start = 0;
    for nl in memchr::memchr_iter(b'\n', &combined) {
        let line = &combined[start..=nl];
        encode_record_into(&mut batch, batch_ts, stream, line);
        start = nl + 1;
    }
    // Anything after the last newline is a partial line; save for next iter
    // unless it exceeds the leftover cap (then flush as one record).
    if start < combined.len() && (combined.len() - start) >= MAX_LEFTOVER_BYTES {
        encode_record_into(&mut batch, batch_ts, stream, &combined[start..]);
        combined.clear();
    } else if start < combined.len() {
        combined.drain(..start);
    } else {
        combined.clear();
    }
    state.leftover = combined;
    if !batch.is_empty() {
        append_batch(events_path, &batch).await?;
    }
    Ok(())
}

async fn append_batch(events_path: &Path, batch: &[u8]) -> Result<()> {
    rotate_if_needed(events_path, batch.len() as u64).await?;
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(events_path)
        .await
        .map_err(|e| Error::Io {
            context: "tailer: open events",
            source: e,
        })?;
    f.write_all(batch).await.map_err(|e| Error::Io {
        context: "tailer: write events",
        source: e,
    })?;
    Ok(())
}

/// If appending `incoming` bytes would push events.log past the rotation
/// threshold, shift the rotated files up by one and start a new events.log.
/// After the rename, immediately recreate events.log empty so a concurrent
/// follower poll observes a continuous file (no `NotFound` gap) — only the
/// inode changes.
async fn rotate_if_needed(events_path: &Path, incoming: u64) -> Result<()> {
    let current_len = match fs::metadata(events_path).await {
        Ok(m) => m.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => {
            return Err(Error::Io {
                context: "tailer: stat events",
                source: e,
            });
        }
    };
    if current_len.saturating_add(incoming) <= EVENTS_ROTATE_BYTES {
        return Ok(());
    }

    // Prune the oldest rotated file if present.
    let oldest = events_path.with_extension(format!("log.{MAX_ROTATED_FILES}"));
    let _ = fs::remove_file(&oldest).await;

    // Shift N..1 up by one. Rename failure = gap in chain; warn, continue.
    for n in (1..MAX_ROTATED_FILES).rev() {
        let from = events_path.with_extension(format!("log.{n}"));
        let to = events_path.with_extension(format!("log.{}", n + 1));
        if fs::try_exists(&from).await.unwrap_or(false) {
            if let Err(e) = fs::rename(&from, &to).await {
                tracing::warn!(?from, ?to, error = %e, "log rotation: rename failed; rotation chain may have a gap");
            }
        }
    }
    // events.log → .1, then recreate empty so followers see no NotFound gap.
    let one = events_path.with_extension("log.1");
    if fs::try_exists(events_path).await.unwrap_or(false) {
        if let Err(e) = fs::rename(events_path, &one).await {
            tracing::warn!(?events_path, ?one, error = %e, "log rotation: failed to rename events.log -> .1");
        }
    }
    let _ = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(events_path)
        .await
        .map_err(|e| Error::Io {
            context: "tailer: create new events after rotate",
            source: e,
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rotate_when_threshold_exceeded() {
        let dir = tempfile::tempdir().unwrap();
        let events = dir.path().join("events.log");
        std::fs::write(&events, vec![0u8; (EVENTS_ROTATE_BYTES - 100) as usize]).unwrap();
        rotate_if_needed(&events, 200).await.unwrap();
        // Rotation renames events.log -> events.log.1 and atomically recreates
        // events.log empty so a concurrent follower sees no NotFound gap.
        assert!(events.with_extension("log.1").exists());
        assert!(events.exists());
        assert_eq!(std::fs::metadata(&events).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn no_rotate_when_under_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let events = dir.path().join("events.log");
        std::fs::write(&events, b"small").unwrap();
        rotate_if_needed(&events, 100).await.unwrap();
        assert!(events.exists());
        assert!(!events.with_extension("log.1").exists());
    }

    #[tokio::test]
    async fn stop_drains_pending_writes() {
        let dir = tempfile::tempdir().unwrap();
        let stdout = dir.path().join("stdout");
        let stderr = dir.path().join("stderr");
        let events = dir.path().join("events.log");
        std::fs::write(&stdout, b"").unwrap();
        std::fs::write(&stderr, b"").unwrap();

        let (stop_tx, stop_rx) = oneshot::channel();
        let task = tokio::spawn(tailer_loop(
            stdout.clone(),
            stderr.clone(),
            events.clone(),
            stop_rx,
        ));

        // Write after the tailer starts but before signalling stop.
        tokio::time::sleep(Duration::from_millis(150)).await;
        std::fs::write(&stdout, b"first-line\n").unwrap();
        std::fs::write(&stderr, b"err-line\n").unwrap();
        // Wait for at least one poll cycle to pick them up before stop.
        tokio::time::sleep(Duration::from_millis(150)).await;
        // Append more right before stop; the final drain should still catch it.
        std::fs::write(&stdout, b"first-line\nsecond-line\n").unwrap();
        let _ = stop_tx.send(());
        let _ = task.await;

        let bytes = std::fs::read(&events).unwrap();
        let entries = crate::logs::parse_events_file_for_test(&bytes);
        let data_concat: Vec<u8> = entries
            .iter()
            .flat_map(|e| e.data.iter().copied())
            .collect();
        assert!(data_concat
            .windows(b"first-line\n".len())
            .any(|w| w == b"first-line\n"));
        assert!(data_concat
            .windows(b"second-line\n".len())
            .any(|w| w == b"second-line\n"));
    }

    #[tokio::test]
    async fn rotate_shifts_existing_files_up() {
        let dir = tempfile::tempdir().unwrap();
        let events = dir.path().join("events.log");
        std::fs::write(&events, vec![0u8; (EVENTS_ROTATE_BYTES) as usize]).unwrap();
        std::fs::write(events.with_extension("log.1"), b"older").unwrap();
        std::fs::write(events.with_extension("log.2"), b"oldest").unwrap();

        rotate_if_needed(&events, 1).await.unwrap();
        assert_eq!(
            std::fs::read(events.with_extension("log.2")).unwrap(),
            b"older"
        );
        assert_eq!(
            std::fs::read(events.with_extension("log.3")).unwrap(),
            b"oldest"
        );
    }
}
