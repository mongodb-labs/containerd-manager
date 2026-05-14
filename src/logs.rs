//! Task stdout/stderr log files.
//!
//! Layout under `~/.containerd-manager/task-logs/<container_id>/`:
//! - `stdout`, `stderr`: raw byte sinks the shim writes to
//! - `events.log`: timestamped + rotated events the tailer produces
//! - `events.log.1` .. `events.log.5`: rotated history
//!
//! At-observation timestamps (~100ms accuracy via the polling tailer; not
//! at-write. Document this for callers needing sub-tick precision).

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};

const FOLLOW_INTERVAL: Duration = Duration::from_millis(100);
const FOLLOW_CHANNEL_CAP: usize = 256;

/// Max bytes per events.log file before rotation. After rotation the oldest
/// rotated file is pruned if `MAX_ROTATED_FILES` is exceeded.
pub(crate) const EVENTS_ROTATE_BYTES: u64 = 10 * 1024 * 1024;
pub(crate) const MAX_ROTATED_FILES: usize = 5;

/// Hard cap on a single decoded record's payload. Anything larger is treated
/// as a malformed length prefix (otherwise a corrupt 4 GiB length would stall
/// the follower forever waiting for bytes that never arrive). The tailer's
/// own `MAX_LEFTOVER_BYTES` flush cap keeps real records well under this.
pub(crate) const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// Max bytes read in one rotated-file drain pass. Stops a malicious or
/// huge `.1` from OOM'ing the follower; the remainder will just be lost
/// (which is what we already accept for the multi-rotation-between-polls
/// case).
const ROTATED_DRAIN_MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub stream: LogStream,
    pub timestamp: SystemTime,
    /// Raw bytes of the log line (including any trailing `\n`). `Bytes` is
    /// refcounted so handing one entry to multiple consumers is cheap.
    pub data: bytes::Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// `~/.containerd-manager/task-logs/{container_id}/` - under `$HOME` so the
/// paths are visible from both the macOS host and the Colima VM.
pub(crate) fn log_dir_for(container_id: &str) -> Result<PathBuf> {
    #[allow(deprecated)] // std::env::home_dir is re-stabilised; warning is stale.
    let home = std::env::home_dir().ok_or_else(|| Error::Io {
        context: "home directory",
        source: std::io::Error::new(std::io::ErrorKind::NotFound, "no home directory"),
    })?;
    Ok(home
        .join(".containerd-manager")
        .join("task-logs")
        .join(container_id))
}

pub(crate) fn log_paths_for(container_id: &str) -> Result<(PathBuf, PathBuf)> {
    let dir = log_dir_for(container_id)?;
    Ok((dir.join("stdout"), dir.join("stderr")))
}

pub(crate) fn events_path_for(container_id: &str) -> Result<PathBuf> {
    Ok(log_dir_for(container_id)?.join("events.log"))
}

/// `events.log` + rotated `.1` .. `.N` in oldest-first read order.
fn events_files_oldest_first(events_path: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = (1..=MAX_ROTATED_FILES)
        .rev()
        .map(|n| events_path.with_extension(format!("log.{n}")))
        .filter(|p| p.exists())
        .collect();
    out.push(events_path.to_path_buf());
    out
}

/// Files must exist before task create so the shim can open them for writing.
/// Uses sync `std::fs` (sub-millisecond local-disk ops); callers from async
/// contexts can call directly without spawn_blocking.
pub(crate) fn prepare_log_files(container_id: &str) -> Result<(PathBuf, PathBuf)> {
    let (stdout_path, stderr_path) = log_paths_for(container_id)?;
    let dir = stdout_path.parent().ok_or_else(|| Error::Io {
        context: "log path has no parent directory",
        source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "no parent"),
    })?;
    std::fs::create_dir_all(dir).map_err(|e| Error::Io {
        context: "create task log dir",
        source: e,
    })?;
    std::fs::write(&stdout_path, b"").map_err(|e| Error::Io {
        context: "create task stdout log",
        source: e,
    })?;
    std::fs::write(&stderr_path, b"").map_err(|e| Error::Io {
        context: "create task stderr log",
        source: e,
    })?;
    Ok((stdout_path, stderr_path))
}

/// Sync best-effort `rm -rf`. Local-disk only; callers from async contexts
/// can call directly. Errors are swallowed (the dir may already be gone).
pub(crate) fn cleanup_log_files(container_id: &str) {
    if let Ok(dir) = log_dir_for(container_id) {
        let _ = std::fs::remove_dir_all(dir);
    }
}

/// Record format: `<unix_ms> <stream_char> <len> <data_bytes>\n`. The length
/// prefix lets us round-trip data bytes exactly — including embedded `\n`
/// and a trailing `\n` — and lets parsers find the next record boundary
/// without escaping. `stream_char` is `o` (stdout) or `e` (stderr).
#[cfg(test)]
pub(crate) fn encode_event(entry: &LogEntry) -> Vec<u8> {
    let mut buf = Vec::with_capacity(45 + entry.data.len());
    encode_record_into(&mut buf, entry.timestamp, entry.stream, &entry.data);
    buf
}

/// Appends one encoded record to `buf`. Takes raw fields (rather than a
/// `LogEntry` whose `Bytes` would force the tailer to copy on every line).
pub(crate) fn encode_record_into(
    buf: &mut Vec<u8>,
    timestamp: SystemTime,
    stream: LogStream,
    data: &[u8],
) {
    let ms: u64 = timestamp
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let stream_char = match stream {
        LogStream::Stdout => b'o',
        LogStream::Stderr => b'e',
    };
    buf.extend_from_slice(ms.to_string().as_bytes());
    buf.push(b' ');
    buf.push(stream_char);
    buf.push(b' ');
    buf.extend_from_slice(data.len().to_string().as_bytes());
    buf.push(b' ');
    buf.extend_from_slice(data);
    buf.push(b'\n');
}

/// Outcome of attempting to decode one record from a buffer.
enum Decoded {
    /// Record decoded; cursor should advance by `consumed` bytes.
    Record { entry: LogEntry, consumed: usize },
    /// Buffer ends mid-record; wait for more bytes.
    NeedMore,
    /// Header parse failed or the expected trailing newline is missing.
    /// Caller should resync by skipping past the next `\n`.
    Malformed,
}

/// Decodes one record starting at byte 0 of `buf`. Zero-copy: the entry's
/// `data` is a refcounted slice of `buf`, not a fresh allocation.
fn decode_event_with_len(buf: &Bytes) -> Decoded {
    let s = buf.as_ref();
    let Some(space1) = s.iter().position(|&b| b == b' ') else {
        return Decoded::NeedMore;
    };
    let Ok(ms_str) = std::str::from_utf8(&s[..space1]) else {
        return Decoded::Malformed;
    };
    let Ok(ms) = ms_str.parse::<u64>() else {
        return Decoded::Malformed;
    };

    let after_ms = &s[space1 + 1..];
    let Some(&stream_char) = after_ms.first() else {
        return Decoded::NeedMore;
    };
    if after_ms.len() < 2 {
        return Decoded::NeedMore;
    }
    if after_ms[1] != b' ' {
        return Decoded::Malformed;
    }
    let stream = match stream_char {
        b'o' => LogStream::Stdout,
        b'e' => LogStream::Stderr,
        _ => return Decoded::Malformed,
    };

    let after_stream = &after_ms[2..];
    let Some(space2) = after_stream.iter().position(|&b| b == b' ') else {
        return Decoded::NeedMore;
    };
    let Ok(len_str) = std::str::from_utf8(&after_stream[..space2]) else {
        return Decoded::Malformed;
    };
    let Ok(len) = len_str.parse::<usize>() else {
        return Decoded::Malformed;
    };
    if len > MAX_RECORD_BYTES {
        return Decoded::Malformed;
    }

    // Absolute offset of the data window inside `buf`.
    let data_abs_start = space1 + 1 + 2 + space2 + 1;
    let data_abs_end = data_abs_start + len;
    if s.len() < data_abs_end + 1 {
        return Decoded::NeedMore;
    }
    if s[data_abs_end] != b'\n' {
        return Decoded::Malformed;
    }

    let data = buf.slice(data_abs_start..data_abs_end);
    Decoded::Record {
        entry: LogEntry {
            stream,
            timestamp: UNIX_EPOCH + Duration::from_millis(ms),
            data,
        },
        consumed: data_abs_end + 1,
    }
}

/// On a malformed record at `cursor`, advance past the next `\n` so the
/// next decode attempt aligns with a record boundary. Returns the number
/// of bytes to skip from `cursor`, or `None` if no terminator exists in the
/// remaining buffer (caller should stop here and wait for more bytes — the
/// "malformed" tail might just be a partial without a terminator yet).
fn resync_offset(buf: &[u8]) -> Option<usize> {
    buf.iter().position(|&b| b == b'\n').map(|i| i + 1)
}

#[cfg(test)]
pub(crate) fn parse_events_file_for_test(bytes: &[u8]) -> Vec<LogEntry> {
    parse_events_file(Bytes::copy_from_slice(bytes))
}

/// Parses the contents of an events file into LogEntries (oldest first).
/// On a malformed record, logs a warning and resyncs to the next `\n`.
fn parse_events_file(bytes: Bytes) -> Vec<LogEntry> {
    let mut out = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        let view = bytes.slice(cursor..);
        match decode_event_with_len(&view) {
            Decoded::Record { entry, consumed } => {
                cursor += consumed;
                out.push(entry);
            }
            Decoded::NeedMore => break,
            Decoded::Malformed => {
                tracing::warn!(offset = cursor, "events log: malformed record, resyncing");
                match resync_offset(&bytes[cursor..]) {
                    Some(skip) => cursor += skip,
                    None => break,
                }
            }
        }
    }
    out
}

fn read_file_or_empty(path: &Path) -> Result<Bytes> {
    match std::fs::read(path) {
        Ok(data) => Ok(Bytes::from(data)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Bytes::new()),
        Err(e) => Err(Error::Io {
            context: "read task log file",
            source: e,
        }),
    }
}

/// Filter applied to `container_logs_filtered`. All fields are `None` by
/// default (no filtering).
#[derive(Debug, Clone, Default, typed_builder::TypedBuilder)]
#[builder(doc)]
pub struct LogsFilter {
    /// Keep only the last N entries after `since` + `until` apply.
    #[builder(default, setter(strip_option))]
    pub tail: Option<usize>,
    /// Drop entries with `timestamp < since`.
    #[builder(default, setter(strip_option))]
    pub since: Option<SystemTime>,
    /// Drop entries with `timestamp > until`.
    #[builder(default, setter(strip_option))]
    pub until: Option<SystemTime>,
}


/// Returns all events in chronological order (timestamps come from the
/// tailer's observation time; see module docs for precision caveats).
pub(crate) fn container_logs(container_id: &str) -> Result<Vec<LogEntry>> {
    container_logs_filtered(container_id, &LogsFilter::default())
}

pub(crate) fn container_logs_filtered(
    container_id: &str,
    filter: &LogsFilter,
) -> Result<Vec<LogEntry>> {
    let events_path = events_path_for(container_id)?;
    let mut all = Vec::new();
    for f in events_files_oldest_first(&events_path) {
        let bytes = read_file_or_empty(&f)?;
        all.extend(parse_events_file(bytes));
    }

    if let Some(since) = filter.since {
        all.retain(|e| e.timestamp >= since);
    }
    if let Some(until) = filter.until {
        all.retain(|e| e.timestamp <= until);
    }
    if let Some(n) = filter.tail {
        if all.len() > n {
            let skip = all.len() - n;
            all.drain(..skip);
        }
    }
    Ok(all)
}

/// Async follower that yields log entries as the shim writes them. Behaves
/// like `docker logs -f`. The follower polls the on-disk log files every
/// [`FOLLOW_INTERVAL`] - `notify`-style fs events are unreliable across the
/// virtiofs boundary between the macOS host and the Colima VM, so polling
/// is the only consistently-correct option here.
///
/// Drop to stop following.
pub struct LogFollower {
    rx: mpsc::Receiver<Result<LogEntry>>,
    task: tokio::task::JoinHandle<()>,
}

impl LogFollower {
    /// Yields the next entry, or `None` when the background task ends
    /// (stop signal fired, transient I/O error, or the follower was dropped).
    ///
    /// `LogFollower` also implements [`futures_core::Stream`], which is the
    /// preferred API for composing with stream combinators (`filter_map`,
    /// `take_while`, etc.). `recv` is a convenience for ad-hoc awaiting.
    pub async fn recv(&mut self) -> Option<Result<LogEntry>> {
        self.rx.recv().await
    }

    /// Spawn an unbounded follower over events.log. Lives until dropped or
    /// fatal I/O error.
    pub(crate) fn start(events_path: PathBuf) -> Self {
        Self::start_inner(events_path, None)
    }

    /// Spawn a follower that ends when `stop` fires. After receiving the
    /// stop signal the task does one final drain so events written between
    /// the signal and shutdown are still delivered.
    pub(crate) fn start_with_stop(
        events_path: PathBuf,
        stop: oneshot::Receiver<()>,
    ) -> Self {
        Self::start_inner(events_path, Some(stop))
    }

    fn start_inner(events_path: PathBuf, stop: Option<oneshot::Receiver<()>>) -> Self {
        let (tx, rx) = mpsc::channel(FOLLOW_CHANNEL_CAP);
        let task = tokio::spawn(follow_loop(events_path, tx, stop));
        Self { rx, task }
    }
}

impl Drop for LogFollower {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl futures_core::Stream for LogFollower {
    type Item = Result<LogEntry>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Per-file tail state for events.log polling. `inode` (Unix) lets us
/// detect rotation: when the tailer renames events.log -> events.log.1,
/// the new events.log has a different inode and we restart from position 0.
#[derive(Default)]
struct EventsTailState {
    pos: u64,
    /// Partial trailing record bytes from the previous poll (incomplete
    /// length-prefixed record). Prepended to the next read.
    leftover: Vec<u8>,
    /// File identity (Unix inode) of the open events.log. `None` until
    /// we've successfully opened the file once.
    inode: Option<u64>,
}

/// On rotation, the bytes appended to the OLD events.log between our last
/// read and the rename moment now live in events.log.1. Read from
/// `prev_pos` to its end and emit any complete records so they're not
/// dropped. Caller must verify inode match before invoking; the inode
/// check here is a second guard against picking up an unrelated file.
async fn drain_rotated_remainder(
    rotated_path: &Path,
    prev_inode: u64,
    prev_pos: u64,
    tx: &mpsc::Sender<Result<LogEntry>>,
) -> PollOutcome {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = match tokio::fs::File::open(rotated_path).await {
        Ok(f) => f,
        // Rotation might delete the oldest file or rotation just hasn't
        // produced a .1 (e.g. truncation rather than rename). Either way,
        // there's nothing to drain.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return PollOutcome::Continue,
        Err(e) => {
            return PollOutcome::Fatal(Error::Io {
                context: "follow: open rotated events",
                source: e,
            });
        }
    };

    let meta = match file.metadata().await {
        Ok(m) => m,
        Err(e) => {
            return PollOutcome::Fatal(Error::Io {
                context: "follow: stat rotated events",
                source: e,
            });
        }
    };
    if file_inode(&meta) != prev_inode {
        // The .1 file is from an earlier rotation cycle, not ours; the
        // remainder is genuinely lost.
        return PollOutcome::Continue;
    }
    let end = meta.len();
    if end <= prev_pos {
        return PollOutcome::Continue;
    }

    if let Err(e) = file.seek(std::io::SeekFrom::Start(prev_pos)).await {
        return PollOutcome::Fatal(Error::Io {
            context: "follow: seek rotated events",
            source: e,
        });
    }
    // Cap drain size; a malicious or huge .1 could otherwise OOM us.
    let want = (end - prev_pos).min(ROTATED_DRAIN_MAX_BYTES as u64);
    if want < (end - prev_pos) {
        tracing::warn!(
            ?rotated_path,
            drained = want,
            skipped = end - prev_pos - want,
            "rotated remainder larger than cap; tail records will be lost"
        );
    }
    let mut buf = Vec::with_capacity(want as usize);
    if let Err(e) = (&mut file).take(want).read_to_end(&mut buf).await {
        return PollOutcome::Fatal(Error::Io {
            context: "follow: read rotated events",
            source: e,
        });
    }

    let bytes = Bytes::from(buf);
    for entry in parse_events_file(bytes) {
        if tx.send(Ok(entry)).await.is_err() {
            return PollOutcome::ReceiverDropped;
        }
    }
    PollOutcome::Continue
}

async fn follow_loop(
    events_path: PathBuf,
    tx: mpsc::Sender<Result<LogEntry>>,
    stop: Option<oneshot::Receiver<()>>,
) {
    let mut state = EventsTailState::default();
    let mut stop = stop;

    loop {
        match poll_events_once(&events_path, &mut state, &tx).await {
            PollOutcome::Continue => {}
            PollOutcome::ReceiverDropped => return,
            PollOutcome::Fatal(e) => {
                let _ = tx.send(Err(e)).await;
                return;
            }
        }

        // Sleep until the next poll. On stop: drain once more so events
        // written between the signal and shutdown still get delivered.
        match stop.as_mut() {
            Some(rx) => {
                tokio::select! {
                    _ = tokio::time::sleep(FOLLOW_INTERVAL) => {}
                    _ = rx => {
                        let _ = poll_events_once(&events_path, &mut state, &tx).await;
                        return;
                    }
                }
            }
            None => tokio::time::sleep(FOLLOW_INTERVAL).await,
        }
    }
}

enum PollOutcome {
    Continue,
    ReceiverDropped,
    Fatal(Error),
}

#[cfg(unix)]
fn file_inode(meta: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    meta.ino()
}

#[cfg(not(unix))]
fn file_inode(_meta: &std::fs::Metadata) -> u64 {
    0 // No rotation-via-inode detection on non-Unix; truncation check still applies.
}

/// Reads new records from events.log since `state.pos`. Handles tailer
/// rotation: when the file's inode changes (rename + create), state resets
/// to read the new file from the start. Records spanning the read window
/// are deferred to the next poll via `state.leftover`.
///
/// Rotation caveat: bytes appended to the OLD events.log between our last
/// read and the rotation moment are missed by this follower (they live in
/// events.log.1 now). `container_logs_filtered` reads the rotated files in
/// chronological order if a complete history is needed.
async fn poll_events_once(
    path: &Path,
    state: &mut EventsTailState,
    tx: &mpsc::Sender<Result<LogEntry>>,
) -> PollOutcome {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};

    let mut file = match tokio::fs::File::open(path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Keep state.inode through transient rotation NotFound so the next
            // open triggers rotation-drain. Reset only on first-poll/deleted.
            if state.inode.is_none() {
                state.pos = 0;
                state.leftover.clear();
            }
            return PollOutcome::Continue;
        }
        Err(e) => {
            return PollOutcome::Fatal(Error::Io {
                context: "follow: open events",
                source: e,
            });
        }
    };

    let meta = match file.metadata().await {
        Ok(m) => m,
        Err(e) => {
            return PollOutcome::Fatal(Error::Io {
                context: "follow: stat events",
                source: e,
            });
        }
    };
    let len = meta.len();
    let inode = file_inode(&meta);

    let rotated = state.inode.is_some_and(|prev| prev != inode);
    let truncated = len < state.pos;

    if rotated {
        // events.log.1 may hold bytes appended between our last poll and
        // the rename — drain before resetting to new file.
        if let Some(prev_inode) = state.inode {
            let prev_pos = state.pos;
            let rotated_path = path.with_extension("log.1");
            match drain_rotated_remainder(&rotated_path, prev_inode, prev_pos, tx).await {
                PollOutcome::Continue => {}
                other => return other,
            }
        }
        state.pos = 0;
        state.leftover.clear();
    } else if truncated {
        tracing::warn!(
            ?path,
            new_len = len,
            old_pos = state.pos,
            leftover = state.leftover.len(),
            "events log truncated; dropping partial leftover and resuming from 0"
        );
        state.pos = 0;
        state.leftover.clear();
    }
    state.inode = Some(inode);

    if len == state.pos {
        return PollOutcome::Continue;
    }

    if let Err(e) = file.seek(std::io::SeekFrom::Start(state.pos)).await {
        return PollOutcome::Fatal(Error::Io {
            context: "follow: seek events",
            source: e,
        });
    }
    // Reuse the leftover Vec's allocation: it already holds the partial
    // record bytes; append the freshly-read bytes after them.
    let mut combined = std::mem::take(&mut state.leftover);
    combined.reserve((len - state.pos) as usize);
    if let Err(e) = file.read_to_end(&mut combined).await {
        return PollOutcome::Fatal(Error::Io {
            context: "follow: read events",
            source: e,
        });
    }
    state.pos = len;

    let bytes = Bytes::from(combined);
    let mut cursor = 0;
    while cursor < bytes.len() {
        let view = bytes.slice(cursor..);
        match decode_event_with_len(&view) {
            Decoded::Record { entry, consumed } => {
                if tx.send(Ok(entry)).await.is_err() {
                    return PollOutcome::ReceiverDropped;
                }
                cursor += consumed;
            }
            Decoded::NeedMore => break,
            Decoded::Malformed => {
                tracing::warn!(
                    ?path,
                    offset = cursor,
                    "events log: malformed record at follower cursor, resyncing"
                );
                match resync_offset(&bytes[cursor..]) {
                    Some(skip) => cursor += skip,
                    None => break,
                }
            }
        }
    }
    // Stuck-leftover guard: oversize without terminator = garbage; discard.
    let remainder = &bytes[cursor..];
    if remainder.len() > MAX_RECORD_BYTES && !remainder.contains(&b'\n') {
        tracing::warn!(
            ?path,
            stuck = remainder.len(),
            "follower leftover stuck (>{MAX_RECORD_BYTES} bytes, no terminator); discarding"
        );
        state.leftover = Vec::new();
    } else {
        // Channel-held Bytes slices prevent try_into_mut; reallocate.
        state.leftover = remainder.to_vec();
    }
    PollOutcome::Continue
}


#[cfg(test)]
mod tests {
    use super::*;

    fn decode_one(buf: &[u8]) -> (LogEntry, usize) {
        match decode_event_with_len(&Bytes::copy_from_slice(buf)) {
            Decoded::Record { entry, consumed } => (entry, consumed),
            Decoded::NeedMore => panic!("expected complete record, got NeedMore"),
            Decoded::Malformed => panic!("expected complete record, got Malformed"),
        }
    }

    #[test]
    fn encode_decode_roundtrip_with_trailing_newline() {
        let entry = LogEntry {
            stream: LogStream::Stdout,
            timestamp: UNIX_EPOCH + Duration::from_millis(1_700_000_000_000),
            data: b"hello\n".as_slice().into(),
        };
        let encoded = encode_event(&entry);
        let (decoded, n) = decode_one(&encoded);
        assert_eq!(n, encoded.len());
        assert_eq!(decoded.stream, LogStream::Stdout);
        assert_eq!(decoded.timestamp, entry.timestamp);
        assert_eq!(decoded.data.as_ref(), b"hello\n");
    }

    #[test]
    fn encode_decode_roundtrip_no_trailing_newline() {
        let entry = LogEntry {
            stream: LogStream::Stderr,
            timestamp: UNIX_EPOCH + Duration::from_millis(42),
            data: b"partial".as_slice().into(),
        };
        let encoded = encode_event(&entry);
        let (decoded, _) = decode_one(&encoded);
        assert_eq!(decoded.data.as_ref(), b"partial");
    }

    #[test]
    fn encode_decode_roundtrip_embedded_newlines_and_binary() {
        let entry = LogEntry {
            stream: LogStream::Stdout,
            timestamp: UNIX_EPOCH + Duration::from_millis(1),
            data: b"line1\nline2\n\x00\xff".as_slice().into(),
        };
        let encoded = encode_event(&entry);
        let (decoded, _) = decode_one(&encoded);
        assert_eq!(decoded.data.as_ref(), b"line1\nline2\n\x00\xff");
    }

    #[test]
    fn decode_rejects_oversize_length_prefix() {
        // Synthesise a record with an outrageous length field. Decoder must
        // classify this as Malformed (not "wait for more bytes").
        let mut buf = b"1 o ".to_vec();
        buf.extend_from_slice((MAX_RECORD_BYTES + 1).to_string().as_bytes());
        buf.extend_from_slice(b" payload\n");
        match decode_event_with_len(&Bytes::copy_from_slice(&buf)) {
            Decoded::Malformed => {}
            other => panic!("expected Malformed, got {}", match other {
                Decoded::Record { .. } => "Record",
                Decoded::NeedMore => "NeedMore",
                Decoded::Malformed => unreachable!(),
            }),
        }
    }

    #[test]
    fn decode_classifies_bad_stream_char_as_malformed() {
        let buf = b"1 x 5 hello\n";
        assert!(matches!(
            decode_event_with_len(&Bytes::copy_from_slice(buf)),
            Decoded::Malformed
        ));
    }

    #[test]
    fn parse_events_file_yields_entries_in_order() {
        let now = UNIX_EPOCH + Duration::from_millis(1_700_000_000_000);
        let e1 = encode_event(&LogEntry {
            stream: LogStream::Stdout,
            timestamp: now,
            data: b"one\n".as_slice().into(),
        });
        let e2 = encode_event(&LogEntry {
            stream: LogStream::Stderr,
            timestamp: now + Duration::from_millis(1),
            data: b"two\n".as_slice().into(),
        });
        let mut buf = e1;
        buf.extend(e2);
        let entries = parse_events_file(Bytes::from(buf));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].stream, LogStream::Stdout);
        assert_eq!(entries[1].stream, LogStream::Stderr);
        assert_eq!(entries[0].data.as_ref(), b"one\n");
        assert_eq!(entries[1].data.as_ref(), b"two\n");
    }

    #[test]
    fn parse_events_file_stops_at_truncated_tail() {
        let entry = LogEntry {
            stream: LogStream::Stdout,
            timestamp: UNIX_EPOCH + Duration::from_millis(1),
            data: b"good\n".as_slice().into(),
        };
        let mut buf = encode_event(&entry);
        buf.extend_from_slice(b"123 o 99 trunc"); // truncated record after valid one
        let entries = parse_events_file(Bytes::from(buf));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data.as_ref(), b"good\n");
    }

    #[test]
    fn parse_events_file_resyncs_past_malformed_record() {
        let entry = LogEntry {
            stream: LogStream::Stdout,
            timestamp: UNIX_EPOCH + Duration::from_millis(1),
            data: b"good\n".as_slice().into(),
        };
        let mut buf = Vec::new();
        // Garbage record terminated by a newline; resync should skip past it.
        buf.extend_from_slice(b"garbage with bad header\n");
        buf.extend(encode_event(&entry));
        let entries = parse_events_file(Bytes::from(buf));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].data.as_ref(), b"good\n");
    }

    #[test]
    fn log_paths_for_returns_correct_structure() {
        // Trailing components are stable user-visible paths; couple to those
        // not to the full prefix so `$HOME` layout changes don't break us.
        let (stdout, stderr) = log_paths_for("test-container").unwrap();
        assert!(stdout.ends_with("task-logs/test-container/stdout"));
        assert!(stderr.ends_with("task-logs/test-container/stderr"));
    }

    #[test]
    fn prepare_and_cleanup_log_files() {
        let container_id = format!("test-logs-{}", std::process::id());
        let (stdout, stderr) = prepare_log_files(&container_id).unwrap();
        assert!(stdout.exists());
        assert!(stderr.exists());

        cleanup_log_files(&container_id);
        assert!(!stdout.exists());
        assert!(!stderr.exists());
    }

    // ---- LogFollower ----

    use std::io::Write;
    use std::time::Duration;

    /// Sets up a tempdir + an empty events.log, then spawns a follower on it.
    fn fixture_follower() -> (LogFollower, std::path::PathBuf, tempfile::TempDir) {
        let tmp = tempfile::Builder::new().prefix("cmlog").tempdir().unwrap();
        let events = tmp.path().join("events.log");
        let f = LogFollower::start(events.clone());
        (f, events, tmp)
    }

    /// Appends one encoded record for `data` (no trailing \n stripped) to
    /// events.log, simulating what the tailer writes.
    fn append_record(events: &std::path::Path, stream: LogStream, ts_ms: u64, data: &[u8]) {
        let entry = LogEntry {
            stream,
            timestamp: UNIX_EPOCH + Duration::from_millis(ts_ms),
            data: bytes::Bytes::copy_from_slice(data),
        };
        let bytes = encode_event(&entry);
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(events)
            .unwrap();
        f.write_all(&bytes).unwrap();
    }

    /// Receive next entry with a timeout so a stuck poll loop fails the test
    /// rather than hanging.
    async fn recv_within(f: &mut LogFollower, timeout: Duration) -> Option<Result<LogEntry>> {
        tokio::time::timeout(timeout, f.recv()).await.ok().flatten()
    }

    #[tokio::test]
    async fn follower_reads_existing_records() {
        let (mut f, events, _tmp) = fixture_follower();
        append_record(&events, LogStream::Stdout, 1, b"first\n");
        append_record(&events, LogStream::Stdout, 2, b"second\n");

        let one = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        let two = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(one.stream, LogStream::Stdout);
        assert_eq!(one.data.as_ref(), b"first\n");
        assert_eq!(two.data.as_ref(), b"second\n");
    }

    #[tokio::test]
    async fn follower_picks_up_records_after_start() {
        let (mut f, events, _tmp) = fixture_follower();
        tokio::time::sleep(Duration::from_millis(150)).await;

        append_record(&events, LogStream::Stdout, 1, b"later\n");
        let entry = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.data.as_ref(), b"later\n");
    }

    #[tokio::test]
    async fn follower_defers_partial_record_across_polls() {
        // Write half a record's bytes, sleep past one poll, then complete it.
        let tmp = tempfile::Builder::new().prefix("cmlog").tempdir().unwrap();
        let events = tmp.path().join("events.log");

        let full = encode_event(&LogEntry {
            stream: LogStream::Stdout,
            timestamp: UNIX_EPOCH + Duration::from_millis(1),
            data: b"hello\n".as_slice().into(),
        });
        let split = full.len() / 2;
        std::fs::write(&events, &full[..split]).unwrap();

        let mut f = LogFollower::start(events.clone());
        tokio::time::sleep(Duration::from_millis(150)).await;

        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&events)
            .unwrap();
        file.write_all(&full[split..]).unwrap();
        drop(file);

        let entry = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.data.as_ref(), b"hello\n");
    }

    #[tokio::test]
    async fn follower_emits_stdout_and_stderr_in_record_order() {
        let (mut f, events, _tmp) = fixture_follower();
        append_record(&events, LogStream::Stdout, 1, b"out\n");
        append_record(&events, LogStream::Stderr, 2, b"err\n");

        let a = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        let b = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(a.stream, LogStream::Stdout);
        assert_eq!(b.stream, LogStream::Stderr);
    }

    #[tokio::test]
    async fn follower_drop_aborts_task() {
        let (f, _events, _tmp) = fixture_follower();
        let handle = f.task.abort_handle();
        drop(f);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            handle.is_finished(),
            "follower task should be aborted after drop"
        );
    }

    #[tokio::test]
    async fn follower_handles_missing_events_then_appears() {
        let tmp = tempfile::Builder::new().prefix("cmlog").tempdir().unwrap();
        let events = tmp.path().join("events.log");
        let mut f = LogFollower::start(events.clone());

        // File doesn't exist yet; follower idles.
        tokio::time::sleep(Duration::from_millis(150)).await;

        append_record(&events, LogStream::Stdout, 1, b"appeared\n");
        let entry = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.data.as_ref(), b"appeared\n");
    }

    #[tokio::test]
    async fn follower_with_stop_signal_drains_then_ends() {
        let tmp = tempfile::Builder::new().prefix("cmlog").tempdir().unwrap();
        let events = tmp.path().join("events.log");
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let mut f = LogFollower::start_with_stop(events.clone(), stop_rx);

        append_record(&events, LogStream::Stdout, 1, b"before-stop\n");
        let entry = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(entry.data.as_ref(), b"before-stop\n");

        append_record(&events, LogStream::Stdout, 2, b"after-stop\n");
        let _ = stop_tx.send(());

        let second = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.data.as_ref(), b"after-stop\n");

        // Channel closes once the task exits.
        assert!(recv_within(&mut f, Duration::from_secs(2)).await.is_none());
    }

    #[tokio::test]
    async fn follower_resets_on_rotation() {
        // Write a record, observe it, then simulate rotation (rename events.log,
        // create a new one with different content). Follower should pick up
        // records from the new file from byte 0.
        let tmp = tempfile::Builder::new().prefix("cmlog").tempdir().unwrap();
        let events = tmp.path().join("events.log");
        append_record(&events, LogStream::Stdout, 1, b"old\n");

        let mut f = LogFollower::start(events.clone());
        let first = recv_within(&mut f, Duration::from_secs(2))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.data.as_ref(), b"old\n");

        // Rotate: rename + create new (different inode).
        let rotated = events.with_extension("log.1");
        std::fs::rename(&events, &rotated).unwrap();
        append_record(&events, LogStream::Stdout, 2, b"new\n");

        let second = recv_within(&mut f, Duration::from_secs(3))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.data.as_ref(), b"new\n");
    }

    #[tokio::test]
    async fn follower_streams_via_stream_trait() {
        use futures_core::Stream;
        use std::pin::Pin;
        use std::task::{Context, Poll};

        let (mut f, events, _tmp) = fixture_follower();
        append_record(&events, LogStream::Stdout, 1, b"first\n");

        let entry = std::future::poll_fn(|cx: &mut Context<'_>| {
            match Pin::new(&mut f).poll_next(cx) {
                Poll::Ready(Some(item)) => Poll::Ready(item),
                Poll::Ready(None) => panic!("stream closed unexpectedly"),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
        .unwrap();
        assert_eq!(entry.data.as_ref(), b"first\n");
    }
}
