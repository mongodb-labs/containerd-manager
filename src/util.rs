//! Internal utilities shared across modules.

use std::future::Future;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// Polls `predicate` until it returns `true` or `timeout` elapses (measured
/// from `start`).
pub(crate) async fn poll_until<F, Fut>(
    start: Instant,
    timeout: Duration,
    interval: Duration,
    timeout_msg: impl Into<String>,
    mut predicate: F,
) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    loop {
        if start.elapsed() > timeout {
            return Err(Error::Timeout(timeout_msg.into()));
        }
        if predicate().await {
            return Ok(());
        }
        tokio::time::sleep(interval).await;
    }
}

/// Maps `tonic::Status` to [`Error::Containerd`] with an operation tag.
pub(crate) trait StatusExt {
    fn into_crate_error(self, op: &'static str) -> Error;
}

impl StatusExt for containerd_client::tonic::Status {
    fn into_crate_error(self, op: &'static str) -> Error {
        Error::Containerd { op, source: self }
    }
}

/// Maps a gRPC status from an image-resolution call to a crate error,
/// turning `NotFound` into the typed [`Error::ImageNotFound`] and leaving
/// other codes generic. Centralizes the pattern that previously appeared
/// at every `get_image` call site.
pub(crate) fn map_image_status(
    op: &'static str,
    image: &str,
    status: containerd_client::tonic::Status,
) -> Error {
    if status.code() == containerd_client::tonic::Code::NotFound {
        Error::ImageNotFound(image.to_string())
    } else {
        status.into_crate_error(op)
    }
}

const MAX_IDENTIFIER_LEN: usize = 76;

/// Validates a containerd identifier (1-76 chars, alnum at boundaries,
/// `.`/`_`/`-` as internal separators with no doubles). Matches containerd's
/// own `identifiers.Validate`.
pub(crate) fn validate_identifier(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(Error::InvalidArgument(
            "identifier must not be empty".into(),
        ));
    }
    if id.len() > MAX_IDENTIFIER_LEN {
        return Err(Error::InvalidArgument(format!(
            "identifier exceeds {} characters: {:?}",
            MAX_IDENTIFIER_LEN, id
        )));
    }

    let bytes = id.as_bytes();
    let is_alnum = |b: u8| b.is_ascii_alphanumeric();
    let is_sep = |b: u8| matches!(b, b'.' | b'_' | b'-');

    if !is_alnum(bytes[0]) || !is_alnum(bytes[bytes.len() - 1]) {
        return Err(Error::InvalidArgument(format!(
            "identifier must start and end with an alphanumeric: {:?}",
            id
        )));
    }

    if bytes.len() <= 2 {
        return Ok(());
    }

    let mut prev_sep = false;
    for &b in &bytes[1..bytes.len() - 1] {
        if is_alnum(b) {
            prev_sep = false;
        } else if is_sep(b) {
            if prev_sep {
                return Err(Error::InvalidArgument(format!(
                    "identifier contains consecutive separators: {:?}",
                    id
                )));
            }
            prev_sep = true;
        } else {
            return Err(Error::InvalidArgument(format!(
                "identifier contains invalid character {:?}: {:?}",
                b as char, id
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_accepts_simple() {
        assert!(validate_identifier("a").is_ok());
        assert!(validate_identifier("abc").is_ok());
        assert!(validate_identifier("ABC123").is_ok());
    }

    #[test]
    fn validate_accepts_internal_separators() {
        assert!(validate_identifier("my-container").is_ok());
        assert!(validate_identifier("my_container").is_ok());
        assert!(validate_identifier("my.container").is_ok());
        assert!(validate_identifier("a.b-c_d").is_ok());
    }

    #[test]
    fn validate_rejects_empty() {
        assert!(validate_identifier("").is_err());
    }

    #[test]
    fn validate_rejects_path_traversal() {
        assert!(validate_identifier("..").is_err());
        assert!(validate_identifier("../foo").is_err());
        assert!(validate_identifier("foo/bar").is_err());
    }

    #[test]
    fn validate_rejects_shell_metachars() {
        assert!(validate_identifier("foo;ls").is_err());
        assert!(validate_identifier("foo$bar").is_err());
        assert!(validate_identifier("foo`bar`").is_err());
        assert!(validate_identifier("foo bar").is_err());
    }

    #[test]
    fn validate_rejects_leading_or_trailing_separator() {
        assert!(validate_identifier("-foo").is_err());
        assert!(validate_identifier("foo-").is_err());
        assert!(validate_identifier(".foo").is_err());
        assert!(validate_identifier("foo.").is_err());
    }

    #[test]
    fn validate_rejects_consecutive_separators() {
        assert!(validate_identifier("foo--bar").is_err());
        assert!(validate_identifier("foo._bar").is_err());
    }

    #[test]
    fn validate_rejects_too_long() {
        let s = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        assert!(validate_identifier(&s).is_err());
    }

    #[test]
    fn validate_accepts_max_length() {
        let s = "a".repeat(MAX_IDENTIFIER_LEN);
        assert!(validate_identifier(&s).is_ok());
    }
}
