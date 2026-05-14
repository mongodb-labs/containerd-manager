//! Wait-for-ready. The `ImageHealthcheck` strategy reads the image's
//! `HEALTHCHECK` instruction and polls with its own interval / timeout /
//! start-period, matching Docker daemon behaviour.

use std::time::Duration;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::types::TaskStatus;

// Docker HEALTHCHECK defaults (matches the Docker daemon).
const DEFAULT_HC_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_HC_TIMEOUT: Duration = Duration::from_secs(30);

const TCP_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const _: () = assert!(TCP_CONNECT_TIMEOUT.as_millis() > 0);

// Exponential backoff for non-healthcheck strategies.
const INITIAL_INTERVAL: Duration = Duration::from_millis(100);
const MAX_INTERVAL: Duration = Duration::from_secs(2);
const MULTIPLIER: u32 = 2;
const _: () = assert!(INITIAL_INTERVAL.as_millis() < MAX_INTERVAL.as_millis());
const _: () = assert!(MULTIPLIER > 1);

#[derive(Debug, Clone)]
pub enum ReadinessStrategy {
    /// Task in `Running` state.
    ProcessRunning,
    /// TCP connect to host port succeeds.
    TcpPort(u16),
    /// Command exits 0 inside the container.
    Exec(Vec<String>),
    /// Use the image's `HEALTHCHECK`. Falls back to `ProcessRunning` if the
    /// image has none.
    ImageHealthcheck,
}

/// Docker `HEALTHCHECK` extracted from an OCI image config.
#[derive(Debug, Clone)]
pub struct HealthCheck {
    /// Exec-ready args, with `CMD`/`CMD-SHELL` prefix already unwrapped.
    pub test: Vec<String>,
    pub interval: Duration,
    pub timeout: Duration,
    /// Grace period before checks start.
    pub start_period: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// Exit code 0.
    Healthy,
    /// Check failed but the task is still inside its `start_period` grace
    /// window. Matches Docker's behavior: failing checks during start_period
    /// don't count toward the unhealthy threshold.
    Starting,
    /// Non-zero exit or timeout, past the start_period grace window.
    Unhealthy,
    /// Image has no `HEALTHCHECK`.
    NoHealthcheck,
}

struct PollTiming {
    initial_interval: Duration,
    start_period: Duration,
    check_timeout: Option<Duration>,
    use_backoff: bool,
}

impl Default for PollTiming {
    fn default() -> Self {
        Self {
            initial_interval: INITIAL_INTERVAL,
            start_period: Duration::ZERO,
            check_timeout: None,
            use_backoff: true,
        }
    }
}

/// `ReadinessStrategy` after `ImageHealthcheck` has been resolved.
enum ResolvedCheck {
    ProcessRunning,
    TcpPort(u16),
    Exec(Vec<String>),
}

/// `timeout` must comfortably exceed the strategy's poll `interval` —
/// `ImageHealthcheck` defaults to the image's `Interval` (typically 30s),
/// so a 10s `timeout` may complete fewer than one healthcheck poll. For
/// fast-startup probes prefer `ReadinessStrategy::TcpPort` or
/// `ReadinessStrategy::Exec` (both use 100ms exponential backoff).
pub(crate) async fn wait_ready(
    client: &Client,
    container_id: &str,
    timeout: Duration,
    strategy: ReadinessStrategy,
) -> Result<()> {
    let (check, timing) = resolve_strategy(client, container_id, &strategy).await?;

    // start_period sleeps first; deadline clock starts AFTER (matches
    // Docker's "orthogonal to wait timeout" semantics).
    if !timing.start_period.is_zero() {
        tokio::time::sleep(timing.start_period).await;
    }
    let start = std::time::Instant::now();

    let mut interval = timing.initial_interval;

    loop {
        let info = crate::inspect::inspect_container(client, container_id).await?;

        match &info.task {
            None => {
                return Err(Error::TaskNotFound(container_id.to_string()));
            }
            Some(task) => match task.status {
                TaskStatus::Stopped => {
                    return Err(Error::TaskExited(container_id.to_string()));
                }
                TaskStatus::Running => {
                    if run_check(client, container_id, &check, timing.check_timeout).await {
                        return Ok(());
                    }
                }
                other => {
                    tracing::trace!(
                        container_id,
                        status = ?other,
                        "wait_ready: task not yet running; sleeping until next poll"
                    );
                }
            },
        }

        if start.elapsed() > timeout {
            return Err(Error::Timeout(format!(
                "container {} did not become ready within {:?}",
                container_id, timeout
            )));
        }
        tokio::time::sleep(interval).await;
        if timing.use_backoff {
            interval = (interval * MULTIPLIER).min(MAX_INTERVAL);
        }
    }
}

/// For `ImageHealthcheck`, reads the image's HEALTHCHECK and converts it to
/// an exec check using Docker's interval/timeout/start-period. Falls back to
/// `ProcessRunning` if no HEALTHCHECK is defined.
async fn resolve_strategy(
    client: &Client,
    container_id: &str,
    strategy: &ReadinessStrategy,
) -> Result<(ResolvedCheck, PollTiming)> {
    match strategy {
        ReadinessStrategy::ProcessRunning => {
            Ok((ResolvedCheck::ProcessRunning, PollTiming::default()))
        }
        ReadinessStrategy::TcpPort(port) => {
            Ok((ResolvedCheck::TcpPort(*port), PollTiming::default()))
        }
        ReadinessStrategy::Exec(cmd) => {
            Ok((ResolvedCheck::Exec(cmd.clone()), PollTiming::default()))
        }
        ReadinessStrategy::ImageHealthcheck => {
            let info = crate::inspect::inspect_container(client, container_id).await?;
            let raw_config = crate::container::get_raw_image_config(client, &info.image).await?;

            match parse_healthcheck(&raw_config) {
                Some(hc) => {
                    let timing = PollTiming {
                        initial_interval: hc.interval,
                        start_period: hc.start_period,
                        check_timeout: Some(hc.timeout),
                        use_backoff: false,
                    };
                    Ok((ResolvedCheck::Exec(hc.test), timing))
                }
                None => Ok((ResolvedCheck::ProcessRunning, PollTiming::default())),
            }
        }
    }
}

async fn run_check(
    client: &Client,
    container_id: &str,
    check: &ResolvedCheck,
    check_timeout: Option<Duration>,
) -> bool {
    match check {
        ResolvedCheck::ProcessRunning => true,
        ResolvedCheck::TcpPort(port) => {
            let addr: std::net::SocketAddr = ([127, 0, 0, 1], *port).into();
            tokio::time::timeout(TCP_CONNECT_TIMEOUT, tokio::net::TcpStream::connect(addr))
                .await
                .is_ok_and(|r| r.is_ok())
        }
        ResolvedCheck::Exec(command) => {
            let cmd_refs: Vec<&str> = command.iter().map(String::as_str).collect();
            let exec_fut = crate::exec::exec(client, container_id, &cmd_refs);

            let result = if let Some(t) = check_timeout {
                match tokio::time::timeout(t, exec_fut).await {
                    Ok(r) => r,
                    Err(_) => return false,
                }
            } else {
                exec_fut.await
            };

            matches!(result, Ok(output) if output.exit_code == 0)
        }
    }
}

/// One-shot probe: reads the image's HEALTHCHECK and executes it once.
pub(crate) async fn probe_health(client: &Client, container_id: &str) -> Result<HealthStatus> {
    let info = crate::inspect::inspect_container(client, container_id).await?;

    let raw_config = crate::container::get_raw_image_config(client, &info.image).await?;

    // parse_healthcheck already returns None for empty tests, so a guard
    // here would be redundant.
    let Some(hc) = parse_healthcheck(&raw_config) else {
        return Ok(HealthStatus::NoHealthcheck);
    };

    let cmd_refs: Vec<&str> = hc.test.iter().map(String::as_str).collect();

    let exec_fut = crate::exec::exec(client, container_id, &cmd_refs);
    let exec_result = tokio::time::timeout(hc.timeout, exec_fut).await;

    let healthy = matches!(
        exec_result,
        Ok(Ok(ref output)) if output.exit_code == 0
    );
    if healthy {
        return Ok(HealthStatus::Healthy);
    }

    // Classify failure as Starting if inside start_period grace window;
    // missing uptime entry (Client restart, race) falls back to Unhealthy.
    if !hc.start_period.is_zero() {
        match client.task_uptime(container_id) {
            Some(elapsed) if elapsed < hc.start_period => {
                return Ok(HealthStatus::Starting);
            }
            Some(_) => {}
            None => {
                tracing::debug!(
                    container_id,
                    "probe_health: no task_uptime entry (post-Client-restart?); skipping start_period grace"
                );
            }
        }
    }

    Ok(HealthStatus::Unhealthy)
}

/// Docker stores the healthcheck under `config.Healthcheck` with fields
/// `Test`, `Interval`, `Timeout`, `Retries`, `StartPeriod` (times in ns).
/// Returns `None` if absent or `Test == ["NONE"]`.
pub fn parse_healthcheck(raw_config: &[u8]) -> Option<HealthCheck> {
    let v: serde_json::Value = serde_json::from_slice(raw_config).ok()?;
    let hc = v.get("config")?.get("Healthcheck")?;

    let test: Vec<String> = hc
        .get("Test")?
        .as_array()?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();

    if test.is_empty() {
        return None;
    }

    let exec_args = match test[0].as_str() {
        "NONE" => return None,
        "CMD" => test[1..].to_vec(),
        "CMD-SHELL" => {
            if test.len() < 2 {
                return None;
            }
            vec!["/bin/sh".to_string(), "-c".to_string(), test[1].clone()]
        }
        // Raw command without Docker prefix.
        _ => test,
    };

    if exec_args.is_empty() {
        return None;
    }

    let nanos_to_dur =
        |key: &str| -> Option<Duration> { hc.get(key)?.as_u64().map(Duration::from_nanos) };

    Some(HealthCheck {
        test: exec_args,
        interval: nanos_to_dur("Interval").unwrap_or(DEFAULT_HC_INTERVAL),
        timeout: nanos_to_dur("Timeout").unwrap_or(DEFAULT_HC_TIMEOUT),
        start_period: nanos_to_dur("StartPeriod").unwrap_or(Duration::ZERO),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_interval_growth() {
        let mut interval = INITIAL_INTERVAL;
        let steps: Vec<Duration> = (0..6)
            .map(|_| {
                let current = interval;
                interval = (interval * MULTIPLIER).min(MAX_INTERVAL);
                current
            })
            .collect();
        assert_eq!(steps[0], Duration::from_millis(100));
        assert_eq!(steps[1], Duration::from_millis(200));
        assert_eq!(steps[2], Duration::from_millis(400));
        assert_eq!(steps[3], Duration::from_millis(800));
        assert_eq!(steps[4], Duration::from_millis(1600));
        assert_eq!(steps[5], Duration::from_secs(2));
    }

    #[test]
    fn parse_healthcheck_cmd_format() {
        let json = r#"{
            "config": {
                "Healthcheck": {
                    "Test": ["CMD", "mongosh", "--eval", "db.runCommand({ping:1})"],
                    "Interval": 30000000000,
                    "Timeout": 30000000000,
                    "Retries": 3,
                    "StartPeriod": 5000000000
                }
            }
        }"#;
        let hc = parse_healthcheck(json.as_bytes()).unwrap();
        assert_eq!(
            hc.test,
            vec!["mongosh", "--eval", "db.runCommand({ping:1})"]
        );
        assert_eq!(hc.interval, Duration::from_secs(30));
        assert_eq!(hc.timeout, Duration::from_secs(30));
        assert_eq!(hc.start_period, Duration::from_secs(5));
    }

    #[test]
    fn parse_healthcheck_cmd_shell_format() {
        let json = r#"{
            "config": {
                "Healthcheck": {
                    "Test": ["CMD-SHELL", "curl -f http://localhost/ || exit 1"],
                    "Interval": 10000000000
                }
            }
        }"#;
        let hc = parse_healthcheck(json.as_bytes()).unwrap();
        assert_eq!(
            hc.test,
            vec!["/bin/sh", "-c", "curl -f http://localhost/ || exit 1"]
        );
        assert_eq!(hc.interval, Duration::from_secs(10));
        assert_eq!(hc.timeout, DEFAULT_HC_TIMEOUT);
        assert_eq!(hc.start_period, Duration::ZERO);
    }

    #[test]
    fn parse_healthcheck_none_returns_none() {
        let json = r#"{
            "config": {
                "Healthcheck": {
                    "Test": ["NONE"]
                }
            }
        }"#;
        assert!(parse_healthcheck(json.as_bytes()).is_none());
    }

    #[test]
    fn parse_healthcheck_missing_healthcheck() {
        let json = r#"{ "config": {} }"#;
        assert!(parse_healthcheck(json.as_bytes()).is_none());
    }

    #[test]
    fn parse_healthcheck_missing_config() {
        let json = r#"{ "architecture": "amd64" }"#;
        assert!(parse_healthcheck(json.as_bytes()).is_none());
    }

    #[test]
    fn parse_healthcheck_empty_test_array() {
        let json = r#"{
            "config": {
                "Healthcheck": {
                    "Test": []
                }
            }
        }"#;
        assert!(parse_healthcheck(json.as_bytes()).is_none());
    }

    #[test]
    fn parse_healthcheck_defaults_applied() {
        let json = r#"{
            "config": {
                "Healthcheck": {
                    "Test": ["CMD", "true"]
                }
            }
        }"#;
        let hc = parse_healthcheck(json.as_bytes()).unwrap();
        assert_eq!(hc.test, vec!["true"]);
        assert_eq!(hc.interval, DEFAULT_HC_INTERVAL);
        assert_eq!(hc.timeout, DEFAULT_HC_TIMEOUT);
        assert_eq!(hc.start_period, Duration::ZERO);
    }

    #[test]
    fn parse_healthcheck_invalid_json() {
        assert!(parse_healthcheck(b"not json").is_none());
    }

    #[test]
    fn parse_healthcheck_cmd_shell_missing_command() {
        let json = r#"{
            "config": {
                "Healthcheck": {
                    "Test": ["CMD-SHELL"]
                }
            }
        }"#;
        assert!(parse_healthcheck(json.as_bytes()).is_none());
    }

    #[test]
    fn poll_timing_default_uses_backoff() {
        let timing = PollTiming::default();
        assert_eq!(timing.initial_interval, INITIAL_INTERVAL);
        assert!(timing.use_backoff);
        assert!(timing.check_timeout.is_none());
        assert_eq!(timing.start_period, Duration::ZERO);
    }
}
