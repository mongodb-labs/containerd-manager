//! Inspect container and task state.

use containerd_client::services::v1::{Container, GetContainerRequest, GetRequest};
use oci_spec::runtime::Spec as OciSpec;

use std::collections::HashMap;

use crate::client::Client;
use crate::error::{Error, Result};
use crate::types::{ContainerId, ContainerInfo, TaskInfo, TaskStatus};

/// Parses OCI-format `"KEY=value"` strings into a map.
fn parse_env_vars(env: &[String]) -> HashMap<String, String> {
    env.iter()
        .filter_map(|e| {
            let (key, value) = e.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect()
}

fn extract_mounts(spec: &OciSpec) -> Vec<(String, String)> {
    spec.mounts()
        .as_ref()
        .map(|mounts| {
            mounts
                .iter()
                .map(|m| {
                    let source = m
                        .source()
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default();
                    let dest = m.destination().display().to_string();
                    (source, dest)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Shared by `inspect_container` and `list_containers`. Port forwards come
/// from container labels so they survive process restarts without an
/// in-process registry.
pub(crate) fn container_to_info(
    container: Container,
    task: Option<TaskInfo>,
) -> Result<ContainerInfo> {
    let labels: HashMap<String, String> = container.labels;
    let port_forwards = crate::port_forward::parse_port_binding_labels(&labels);

    // Tolerant: foreign containers (nerdctl/ctr/docker) may use OCI variants
    // we don't parse. Warn + empty env/mounts beats breaking list_containers.
    let (env, mounts) = if let Some(spec_any) = container.spec {
        match serde_json::from_slice::<OciSpec>(&spec_any.value) {
            Ok(spec) => {
                let env = spec
                    .process()
                    .as_ref()
                    .and_then(|p| p.env().as_ref())
                    .map(|e| parse_env_vars(e))
                    .unwrap_or_default();
                let mounts = extract_mounts(&spec);
                (env, mounts)
            }
            Err(e) => {
                tracing::warn!(
                    container_id = %container.id,
                    error = %e,
                    "OCI spec did not parse; env+mounts will be empty (foreign container?)"
                );
                (HashMap::new(), vec![])
            }
        }
    } else {
        (HashMap::new(), vec![])
    };

    Ok(ContainerInfo {
        id: ContainerId::new(container.id)?,
        image: container.image,
        labels,
        env,
        mounts,
        task,
        port_forwards,
    })
}

pub(crate) async fn inspect_container(
    client: &Client,
    container_id: &str,
) -> Result<ContainerInfo> {
    let get_req = client.ns_req(GetContainerRequest {
        id: container_id.to_string(),
    });
    let container_resp = client
        .containers_client()
        .get(get_req)
        .await
        .map_err(|e| {
            if e.code() == containerd_client::tonic::Code::NotFound {
                Error::ContainerNotFound(container_id.to_string())
            } else {
                crate::util::StatusExt::into_crate_error(e, "get_container")
            }
        })?;

    let container = container_resp
        .into_inner()
        .container
        .ok_or_else(|| Error::ContainerNotFound(container_id.to_string()))?;

    let container_id_str = container.id.clone();
    let task = get_task_info(client, &container_id_str).await;

    container_to_info(container, task)
}

/// `None` if no task exists for the container. Transport errors that aren't
/// `NotFound` are logged but still mapped to `None` to preserve the existing
/// caller contract (inspect should be tolerant of missing-task races).
pub(crate) async fn get_task_info(client: &Client, container_id: &str) -> Option<TaskInfo> {
    let get_req = client.ns_req(GetRequest {
        container_id: container_id.to_string(),
        exec_id: String::new(),
    });
    match client.tasks().get(get_req).await {
        Ok(resp) => {
            let process = resp.into_inner().process?;
            let status = TaskStatus::from(process.status);
            let exit_code = (status == TaskStatus::Stopped).then(|| {
                i32::try_from(process.exit_status).unwrap_or_else(|_| {
                    // Real exit codes are 0..=255; overflow = corrupt state.
                    tracing::warn!(
                        container_id,
                        exit_status = process.exit_status,
                        "get_task_info: u32 exit_status doesn't fit in i32; saturating to i32::MAX"
                    );
                    i32::MAX
                })
            });
            Some(TaskInfo {
                pid: process.pid,
                status,
                exit_code,
            })
        }
        Err(e) if e.code() == containerd_client::tonic::Code::NotFound => None,
        Err(e) => {
            // Transport / permission errors: log so users can see they're
            // misclassified as "no task" rather than failing the inspect.
            tracing::debug!(container_id, error = %e, "get_task_info: non-NotFound error treated as no task");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_vars_splits_correctly() {
        let env = vec![
            "KEY=value".to_string(),
            "PATH=/usr/bin:/bin".to_string(),
            "EMPTY=".to_string(),
        ];
        let parsed = parse_env_vars(&env);
        assert_eq!(parsed.len(), 3);
        assert_eq!(parsed.get("KEY").unwrap(), "value");
        assert_eq!(parsed.get("PATH").unwrap(), "/usr/bin:/bin");
        assert_eq!(parsed.get("EMPTY").unwrap(), "");
    }

    #[test]
    fn parse_env_vars_ignores_malformed() {
        let env = vec!["NOEQUALS".to_string(), "VALID=yes".to_string()];
        let parsed = parse_env_vars(&env);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed.get("VALID").unwrap(), "yes");
    }
}
