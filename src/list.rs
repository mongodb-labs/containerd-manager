//! List containers in a namespace.

use std::collections::HashMap;

use containerd_client::services::v1::{ListContainersRequest, ListTasksRequest};

use crate::client::Client;
use crate::error::Result;
use crate::inspect::container_to_info;
use crate::types::{ContainerInfo, TaskInfo, TaskStatus};
use crate::util::StatusExt;

/// `"k=v"` → `labels."k"==v` (containerd's filter syntax).
fn build_label_filter(label_filter: &str) -> Option<String> {
    let parts: Vec<&str> = label_filter.splitn(2, '=').collect();
    if parts.len() == 2 {
        Some(format!("labels.\"{}\"=={}", parts[0], parts[1]))
    } else {
        None
    }
}

/// Multiple filters become a single comma-separated string (AND). Separate
/// repeated-field entries would be OR'd, which is not what we want.
fn build_list_request(label_filters: &[&str]) -> ListContainersRequest {
    let parsed: Vec<String> = label_filters
        .iter()
        .filter_map(|f| build_label_filter(f))
        .collect();

    let filters = if parsed.is_empty() {
        vec![]
    } else {
        vec![parsed.join(",")]
    };

    ListContainersRequest { filters }
}

pub(crate) async fn list_containers(
    client: &Client,
    label_filters: &[&str],
) -> Result<Vec<ContainerInfo>> {
    let list_req = client.ns_req(build_list_request(label_filters));
    let containers = client
        .containers_client()
        .list(list_req)
        .await
        .map_err(|e| e.into_crate_error("list_containers"))?
        .into_inner()
        .containers;

    // One bulk tasks.List instead of N tasks.Get calls.
    let tasks = fetch_task_map(client).await?;

    containers
        .into_iter()
        .map(|c| {
            let task = tasks.get(&c.id).cloned();
            container_to_info(c, task)
        })
        .collect()
}

/// Returns `container_id -> TaskInfo` for every task in the namespace. A
/// gRPC failure here is non-fatal - we return an empty map so the caller can
/// still surface container metadata without task state.
async fn fetch_task_map(client: &Client) -> Result<HashMap<String, TaskInfo>> {
    let req = client.ns_req(ListTasksRequest {
        filter: String::new(),
    });
    let tasks = match client.tasks().list(req).await {
        Ok(resp) => resp.into_inner().tasks,
        Err(_) => return Ok(HashMap::new()),
    };

    Ok(tasks
        .into_iter()
        .map(|p| {
            let status = TaskStatus::from(p.status);
            let exit_code = (status == TaskStatus::Stopped).then_some(p.exit_status as i32);
            (
                p.container_id,
                TaskInfo {
                    pid: p.pid,
                    status,
                    exit_code,
                },
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_label_filter_parses_correctly() {
        let filter = build_label_filter("app=test");
        assert_eq!(filter, Some("labels.\"app\"==test".to_string()));
    }

    #[test]
    fn build_label_filter_handles_values_with_equals() {
        let filter = build_label_filter("key=value=with=equals");
        assert_eq!(
            filter,
            Some("labels.\"key\"==value=with=equals".to_string())
        );
    }

    #[test]
    fn build_label_filter_returns_none_for_invalid() {
        let filter = build_label_filter("noequals");
        assert_eq!(filter, None);
    }

    #[test]
    fn build_list_request_no_filter() {
        let req = build_list_request(&[]);
        assert!(req.filters.is_empty());
    }

    #[test]
    fn build_list_request_with_filter() {
        let req = build_list_request(&["app=myapp"]);
        assert_eq!(req.filters.len(), 1);
        assert_eq!(req.filters[0], "labels.\"app\"==myapp");
    }

    #[test]
    fn build_list_request_ignores_invalid_filter() {
        let req = build_list_request(&["invalid"]);
        assert!(req.filters.is_empty());
    }

    #[test]
    fn build_list_request_multiple_filters_joined_as_and() {
        let req = build_list_request(&["app=myapp", "env=prod"]);
        assert_eq!(req.filters.len(), 1);
        assert_eq!(req.filters[0], "labels.\"app\"==myapp,labels.\"env\"==prod");
    }

    #[test]
    fn build_list_request_skips_invalid_keeps_valid() {
        let req = build_list_request(&["invalid", "app=myapp"]);
        assert_eq!(req.filters.len(), 1);
        assert_eq!(req.filters[0], "labels.\"app\"==myapp");
    }
}
