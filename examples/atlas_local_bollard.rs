//! MongoDB Atlas Local lifecycle using Bollard (Docker Engine API).
//!
//! Mirrors examples/atlas_local.rs - same operations, same image - but uses
//! the Docker Engine API via Bollard instead of containerd's gRPC API directly.
//!
//! Run with: cargo run --example atlas_local_bollard
//!
//! Requires Docker running (e.g., `colima start` or Docker Desktop).

use std::collections::HashMap;
use std::time::Duration;

use bollard::container::LogOutput;
use bollard::models::{ContainerCreateBody, HealthStatusEnum, HostConfig};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, LogsOptionsBuilder,
    RemoveContainerOptionsBuilder,
};
use bollard::Docker;
use futures_util::StreamExt;

const IMAGE: &str = "docker.io/mongodb/mongodb-atlas-local:latest";
const CONTAINER_NAME: &str = "atlas-local-bollard";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Connect ---
    // Reads DOCKER_HOST if set; falls back to /var/run/docker.sock.
    // On macOS with Colima, DOCKER_HOST is set automatically when Colima starts.
    let docker = Docker::connect_with_local_defaults()?;
    let version = docker.version().await?;
    println!(
        "Connected to Docker: {}",
        version.version.as_deref().unwrap_or("unknown")
    );

    // --- Clean up any previous run ---
    println!("\nCleaning up existing container...");
    let _ = docker
        .remove_container(
            CONTAINER_NAME,
            Some(RemoveContainerOptionsBuilder::default().force(true).build()),
        )
        .await;

    // --- Pull image ---
    println!("Pulling image '{}' (this may take a while)...", IMAGE);
    let pull_opts = CreateImageOptionsBuilder::default()
        .from_image(IMAGE)
        .build();
    let mut pull_stream = docker.create_image(Some(pull_opts), None, None);
    while let Some(info) = pull_stream.next().await {
        let info = info?;
        if let Some(status) = info.status {
            print!("\r  {}          ", status);
        }
    }
    println!("\nImage pulled.");

    // --- Create container ---
    let mut labels = HashMap::new();
    labels.insert("app".to_string(), "mongodb-atlas-local".to_string());
    labels.insert("managed-by".to_string(), "bollard-example".to_string());

    let create_opts = CreateContainerOptionsBuilder::default()
        .name(CONTAINER_NAME)
        .build();

    let config = ContainerCreateBody {
        image: Some(IMAGE.to_string()),
        labels: Some(labels),
        host_config: Some(HostConfig {
            network_mode: Some("host".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    };

    println!("Creating container '{}'...", CONTAINER_NAME);
    docker.create_container(Some(create_opts), config).await?;
    println!("Container created.");

    // --- Start container ---
    println!("Starting container...");
    docker.start_container(CONTAINER_NAME, None).await?;

    let inspect = docker.inspect_container(CONTAINER_NAME, None).await?;
    let pid = inspect.state.as_ref().and_then(|s| s.pid).unwrap_or(0);
    println!("Container started! PID: {}", pid);

    // --- Wait for readiness (Docker HEALTHCHECK) ---
    println!("\nWaiting for MongoDB to become ready (using Docker HEALTHCHECK)...");
    let timeout = Duration::from_secs(120);
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return Err("timed out waiting for MongoDB to become healthy".into());
        }

        let inspect = docker.inspect_container(CONTAINER_NAME, None).await?;
        let status = inspect
            .state
            .as_ref()
            .and_then(|s| s.health.as_ref())
            .and_then(|h| h.status.as_ref());

        match status {
            Some(HealthStatusEnum::HEALTHY) => break,
            Some(HealthStatusEnum::UNHEALTHY) => {
                return Err("container health check failed".into());
            }
            _ => tokio::time::sleep(Duration::from_secs(2)).await,
        }
    }
    println!("MongoDB is ready!");

    // --- Inspect container ---
    let info = docker.inspect_container(CONTAINER_NAME, None).await?;
    if let Some(cfg) = &info.config {
        println!("Image: {}", cfg.image.as_deref().unwrap_or("unknown"));
        if let Some(labels) = &cfg.labels {
            println!("Labels: {:?}", labels);
        }
    }
    if let Some(state) = &info.state {
        println!("Status: {:?}", state.status);
        println!("PID: {:?}", state.pid);
        if state.running == Some(true) {
            println!("MongoDB Atlas Local is RUNNING!");
        }
    }

    // --- Exec ---
    println!("\n--- exec: mongosh ping ---");
    use bollard::exec::{CreateExecOptions, StartExecResults};

    let exec = docker
        .create_exec(
            CONTAINER_NAME,
            CreateExecOptions {
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                cmd: Some(vec![
                    "mongosh",
                    "--quiet",
                    "--eval",
                    "db.runCommand({ping:1})",
                ]),
                ..Default::default()
            },
        )
        .await?;

    let mut stdout_bytes = Vec::new();
    if let StartExecResults::Attached { mut output, .. } = docker.start_exec(&exec.id, None).await?
    {
        while let Some(msg) = output.next().await {
            if let LogOutput::StdOut { message } = msg? {
                stdout_bytes.extend_from_slice(&message);
            }
        }
    }
    println!("stdout: {}", String::from_utf8_lossy(&stdout_bytes).trim());

    let exec_inspect = docker.inspect_exec(&exec.id).await?;
    println!("exit_code: {}", exec_inspect.exit_code.unwrap_or(-1));

    // --- Container logs ---
    println!("\n--- container logs (first 10 entries) ---");
    let log_opts = LogsOptionsBuilder::default()
        .stdout(true)
        .stderr(true)
        .build();
    let mut log_stream = docker.logs(CONTAINER_NAME, Some(log_opts));

    let mut total = 0usize;
    while let Some(entry) = log_stream.next().await {
        let entry = entry?;
        if total < 10 {
            let (label, data) = match entry {
                LogOutput::StdOut { message } => ("stdout", message),
                LogOutput::StdErr { message } => ("stderr", message),
                _ => continue,
            };
            print!("  [{}] {}", label, String::from_utf8_lossy(&data));
        }
        total += 1;
    }
    if total > 10 {
        println!("  ... ({} more entries)", total - 10);
    }
    println!("Total entries: {}", total);

    // --- Delete container ---
    println!("\nDeleting container...");
    docker
        .remove_container(
            CONTAINER_NAME,
            Some(RemoveContainerOptionsBuilder::default().force(true).build()),
        )
        .await?;
    println!("Container deleted. Full lifecycle complete.");

    Ok(())
}
