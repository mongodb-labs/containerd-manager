//! Comprehensive example: MongoDB Atlas Local lifecycle.
//!
//! Demonstrates the full containerd-manager API:
//!   connect, pull, create, start, wait_ready, inspect, exec, logs_stream,
//!   delete.
//!
//! Run with: cargo run --example atlas_local
//!
//! Requires containerd running (e.g., via Colima).

use std::time::Duration;

use containerd_manager::{
    ContainerId, CreateContainerOpts, LogStream, NetworkMode, NetworkOpts, ReadinessStrategy,
    TaskStatus,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let image = "docker.io/mongodb/mongodb-atlas-local:latest";
    let container_id = ContainerId::new("atlas-local-1")?;
    let timeout = Duration::from_secs(30);

    // --- Connect ---
    let client = containerd_manager::connect(None)?.with_namespace("atlas-local");
    let version = client.server_version().await?;
    println!("Connected to containerd: {}", version);

    // --- Clean up any previous run ---
    println!("\nCleaning up existing container...");
    let _ = client.delete_container(&container_id, timeout).await;

    // --- Pull image ---
    println!("Pulling image '{}' (this may take a while)...", image);
    client.pull_image(image).await?;
    println!("Image pulled.");

    // --- Create container ---
    let opts = CreateContainerOpts::builder()
        .label("app", "mongodb-atlas-local")
        .label("managed-by", "containerd-manager")
        .network(NetworkOpts {
            mode: NetworkMode::Host,
        })
        .build();

    println!("Creating container '{}'...", container_id);
    client.create_container(&container_id, image, opts).await?;
    println!("Container created.");

    // --- Start container ---
    println!("Starting container...");
    let task_id = client.start_container(&container_id).await?;
    println!(
        "Container started! ID: {}, PID: {}",
        task_id.container_id(),
        task_id.pid()
    );

    // --- Wait for readiness (Docker-compatible) ---
    println!("\nWaiting for MongoDB to become ready (using image HEALTHCHECK)...");
    client
        .wait_ready(
            &container_id,
            Duration::from_secs(120),
            ReadinessStrategy::ImageHealthcheck,
        )
        .await?;
    println!("MongoDB is ready!");

    // --- Inspect container ---
    let info = client.inspect_container(&container_id).await?;
    println!("Image: {}", info.image);
    println!("Labels: {:?}", info.labels);
    if let Some(task) = &info.task {
        println!("Task status: {:?}, PID: {}", task.status, task.pid);
        if task.status == TaskStatus::Running {
            println!("MongoDB Atlas Local is RUNNING!");
        }
    }

    // --- Exec ---
    println!("\n--- exec: mongosh ping ---");
    let output = client
        .exec(
            &container_id,
            &["mongosh", "--quiet", "--eval", "db.runCommand({ping:1})"],
        )
        .await?;
    println!("stdout: {}", String::from_utf8_lossy(&output.stdout).trim());
    println!("exit_code: {}", output.exit_code);

    // --- Streaming logs (live tail until Ctrl-C) ---
    // Cleanup is explicit after the select breaks. Async Drop doesn't exist
    // in stable Rust, so RAII-style cleanup of `delete_container` (which is
    // async) has to be a manual `.await` here. The follower itself DOES use
    // Drop - dropping it aborts the polling task synchronously.
    println!("\n--- streaming logs (live, Ctrl-C to stop) ---");
    let mut follower = client.container_logs_stream(&container_id)?;
    loop {
        tokio::select! {
            entry = follower.recv() => {
                let Some(result) = entry else { break; };
                let entry = result?;
                let label = match entry.stream {
                    LogStream::Stdout => "stdout",
                    LogStream::Stderr => "stderr",
                };
                print!("  [{}] {}", label, String::from_utf8_lossy(&entry.data));
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nCtrl-C received, cleaning up...");
                break;
            }
        }
    }
    drop(follower);

    // --- Delete container ---
    println!("\nDeleting container...");
    client.delete_container(&container_id, timeout).await?;
    println!("Container deleted. Full lifecycle complete.");

    Ok(())
}
