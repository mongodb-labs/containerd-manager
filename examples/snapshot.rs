//! Snapshot + restore round-trip.
//!
//! Flow:
//!   1. Prime a container, write a doc.
//!   2. Snapshot it under a user-chosen name.
//!   3. Delete the source container.
//!   4. Restore the snapshot into a brand-new container.
//!   5. Show the data survived.
//!
//! Run:  cargo run --example snapshot

use std::time::Duration;

use containerd_manager::{CreateContainerOpts, ReadinessStrategy, SnapshotContainerOpts};

const IMAGE: &str = "quay.io/mongodb/mongodb-atlas-local:latest";
const SNAPSHOT_NAME: &str = "atlas-local-sentinel";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = containerd_manager::connect(None)?.with_namespace("snapshot-example");
    let timeout = Duration::from_secs(120);

    let primed_name = "primed";
    let restored_name = "restored";

    // Clean any leftover state from a prior run (resolve-by-name, ignore miss).
    if let Ok(id) = client.resolve_name(primed_name).await {
        let _ = client.delete_container(&id, timeout).await;
    }
    if let Ok(id) = client.resolve_name(restored_name).await {
        let _ = client.delete_container(&id, timeout).await;
    }
    let _ = client.delete_snapshot(SNAPSHOT_NAME).await;

    client.pull_image(IMAGE).await?;

    println!("priming source");
    let primed_id = client
        .create_container(primed_name, IMAGE, CreateContainerOpts::default())
        .await?;
    client.start_container(&primed_id).await?;
    client
        .wait_ready(&primed_id, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;
    client
        .exec(
            &primed_id,
            &[
                "mongosh",
                "--quiet",
                "--eval",
                "db.fixture.insertOne({_id: 'sentinel'})",
            ],
        )
        .await?;

    println!("snapshotting as '{SNAPSHOT_NAME}'");
    client
        .snapshot_container_with_opts(
            &primed_id,
            SNAPSHOT_NAME,
            SnapshotContainerOpts::builder()
                .description("primed fixture with sentinel doc")
                .build(),
        )
        .await?;

    // The snapshot is self-contained: source can go away.
    println!("deleting source. snapshot stands on its own");
    client.delete_container(&primed_id, timeout).await?;

    for snap in client.list_snapshots().await? {
        println!(
            "  available snapshot: {} ({:?})",
            snap.name, snap.description
        );
    }

    println!("\nrestoring '{SNAPSHOT_NAME}' into '{restored_name}'");
    let restored_id = client
        .restore_container(SNAPSHOT_NAME, restored_name)
        .await?;
    client.start_container(&restored_id).await?;
    client
        .wait_ready(&restored_id, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;

    let result = client
        .exec(
            &restored_id,
            &[
                "mongosh",
                "--quiet",
                "--eval",
                "print(db.fixture.findOne({_id: 'sentinel'})?._id)",
            ],
        )
        .await?;
    println!(
        "restored container reports: {}",
        String::from_utf8_lossy(&result.stdout).trim()
    );

    println!("\ncleaning up");
    client.delete_container(&restored_id, timeout).await?;
    client.delete_snapshot(SNAPSHOT_NAME).await?;
    Ok(())
}
