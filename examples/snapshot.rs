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

use containerd_manager::{
    ContainerId, CreateContainerOpts, ReadinessStrategy, SnapshotContainerOpts,
};

const IMAGE: &str = "quay.io/mongodb/mongodb-atlas-local:latest";
const SNAPSHOT_NAME: &str = "atlas-local-sentinel";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = containerd_manager::connect(None)?.with_namespace("snapshot-example");
    let timeout = Duration::from_secs(120);

    let primed = ContainerId::new("primed")?;
    let restored = ContainerId::new("restored")?;

    // Clean any leftover state from a prior run.
    let _ = client.delete_container(&primed, timeout).await;
    let _ = client.delete_container(&restored, timeout).await;
    let _ = client.delete_snapshot(SNAPSHOT_NAME).await;

    client.pull_image(IMAGE).await?;

    println!("priming source");
    client
        .create_container(&primed, IMAGE, CreateContainerOpts::default())
        .await?;
    client.start_container(&primed).await?;
    client
        .wait_ready(&primed, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;
    client
        .exec(
            &primed,
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
            &primed,
            SNAPSHOT_NAME,
            SnapshotContainerOpts::builder()
                .description("primed fixture with sentinel doc")
                .build(),
        )
        .await?;

    // The snapshot is self-contained: source can go away.
    println!("deleting source. snapshot stands on its own");
    client.delete_container(&primed, timeout).await?;

    for snap in client.list_snapshots().await? {
        println!("  available snapshot: {} ({:?})", snap.name, snap.description);
    }

    println!("\nrestoring '{SNAPSHOT_NAME}' into '{restored}'");
    client.restore_container(SNAPSHOT_NAME, &restored).await?;
    client.start_container(&restored).await?;
    client
        .wait_ready(&restored, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;

    let result = client
        .exec(
            &restored,
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
    client.delete_container(&restored, timeout).await?;
    client.delete_snapshot(SNAPSHOT_NAME).await?;
    Ok(())
}
