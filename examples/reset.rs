//! In-place rootfs reset: same container, fresh data each time.
//!
//! Use case: a test runner that wants to share one Atlas Local instance
//! across many tests, with a clean DB state per test. Network identity
//! (bridge IP, host ports) is preserved across resets.
//!
//! Flow:
//!   1. Prime a container, write a doc.
//!   2. Snapshot it.
//!   3. Mutate it (insert junk).
//!   4. reset_to_snapshot — the container's rootfs rewinds to the snapshot.
//!   5. Show the original data is back and the junk is gone.
//!
//! Run:  cargo run --example reset

use std::time::Duration;

use containerd_manager::{CreateContainerOpts, ReadinessStrategy, SnapshotContainerOpts};

const IMAGE: &str = "quay.io/mongodb/mongodb-atlas-local:latest";
const SNAPSHOT: &str = "primed-baseline";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = containerd_manager::connect(None)?.with_namespace("reset-example");
    let timeout = Duration::from_secs(120);
    let name = "test-runner";

    if let Ok(prior) = client.resolve_name(name).await {
        let _ = client.delete_container(&prior, timeout).await;
    }
    let _ = client.delete_snapshot(SNAPSHOT).await;

    client.pull_image(IMAGE).await?;

    println!("priming '{name}'");
    let cid = client
        .create_container(name, IMAGE, CreateContainerOpts::default())
        .await?;
    client.start_container(&cid).await?;
    client
        .wait_ready(&cid, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;
    client
        .exec(
            &cid,
            &[
                "mongosh",
                "--quiet",
                "--eval",
                "db.fixture.insertOne({_id: 'sentinel'})",
            ],
        )
        .await?;

    println!("snapshotting baseline as '{SNAPSHOT}'");
    client
        .snapshot_container_with_opts(
            &cid,
            SNAPSHOT,
            SnapshotContainerOpts::builder()
                .description("baseline before mutation")
                .build(),
        )
        .await?;

    // Pretend a test ran and trashed the DB state.
    println!("\nmutating: dropping sentinel, inserting junk");
    client.start_container(&cid).await?;
    client
        .wait_ready(&cid, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;
    client
        .exec(
            &cid,
            &[
                "mongosh",
                "--quiet",
                "--eval",
                "db.fixture.deleteOne({_id: 'sentinel'}); db.fixture.insertOne({_id: 'junk'})",
            ],
        )
        .await?;

    // Reset rewinds the rootfs underneath the same container. The OCI spec,
    // container id, bridge IP and host port stay the same.
    let before = client.inspect_container(&cid).await?.port_forwards.clone();
    println!("\nresetting to '{SNAPSHOT}'");
    client.reset_to_snapshot(&cid, SNAPSHOT).await?;
    client.start_container(&cid).await?;
    client
        .wait_ready(&cid, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;
    let after = client.inspect_container(&cid).await?.port_forwards.clone();
    println!("port allocation preserved: {before:?} == {after:?}");

    let out = client
        .exec(
            &cid,
            &[
                "mongosh",
                "--quiet",
                "--eval",
                "print(JSON.stringify({\
                    sentinel: db.fixture.findOne({_id:'sentinel'})?._id,\
                    junk: db.fixture.findOne({_id:'junk'})?._id ?? null\
                }))",
            ],
        )
        .await?;
    println!(
        "container state after reset: {}",
        String::from_utf8_lossy(&out.stdout).trim()
    );

    println!("\ncleaning up");
    client.delete_container(&cid, timeout).await?;
    client.delete_snapshot(SNAPSHOT).await?;
    Ok(())
}
