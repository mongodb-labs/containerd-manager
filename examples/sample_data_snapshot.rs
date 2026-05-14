//! Atlas Local sample-dataset fixture cached via snapshot.
//!
//! Atlas Local's runner downloads + restores sample_mflix etc. when the
//! `MONGODB_LOAD_SAMPLE_DATA=true` env var is set. That import takes minutes.
//! This example shows how to pay that cost once, snapshot the result, then
//! spin up fresh containers from the snapshot in seconds.
//!
//! Flow:
//!   1. Create container with MONGODB_LOAD_SAMPLE_DATA=true. Wait for the
//!      sample_mflix.movies collection to populate (~minutes).
//!   2. Snapshot the primed container.
//!   3. Restore into a brand-new container.
//!   4. Verify the sample data is there.
//!
//! Run:  cargo run --example search_snapshot

use std::time::Duration;

use containerd_manager::{
    ContainerId, CreateContainerOpts, ReadinessStrategy, SnapshotContainerOpts,
};

const IMAGE: &str = "quay.io/mongodb/mongodb-atlas-local:latest";
const SNAPSHOT: &str = "mflix-primed";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = containerd_manager::connect(None)?.with_namespace("sampledata-example");
    let timeout = Duration::from_secs(120);
    let ready_timeout = Duration::from_secs(180);

    let primed = ContainerId::new("primed")?;
    let restored = ContainerId::new("restored")?;

    let _ = client.delete_container(&primed, timeout).await;
    let _ = client.delete_container(&restored, timeout).await;
    let _ = client.delete_snapshot(SNAPSHOT).await;

    client.pull_image(IMAGE).await?;

    // The MONGODB_LOAD_SAMPLE_DATA env is read by the atlas-local image's
    // entrypoint; the runner imports sample_mflix + sample_airbnb + others
    // after mongod comes up.
    println!("creating container with sample data load enabled");
    let opts = CreateContainerOpts::builder().env("MONGODB_LOAD_SAMPLE_DATA", "true").build();
    client.create_container(&primed, IMAGE, opts).await?;
    client.start_container(&primed).await?;
    client
        .wait_ready(&primed, ready_timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;

    println!("waiting for sample_mflix import to finish (this is the slow part)");
    wait_for_mflix(&client, &primed).await?;

    println!("\nsnapshotting primed fixture");
    client
        .snapshot_container_with_opts(
            &primed,
            SNAPSHOT,
            SnapshotContainerOpts::builder()
                .description("atlas-local + sample_mflix imported")
                .build(),
        )
        .await?;

    // Source is now a template; we don't need it running for restores.
    client.delete_container(&primed, timeout).await?;

    println!("restoring snapshot into a fresh container");
    client.restore_container(SNAPSHOT, &restored).await?;
    client.start_container(&restored).await?;
    client
        .wait_ready(&restored, ready_timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;

    let out = client
        .exec(
            &restored,
            &[
                "mongosh",
                "--quiet",
                "--eval",
                "print(db.getSiblingDB('sample_mflix').movies.estimatedDocumentCount())",
            ],
        )
        .await?;
    println!(
        "restored container's sample_mflix.movies count: {}",
        String::from_utf8_lossy(&out.stdout).trim()
    );

    println!("\ncleaning up");
    client.delete_container(&restored, timeout).await?;
    client.delete_snapshot(SNAPSHOT).await?;
    Ok(())
}

async fn wait_for_mflix(
    client: &containerd_manager::Client,
    cid: &ContainerId,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Duration::from_secs(900);
    let start = std::time::Instant::now();
    loop {
        let out = client
            .exec(
                cid,
                &[
                    "mongosh",
                    "--quiet",
                    "--eval",
                    "print(db.getSiblingDB('sample_mflix').movies.estimatedDocumentCount())",
                ],
            )
            .await?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let count: i64 = stdout.lines().last().unwrap_or("0").trim().parse().unwrap_or(0);
        if count > 20000 {
            return Ok(());
        }
        if start.elapsed() > deadline {
            return Err(format!("sample data never populated (last count={count})").into());
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
