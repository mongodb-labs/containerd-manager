//! CoW-clones a primed Atlas Local container.
//!
//! Flow:
//!   1. Create a source container, write a doc.
//!   2. Clone it. The clone inherits the source's data via overlayfs CoW and
//!      gets its own bridge IP + host port.
//!   3. Show that the clone has the source's doc.
//!
//! Run:  cargo run --example clone

use std::time::Duration;

use containerd_manager::{CreateContainerOpts, ReadinessStrategy};

const IMAGE: &str = "quay.io/mongodb/mongodb-atlas-local:latest";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = containerd_manager::connect(None)?.with_namespace("clone-example");
    let timeout = Duration::from_secs(120);

    let src_name = "primed";
    let dst_name = "clone-of-primed";

    // Clean any leftover state from a prior run.
    if let Ok(id) = client.resolve_name(src_name).await {
        let _ = client.delete_container(&id, timeout).await;
    }
    if let Ok(id) = client.resolve_name(dst_name).await {
        let _ = client.delete_container(&id, timeout).await;
    }

    client.pull_image(IMAGE).await?;

    println!("creating + priming source");
    let src = client
        .create_container(src_name, IMAGE, CreateContainerOpts::default())
        .await?;
    client.start_container(&src).await?;
    client
        .wait_ready(&src, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;

    client
        .exec(
            &src,
            &[
                "mongosh",
                "--quiet",
                "--eval",
                "db.fixture.insertOne({_id: 'sentinel'})",
            ],
        )
        .await?;
    println!("inserted sentinel doc into source");

    // clone_container: stops src, commits its rootfs to a frozen base, and
    // prepares a CoW snapshot for dst rooted at that base.
    println!("\ncloning '{src_name}' -> '{dst_name}'");
    let dst = client.clone_container(&src, dst_name).await?;

    println!("starting clone");
    client.start_container(&dst).await?;
    client
        .wait_ready(&dst, timeout, ReadinessStrategy::ImageHealthcheck)
        .await?;

    let result = client
        .exec(
            &dst,
            &[
                "mongosh",
                "--quiet",
                "--eval",
                "print(db.fixture.findOne({_id: 'sentinel'})?._id)",
            ],
        )
        .await?;
    println!(
        "clone reports: {}",
        String::from_utf8_lossy(&result.stdout).trim()
    );

    let info = client.inspect_container(&dst).await?;
    println!(
        "clone runs on its own port allocation: {:?}",
        info.port_forwards
    );

    println!("\ncleaning up");
    client.delete_container(&dst, timeout).await?;
    client.delete_container(&src, timeout).await?;
    Ok(())
}
