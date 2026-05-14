//! Streams a busybox container's stdout/stderr live, like `docker logs -f`.
//!
//! Run:  cargo run --example follow_logs
//! Stop: Ctrl-C (container is cleaned up before exit).

use std::time::Duration;

use containerd_manager::{CreateContainerOpts, LogStream};

const IMAGE: &str = "docker.io/library/busybox:latest";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = containerd_manager::connect(None)?.with_namespace("follow-logs-example");
    let name = "follow-logs-demo";

    // Clean up any leftover from a previous run.
    if let Ok(prior) = client.resolve_name(name).await {
        let _ = client
            .delete_container(&prior, Duration::from_secs(10))
            .await;
    }

    println!("pulling {IMAGE}");
    client.pull_image(IMAGE).await?;

    let opts = CreateContainerOpts::builder()
        .cmd([
            "sh",
            "-c",
            // Numbered lines once a second; even = stdout, odd = stderr.
            r#"i=0; while :; do
             if [ $((i % 2)) -eq 0 ]; then echo "stdout #$i";
             else echo "stderr #$i" >&2; fi
             i=$((i+1)); sleep 1
           done"#,
        ])
        .build();
    println!("creating container");
    let id = client.create_container(name, IMAGE, opts).await?;

    println!("starting container");
    client.start_container(&id).await?;

    // Open the live stream. Cleanup is explicit after the select breaks -
    // async Drop doesn't exist on stable, so the async `delete_container`
    // call has to live after the main loop. The follower itself uses Drop
    // to abort its polling task synchronously.
    let mut follower = client.container_logs_stream(&id)?;
    println!("streaming logs - Ctrl-C to stop\n");

    loop {
        tokio::select! {
            entry = follower.recv() => {
                let Some(result) = entry else { break; };
                let entry = result?;
                let label = match entry.stream {
                    LogStream::Stdout => "out",
                    LogStream::Stderr => "ERR",
                };
                print!("[{label}] {}", String::from_utf8_lossy(&entry.data));
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nCtrl-C received");
                break;
            }
        }
    }

    drop(follower);
    client
        .delete_container(&id, Duration::from_secs(10))
        .await?;
    println!("cleaned up");
    Ok(())
}
