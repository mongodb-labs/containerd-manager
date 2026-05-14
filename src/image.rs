//! Pull image (content store + snapshotter).

use containerd_client::services::v1::{GetImageRequest, TransferRequest};
use containerd_client::to_any;
use containerd_client::types::transfer::{ImageStore, OciRegistry, UnpackConfiguration};
use containerd_client::types::Platform;

use crate::client::Client;
use crate::consts::DEFAULT_SNAPSHOTTER;
use crate::error::Result;
use crate::oci_arch;
use crate::util::StatusExt;

fn build_source(image: &str) -> OciRegistry {
    OciRegistry {
        reference: image.to_string(),
        ..Default::default()
    }
}

fn build_destination(image: &str) -> ImageStore {
    ImageStore {
        name: image.to_string(),
        unpacks: vec![UnpackConfiguration {
            snapshotter: DEFAULT_SNAPSHOTTER.to_string(),
            platform: Some(Platform {
                os: "linux".to_string(),
                architecture: oci_arch().to_string(),
                variant: "".to_string(),
                os_version: "".to_string(),
            }),
        }],
        ..Default::default()
    }
}

fn build_transfer_request(image: &str) -> TransferRequest {
    let source = build_source(image);
    let dest = build_destination(image);

    TransferRequest {
        source: Some(to_any(&source)),
        destination: Some(to_any(&dest)),
        options: None,
    }
}

async fn image_exists(client: &Client, image: &str) -> Result<bool> {
    let req = client.ns_req(GetImageRequest {
        name: image.to_string(),
    });
    match client.images_client().get(req).await {
        Ok(_) => Ok(true),
        Err(status) if status.code() == containerd_client::tonic::Code::NotFound => Ok(false),
        Err(e) => Err(e.into_crate_error("image_exists")),
    }
}

/// Idempotent: returns immediately if the image is already in the namespace.
pub(crate) async fn pull_image(client: &Client, image: &str) -> Result<()> {
    if image_exists(client, image).await? {
        tracing::debug!(image, "pull_image: already cached");
        return Ok(());
    }

    tracing::info!(image, "pull_image: pulling");
    let req = client.ns_req(build_transfer_request(image));

    client
        .transfer()
        .transfer(req)
        .await
        .map_err(|e| e.into_crate_error("pull_image"))?;

    tracing::info!(image, "pull_image: done");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_source_sets_reference() {
        let image = "docker.io/library/alpine:latest";
        let source = build_source(image);

        assert_eq!(source.reference, image);
        assert!(source.resolver.is_none());
    }

    #[test]
    fn build_destination_sets_name_and_unpacks() {
        let image = "docker.io/library/alpine:latest";
        let dest = build_destination(image);

        assert_eq!(dest.name, image);
        assert_eq!(dest.unpacks.len(), 1);

        let unpack = &dest.unpacks[0];
        assert_eq!(unpack.snapshotter, DEFAULT_SNAPSHOTTER);
        assert!(unpack.platform.is_some());

        let platform = unpack.platform.as_ref().unwrap();
        assert_eq!(platform.os, "linux");
        assert_eq!(platform.architecture, oci_arch());
    }

    #[test]
    fn build_transfer_request_has_source_and_destination() {
        let image = "mongodb/mongodb-atlas-local:latest";
        let req = build_transfer_request(image);

        assert!(req.source.is_some());
        assert!(req.destination.is_some());
        assert!(req.options.is_none());

        let source_any = req.source.as_ref().unwrap();
        assert!(source_any.type_url.contains("OCIRegistry"));

        let dest_any = req.destination.as_ref().unwrap();
        assert!(dest_any.type_url.contains("ImageStore"));
    }

    #[test]
    fn build_destination_uses_current_arch() {
        let image = "test:latest";
        let dest = build_destination(image);

        let platform = dest.unpacks[0].platform.as_ref().unwrap();

        // OCI uses `amd64`/`arm64`, not Rust's `x86_64`/`aarch64`.
        #[cfg(target_arch = "x86_64")]
        assert_eq!(platform.architecture, "amd64");

        #[cfg(target_arch = "aarch64")]
        assert_eq!(platform.architecture, "arm64");
    }

    #[test]
    fn oci_arch_maps_rust_names_to_oci_names() {
        assert_eq!(
            oci_arch(),
            match std::env::consts::ARCH {
                "x86_64" => "amd64",
                "aarch64" => "arm64",
                other => other,
            }
        );
    }
}
