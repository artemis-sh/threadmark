//! Object storage backends.
//!
//! [`ObjectStore`] dispatches between an S3-compatible service and a local
//! directory. Both are selected at startup by `BLOB_BACKEND`.
//!
//! The two are not equivalent, and the difference is advertised rather than
//! emulated: the filesystem backend has no presigning and therefore supports
//! neither direct browser upload nor presigned download. Callers ask
//! [`ObjectStore::supports_public_urls`] and fall back to the server-mediated
//! upload endpoint and the streaming download proxy.

pub mod fs;
pub mod s3;

use anyhow::Context;
use aws_sdk_s3::primitives::ByteStream;
use chrono::{DateTime, Utc};
use tokio_util::io::ReaderStream;

use crate::config::{BlobBackend, Config};

pub use s3::{ObjectHead, PresignedPost};

/// A stream of object bytes, whatever the backend.
pub enum ObjectBody {
    S3(ByteStream),
    File(tokio::fs::File),
}

/// Object storage, selected by configuration.
#[derive(Clone)]
pub enum ObjectStore {
    S3(s3::S3Store),
    Filesystem(fs::FsStore),
}

impl ObjectStore {
    pub fn new(config: &Config) -> Self {
        match config.blob_backend {
            BlobBackend::S3 => Self::S3(s3::S3Store::new(config)),
            BlobBackend::Filesystem => Self::Filesystem(fs::FsStore::new(
                config.blob_dir.as_deref().unwrap_or("./data/blobs"),
            )),
        }
    }

    /// Verify the backend is reachable and satisfies the active configuration.
    pub async fn ensure_ready(&self) -> anyhow::Result<()> {
        match self {
            Self::S3(store) => store.ensure_ready().await,
            Self::Filesystem(store) => store.ensure_ready().await,
        }
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        match self {
            Self::S3(store) => store.ping().await,
            Self::Filesystem(store) => store.ping().await,
        }
    }

    /// Whether the backend can hand a browser a URL that reaches storage
    /// directly, which gates both direct upload and presigned download.
    pub fn supports_public_urls(&self) -> bool {
        match self {
            Self::S3(store) => store.supports_public_urls(),
            Self::Filesystem(_) => false,
        }
    }

    /// Whether deleting an object must also remove prior versions.
    ///
    /// Always false on a filesystem, which keeps no version history.
    pub fn is_versioned(&self) -> bool {
        match self {
            Self::S3(store) => store.is_versioned(),
            Self::Filesystem(_) => false,
        }
    }

    pub async fn put(&self, key: &str, bytes: Vec<u8>, content_type: &str) -> anyhow::Result<()> {
        match self {
            Self::S3(store) => store.put(key, bytes, content_type).await,
            Self::Filesystem(store) => store.put(key, bytes, content_type).await,
        }
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        match self {
            Self::S3(store) => store.get(key).await,
            Self::Filesystem(store) => store.get(key).await,
        }
    }

    pub async fn get_stream(&self, key: &str) -> anyhow::Result<ObjectBody> {
        match self {
            Self::S3(store) => Ok(ObjectBody::S3(store.get_stream(key).await?)),
            Self::Filesystem(store) => Ok(ObjectBody::File(store.open(key).await?)),
        }
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        match self {
            Self::S3(store) => store.delete(key).await,
            Self::Filesystem(store) => store.delete(key).await,
        }
    }

    pub async fn head(&self, key: &str) -> anyhow::Result<Option<ObjectHead>> {
        match self {
            Self::S3(store) => store.head(key).await,
            Self::Filesystem(store) => Ok(store.size(key).await?.map(|size| ObjectHead {
                size: i64::try_from(size).unwrap_or(i64::MAX),
                content_type: None,
                metadata: std::collections::HashMap::new(),
                version_id: None,
            })),
        }
    }

    pub async fn versions(&self, key: &str) -> anyhow::Result<Vec<String>> {
        match self {
            Self::S3(store) => store.versions(key).await,
            // Never reached: callers consult `is_versioned` first.
            Self::Filesystem(_) => Ok(Vec::new()),
        }
    }

    pub async fn delete_version(&self, key: &str, version_id: &str) -> anyhow::Result<()> {
        match self {
            Self::S3(store) => store.delete_version(key, version_id).await,
            Self::Filesystem(store) => store.delete(key).await,
        }
    }

    pub async fn copy(
        &self,
        source_key: &str,
        source_version_id: &str,
        destination_key: &str,
        content_type: &str,
    ) -> anyhow::Result<()> {
        match self {
            Self::S3(store) => {
                store
                    .copy(source_key, source_version_id, destination_key, content_type)
                    .await
            }
            // Only the direct-upload path copies, and the filesystem backend
            // declines direct upload before a session is ever created.
            Self::Filesystem(_) => {
                anyhow::bail!("filesystem storage does not support server-side copy")
            }
        }
    }

    pub async fn presigned_get(
        &self,
        key: &str,
        ttl_seconds: u64,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        match self {
            Self::S3(store) => {
                store
                    .presigned_get(key, ttl_seconds, content_type, content_disposition)
                    .await
            }
            Self::Filesystem(_) => Ok(None),
        }
    }

    pub fn presigned_post(
        &self,
        key: &str,
        content_type: &str,
        upload_id: &str,
        size: i64,
        expires_at: DateTime<Utc>,
    ) -> anyhow::Result<PresignedPost> {
        match self {
            Self::S3(store) => store.presigned_post(key, content_type, upload_id, size, expires_at),
            Self::Filesystem(_) => {
                anyhow::bail!("filesystem storage does not support presigned uploads")
            }
        }
    }
}

impl ObjectBody {
    /// Convert to an axum body for streaming to the client.
    pub fn into_body(self) -> axum::body::Body {
        match self {
            Self::S3(stream) => {
                axum::body::Body::from_stream(ReaderStream::new(stream.into_async_read()))
            }
            Self::File(file) => axum::body::Body::from_stream(ReaderStream::new(file)),
        }
    }
}

/// Confirm a configuration the backend cannot satisfy is rejected at startup
/// rather than at the first request.
pub fn validate(config: &Config) -> anyhow::Result<()> {
    if config.blob_backend == BlobBackend::Filesystem {
        anyhow::ensure!(
            !config.direct_upload_enabled,
            "DIRECT_UPLOAD_ENABLED requires an S3 blob backend; filesystem storage cannot presign uploads"
        );
        config
            .blob_dir
            .as_deref()
            .context("BLOB_BACKEND=filesystem requires BLOB_DIR")?;
    }
    Ok(())
}
