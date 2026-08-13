use std::time::Duration;

use anyhow::Context;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, config::Region, presigning::PresigningConfig, primitives::ByteStream};

use crate::config::Config;

#[derive(Clone)]
pub struct ObjectStore {
    client: Client,
    public_client: Option<Client>,
    bucket: String,
}

impl ObjectStore {
    pub fn new(config: &Config) -> Self {
        let client = build_client(config, &config.s3_endpoint);
        let public_client = config
            .s3_public_url
            .as_deref()
            .map(|endpoint| build_client(config, endpoint));
        Self {
            client,
            public_client,
            bucket: config.s3_bucket.clone(),
        }
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        self.client
            .head_bucket()
            .bucket(&self.bucket)
            .send()
            .await
            .context("head S3 bucket")?;
        Ok(())
    }

    pub async fn put(&self, key: &str, bytes: Vec<u8>, content_type: &str) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .context("put S3 object")?;
        Ok(())
    }

    pub async fn get(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let output = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context("get S3 object")?;
        Ok(output
            .body
            .collect()
            .await
            .context("read S3 object")?
            .into_bytes()
            .to_vec())
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context("delete S3 object")?;
        Ok(())
    }

    pub async fn presigned_get(
        &self,
        key: &str,
        ttl_seconds: u64,
    ) -> anyhow::Result<Option<String>> {
        let Some(client) = &self.public_client else {
            return Ok(None);
        };
        let expires = PresigningConfig::expires_in(Duration::from_secs(ttl_seconds))
            .context("invalid presigned URL lifetime")?;
        let request = client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(expires)
            .await
            .context("presign S3 object")?;
        Ok(Some(request.uri().to_string()))
    }
}

fn build_client(config: &Config, endpoint: &str) -> Client {
    let credentials = Credentials::new(
        config.s3_access_key_id.clone(),
        config.s3_secret_access_key.clone(),
        None,
        None,
        "threadmark-config",
    );
    let sdk_config = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new(config.s3_region.clone()))
        .credentials_provider(credentials)
        .endpoint_url(endpoint)
        .force_path_style(config.s3_force_path_style)
        .build();
    Client::from_conf(sdk_config)
}
