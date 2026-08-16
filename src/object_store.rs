use std::time::Duration;

use anyhow::Context;
use aws_credential_types::Credentials;
use aws_sdk_s3::{Client, config::Region, presigning::PresigningConfig, primitives::ByteStream};
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use hmac::{Hmac, Mac};
use serde::Serialize;
use serde_json::json;
use sha2::Sha256;

use crate::config::Config;

#[derive(Clone)]
pub struct ObjectStore {
    client: Client,
    public_client: Option<Client>,
    bucket: String,
    public_url: Option<String>,
    region: String,
    access_key_id: String,
    secret_access_key: String,
    force_path_style: bool,
}

#[derive(Serialize)]
pub struct PresignedPost {
    pub method: &'static str,
    pub url: String,
    pub fields: std::collections::BTreeMap<String, String>,
    pub expires_at: DateTime<Utc>,
}

pub struct ObjectHead {
    pub size: i64,
    pub content_type: Option<String>,
    pub metadata: std::collections::HashMap<String, String>,
    pub version_id: Option<String>,
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
            public_url: config.s3_public_url.clone(),
            region: config.s3_region.clone(),
            access_key_id: config.s3_access_key_id.clone(),
            secret_access_key: config.s3_secret_access_key.clone(),
            force_path_style: config.s3_force_path_style,
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

    pub async fn versioning_enabled(&self) -> anyhow::Result<bool> {
        let output = self
            .client
            .get_bucket_versioning()
            .bucket(&self.bucket)
            .send()
            .await
            .context("get S3 bucket versioning")?;
        Ok(output
            .status()
            .is_some_and(|status| status.as_str() == "Enabled"))
    }

    pub fn supports_public_urls(&self) -> bool {
        self.public_client.is_some()
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

    pub async fn get_stream(&self, key: &str) -> anyhow::Result<ByteStream> {
        Ok(self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .context("get S3 object")?
            .body)
    }

    pub async fn delete_version(&self, key: &str, version_id: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .version_id(version_id)
            .send()
            .await
            .context("delete versioned S3 object")?;
        Ok(())
    }

    pub async fn versions(&self, key: &str) -> anyhow::Result<Vec<String>> {
        let mut version_ids = Vec::new();
        let mut key_marker = None;
        let mut version_id_marker = None;
        loop {
            let mut request = self
                .client
                .list_object_versions()
                .bucket(&self.bucket)
                .prefix(key);
            if let Some(marker) = key_marker.as_deref() {
                request = request.key_marker(marker);
            }
            if let Some(marker) = version_id_marker.as_deref() {
                request = request.version_id_marker(marker);
            }
            let output = request.send().await.context("list S3 object versions")?;
            for object in output.versions() {
                if object.key() == Some(key)
                    && let Some(version_id) = object.version_id()
                {
                    version_ids.push(version_id.to_owned());
                }
            }
            for marker in output.delete_markers() {
                if marker.key() == Some(key)
                    && let Some(version_id) = marker.version_id()
                {
                    version_ids.push(version_id.to_owned());
                }
            }
            if !output.is_truncated().unwrap_or(false) {
                break;
            }
            key_marker = output.next_key_marker().map(str::to_owned);
            version_id_marker = output.next_version_id_marker().map(str::to_owned);
            if key_marker.is_none() || version_id_marker.is_none() {
                anyhow::bail!("truncated S3 version listing omitted continuation markers");
            }
        }
        Ok(version_ids)
    }

    pub async fn head(&self, key: &str) -> anyhow::Result<Option<ObjectHead>> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => Ok(Some(ObjectHead {
                size: output.content_length.unwrap_or_default(),
                content_type: output.content_type,
                metadata: output.metadata.unwrap_or_default(),
                version_id: output.version_id,
            })),
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_not_found()) =>
            {
                Ok(None)
            }
            Err(error) => Err(error).context("head S3 object"),
        }
    }

    pub async fn copy(
        &self,
        source_key: &str,
        source_version_id: &str,
        destination_key: &str,
        content_type: &str,
    ) -> anyhow::Result<()> {
        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(destination_key)
            .copy_source(format!(
                "{}/{}?versionId={}",
                self.bucket,
                url::form_urlencoded::byte_serialize(source_key.as_bytes()).collect::<String>(),
                url::form_urlencoded::byte_serialize(source_version_id.as_bytes())
                    .collect::<String>(),
            ))
            .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
            .content_type(content_type)
            .send()
            .await
            .context("copy S3 object")?;
        Ok(())
    }

    pub async fn presigned_get(
        &self,
        key: &str,
        ttl_seconds: u64,
        content_type: Option<&str>,
        content_disposition: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let Some(client) = &self.public_client else {
            return Ok(None);
        };
        let expires = PresigningConfig::expires_in(Duration::from_secs(ttl_seconds))
            .context("invalid presigned URL lifetime")?;
        let mut request = client.get_object().bucket(&self.bucket).key(key);
        if let Some(content_type) = content_type {
            request = request.response_content_type(content_type);
        }
        if let Some(content_disposition) = content_disposition {
            request = request.response_content_disposition(content_disposition);
        }
        let request = request
            .presigned(expires)
            .await
            .context("presign S3 object")?;
        Ok(Some(request.uri().to_string()))
    }

    pub fn presigned_post(
        &self,
        key: &str,
        content_type: &str,
        upload_id: &str,
        size: i64,
        expires_at: DateTime<Utc>,
    ) -> anyhow::Result<PresignedPost> {
        let public_url = self
            .public_url
            .as_deref()
            .context("S3_PUBLIC_URL is not configured")?;
        let now = Utc::now();
        let date = now.format("%Y%m%d").to_string();
        let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let credential_scope = format!("{date}/{}/s3/aws4_request", self.region);
        let credential = format!("{}/{}", self.access_key_id, credential_scope);
        let policy = json!({
            "expiration": expires_at.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "conditions": [
                {"bucket": self.bucket},
                {"key": key},
                {"Content-Type": content_type},
                {"x-amz-meta-threadmark-upload": upload_id},
                ["content-length-range", size, size],
                {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
                {"x-amz-credential": credential},
                {"x-amz-date": timestamp}
            ]
        });
        let policy = STANDARD.encode(serde_json::to_vec(&policy)?);
        let signing_key = signing_key(&self.secret_access_key, &date, &self.region);
        let signature = hex(&hmac(&signing_key, &policy));
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("key".into(), key.into());
        fields.insert("Content-Type".into(), content_type.into());
        fields.insert("x-amz-meta-threadmark-upload".into(), upload_id.into());
        fields.insert("policy".into(), policy);
        fields.insert("x-amz-algorithm".into(), "AWS4-HMAC-SHA256".into());
        fields.insert("x-amz-credential".into(), credential);
        fields.insert("x-amz-date".into(), timestamp);
        fields.insert("x-amz-signature".into(), signature);
        let url = post_url(public_url, &self.bucket, self.force_path_style)?;
        Ok(PresignedPost {
            method: "POST",
            url,
            fields,
            expires_at: expires_at.min(now + ChronoDuration::minutes(5)),
        })
    }
}

fn post_url(endpoint: &str, bucket: &str, force_path_style: bool) -> anyhow::Result<String> {
    let mut endpoint =
        url::Url::parse(endpoint).context("S3_PUBLIC_URL must be an absolute URL")?;
    if force_path_style {
        endpoint.set_path(&format!(
            "{}/{}",
            endpoint.path().trim_end_matches('/'),
            bucket
        ));
    } else {
        let host = endpoint
            .host_str()
            .context("S3_PUBLIC_URL must include a host")?;
        endpoint
            .set_host(Some(&format!("{bucket}.{host}")))
            .context("invalid bucket-qualified S3 hostname")?;
    }
    Ok(endpoint.to_string().trim_end_matches('/').into())
}

fn hmac(key: &[u8], value: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(value.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

fn signing_key(secret: &str, date: &str, region: &str) -> Vec<u8> {
    let date = hmac(format!("AWS4{secret}").as_bytes(), date);
    let region = hmac(&date, region);
    let service = hmac(&region, "s3");
    hmac(&service, "aws4_request")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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

#[cfg(test)]
mod tests {
    use super::post_url;

    #[test]
    fn builds_path_and_virtual_host_post_urls() {
        assert_eq!(
            post_url("https://objects.example/base", "threadmark", true).unwrap(),
            "https://objects.example/base/threadmark"
        );
        assert_eq!(
            post_url("https://s3.us-east-1.amazonaws.com", "threadmark", false).unwrap(),
            "https://threadmark.s3.us-east-1.amazonaws.com"
        );
    }
}
