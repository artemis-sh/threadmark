use std::sync::Arc;

use anyhow::{Context, ensure};

#[derive(Clone)]
pub struct Config(pub(crate) Arc<Inner>);

pub struct Inner {
    pub database_url: String,
    pub listen_addr: String,
    pub public_url: String,
    pub secret: String,
    pub capability_ttl_seconds: u64,
    pub file_max_bytes: usize,
    pub s3_endpoint: String,
    pub s3_public_url: Option<String>,
    pub s3_region: String,
    pub s3_bucket: String,
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub s3_force_path_style: bool,
    pub direct_upload_enabled: bool,
    pub file_upload_url_ttl_seconds: u64,
    pub file_upload_session_ttl_seconds: u64,
    pub auth_mode: AuthMode,
    pub auth_issuer: Option<String>,
    pub auth_audience: Option<String>,
    pub auth_jwks_url: Option<String>,
    pub auth_max_owner_token_seconds: u64,
    pub auth_max_delegated_token_seconds: u64,
    pub agent_replay_max_items: usize,
    pub agent_replay_max_bytes: usize,
    pub agent_replay_strip_top_level_fields: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMode {
    Jwt,
    #[cfg(feature = "trusted-headers")]
    TrustedHeaders,
}

impl std::ops::Deref for Config {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let required =
            |name: &str| std::env::var(name).with_context(|| format!("{name} is required"));
        let parse = |name: &str, default: &str| -> anyhow::Result<u64> {
            std::env::var(name)
                .unwrap_or_else(|_| default.into())
                .parse()
                .with_context(|| format!("{name} must be an unsigned integer"))
        };
        let public_url =
            std::env::var("PUBLIC_URL").unwrap_or_else(|_| "http://localhost:8090".into());
        url::Url::parse(&public_url).context("PUBLIC_URL must be an absolute URL")?;
        let secret = required("THREADMARK_SECRET")?;
        ensure!(
            secret.len() >= 32,
            "THREADMARK_SECRET must contain at least 32 characters"
        );
        let file_max_mb = parse("FILE_MAX_MB", "32")?;
        let file_max_bytes = usize::try_from(file_max_mb)
            .ok()
            .and_then(|value| value.checked_mul(1024 * 1024))
            .context("FILE_MAX_MB is too large")?;

        let auth_mode = match required("AUTH_MODE")?.as_str() {
            "jwt" => AuthMode::Jwt,
            #[cfg(feature = "trusted-headers")]
            "trusted_headers" => AuthMode::TrustedHeaders,
            _ => anyhow::bail!("AUTH_MODE is not supported by this build"),
        };
        let auth_setting = |name: &str| -> anyhow::Result<Option<String>> {
            match auth_mode {
                AuthMode::Jwt => required(name).map(Some),
                #[cfg(feature = "trusted-headers")]
                AuthMode::TrustedHeaders => Ok(None),
            }
        };
        let auth_issuer = auth_setting("AUTH_ISSUER")?;
        let auth_audience = auth_setting("AUTH_AUDIENCE")?;
        let auth_jwks_url = auth_setting("AUTH_JWKS_URL")?;
        if let (Some(issuer), Some(jwks_url)) = (&auth_issuer, &auth_jwks_url) {
            let issuer = url::Url::parse(issuer).context("AUTH_ISSUER must be an absolute URL")?;
            let jwks_url =
                url::Url::parse(jwks_url).context("AUTH_JWKS_URL must be an absolute URL")?;
            ensure!(issuer.scheme() == "https", "AUTH_ISSUER must use HTTPS");
            ensure!(jwks_url.scheme() == "https", "AUTH_JWKS_URL must use HTTPS");
            ensure!(
                issuer.origin() == jwks_url.origin(),
                "AUTH_JWKS_URL must have the same origin as AUTH_ISSUER"
            );
        }
        let auth_max_owner_token_seconds = parse("AUTH_MAX_OWNER_TOKEN_SECONDS", "300")?;
        ensure!(
            auth_max_owner_token_seconds > 0,
            "AUTH_MAX_OWNER_TOKEN_SECONDS must be greater than zero"
        );
        let auth_max_delegated_token_seconds = parse("AUTH_MAX_DELEGATED_TOKEN_SECONDS", "600")?;
        ensure!(
            auth_max_delegated_token_seconds > 0,
            "AUTH_MAX_DELEGATED_TOKEN_SECONDS must be greater than zero"
        );
        let agent_replay_max_items = usize::try_from(parse("AGENT_REPLAY_MAX_ITEMS", "200")?)
            .context("AGENT_REPLAY_MAX_ITEMS is too large")?;
        ensure!(
            agent_replay_max_items > 0,
            "AGENT_REPLAY_MAX_ITEMS must be greater than zero"
        );
        let agent_replay_max_bytes = usize::try_from(parse("AGENT_REPLAY_MAX_BYTES", "1048576")?)
            .context("AGENT_REPLAY_MAX_BYTES is too large")?;
        ensure!(
            agent_replay_max_bytes > 0,
            "AGENT_REPLAY_MAX_BYTES must be greater than zero"
        );
        let agent_replay_strip_top_level_fields =
            std::env::var("AGENT_REPLAY_STRIP_TOP_LEVEL_FIELDS")
                .unwrap_or_else(|_| "id".into())
                .split(',')
                .map(str::trim)
                .filter(|field| !field.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
        let mut unique_fields = agent_replay_strip_top_level_fields.clone();
        unique_fields.sort();
        unique_fields.dedup();
        ensure!(
            unique_fields.len() == agent_replay_strip_top_level_fields.len()
                && agent_replay_strip_top_level_fields
                    .iter()
                    .all(|field| field.chars().count() <= 200),
            "AGENT_REPLAY_STRIP_TOP_LEVEL_FIELDS must contain unique field names of at most 200 characters"
        );
        let direct_upload_enabled = std::env::var("DIRECT_UPLOAD_ENABLED")
            .unwrap_or_else(|_| "false".into())
            .parse()
            .context("DIRECT_UPLOAD_ENABLED must be true or false")?;
        let file_upload_url_ttl_seconds = parse("FILE_UPLOAD_URL_TTL_SECONDS", "60")?;
        ensure!(
            (1..=300).contains(&file_upload_url_ttl_seconds),
            "FILE_UPLOAD_URL_TTL_SECONDS must be from 1 through 300"
        );
        let file_upload_session_ttl_seconds = parse("FILE_UPLOAD_SESSION_TTL_SECONDS", "3600")?;
        ensure!(
            (1..=i64::MAX as u64).contains(&file_upload_session_ttl_seconds),
            "FILE_UPLOAD_SESSION_TTL_SECONDS must be from 1 through i64::MAX"
        );
        if direct_upload_enabled {
            ensure!(
                std::env::var("S3_PUBLIC_URL")
                    .ok()
                    .is_some_and(|value| !value.is_empty()),
                "DIRECT_UPLOAD_ENABLED requires S3_PUBLIC_URL"
            );
        }
        #[cfg(feature = "trusted-headers")]
        if auth_mode == AuthMode::TrustedHeaders {
            tracing::warn!("trusted-header authentication is enabled; do not expose this service");
        }

        Ok(Self(Arc::new(Inner {
            database_url: required("DATABASE_URL")?,
            listen_addr: std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8090".into()),
            public_url,
            secret,
            capability_ttl_seconds: parse("CAPABILITY_TTL_SECONDS", "900")?,
            file_max_bytes,
            s3_endpoint: required("S3_ENDPOINT")?,
            s3_public_url: std::env::var("S3_PUBLIC_URL")
                .ok()
                .filter(|value| !value.is_empty()),
            s3_region: std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
            s3_bucket: required("S3_BUCKET")?,
            s3_access_key_id: required("S3_ACCESS_KEY_ID")?,
            s3_secret_access_key: required("S3_SECRET_ACCESS_KEY")?,
            s3_force_path_style: std::env::var("S3_FORCE_PATH_STYLE")
                .unwrap_or_else(|_| "true".into())
                .parse()
                .context("S3_FORCE_PATH_STYLE must be true or false")?,
            direct_upload_enabled,
            file_upload_url_ttl_seconds,
            file_upload_session_ttl_seconds,
            auth_mode,
            auth_issuer,
            auth_audience,
            auth_jwks_url,
            auth_max_owner_token_seconds,
            auth_max_delegated_token_seconds,
            agent_replay_max_items,
            agent_replay_max_bytes,
            agent_replay_strip_top_level_fields,
        })))
    }
}
