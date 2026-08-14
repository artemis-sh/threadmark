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
    pub file_transactional_delete_enabled: bool,
    pub s3_endpoint: String,
    pub s3_public_url: Option<String>,
    pub s3_region: String,
    pub s3_bucket: String,
    pub s3_access_key_id: String,
    pub s3_secret_access_key: String,
    pub s3_force_path_style: bool,
    pub auth_mode: AuthMode,
    pub auth_issuer: Option<String>,
    pub auth_audience: Option<String>,
    pub auth_jwks_url: Option<String>,
    pub auth_max_owner_token_seconds: u64,
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
            file_transactional_delete_enabled: std::env::var("FILE_TRANSACTIONAL_DELETE_ENABLED")
                .unwrap_or_else(|_| "false".into())
                .parse()
                .context("FILE_TRANSACTIONAL_DELETE_ENABLED must be true or false")?,
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
            auth_mode,
            auth_issuer,
            auth_audience,
            auth_jwks_url,
            auth_max_owner_token_seconds,
        })))
    }
}
