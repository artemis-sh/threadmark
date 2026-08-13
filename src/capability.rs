use std::time::{SystemTime, UNIX_EPOCH};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::{config::Config, model::Actor};

type HmacSha256 = Hmac<Sha256>;

pub fn file_url(config: &Config, actor: &Actor, file_id: &str) -> String {
    let expires = now_seconds() + config.capability_ttl_seconds;
    let signature = signature(config, actor, file_id, expires);
    let mut url = url::Url::parse(&config.public_url).expect("validated PUBLIC_URL");
    url.set_path(&format!("/v1/capabilities/files/{file_id}"));
    url.query_pairs_mut()
        .append_pair("tenant", &actor.tenant_id)
        .append_pair("owner", &actor.principal_id)
        .append_pair("expires", &expires.to_string())
        .append_pair("signature", &signature);
    url.into()
}

pub fn verify_file(
    config: &Config,
    actor: &Actor,
    file_id: &str,
    expires: u64,
    candidate: &str,
) -> bool {
    if expires < now_seconds() {
        return false;
    }
    let Ok(candidate) = URL_SAFE_NO_PAD.decode(candidate) else {
        return false;
    };
    let Ok(mut mac) = HmacSha256::new_from_slice(config.secret.as_bytes()) else {
        return false;
    };
    mac.update(&message(actor, file_id, expires));
    mac.verify_slice(&candidate).is_ok()
}

fn signature(config: &Config, actor: &Actor, file_id: &str, expires: u64) -> String {
    let mut mac =
        HmacSha256::new_from_slice(config.secret.as_bytes()).expect("HMAC accepts any key");
    mac.update(&message(actor, file_id, expires));
    URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
}

fn message(actor: &Actor, file_id: &str, expires: u64) -> Vec<u8> {
    format!(
        "threadmark.file.v1\0{}\0{}\0{}\0{}",
        actor.tenant_id, actor.principal_id, file_id, expires
    )
    .into_bytes()
}

fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::Inner;

    fn config() -> Config {
        Config(Arc::new(Inner {
            database_url: String::new(),
            listen_addr: String::new(),
            public_url: "https://threadmark.example/base".into(),
            secret: "a-secret-that-is-longer-than-32-bytes".into(),
            capability_ttl_seconds: 900,
            file_max_bytes: 32 * 1024 * 1024,
            s3_endpoint: String::new(),
            s3_public_url: None,
            s3_region: String::new(),
            s3_bucket: String::new(),
            s3_access_key_id: String::new(),
            s3_secret_access_key: String::new(),
            s3_force_path_style: true,
        }))
    }

    #[test]
    fn capability_round_trips_and_binds_identity() {
        let config = config();
        let actor = Actor {
            tenant_id: "tenant-a".into(),
            principal_id: "user-a".into(),
        };
        let url = url::Url::parse(&file_url(&config, &actor, "file_1")).unwrap();
        let query: std::collections::HashMap<_, _> = url.query_pairs().collect();
        let expires = query["expires"].parse().unwrap();
        assert!(verify_file(
            &config,
            &actor,
            "file_1",
            expires,
            &query["signature"]
        ));

        let another_actor = Actor {
            tenant_id: "tenant-a".into(),
            principal_id: "user-b".into(),
        };
        assert!(!verify_file(
            &config,
            &another_actor,
            "file_1",
            expires,
            &query["signature"]
        ));
    }
}
