use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, ensure};
use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, header, request::Parts},
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde::Deserialize;

use crate::{
    api::AppState,
    config::{AuthMode, Config},
    error::ApiError,
    model::Actor,
};

#[derive(Clone)]
pub struct Authenticator(Arc<Kind>);

enum Kind {
    Jwt(JwtVerifier),
    #[cfg(feature = "trusted-headers")]
    TrustedHeaders,
}

struct JwtVerifier {
    issuer: String,
    audience: String,
    max_owner_seconds: u64,
    keys: HashMap<String, DecodingKey>,
}

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub actor: Actor,
    pub client_id: String,
    agent_ref: Option<String>,
    permissions: HashSet<Permission>,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Permission {
    ConversationList,
    ConversationCreate,
    ConversationRead,
    ConversationUpdate,
    ConversationDelete,
    ConversationTruncate,
    ConversationRegenerate,
    TranscriptRead,
    TranscriptAppend,
    TurnCreate,
    TurnRead,
    TurnUpdate,
    ContinuationRead,
    ContinuationWrite,
    FileCreate,
    FileRead,
    FileDelete,
    FileGrant,
}

#[cfg(feature = "trusted-headers")]
const OWNER_PERMISSIONS: [Permission; 18] = [
    Permission::ConversationList,
    Permission::ConversationCreate,
    Permission::ConversationRead,
    Permission::ConversationUpdate,
    Permission::ConversationDelete,
    Permission::ConversationTruncate,
    Permission::ConversationRegenerate,
    Permission::TranscriptRead,
    Permission::TranscriptAppend,
    Permission::TurnCreate,
    Permission::TurnRead,
    Permission::TurnUpdate,
    Permission::ContinuationRead,
    Permission::ContinuationWrite,
    Permission::FileCreate,
    Permission::FileRead,
    Permission::FileDelete,
    Permission::FileGrant,
];

#[derive(Debug, Deserialize)]
struct Claims {
    iss: String,
    aud: Audience,
    sub: String,
    client_id: String,
    iat: u64,
    nbf: u64,
    exp: u64,
    jti: String,
    token_kind: String,
    tenant: String,
    principal: String,
    permissions: Vec<String>,
    agent_ref: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Authenticator {
    pub async fn from_config(config: &Config) -> anyhow::Result<Self> {
        let kind = match config.auth_mode {
            #[cfg(feature = "trusted-headers")]
            AuthMode::TrustedHeaders => Kind::TrustedHeaders,
            AuthMode::Jwt => {
                let issuer = config
                    .auth_issuer
                    .clone()
                    .context("AUTH_ISSUER is required")?;
                let audience = config
                    .auth_audience
                    .clone()
                    .context("AUTH_AUDIENCE is required")?;
                let jwks_url = config
                    .auth_jwks_url
                    .as_ref()
                    .context("AUTH_JWKS_URL is required")?;
                let response = reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(std::time::Duration::from_secs(5))
                    .build()?
                    .get(jwks_url)
                    .send()
                    .await
                    .context("fetch AUTH_JWKS_URL")?
                    .error_for_status()
                    .context("AUTH_JWKS_URL returned an error")?;
                let bytes = response.bytes().await.context("read JWKS response")?;
                ensure!(bytes.len() <= 1024 * 1024, "JWKS response is too large");
                let jwks: JwkSet = serde_json::from_slice(&bytes).context("parse JWKS response")?;
                Kind::Jwt(JwtVerifier::new(
                    issuer,
                    audience,
                    config.auth_max_owner_token_seconds,
                    jwks,
                )?)
            }
        };
        Ok(Self(Arc::new(kind)))
    }

    fn authenticate(&self, headers: &HeaderMap) -> Result<AuthContext, ApiError> {
        match self.0.as_ref() {
            Kind::Jwt(verifier) => verifier.authenticate(headers),
            #[cfg(feature = "trusted-headers")]
            Kind::TrustedHeaders => trusted_headers(headers),
        }
    }
}

impl JwtVerifier {
    fn new(
        issuer: String,
        audience: String,
        max_owner_seconds: u64,
        jwks: JwkSet,
    ) -> anyhow::Result<Self> {
        ensure!(!jwks.keys.is_empty(), "JWKS contains no keys");
        let mut keys = HashMap::new();
        for jwk in jwks.keys {
            let kid = jwk
                .common
                .key_id
                .clone()
                .context("every JWK must have a kid")?;
            ensure!(
                jwk.common.key_algorithm == Some(jsonwebtoken::jwk::KeyAlgorithm::EdDSA),
                "JWK {kid} must use EdDSA"
            );
            let key = DecodingKey::from_jwk(&jwk).with_context(|| format!("invalid JWK {kid}"))?;
            ensure!(
                keys.insert(kid.clone(), key).is_none(),
                "duplicate JWK kid {kid}"
            );
        }
        Ok(Self {
            issuer,
            audience,
            max_owner_seconds,
            keys,
        })
    }

    fn authenticate(&self, headers: &HeaderMap) -> Result<AuthContext, ApiError> {
        let token = bearer(headers).ok_or(ApiError::Unauthorized)?;
        let token_header = decode_header(token).map_err(|_| ApiError::Unauthorized)?;
        if token_header.alg != Algorithm::EdDSA || token_header.typ.as_deref() != Some("at+jwt") {
            return Err(ApiError::Unauthorized);
        }
        let kid = token_header.kid.as_deref().ok_or(ApiError::Unauthorized)?;
        let key = self.keys.get(kid).ok_or(ApiError::Unauthorized)?;
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.leeway = 30;
        validation.validate_nbf = true;
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);
        validation.set_required_spec_claims(&["exp", "nbf", "iss", "aud", "sub"]);
        let claims = decode::<Claims>(token, key, &validation)
            .map_err(|_| ApiError::Unauthorized)?
            .claims;
        claims.context(self).ok_or(ApiError::Unauthorized)
    }
}

impl Claims {
    fn context(self, verifier: &JwtVerifier) -> Option<AuthContext> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        let valid_audience = match &self.aud {
            Audience::One(value) => value == &verifier.audience,
            Audience::Many(values) => values.len() == 1 && values[0] == verifier.audience,
        };
        if self.iss != verifier.issuer
            || !valid_audience
            || self.token_kind != "owner_session"
            || self.exp <= self.iat
            || self.exp <= self.nbf
            || self.iat > now.saturating_add(30)
            || self.exp.saturating_sub(self.iat) > verifier.max_owner_seconds
            || !valid_id(&self.sub)
            || !valid_id(&self.client_id)
            || self.client_id == "threadmark:trusted-headers"
            || !valid_id(&self.jti)
            || !valid_id(&self.tenant)
            || !valid_id(&self.principal)
            || self
                .agent_ref
                .as_deref()
                .is_some_and(|value| !valid_id(value))
        {
            return None;
        }
        let permissions = self
            .permissions
            .iter()
            .map(|value| Permission::parse(value))
            .collect::<Option<HashSet<_>>>()?;
        Some(AuthContext {
            actor: Actor {
                tenant_id: self.tenant,
                principal_id: self.principal,
            },
            client_id: self.client_id,
            agent_ref: self.agent_ref,
            permissions,
        })
    }
}

impl AuthContext {
    pub fn require(&self, permission: Permission) -> Result<(), ApiError> {
        self.permissions
            .contains(&permission)
            .then_some(())
            .ok_or(ApiError::Forbidden)
    }

    pub fn agent_ref(&self) -> Option<&str> {
        self.agent_ref.as_deref()
    }

    pub fn require_agent(&self, agent_ref: &str) -> Result<(), ApiError> {
        match &self.agent_ref {
            Some(bound) if bound != agent_ref => {
                Err(ApiError::NotFound("Agent resource not found.".into()))
            }
            _ => Ok(()),
        }
    }
}

impl std::ops::Deref for AuthContext {
    type Target = Actor;

    fn deref(&self) -> &Self::Target {
        &self.actor
    }
}

impl FromRequestParts<AppState> for AuthContext {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        state.auth.authenticate(&parts.headers)
    }
}

impl Permission {
    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "conversation:list" => Self::ConversationList,
            "conversation:create" => Self::ConversationCreate,
            "conversation:read" => Self::ConversationRead,
            "conversation:update" => Self::ConversationUpdate,
            "conversation:delete" => Self::ConversationDelete,
            "conversation:truncate" => Self::ConversationTruncate,
            "conversation:regenerate" => Self::ConversationRegenerate,
            "transcript:read" => Self::TranscriptRead,
            "transcript:append" => Self::TranscriptAppend,
            "turn:create" => Self::TurnCreate,
            "turn:read" => Self::TurnRead,
            "turn:update" => Self::TurnUpdate,
            "continuation:read" => Self::ContinuationRead,
            "continuation:write" => Self::ContinuationWrite,
            "file:create" => Self::FileCreate,
            "file:read" => Self::FileRead,
            "file:delete" => Self::FileDelete,
            "file:grant" => Self::FileGrant,
            _ => return None,
        })
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .filter(|token| !token.is_empty() && !token.contains(char::is_whitespace))
}

#[cfg(feature = "trusted-headers")]
fn trusted_headers(headers: &HeaderMap) -> Result<AuthContext, ApiError> {
    fn required(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .filter(|value| valid_id(value))
            .map(str::to_owned)
            .ok_or(ApiError::Unauthorized)
    }
    Ok(AuthContext {
        actor: Actor {
            tenant_id: required(headers, "x-threadmark-tenant")?,
            principal_id: required(headers, "x-threadmark-principal")?,
        },
        client_id: "threadmark:trusted-headers".into(),
        agent_ref: None,
        permissions: OWNER_PERMISSIONS.into_iter().collect(),
    })
}

fn valid_id(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= 200
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::{Value, json};

    use super::*;

    const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMC4CAQAwBQYDK2VwBCIEIGrD/e7uKYqSY4twDEsRfMMuLSrODf14dpTiTK6K1YI0\n-----END PRIVATE KEY-----\n";

    fn verifier() -> JwtVerifier {
        let jwks = serde_json::from_value(json!({"keys": [{
            "kty": "OKP",
            "use": "sig",
            "crv": "Ed25519",
            "x": "2-Jj2UvNCvQiUPNYRgSi0cJSPiJI6Rs6D0UTeEpQVj8",
            "kid": "test-key",
            "alg": "EdDSA"
        }]}))
        .unwrap();
        JwtVerifier::new(
            "https://issuer.example".into(),
            "threadmark-api".into(),
            300,
            jwks,
        )
        .unwrap()
    }

    fn claims() -> Value {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        json!({
            "iss": "https://issuer.example",
            "aud": "threadmark-api",
            "sub": "svc_parley",
            "client_id": "parley-test",
            "iat": now,
            "nbf": now,
            "exp": now + 300,
            "jti": "token-1",
            "token_kind": "owner_session",
            "tenant": "tenant-a",
            "principal": "user-a",
            "permissions": ["conversation:read"]
        })
    }

    fn headers(claims: &Value) -> HeaderMap {
        let mut header = Header::new(Algorithm::EdDSA);
        header.typ = Some("at+jwt".into());
        header.kid = Some("test-key".into());
        let token = encode(
            &header,
            claims,
            &EncodingKey::from_ed_pem(PRIVATE_KEY.as_bytes()).unwrap(),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    #[test]
    fn verifies_owner_identity_and_permissions() {
        let context = verifier().authenticate(&headers(&claims())).unwrap();
        assert_eq!(context.tenant_id, "tenant-a");
        assert_eq!(context.principal_id, "user-a");
        assert_eq!(context.client_id, "parley-test");
        assert!(context.require(Permission::ConversationRead).is_ok());
        assert!(matches!(
            context.require(Permission::ConversationDelete),
            Err(ApiError::Forbidden)
        ));
    }

    #[test]
    fn ignores_unsigned_identity_headers_in_jwt_mode() {
        let mut headers = headers(&claims());
        headers.insert("x-threadmark-tenant", HeaderValue::from_static("tenant-b"));
        headers.insert("x-threadmark-principal", HeaderValue::from_static("user-b"));
        let context = verifier().authenticate(&headers).unwrap();
        assert_eq!(context.tenant_id, "tenant-a");
        assert_eq!(context.principal_id, "user-a");
    }

    #[test]
    fn rejects_unknown_permission() {
        let mut claims = claims();
        claims["permissions"] = json!(["conversation:read", "conversation:destroy"]);
        assert!(matches!(
            verifier().authenticate(&headers(&claims)),
            Err(ApiError::Unauthorized)
        ));
    }

    #[test]
    fn rejects_reserved_trusted_header_client_id_in_jwt() {
        let mut claims = claims();
        claims["client_id"] = json!("threadmark:trusted-headers");
        assert!(matches!(
            verifier().authenticate(&headers(&claims)),
            Err(ApiError::Unauthorized)
        ));
    }

    #[test]
    fn retains_and_enforces_bound_agent() {
        let mut claims = claims();
        claims["agent_ref"] = json!("agent/prod");
        let context = verifier().authenticate(&headers(&claims)).unwrap();
        assert!(context.require_agent("agent/prod").is_ok());
        assert!(matches!(
            context.require_agent("agent/other"),
            Err(ApiError::NotFound(_))
        ));
    }

    #[test]
    fn rejects_delegated_tokens_until_resource_policy_is_implemented() {
        let mut claims = claims();
        claims["token_kind"] = json!("delegated_agent");
        assert!(matches!(
            verifier().authenticate(&headers(&claims)),
            Err(ApiError::Unauthorized)
        ));
    }

    #[test]
    fn rejects_excessive_lifetime_and_multi_audience_tokens() {
        let mut long_lived = claims();
        long_lived["exp"] = json!(long_lived["iat"].as_u64().unwrap() + 301);
        assert!(matches!(
            verifier().authenticate(&headers(&long_lived)),
            Err(ApiError::Unauthorized)
        ));

        let mut multi_audience = claims();
        multi_audience["aud"] = json!(["threadmark-api", "another-api"]);
        assert!(matches!(
            verifier().authenticate(&headers(&multi_audience)),
            Err(ApiError::Unauthorized)
        ));
    }

    #[test]
    fn rejects_wrong_token_type_and_missing_bearer_token() {
        let token_headers = headers(&claims());
        let token = token_headers
            .get(header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        let token = token.strip_prefix("Bearer ").unwrap();
        let mut invalid = HeaderMap::new();
        invalid.insert(
            header::AUTHORIZATION,
            HeaderValue::from_str(&format!("bearer {token}")).unwrap(),
        );
        assert!(matches!(
            verifier().authenticate(&invalid),
            Err(ApiError::Unauthorized)
        ));
        assert!(matches!(
            verifier().authenticate(&HeaderMap::new()),
            Err(ApiError::Unauthorized)
        ));
    }
}
