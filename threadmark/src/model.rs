use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::{Map, Number, Value};
use sqlx::FromRow;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct Actor {
    pub tenant_id: String,
    pub principal_id: String,
}

#[derive(Debug, Serialize, FromRow)]
pub struct Conversation {
    pub id: String,
    pub tenant_id: String,
    pub owner_ref: String,
    pub title: String,
    pub metadata: Value,
    pub next_seq: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CreateConversation {
    pub title: Option<String>,
    #[serde(default = "empty_object")]
    pub metadata: Value,
}

#[derive(Debug, Deserialize)]
pub struct StartTurn {
    pub idempotency_key: String,
    pub conversation_id: Option<String>,
    pub conversation: Option<CreateConversation>,
    pub agent_ref: String,
    pub items: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct StartTurnResult {
    pub conversation_id: String,
    pub turn_id: String,
    pub item_ids: Vec<String>,
    pub first_seq: i64,
    pub last_seq: i64,
    pub replayed: bool,
}

#[derive(Debug, Deserialize)]
pub struct ListConversationsQuery {
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateConversation {
    pub title: Option<String>,
    pub metadata: Option<Value>,
}

fn empty_object() -> Value {
    serde_json::json!({})
}

pub struct StrictJson(pub Value);

impl<'de> Deserialize<'de> for StrictJson {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = StrictJson;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("valid JSON without duplicate keys")
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(StrictJson(Value::Bool(value)))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let as_float = value as f64;
                if as_float as i128 != value as i128 {
                    return Err(E::custom("number is not exactly representable as binary64"));
                }
                Ok(StrictJson(Value::Number(Number::from(value))))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let as_float = value as f64;
                if as_float as u128 != value as u128 {
                    return Err(E::custom("number is not exactly representable as binary64"));
                }
                Ok(StrictJson(Value::Number(Number::from(value))))
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if !value.is_finite() || (value == 0.0 && value.is_sign_negative()) {
                    return Err(E::custom("number is outside the canonical JSON domain"));
                }
                Number::from_f64(value)
                    .map(Value::Number)
                    .map(StrictJson)
                    .ok_or_else(|| E::custom("number is outside the canonical JSON domain"))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                self.visit_string(value.to_owned())
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value.contains('\0') {
                    return Err(E::custom("JSON strings cannot contain U+0000"));
                }
                Ok(StrictJson(Value::String(value)))
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(StrictJson(Value::Null))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(StrictJson(Value::Null))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: de::SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(StrictJson(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(StrictJson(Value::Array(values)))
            }

            fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
            where
                A: de::MapAccess<'de>,
            {
                let mut values = Map::new();
                while let Some(key) = object.next_key::<String>()? {
                    if key.contains('\0') {
                        return Err(de::Error::custom("JSON keys cannot contain U+0000"));
                    }
                    if values.contains_key(&key) {
                        return Err(de::Error::custom(format!("duplicate JSON key: {key}")));
                    }
                    let StrictJson(value) = object.next_value()?;
                    values.insert(key, value);
                }
                Ok(StrictJson(Value::Object(values)))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

fn parse_json_number(token: &str) -> Result<Value, &'static str> {
    if token.starts_with('-') && token[1..].parse::<f64>().ok() == Some(0.0) {
        return Err("negative zero is outside the canonical JSON domain");
    }
    let value = token
        .parse::<f64>()
        .map_err(|_| "number is outside the canonical JSON domain")?;
    if !value.is_finite() {
        return Err("number is outside the canonical JSON domain");
    }
    let canonical = serde_json_canonicalizer::to_string(&value)
        .map_err(|_| "number is outside the canonical JSON domain")?;
    let original = bigdecimal::BigDecimal::from_str(token)
        .map_err(|_| "number is outside the canonical JSON domain")?;
    let canonical_decimal = bigdecimal::BigDecimal::from_str(&canonical)
        .map_err(|_| "number is outside the canonical JSON domain")?;
    if original != canonical_decimal {
        return Err("number is not exactly representable as binary64");
    }
    let canonical_number = canonical
        .parse::<Number>()
        .map_err(|_| "number is outside the canonical JSON domain")?;
    Ok(Value::Number(canonical_number))
}

pub fn validate_json_number_tokens(input: &[u8]) -> Result<(), &'static str> {
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < input.len() {
        let byte = input[index];
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if byte == b'"' {
            in_string = true;
            index += 1;
            continue;
        }
        if byte == b'-' || byte.is_ascii_digit() {
            let start = index;
            index += 1;
            while index < input.len()
                && matches!(input[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
            {
                index += 1;
            }
            let token =
                std::str::from_utf8(&input[start..index]).map_err(|_| "invalid JSON number")?;
            parse_json_number(token)?;
            continue;
        }
        index += 1;
    }
    Ok(())
}

#[cfg(test)]
mod strict_json_tests {
    use super::{StrictJson, validate_json_number_tokens};

    fn parse(input: &str) -> Result<StrictJson, serde_json::Error> {
        serde_json::from_str(input)
    }

    #[test]
    fn rejects_duplicate_keys_recursively() {
        assert!(parse(r#"{"outer":{"value":1,"value":2}}"#).is_err());
    }

    #[test]
    fn preserves_serde_number_sentinel_as_an_opaque_key() {
        let StrictJson(value) = parse(r#"{"$serde_json::private::Number":"1"}"#).unwrap();
        assert_eq!(value["$serde_json::private::Number"], "1");
    }

    #[test]
    fn rejects_postgres_nul_and_inexact_integers() {
        assert!(parse(r#"{"value":"\u0000"}"#).is_err());
        assert!(validate_json_number_tokens(br#"{"value":9007199254740993}"#).is_err());
        assert!(validate_json_number_tokens(br#"{"value":0.10000000000000001}"#).is_err());
        assert!(validate_json_number_tokens(br#"{"value":-0}"#).is_err());
        assert!(validate_json_number_tokens(br#"{"value":-0.0}"#).is_err());
        assert!(validate_json_number_tokens(br#"{"value":"-0"}"#).is_ok());
    }
}

#[derive(Debug, Serialize, FromRow)]
pub struct Item {
    pub id: String,
    pub conversation_id: String,
    pub turn_id: Option<String>,
    pub seq: i64,
    pub source: String,
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct AppendItems {
    pub idempotency_key: String,
    pub turn_id: Option<String>,
    pub source: String,
    pub items: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct AppendResult {
    pub items: Vec<Item>,
    pub first_seq: i64,
    pub last_seq: i64,
    pub replayed: bool,
}

#[derive(Debug, Deserialize)]
pub struct ListItemsQuery {
    #[serde(default)]
    pub after_seq: i64,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    #[serde(default)]
    pub after_seq: i64,
    pub through_seq: Option<i64>,
    #[serde(default = "default_true")]
    pub strip_top_level_ids: bool,
    #[serde(default)]
    pub file_delivery: FileDelivery,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileDelivery {
    #[default]
    Preserve,
    CapabilityUrl,
    PresignedUrl,
    Inline,
}

pub(crate) fn default_true() -> bool {
    true
}

#[derive(Debug, Serialize)]
pub struct ReplayResult {
    pub conversation_id: String,
    pub through_seq: i64,
    pub input: Vec<Value>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct Turn {
    pub id: String,
    pub conversation_id: String,
    pub agent_ref: String,
    pub status: String,
    pub response_id: Option<String>,
    pub error: Option<Value>,
    pub usage: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTurn {
    pub idempotency_key: String,
    pub agent_ref: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateTurn {
    pub status: String,
    pub response_id: Option<String>,
    pub error: Option<Value>,
    pub usage: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct TruncateConversation {
    pub item_id: String,
}

#[derive(Debug, Serialize)]
pub struct RegenerateResult {
    pub turn_id: Option<String>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct Continuation {
    pub id: String,
    pub tenant_id: String,
    pub conversation_id: String,
    pub agent_ref: String,
    pub response_id: String,
    pub parent_response_id: Option<String>,
    pub through_seq: i64,
    pub state: Option<Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct CreateContinuation {
    pub agent_ref: String,
    pub response_id: String,
    pub parent_response_id: Option<String>,
    pub through_seq: Option<i64>,
    pub state: Option<Value>,
}

#[derive(Debug, Deserialize)]
pub struct ContinuationQuery {
    pub agent_ref: String,
}

#[derive(Debug, Serialize, FromRow)]
pub struct FileRecord {
    pub id: String,
    pub tenant_id: String,
    pub owner_ref: String,
    pub filename: String,
    pub mime_type: String,
    pub size: i64,
    #[serde(skip)]
    pub storage_key: String,
    pub created_at: DateTime<Utc>,
}

impl FileRecord {
    pub fn uri(&self) -> String {
        format!("threadmark://files/{}", self.id)
    }
}

#[derive(Debug, Serialize)]
pub struct FileResponse {
    pub id: String,
    pub filename: String,
    pub mime_type: String,
    pub size: i64,
    pub uri: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DownloadDelivery {
    Redirect,
    Proxy,
}

impl DownloadDelivery {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Redirect => "redirect",
            Self::Proxy => "proxy",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateDownload {
    pub delivery: DownloadDelivery,
}

#[derive(Debug, Serialize)]
pub struct DownloadGrant {
    pub url: String,
    pub delivery: String,
    pub expires_at: DateTime<Utc>,
}

impl From<FileRecord> for FileResponse {
    fn from(file: FileRecord) -> Self {
        let uri = file.uri();
        Self {
            id: file.id.clone(),
            filename: file.filename,
            mime_type: file.mime_type,
            size: file.size,
            uri,
            created_at: file.created_at,
        }
    }
}
