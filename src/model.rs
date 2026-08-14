use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;

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

#[derive(Debug, Deserialize)]
pub struct CreateConversation {
    pub title: Option<String>,
    #[serde(default = "empty_object")]
    pub metadata: Value,
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
