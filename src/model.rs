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
