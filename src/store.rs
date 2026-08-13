use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    error::{ApiError, ApiResult},
    ids::new_id,
    model::{
        Actor, AppendItems, AppendResult, Continuation, Conversation, CreateContinuation,
        CreateConversation, CreateTurn, Item, ReplayRequest, ReplayResult, Turn, UpdateTurn,
    },
};

pub async fn create_conversation(
    pool: &PgPool,
    actor: &Actor,
    request: CreateConversation,
) -> ApiResult<Conversation> {
    if !request.metadata.is_object() {
        return Err(ApiError::BadRequest(
            "metadata must be a JSON object".into(),
        ));
    }
    let title = request.title.unwrap_or_else(|| "New conversation".into());
    if title.trim().is_empty() || title.len() > 200 {
        return Err(ApiError::BadRequest(
            "title must contain 1 to 200 characters".into(),
        ));
    }
    Ok(sqlx::query_as::<_, Conversation>(
        "INSERT INTO conversations (id, tenant_id, owner_ref, title, metadata)
         VALUES ($1, $2, $3, $4, $5) RETURNING *",
    )
    .bind(new_id("conv"))
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .bind(title.trim())
    .bind(request.metadata)
    .fetch_one(pool)
    .await?)
}

pub async fn get_conversation(pool: &PgPool, actor: &Actor, id: &str) -> ApiResult<Conversation> {
    sqlx::query_as::<_, Conversation>(
        "SELECT * FROM conversations WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3",
    )
    .bind(id)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("Conversation not found.".into()))
}

pub async fn list_items(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
    after_seq: i64,
    limit: i64,
) -> ApiResult<Vec<Item>> {
    get_conversation(pool, actor, conversation_id).await?;
    Ok(sqlx::query_as::<_, Item>(
        "SELECT * FROM conversation_items
         WHERE conversation_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3",
    )
    .bind(conversation_id)
    .bind(after_seq.max(0))
    .bind(limit.clamp(1, 1000))
    .fetch_all(pool)
    .await?)
}

async fn lock_conversation<'a>(
    tx: &mut Transaction<'a, Postgres>,
    actor: &Actor,
    id: &str,
) -> ApiResult<Conversation> {
    sqlx::query_as::<_, Conversation>(
        "SELECT * FROM conversations
         WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3 FOR UPDATE",
    )
    .bind(id)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or_else(|| ApiError::NotFound("Conversation not found.".into()))
}

pub async fn append_items(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
    request: AppendItems,
) -> ApiResult<AppendResult> {
    if request.idempotency_key.trim().is_empty() || request.idempotency_key.len() > 200 {
        return Err(ApiError::BadRequest(
            "idempotency_key must contain 1 to 200 characters".into(),
        ));
    }
    if !matches!(request.source.as_str(), "user" | "agent" | "system") {
        return Err(ApiError::BadRequest(
            "source must be user, agent, or system".into(),
        ));
    }
    if request.items.is_empty() || request.items.len() > 100 {
        return Err(ApiError::BadRequest(
            "items must contain between 1 and 100 entries".into(),
        ));
    }
    if request.items.iter().any(|item| !item.is_object()) {
        return Err(ApiError::BadRequest(
            "each item must be a JSON object".into(),
        ));
    }

    let mut tx = pool.begin().await?;
    let conversation = lock_conversation(&mut tx, actor, conversation_id).await?;
    if let Some((first_seq, last_seq)) = sqlx::query_as::<_, (i64, i64)>(
        "SELECT first_seq, last_seq FROM append_batches
         WHERE conversation_id = $1 AND idempotency_key = $2",
    )
    .bind(conversation_id)
    .bind(&request.idempotency_key)
    .fetch_optional(&mut *tx)
    .await?
    {
        let items = sqlx::query_as::<_, Item>(
            "SELECT * FROM conversation_items
             WHERE conversation_id = $1 AND seq BETWEEN $2 AND $3 ORDER BY seq ASC",
        )
        .bind(conversation_id)
        .bind(first_seq)
        .bind(last_seq)
        .fetch_all(&mut *tx)
        .await?;
        tx.commit().await?;
        return Ok(AppendResult {
            items,
            replayed: true,
        });
    }

    if let Some(turn_id) = &request.turn_id {
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM turns WHERE id = $1 AND conversation_id = $2)",
        )
        .bind(turn_id)
        .bind(conversation_id)
        .fetch_one(&mut *tx)
        .await?;
        if !exists {
            return Err(ApiError::BadRequest(
                "turn_id does not belong to this conversation".into(),
            ));
        }
    }

    let first_seq = conversation.next_seq;
    let last_seq = first_seq + request.items.len() as i64 - 1;
    let mut inserted = Vec::with_capacity(request.items.len());
    for (offset, payload) in request.items.into_iter().enumerate() {
        inserted.push(
            sqlx::query_as::<_, Item>(
                "INSERT INTO conversation_items
                 (id, conversation_id, turn_id, seq, source, payload)
                 VALUES ($1, $2, $3, $4, $5, $6) RETURNING *",
            )
            .bind(new_id("item"))
            .bind(conversation_id)
            .bind(&request.turn_id)
            .bind(first_seq + offset as i64)
            .bind(&request.source)
            .bind(payload)
            .fetch_one(&mut *tx)
            .await?,
        );
    }
    sqlx::query(
        "INSERT INTO append_batches (conversation_id, idempotency_key, first_seq, last_seq)
         VALUES ($1, $2, $3, $4)",
    )
    .bind(conversation_id)
    .bind(request.idempotency_key)
    .bind(first_seq)
    .bind(last_seq)
    .execute(&mut *tx)
    .await?;
    sqlx::query("UPDATE conversations SET next_seq = $2, updated_at = now() WHERE id = $1")
        .bind(conversation_id)
        .bind(last_seq + 1)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(AppendResult {
        items: inserted,
        replayed: false,
    })
}

pub async fn replay(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
    request: ReplayRequest,
) -> ApiResult<ReplayResult> {
    let conversation = get_conversation(pool, actor, conversation_id).await?;
    let maximum = conversation.next_seq - 1;
    let through_seq = request.through_seq.unwrap_or(maximum).min(maximum);
    if through_seq < request.after_seq.max(0) {
        return Err(ApiError::BadRequest(
            "through_seq must not be less than after_seq".into(),
        ));
    }
    let rows = sqlx::query_scalar::<_, Value>(
        "SELECT payload FROM conversation_items
         WHERE conversation_id = $1 AND seq > $2 AND seq <= $3 ORDER BY seq ASC",
    )
    .bind(conversation_id)
    .bind(request.after_seq.max(0))
    .bind(through_seq)
    .fetch_all(pool)
    .await?;
    let input = rows
        .into_iter()
        .filter_map(|mut item| {
            let object = item.as_object_mut()?;
            if request.strip_top_level_ids {
                object.remove("id");
            }
            Some(item)
        })
        .collect();
    Ok(ReplayResult {
        conversation_id: conversation_id.into(),
        through_seq,
        input,
    })
}

pub async fn create_turn(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
    request: CreateTurn,
) -> ApiResult<Turn> {
    get_conversation(pool, actor, conversation_id).await?;
    if request.agent_ref.trim().is_empty() || request.idempotency_key.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "agent_ref and idempotency_key are required".into(),
        ));
    }
    Ok(sqlx::query_as::<_, Turn>(
        "INSERT INTO turns (id, conversation_id, agent_ref, idempotency_key)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (conversation_id, idempotency_key) DO UPDATE
         SET idempotency_key = EXCLUDED.idempotency_key RETURNING *",
    )
    .bind(new_id("turn"))
    .bind(conversation_id)
    .bind(request.agent_ref)
    .bind(request.idempotency_key)
    .fetch_one(pool)
    .await?)
}

pub async fn update_turn(
    pool: &PgPool,
    actor: &Actor,
    turn_id: &str,
    request: UpdateTurn,
) -> ApiResult<Turn> {
    if !matches!(
        request.status.as_str(),
        "pending" | "streaming" | "completed" | "incomplete" | "failed" | "cancelled"
    ) {
        return Err(ApiError::BadRequest("invalid turn status".into()));
    }
    let terminal = matches!(
        request.status.as_str(),
        "completed" | "incomplete" | "failed" | "cancelled"
    );
    sqlx::query_as::<_, Turn>(
        "UPDATE turns SET status = $1, response_id = $2, error = $3, usage = $4,
             completed_at = CASE WHEN $5 THEN COALESCE(completed_at, now()) ELSE NULL END
         WHERE id = $6 AND conversation_id IN
             (SELECT id FROM conversations WHERE tenant_id = $7 AND owner_ref = $8)
         RETURNING *",
    )
    .bind(request.status)
    .bind(request.response_id)
    .bind(request.error)
    .bind(request.usage)
    .bind(terminal)
    .bind(turn_id)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("Turn not found.".into()))
}

pub async fn create_continuation(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
    request: CreateContinuation,
) -> ApiResult<Continuation> {
    let conversation = get_conversation(pool, actor, conversation_id).await?;
    if request.agent_ref.trim().is_empty() || request.response_id.trim().is_empty() {
        return Err(ApiError::BadRequest(
            "agent_ref and response_id are required".into(),
        ));
    }
    let through_seq = request.through_seq.unwrap_or(conversation.next_seq - 1);
    if through_seq < 0 || through_seq >= conversation.next_seq {
        return Err(ApiError::BadRequest(
            "through_seq is outside the conversation transcript".into(),
        ));
    }
    let result = sqlx::query_as::<_, Continuation>(
        "INSERT INTO continuations
         (id, tenant_id, conversation_id, agent_ref, response_id, parent_response_id, through_seq, state)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (tenant_id, agent_ref, response_id) DO NOTHING RETURNING *",
    )
    .bind(new_id("cont"))
    .bind(&actor.tenant_id)
    .bind(conversation_id)
    .bind(request.agent_ref)
    .bind(request.response_id)
    .bind(request.parent_response_id)
    .bind(through_seq)
    .bind(request.state)
    .fetch_optional(pool)
    .await?;
    result.ok_or_else(|| ApiError::Conflict("Continuation response_id already exists.".into()))
}

pub async fn get_continuation(
    pool: &PgPool,
    actor: &Actor,
    response_id: &str,
    agent_ref: &str,
) -> ApiResult<Continuation> {
    sqlx::query_as::<_, Continuation>(
        "SELECT c.* FROM continuations c
         JOIN conversations v ON v.id = c.conversation_id
         WHERE c.response_id = $1 AND c.agent_ref = $2
           AND c.tenant_id = $3 AND v.owner_ref = $4",
    )
    .bind(response_id)
    .bind(agent_ref)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("Continuation not found.".into()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn replay_defaults_to_stripping_protocol_ids() {
        let request: ReplayRequest = serde_json::from_value(json!({})).unwrap();
        assert!(request.strip_top_level_ids);
        assert_eq!(request.after_seq, 0);
    }

    #[test]
    fn replay_can_preserve_protocol_ids() {
        let request: ReplayRequest =
            serde_json::from_value(json!({ "strip_top_level_ids": false })).unwrap();
        assert!(!request.strip_top_level_ids);
    }

    #[test]
    fn conversation_metadata_defaults_to_an_object() {
        let request: CreateConversation = serde_json::from_value(json!({})).unwrap();
        assert_eq!(request.metadata, json!({}));
    }
}
