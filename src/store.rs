use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool, Postgres, Transaction};

use crate::{
    api::AppState,
    capability,
    error::{ApiError, ApiResult},
    files,
    ids::new_id,
    model::{
        Actor, AppendItems, AppendResult, Continuation, Conversation, CreateContinuation,
        CreateConversation, CreateTurn, FileDelivery, Item, ReplayRequest, ReplayResult,
        STORED_RESPONSE_MAX_BYTES, STORED_RESPONSE_SCHEMA, StartTurn, StartTurnResult,
        StoreResponse, StrictJson, Turn, UpdateConversation, UpdateTurn,
        validate_json_number_tokens,
    },
};

#[derive(Serialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
enum TurnStartDigest<'a> {
    Existing {
        operation: &'static str,
        version: i16,
        conversation_id: &'a str,
        agent_ref: &'a str,
        items: &'a [Value],
    },
    Create {
        operation: &'static str,
        version: i16,
        conversation: &'a CreateConversation,
        agent_ref: &'a str,
        items: &'a [Value],
    },
}

fn coded_conflict(code: &'static str, message: &str) -> ApiError {
    ApiError::CodedConflict {
        code,
        message: message.into(),
    }
}

fn normalize_title(value: Option<&str>) -> ApiResult<String> {
    let title = value.unwrap_or("New conversation").trim();
    if title.is_empty() || title.chars().count() > 200 {
        return Err(ApiError::BadRequest(
            "title must contain 1 to 200 characters".into(),
        ));
    }
    Ok(title.to_owned())
}

fn turn_start_digest_v1(request: &StartTurn) -> ApiResult<Vec<u8>> {
    let digest_input = match (
        request.conversation_id.as_deref(),
        request.conversation.as_ref(),
    ) {
        (Some(conversation_id), None) => TurnStartDigest::Existing {
            operation: "turn_start",
            version: 1,
            conversation_id,
            agent_ref: &request.agent_ref,
            items: &request.items,
        },
        (None, Some(conversation)) => TurnStartDigest::Create {
            operation: "turn_start",
            version: 1,
            conversation,
            agent_ref: &request.agent_ref,
            items: &request.items,
        },
        _ => unreachable!("conversation mode validated"),
    };
    Ok(Sha256::digest(
        serde_json_canonicalizer::to_vec(&digest_input)
            .map_err(|error| ApiError::BadRequest(format!("invalid request JSON: {error}")))?,
    )
    .to_vec())
}

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
    let title = normalize_title(request.title.as_deref())?;
    Ok(sqlx::query_as::<_, Conversation>(
        "INSERT INTO conversations (id, tenant_id, owner_ref, title, metadata)
         VALUES ($1, $2, $3, $4, $5) RETURNING *",
    )
    .bind(new_id("conv"))
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .bind(title)
    .bind(request.metadata)
    .fetch_one(pool)
    .await?)
}

pub async fn list_conversations(
    pool: &PgPool,
    actor: &Actor,
    limit: i64,
) -> ApiResult<Vec<Conversation>> {
    Ok(sqlx::query_as::<_, Conversation>(
        "SELECT * FROM conversations WHERE tenant_id = $1 AND owner_ref = $2
         ORDER BY updated_at DESC LIMIT $3",
    )
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .bind(limit.clamp(1, 1000))
    .fetch_all(pool)
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

pub async fn update_conversation(
    pool: &PgPool,
    actor: &Actor,
    id: &str,
    request: UpdateConversation,
) -> ApiResult<Conversation> {
    if request.title.is_none() && request.metadata.is_none() {
        return Err(ApiError::BadRequest(
            "No conversation changes supplied.".into(),
        ));
    }
    let title = request
        .title
        .as_deref()
        .map(|title| normalize_title(Some(title)))
        .transpose()?;
    if let Some(metadata) = &request.metadata
        && !metadata.is_object()
    {
        return Err(ApiError::BadRequest(
            "metadata must be a JSON object".into(),
        ));
    }
    sqlx::query_as::<_, Conversation>(
        "UPDATE conversations SET title = COALESCE($1, title),
            metadata = CASE WHEN $2::jsonb IS NULL THEN metadata ELSE metadata || $2 END,
            updated_at = now()
         WHERE id = $3 AND tenant_id = $4 AND owner_ref = $5 RETURNING *",
    )
    .bind(title)
    .bind(request.metadata)
    .bind(id)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("Conversation not found.".into()))
}

pub async fn delete_conversation(pool: &PgPool, actor: &Actor, id: &str) -> ApiResult<()> {
    let result = sqlx::query(
        "DELETE FROM conversations WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3",
    )
    .bind(id)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .execute(pool)
    .await?;
    if result.rows_affected() == 0 {
        return Err(ApiError::NotFound("Conversation not found.".into()));
    }
    Ok(())
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

async fn create_turn_file_snapshot(
    tx: &mut Transaction<'_, Postgres>,
    conversation_id: &str,
    turn_id: &str,
) -> ApiResult<()> {
    let snapshot_id = new_id("fsnap");
    sqlx::query(
        "INSERT INTO turn_file_snapshots (id, turn_id, authoritative)
         VALUES ($1, $2, true)",
    )
    .bind(&snapshot_id)
    .bind(turn_id)
    .execute(&mut **tx)
    .await?;
    sqlx::query(
        "INSERT INTO turn_file_snapshot_files (snapshot_id, file_id)
         SELECT $1, item_file.file_id
         FROM conversation_item_files item_file
         JOIN conversation_items item ON item.id = item_file.item_id
         WHERE item.conversation_id = $2
         GROUP BY item_file.file_id",
    )
    .bind(snapshot_id)
    .bind(conversation_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn lock_turn_files(
    tx: &mut Transaction<'_, Postgres>,
    actor: &Actor,
    conversation_id: &str,
    additional_file_ids: &[String],
) -> ApiResult<()> {
    let file_ids = sqlx::query_scalar::<_, String>(
        "SELECT file.id
         FROM files file
         WHERE file.id IN (
             SELECT item_file.file_id
             FROM conversation_item_files item_file
             JOIN conversation_items item ON item.id = item_file.item_id
             WHERE item.conversation_id = $1
             UNION
             SELECT unnest($2::text[])
         )
         AND file.tenant_id = $3 AND file.owner_ref = $4
         ORDER BY file.id
         FOR KEY SHARE",
    )
    .bind(conversation_id)
    .bind(additional_file_ids)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_all(&mut **tx)
    .await?;
    let expected = sqlx::query_scalar::<_, i64>(
        "SELECT count(DISTINCT file_id)
         FROM (
             SELECT item_file.file_id
             FROM conversation_item_files item_file
             JOIN conversation_items item ON item.id = item_file.item_id
             WHERE item.conversation_id = $1
             UNION
             SELECT unnest($2::text[])
         ) referenced_files",
    )
    .bind(conversation_id)
    .bind(additional_file_ids)
    .fetch_one(&mut **tx)
    .await?;
    if file_ids.len() != expected as usize {
        return Err(ApiError::NotFound("File not found.".into()));
    }
    Ok(())
}

pub async fn start_turn(
    pool: &PgPool,
    auth: &crate::auth::AuthContext,
    mut request: StartTurn,
) -> ApiResult<StartTurnResult> {
    request.idempotency_key = request.idempotency_key.trim().to_owned();
    request.agent_ref = request.agent_ref.trim().to_owned();
    if request.idempotency_key.is_empty() || request.idempotency_key.chars().count() > 200 {
        return Err(ApiError::BadRequest(
            "idempotency_key must contain 1 to 200 characters".into(),
        ));
    }
    if request.agent_ref.is_empty() || request.agent_ref.chars().count() > 200 {
        return Err(ApiError::BadRequest(
            "agent_ref must contain 1 to 200 characters".into(),
        ));
    }
    if request.conversation_id.is_some() == request.conversation.is_some() {
        return Err(ApiError::BadRequest(
            "exactly one of conversation_id and conversation is required".into(),
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
    if let Some(conversation) = &mut request.conversation {
        if !conversation.metadata.is_object() {
            return Err(ApiError::BadRequest(
                "metadata must be a JSON object".into(),
            ));
        }
        conversation.title = Some(normalize_title(conversation.title.as_deref())?);
    }

    let request_digest = turn_start_digest_v1(&request)?;
    let lock_key = turn_start_lock_key(
        &auth.tenant_id,
        &auth.principal_id,
        &auth.client_id,
        &request.idempotency_key,
    );

    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_key)
        .execute(&mut *tx)
        .await?;

    if let Some((
        turn_start_id,
        request_version,
        stored_digest,
        conversation_id,
        turn_id,
        first_seq,
        last_seq,
    )) = sqlx::query_as::<_, (String, i16, Vec<u8>, String, String, i64, i64)>(
        "SELECT id, request_version, request_digest, conversation_id, turn_id, first_seq, last_seq
             FROM turn_starts
             WHERE tenant_id = $1 AND owner_ref = $2 AND client_id = $3
               AND idempotency_key = $4",
    )
    .bind(&auth.tenant_id)
    .bind(&auth.principal_id)
    .bind(&auth.client_id)
    .bind(&request.idempotency_key)
    .fetch_optional(&mut *tx)
    .await?
    {
        let request_digest = match request_version {
            1 => turn_start_digest_v1(&request)?,
            _ => {
                return Err(coded_conflict(
                    "idempotency_version_unsupported",
                    "the original request version is not supported by this server",
                ));
            }
        };
        if stored_digest != request_digest {
            if let Some(conversation_id) = &request.conversation_id {
                let owned = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM conversations
                     WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3)",
                )
                .bind(conversation_id)
                .bind(&auth.tenant_id)
                .bind(&auth.principal_id)
                .fetch_one(&mut *tx)
                .await?;
                if !owned {
                    return Err(ApiError::NotFound("Conversation not found.".into()));
                }
            }
            return Err(coded_conflict(
                "idempotency_key_reused",
                "idempotency_key was already used for a different request",
            ));
        }
        let conversation_exists = sqlx::query_scalar::<_, String>(
            "SELECT id FROM conversations
             WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3 FOR UPDATE",
        )
        .bind(&conversation_id)
        .bind(&auth.tenant_id)
        .bind(&auth.principal_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        let children = sqlx::query_as::<_, (i32, String, i64, Option<String>)>(
            "SELECT tsi.ordinal, tsi.item_id, tsi.seq, i.id
             FROM turn_start_items tsi
             LEFT JOIN conversation_items i ON i.id = tsi.item_id
               AND i.conversation_id = $2 AND i.turn_id = $3 AND i.seq = tsi.seq
             WHERE tsi.turn_start_id = $1
             ORDER BY tsi.ordinal",
        )
        .bind(&turn_start_id)
        .bind(&conversation_id)
        .bind(&turn_id)
        .fetch_all(&mut *tx)
        .await?;
        let expected_count = last_seq - first_seq + 1;
        let valid_children = expected_count > 0
            && children.len() == expected_count as usize
            && children.iter().enumerate().all(|(index, child)| {
                child.0 == index as i32
                    && child.2 == first_seq + index as i64
                    && child.3.as_deref() == Some(child.1.as_str())
            });
        if !conversation_exists || !valid_children {
            return Err(coded_conflict(
                "idempotency_result_deleted",
                "the original turn start result is no longer available",
            ));
        }
        let item_ids = children
            .into_iter()
            .map(|(_, item_id, _, _)| item_id)
            .collect();
        tx.commit().await?;
        return Ok(StartTurnResult {
            conversation_id,
            turn_id,
            item_ids,
            first_seq,
            last_seq,
            replayed: true,
        });
    }

    let conversation = if let Some(conversation_id) = &request.conversation_id {
        lock_conversation(&mut tx, auth, conversation_id).await?
    } else {
        let conversation = request
            .conversation
            .as_ref()
            .expect("validated conversation");
        sqlx::query_as::<_, Conversation>(
            "INSERT INTO conversations (id, tenant_id, owner_ref, title, metadata)
             VALUES ($1, $2, $3, $4, $5) RETURNING *",
        )
        .bind(new_id("conv"))
        .bind(&auth.tenant_id)
        .bind(&auth.principal_id)
        .bind(conversation.title.as_deref().expect("normalized title"))
        .bind(&conversation.metadata)
        .fetch_one(&mut *tx)
        .await?
    };

    let active_turn_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM turns WHERE conversation_id = $1
         AND status IN ('pending', 'streaming'))",
    )
    .bind(&conversation.id)
    .fetch_one(&mut *tx)
    .await?;
    if active_turn_exists {
        return Err(coded_conflict(
            "active_turn_exists",
            "conversation already has an active turn",
        ));
    }

    let mut file_ids = request
        .items
        .iter()
        .flat_map(referenced_file_ids)
        .collect::<Vec<_>>();
    file_ids.sort();
    file_ids.dedup();
    lock_turn_files(&mut tx, auth, &conversation.id, &file_ids).await?;

    let turn_id = new_id("turn");
    let turn_insert = sqlx::query(
        "INSERT INTO turns (id, conversation_id, agent_ref, idempotency_key)
         VALUES ($1, $2, $3, NULL)",
    )
    .bind(&turn_id)
    .bind(&conversation.id)
    .bind(&request.agent_ref)
    .execute(&mut *tx)
    .await;
    if let Err(error) = turn_insert {
        if error
            .as_database_error()
            .and_then(|error| error.constraint())
            == Some("turns_one_active_per_conversation_idx")
        {
            return Err(coded_conflict(
                "active_turn_exists",
                "conversation already has an active turn",
            ));
        }
        return Err(error.into());
    }
    create_turn_file_snapshot(&mut tx, &conversation.id, &turn_id).await?;

    let item_count = i64::try_from(request.items.len())
        .map_err(|_| coded_conflict("sequence_space_exhausted", "sequence space exhausted"))?;
    let next_seq = conversation
        .next_seq
        .checked_add(item_count)
        .ok_or_else(|| coded_conflict("sequence_space_exhausted", "sequence space exhausted"))?;
    let first_seq = conversation.next_seq;
    let last_seq = next_seq - 1;
    let mut item_ids = Vec::with_capacity(request.items.len());
    for (offset, payload) in request.items.into_iter().enumerate() {
        let file_ids = referenced_file_ids(&payload);
        let item_id = new_id("item");
        sqlx::query(
            "INSERT INTO conversation_items
             (id, conversation_id, turn_id, seq, source, payload)
             VALUES ($1, $2, $3, $4, 'user', $5)",
        )
        .bind(&item_id)
        .bind(&conversation.id)
        .bind(&turn_id)
        .bind(first_seq + offset as i64)
        .bind(payload)
        .execute(&mut *tx)
        .await?;
        for file_id in file_ids {
            sqlx::query(
                "INSERT INTO conversation_item_files (item_id, file_id)
                 VALUES ($1, $2)",
            )
            .bind(&item_id)
            .bind(file_id)
            .execute(&mut *tx)
            .await?;
        }
        item_ids.push(item_id);
    }
    sqlx::query("UPDATE conversations SET next_seq = $2, updated_at = now() WHERE id = $1")
        .bind(&conversation.id)
        .bind(next_seq)
        .execute(&mut *tx)
        .await?;

    let turn_start_id = new_id("tstart");
    sqlx::query(
        "INSERT INTO turn_starts
         (id, tenant_id, owner_ref, client_id, idempotency_key, request_version,
          request_digest, conversation_id, turn_id, first_seq, last_seq)
         VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9, $10)",
    )
    .bind(&turn_start_id)
    .bind(&auth.tenant_id)
    .bind(&auth.principal_id)
    .bind(&auth.client_id)
    .bind(&request.idempotency_key)
    .bind(request_digest)
    .bind(&conversation.id)
    .bind(&turn_id)
    .bind(first_seq)
    .bind(last_seq)
    .execute(&mut *tx)
    .await?;
    for (ordinal, item_id) in item_ids.iter().enumerate() {
        sqlx::query(
            "INSERT INTO turn_start_items (turn_start_id, ordinal, item_id, seq)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(&turn_start_id)
        .bind(ordinal as i32)
        .bind(item_id)
        .bind(first_seq + ordinal as i64)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;

    Ok(StartTurnResult {
        conversation_id: conversation.id,
        turn_id,
        item_ids,
        first_seq,
        last_seq,
        replayed: false,
    })
}

fn turn_start_lock_key(tenant: &str, owner: &str, client: &str, key: &str) -> i64 {
    let mut digest = Sha256::new();
    digest.update(b"threadmark:turn-start-lock:v1\0");
    for value in [tenant, owner, client, key] {
        digest.update((value.len() as u32).to_be_bytes());
        digest.update(value.as_bytes());
    }
    i64::from_be_bytes(
        digest.finalize()[..8]
            .try_into()
            .expect("eight digest bytes"),
    )
}

fn stored_response_lock_key(tenant: &str, owner: &str, agent: &str, response_id: &str) -> i64 {
    let mut digest = Sha256::new();
    digest.update(b"threadmark:stored-response-lock:v1\0");
    for value in [tenant, owner, agent, response_id] {
        digest.update((value.len() as u32).to_be_bytes());
        digest.update(value.as_bytes());
    }
    i64::from_be_bytes(
        digest.finalize()[..8]
            .try_into()
            .expect("eight digest bytes"),
    )
}

#[cfg(test)]
mod turn_start_tests {
    use super::turn_start_lock_key;

    #[test]
    fn turn_start_lock_key_is_stable_and_framed() {
        assert_eq!(
            turn_start_lock_key("tenant", "owner", "client", "request"),
            turn_start_lock_key("tenant", "owner", "client", "request")
        );
        assert_ne!(
            turn_start_lock_key("ab", "c", "client", "request"),
            turn_start_lock_key("a", "bc", "client", "request")
        );
        assert_ne!(
            turn_start_lock_key("tenant", "owner", "client-a", "request"),
            turn_start_lock_key("tenant", "owner", "client-b", "request")
        );
    }
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

    let mut file_ids = request
        .items
        .iter()
        .flat_map(referenced_file_ids)
        .collect::<Vec<_>>();
    file_ids.sort();
    file_ids.dedup();
    for file_id in &file_ids {
        let exists = sqlx::query_scalar::<_, String>(
            "SELECT id FROM files
             WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3 FOR KEY SHARE",
        )
        .bind(file_id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&mut *tx)
        .await?
        .is_some();
        if !exists {
            return Err(ApiError::NotFound("File not found.".into()));
        }
    }

    let first_seq = conversation.next_seq;
    let last_seq = first_seq + request.items.len() as i64 - 1;
    let mut inserted = Vec::with_capacity(request.items.len());
    for (offset, payload) in request.items.into_iter().enumerate() {
        let file_ids = referenced_file_ids(&payload);
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
        let item_id = &inserted.last().expect("item was inserted").id;
        for file_id in file_ids {
            sqlx::query(
                "INSERT INTO conversation_item_files (item_id, file_id)
                 VALUES ($1, $2)",
            )
            .bind(item_id)
            .bind(file_id)
            .execute(&mut *tx)
            .await?;
        }
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

fn referenced_file_ids(value: &Value) -> Vec<String> {
    fn visit(value: &Value, ids: &mut Vec<String>) {
        match value {
            Value::String(value) => {
                if let Some(id) = files::parse_uri(value) {
                    ids.push(id.to_owned());
                }
            }
            Value::Array(values) => {
                for value in values {
                    visit(value, ids);
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    visit(value, ids);
                }
            }
            _ => {}
        }
    }
    let mut ids = Vec::new();
    visit(value, &mut ids);
    ids.sort();
    ids.dedup();
    ids
}

pub async fn replay(
    state: &AppState,
    actor: &Actor,
    conversation_id: &str,
    request: ReplayRequest,
) -> ApiResult<ReplayResult> {
    let conversation = get_conversation(&state.pool, actor, conversation_id).await?;
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
    .fetch_all(&state.pool)
    .await?;
    let mut input = project_replay_items(rows, request.strip_top_level_ids);
    if request.file_delivery != FileDelivery::Preserve {
        for item in &mut input {
            hydrate_file_references(state, actor, item, request.file_delivery).await?;
        }
    }
    Ok(ReplayResult {
        conversation_id: conversation_id.into(),
        through_seq,
        input,
    })
}

async fn hydrate_file_references(
    state: &AppState,
    actor: &Actor,
    item: &mut Value,
    delivery: FileDelivery,
) -> ApiResult<()> {
    let Some(content) = item
        .as_object_mut()
        .and_then(|item| item.get_mut("content"))
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };
    for part in content {
        let Some(object) = part.as_object_mut() else {
            continue;
        };
        let part_type = object.get("type").and_then(Value::as_str);
        let field = match part_type {
            Some("input_image") => "image_url",
            Some("input_file") => "file_url",
            _ => continue,
        };
        let Some(file_id) = object
            .get(field)
            .and_then(Value::as_str)
            .and_then(files::parse_uri)
            .map(str::to_owned)
        else {
            continue;
        };
        let file = files::get_owned(&state.pool, actor, &file_id).await?;
        match delivery {
            FileDelivery::Preserve => {}
            FileDelivery::CapabilityUrl => {
                object.insert(
                    field.into(),
                    Value::String(
                        capability::file_url(
                            &state.config,
                            actor,
                            &file_id,
                            crate::model::DownloadDelivery::Proxy,
                        )
                        .url,
                    ),
                );
            }
            FileDelivery::PresignedUrl => {
                let url = state
                    .object_store
                    .presigned_get(
                        &file.storage_key,
                        state.config.capability_ttl_seconds,
                        Some(&file.mime_type),
                        None,
                    )
                    .await
                    .map_err(ApiError::ObjectStore)?
                    .ok_or_else(|| {
                        ApiError::BadRequest("presigned_url delivery requires S3_PUBLIC_URL".into())
                    })?;
                object.insert(field.into(), Value::String(url));
            }
            FileDelivery::Inline => {
                let (_, bytes) =
                    files::bytes(&state.pool, &state.object_store, actor, &file_id).await?;
                if part_type == Some("input_image") {
                    object.insert(
                        "image_url".into(),
                        Value::String(format!(
                            "data:{};base64,{}",
                            file.mime_type,
                            STANDARD.encode(bytes)
                        )),
                    );
                } else {
                    object.remove("file_url");
                    object.insert("file_data".into(), Value::String(STANDARD.encode(bytes)));
                    object
                        .entry("filename")
                        .or_insert_with(|| Value::String(file.filename));
                }
            }
        }
    }
    Ok(())
}

fn project_replay_items(items: Vec<Value>, strip_top_level_ids: bool) -> Vec<Value> {
    items
        .into_iter()
        .filter_map(|mut item| {
            let object = item.as_object_mut()?;
            if strip_top_level_ids {
                object.remove("id");
            }
            Some(item)
        })
        .collect()
}

pub async fn create_turn(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
    request: CreateTurn,
) -> ApiResult<Turn> {
    let agent_ref = request.agent_ref.trim();
    let idempotency_key = request.idempotency_key.as_str();
    let mut tx = pool.begin().await?;
    lock_conversation(&mut tx, actor, conversation_id).await?;
    if let Some(turn) = sqlx::query_as::<_, Turn>(
        "SELECT * FROM turns WHERE conversation_id = $1 AND idempotency_key = $2",
    )
    .bind(conversation_id)
    .bind(idempotency_key)
    .fetch_optional(&mut *tx)
    .await?
    {
        if turn.agent_ref != request.agent_ref && turn.agent_ref != agent_ref {
            return Err(coded_conflict(
                "idempotency_key_reused",
                "idempotency_key was already used for a different agent",
            ));
        }
        tx.commit().await?;
        return Ok(turn);
    }
    if agent_ref.is_empty()
        || agent_ref.chars().count() > 200
        || idempotency_key.trim().is_empty()
        || idempotency_key.chars().count() > 200
    {
        return Err(ApiError::BadRequest(
            "agent_ref and idempotency_key must contain 1 to 200 characters".into(),
        ));
    }
    let active = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM turns WHERE conversation_id = $1
         AND status IN ('pending', 'streaming'))",
    )
    .bind(conversation_id)
    .fetch_one(&mut *tx)
    .await?;
    if active {
        return Err(coded_conflict(
            "active_turn_exists",
            "conversation already has an active turn",
        ));
    }
    lock_turn_files(&mut tx, actor, conversation_id, &[]).await?;
    let turn = sqlx::query_as::<_, Turn>(
        "INSERT INTO turns (id, conversation_id, agent_ref, idempotency_key)
         VALUES ($1, $2, $3, $4) RETURNING *",
    )
    .bind(new_id("turn"))
    .bind(conversation_id)
    .bind(agent_ref)
    .bind(idempotency_key)
    .fetch_one(&mut *tx)
    .await;
    let turn = match turn {
        Ok(turn) => turn,
        Err(error)
            if error
                .as_database_error()
                .and_then(|error| error.constraint())
                == Some("turns_one_active_per_conversation_idx") =>
        {
            return Err(coded_conflict(
                "active_turn_exists",
                "conversation already has an active turn",
            ));
        }
        Err(error) => return Err(error.into()),
    };
    create_turn_file_snapshot(&mut tx, conversation_id, &turn.id).await?;
    tx.commit().await?;
    Ok(turn)
}

pub async fn list_turns(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
) -> ApiResult<Vec<Turn>> {
    get_conversation(pool, actor, conversation_id).await?;
    Ok(sqlx::query_as::<_, Turn>(
        "SELECT * FROM turns WHERE conversation_id = $1 ORDER BY created_at ASC",
    )
    .bind(conversation_id)
    .fetch_all(pool)
    .await?)
}

pub async fn active_turn(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
) -> ApiResult<Option<Turn>> {
    get_conversation(pool, actor, conversation_id).await?;
    Ok(sqlx::query_as::<_, Turn>(
        "SELECT * FROM turns WHERE conversation_id = $1
         AND status IN ('pending', 'streaming') ORDER BY created_at DESC LIMIT 1",
    )
    .bind(conversation_id)
    .fetch_optional(pool)
    .await?)
}

pub async fn get_turn(pool: &PgPool, actor: &Actor, id: &str) -> ApiResult<Turn> {
    sqlx::query_as::<_, Turn>(
        "SELECT t.* FROM turns t JOIN conversations c ON c.id = t.conversation_id
         WHERE t.id = $1 AND c.tenant_id = $2 AND c.owner_ref = $3",
    )
    .bind(id)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("Turn not found.".into()))
}

pub async fn truncate_conversation(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
    item_id: &str,
) -> ApiResult<()> {
    let mut tx = pool.begin().await?;
    lock_conversation(&mut tx, actor, conversation_id).await?;
    let seq = sqlx::query_scalar::<_, i64>(
        "SELECT seq FROM conversation_items WHERE id = $1 AND conversation_id = $2",
    )
    .bind(item_id)
    .bind(conversation_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::NotFound("Item not found in conversation.".into()))?;
    sqlx::query("DELETE FROM conversation_items WHERE conversation_id = $1 AND seq >= $2")
        .bind(conversation_id)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM append_batches WHERE conversation_id = $1 AND last_seq >= $2")
        .bind(conversation_id)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
    sqlx::query("DELETE FROM continuations WHERE conversation_id = $1 AND through_seq >= $2")
        .bind(conversation_id)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE conversations SET next_seq = $2, updated_at = now() WHERE id = $1")
        .bind(conversation_id)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn regenerate_conversation(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
) -> ApiResult<Option<String>> {
    let mut tx = pool.begin().await?;
    lock_conversation(&mut tx, actor, conversation_id).await?;
    let turn_id = sqlx::query_scalar::<_, String>(
        "SELECT id FROM turns WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1",
    )
    .bind(conversation_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(turn_id) = &turn_id {
        sqlx::query("DELETE FROM conversation_items WHERE turn_id = $1 AND source = 'agent'")
            .bind(turn_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM continuations WHERE conversation_id = $1")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE conversations SET updated_at = now() WHERE id = $1")
            .bind(conversation_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(turn_id)
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
         (id, tenant_id, owner_ref, conversation_id, agent_ref, response_id,
          parent_response_id, through_seq, state)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (tenant_id, owner_ref, agent_ref, response_id) DO NOTHING RETURNING *",
    )
    .bind(new_id("cont"))
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
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
        "SELECT * FROM continuations
         WHERE response_id = $1 AND agent_ref = $2
           AND tenant_id = $3 AND owner_ref = $4",
    )
    .bind(response_id)
    .bind(agent_ref)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("Continuation not found.".into()))
}

#[derive(Debug, FromRow)]
struct StoredResponseRow {
    tenant_id: String,
    owner_ref: String,
    agent_ref: String,
    conversation_id: String,
    turn_id: String,
    response_id: String,
    previous_response_id: Option<String>,
    continuation_parent_response_id: Option<String>,
    terminal_status: String,
    public_response: Value,
    public_response_text: String,
    canonical_digest: Vec<u8>,
    schema_marker: String,
    canonical_size: i64,
    through_seq: i64,
    response_created_at: DateTime<Utc>,
    terminal_at: DateTime<Utc>,
    state: Option<Value>,
}

struct ValidatedResponse {
    value: Value,
    public_response_text: String,
    response_id: String,
    previous_response_id: Option<String>,
    status: String,
    digest: Vec<u8>,
    size: i64,
}

fn validate_public_response(
    raw: &serde_json::value::RawValue,
    schema_marker: &str,
) -> ApiResult<ValidatedResponse> {
    if schema_marker != STORED_RESPONSE_SCHEMA {
        return Err(ApiError::BadRequest(format!(
            "schema_marker must be {STORED_RESPONSE_SCHEMA}"
        )));
    }
    if raw.get().len() > STORED_RESPONSE_MAX_BYTES {
        return Err(ApiError::PayloadTooLarge(format!(
            "public_response exceeds {STORED_RESPONSE_MAX_BYTES} bytes"
        )));
    }
    validate_json_number_tokens(raw.get().as_bytes())
        .map_err(|error| ApiError::BadRequest(format!("invalid public_response JSON: {error}")))?;
    let mut deserializer = serde_json::Deserializer::from_str(raw.get());
    let StrictJson(value) = StrictJson::deserialize(&mut deserializer)
        .map_err(|error| ApiError::BadRequest(format!("invalid public_response JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| ApiError::BadRequest(format!("invalid public_response JSON: {error}")))?;
    let object = value.as_object().ok_or_else(|| {
        ApiError::BadRequest("public_response must be a JSON object".into())
    })?;
    if object.get("object").and_then(Value::as_str) != Some("response") {
        return Err(ApiError::BadRequest(
            "public_response.object must be response".into(),
        ));
    }
    let response_id = bounded_response_id(object.get("id"), "public_response.id")?;
    let status = object
        .get("status")
        .and_then(Value::as_str)
        .filter(|status| matches!(*status, "completed" | "incomplete" | "failed" | "cancelled"))
        .ok_or_else(|| ApiError::BadRequest("public_response.status must be terminal".into()))?
        .to_owned();
    let previous_response_id = match object.get("previous_response_id") {
        None | Some(Value::Null) => None,
        Some(value) => Some(bounded_response_id(
            Some(value),
            "public_response.previous_response_id",
        )?),
    };
    let canonical = serde_json_canonicalizer::to_vec(&value)
        .map_err(|error| ApiError::BadRequest(format!("invalid public_response JSON: {error}")))?;
    if canonical.len() > STORED_RESPONSE_MAX_BYTES {
        return Err(ApiError::PayloadTooLarge(format!(
            "public_response exceeds {STORED_RESPONSE_MAX_BYTES} bytes"
        )));
    }
    Ok(ValidatedResponse {
        value,
        public_response_text: raw.get().to_owned(),
        response_id,
        previous_response_id,
        status,
        digest: Sha256::digest(&canonical).to_vec(),
        size: canonical.len() as i64,
    })
}

fn bounded_response_id(value: Option<&Value>, field: &str) -> ApiResult<String> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.chars().count() <= 200)
        .map(str::to_owned)
        .ok_or_else(|| ApiError::BadRequest(format!("{field} must contain 1 to 200 characters")))
}

pub async fn store_response(
    pool: &PgPool,
    actor: &Actor,
    conversation_id: &str,
    mut request: StoreResponse,
) -> ApiResult<(bool, String)> {
    request.agent_ref = request.agent_ref.trim().to_owned();
    request.turn_id = request.turn_id.trim().to_owned();
    if request.agent_ref.is_empty() || request.agent_ref.chars().count() > 200 {
        return Err(ApiError::BadRequest(
            "agent_ref must contain 1 to 200 characters".into(),
        ));
    }
    if request.turn_id.is_empty() || request.turn_id.chars().count() > 200 {
        return Err(ApiError::BadRequest(
            "turn_id must contain 1 to 200 characters".into(),
        ));
    }
    if request.terminal_at < request.response_created_at {
        return Err(ApiError::BadRequest(
            "terminal_at must not precede response_created_at".into(),
        ));
    }
    let validated = validate_public_response(&request.public_response, &request.schema_marker)?;
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(stored_response_lock_key(
            &actor.tenant_id,
            &actor.principal_id,
            &request.agent_ref,
            &validated.response_id,
        ))
        .execute(&mut *tx)
        .await?;
    let conversation = lock_conversation(&mut tx, actor, conversation_id).await?;
    let through_seq = request.through_seq.unwrap_or(conversation.next_seq - 1);
    if through_seq < 0 || through_seq >= conversation.next_seq {
        return Err(ApiError::BadRequest(
            "through_seq is outside the conversation transcript".into(),
        ));
    }

    let turn = sqlx::query_as::<_, (String, String, Option<String>, Option<DateTime<Utc>>)>(
        "SELECT agent_ref, status, response_id, completed_at FROM turns
         WHERE id = $1 AND conversation_id = $2 FOR UPDATE",
    )
    .bind(&request.turn_id)
    .bind(conversation_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| ApiError::NotFound("Turn not found.".into()))?;
    if turn.0 != request.agent_ref
        || turn.1 != validated.status
        || turn.2.as_deref() != Some(validated.response_id.as_str())
        || turn.3.is_none()
    {
        return Err(ApiError::BadRequest(
            "public_response does not match the terminal turn".into(),
        ));
    }

    if let Some(parent) = &validated.previous_response_id {
        let parent_exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM continuations
             WHERE tenant_id = $1 AND owner_ref = $2 AND conversation_id = $3
               AND agent_ref = $4 AND response_id = $5)",
        )
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .bind(conversation_id)
        .bind(&request.agent_ref)
        .bind(parent)
        .fetch_one(&mut *tx)
        .await?;
        if !parent_exists {
            return Err(ApiError::NotFound("Previous response not found.".into()));
        }
    }

    let existing = sqlx::query_as::<_, StoredResponseRow>(
        "SELECT response.tenant_id, response.owner_ref, response.agent_ref,
                response.conversation_id, response.turn_id, response.response_id,
                response.previous_response_id,
                continuation.parent_response_id AS continuation_parent_response_id,
                response.terminal_status,
                response.public_response, response.public_response_text,
                response.canonical_digest,
                response.schema_marker, response.canonical_size, response.through_seq,
                response.response_created_at, response.terminal_at, continuation.state
         FROM stored_responses response
         JOIN continuations continuation ON continuation.id = response.continuation_id
         WHERE response.tenant_id = $1 AND response.owner_ref = $2
           AND response.agent_ref = $3 AND response.response_id = $4",
    )
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .bind(&request.agent_ref)
    .bind(&validated.response_id)
    .fetch_optional(&mut *tx)
    .await?;
    if let Some(existing) = existing {
        validate_stored_response_row(&existing, actor, &request.agent_ref)?;
        let retry_through_seq = request.through_seq.unwrap_or(existing.through_seq);
        let identical = existing.conversation_id == conversation_id
            && existing.turn_id == request.turn_id
            && existing.previous_response_id == validated.previous_response_id
            && existing.terminal_status == validated.status
            && existing.canonical_digest == validated.digest
            && existing.schema_marker == request.schema_marker
            && existing.canonical_size == validated.size
            && existing.through_seq == retry_through_seq
            && existing.response_created_at == request.response_created_at
            && existing.terminal_at == request.terminal_at
            && existing.state == request.state;
        if !identical {
            return Err(ApiError::Conflict(
                "Response ID already stores a different terminal response.".into(),
            ));
        }
        tx.commit().await?;
        return Ok((true, existing.public_response_text));
    }

    let inserted_continuation_id = sqlx::query_scalar::<_, String>(
        "INSERT INTO continuations
         (id, tenant_id, owner_ref, conversation_id, turn_id, agent_ref,
          response_id, parent_response_id, through_seq, state)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          ON CONFLICT (tenant_id, owner_ref, agent_ref, response_id) DO NOTHING
          RETURNING id",
    )
    .bind(new_id("cont"))
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .bind(conversation_id)
    .bind(&request.turn_id)
    .bind(&request.agent_ref)
    .bind(&validated.response_id)
    .bind(&validated.previous_response_id)
    .bind(through_seq)
    .bind(&request.state)
    .fetch_optional(&mut *tx)
    .await?;
    let continuation_id = if let Some(id) = inserted_continuation_id {
        id
    } else {
        let existing = sqlx::query_as::<
            _,
            (String, String, Option<String>, Option<String>, i64, Option<Value>),
        >(
            "SELECT id, conversation_id, turn_id, parent_response_id, through_seq, state
             FROM continuations
             WHERE tenant_id = $1 AND owner_ref = $2 AND agent_ref = $3
               AND response_id = $4",
        )
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .bind(&request.agent_ref)
        .bind(&validated.response_id)
        .fetch_one(&mut *tx)
        .await?;
        if existing.1 != conversation_id
            || existing.2.as_deref() != Some(request.turn_id.as_str())
            || existing.3 != validated.previous_response_id
            || existing.4 != through_seq
            || existing.5 != request.state
        {
            return Err(ApiError::Conflict(
                "Response ID already stores a different continuation.".into(),
            ));
        }
        existing.0
    };
    sqlx::query(
        "INSERT INTO stored_responses
         (id, tenant_id, owner_ref, agent_ref, conversation_id, turn_id,
          continuation_id, response_id, previous_response_id, terminal_status,
          public_response, public_response_text, canonical_digest, schema_marker, canonical_size,
          through_seq, response_created_at, terminal_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)",
    )
    .bind(new_id("sresp"))
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .bind(&request.agent_ref)
    .bind(conversation_id)
    .bind(&request.turn_id)
    .bind(continuation_id)
    .bind(&validated.response_id)
    .bind(&validated.previous_response_id)
    .bind(&validated.status)
    .bind(&validated.value)
    .bind(&validated.public_response_text)
    .bind(&validated.digest)
    .bind(&request.schema_marker)
    .bind(validated.size)
    .bind(through_seq)
    .bind(request.response_created_at)
    .bind(request.terminal_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok((false, validated.public_response_text))
}

pub async fn get_stored_response(
    pool: &PgPool,
    actor: &Actor,
    response_id: &str,
    agent_ref: &str,
) -> ApiResult<String> {
    let row = sqlx::query_as::<_, StoredResponseRow>(
        "SELECT response.tenant_id, response.owner_ref, response.agent_ref,
                response.conversation_id, response.turn_id, response.response_id,
                response.previous_response_id,
                continuation.parent_response_id AS continuation_parent_response_id,
                response.terminal_status,
                response.public_response, response.public_response_text,
                response.canonical_digest,
                response.schema_marker, response.canonical_size, response.through_seq,
                response.response_created_at, response.terminal_at, continuation.state
         FROM stored_responses response
         JOIN continuations continuation ON continuation.id = response.continuation_id
         WHERE response.response_id = $1 AND response.agent_ref = $2
           AND response.tenant_id = $3 AND response.owner_ref = $4",
    )
    .bind(response_id)
    .bind(agent_ref)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("Response not found.".into()))?;

    validate_stored_response_row(&row, actor, agent_ref)?;
    Ok(row.public_response_text)
}

fn validate_stored_response_row(
    row: &StoredResponseRow,
    actor: &Actor,
    agent_ref: &str,
) -> ApiResult<()> {
    let raw = serde_json::value::RawValue::from_string(row.public_response_text.clone())
        .map_err(|_| ApiError::CorruptStoredResponse)?;
    let validated = validate_public_response(&raw, &row.schema_marker)
        .map_err(|_| ApiError::CorruptStoredResponse)?;
    if row.tenant_id != actor.tenant_id
        || row.owner_ref != actor.principal_id
        || row.agent_ref != agent_ref
        || row.response_id != validated.response_id
        || row.previous_response_id != validated.previous_response_id
        || row.continuation_parent_response_id != validated.previous_response_id
        || row.terminal_status != validated.status
        || row.canonical_digest != validated.digest
        || row.canonical_size != validated.size
        || row.public_response != validated.value
        || row.response_created_at > row.terminal_at
    {
        return Err(ApiError::CorruptStoredResponse);
    }
    Ok(())
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

    #[test]
    fn replay_preserves_multimodal_content_verbatim() {
        let item = json!({
            "type": "message",
            "id": "msg_provider",
            "role": "user",
            "content": [
                {
                    "type": "input_image",
                    "image_url": "data:image/png;base64,iVBORw0KGgo=",
                    "detail": "high"
                },
                {
                    "type": "input_file",
                    "filename": "brief.pdf",
                    "file_url": "threadmark://files/file_01k2example",
                    "metadata": { "id": "nested-id-is-protocol-data" }
                },
                {
                    "type": "input_audio",
                    "audio_url": "https://media.example.test/sample.wav",
                    "format": "wav"
                }
            ]
        });

        let replayed = project_replay_items(vec![item], true);

        assert_eq!(
            replayed,
            vec![json!({
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_image",
                        "image_url": "data:image/png;base64,iVBORw0KGgo=",
                        "detail": "high"
                    },
                    {
                        "type": "input_file",
                        "filename": "brief.pdf",
                        "file_url": "threadmark://files/file_01k2example",
                        "metadata": { "id": "nested-id-is-protocol-data" }
                    },
                    {
                        "type": "input_audio",
                        "audio_url": "https://media.example.test/sample.wav",
                        "format": "wav"
                    }
                ]
            })]
        );
    }

    #[test]
    fn replay_can_preserve_multimodal_item_ids() {
        let item = json!({
            "type": "message",
            "id": "msg_provider",
            "role": "user",
            "content": [{
                "type": "input_image",
                "image_url": "https://media.example.test/image.png"
            }]
        });

        assert_eq!(project_replay_items(vec![item.clone()], false), vec![item]);
    }

    #[test]
    fn finds_threadmark_file_references_in_opaque_items() {
        let value = json!({
            "content": [
                { "image_url": "threadmark://files/file_image" },
                { "file_url": "threadmark://files/file_document" },
                { "nested": ["threadmark://files/file_image"] },
                { "external": "https://example.test/file" }
            ]
        });
        assert_eq!(
            referenced_file_ids(&value),
            vec!["file_document".to_owned(), "file_image".to_owned()]
        );
    }

    #[test]
    fn validates_terminal_public_response_and_digest() {
        let response = serde_json::value::RawValue::from_string(
            r#"{"usage":{"output_tokens":4,"input_tokens":3},"output":[{"type":"message","id":"msg_1"}],"previous_response_id":null,"status":"completed","object":"response","id":"resp_123"}"#.into(),
        )
        .unwrap();
        let validated = validate_public_response(&response, STORED_RESPONSE_SCHEMA).unwrap();
        assert_eq!(validated.response_id, "resp_123");
        assert_eq!(validated.status, "completed");
        assert_eq!(validated.digest.len(), 32);
        assert_eq!(validated.public_response_text, response.get());
    }

    #[test]
    fn rejects_nonterminal_and_oversized_public_responses() {
        let nonterminal = serde_json::value::to_raw_value(&json!({
            "id": "resp_123",
            "object": "response",
            "status": "in_progress"
        }))
        .unwrap();
        assert!(validate_public_response(&nonterminal, STORED_RESPONSE_SCHEMA).is_err());

        let oversized = serde_json::value::to_raw_value(&json!({
            "id": "resp_123",
            "object": "response",
            "status": "completed",
            "output": "x".repeat(STORED_RESPONSE_MAX_BYTES)
        }))
        .unwrap();
        assert!(matches!(
            validate_public_response(&oversized, STORED_RESPONSE_SCHEMA),
            Err(ApiError::PayloadTooLarge(_))
        ));
    }

    #[test]
    fn malformed_stored_response_is_an_internal_integrity_error() {
        let now = Utc::now();
        let row = StoredResponseRow {
            tenant_id: "tenant-a".into(),
            owner_ref: "owner-a".into(),
            agent_ref: "agent-a".into(),
            conversation_id: "conv-a".into(),
            turn_id: "turn-a".into(),
            response_id: "resp-a".into(),
            previous_response_id: None,
            continuation_parent_response_id: None,
            terminal_status: "completed".into(),
            public_response: json!({
                "id": "resp-a",
                "object": "response",
                "status": "completed"
            }),
            public_response_text:
                r#"{"id":"resp-a","object":"response","status":"completed"}"#.into(),
            canonical_digest: vec![0; 32],
            schema_marker: STORED_RESPONSE_SCHEMA.into(),
            canonical_size: 1,
            through_seq: 0,
            response_created_at: now,
            terminal_at: now,
            state: None,
        };
        let actor = Actor {
            tenant_id: "tenant-a".into(),
            principal_id: "owner-a".into(),
        };
        assert!(matches!(
            validate_stored_response_row(&row, &actor, "agent-a"),
            Err(ApiError::CorruptStoredResponse)
        ));
    }
}
