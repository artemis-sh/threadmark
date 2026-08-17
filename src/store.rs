use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Transaction};

use crate::{
    api::AppState,
    capability,
    error::{ApiError, ApiResult},
    files,
    ids::new_id,
    model::{
        Actor, AppendItems, AppendResult, Continuation, Conversation, CreateContinuation,
        CreateConversation, CreateTurn, FileDelivery, FinalizeTurn, FinalizeTurnResult, Item,
        ReplayRequest, ReplayResult, StartTurn, StartTurnResult, Turn, UpdateConversation,
        UpdateTurn,
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
        response_id: &'a str,
        items: &'a [Value],
    },
    Create {
        operation: &'static str,
        version: i16,
        conversation: &'a CreateConversation,
        agent_ref: &'a str,
        response_id: &'a str,
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
            response_id: &request.response_id,
            items: &request.items,
        },
        (None, Some(conversation)) => TurnStartDigest::Create {
            operation: "turn_start",
            version: 1,
            conversation,
            agent_ref: &request.agent_ref,
            response_id: &request.response_id,
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

#[derive(Serialize)]
struct TurnFinalizeDigest<'a> {
    operation: &'static str,
    version: i16,
    response_id: &'a str,
    status: &'a str,
    output_items: &'a [Value],
    response: &'a Value,
    parent_response_id: &'a Option<String>,
    state: &'a Option<Value>,
    error: &'a Option<Value>,
    usage: &'a Option<Value>,
}

fn turn_finalize_digest_v1(request: &FinalizeTurn) -> ApiResult<Vec<u8>> {
    Ok(Sha256::digest(
        serde_json_canonicalizer::to_vec(&TurnFinalizeDigest {
            operation: "turn_finalize",
            version: 1,
            response_id: &request.response_id,
            status: &request.status,
            output_items: &request.output_items,
            response: &request.response,
            parent_response_id: &request.parent_response_id,
            state: &request.state,
            error: &request.error,
            usage: &request.usage,
        })
        .map_err(|error| ApiError::BadRequest(format!("invalid request JSON: {error}")))?,
    )
    .to_vec())
}

fn response_id_is_reserved_or_legacy(
    reserved_response_id: Option<&str>,
    response_id: &str,
) -> bool {
    reserved_response_id.is_none_or(|reserved| reserved == response_id)
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
    request.response_id = request.response_id.trim().to_owned();
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
    if request.response_id.is_empty() || request.response_id.chars().count() > 200 {
        return Err(ApiError::BadRequest(
            "response_id must contain 1 to 200 characters".into(),
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
        "INSERT INTO turns (id, conversation_id, agent_ref, idempotency_key, reserved_response_id)
         VALUES ($1, $2, $3, NULL, $4)",
    )
    .bind(&turn_id)
    .bind(&conversation.id)
    .bind(&request.agent_ref)
    .bind(&request.response_id)
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
        let reserved_response_id = sqlx::query_scalar::<_, Option<String>>(
            "SELECT reserved_response_id FROM turns WHERE id = $1",
        )
        .bind(&turn.id)
        .fetch_one(&mut *tx)
        .await?;
        if reserved_response_id.as_deref() != Some(request.response_id.trim()) {
            return Err(coded_conflict(
                "idempotency_key_reused",
                "idempotency_key was already used for a different response_id",
            ));
        }
        tx.commit().await?;
        return Ok(turn);
    }
    if agent_ref.is_empty()
        || agent_ref.chars().count() > 200
        || idempotency_key.trim().is_empty()
        || idempotency_key.chars().count() > 200
        || request.response_id.trim().is_empty()
        || request.response_id.chars().count() > 200
    {
        return Err(ApiError::BadRequest(
            "agent_ref, idempotency_key, and response_id must contain 1 to 200 characters".into(),
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
        "INSERT INTO turns (id, conversation_id, agent_ref, idempotency_key, reserved_response_id)
         VALUES ($1, $2, $3, $4, $5) RETURNING *",
    )
    .bind(new_id("turn"))
    .bind(conversation_id)
    .bind(agent_ref)
    .bind(idempotency_key)
    .bind(request.response_id.trim())
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
    if matches!(
        request.status.as_str(),
        "completed" | "incomplete" | "failed" | "cancelled"
    ) {
        return Err(ApiError::BadRequest(
            "terminal turn statuses must be set through the finalization endpoint".into(),
        ));
    }
    sqlx::query_as::<_, Turn>(
        "UPDATE turns SET status = $1, response_id = $2, error = $3, usage = $4,
               completed_at = NULL
          WHERE id = $5 AND conversation_id IN
              (SELECT id FROM conversations WHERE tenant_id = $6 AND owner_ref = $7)
            AND status IN ('pending', 'streaming')
          RETURNING *",
    )
    .bind(request.status)
    .bind(request.response_id)
    .bind(request.error)
    .bind(request.usage)
    .bind(turn_id)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("Turn not found.".into()))
}

pub async fn finalize_turn(
    pool: &PgPool,
    auth: &crate::auth::AuthContext,
    turn_id: &str,
    mut request: FinalizeTurn,
) -> ApiResult<FinalizeTurnResult> {
    request.idempotency_key = request.idempotency_key.trim().to_owned();
    request.response_id = request.response_id.trim().to_owned();
    request.status = request.status.trim().to_owned();
    if request.idempotency_key.is_empty()
        || request.idempotency_key.chars().count() > 200
        || request.response_id.is_empty()
        || request.response_id.chars().count() > 200
    {
        return Err(ApiError::BadRequest(
            "idempotency_key and response_id must contain 1 to 200 characters".into(),
        ));
    }
    if !matches!(
        request.status.as_str(),
        "completed" | "incomplete" | "failed" | "cancelled"
    ) {
        return Err(ApiError::BadRequest("status must be terminal".into()));
    }
    if request.output_items.len() > 100 || request.output_items.iter().any(|item| !item.is_object())
    {
        return Err(ApiError::BadRequest(
            "output_items must contain at most 100 JSON objects".into(),
        ));
    }
    if !request.response.is_object() {
        return Err(ApiError::BadRequest(
            "response must be a JSON object".into(),
        ));
    }
    if request
        .parent_response_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.chars().count() > 200)
    {
        return Err(ApiError::BadRequest(
            "parent_response_id must contain 1 to 200 characters when supplied".into(),
        ));
    }
    let request_digest = turn_finalize_digest_v1(&request)?;
    let response_digest = Sha256::digest(
        serde_json_canonicalizer::to_vec(&request.response)
            .map_err(|error| ApiError::BadRequest(format!("invalid response JSON: {error}")))?,
    )
    .to_vec();

    let mut tx = pool.begin().await?;
    // Lock the conversation before the turn, matching all transcript writers.
    let (conversation_id, turn_agent_ref, _, _) =
        sqlx::query_as::<_, (String, String, String, Option<String>)>(
            "SELECT t.conversation_id, t.agent_ref, t.status, t.reserved_response_id
             FROM turns t JOIN conversations c ON c.id = t.conversation_id
             WHERE t.id = $1 AND c.tenant_id = $2 AND c.owner_ref = $3",
        )
        .bind(turn_id)
        .bind(&auth.tenant_id)
        .bind(&auth.principal_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ApiError::NotFound("Turn not found.".into()))?;
    auth.require_agent(&turn_agent_ref)?;
    let conversation = lock_conversation(&mut tx, auth, &conversation_id).await?;
    // Re-read under a row lock after acquiring the conversation lock.
    let (turn_agent_ref, status, reserved_response_id) =
        sqlx::query_as::<_, (String, String, Option<String>)>(
            "SELECT agent_ref, status, reserved_response_id FROM turns WHERE id = $1 FOR UPDATE",
        )
        .bind(turn_id)
        .fetch_one(&mut *tx)
        .await?;
    auth.require_agent(&turn_agent_ref)?;

    if let Some((version, digest, response, first_seq, last_seq, continuation_id)) =
        sqlx::query_as::<_, (i16, Vec<u8>, Value, i64, i64, String)>(
            "SELECT request_version, request_digest, response, first_seq, last_seq, continuation_id
             FROM turn_finalizations WHERE turn_id = $1",
        )
        .bind(turn_id)
        .fetch_optional(&mut *tx)
        .await?
    {
        if version != 1 {
            return Err(coded_conflict(
                "idempotency_version_unsupported",
                "the original finalization version is not supported by this server",
            ));
        }
        if digest != request_digest {
            return Err(coded_conflict(
                "finalization_reused",
                "turn was already finalized with a different request",
            ));
        }
        let turn = sqlx::query_as::<_, Turn>("SELECT * FROM turns WHERE id = $1")
            .bind(turn_id)
            .fetch_one(&mut *tx)
            .await?;
        let continuation =
            sqlx::query_as::<_, Continuation>("SELECT * FROM continuations WHERE id = $1")
                .bind(continuation_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(|| {
                    coded_conflict(
                        "idempotency_result_deleted",
                        "the original finalization result is no longer available",
                    )
                })?;
        let items = if last_seq < first_seq {
            Vec::new()
        } else {
            sqlx::query_as::<_, Item>(
                "SELECT * FROM conversation_items WHERE conversation_id = $1
                 AND turn_id = $2 AND seq BETWEEN $3 AND $4 ORDER BY seq ASC",
            )
            .bind(&conversation_id)
            .bind(turn_id)
            .bind(first_seq)
            .bind(last_seq)
            .fetch_all(&mut *tx)
            .await?
        };
        tx.commit().await?;
        return Ok(FinalizeTurnResult {
            turn,
            items,
            continuation,
            response,
            first_seq,
            last_seq,
            replayed: true,
        });
    }

    // Turns created before reserved_response_id was introduced have no value
    // here. Their first finalization establishes the response ID atomically
    // with the terminal turn update and finalization record below.
    if !response_id_is_reserved_or_legacy(
        reserved_response_id.as_deref(),
        request.response_id.as_str(),
    ) {
        return Err(coded_conflict(
            "response_id_mismatch",
            "response_id does not match the response ID reserved for this turn",
        ));
    }
    if !matches!(status.as_str(), "pending" | "streaming") {
        return Err(coded_conflict(
            "turn_not_active",
            "only an active turn can be finalized",
        ));
    }
    if sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM turn_finalizations
         WHERE tenant_id = $1 AND agent_ref = $2 AND response_id = $3)",
    )
    .bind(&auth.tenant_id)
    .bind(&turn_agent_ref)
    .bind(&request.response_id)
    .fetch_one(&mut *tx)
    .await?
    {
        return Err(coded_conflict(
            "response_id_reused",
            "response_id was already finalized for this agent",
        ));
    }
    if sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM turn_finalizations
         WHERE tenant_id = $1 AND owner_ref = $2 AND agent_ref = $3 AND idempotency_key = $4)",
    )
    .bind(&auth.tenant_id)
    .bind(&auth.principal_id)
    .bind(&turn_agent_ref)
    .bind(&request.idempotency_key)
    .fetch_one(&mut *tx)
    .await?
    {
        return Err(coded_conflict(
            "idempotency_key_reused",
            "idempotency_key was already used to finalize a different turn",
        ));
    }
    if sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM continuations
         WHERE tenant_id = $1 AND agent_ref = $2 AND response_id = $3)",
    )
    .bind(&auth.tenant_id)
    .bind(&turn_agent_ref)
    .bind(&request.response_id)
    .fetch_one(&mut *tx)
    .await?
    {
        return Err(coded_conflict(
            "response_id_reused",
            "response_id already has a continuation for this agent",
        ));
    }

    let mut file_ids = request
        .output_items
        .iter()
        .flat_map(referenced_file_ids)
        .collect::<Vec<_>>();
    file_ids.sort();
    file_ids.dedup();
    lock_turn_files(&mut tx, auth, &conversation_id, &file_ids).await?;

    let first_seq = conversation.next_seq;
    let last_seq = first_seq + request.output_items.len() as i64 - 1;
    let mut items = Vec::with_capacity(request.output_items.len());
    for (offset, payload) in request.output_items.into_iter().enumerate() {
        let file_ids = referenced_file_ids(&payload);
        let item = sqlx::query_as::<_, Item>(
            "INSERT INTO conversation_items (id, conversation_id, turn_id, seq, source, payload)
             VALUES ($1, $2, $3, $4, 'agent', $5) RETURNING *",
        )
        .bind(new_id("item"))
        .bind(&conversation_id)
        .bind(turn_id)
        .bind(first_seq + offset as i64)
        .bind(payload)
        .fetch_one(&mut *tx)
        .await?;
        for file_id in file_ids {
            sqlx::query("INSERT INTO conversation_item_files (item_id, file_id) VALUES ($1, $2)")
                .bind(&item.id)
                .bind(file_id)
                .execute(&mut *tx)
                .await?;
        }
        items.push(item);
    }
    let through_seq = last_seq.max(0);
    let continuation = sqlx::query_as::<_, Continuation>(
        "INSERT INTO continuations
         (id, tenant_id, conversation_id, agent_ref, response_id, parent_response_id, through_seq, state)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (tenant_id, agent_ref, response_id) DO NOTHING RETURNING *",
    )
    .bind(new_id("cont"))
    .bind(&auth.tenant_id)
    .bind(&conversation_id)
    .bind(&turn_agent_ref)
    .bind(&request.response_id)
    .bind(&request.parent_response_id)
    .bind(through_seq)
    .bind(&request.state)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| {
        coded_conflict(
            "response_id_reused",
            "response_id already has a continuation for this agent",
        )
    })?;
    sqlx::query("UPDATE conversations SET next_seq = $2, updated_at = now() WHERE id = $1")
        .bind(&conversation_id)
        .bind(last_seq + 1)
        .execute(&mut *tx)
        .await?;
    let turn = sqlx::query_as::<_, Turn>(
        "UPDATE turns SET status = $1, response_id = $2, reserved_response_id = $2,
         error = $3, usage = $4, completed_at = COALESCE(completed_at, now())
         WHERE id = $5 RETURNING *",
    )
    .bind(&request.status)
    .bind(&request.response_id)
    .bind(&request.error)
    .bind(&request.usage)
    .bind(turn_id)
    .fetch_one(&mut *tx)
    .await?;
    let finalization_insert = sqlx::query(
        "INSERT INTO turn_finalizations
         (turn_id, tenant_id, owner_ref, agent_ref, idempotency_key, response_id, request_version,
          request_digest, response, response_digest, first_seq, last_seq, continuation_id)
         VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, $9, $10, $11, $12)
         ON CONFLICT DO NOTHING",
    )
    .bind(turn_id)
    .bind(&auth.tenant_id)
    .bind(&auth.principal_id)
    .bind(&turn_agent_ref)
    .bind(&request.idempotency_key)
    .bind(&request.response_id)
    .bind(request_digest)
    .bind(&request.response)
    .bind(response_digest)
    .bind(first_seq)
    .bind(last_seq)
    .bind(&continuation.id)
    .execute(&mut *tx)
    .await?;
    if finalization_insert.rows_affected() == 0 {
        return Err(coded_conflict(
            "finalization_conflict",
            "a finalization with this idempotency key or response_id already exists",
        ));
    }
    tx.commit().await?;
    Ok(FinalizeTurnResult {
        turn,
        items,
        continuation,
        response: request.response,
        first_seq,
        last_seq,
        replayed: false,
    })
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
    fn finalization_digest_binds_terminal_outcome_and_response() {
        let request = FinalizeTurn {
            idempotency_key: "finish-1".into(),
            response_id: "resp_1".into(),
            status: "completed".into(),
            output_items: vec![json!({"type": "message", "role": "assistant"})],
            response: json!({"id": "resp_1", "status": "completed"}),
            parent_response_id: None,
            state: Some(json!({"provider_thread": "thread_1"})),
            error: None,
            usage: Some(json!({"total_tokens": 3})),
        };
        let original = turn_finalize_digest_v1(&request).unwrap();
        let mut changed = request;
        changed.status = "failed".into();
        assert_ne!(original, turn_finalize_digest_v1(&changed).unwrap());
        changed.status = "completed".into();
        changed.response["status"] = json!("incomplete");
        assert_ne!(original, turn_finalize_digest_v1(&changed).unwrap());
    }

    #[test]
    fn legacy_turns_can_finalize_with_their_first_response_id() {
        assert!(response_id_is_reserved_or_legacy(None, "resp_1"));
        assert!(response_id_is_reserved_or_legacy(Some("resp_1"), "resp_1"));
        assert!(!response_id_is_reserved_or_legacy(Some("resp_1"), "resp_2"));
    }
}
