use std::sync::Arc;

use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::Utc;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{Decode, Encode, Executor, IntoArguments, Pool, Transaction, Type};

use crate::{
    auth::AuthContext,
    blob::ObjectStore,
    capability,
    config::Config,
    db::{Backend, UniqueIndex, in_list},
    error::{ApiError, ApiResult},
    files,
    ids::new_id,
    model::{
        Actor, AppendItems, AppendResult, Continuation, Conversation, CreateContinuation,
        CreateConversation, CreateTurn, FileDelivery, Item, ReplayRequest, ReplayResult, StartTurn,
        StartTurnResult, Turn, UpdateConversation, UpdateTurn,
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

/// Shallow-merge a metadata patch over the stored object, replacing top-level
/// keys and preserving explicit JSON nulls.
///
/// Mirrors PostgreSQL's `jsonb || jsonb`, which the store previously relied on.
fn merge_metadata(mut base: Value, patch: Value) -> Value {
    match (base.as_object_mut(), patch) {
        (Some(base_object), Value::Object(patch_object)) => {
            for (key, value) in patch_object {
                base_object.insert(key, value);
            }
        }
        (_, patch) => return patch,
    }
    base
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

/// A SQL-backed conversation store.
///
/// One implementation serves both PostgreSQL and SQLite. The bound list below is
/// declared once for the whole store rather than repeated per method; it names
/// every value type Threadmark binds or decodes.
pub struct SqlStore<DB: Backend> {
    pub(crate) pool: Pool<DB>,
}

impl<DB: Backend> SqlStore<DB> {
    pub fn new(pool: Pool<DB>) -> Self {
        Self { pool }
    }

    /// Open a write transaction.
    ///
    /// SQLite needs `BEGIN IMMEDIATE` so the write lock is taken up front; see
    /// [`Backend::BEGIN_WRITE`].
    pub(crate) async fn begin_write(&self) -> Result<Transaction<'_, DB>, sqlx::Error> {
        match DB::BEGIN_WRITE {
            Some(statement) => self.pool.begin_with(statement).await,
            None => self.pool.begin().await,
        }
    }
}

impl<DB> SqlStore<DB>
where
    DB: Backend,
    for<'a> &'a Pool<DB>: Executor<'a, Database = DB>,
    for<'a> &'a mut <DB as sqlx::Database>::Connection: Executor<'a, Database = DB>,
    for<'a> <DB as sqlx::Database>::Arguments<'a>: IntoArguments<'a, DB>,
    for<'a> &'a str: Encode<'a, DB> + Type<DB>,
    for<'a> String: Encode<'a, DB> + Type<DB> + Decode<'a, DB>,
    for<'a> Option<String>: Encode<'a, DB> + Type<DB>,
    for<'a> i16: Encode<'a, DB> + Type<DB> + Decode<'a, DB>,
    for<'a> i32: Encode<'a, DB> + Type<DB> + Decode<'a, DB>,
    for<'a> i64: Encode<'a, DB> + Type<DB> + Decode<'a, DB>,
    for<'a> bool: Encode<'a, DB> + Type<DB> + Decode<'a, DB>,
    for<'a> Vec<u8>: Encode<'a, DB> + Type<DB> + Decode<'a, DB>,
    for<'a> Value: Encode<'a, DB> + Type<DB> + Decode<'a, DB>,
    for<'a> Option<Value>: Encode<'a, DB> + Type<DB>,
    for<'a> chrono::DateTime<chrono::Utc>: Encode<'a, DB> + Type<DB> + Decode<'a, DB>,
    for<'a> Conversation: sqlx::FromRow<'a, DB::Row>,
    for<'a> Turn: sqlx::FromRow<'a, DB::Row>,
    for<'a> Item: sqlx::FromRow<'a, DB::Row>,
    for<'a> Continuation: sqlx::FromRow<'a, DB::Row>,
    for<'a> crate::model::FileRecord: sqlx::FromRow<'a, DB::Row>,
    usize: sqlx::ColumnIndex<DB::Row>,
    for<'a> &'a str: sqlx::ColumnIndex<DB::Row>,
{
    /// Confirm the database is reachable.
    pub async fn ping(&self) -> ApiResult<()> {
        sqlx::query_scalar::<DB, i32>("SELECT 1")
            .fetch_one(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn create_conversation(
        &self,
        actor: &Actor,
        request: CreateConversation,
    ) -> ApiResult<Conversation> {
        if !request.metadata.is_object() {
            return Err(ApiError::BadRequest(
                "metadata must be a JSON object".into(),
            ));
        }
        let title = normalize_title(request.title.as_deref())?;
        Ok(sqlx::query_as::<DB, Conversation>(
            "INSERT INTO conversations (id, tenant_id, owner_ref, title, metadata)
             VALUES ($1, $2, $3, $4, $5) RETURNING *",
        )
        .bind(new_id("conv"))
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .bind(title)
        .bind(request.metadata)
        .fetch_one(&self.pool)
        .await?)
    }

    pub async fn list_conversations(
        &self,
        actor: &Actor,
        limit: i64,
    ) -> ApiResult<Vec<Conversation>> {
        Ok(sqlx::query_as::<DB, Conversation>(
            "SELECT * FROM conversations WHERE tenant_id = $1 AND owner_ref = $2
             ORDER BY updated_at DESC LIMIT $3",
        )
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .bind(limit.clamp(1, 1000))
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn get_conversation(&self, actor: &Actor, id: &str) -> ApiResult<Conversation> {
        sqlx::query_as::<DB, Conversation>(
            "SELECT * FROM conversations WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3",
        )
        .bind(id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound("Conversation not found.".into()))
    }

    pub async fn update_conversation(
        &self,
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
        // The metadata merge is performed in Rust rather than in SQL. PostgreSQL
        // spells it `metadata || $2`; SQLite's nearest equivalent, `json_patch`,
        // deletes keys whose value is null instead of storing a JSON null. Merging
        // here keeps the observable behaviour identical on both engines.
        let mut tx = self.begin_write().await?;
        let current = sqlx::query_scalar::<DB, Value>(&format!(
            "SELECT metadata FROM conversations
             WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3{}",
            DB::FOR_UPDATE
        ))
        .bind(id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ApiError::NotFound("Conversation not found.".into()))?;
        let metadata = match request.metadata {
            Some(patch) => merge_metadata(current, patch),
            None => current,
        };
        let updated = sqlx::query_as::<DB, Conversation>(
            "UPDATE conversations SET title = COALESCE($1, title),
                metadata = $2, updated_at = $3
             WHERE id = $4 AND tenant_id = $5 AND owner_ref = $6 RETURNING *",
        )
        .bind(title)
        .bind(metadata)
        .bind(Utc::now())
        .bind(id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        updated.ok_or_else(|| ApiError::NotFound("Conversation not found.".into()))
    }

    pub async fn delete_conversation(&self, actor: &Actor, id: &str) -> ApiResult<()> {
        let result = sqlx::query::<DB>(
            "DELETE FROM conversations WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3",
        )
        .bind(id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .execute(&self.pool)
        .await?;
        if DB::rows_affected(&result) == 0 {
            return Err(ApiError::NotFound("Conversation not found.".into()));
        }
        Ok(())
    }

    pub async fn list_items(
        &self,
        actor: &Actor,
        conversation_id: &str,
        after_seq: i64,
        limit: i64,
    ) -> ApiResult<Vec<Item>> {
        self.get_conversation(actor, conversation_id).await?;
        Ok(sqlx::query_as::<DB, Item>(
            "SELECT * FROM conversation_items
             WHERE conversation_id = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3",
        )
        .bind(conversation_id)
        .bind(after_seq.max(0))
        .bind(limit.clamp(1, 1000))
        .fetch_all(&self.pool)
        .await?)
    }

    async fn lock_conversation<'a>(
        tx: &mut Transaction<'a, DB>,
        actor: &Actor,
        id: &str,
    ) -> ApiResult<Conversation> {
        sqlx::query_as::<DB, Conversation>(&format!(
            "SELECT * FROM conversations
             WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3{}",
            DB::FOR_UPDATE
        ))
        .bind(id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or_else(|| ApiError::NotFound("Conversation not found.".into()))
    }

    async fn create_turn_file_snapshot(
        tx: &mut Transaction<'_, DB>,
        conversation_id: &str,
        turn_id: &str,
    ) -> ApiResult<()> {
        let snapshot_id = new_id("fsnap");
        sqlx::query::<DB>(
            "INSERT INTO turn_file_snapshots (id, turn_id, authoritative)
             VALUES ($1, $2, true)",
        )
        .bind(&snapshot_id)
        .bind(turn_id)
        .execute(&mut **tx)
        .await?;
        sqlx::query::<DB>(
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

    /// Pin every file the turn will reference, so none can be deleted while the
    /// turn is being recorded, and confirm the caller owns all of them.
    ///
    /// PostgreSQL expresses the extra ids as `unnest($n::text[])`. SQLite has no
    /// array type, so the id set is rendered as numbered placeholders instead;
    /// see [`in_list`].
    async fn lock_turn_files(
        tx: &mut Transaction<'_, DB>,
        actor: &Actor,
        conversation_id: &str,
        additional_file_ids: &[String],
    ) -> ApiResult<()> {
        let referenced = format!(
            "SELECT item_file.file_id
             FROM conversation_item_files item_file
             JOIN conversation_items item ON item.id = item_file.item_id
             WHERE item.conversation_id = $1
             UNION
             {}",
            in_list(additional_file_ids.len(), 2)
        );
        let owner_first = additional_file_ids.len() + 2;
        let select = format!(
            "SELECT file.id
             FROM files file
             WHERE file.id IN ({referenced})
             AND file.tenant_id = ${} AND file.owner_ref = ${}
             ORDER BY file.id{}",
            owner_first,
            owner_first + 1,
            DB::FOR_KEY_SHARE,
        );
        let mut query = sqlx::query_scalar::<DB, String>(&select).bind(conversation_id);
        for file_id in additional_file_ids {
            query = query.bind(file_id);
        }
        let file_ids = query
            .bind(&actor.tenant_id)
            .bind(&actor.principal_id)
            .fetch_all(&mut **tx)
            .await?;

        let count = format!("SELECT count(DISTINCT file_id) FROM ({referenced}) referenced_files");
        let mut query = sqlx::query_scalar::<DB, i64>(&count).bind(conversation_id);
        for file_id in additional_file_ids {
            query = query.bind(file_id);
        }
        let expected = query.fetch_one(&mut **tx).await?;
        if file_ids.len() != expected as usize {
            return Err(ApiError::NotFound("File not found.".into()));
        }
        Ok(())
    }

    pub async fn start_turn(
        &self,
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

        let mut tx = self.begin_write().await?;
        // Serialize concurrent requests carrying the same idempotency key so the
        // replay check below cannot race a competing insert. PostgreSQL needs an
        // explicit advisory lock; SQLite's write transaction already excludes
        // other writers, so it has no statement to issue here.
        if let Some(statement) = DB::ADVISORY_XACT_LOCK {
            sqlx::query::<DB>(statement)
                .bind(lock_key)
                .execute(&mut *tx)
                .await?;
        }

        if let Some((
            turn_start_id,
            request_version,
            stored_digest,
            conversation_id,
            turn_id,
            first_seq,
            last_seq,
        )) = sqlx::query_as::<DB, (String, i16, Vec<u8>, String, String, i64, i64)>(
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
                    let owned = sqlx::query_scalar::<DB, bool>(
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
            let conversation_exists = sqlx::query_scalar::<DB, String>(&format!(
                "SELECT id FROM conversations
                 WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3{}",
                DB::FOR_UPDATE
            ))
            .bind(&conversation_id)
            .bind(&auth.tenant_id)
            .bind(&auth.principal_id)
            .fetch_optional(&mut *tx)
            .await?
            .is_some();
            let children = sqlx::query_as::<DB, (i32, String, i64, Option<String>)>(
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
            Self::lock_conversation(&mut tx, auth, conversation_id).await?
        } else {
            let conversation = request
                .conversation
                .as_ref()
                .expect("validated conversation");
            sqlx::query_as::<DB, Conversation>(
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

        let active_turn_exists = sqlx::query_scalar::<DB, bool>(
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
        Self::lock_turn_files(&mut tx, auth, &conversation.id, &file_ids).await?;

        let turn_id = new_id("turn");
        let turn_insert = sqlx::query::<DB>(
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
                .is_some_and(|error| DB::violated(error, UniqueIndex::ActiveTurnPerConversation))
            {
                return Err(coded_conflict(
                    "active_turn_exists",
                    "conversation already has an active turn",
                ));
            }
            return Err(error.into());
        }
        Self::create_turn_file_snapshot(&mut tx, &conversation.id, &turn_id).await?;

        let item_count = i64::try_from(request.items.len())
            .map_err(|_| coded_conflict("sequence_space_exhausted", "sequence space exhausted"))?;
        let next_seq = conversation
            .next_seq
            .checked_add(item_count)
            .ok_or_else(|| {
                coded_conflict("sequence_space_exhausted", "sequence space exhausted")
            })?;
        let first_seq = conversation.next_seq;
        let last_seq = next_seq - 1;
        let mut item_ids = Vec::with_capacity(request.items.len());
        for (offset, payload) in request.items.into_iter().enumerate() {
            let file_ids = referenced_file_ids(&payload);
            let item_id = new_id("item");
            sqlx::query::<DB>(
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
                sqlx::query::<DB>(
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
        sqlx::query::<DB>("UPDATE conversations SET next_seq = $2, updated_at = $3 WHERE id = $1")
            .bind(&conversation.id)
            .bind(next_seq)
            .bind(Utc::now())
            .execute(&mut *tx)
            .await?;

        let turn_start_id = new_id("tstart");
        sqlx::query::<DB>(
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
            sqlx::query::<DB>(
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

    pub async fn append_items(
        &self,
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
        let mut tx = self.begin_write().await?;
        let conversation = Self::lock_conversation(&mut tx, actor, conversation_id).await?;
        if let Some((first_seq, last_seq)) = sqlx::query_as::<DB, (i64, i64)>(
            "SELECT first_seq, last_seq FROM append_batches
             WHERE conversation_id = $1 AND idempotency_key = $2",
        )
        .bind(conversation_id)
        .bind(&request.idempotency_key)
        .fetch_optional(&mut *tx)
        .await?
        {
            let items = sqlx::query_as::<DB, Item>(
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
            let exists = sqlx::query_scalar::<DB, bool>(
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
            let exists = sqlx::query_scalar::<DB, String>(&format!(
                "SELECT id FROM files
                 WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3{}",
                DB::FOR_KEY_SHARE
            ))
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
                sqlx::query_as::<DB, Item>(
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
                sqlx::query::<DB>(
                    "INSERT INTO conversation_item_files (item_id, file_id)
                     VALUES ($1, $2)",
                )
                .bind(item_id)
                .bind(file_id)
                .execute(&mut *tx)
                .await?;
            }
        }
        sqlx::query::<DB>(
            "INSERT INTO append_batches (conversation_id, idempotency_key, first_seq, last_seq)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(conversation_id)
        .bind(request.idempotency_key)
        .bind(first_seq)
        .bind(last_seq)
        .execute(&mut *tx)
        .await?;
        sqlx::query::<DB>("UPDATE conversations SET next_seq = $2, updated_at = $3 WHERE id = $1")
            .bind(conversation_id)
            .bind(last_seq + 1)
            .bind(Utc::now())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(AppendResult {
            items: inserted,
            replayed: false,
        })
    }

    /// Project a conversation's items for handoff to an agent.
    ///
    /// Takes the object store and config explicitly rather than the router state
    /// so the store stays independent of the HTTP layer and of any one backend.
    pub async fn replay(
        &self,
        objects: &ObjectStore,
        config: &Config,
        actor: &Actor,
        conversation_id: &str,
        request: ReplayRequest,
    ) -> ApiResult<ReplayResult> {
        let conversation = self.get_conversation(actor, conversation_id).await?;
        let maximum = conversation.next_seq - 1;
        let through_seq = request.through_seq.unwrap_or(maximum).min(maximum);
        if through_seq < request.after_seq.max(0) {
            return Err(ApiError::BadRequest(
                "through_seq must not be less than after_seq".into(),
            ));
        }
        let rows = sqlx::query_scalar::<DB, Value>(
            "SELECT payload FROM conversation_items
             WHERE conversation_id = $1 AND seq > $2 AND seq <= $3 ORDER BY seq ASC",
        )
        .bind(conversation_id)
        .bind(request.after_seq.max(0))
        .bind(through_seq)
        .fetch_all(&self.pool)
        .await?;
        let mut input = project_replay_items(rows, request.strip_top_level_ids);
        if request.file_delivery != FileDelivery::Preserve {
            for item in &mut input {
                self.hydrate_file_references(objects, config, actor, item, request.file_delivery)
                    .await?;
            }
        }
        Ok(ReplayResult {
            conversation_id: conversation_id.into(),
            through_seq,
            input,
        })
    }

    async fn hydrate_file_references(
        &self,
        objects: &ObjectStore,
        config: &Config,
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
            let file = self.get_owned_file(actor, &file_id).await?;
            match delivery {
                FileDelivery::Preserve => {}
                FileDelivery::CapabilityUrl => {
                    object.insert(
                        field.into(),
                        Value::String(
                            capability::file_url(
                                config,
                                actor,
                                &file_id,
                                crate::model::DownloadDelivery::Proxy,
                            )
                            .url,
                        ),
                    );
                }
                FileDelivery::PresignedUrl => {
                    let url = objects
                        .presigned_get(
                            &file.storage_key,
                            config.capability_ttl_seconds,
                            Some(&file.mime_type),
                            None,
                        )
                        .await
                        .map_err(ApiError::ObjectStore)?
                        .ok_or_else(|| {
                            ApiError::BadRequest(
                                "presigned_url delivery requires an S3 blob backend with S3_PUBLIC_URL set."
                                    .into(),
                            )
                        })?;
                    object.insert(field.into(), Value::String(url));
                }
                FileDelivery::Inline => {
                    let (_, bytes) = self.file_bytes(objects, actor, &file_id).await?;
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

    pub async fn create_turn(
        &self,
        actor: &Actor,
        conversation_id: &str,
        request: CreateTurn,
    ) -> ApiResult<Turn> {
        let agent_ref = request.agent_ref.trim();
        let idempotency_key = request.idempotency_key.as_str();
        let mut tx = self.begin_write().await?;
        Self::lock_conversation(&mut tx, actor, conversation_id).await?;
        if let Some(turn) = sqlx::query_as::<DB, Turn>(
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
        let active = sqlx::query_scalar::<DB, bool>(
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
        Self::lock_turn_files(&mut tx, actor, conversation_id, &[]).await?;
        let turn = sqlx::query_as::<DB, Turn>(
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
                if error.as_database_error().is_some_and(|error| {
                    DB::violated(error, UniqueIndex::ActiveTurnPerConversation)
                }) =>
            {
                return Err(coded_conflict(
                    "active_turn_exists",
                    "conversation already has an active turn",
                ));
            }
            Err(error) => return Err(error.into()),
        };
        Self::create_turn_file_snapshot(&mut tx, conversation_id, &turn.id).await?;
        tx.commit().await?;
        Ok(turn)
    }

    pub async fn list_turns(&self, actor: &Actor, conversation_id: &str) -> ApiResult<Vec<Turn>> {
        self.get_conversation(actor, conversation_id).await?;
        Ok(sqlx::query_as::<DB, Turn>(
            "SELECT * FROM turns WHERE conversation_id = $1 ORDER BY created_at ASC",
        )
        .bind(conversation_id)
        .fetch_all(&self.pool)
        .await?)
    }

    pub async fn active_turn(
        &self,
        actor: &Actor,
        conversation_id: &str,
    ) -> ApiResult<Option<Turn>> {
        self.get_conversation(actor, conversation_id).await?;
        Ok(sqlx::query_as::<DB, Turn>(
            "SELECT * FROM turns WHERE conversation_id = $1
             AND status IN ('pending', 'streaming') ORDER BY created_at DESC LIMIT 1",
        )
        .bind(conversation_id)
        .fetch_optional(&self.pool)
        .await?)
    }

    pub async fn get_turn(&self, actor: &Actor, id: &str) -> ApiResult<Turn> {
        sqlx::query_as::<DB, Turn>(
            "SELECT t.* FROM turns t JOIN conversations c ON c.id = t.conversation_id
             WHERE t.id = $1 AND c.tenant_id = $2 AND c.owner_ref = $3",
        )
        .bind(id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound("Turn not found.".into()))
    }

    pub async fn truncate_conversation(
        &self,
        actor: &Actor,
        conversation_id: &str,
        item_id: &str,
    ) -> ApiResult<()> {
        let mut tx = self.begin_write().await?;
        Self::lock_conversation(&mut tx, actor, conversation_id).await?;
        let seq = sqlx::query_scalar::<DB, i64>(
            "SELECT seq FROM conversation_items WHERE id = $1 AND conversation_id = $2",
        )
        .bind(item_id)
        .bind(conversation_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ApiError::NotFound("Item not found in conversation.".into()))?;
        sqlx::query::<DB>(
            "DELETE FROM conversation_items WHERE conversation_id = $1 AND seq >= $2",
        )
        .bind(conversation_id)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
        sqlx::query::<DB>(
            "DELETE FROM append_batches WHERE conversation_id = $1 AND last_seq >= $2",
        )
        .bind(conversation_id)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
        sqlx::query::<DB>(
            "DELETE FROM continuations WHERE conversation_id = $1 AND through_seq >= $2",
        )
        .bind(conversation_id)
        .bind(seq)
        .execute(&mut *tx)
        .await?;
        sqlx::query::<DB>("UPDATE conversations SET next_seq = $2, updated_at = $3 WHERE id = $1")
            .bind(conversation_id)
            .bind(seq)
            .bind(Utc::now())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn regenerate_conversation(
        &self,
        actor: &Actor,
        conversation_id: &str,
    ) -> ApiResult<Option<String>> {
        let mut tx = self.begin_write().await?;
        Self::lock_conversation(&mut tx, actor, conversation_id).await?;
        let turn_id = sqlx::query_scalar::<DB, String>(
            "SELECT id FROM turns WHERE conversation_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(conversation_id)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(turn_id) = &turn_id {
            sqlx::query::<DB>(
                "DELETE FROM conversation_items WHERE turn_id = $1 AND source = 'agent'",
            )
            .bind(turn_id)
            .execute(&mut *tx)
            .await?;
            sqlx::query::<DB>("DELETE FROM continuations WHERE conversation_id = $1")
                .bind(conversation_id)
                .execute(&mut *tx)
                .await?;
            sqlx::query::<DB>("UPDATE conversations SET updated_at = $2 WHERE id = $1")
                .bind(conversation_id)
                .bind(Utc::now())
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(turn_id)
    }

    pub async fn update_turn(
        &self,
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
        sqlx::query_as::<DB, Turn>(
            "UPDATE turns SET status = $1, response_id = $2, error = $3, usage = $4,
                 completed_at = CASE WHEN $5 THEN COALESCE(completed_at, $6) ELSE NULL END
             WHERE id = $7 AND conversation_id IN
                 (SELECT id FROM conversations WHERE tenant_id = $8 AND owner_ref = $9)
             RETURNING *",
        )
        .bind(request.status)
        .bind(request.response_id)
        .bind(request.error)
        .bind(request.usage)
        .bind(terminal)
        .bind(Utc::now())
        .bind(turn_id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound("Turn not found.".into()))
    }

    pub async fn create_continuation(
        &self,
        actor: &Actor,
        conversation_id: &str,
        request: CreateContinuation,
    ) -> ApiResult<Continuation> {
        let conversation = self.get_conversation(actor, conversation_id).await?;
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
        let result = sqlx::query_as::<DB, Continuation>(
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
        .fetch_optional(&self.pool)
        .await?;
        result.ok_or_else(|| ApiError::Conflict("Continuation response_id already exists.".into()))
    }

    pub async fn get_continuation(
        &self,
        actor: &Actor,
        response_id: &str,
        agent_ref: &str,
    ) -> ApiResult<Continuation> {
        sqlx::query_as::<DB, Continuation>(
            "SELECT c.* FROM continuations c
             JOIN conversations v ON v.id = c.conversation_id
             WHERE c.response_id = $1 AND c.agent_ref = $2
               AND c.tenant_id = $3 AND v.owner_ref = $4",
        )
        .bind(response_id)
        .bind(agent_ref)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound("Continuation not found.".into()))
    }
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

/// The active store, chosen at startup by `STORAGE_BACKEND`.
///
/// Both variants wrap the same generic [`SqlStore`]; only the SQL engine
/// differs. Dispatch is a static match rather than a trait object, so no method
/// is virtual and each backend keeps its own monomorphized queries.
#[derive(Clone)]
pub enum Stores {
    Postgres(Arc<SqlStore<sqlx::Postgres>>),
    Sqlite(Arc<SqlStore<sqlx::Sqlite>>),
}

/// Forward each store method to whichever engine is configured.
///
/// Written as a macro so the signature appears once. Adding a store method means
/// adding one line here, and forgetting to is a compile error at the call site.
macro_rules! dispatch {
    ($(
        $(#[$meta:meta])*
        fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty;
    )*) => {
        impl Stores {
            $(
                $(#[$meta])*
                pub async fn $name(&self $(, $arg: $ty)*) -> $ret {
                    match self {
                        Self::Postgres(store) => store.$name($($arg),*).await,
                        Self::Sqlite(store) => store.$name($($arg),*).await,
                    }
                }
            )*
        }
    };
}

dispatch! {
    fn ping() -> ApiResult<()>;
    fn create_conversation(actor: &Actor, request: CreateConversation) -> ApiResult<Conversation>;
    fn list_conversations(actor: &Actor, limit: i64) -> ApiResult<Vec<Conversation>>;
    fn get_conversation(actor: &Actor, id: &str) -> ApiResult<Conversation>;
    fn update_conversation(actor: &Actor, id: &str, request: UpdateConversation) -> ApiResult<Conversation>;
    fn delete_conversation(actor: &Actor, id: &str) -> ApiResult<()>;
    fn list_items(actor: &Actor, conversation_id: &str, after_seq: i64, limit: i64) -> ApiResult<Vec<Item>>;
    fn start_turn(auth: &crate::auth::AuthContext, request: StartTurn) -> ApiResult<StartTurnResult>;
    fn append_items(actor: &Actor, conversation_id: &str, request: AppendItems) -> ApiResult<AppendResult>;
    fn replay(objects: &ObjectStore, config: &Config, actor: &Actor, conversation_id: &str, request: ReplayRequest) -> ApiResult<ReplayResult>;
    fn create_turn(actor: &Actor, conversation_id: &str, request: CreateTurn) -> ApiResult<Turn>;
    fn list_turns(actor: &Actor, conversation_id: &str) -> ApiResult<Vec<Turn>>;
    fn active_turn(actor: &Actor, conversation_id: &str) -> ApiResult<Option<Turn>>;
    fn get_turn(actor: &Actor, id: &str) -> ApiResult<Turn>;
    fn truncate_conversation(actor: &Actor, conversation_id: &str, item_id: &str) -> ApiResult<()>;
    fn regenerate_conversation(actor: &Actor, conversation_id: &str) -> ApiResult<Option<String>>;
    fn update_turn(actor: &Actor, turn_id: &str, request: UpdateTurn) -> ApiResult<Turn>;
    fn create_continuation(actor: &Actor, conversation_id: &str, request: CreateContinuation) -> ApiResult<Continuation>;
    fn get_continuation(actor: &Actor, response_id: &str, agent_ref: &str) -> ApiResult<Continuation>;

    fn save_file(objects: &ObjectStore, actor: &Actor, filename: &str, mime_type: &str, bytes: axum::body::Bytes, max_bytes: usize) -> ApiResult<crate::model::FileRecord>;
    fn get_owned_file(actor: &Actor, id: &str) -> ApiResult<crate::model::FileRecord>;
    fn remove_file(objects: &ObjectStore, actor: &Actor, id: &str) -> ApiResult<()>;
    fn cleanup_deletions(objects: &ObjectStore) -> ApiResult<()>;

    fn initiate_upload(objects: &ObjectStore, config: &Config, auth: &AuthContext, request: crate::uploads::InitiateRequest) -> ApiResult<(bool, crate::uploads::InitiateResponse)>;
    fn complete_upload(objects: &ObjectStore, config: &Config, auth: &AuthContext, id: &str) -> ApiResult<(bool, crate::model::FileResponse)>;
    fn cleanup_expired(objects: &ObjectStore) -> ApiResult<()>;
}

#[cfg(test)]
mod tests {
    use super::merge_metadata;
    use serde_json::json;

    #[test]
    fn merges_metadata_shallowly_preserving_explicit_nulls() {
        // Mirrors PostgreSQL `jsonb || jsonb`: top-level keys are replaced, and a
        // null value is stored rather than deleting the key as json_patch would.
        let merged = merge_metadata(
            json!({"keep": 1, "replace": {"deep": true}, "drop": "x"}),
            json!({"replace": {"other": 1}, "drop": null, "add": 2}),
        );
        assert_eq!(
            merged,
            json!({"keep": 1, "replace": {"other": 1}, "drop": null, "add": 2})
        );
    }

    #[test]
    fn merge_replaces_when_either_side_is_not_an_object() {
        assert_eq!(merge_metadata(json!({"a": 1}), json!(5)), json!(5));
        assert_eq!(merge_metadata(json!(7), json!({"a": 1})), json!({"a": 1}));
    }

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
}
