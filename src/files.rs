use axum::body::Bytes;
use serde_json::Value;
use sqlx::{Decode, Encode, Executor, IntoArguments, Pool, Type};

use crate::{
    blob::ObjectStore,
    db::Backend,
    error::{ApiError, ApiResult},
    ids::new_id,
    model::{Actor, FileRecord},
    store::SqlStore,
};

/// File storage methods on the shared store.
///
/// The bound list mirrors the canonical one in `store.rs`; see the comment there
/// for why it is declared per impl block rather than on [`Backend`].
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
    for<'a> FileRecord: sqlx::FromRow<'a, DB::Row>,
    usize: sqlx::ColumnIndex<DB::Row>,
    for<'a> &'a str: sqlx::ColumnIndex<DB::Row>,
{
    pub async fn save_file(
        &self,
        objects: &ObjectStore,
        actor: &Actor,
        filename: &str,
        mime_type: &str,
        bytes: Bytes,
        max_bytes: usize,
    ) -> ApiResult<FileRecord> {
        if bytes.len() > max_bytes {
            return Err(ApiError::PayloadTooLarge(format!(
                "File exceeds the {} MiB limit.",
                max_bytes / 1024 / 1024
            )));
        }
        let id = new_id("file");
        let filename = clean_filename(filename);
        let mime_type = clean_mime_type(mime_type);
        let storage_key = format!("{}/{}/{}", actor.tenant_id, actor.principal_id, id);
        objects
            .put(&storage_key, bytes.to_vec(), &mime_type)
            .await
            .map_err(ApiError::ObjectStore)?;
        let inserted = sqlx::query_as::<DB, FileRecord>(
            "INSERT INTO files
             (id, tenant_id, owner_ref, filename, mime_type, size, storage_key)
             VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *",
        )
        .bind(&id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .bind(filename)
        .bind(mime_type)
        .bind(i64::try_from(bytes.len()).expect("file size fits i64"))
        .bind(&storage_key)
        .fetch_one(&self.pool)
        .await;
        match inserted {
            Ok(file) => Ok(file),
            Err(error) => {
                if let Err(cleanup_error) = delete_stored_object(objects, &storage_key).await {
                    tracing::error!(?cleanup_error, %storage_key, "failed to clean up untracked object");
                }
                Err(ApiError::Database(error))
            }
        }
    }

    pub async fn get_owned_file(&self, actor: &Actor, id: &str) -> ApiResult<FileRecord> {
        sqlx::query_as::<DB, FileRecord>(
            "SELECT * FROM files WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3",
        )
        .bind(id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ApiError::NotFound("File not found.".into()))
    }

    pub async fn file_bytes(
        &self,
        objects: &ObjectStore,
        actor: &Actor,
        id: &str,
    ) -> ApiResult<(FileRecord, Vec<u8>)> {
        let file = self.get_owned_file(actor, id).await?;
        let bytes = objects
            .get(&file.storage_key)
            .await
            .map_err(ApiError::ObjectStore)?;
        Ok((file, bytes))
    }

    pub async fn remove_file(
        &self,
        objects: &ObjectStore,
        actor: &Actor,
        id: &str,
    ) -> ApiResult<()> {
        let mut tx = self.begin_write().await?;
        let file = sqlx::query_as::<DB, FileRecord>(&format!(
            "SELECT * FROM files
             WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3{}",
            DB::FOR_UPDATE
        ))
        .bind(id)
        .bind(&actor.tenant_id)
        .bind(&actor.principal_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| ApiError::NotFound("File not found.".into()))?;
        let referenced = sqlx::query_scalar::<DB, bool>(
            "SELECT EXISTS(SELECT 1 FROM conversation_item_files WHERE file_id = $1)
             OR EXISTS(SELECT 1 FROM turn_file_snapshot_files WHERE file_id = $1)",
        )
        .bind(id)
        .fetch_one(&mut *tx)
        .await?;
        if referenced {
            return Err(ApiError::Conflict(
                "File is referenced by a conversation and cannot be deleted.".into(),
            ));
        }
        sqlx::query::<DB>("DELETE FROM files WHERE id = $1")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query::<DB>(
            "INSERT INTO file_deletion_outbox (file_id, storage_key) VALUES ($1, $2)",
        )
        .bind(id)
        .bind(&file.storage_key)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        if let Err(error) = self.delete_pending(objects, id, &file.storage_key).await {
            tracing::error!(?error, file_id = %id, "file object cleanup deferred");
        }
        Ok(())
    }

    pub async fn cleanup_deletions(&self, objects: &ObjectStore) -> ApiResult<()> {
        let pending = sqlx::query_as::<DB, (String, String)>(
            "SELECT file_id, storage_key FROM file_deletion_outbox ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;
        for (file_id, storage_key) in pending {
            if let Err(error) = self.delete_pending(objects, &file_id, &storage_key).await {
                tracing::error!(?error, %file_id, %storage_key, "file deletion cleanup failed");
            }
        }
        Ok(())
    }

    async fn delete_pending(
        &self,
        objects: &ObjectStore,
        file_id: &str,
        storage_key: &str,
    ) -> ApiResult<()> {
        delete_stored_object(objects, storage_key).await?;
        sqlx::query::<DB>("DELETE FROM file_deletion_outbox WHERE file_id = $1")
            .bind(file_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// Remove every trace of a stored object.
///
/// On a versioned bucket this enumerates and deletes each version, so a deleted
/// file leaves no recoverable copy. On an unversioned bucket there is no version
/// list to enumerate and a single delete removes the object.
async fn delete_stored_object(objects: &ObjectStore, storage_key: &str) -> ApiResult<()> {
    if !objects.is_versioned() {
        return objects
            .delete(storage_key)
            .await
            .map_err(ApiError::ObjectStore);
    }
    for version_id in objects
        .versions(storage_key)
        .await
        .map_err(ApiError::ObjectStore)?
    {
        objects
            .delete_version(storage_key, &version_id)
            .await
            .map_err(ApiError::ObjectStore)?;
    }
    Ok(())
}

pub fn parse_uri(value: &str) -> Option<&str> {
    value.strip_prefix("threadmark://files/").filter(|id| {
        !id.is_empty()
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    })
}

fn clean_filename(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .filter(|character| !character.is_control() && *character != '/' && *character != '\\')
        .take(300)
        .collect();
    if cleaned.trim().is_empty() {
        "file".into()
    } else {
        cleaned
    }
}

fn clean_mime_type(value: &str) -> String {
    let value = value.trim();
    if value.is_empty() || value.len() > 200 || value.contains(['\r', '\n']) {
        "application/octet-stream".into()
    } else {
        value.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_only_canonical_file_uris() {
        assert_eq!(parse_uri("threadmark://files/file_123"), Some("file_123"));
        assert_eq!(parse_uri("threadmark-file:file_123"), None);
        assert_eq!(parse_uri("threadmark://files/../secret"), None);
        assert_eq!(parse_uri("threadmark://artifacts/file_123"), None);
    }
}
