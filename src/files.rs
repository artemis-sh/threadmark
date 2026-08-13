use axum::body::Bytes;
use sqlx::PgPool;

use crate::{
    error::{ApiError, ApiResult},
    ids::new_id,
    model::{Actor, FileRecord},
    object_store::ObjectStore,
};

pub async fn save(
    pool: &PgPool,
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
    let inserted = sqlx::query_as::<_, FileRecord>(
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
    .fetch_one(pool)
    .await;
    match inserted {
        Ok(file) => Ok(file),
        Err(error) => {
            if let Err(cleanup_error) = objects.delete(&storage_key).await {
                tracing::error!(?cleanup_error, %storage_key, "failed to clean up untracked object");
            }
            Err(ApiError::Database(error))
        }
    }
}

pub async fn get_owned(pool: &PgPool, actor: &Actor, id: &str) -> ApiResult<FileRecord> {
    sqlx::query_as::<_, FileRecord>(
        "SELECT * FROM files WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3",
    )
    .bind(id)
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::NotFound("File not found.".into()))
}

pub async fn bytes(
    pool: &PgPool,
    objects: &ObjectStore,
    actor: &Actor,
    id: &str,
) -> ApiResult<(FileRecord, Vec<u8>)> {
    let file = get_owned(pool, actor, id).await?;
    let bytes = objects
        .get(&file.storage_key)
        .await
        .map_err(ApiError::ObjectStore)?;
    Ok((file, bytes))
}

pub async fn remove(
    pool: &PgPool,
    objects: &ObjectStore,
    actor: &Actor,
    id: &str,
) -> ApiResult<()> {
    let file = get_owned(pool, actor, id).await?;
    let uri = file.uri();
    let referenced = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
            SELECT 1 FROM conversation_items i
            JOIN conversations c ON c.id = i.conversation_id
            WHERE c.tenant_id = $1 AND c.owner_ref = $2
              AND i.payload::text LIKE '%' || $3 || '%'
        )",
    )
    .bind(&actor.tenant_id)
    .bind(&actor.principal_id)
    .bind(uri)
    .fetch_one(pool)
    .await?;
    if referenced {
        return Err(ApiError::Conflict(
            "File is referenced by a conversation and cannot be deleted.".into(),
        ));
    }
    objects
        .delete(&file.storage_key)
        .await
        .map_err(ApiError::ObjectStore)?;
    sqlx::query("DELETE FROM files WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
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
