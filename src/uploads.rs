use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::json;
use sha2::{Digest, Sha256};
use sqlx::{Decode, Encode, Executor, FromRow, IntoArguments, Pool, Type};

use crate::{
    auth::AuthContext,
    config::Config,
    db::Backend,
    error::{ApiError, ApiResult},
    ids::new_id,
    model::{FileRecord, FileResponse},
    object_store::{ObjectStore, PresignedPost},
    store::SqlStore,
};

#[derive(FromRow)]
pub(crate) struct Upload {
    id: String,
    tenant_id: String,
    owner_ref: String,
    request_hash: String,
    file_id: String,
    filename: String,
    mime_type: String,
    expected_size: i64,
    staging_key: String,
    status: String,
    expires_at: DateTime<Utc>,
}

#[derive(Serialize)]
pub struct InitiateResponse {
    pub id: String,
    pub status: String,
    pub expires_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload: Option<PresignedPost>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<FileResponse>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitiateRequest {
    pub idempotency_key: String,
    pub filename: String,
    pub mime_type: String,
    pub size: i64,
}

/// Number of seconds a finalization lease is held before another caller may
/// reclaim it.
const LEASE_SECONDS: i64 = 120;

/// Direct-upload session methods on the shared store.
///
/// The bound list mirrors the canonical one in `store.rs`.
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
    for<'a> Upload: sqlx::FromRow<'a, DB::Row>,
    for<'a> (String, Option<String>): sqlx::FromRow<'a, DB::Row>,
    for<'a> (String, String, Option<String>, bool): sqlx::FromRow<'a, DB::Row>,
    for<'a> Option<String>: Decode<'a, DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
    for<'a> &'a str: sqlx::ColumnIndex<DB::Row>,
{
    pub async fn initiate_upload(
        &self,
        objects: &ObjectStore,
        config: &Config,
        auth: &AuthContext,
        request: InitiateRequest,
    ) -> ApiResult<(bool, InitiateResponse)> {
        if !config.direct_upload_enabled || !objects.supports_public_urls() {
            return Err(ApiError::CodedUnavailable {
                code: "direct_upload_unavailable",
                message: "Direct browser uploads are unavailable.".into(),
            });
        }
        if !objects
            .versioning_enabled()
            .await
            .map_err(ApiError::ObjectStore)?
        {
            return Err(ApiError::CodedUnavailable {
                code: "direct_upload_unavailable",
                message: "Direct browser uploads require bucket versioning.".into(),
            });
        }
        let idempotency_key = bounded_trimmed("idempotency_key", request.idempotency_key, 200)?;
        let filename = normalized_filename(request.filename)?;
        let mime_type = normalized_mime_type(request.mime_type)?;
        if !(0..=i64::try_from(config.file_max_bytes).expect("usize fits i64"))
            .contains(&request.size)
        {
            return Err(ApiError::BadRequest(
                "size is outside the configured file limit.".into(),
            ));
        }
        let request_hash = hash_request(&filename, &mime_type, request.size);
        let existing = sqlx::query_as::<DB, Upload>(
            "SELECT id, tenant_id, owner_ref, request_hash, file_id, filename, mime_type, expected_size, staging_key, status, expires_at
             FROM file_uploads WHERE tenant_id = $1 AND owner_ref = $2 AND client_id = $3 AND idempotency_key = $4",
        )
        .bind(&auth.tenant_id).bind(&auth.principal_id).bind(&auth.client_id).bind(&idempotency_key)
        .fetch_optional(&self.pool).await?;
        let (created, upload) = match existing {
            Some(upload) => {
                if upload.request_hash != request_hash {
                    return Err(ApiError::CodedConflict {
                        code: "idempotency_mismatch",
                        message: "Idempotency key was used with different input.".into(),
                    });
                }
                (false, upload)
            }
            None => {
                let id = new_id("upload");
                let staging_key = format!(
                    "uploads/{}/{}/{}/{}",
                    component(&auth.tenant_id),
                    component(&auth.principal_id),
                    id,
                    new_id("object")
                );
                let expires_at = Utc::now()
                    .checked_add_signed(Duration::seconds(
                        i64::try_from(config.file_upload_session_ttl_seconds)
                            .expect("configuration bounds this to i64"),
                    ))
                    .ok_or_else(|| ApiError::CodedUnavailable {
                        code: "direct_upload_unavailable",
                        message: "Configured upload session lifetime is not representable.".into(),
                    })?;
                let inserted = sqlx::query_as::<DB, Upload>(
                    "INSERT INTO file_uploads (id, tenant_id, owner_ref, client_id, idempotency_key, request_hash, file_id, filename, mime_type, expected_size, staging_key, status, expires_at)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending',$12)
                     ON CONFLICT (tenant_id, owner_ref, client_id, idempotency_key) DO NOTHING
                     RETURNING id, tenant_id, owner_ref, request_hash, file_id, filename, mime_type, expected_size, staging_key, status, expires_at",
                )
                .bind(&id).bind(&auth.tenant_id).bind(&auth.principal_id).bind(&auth.client_id).bind(&idempotency_key).bind(&request_hash)
                .bind(new_id("file")).bind(&filename).bind(&mime_type).bind(request.size).bind(&staging_key).bind(expires_at)
                .fetch_optional(&self.pool).await?;
                match inserted {
                    Some(upload) => (true, upload),
                    None => {
                        return Box::pin(self.initiate_upload(
                            objects,
                            config,
                            auth,
                            InitiateRequest {
                                idempotency_key,
                                filename,
                                mime_type,
                                size: request.size,
                            },
                        ))
                        .await;
                    }
                }
            }
        };
        self.response_for_upload(objects, config, upload, created)
            .await
    }

    async fn response_for_upload(
        &self,
        objects: &ObjectStore,
        config: &Config,
        upload: Upload,
        created: bool,
    ) -> ApiResult<(bool, InitiateResponse)> {
        if upload.status == "completed" {
            let file = self.file_for_upload(&upload).await?;
            return Ok((
                false,
                InitiateResponse {
                    id: upload.id,
                    status: "completed".into(),
                    expires_at: upload.expires_at,
                    upload: None,
                    file: Some(file.into()),
                    retry_after_seconds: None,
                },
            ));
        }
        if upload.status == "cleanup_pending" || upload.expires_at <= Utc::now() {
            return Err(ApiError::CodedGone {
                code: "upload_expired",
                message: "Upload session has expired.".into(),
            });
        }
        if upload.status == "finalizing" {
            return Ok((
                false,
                InitiateResponse {
                    id: upload.id,
                    status: "finalizing".into(),
                    expires_at: upload.expires_at,
                    upload: None,
                    file: None,
                    retry_after_seconds: Some(5),
                },
            ));
        }
        let form_expiry = upload.expires_at.min(
            Utc::now()
                + Duration::seconds(
                    i64::try_from(config.file_upload_url_ttl_seconds).expect("duration fits i64"),
                ),
        );
        let form = objects
            .presigned_post(
                &upload.staging_key,
                &upload.mime_type,
                &upload.id,
                upload.expected_size,
                form_expiry,
            )
            .map_err(ApiError::ObjectStore)?;
        Ok((
            created,
            InitiateResponse {
                id: upload.id,
                status: "pending".into(),
                expires_at: upload.expires_at,
                upload: Some(form),
                file: None,
                retry_after_seconds: None,
            },
        ))
    }

    pub async fn complete_upload(
        &self,
        objects: &ObjectStore,
        config: &Config,
        auth: &AuthContext,
        id: &str,
    ) -> ApiResult<(bool, FileResponse)> {
        let upload = self.owned_upload(auth, id).await?;
        if upload.status == "completed" {
            return Ok((false, self.file_for_upload(&upload).await?.into()));
        }
        if upload.status == "cleanup_pending" || upload.expires_at <= Utc::now() {
            return Err(ApiError::CodedGone {
                code: "upload_expired",
                message: "Upload session has expired.".into(),
            });
        }
        let lease_token = new_id("lease");
        let candidate_key = format!(
            "upload-candidates/{}/{}/{}/{}",
            component(&upload.tenant_id),
            component(&upload.owner_ref),
            new_id("attempt"),
            new_id("object")
        );
        if !self
            .claim_completion(id, &lease_token, &candidate_key)
            .await?
        {
            return Err(ApiError::CodedConflict {
                code: "upload_finalizing",
                message: "Upload is being finalized.".into(),
            });
        }
        let Some(head) = objects
            .head(&upload.staging_key)
            .await
            .map_err(ApiError::ObjectStore)?
        else {
            self.restore_pending(id, &lease_token).await?;
            return Err(ApiError::CodedConflict {
                code: "upload_incomplete",
                message: "Upload has not reached object storage.".into(),
            });
        };
        let Some(source_version_id) = head
            .version_id
            .as_deref()
            .filter(|id| !id.is_empty() && *id != "null")
        else {
            return Err(ApiError::ObjectStore(anyhow::anyhow!(
                "S3 versioning is required for direct uploads"
            )));
        };
        if head.size != upload.expected_size {
            self.restore_pending(id, &lease_token).await?;
            return Err(ApiError::CodedConflict {
                code: "upload_size_mismatch",
                message: "Uploaded object size does not match the session.".into(),
            });
        }
        if head.size > i64::try_from(config.file_max_bytes).expect("usize fits i64") {
            self.restore_pending(id, &lease_token).await?;
            return Err(ApiError::CodedConflict {
                code: "upload_size_limit",
                message: "Uploaded object exceeds the current file limit.".into(),
            });
        }
        if head.content_type.as_deref() != Some(&upload.mime_type)
            || head.metadata.get("threadmark-upload").map(String::as_str) != Some(&upload.id)
        {
            self.restore_pending(id, &lease_token).await?;
            return Err(ApiError::CodedConflict {
                code: "upload_metadata_mismatch",
                message: "Uploaded object metadata does not match the session.".into(),
            });
        }
        objects
            .copy(
                &upload.staging_key,
                source_version_id,
                &candidate_key,
                &upload.mime_type,
            )
            .await
            .map_err(ApiError::ObjectStore)?;
        let candidate = objects
            .head(&candidate_key)
            .await
            .map_err(ApiError::ObjectStore)?
            .ok_or_else(|| ApiError::ObjectStore(anyhow::anyhow!("copied object was not found")))?;
        if candidate
            .version_id
            .as_deref()
            .is_none_or(|id| id.is_empty() || id == "null")
            || candidate.size != upload.expected_size
            || candidate.content_type.as_deref() != Some(&upload.mime_type)
        {
            self.enqueue_all_versions_pool(&candidate_key).await?;
            self.restore_pending(id, &lease_token).await?;
            return Err(ApiError::CodedConflict {
                code: "upload_copy_mismatch",
                message: "Finalized object validation failed.".into(),
            });
        }
        let mut tx = self.begin_write().await?;
        let file = sqlx::query_as::<DB, FileRecord>("INSERT INTO files (id, tenant_id, owner_ref, filename, mime_type, size, storage_key) VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *")
            .bind(&upload.file_id).bind(&upload.tenant_id).bind(&upload.owner_ref).bind(&upload.filename).bind(&upload.mime_type).bind(upload.expected_size).bind(&candidate_key).fetch_one(&mut *tx).await?;
        let completed = sqlx::query::<DB>("UPDATE file_uploads SET status = 'completed', completed_at = $3, lease_token = NULL, lease_expires_at = NULL WHERE id = $1 AND status = 'finalizing' AND lease_token = $2 AND expires_at > $3 AND lease_expires_at > $3")
            .bind(id)
            .bind(&lease_token)
            .bind(Utc::now())
            .execute(&mut *tx)
            .await?;
        if DB::rows_affected(&completed) != 1 {
            return Err(ApiError::CodedConflict {
                code: "upload_finalizing",
                message: "Upload finalization lease was lost.".into(),
            });
        }
        // A valid form remains replayable briefly after completion. Do not declare
        // the staging key clean until every form can no longer be accepted.
        Self::enqueue_all_versions_after(&mut tx, &upload.staging_key, 305).await?;
        tx.commit().await?;
        Ok((true, file.into()))
    }

    async fn restore_pending(&self, id: &str, lease_token: &str) -> ApiResult<()> {
        sqlx::query::<DB>(
            "UPDATE file_uploads SET status = 'pending', lease_token = NULL, lease_expires_at = NULL WHERE id = $1 AND status = 'finalizing' AND lease_token = $2",
        )
        .bind(id)
        .bind(lease_token)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn claim_completion(
        &self,
        id: &str,
        lease_token: &str,
        candidate_key: &str,
    ) -> ApiResult<bool> {
        let mut tx = self.begin_write().await?;
        let previous_candidate = sqlx::query_scalar::<DB, Option<String>>(&format!(
            "SELECT candidate_key FROM file_uploads WHERE id = $1{}",
            DB::FOR_UPDATE
        ))
        .bind(id)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        let now = Utc::now();
        let claimed = sqlx::query::<DB>(
            "UPDATE file_uploads
             SET status = 'finalizing', lease_token = $2, lease_expires_at = $4, candidate_key = $3
             WHERE id = $1 AND expires_at > $5
               AND (status = 'pending' OR (status = 'finalizing' AND lease_expires_at <= $5))",
        )
        .bind(id)
        .bind(lease_token)
        .bind(candidate_key)
        .bind(now + Duration::seconds(LEASE_SECONDS))
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if DB::rows_affected(&claimed) == 1 {
            if let Some(previous_candidate) = previous_candidate {
                Self::enqueue_all_versions(&mut tx, &previous_candidate).await?;
            }
            tx.commit().await?;
            Ok(true)
        } else {
            tx.rollback().await?;
            Ok(false)
        }
    }
    async fn owned_upload(&self, auth: &AuthContext, id: &str) -> ApiResult<Upload> {
        sqlx::query_as::<DB, Upload>("SELECT id, tenant_id, owner_ref, request_hash, file_id, filename, mime_type, expected_size, staging_key, status, expires_at FROM file_uploads WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3").bind(id).bind(&auth.tenant_id).bind(&auth.principal_id).fetch_optional(&self.pool).await?.ok_or_else(|| ApiError::NotFound("Upload not found.".into()))
    }
    async fn file_for_upload(&self, upload: &Upload) -> ApiResult<FileRecord> {
        sqlx::query_as::<DB, FileRecord>(
            "SELECT * FROM files WHERE id = $1 AND tenant_id = $2 AND owner_ref = $3",
        )
        .bind(&upload.file_id)
        .bind(&upload.tenant_id)
        .bind(&upload.owner_ref)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| ApiError::CodedGone {
            code: "upload_file_deleted",
            message: "The completed upload's file was deleted.".into(),
        })
    }

    pub async fn cleanup_expired(&self, objects: &ObjectStore) -> ApiResult<()> {
        let mut tx = self.begin_write().await?;
        let keys = sqlx::query_as::<DB, (String, Option<String>)>("UPDATE file_uploads SET status = 'cleanup_pending', lease_token = NULL, lease_expires_at = NULL WHERE expires_at <= $1 AND (status = 'pending' OR (status = 'finalizing' AND lease_expires_at <= $1)) RETURNING staging_key, candidate_key").bind(Utc::now()).fetch_all(&mut *tx).await?;
        for (key, candidate_key) in keys {
            Self::enqueue_all_versions(&mut tx, &key).await?;
            if let Some(candidate_key) = candidate_key {
                Self::enqueue_all_versions(&mut tx, &candidate_key).await?;
            }
        }
        tx.commit().await?;
        let keys = sqlx::query_as::<DB, (String, String, Option<String>, bool)>(
            "SELECT id, storage_key, version_id, all_versions FROM object_deletion_outbox WHERE not_before <= $1",
        )
        .bind(Utc::now())
        .fetch_all(&self.pool)
        .await?;
        for (id, key, version_id, all_versions) in keys {
            let deleted = if all_versions {
                let versions = objects
                    .versions(&key)
                    .await
                    .map_err(ApiError::ObjectStore)?;
                for version_id in versions {
                    objects
                        .delete_version(&key, &version_id)
                        .await
                        .map_err(ApiError::ObjectStore)?;
                }
                objects
                    .versions(&key)
                    .await
                    .map_err(ApiError::ObjectStore)?
                    .is_empty()
            } else {
                objects
                    .delete_version(
                        &key,
                        version_id
                            .as_deref()
                            .expect("version deletion has version ID"),
                    )
                    .await
                    .is_ok()
            };
            if deleted {
                sqlx::query::<DB>("DELETE FROM object_deletion_outbox WHERE id = $1")
                    .bind(id)
                    .execute(&self.pool)
                    .await?;
            }
        }
        Ok(())
    }

    async fn enqueue_all_versions(
        tx: &mut sqlx::Transaction<'_, DB>,
        storage_key: &str,
    ) -> ApiResult<()> {
        sqlx::query::<DB>("INSERT INTO object_deletion_outbox (id, storage_key, all_versions) VALUES ($1, $2, true) ON CONFLICT DO NOTHING")
            .bind(new_id("object_delete")).bind(storage_key).execute(&mut **tx).await?;
        Ok(())
    }

    async fn enqueue_all_versions_after(
        tx: &mut sqlx::Transaction<'_, DB>,
        storage_key: &str,
        delay_seconds: i64,
    ) -> ApiResult<()> {
        sqlx::query::<DB>("INSERT INTO object_deletion_outbox (id, storage_key, all_versions, not_before) VALUES ($1, $2, true, $3) ON CONFLICT DO NOTHING")
            .bind(new_id("object_delete"))
            .bind(storage_key)
            .bind(Utc::now() + Duration::seconds(delay_seconds))
            .execute(&mut **tx)
            .await?;
        Ok(())
    }

    async fn enqueue_all_versions_pool(&self, storage_key: &str) -> ApiResult<()> {
        sqlx::query::<DB>("INSERT INTO object_deletion_outbox (id, storage_key, all_versions) VALUES ($1, $2, true) ON CONFLICT DO NOTHING")
            .bind(new_id("object_delete")).bind(storage_key).execute(&self.pool).await?;
        Ok(())
    }
}

fn bounded_trimmed(name: &str, value: String, maximum: usize) -> ApiResult<String> {
    let value = value.trim().to_owned();
    if value.is_empty() || value.contains('\0') || value.chars().count() > maximum {
        Err(ApiError::BadRequest(format!(
            "{name} must contain 1 to {maximum} characters."
        )))
    } else {
        Ok(value)
    }
}
fn normalized_filename(value: String) -> ApiResult<String> {
    let value: String = value
        .chars()
        .filter(|c| !c.is_control() && *c != '/' && *c != '\\')
        .take(300)
        .collect();
    bounded_trimmed("filename", value, 300)
}
fn normalized_mime_type(value: String) -> ApiResult<String> {
    let value = bounded_trimmed("mime_type", value, 200)?;
    let mut parts = value.split('/');
    let valid_parts = matches!((parts.next(), parts.next(), parts.next()), (Some(kind), Some(subtype), None) if !kind.is_empty() && !subtype.is_empty());
    if !value.is_ascii()
        || value.contains(';')
        || value.bytes().any(|c| c.is_ascii_control())
        || !valid_parts
    {
        return Err(ApiError::BadRequest(
            "mime_type must be a media type without parameters.".into(),
        ));
    }
    Ok(value.to_ascii_lowercase())
}
fn hash_request(filename: &str, mime_type: &str, size: i64) -> String {
    let value = json!({"filename": filename, "mime_type": mime_type, "operation": "file_upload", "size": size, "version": 1});
    let canonical =
        serde_json_canonicalizer::to_string(&value).expect("canonical request is valid");
    format!("{:x}", Sha256::digest(canonical.as_bytes()))
}
fn component(value: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_and_rejects_invalid_mime_types() {
        assert_eq!(
            normalized_mime_type(" Image/PNG ".into()).unwrap(),
            "image/png"
        );
        for value in [
            "",
            "text/plain; charset=utf-8",
            "text/",
            "/plain",
            "text/plain/extra",
            "text/pla\nin",
        ] {
            assert!(normalized_mime_type(value.into()).is_err(), "{value:?}");
        }
    }

    #[test]
    fn normalizes_filename_without_path_components() {
        assert_eq!(
            normalized_filename(" report/one\\two.txt ".into()).unwrap(),
            "reportonetwo.txt"
        );
        assert!(normalized_filename(" /\\ ".into()).is_err());
    }

    #[test]
    fn request_hash_is_stable_and_binds_every_input() {
        let expected = hash_request("diagram.png", "image/png", 42);
        assert_eq!(expected, hash_request("diagram.png", "image/png", 42));
        assert_ne!(expected, hash_request("diagram.png", "image/jpeg", 42));
        assert_ne!(expected, hash_request("other.png", "image/png", 42));
        assert_ne!(expected, hash_request("diagram.png", "image/png", 43));
    }

    #[test]
    fn owner_components_are_collision_free() {
        assert_ne!(component("owner/a"), component("owner:a"));
    }
}
