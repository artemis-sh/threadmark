use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use tower_http::trace::TraceLayer;

use crate::{
    auth::{AuthContext, Authenticator, Permission},
    capability,
    config::Config,
    error::{ApiError, ApiResult},
    model::{
        Actor, AppendItems, AppendResult, Continuation, ContinuationQuery, Conversation,
        CreateContinuation, CreateConversation, CreateDownload, CreateTurn, DownloadDelivery,
        DownloadGrant, FileResponse, Item, ListConversationsQuery, ListItemsQuery,
        RegenerateResult, ReplayRequest, ReplayResult, StartTurn, StartTurnResult, StrictJson,
        TruncateConversation, Turn, UpdateConversation, UpdateTurn, validate_json_number_tokens,
    },
    object_store::ObjectStore,
    uploads,
};

#[derive(Clone)]
pub struct AppState {
    pub store: crate::store::Stores,
    pub object_store: ObjectStore,
    pub config: Config,
    pub auth: Authenticator,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route(
            "/v1/conversations",
            get(list_conversations).post(create_conversation),
        )
        .route("/v1/turn-starts", post(start_turn))
        .route(
            "/v1/conversations/{id}",
            get(get_conversation)
                .patch(update_conversation)
                .delete(delete_conversation),
        )
        .route(
            "/v1/conversations/{id}/items",
            get(list_items).post(append_items),
        )
        .route("/v1/conversations/{id}/replay", post(replay))
        .route(
            "/v1/conversations/{id}/turns",
            get(list_turns).post(create_turn),
        )
        .route("/v1/conversations/{id}/active-turn", get(get_active_turn))
        .route("/v1/turns/{id}", get(get_turn).patch(update_turn))
        .route(
            "/v1/conversations/{id}/truncate",
            post(truncate_conversation),
        )
        .route(
            "/v1/conversations/{id}/regenerate",
            post(regenerate_conversation),
        )
        .route(
            "/v1/conversations/{id}/continuations",
            post(create_continuation),
        )
        .route("/v1/continuations/{response_id}", get(get_continuation))
        .route("/v1/files", post(upload_file))
        .route("/v1/file-uploads", post(initiate_file_upload))
        .route("/v1/file-uploads/{id}/complete", post(complete_file_upload))
        .route("/v1/files/{id}", get(get_file).delete(delete_file))
        .route("/v1/files/{id}/content", get(get_file_content))
        .route("/v1/files/{id}/downloads", post(create_file_download))
        .route("/v1/downloads/files/{id}", get(download_file))
        // Open Responses permits ~32 MiB base64 file parts. Leave room for
        // the surrounding item and batch JSON while still bounding memory.
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health(State(state): State<AppState>) -> ApiResult<StatusCode> {
    state.store.ping().await?;
    state
        .object_store
        .ping()
        .await
        .map_err(ApiError::ObjectStore)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_conversation(
    State(state): State<AppState>,
    auth: AuthContext,
    Json(request): Json<CreateConversation>,
) -> ApiResult<(StatusCode, Json<Conversation>)> {
    auth.require(Permission::ConversationCreate)?;
    Ok((
        StatusCode::CREATED,
        Json(state.store.create_conversation(&auth, request).await?),
    ))
}

async fn start_turn(
    State(state): State<AppState>,
    auth: AuthContext,
    body: Bytes,
) -> ApiResult<(StatusCode, Json<StartTurnResult>)> {
    let request = parse_start_turn(&body)?;
    auth.require(Permission::TurnCreate)?;
    auth.require(Permission::TranscriptAppend)?;
    auth.require_agent(request.agent_ref.trim())?;
    if request.conversation.is_some() {
        auth.require(Permission::ConversationCreate)?;
    }
    let result = state.store.start_turn(&auth, request).await?;
    let status = if result.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(result)))
}

fn parse_start_turn(body: &[u8]) -> ApiResult<StartTurn> {
    validate_json_number_tokens(body)
        .map_err(|error| ApiError::BadRequest(format!("invalid request JSON: {error}")))?;
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let StrictJson(value) = StrictJson::deserialize(&mut deserializer)
        .map_err(|error| ApiError::BadRequest(format!("invalid request JSON: {error}")))?;
    deserializer
        .end()
        .map_err(|error| ApiError::BadRequest(format!("invalid request JSON: {error}")))?;
    serde_json::from_value(value)
        .map_err(|error| ApiError::BadRequest(format!("invalid request JSON: {error}")))
}

async fn list_conversations(
    State(state): State<AppState>,
    auth: AuthContext,
    Query(query): Query<ListConversationsQuery>,
) -> ApiResult<Json<Vec<Conversation>>> {
    auth.require(Permission::ConversationList)?;
    Ok(Json(
        state
            .store
            .list_conversations(&auth, query.limit.unwrap_or(200))
            .await?,
    ))
}

async fn get_conversation(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<Json<Conversation>> {
    auth.require(Permission::ConversationRead)?;
    Ok(Json(state.store.get_conversation(&auth, &id).await?))
}

async fn update_conversation(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Json(request): Json<UpdateConversation>,
) -> ApiResult<Json<Conversation>> {
    auth.require(Permission::ConversationUpdate)?;
    Ok(Json(
        state.store.update_conversation(&auth, &id, request).await?,
    ))
}

async fn delete_conversation(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    auth.require(Permission::ConversationDelete)?;
    state.store.delete_conversation(&auth, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_items(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Query(query): Query<ListItemsQuery>,
) -> ApiResult<Json<Vec<Item>>> {
    auth.require(Permission::TranscriptRead)?;
    Ok(Json(
        state
            .store
            .list_items(&auth, &id, query.after_seq, query.limit.unwrap_or(100))
            .await?,
    ))
}

async fn append_items(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Json(request): Json<AppendItems>,
) -> ApiResult<Json<AppendResult>> {
    auth.require(Permission::TranscriptAppend)?;
    Ok(Json(state.store.append_items(&auth, &id, request).await?))
}

async fn replay(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Json(request): Json<ReplayRequest>,
) -> ApiResult<Json<ReplayResult>> {
    auth.require(Permission::TranscriptRead)?;
    if request.file_delivery != crate::model::FileDelivery::Preserve {
        auth.require(Permission::FileRead)?;
    }
    Ok(Json(
        state
            .store
            .replay(&state.object_store, &state.config, &auth, &id, request)
            .await?,
    ))
}

async fn create_turn(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Json(request): Json<CreateTurn>,
) -> ApiResult<(StatusCode, Json<Turn>)> {
    auth.require(Permission::TurnCreate)?;
    auth.require_agent(request.agent_ref.trim())?;
    Ok((
        StatusCode::CREATED,
        Json(state.store.create_turn(&auth, &id, request).await?),
    ))
}

async fn list_turns(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<Json<Vec<Turn>>> {
    auth.require(Permission::TurnRead)?;
    Ok(Json(state.store.list_turns(&auth, &id).await?))
}

async fn get_active_turn(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<Json<Option<Turn>>> {
    auth.require(Permission::ConversationRead)?;
    auth.require(Permission::TurnRead)?;
    Ok(Json(state.store.active_turn(&auth, &id).await?))
}

async fn get_turn(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<Json<Turn>> {
    auth.require(Permission::TurnRead)?;
    Ok(Json(state.store.get_turn(&auth, &id).await?))
}

async fn update_turn(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Json(request): Json<UpdateTurn>,
) -> ApiResult<Json<Turn>> {
    auth.require(Permission::TurnUpdate)?;
    Ok(Json(state.store.update_turn(&auth, &id, request).await?))
}

async fn create_continuation(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Json(request): Json<CreateContinuation>,
) -> ApiResult<(StatusCode, Json<Continuation>)> {
    auth.require(Permission::ContinuationWrite)?;
    Ok((
        StatusCode::CREATED,
        Json(state.store.create_continuation(&auth, &id, request).await?),
    ))
}

async fn get_continuation(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(response_id): Path<String>,
    Query(query): Query<ContinuationQuery>,
) -> ApiResult<Json<Continuation>> {
    auth.require(Permission::ContinuationRead)?;
    Ok(Json(
        state
            .store
            .get_continuation(&auth, &response_id, &query.agent_ref)
            .await?,
    ))
}

async fn truncate_conversation(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Json(request): Json<TruncateConversation>,
) -> ApiResult<StatusCode> {
    auth.require(Permission::ConversationTruncate)?;
    state
        .store
        .truncate_conversation(&auth, &id, &request.item_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn regenerate_conversation(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<Json<RegenerateResult>> {
    auth.require(Permission::ConversationRegenerate)?;
    Ok(Json(RegenerateResult {
        turn_id: state.store.regenerate_conversation(&auth, &id).await?,
    }))
}

async fn upload_file(
    State(state): State<AppState>,
    auth: AuthContext,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<FileResponse>)> {
    auth.require(Permission::FileCreate)?;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|error| ApiError::BadRequest(format!("Invalid multipart body: {error}")))?
    {
        if field.name() != Some("file") {
            continue;
        }
        let filename = field.file_name().unwrap_or("file").to_owned();
        let mime_type = field
            .content_type()
            .unwrap_or("application/octet-stream")
            .to_owned();
        let bytes = field.bytes().await.map_err(|error| {
            ApiError::BadRequest(format!("Could not read multipart file: {error}"))
        })?;
        let file = state
            .store
            .save_file(
                &state.object_store,
                &auth,
                &filename,
                &mime_type,
                bytes,
                state.config.file_max_bytes,
            )
            .await?;
        return Ok((StatusCode::CREATED, Json(file.into())));
    }
    Err(ApiError::BadRequest("Missing `file` field.".into()))
}

async fn initiate_file_upload(
    State(state): State<AppState>,
    auth: AuthContext,
    Json(request): Json<uploads::InitiateRequest>,
) -> ApiResult<Response> {
    auth.require(Permission::FileCreate)?;
    if auth.agent_ref().is_some() {
        return Err(ApiError::Forbidden);
    }
    let (created, upload) = state
        .store
        .initiate_upload(&state.object_store, &state.config, &auth, request)
        .await?;
    let finalizing = upload.status == "finalizing";
    let mut response = (
        if created {
            StatusCode::CREATED
        } else {
            StatusCode::OK
        },
        Json(upload),
    )
        .into_response();
    if finalizing {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
    }
    Ok(response)
}

async fn complete_file_upload(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    auth.require(Permission::FileCreate)?;
    if auth.agent_ref().is_some() {
        return Err(ApiError::Forbidden);
    }
    match state
        .store
        .complete_upload(&state.object_store, &state.config, &auth, &id)
        .await
    {
        Ok((created, file)) => Ok((
            if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            },
            Json(file),
        )
            .into_response()),
        Err(ApiError::CodedConflict {
            code: "upload_finalizing",
            message,
        }) => {
            let mut response = ApiError::CodedConflict {
                code: "upload_finalizing",
                message,
            }
            .into_response();
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from_static("5"));
            Ok(response)
        }
        Err(error) => Err(error),
    }
}

async fn get_file(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<Json<FileResponse>> {
    auth.require(Permission::FileRead)?;
    Ok(Json(state.store.get_owned_file(&auth, &id).await?.into()))
}

async fn get_file_content(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    auth.require(Permission::FileRead)?;
    let file = state.store.get_owned_file(&auth, &id).await?;
    stream_file(&state, file).await
}

async fn delete_file(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    auth.require(Permission::FileDelete)?;
    state
        .store
        .remove_file(&state.object_store, &auth, &id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize)]
struct DownloadQuery {
    tenant: String,
    owner: String,
    delivery: DownloadDelivery,
    expires: u64,
    signature: String,
}

async fn create_file_download(
    State(state): State<AppState>,
    auth: AuthContext,
    Path(id): Path<String>,
    Json(request): Json<CreateDownload>,
) -> ApiResult<Json<DownloadGrant>> {
    auth.require(Permission::FileGrant)?;
    state.store.get_owned_file(&auth, &id).await?;
    if request.delivery == DownloadDelivery::Redirect && !state.object_store.supports_public_urls()
    {
        return Err(ApiError::BadRequest(
            "redirect delivery requires S3_PUBLIC_URL".into(),
        ));
    }
    Ok(Json(capability::file_url(
        &state.config,
        &auth,
        &id,
        request.delivery,
    )))
}

async fn download_file(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<DownloadQuery>,
) -> ApiResult<Response> {
    let actor = Actor {
        tenant_id: query.tenant,
        principal_id: query.owner,
    };
    if !capability::verify_file(
        &state.config,
        &actor,
        &id,
        query.delivery,
        query.expires,
        &query.signature,
    ) {
        return Err(ApiError::NotFound("File capability not found.".into()));
    }
    let file = state.store.get_owned_file(&actor, &id).await?;
    match query.delivery {
        DownloadDelivery::Redirect => {
            let url = state
                .object_store
                .presigned_get(
                    &file.storage_key,
                    state.config.capability_ttl_seconds,
                    Some(&file.mime_type),
                    Some(&content_disposition(&file.filename)),
                )
                .await
                .map_err(ApiError::ObjectStore)?
                .ok_or_else(|| ApiError::BadRequest("S3_PUBLIC_URL is not configured".into()))?;
            Ok(axum::response::Redirect::temporary(&url).into_response())
        }
        DownloadDelivery::Proxy => stream_file(&state, file).await,
    }
}

async fn stream_file(state: &AppState, file: crate::model::FileRecord) -> ApiResult<Response> {
    let stream = state
        .object_store
        .get_stream(&file.storage_key)
        .await
        .map_err(ApiError::ObjectStore)?;
    let body = Body::from_stream(tokio_util::io::ReaderStream::new(stream.into_async_read()));
    let disposition = content_disposition(&file.filename);
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(&file.mime_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&file.size.to_string()).expect("numeric content length"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&disposition)
            .unwrap_or_else(|_| HeaderValue::from_static("attachment")),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    Ok(response)
}

fn content_disposition(filename: &str) -> String {
    format!(
        "attachment; filename=\"{}\"; filename*=UTF-8''{}",
        ascii_filename(filename),
        percent_encode_filename(filename)
    )
}

fn ascii_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|character| {
            if character.is_ascii_graphic() && character != '"' && character != '\\' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn percent_encode_filename(filename: &str) -> String {
    url::form_urlencoded::byte_serialize(filename.as_bytes()).collect()
}
