use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, FromRequestParts, Multipart, Path, Query, State},
    http::{HeaderValue, StatusCode, header, header::HeaderMap, request::Parts},
    response::Response,
    routing::{get, patch, post},
};
use sqlx::PgPool;
use tower_http::trace::TraceLayer;

use crate::{
    capability,
    config::Config,
    error::{ApiError, ApiResult},
    files,
    model::{
        Actor, AppendItems, AppendResult, Continuation, ContinuationQuery, Conversation,
        CreateContinuation, CreateConversation, CreateTurn, FileResponse, Item, ListItemsQuery,
        ReplayRequest, ReplayResult, Turn, UpdateTurn,
    },
    object_store::ObjectStore,
    store,
};

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub object_store: ObjectStore,
    pub config: Config,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/conversations", post(create_conversation))
        .route("/v1/conversations/{id}", get(get_conversation))
        .route(
            "/v1/conversations/{id}/items",
            get(list_items).post(append_items),
        )
        .route("/v1/conversations/{id}/replay", post(replay))
        .route("/v1/conversations/{id}/turns", post(create_turn))
        .route("/v1/turns/{id}", patch(update_turn))
        .route(
            "/v1/conversations/{id}/continuations",
            post(create_continuation),
        )
        .route("/v1/continuations/{response_id}", get(get_continuation))
        .route("/v1/files", post(upload_file))
        .route("/v1/files/{id}", get(get_file).delete(delete_file))
        .route("/v1/capabilities/files/{id}", get(download_capability))
        // Open Responses permits ~32 MiB base64 file parts. Leave room for
        // the surrounding item and batch JSON while still bounding memory.
        .layer(DefaultBodyLimit::max(64 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

impl FromRequestParts<AppState> for Actor {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        fn required(headers: &HeaderMap, name: &'static str) -> ApiResult<String> {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.trim().is_empty() && value.len() <= 200)
                .map(str::to_owned)
                .ok_or_else(|| ApiError::BadRequest(format!("Missing or invalid {name} header.")))
        }
        Ok(Self {
            tenant_id: required(&parts.headers, "x-threadmark-tenant")?,
            principal_id: required(&parts.headers, "x-threadmark-principal")?,
        })
    }
}

async fn health(State(state): State<AppState>) -> ApiResult<StatusCode> {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await?;
    state
        .object_store
        .ping()
        .await
        .map_err(ApiError::ObjectStore)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_conversation(
    State(state): State<AppState>,
    actor: Actor,
    Json(request): Json<CreateConversation>,
) -> ApiResult<(StatusCode, Json<Conversation>)> {
    Ok((
        StatusCode::CREATED,
        Json(store::create_conversation(&state.pool, &actor, request).await?),
    ))
}

async fn get_conversation(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
) -> ApiResult<Json<Conversation>> {
    Ok(Json(
        store::get_conversation(&state.pool, &actor, &id).await?,
    ))
}

async fn list_items(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
    Query(query): Query<ListItemsQuery>,
) -> ApiResult<Json<Vec<Item>>> {
    Ok(Json(
        store::list_items(
            &state.pool,
            &actor,
            &id,
            query.after_seq,
            query.limit.unwrap_or(100),
        )
        .await?,
    ))
}

async fn append_items(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<AppendItems>,
) -> ApiResult<Json<AppendResult>> {
    Ok(Json(
        store::append_items(&state.pool, &actor, &id, request).await?,
    ))
}

async fn replay(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<ReplayRequest>,
) -> ApiResult<Json<ReplayResult>> {
    Ok(Json(store::replay(&state, &actor, &id, request).await?))
}

async fn create_turn(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<CreateTurn>,
) -> ApiResult<(StatusCode, Json<Turn>)> {
    Ok((
        StatusCode::CREATED,
        Json(store::create_turn(&state.pool, &actor, &id, request).await?),
    ))
}

async fn update_turn(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<UpdateTurn>,
) -> ApiResult<Json<Turn>> {
    Ok(Json(
        store::update_turn(&state.pool, &actor, &id, request).await?,
    ))
}

async fn create_continuation(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<CreateContinuation>,
) -> ApiResult<(StatusCode, Json<Continuation>)> {
    Ok((
        StatusCode::CREATED,
        Json(store::create_continuation(&state.pool, &actor, &id, request).await?),
    ))
}

async fn get_continuation(
    State(state): State<AppState>,
    actor: Actor,
    Path(response_id): Path<String>,
    Query(query): Query<ContinuationQuery>,
) -> ApiResult<Json<Continuation>> {
    Ok(Json(
        store::get_continuation(&state.pool, &actor, &response_id, &query.agent_ref).await?,
    ))
}

async fn upload_file(
    State(state): State<AppState>,
    actor: Actor,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<FileResponse>)> {
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
        let file = files::save(
            &state.pool,
            &state.object_store,
            &actor,
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

async fn get_file(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
) -> ApiResult<Json<FileResponse>> {
    Ok(Json(
        files::get_owned(&state.pool, &actor, &id).await?.into(),
    ))
}

async fn delete_file(
    State(state): State<AppState>,
    actor: Actor,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    files::remove(&state.pool, &state.object_store, &actor, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(serde::Deserialize)]
struct CapabilityQuery {
    tenant: String,
    owner: String,
    expires: u64,
    signature: String,
}

async fn download_capability(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<CapabilityQuery>,
) -> ApiResult<Response> {
    let actor = Actor {
        tenant_id: query.tenant,
        principal_id: query.owner,
    };
    if !capability::verify_file(&state.config, &actor, &id, query.expires, &query.signature) {
        return Err(ApiError::NotFound("File capability not found.".into()));
    }
    let (file, bytes) = files::bytes(&state.pool, &state.object_store, &actor, &id).await?;
    let disposition = format!(
        "attachment; filename=\"{}\"; filename*=UTF-8''{}",
        ascii_filename(&file.filename),
        percent_encode_filename(&file.filename)
    );
    let mut response = Response::new(Body::from(bytes));
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
