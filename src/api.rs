use axum::{
    Json, Router,
    extract::{FromRequestParts, Path, Query, State},
    http::{StatusCode, header::HeaderMap, request::Parts},
    routing::{get, patch, post},
};
use sqlx::PgPool;
use tower_http::trace::TraceLayer;

use crate::{
    error::{ApiError, ApiResult},
    model::{
        Actor, AppendItems, AppendResult, Continuation, ContinuationQuery, Conversation,
        CreateContinuation, CreateConversation, CreateTurn, Item, ListItemsQuery, ReplayRequest,
        ReplayResult, Turn, UpdateTurn,
    },
    store,
};

pub fn router(pool: PgPool) -> Router {
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
        .layer(TraceLayer::new_for_http())
        .with_state(pool)
}

impl FromRequestParts<PgPool> for Actor {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &PgPool,
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

async fn health(State(pool): State<PgPool>) -> ApiResult<StatusCode> {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&pool)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn create_conversation(
    State(pool): State<PgPool>,
    actor: Actor,
    Json(request): Json<CreateConversation>,
) -> ApiResult<(StatusCode, Json<Conversation>)> {
    Ok((
        StatusCode::CREATED,
        Json(store::create_conversation(&pool, &actor, request).await?),
    ))
}

async fn get_conversation(
    State(pool): State<PgPool>,
    actor: Actor,
    Path(id): Path<String>,
) -> ApiResult<Json<Conversation>> {
    Ok(Json(store::get_conversation(&pool, &actor, &id).await?))
}

async fn list_items(
    State(pool): State<PgPool>,
    actor: Actor,
    Path(id): Path<String>,
    Query(query): Query<ListItemsQuery>,
) -> ApiResult<Json<Vec<Item>>> {
    Ok(Json(
        store::list_items(
            &pool,
            &actor,
            &id,
            query.after_seq,
            query.limit.unwrap_or(100),
        )
        .await?,
    ))
}

async fn append_items(
    State(pool): State<PgPool>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<AppendItems>,
) -> ApiResult<Json<AppendResult>> {
    Ok(Json(
        store::append_items(&pool, &actor, &id, request).await?,
    ))
}

async fn replay(
    State(pool): State<PgPool>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<ReplayRequest>,
) -> ApiResult<Json<ReplayResult>> {
    Ok(Json(store::replay(&pool, &actor, &id, request).await?))
}

async fn create_turn(
    State(pool): State<PgPool>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<CreateTurn>,
) -> ApiResult<(StatusCode, Json<Turn>)> {
    Ok((
        StatusCode::CREATED,
        Json(store::create_turn(&pool, &actor, &id, request).await?),
    ))
}

async fn update_turn(
    State(pool): State<PgPool>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<UpdateTurn>,
) -> ApiResult<Json<Turn>> {
    Ok(Json(store::update_turn(&pool, &actor, &id, request).await?))
}

async fn create_continuation(
    State(pool): State<PgPool>,
    actor: Actor,
    Path(id): Path<String>,
    Json(request): Json<CreateContinuation>,
) -> ApiResult<(StatusCode, Json<Continuation>)> {
    Ok((
        StatusCode::CREATED,
        Json(store::create_continuation(&pool, &actor, &id, request).await?),
    ))
}

async fn get_continuation(
    State(pool): State<PgPool>,
    actor: Actor,
    Path(response_id): Path<String>,
    Query(query): Query<ContinuationQuery>,
) -> ApiResult<Json<Continuation>> {
    Ok(Json(
        store::get_continuation(&pool, &actor, &response_id, &query.agent_ref).await?,
    ))
}
