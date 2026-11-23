use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::boards;
use crate::error::BoardResult;
use crate::messages::{
    self, BasicMessageQuery, DeleteMessageInput, EditMessageInput, PostMessageInput, ReactInput,
    ReplyMessageInput,
};
use crate::AppState;

#[derive(Debug, Default, Deserialize)]
struct ListQueryParams {
    ensure: Option<bool>,
    channel: Option<String>,
    re: Option<String>,
    by: Option<String>,
    after: Option<String>,
    before: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Default, Deserialize)]
struct DeleteQueryParams {
    ensure: Option<bool>,
    revision: Option<i64>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/messageboard/boards/{board}/messages",
            get(list_messages).post(post_message),
        )
        .route(
            "/v1/messageboard/boards/{board}/messages/{id}",
            get(get_message).patch(edit_message).delete(delete_message),
        )
        .route(
            "/v1/messageboard/boards/{board}/messages/{id}/replies",
            post(reply_message),
        )
        .route(
            "/v1/messageboard/boards/{board}/messages/{id}/reactions",
            post(react_message),
        )
        .route(
            "/v1/messageboard/boards/{board}/messages/{id}/reactions/{emoji}",
            delete(remove_reaction),
        )
        .route("/v1/messageboard/boards/{board}/threads", get(list_threads))
        .route(
            "/v1/messageboard/boards/{board}/threads/{id}",
            get(get_thread),
        )
        .route(
            "/v1/messageboard/boards/{board}/posted/{key}",
            get(get_posted),
        )
}

async fn post_message(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<PostMessageInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let res = messages::post_message(&state.db, &resolved.id, &caller, input)?;
    let status = if res.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(json!(res))))
}

async fn reply_message(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<ReplyMessageInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let res = messages::reply_message(&state.db, &resolved.id, &id, &caller, input)?;
    let status = if res.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(json!(res))))
}

async fn get_message(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let msg = messages::get_message(&state.db, &resolved.id, &id, &caller)?;
    Ok(Json(json!(msg)))
}

async fn edit_message(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<EditMessageInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let (msg, mentions) = messages::edit_message(&state.db, &resolved.id, &id, &caller, input)?;
    let mut body = json!({ "ok": true, "message": msg });
    if !mentions.is_empty() {
        body["mentions"] = json!(mentions);
    }
    Ok(Json(body))
}

async fn delete_message(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<DeleteQueryParams>,
    body: Option<Json<DeleteMessageInput>>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let revision = body.and_then(|Json(b)| b.revision).or(query.revision);
    let _ = messages::delete_message(
        &state.db,
        &resolved.id,
        &id,
        &caller,
        DeleteMessageInput { revision },
    )?;
    Ok(Json(json!({ "ok": true })))
}

async fn react_message(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<ReactInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let (emoji, _changed) = messages::react_message(&state.db, &resolved.id, &id, &caller, input)?;
    Ok(Json(json!({ "ok": true, "emoji": emoji })))
}

async fn remove_reaction(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id, emoji)): Path<(String, String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let (emoji, _changed) = messages::react_message(
        &state.db,
        &resolved.id,
        &id,
        &caller,
        ReactInput {
            emoji,
            remove: true,
        },
    )?;
    Ok(Json(json!({ "ok": true, "emoji": emoji })))
}

async fn list_messages(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(params): Query<ListQueryParams>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, params.ensure, &caller)?;
    let page = messages::list_messages(
        &state.db,
        &resolved.id,
        &caller,
        BasicMessageQuery {
            channel: params.channel,
            re: params.re,
            by: params.by,
            after: params.after,
            before: params.before,
            limit: params.limit,
        },
    )?;
    Ok(Json(json!(page)))
}

async fn list_threads(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(params): Query<ListQueryParams>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, params.ensure, &caller)?;
    let page = messages::list_threads(
        &state.db,
        &resolved.id,
        &caller,
        BasicMessageQuery {
            channel: params.channel,
            re: params.re,
            by: params.by,
            after: params.after,
            before: params.before,
            limit: params.limit,
        },
    )?;
    Ok(Json(json!(page)))
}

async fn get_thread(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let thread = messages::get_thread(&state.db, &resolved.id, &id, &caller)?;
    Ok(Json(json!(thread)))
}

async fn get_posted(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, key)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let msg = messages::get_posted_by_idempotency_key(&state.db, &resolved.id, &caller, &key)?;
    Ok(Json(json!(msg)))
}
