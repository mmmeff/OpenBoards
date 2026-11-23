use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::boards;
use crate::error::{BoardError, BoardResult};
use crate::filter::{LabelPredicate, QueryFilter};
use crate::messages::{
    self, DeleteMessageInput, EditMessageInput, MessageListQuery, PostMessageInput, ReactInput,
    ReplyMessageInput,
};
use crate::AppState;

#[derive(Debug, Default, Deserialize)]
struct DeleteQueryParams {
    ensure: Option<bool>,
    revision: Option<i64>,
}

/// Parses repeated query pairs (`?label=a=1&label=b=2|3&channel=dev/&...`) into `ensure` + `MessageListQuery`.
pub fn parse_list_query_pairs(
    pairs: Vec<(String, String)>,
) -> BoardResult<(Option<bool>, MessageListQuery)> {
    let mut ensure = None;
    let mut filter = QueryFilter::default();
    let mut after = None;
    let mut before = None;
    let mut limit = None;

    for (k, v) in pairs {
        match k.as_str() {
            "ensure" => {
                ensure = Some(parse_bool_param("ensure", &v)?);
            }
            "channel" => filter.channel = Some(v),
            "re" => filter.re = Some(v),
            "by" => filter.by = Some(v),
            "mentions" => filter.mentions = Some(v),
            "label" => filter.labels.push(LabelPredicate::parse(&v)?),
            "from" | "source" => filter.from = Some(v),
            "text" => filter.text = Some(v),
            "after" => after = Some(v),
            "before" => before = Some(v),
            "limit" => {
                let parsed: usize = v
                    .parse()
                    .map_err(|_| BoardError::bad_request(format!("invalid limit '{v}'")))?;
                limit = Some(parsed);
            }
            _ => {}
        }
    }

    Ok((
        ensure,
        MessageListQuery {
            filter,
            after,
            before,
            limit,
        },
    ))
}

pub fn parse_bool_param(name: &str, v: &str) -> BoardResult<bool> {
    match v.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(BoardError::bad_request(format!(
            "invalid boolean value '{other}' for '{name}'"
        ))),
    }
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
    if let Some(seq) = res.event_seq {
        state.events.notify(&resolved.id, seq);
        state.sync_wake.notify_one();
    }
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
    if let Some(seq) = res.event_seq {
        state.events.notify(&resolved.id, seq);
        state.sync_wake.notify_one();
    }
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
    let (msg, mentions, seqs) = messages::edit_message(&state.db, &resolved.id, &id, &caller, input)?;
    for seq in seqs {
        state.events.notify(&resolved.id, seq);
    }
    state.sync_wake.notify_one();
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
    for seq in messages::delete_message(
        &state.db,
        &resolved.id,
        &id,
        &caller,
        DeleteMessageInput { revision },
    )? {
        state.events.notify(&resolved.id, seq);
    }
    state.sync_wake.notify_one();
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
    let (emoji, event_seq) = messages::react_message(&state.db, &resolved.id, &id, &caller, input)?;
    if let Some(seq) = event_seq {
        state.events.notify(&resolved.id, seq);
        state.sync_wake.notify_one();
    }
    Ok(Json(json!({ "ok": true, "emoji": emoji })))
}

async fn remove_reaction(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id, emoji)): Path<(String, String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let (emoji, event_seq) = messages::react_message(
        &state.db,
        &resolved.id,
        &id,
        &caller,
        ReactInput {
            emoji,
            remove: true,
        },
    )?;
    if let Some(seq) = event_seq {
        state.events.notify(&resolved.id, seq);
        state.sync_wake.notify_one();
    }
    Ok(Json(json!({ "ok": true, "emoji": emoji })))
}

async fn list_messages(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> BoardResult<Json<Value>> {
    let (ensure, query) = parse_list_query_pairs(pairs)?;
    let resolved = boards::resolve_board(&state.db, &board, ensure, &caller)?;
    let page = messages::list_messages(&state.db, &resolved.id, &caller, query)?;
    Ok(Json(json!(page)))
}

async fn list_threads(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> BoardResult<Json<Value>> {
    let (ensure, query) = parse_list_query_pairs(pairs)?;
    let resolved = boards::resolve_board(&state.db, &board, ensure, &caller)?;
    let page = messages::list_threads(&state.db, &resolved.id, &caller, query)?;
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
