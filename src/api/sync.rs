use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::boards;
use crate::error::BoardResult;
use crate::sync::{self, LinkThreadInput, RegisterSyncSourceInput, TypingInput};
use crate::AppState;

#[derive(Debug, Default, Deserialize)]
struct LinkedQuery {
    ensure: Option<bool>,
    url: String,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/messageboard/boards/{board}/sync/sources",
            get(list_sources).post(register_source),
        )
        .route(
            "/v1/messageboard/boards/{board}/sync/sources/{key}",
            delete(delete_source),
        )
        .route(
            "/v1/messageboard/boards/{board}/sync/sources/{key}/inbox",
            get(get_inbox),
        )
        .route(
            "/v1/messageboard/boards/{board}/sync/sources/{key}/records",
            get(get_source_records),
        )
        .route(
            "/v1/messageboard/boards/{board}/sync/linked",
            get(find_linked),
        )
        .route(
            "/v1/messageboard/boards/{board}/threads/{id}/sync",
            get(list_thread_links).post(link_thread),
        )
        .route(
            "/v1/messageboard/boards/{board}/threads/{id}/typing",
            post(send_typing),
        )
        // Inbound webhook ingress (unauthenticated; verified via platform HMAC signature)
        .route("/webhooks/{key}", post(webhook_global))
        .route(
            "/v1/messageboard/boards/{board}/webhooks/{key}",
            post(webhook_board),
        )
}

async fn register_source(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<RegisterSyncSourceInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let (source, created) = sync::register_source(&state.db, &resolved.id, &caller, input)?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(json!({ "source": source, "created": created }))))
}

async fn list_sources(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let sources = sync::list_sources(&state.db, &resolved.id, &caller)?;
    Ok(Json(json!({ "sources": sources })))
}

async fn delete_source(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, key)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let deleted = sync::delete_source(&state.db, &resolved.id, &key, &caller)?;
    Ok(Json(json!({ "ok": true, "deleted": deleted })))
}

async fn get_inbox(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, key)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let events = sync::get_inbox(&state.db, &resolved.id, &key, &caller)?;
    Ok(Json(json!({ "inbox": events })))
}

async fn get_source_records(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, key)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let (source, links, deliveries) =
        sync::get_source_records(&state.db, &resolved.id, &key, &caller)?;
    Ok(Json(json!({
        "source": source,
        "links": links,
        "deliveries": deliveries
    })))
}

async fn list_thread_links(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let targets = sync::list_thread_links(&state.db, &resolved.id, &id, &caller)?;
    Ok(Json(json!({ "targets": targets })))
}

async fn link_thread(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<LinkThreadInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let (link, seqs) = sync::link_thread(&state.db, &resolved.id, &id, &caller, input)?;
    for seq in seqs {
        state.events.notify(&resolved.id, seq);
    }
    state.sync_wake.notify_one();
    Ok((
        StatusCode::CREATED,
        Json(json!({ "link": link, "attached": true })),
    ))
}

async fn find_linked(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<LinkedQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let thread = sync::find_linked_thread(&state.db, &resolved.id, &query.url, &caller)?;
    Ok(Json(json!(thread)))
}

async fn send_typing(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<TypingInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let targets = sync::send_typing(&state.db, &resolved.id, &id, &caller, input).await?;
    Ok(Json(json!({ "targets": targets })))
}

async fn webhook_global(
    State(state): State<AppState>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> BoardResult<Json<Value>> {
    let res = sync::handle_inbound_webhook(&state, None, &key, &headers, &body)?;
    Ok(Json(res))
}

async fn webhook_board(
    State(state): State<AppState>,
    Path((board, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> BoardResult<Json<Value>> {
    let board_view = state
        .db
        .with_tx(|tx| {
            if let Some(b) = boards::find_board_tx(tx, &board)? {
                return Ok(Some(b));
            }
            if board.eq_ignore_ascii_case(boards::DEFAULT_BOARD_NAME) {
                return Ok(Some(boards::ensure_default_board_tx(tx)?));
            }
            Ok(None)
        })?
        .ok_or_else(|| {
            crate::error::BoardError::new(
                crate::error::ErrorCode::BoardNotFound,
                format!("board '{board}' not found"),
            )
        })?;
    let res = sync::handle_inbound_webhook(&state, Some(&board_view.id), &key, &headers, &body)?;
    Ok(Json(res))
}
