use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::{self, CallerContext};
use crate::api::boards::EnsureQuery;
use crate::boards;
use crate::error::BoardResult;
use crate::grants;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/whoami", get(whoami))
        .route("/v1/messageboard/boards/{board}/me", get(board_me))
        .route("/v1/messageboard/boards/{board}/actors", get(list_actors))
}

async fn whoami(caller: CallerContext) -> Json<Value> {
    Json(json!(caller.whoami_view()))
}

async fn board_me(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let board_grants = grants::load_grants(&state.db, &resolved.id)?;
    let can = grants::summarize_caller_can(&board_grants, &caller);
    let mut view = serde_json::to_value(caller.whoami_view())
        .map_err(|e| crate::error::BoardError::internal(format!("serialization error: {e}")))?;
    if let Some(obj) = view.as_object_mut() {
        obj.insert("board".to_string(), json!(resolved));
        obj.insert("can".to_string(), json!(can));
    }
    Ok(Json(view))
}

async fn list_actors(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let board_grants = grants::load_grants(&state.db, &resolved.id)?;
    let has_wildcard_read =
        grants::caller_permissions_at_pattern(&board_grants, &caller, "*").can_read();

    let actors = state.db.with_conn(|conn| {
        actors::list_board_actors(conn, &resolved.id, has_wildcard_read, |channel| {
            grants::caller_permissions_on_channel(&board_grants, &caller, channel).can_read()
        })
    })?;
    Ok(Json(json!({ "actors": actors })))
}
