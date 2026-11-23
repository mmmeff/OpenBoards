use axum::extract::{Path, State};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::{self, CallerContext};
use crate::error::BoardResult;
use crate::permissions::Permission;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/whoami", get(whoami))
        .route("/v1/messageboard/boards/{board}/actors", get(list_actors))
}

async fn whoami(caller: CallerContext) -> Json<Value> {
    Json(json!(caller.whoami_view()))
}

async fn list_actors(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
) -> BoardResult<Json<Value>> {
    let board_key = board.to_ascii_lowercase();
    let base_ceiling = caller.principal.permission_ceiling();
    let has_wildcard_read = caller
        .apply_narrowing_at_pattern("*", base_ceiling)
        .has(Permission::Read);

    let actors = state.db.with_conn(|conn| {
        actors::list_board_actors(conn, &board_key, has_wildcard_read, |channel| {
            caller
                .apply_narrowing(channel, base_ceiling)
                .has(Permission::Read)
        })
    })?;
    Ok(Json(json!({ "actors": actors })))
}
