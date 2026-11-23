use axum::extract::{Path, Query, State};
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::boards;
use crate::error::BoardResult;
use crate::labels::{self, UpdateLabelsInput};
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new().route(
        "/v1/messageboard/boards/{board}/messages/{id}/labels",
        post(update_labels).patch(update_labels),
    )
}

async fn update_labels(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<UpdateLabelsInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let outcome = labels::update_labels(&state.db, &resolved.id, &id, &caller, input).await?;
    if let Some(seq) = outcome.event_seq {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({
        "ok": true,
        "set": outcome.set,
        "unset": outcome.unset,
        "labels": outcome.labels
    })))
}
