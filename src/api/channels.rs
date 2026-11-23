use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::boards;
use crate::channels::{self, CreateChannelInput};
use crate::error::BoardResult;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new().route(
        "/v1/messageboard/boards/{board}/channels",
        get(list_channels).post(create_channel),
    )
}

async fn list_channels(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let list = channels::list_channels(&state.db, &resolved.id, &caller).await?;
    Ok(Json(json!({ "channels": list })))
}

async fn create_channel(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<CreateChannelInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = channels::create_channel(&state.db, &resolved.id, &caller, input).await?;
    if let Some(seq) = res.event_seq {
        state.events.notify(&resolved.id, seq);
    }
    let status = if res.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(json!(res))))
}
