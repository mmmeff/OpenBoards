use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::boards::{self, CreateBoardInput, DEFAULT_BOARD_NAME};
use crate::error::BoardResult;
use crate::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct EnsureQuery {
    pub ensure: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DiscoveryQuery {
    pub board: Option<String>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/discovery", get(discovery_default))
        .route(
            "/v1/messageboard/boards/{board}/discovery",
            get(discovery_named),
        )
        .route(
            "/v1/messageboard/boards",
            get(list_boards).post(create_board),
        )
        .route("/v1/messageboard/boards/{board}", get(get_board))
}

fn build_discovery_payload(state: &AppState, headers: &HeaderMap, board_name: &str) -> Value {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .and_then(|raw| raw.split(',').next())
        .map(str::trim)
        .filter(|s| s.eq_ignore_ascii_case("https") || s.eq_ignore_ascii_case("http"))
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| "http".to_string());
    let host = headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .filter(|h| !h.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| state.config.listen.to_string());
    let team = &state.config.team;
    let url = format!("{scheme}://{host}/v1/messageboard/boards/{board_name}?teamId={team}");
    json!({
        "url": url,
        "team_id": team,
        "name": board_name
    })
}

async fn discovery_default(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<DiscoveryQuery>,
) -> Json<Value> {
    let name = query
        .board
        .as_deref()
        .filter(|b| !b.is_empty())
        .unwrap_or(DEFAULT_BOARD_NAME);
    let canonical_name = if name.eq_ignore_ascii_case(DEFAULT_BOARD_NAME) {
        DEFAULT_BOARD_NAME
    } else {
        name
    };
    Json(build_discovery_payload(&state, &headers, canonical_name))
}

async fn discovery_named(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(board): Path<String>,
) -> Json<Value> {
    let canonical_name = if board.eq_ignore_ascii_case(DEFAULT_BOARD_NAME) {
        DEFAULT_BOARD_NAME.to_string()
    } else {
        board
    };
    Json(build_discovery_payload(&state, &headers, &canonical_name))
}

async fn list_boards(
    State(state): State<AppState>,
    caller: CallerContext,
) -> BoardResult<Json<Value>> {
    let boards = boards::list_boards(&state.db, &caller).await?;
    Ok(Json(json!({ "boards": boards })))
}

async fn create_board(
    State(state): State<AppState>,
    caller: CallerContext,
    Json(input): Json<CreateBoardInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let (board, created) = boards::create_board(&state.db, &caller, input).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(json!({ "board": board, "created": created }))))
}

async fn get_board(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    Ok(Json(json!({ "board": resolved })))
}
