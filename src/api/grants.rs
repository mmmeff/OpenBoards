use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, patch};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::boards;
use crate::error::BoardResult;
use crate::grants::{self, CreateGrantInput, ExplainQuery, UpdateGrantInput};
use crate::AppState;

#[derive(Debug, Default, Deserialize)]
struct ExplainHttpQuery {
    ensure: Option<bool>,
    channel: String,
    user: Option<String>,
    project: Option<String>,
    environment: Option<String>,
    app: Option<String>,
    principal: Option<String>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/messageboard/boards/{board}/grants",
            get(list_grants).post(create_grant),
        )
        .route(
            "/v1/messageboard/boards/{board}/grants/explain",
            get(explain_grant),
        )
        .route(
            "/v1/messageboard/boards/{board}/grants/{id}",
            patch(update_grant).delete(revoke_grant),
        )
}

async fn list_grants(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let board_grants = grants::load_grants(&state.db, &resolved.id)?;
    grants::require_list_grants_access(&board_grants, &caller)?;
    Ok(Json(json!({ "grants": board_grants })))
}

async fn create_grant(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<CreateGrantInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let grant = grants::create_grant(&state.db, &resolved.id, &caller, input)?;
    Ok((StatusCode::CREATED, Json(json!({ "grant": grant }))))
}

async fn update_grant(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<UpdateGrantInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let grant = grants::update_grant(&state.db, &resolved.id, &id, &caller, input)?;
    Ok(Json(json!({ "grant": grant })))
}

async fn revoke_grant(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    grants::revoke_grant(&state.db, &resolved.id, &id, &caller)?;
    Ok(Json(json!({ "ok": true, "revoked": id })))
}

async fn explain_grant(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<ExplainHttpQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let explanation = grants::explain_access(
        &state.db,
        &resolved.id,
        &caller,
        ExplainQuery {
            channel: query.channel,
            user: query.user,
            project: query.project,
            environment: query.environment,
            app: query.app,
            principal: query.principal,
        },
    )?;
    Ok(Json(json!(explanation)))
}
