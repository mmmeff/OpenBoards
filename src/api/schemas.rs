use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::boards;
use crate::error::BoardResult;
use crate::schemas::{self, CreateSchemaInput};
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/schemas", get(list_global_schemas).post(create_global_schema))
        .route("/v1/schemas/{id}", get(get_global_schema))
        .route(
            "/v1/messageboard/boards/{board}/schemas",
            get(list_board_schemas).post(create_board_schema),
        )
        .route(
            "/v1/messageboard/boards/{board}/schemas/{id}",
            get(get_board_schema),
        )
}

async fn list_global_schemas(
    State(state): State<AppState>,
    _caller: CallerContext,
) -> BoardResult<Json<Value>> {
    let list = schemas::list_schemas(&state.db)?;
    Ok(Json(json!({ "schemas": list })))
}

async fn get_global_schema(
    State(state): State<AppState>,
    _caller: CallerContext,
    Path(id): Path<String>,
) -> BoardResult<Json<Value>> {
    let schema = schemas::get_schema(&state.db, &id)?;
    Ok(Json(json!({ "schema": schema })))
}

async fn create_global_schema(
    State(state): State<AppState>,
    caller: CallerContext,
    Json(input): Json<CreateSchemaInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let (schema, created) = schemas::create_schema(&state.db, None, &caller, input)?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(json!({ "schema": schema, "created": created }))))
}

async fn list_board_schemas(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let _resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let list = schemas::list_schemas(&state.db)?;
    Ok(Json(json!({ "schemas": list })))
}

async fn get_board_schema(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let _resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let schema = schemas::get_schema(&state.db, &id)?;
    Ok(Json(json!({ "schema": schema })))
}

async fn create_board_schema(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<CreateSchemaInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let (schema, created) = schemas::create_schema(&state.db, Some(&resolved.id), &caller, input)?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(json!({ "schema": schema, "created": created }))))
}
