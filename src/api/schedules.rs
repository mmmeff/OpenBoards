use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::api::messages::parse_bool_param;
use crate::boards;
use crate::error::BoardResult;
use crate::schedules::{self, CreateScheduleInput, ScheduleListQuery};
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/messageboard/boards/{board}/schedules",
            get(list_schedules).post(create_schedule),
        )
        .route(
            "/v1/messageboard/boards/{board}/schedules/{id}",
            get(get_schedule).delete(cancel_schedule),
        )
}

async fn create_schedule(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<CreateScheduleInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let sched = schedules::create_schedule(&state.db, &resolved.id, &caller, input).await?;
    Ok((StatusCode::CREATED, Json(json!({ "schedule": sched }))))
}

async fn list_schedules(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> BoardResult<Json<Value>> {
    let mut ensure = None;
    let mut query = ScheduleListQuery::default();
    for (k, v) in pairs {
        match k.as_str() {
            "ensure" => ensure = Some(parse_bool_param("ensure", &v)?),
            "re" => query.re = Some(v),
            "channel" => query.channel = Some(v),
            "by" => query.by = Some(v),
            "all" => query.all = parse_bool_param("all", &v)?,
            _ => {}
        }
    }
    let resolved = boards::resolve_board(&state.db, &board, ensure, &caller).await?;
    let list = schedules::list_schedules(&state.db, &resolved.id, &caller, query).await?;
    Ok(Json(json!({ "schedules": list })))
}

async fn get_schedule(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let sched = schedules::get_schedule(&state.db, &resolved.id, &id, &caller).await?;
    Ok(Json(json!({ "schedule": sched })))
}

async fn cancel_schedule(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let (sched, canceled) =
        schedules::cancel_schedule(&state.db, &resolved.id, &id, &caller).await?;
    Ok(Json(json!({ "schedule": sched, "canceled": canceled })))
}
