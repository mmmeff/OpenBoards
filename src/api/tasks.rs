use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::api::messages::parse_bool_param;
use crate::boards;
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::filter::LabelPredicate;
use crate::tasks::{
    self, AssignInput, BlockInput, CancelInput, ClaimInput, ClaimReadyInput, CompleteInput,
    CreateManyTasksInput, CreateTaskInput, FailInput, ReleaseInput, RenewInput, TaskListQuery,
    UpdateNeedsInput,
};
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/messageboard/boards/{board}/tasks",
            get(list_tasks).post(create_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/batch",
            post(create_many_tasks),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/claim-ready",
            post(claim_ready),
        )
        .route("/v1/messageboard/boards/{board}/tasks/{id}", get(get_task))
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/claim",
            post(claim_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/renew",
            post(renew_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/release",
            post(release_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/complete",
            post(complete_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/fail",
            post(fail_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/block",
            post(block_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/unblock",
            post(unblock_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/reopen",
            post(reopen_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/cancel",
            post(cancel_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/assign",
            post(assign_task),
        )
        .route(
            "/v1/messageboard/boards/{board}/tasks/{id}/needs",
            post(update_needs).put(update_needs),
        )
}

async fn create_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<CreateTaskInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::create_task(&state.db, &resolved.id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    let status = if res.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    let mut body = json!({ "task": res.task });
    if let Some(claim) = res.claim {
        body["claim"] = json!(claim);
    }
    Ok((status, Json(body)))
}

async fn create_many_tasks(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<CreateManyTasksInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::create_many_tasks(&state.db, &resolved.id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    let status = if res.replayed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(json!({ "tasks": res.tasks }))))
}

async fn list_tasks(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> BoardResult<Json<Value>> {
    let mut ensure = None;
    let mut query = TaskListQuery::default();
    for (k, v) in pairs {
        match k.as_str() {
            "ensure" => ensure = Some(parse_bool_param("ensure", &v)?),
            "channel" => query.channel = Some(v),
            "state" => query.state = Some(v),
            "label" => query.labels.push(LabelPredicate::parse(&v)?),
            "by" => query.by = Some(v),
            "owner" => query.owner = Some(v),
            "for" => query.assigned_for = Some(v),
            "needs" => query.needs = Some(v),
            "after" => query.after = Some(v),
            "limit" => {
                let n: usize = v
                    .parse()
                    .map_err(|_| BoardError::bad_request(format!("invalid limit '{v}'")))?;
                query.limit = Some(n);
            }
            _ => {}
        }
    }
    let resolved = boards::resolve_board(&state.db, &board, ensure, &caller).await?;
    let page = tasks::list_tasks(&state.db, &state.events, &resolved.id, &caller, query).await?;
    Ok(Json(json!(page)))
}

async fn get_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let task = tasks::get_task(&state.db, &state.events, &resolved.id, &id, &caller).await?;
    Ok(Json(json!({ "task": task })))
}

async fn claim_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    body: Option<Json<ClaimInput>>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let input = body.map(|Json(b)| b).unwrap_or_default();
    let res = tasks::claim_task(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!(res.claim)))
}

async fn claim_ready(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<ClaimReadyInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let (preds, lease_ms, wait_dur) = tasks::parse_claim_ready_params(&input)?;

    // Subscribe BEFORE querying SQLite so concurrent task.ready events cannot be missed.
    let mut rx = state.events.subscribe(&resolved.id);

    let attempt = tasks::try_claim_ready_once(
        &state.db,
        &resolved.id,
        &caller,
        &input.channel,
        &preds,
        &input.from,
        lease_ms,
    )
    .await?;
    for seq in attempt.expired_seqs {
        state.events.notify(&resolved.id, seq);
    }
    if let Some(res) = attempt.claimed {
        for seq in res.event_seqs {
            state.events.notify(&resolved.id, seq);
        }
        return Ok((StatusCode::OK, Json(json!(res.claim))));
    }
    let mut next_expiry = attempt.next_expiry;

    if wait_dur.is_zero() {
        return Err(BoardError::new(
            ErrorCode::NothingReady,
            "no ready task matching criteria",
        ));
    }

    let deadline = tokio::time::Instant::now() + wait_dur;
    loop {
        let now_instant = tokio::time::Instant::now();
        if now_instant >= deadline {
            break;
        }

        // Wake up at either the overall wait deadline OR the earliest active lease expiry
        let wake_at = match next_expiry {
            Some(exp_utc) => {
                let remaining = (exp_utc - Utc::now())
                    .to_std()
                    .unwrap_or(std::time::Duration::from_millis(5));
                let exp_instant = now_instant + remaining + std::time::Duration::from_millis(10);
                exp_instant.min(deadline)
            }
            None => deadline,
        };

        tokio::select! {
            recv = rx.recv() => {
                if matches!(recv, Err(tokio::sync::broadcast::error::RecvError::Closed)) {
                    break;
                }
            }
            _ = tokio::time::sleep_until(wake_at) => {}
        }

        let attempt = tasks::try_claim_ready_once(
            &state.db,
            &resolved.id,
            &caller,
            &input.channel,
            &preds,
            &input.from,
            lease_ms,
        )
        .await?;
        for seq in attempt.expired_seqs {
            state.events.notify(&resolved.id, seq);
        }
        if let Some(res) = attempt.claimed {
            for seq in res.event_seqs {
                state.events.notify(&resolved.id, seq);
            }
            return Ok((StatusCode::OK, Json(json!(res.claim))));
        }
        next_expiry = attempt.next_expiry;
    }

    Ok((
        StatusCode::REQUEST_TIMEOUT,
        Json(json!({
            "code": "timeout",
            "message": "timed out waiting for a ready task",
            "task": Value::Null
        })),
    ))
}

async fn renew_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<RenewInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::renew_task(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!(res.claim)))
}

async fn release_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<ReleaseInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::release_task(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task })))
}

async fn complete_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<CompleteInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::complete_task(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task, "readied": res.readied })))
}

async fn fail_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<FailInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::fail_task(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task })))
}

async fn block_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<BlockInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::block_task(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task })))
}

async fn unblock_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::unblock_task(&state.db, &resolved.id, &id, &caller).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task })))
}

async fn reopen_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::reopen_task(&state.db, &resolved.id, &id, &caller).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task })))
}

async fn cancel_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<CancelInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::cancel_task(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task })))
}

async fn assign_task(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<AssignInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::assign_task(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task })))
}

async fn update_needs(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<UpdateNeedsInput>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller).await?;
    let res = tasks::update_task_needs(&state.db, &resolved.id, &id, &caller, input).await?;
    for seq in res.event_seqs {
        state.events.notify(&resolved.id, seq);
    }
    Ok(Json(json!({ "task": res.task })))
}
