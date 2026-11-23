use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::messages::parse_bool_param;
use crate::boards;
use crate::error::{BoardError, BoardResult};
use crate::events::{
    self, EventQuery, EventType, DEFAULT_WAIT_TIMEOUT_SECS, MAX_POLL_WAIT_SECS,
    MAX_WAIT_TIMEOUT_SECS,
};
use crate::filter::{LabelPredicate, QueryFilter};
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/messageboard/boards/{board}/events", get(poll_events))
        .route("/v1/messageboard/boards/{board}/wait", get(wait_event))
}

struct ParsedEventParams {
    ensure: Option<bool>,
    query: EventQuery,
    wait: Option<Duration>,
    timeout: Option<Duration>,
}

fn parse_event_params(pairs: Vec<(String, String)>) -> BoardResult<ParsedEventParams> {
    let mut ensure = None;
    let mut filter = QueryFilter::default();
    let mut after = None;
    let mut limit = None;
    let mut all = false;
    let mut types = Vec::new();
    let mut wait = None;
    let mut timeout = None;

    for (k, v) in pairs {
        match k.as_str() {
            "ensure" => ensure = Some(parse_bool_param("ensure", &v)?),
            "channel" => filter.channel = Some(v),
            "re" => filter.re = Some(v),
            "by" => filter.by = Some(v),
            "mentions" => filter.mentions = Some(v),
            "label" => filter.labels.push(LabelPredicate::parse(&v)?),
            "from" | "source" => filter.from = Some(v),
            "text" => filter.text = Some(v),
            "after" => after = Some(v),
            "all" => all = parse_bool_param("all", &v)?,
            "limit" => {
                let n: usize = v
                    .parse()
                    .map_err(|_| BoardError::bad_request(format!("invalid limit '{v}'")))?;
                limit = Some(n);
            }
            "types" | "type" => {
                for part in v.split(',') {
                    let trimmed = part.trim();
                    if !trimmed.is_empty() {
                        types.push(EventType::parse(trimmed)?);
                    }
                }
            }
            "wait" => {
                wait = Some(events::parse_duration_param(&v, MAX_POLL_WAIT_SECS)?);
            }
            "timeout" => {
                timeout = Some(events::parse_duration_param(&v, MAX_WAIT_TIMEOUT_SECS)?);
            }
            _ => {}
        }
    }

    filter.validate()?;

    Ok(ParsedEventParams {
        ensure,
        query: EventQuery {
            filter,
            after,
            limit,
            all,
            types,
        },
        wait,
        timeout,
    })
}

async fn poll_events(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> BoardResult<Json<Value>> {
    let parsed = parse_event_params(pairs)?;
    let resolved = boards::resolve_board(&state.db, &board, parsed.ensure, &caller).await?;
    let wait_dur = parsed.wait.unwrap_or(Duration::ZERO);

    let mut rx = state.events.subscribe(&resolved.id);

    let after_seq = {
        let mut conn = state.db.read_conn();
        match &parsed.query.after {
            Some(c) => events::resolve_event_cursor_tx(&mut conn, &resolved.id, c).await?,
            None => {
                if wait_dur.is_zero() {
                    0
                } else {
                    events::current_event_seq_tx(&mut conn, &resolved.id).await?
                }
            }
        }
    };

    let page =
        events::query_events_after(&state.db, &resolved.id, &caller, after_seq, &parsed.query)
            .await?;
    if !page.events.is_empty() || wait_dur.is_zero() {
        return Ok(Json(json!(page)));
    }

    let deadline = tokio::time::Instant::now() + wait_dur;
    loop {
        tokio::select! {
            recv = rx.recv() => {
                match recv {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let page = events::query_events_after(
                            &state.db,
                            &resolved.id,
                            &caller,
                            after_seq,
                            &parsed.query,
                        ).await?;
                        if !page.events.is_empty() {
                            return Ok(Json(json!(page)));
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        let page = events::query_events_after(
                            &state.db,
                            &resolved.id,
                            &caller,
                            after_seq,
                            &parsed.query,
                        ).await?;
                        return Ok(Json(json!(page)));
                    }
                }
            }
            _ = tokio::time::sleep_until(deadline) => {
                let page = events::query_events_after(
                    &state.db,
                    &resolved.id,
                    &caller,
                    after_seq,
                    &parsed.query,
                ).await?;
                return Ok(Json(json!(page)));
            }
        }
    }
}

async fn wait_event(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(pairs): Query<Vec<(String, String)>>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let mut parsed = parse_event_params(pairs)?;
    parsed.query.limit = Some(1);
    let resolved = boards::resolve_board(&state.db, &board, parsed.ensure, &caller).await?;
    let timeout_dur = parsed
        .timeout
        .or(parsed.wait)
        .unwrap_or_else(|| Duration::from_secs_f64(DEFAULT_WAIT_TIMEOUT_SECS));

    let mut rx = state.events.subscribe(&resolved.id);

    let after_seq = {
        let mut conn = state.db.read_conn();
        match &parsed.query.after {
            Some(c) => events::resolve_event_cursor_tx(&mut conn, &resolved.id, c).await?,
            None => events::current_event_seq_tx(&mut conn, &resolved.id).await?,
        }
    };

    let page =
        events::query_events_after(&state.db, &resolved.id, &caller, after_seq, &parsed.query)
            .await?;
    if let Some(ev) = page.events.into_iter().next() {
        let cursor = ev.cursor.clone();
        return Ok((StatusCode::OK, Json(json!({ "event": ev, "cursor": cursor }))));
    }

    if timeout_dur.is_zero() {
        return Ok((
            StatusCode::REQUEST_TIMEOUT,
            Json(json!({
                "code": "timeout",
                "message": "timed out waiting for matching event",
                "cursor": page.cursor
            })),
        ));
    }

    let deadline = tokio::time::Instant::now() + timeout_dur;
    loop {
        tokio::select! {
            recv = rx.recv() => {
                match recv {
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let page = events::query_events_after(
                            &state.db,
                            &resolved.id,
                            &caller,
                            after_seq,
                            &parsed.query,
                        ).await?;
                        if let Some(ev) = page.events.into_iter().next() {
                            let cursor = ev.cursor.clone();
                            return Ok((StatusCode::OK, Json(json!({ "event": ev, "cursor": cursor }))));
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = tokio::time::sleep_until(deadline) => break,
        }
    }

    let final_page =
        events::query_events_after(&state.db, &resolved.id, &caller, after_seq, &parsed.query)
            .await?;
    if let Some(ev) = final_page.events.into_iter().next() {
        let cursor = ev.cursor.clone();
        return Ok((StatusCode::OK, Json(json!({ "event": ev, "cursor": cursor }))));
    }

    Ok((
        StatusCode::REQUEST_TIMEOUT,
        Json(json!({
            "code": "timeout",
            "message": "timed out waiting for matching event",
            "cursor": final_page.cursor
        })),
    ))
}
