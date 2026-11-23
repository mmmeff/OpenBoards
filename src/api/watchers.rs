use std::collections::BTreeMap;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::boards::{self, DEFAULT_BOARD_NAME};
use crate::error::{BoardError, BoardResult};
use crate::events::EventType;
use crate::filter::LabelPredicate;
use crate::watchers::{self, UpsertWatcherInput, WatcherFilter};
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/messageboard/boards/{board}/watchers",
            post(register_watcher)
                .get(list_watchers)
                .delete(delete_board_watchers),
        )
        .route(
            "/v1/messageboard/boards/{board}/watchers/{id}",
            get(get_watcher).delete(delete_watcher),
        )
        .route("/api/board/watchers", post(sync_deployment_watchers))
}

#[derive(Debug, Default, Deserialize)]
struct BoardEnsureQuery {
    ensure: Option<bool>,
    owner: Option<String>,
    id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct DeploymentSyncQuery {
    board: Option<String>,
    ensure: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum LabelFilterInput {
    List(Vec<String>),
    Map(BTreeMap<String, String>),
}

impl LabelFilterInput {
    fn into_predicates(self) -> BoardResult<Vec<String>> {
        let mut out = Vec::new();
        match self {
            Self::List(list) => {
                for raw in list {
                    LabelPredicate::parse(&raw)?;
                    out.push(raw);
                }
            }
            Self::Map(map) => {
                for (k, v) in map {
                    let raw = if v.is_empty() {
                        k
                    } else {
                        format!("{k}={v}")
                    };
                    LabelPredicate::parse(&raw)?;
                    out.push(raw);
                }
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Default, Deserialize)]
struct StructuredFilterInput {
    channel: Option<String>,
    re: Option<String>,
    by: Option<String>,
    mentions: Option<String>,
    #[serde(alias = "labels")]
    label: Option<LabelFilterInput>,
    #[serde(alias = "source")]
    from: Option<String>,
    text: Option<String>,
    types: Option<Vec<String>>,
    all: Option<bool>,
    after: Option<String>,
}

#[derive(Debug, Deserialize)]
struct WatcherSpecBody {
    id: Option<String>,
    board: Option<String>,
    tail: Option<String>,
    #[serde(alias = "webhook_url", alias = "webhookUrl", alias = "endpoint")]
    url: Option<String>,
    secret: Option<String>,
    filter: Option<StructuredFilterInput>,
    // Convenience top-level filter fields
    channel: Option<String>,
    re: Option<String>,
    by: Option<String>,
    mentions: Option<String>,
    #[serde(alias = "labels")]
    label: Option<LabelFilterInput>,
    #[serde(alias = "source")]
    from: Option<String>,
    text: Option<String>,
    types: Option<Vec<String>>,
    all: Option<bool>,
    after: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SyncWatchersCollection {
    List(Vec<WatcherSpecBody>),
    Map(BTreeMap<String, WatcherSpecBody>),
}

#[derive(Debug, Deserialize)]
struct SyncDeploymentWatchersBody {
    board: Option<String>,
    ensure: Option<bool>,
    #[serde(alias = "webhook_url", alias = "webhookUrl", alias = "endpoint")]
    url: Option<String>,
    secret: Option<String>,
    watchers: SyncWatchersCollection,
}

fn apply_structured_filter(
    target: &mut WatcherFilter,
    input: StructuredFilterInput,
) -> BoardResult<()> {
    if input.after.is_some() {
        return Err(BoardError::bad_request(
            "'after' is not allowed in watcher filters",
        ));
    }
    if let Some(ch) = input.channel {
        target.channel = Some(ch);
    }
    if let Some(re) = input.re {
        target.re = Some(re);
    }
    if let Some(by) = input.by {
        target.by = Some(by);
    }
    if let Some(mentions) = input.mentions {
        target.mentions = Some(watchers::validate_watcher_mentions(&mentions)?);
    }
    if let Some(label_in) = input.label {
        target.labels.extend(label_in.into_predicates()?);
    }
    if let Some(from) = input.from {
        target.from = Some(from);
    }
    if let Some(text) = input.text {
        target.text = Some(text);
    }
    if let Some(types) = input.types {
        for t in types {
            let parsed = EventType::parse(&t)?;
            if !target.types.contains(&parsed) {
                target.types.push(parsed);
            }
        }
    }
    if let Some(all) = input.all {
        target.all = all;
    }
    Ok(())
}

fn build_upsert_input(
    spec: WatcherSpecBody,
    board_id: String,
    fallback_id: Option<String>,
    default_url: Option<&str>,
    default_secret: Option<&str>,
) -> BoardResult<UpsertWatcherInput> {
    if spec.after.is_some() {
        return Err(BoardError::bad_request(
            "'after' is not allowed in watcher filters",
        ));
    }

    let id = spec
        .id
        .or(fallback_id)
        .ok_or_else(|| BoardError::bad_request("watcher 'id' is required"))?;
    watchers::validate_watcher_id(&id)?;

    let webhook_url = spec
        .url
        .or_else(|| default_url.map(str::to_string))
        .ok_or_else(|| BoardError::bad_request("watcher 'url' is required"))?;
    watchers::validate_webhook_url(&webhook_url)?;

    let mut filter = match &spec.tail {
        Some(raw_tail) => watchers::parse_tail_expression(raw_tail)?,
        None => WatcherFilter::default(),
    };

    if let Some(nested) = spec.filter {
        apply_structured_filter(&mut filter, nested)?;
    }

    let top_level_structured = StructuredFilterInput {
        channel: spec.channel,
        re: spec.re,
        by: spec.by,
        mentions: spec.mentions,
        label: spec.label,
        from: spec.from,
        text: spec.text,
        types: spec.types,
        all: spec.all,
        after: None,
    };
    apply_structured_filter(&mut filter, top_level_structured)?;
    filter.validate()?;

    let tail_expr = spec
        .tail
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| filter.to_tail_expression());

    let secret = spec.secret.or_else(|| default_secret.map(str::to_string));

    Ok(UpsertWatcherInput {
        board_id,
        id,
        tail_expr,
        filter,
        webhook_url,
        secret,
    })
}

async fn register_watcher(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<BoardEnsureQuery>,
    Json(body): Json<WatcherSpecBody>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    watchers::require_watcher_registration_access(&caller)?;
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let input = build_upsert_input(body, resolved.id, None, None, None)?;
    let (watcher, created) = watchers::upsert_watcher(&state.db, &caller, input)?;
    state.watcher_wake.notify_one();

    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((
        status,
        Json(json!({ "watcher": watcher, "created": created })),
    ))
}

async fn list_watchers(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<BoardEnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let scope = watchers::resolve_watcher_access_scope(
        &state.db,
        &resolved.id,
        &caller,
        query.owner.as_deref(),
    )?;
    let list = watchers::list_watchers(&state.db, &resolved.id, &scope)?;
    Ok(Json(json!({ "watchers": list })))
}

async fn get_watcher(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<BoardEnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let scope = watchers::resolve_watcher_access_scope(
        &state.db,
        &resolved.id,
        &caller,
        query.owner.as_deref(),
    )?;
    let detail = watchers::get_watcher_detail(&state.db, &resolved.id, &id, &scope)?;
    Ok(Json(json!(detail)))
}

async fn delete_watcher(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<BoardEnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let scope = watchers::resolve_watcher_access_scope(
        &state.db,
        &resolved.id,
        &caller,
        query.owner.as_deref(),
    )?;
    watchers::delete_watcher(&state.db, &resolved.id, &id, &scope)?;
    Ok(Json(json!({ "ok": true, "deleted": id })))
}

async fn delete_board_watchers(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<BoardEnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let scope = watchers::resolve_watcher_access_scope(
        &state.db,
        &resolved.id,
        &caller,
        query.owner.as_deref(),
    )?;
    if let Some(id) = query.id {
        watchers::delete_watcher(&state.db, &resolved.id, &id, &scope)?;
        return Ok(Json(json!({ "ok": true, "deleted": 1, "id": id })));
    }
    let deleted = watchers::delete_board_watchers(&state.db, &resolved.id, &scope)?;
    Ok(Json(json!({ "ok": true, "deleted": deleted })))
}

async fn sync_deployment_watchers(
    State(state): State<AppState>,
    caller: CallerContext,
    Query(query): Query<DeploymentSyncQuery>,
    Json(body): Json<SyncDeploymentWatchersBody>,
) -> BoardResult<Json<Value>> {
    watchers::require_watcher_registration_access(&caller)?;

    let default_board = body
        .board
        .or(query.board)
        .unwrap_or_else(|| DEFAULT_BOARD_NAME.to_string());
    let ensure = body.ensure.or(query.ensure);
    let default_url = body.url.as_deref();
    let default_secret = body.secret.as_deref();

    let specs: Vec<(Option<String>, WatcherSpecBody)> = match body.watchers {
        SyncWatchersCollection::List(list) => list.into_iter().map(|s| (None, s)).collect(),
        SyncWatchersCollection::Map(map) => {
            map.into_iter().map(|(k, s)| (Some(k), s)).collect()
        }
    };

    let mut desired = Vec::with_capacity(specs.len());
    for (fallback_id, spec) in specs {
        let target_board = spec
            .board
            .clone()
            .unwrap_or_else(|| default_board.clone());
        let resolved = boards::resolve_board(&state.db, &target_board, ensure, &caller)?;
        let input = build_upsert_input(
            spec,
            resolved.id,
            fallback_id,
            default_url,
            default_secret,
        )?;
        desired.push(input);
    }

    let result = watchers::sync_deployment_watchers(&state.db, &caller, desired)?;
    state.watcher_wake.notify_one();

    Ok(Json(json!({
        "ok": true,
        "watchers": result.watchers,
        "created": result.created,
        "updated": result.updated,
        "deleted": result.deleted,
    })))
}
