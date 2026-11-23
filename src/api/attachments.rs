use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::api::boards::EnsureQuery;
use crate::attachments::{self, InitUploadInput};
use crate::boards;
use crate::error::{BoardError, BoardResult};
use crate::AppState;

#[derive(Debug, Default, Deserialize)]
struct InlineUploadQuery {
    ensure: Option<bool>,
    name: Option<String>,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/messageboard/boards/{board}/attachments",
            post(upload_inline).layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/v1/messageboard/boards/{board}/attachments/uploads",
            post(init_upload),
        )
        .route(
            "/v1/messageboard/boards/{board}/attachments/uploads/{id}",
            put(put_upload).layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/v1/messageboard/boards/{board}/attachments/uploads/{id}/complete",
            post(complete_upload),
        )
        .route(
            "/v1/messageboard/boards/{board}/attachments/{id}",
            get(download_attachment),
        )
}

async fn upload_inline(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<InlineUploadQuery>,
    headers: HeaderMap,
    body: Body,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let filename = query
        .name
        .or_else(|| {
            headers
                .get("x-filename")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        })
        .ok_or_else(|| {
            BoardError::bad_request("missing attachment filename (pass ?name=... or X-Filename header)")
        })?;
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());

    let att = attachments::upload_inline(
        &state.db,
        &state.config.data_dir,
        &resolved.id,
        &caller,
        &filename,
        content_type,
        body,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(json!(att))))
}

async fn init_upload(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(board): Path<String>,
    Query(query): Query<EnsureQuery>,
    Json(input): Json<InitUploadInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let res = attachments::init_streaming_upload(&state.db, &resolved.id, &board, &caller, input)?;
    Ok((StatusCode::CREATED, Json(json!(res))))
}

async fn put_upload(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
    body: Body,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let res = attachments::put_streaming_upload(
        &state.db,
        &state.config.data_dir,
        &resolved.id,
        &id,
        &caller,
        body,
    )
    .await?;
    Ok(Json(json!(res)))
}

async fn complete_upload(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Json<Value>> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let att = attachments::complete_streaming_upload(
        &state.db,
        &state.config.data_dir,
        &resolved.id,
        &id,
        &caller,
    )
    .await?;
    Ok(Json(json!({ "attachment": att })))
}

async fn download_attachment(
    State(state): State<AppState>,
    caller: CallerContext,
    Path((board, id)): Path<(String, String)>,
    Query(query): Query<EnsureQuery>,
) -> BoardResult<Response> {
    let resolved = boards::resolve_board(&state.db, &board, query.ensure, &caller)?;
    let att = attachments::authorize_download(&state.db, &resolved.id, &id, &caller)?;
    let blob_path = attachments::content_blob_path(&state.config.data_dir, &att.sha256);

    let file = tokio::fs::File::open(&blob_path)
        .await
        .map_err(|e| BoardError::internal(format!("failed to open attachment blob: {e}")))?;

    // Stream file in 64 KiB chunks so large downloads never buffer in RAM
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<axum::body::Bytes, std::io::Error>>(4);
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut reader = file;
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if tx.send(Ok(axum::body::Bytes::copy_from_slice(&buf[..n]))).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    break;
                }
            }
        }
    });

    let stream = ReceiverStream(rx);
    let mut resp = Body::from_stream(stream).into_response();
    if let Ok(ct) = HeaderValue::from_str(&att.content_type) {
        resp.headers_mut().insert(CONTENT_TYPE, ct);
    }
    let disp = format!("attachment; filename=\"{}\"", att.name.replace('"', "_"));
    if let Ok(cd) = HeaderValue::from_str(&disp) {
        resp.headers_mut().insert(CONTENT_DISPOSITION, cd);
    }
    Ok(resp)
}

struct ReceiverStream<T>(tokio::sync::mpsc::Receiver<T>);

impl<T> futures_core::Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}
