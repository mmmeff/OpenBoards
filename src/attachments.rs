use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use axum::body::{Body, Bytes};
use axum::http::StatusCode;
use chrono::{Duration as ChronoDuration, Utc};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::actors::CallerContext;
use crate::db::{sql_placeholders, Database, DbTx, Dialect, Queryable, SqlArg};
use crate::error::{BoardError, BoardResult};
use crate::grants::{self, Grant};
use crate::id::new_attachment_id;
use crate::messages::AttachmentRef;
use crate::sql_args;

pub const MAX_ATTACHMENTS_PER_MESSAGE: usize = 10;
pub const MAX_INLINE_UPLOAD_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_ATTACHMENT_BYTES: u64 = 100 * 1024 * 1024;
pub const BLOB_CHUNK_BYTES: usize = 1024 * 1024;
const MAX_UPLOAD_PART_INDEX: u32 = 10_000;
const UPLOAD_PART_CHUNK_STRIDE: i64 = 1024;

/// Attachment blob backend: content-addressed local filesystem for SQLite,
/// 1 MiB chunked `BYTEA` storage in PostgreSQL for multi-replica deployments.
/// ponytail: storing large binaries in Postgres inflates DB backups at tens/hundreds of GB
/// (upgrade path: add a `BlobStore::S3` variant behind this enum without changing API handlers).
#[derive(Clone)]
pub enum BlobStore {
    LocalFs(PathBuf),
    PostgresSql(Database),
}

impl BlobStore {
    pub fn for_database(db: &Database, data_dir: PathBuf) -> Self {
        match db.dialect() {
            Dialect::Sqlite => Self::LocalFs(data_dir),
            Dialect::Postgres => Self::PostgresSql(db.clone()),
        }
    }

    pub async fn open_download_stream(
        &self,
        sha256: &str,
    ) -> BoardResult<tokio::sync::mpsc::Receiver<Result<Bytes, std::io::Error>>> {
        match self {
            Self::LocalFs(data_dir) => {
                let blob_path = content_blob_path(data_dir, sha256);
                let file = tokio::fs::File::open(&blob_path).await.map_err(|e| {
                    BoardError::internal(format!("failed to open attachment blob: {e}"))
                })?;

                // Stream file in 64 KiB chunks so large downloads never buffer in RAM
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(4);
                tokio::spawn(async move {
                    let mut reader = file;
                    let mut buf = vec![0u8; 64 * 1024];
                    loop {
                        match reader.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => {
                                if tx
                                    .send(Ok(Bytes::copy_from_slice(&buf[..n])))
                                    .await
                                    .is_err()
                                {
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
                Ok(rx)
            }
            Self::PostgresSql(db) => {
                let chunk_count = {
                    let mut conn = db.read_conn();
                    let row = conn
                        .fetch_optional(
                            "SELECT chunk_count FROM blob_objects WHERE sha256 = ?1",
                            sql_args![sha256],
                        )
                        .await?
                        .ok_or_else(|| {
                            BoardError::internal("attachment blob missing from Postgres blob store")
                        })?;
                    row.get_i64(0)?
                };

                let db_clone = db.clone();
                let sha = sha256.to_string();
                let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(2);
                tokio::spawn(async move {
                    let mut conn = db_clone.read_conn();
                    for chunk_idx in 0..chunk_count {
                        let res = conn
                            .fetch_optional(
                                "SELECT data FROM blob_chunks WHERE sha256 = ?1 AND chunk_idx = ?2",
                                sql_args![&sha, chunk_idx],
                            )
                            .await;
                        match res {
                            Ok(Some(row)) => match row.get_bytes(0) {
                                Ok(bytes) => {
                                    if tx.send(Ok(Bytes::from(bytes))).await.is_err() {
                                        break;
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(std::io::Error::other(e.to_string()))).await;
                                    break;
                                }
                            },
                            Ok(None) => {
                                let _ = tx
                                    .send(Err(std::io::Error::new(
                                        std::io::ErrorKind::UnexpectedEof,
                                        format!("missing blob chunk {chunk_idx}"),
                                    )))
                                    .await;
                                break;
                            }
                            Err(e) => {
                                let _ = tx.send(Err(std::io::Error::other(e.to_string()))).await;
                                break;
                            }
                        }
                    }
                });
                Ok(rx)
            }
        }
    }

    /// Removes abandoned staging uploads older than `older_than` (cascading to
    /// `blob_staging_chunks` on Postgres and deleting `.part` files on LocalFs).
    pub async fn cleanup_expired_staging(
        &self,
        db: &Database,
        older_than: ChronoDuration,
    ) -> BoardResult<u64> {
        let cutoff = (Utc::now() - older_than).to_rfc3339();
        let expired_ids: Vec<String> = {
            let mut conn = db.read_conn();
            let rows = conn
                .fetch_all(
                    "SELECT id FROM attachments
                     WHERE status IN ('pending', 'uploaded', 'staging') AND created_at < ?1
                     LIMIT 100",
                    sql_args![&cutoff],
                )
                .await?;
            let mut ids = Vec::with_capacity(rows.len());
            for r in rows {
                ids.push(r.get_str(0)?);
            }
            ids
        };

        if expired_ids.is_empty() {
            return Ok(0);
        }

        if let Self::LocalFs(data_dir) = self {
            for att_id in &expired_ids {
                remove_local_staging_files(data_dir, att_id).await;
            }
        }

        let mut tx = db.begin_tx().await?;
        let placeholders = sql_placeholders(expired_ids.len());
        let sql = format!(
            "DELETE FROM attachments WHERE status IN ('pending', 'uploaded', 'staging') AND id IN ({placeholders})"
        );
        let args: Vec<SqlArg<'_>> = expired_ids.iter().map(SqlArg::from).collect();
        let deleted = tx.execute(&sql, &args).await?;
        tx.commit().await?;
        Ok(deleted)
    }
}

#[derive(Debug, Clone)]
pub struct AttachmentRecord {
    pub id: String,
    pub board_id: String,
    pub name: String,
    pub content_type: String,
    pub size: u64,
    pub sha256: String,
    pub uploader_actor: String,
    pub status: String,
    pub created_at: String,
}

impl AttachmentRecord {
    pub fn to_ref(&self) -> AttachmentRef {
        AttachmentRef {
            id: self.id.clone(),
            name: self.name.clone(),
            content_type: self.content_type.clone(),
            size: self.size,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct InitUploadInput {
    pub name: String,
    #[serde(rename = "type")]
    pub content_type: Option<String>,
    pub size: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct InitUploadResult {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub content_type: String,
    pub upload_url: String,
    pub complete_url: String,
    pub max_size: u64,
}

#[derive(Debug, Serialize)]
pub struct UploadChunkResult {
    pub id: String,
    pub size: u64,
    pub sha256: String,
}

pub fn blobs_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("blobs")
}

pub fn temp_part_path(data_dir: &Path, att_id: &str) -> PathBuf {
    blobs_dir(data_dir).join("tmp").join(format!("{att_id}.part"))
}

pub fn temp_multipart_path(data_dir: &Path, att_id: &str, part: u32) -> PathBuf {
    blobs_dir(data_dir)
        .join("tmp")
        .join(format!("{att_id}.part.{part:06}"))
}

async fn remove_local_staging_files(data_dir: &Path, att_id: &str) {
    let _ = tokio::fs::remove_file(temp_part_path(data_dir, att_id)).await;
    let tmp_dir = blobs_dir(data_dir).join("tmp");
    let prefix = format!("{att_id}.part.");
    if let Ok(mut rd) = tokio::fs::read_dir(&tmp_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&prefix) {
                    let _ = tokio::fs::remove_file(entry.path()).await;
                }
            }
        }
    }
}

pub fn content_blob_path(data_dir: &Path, sha256: &str) -> PathBuf {
    let prefix = if sha256.len() >= 2 { &sha256[..2] } else { "00" };
    blobs_dir(data_dir).join(prefix).join(sha256)
}

pub fn validate_filename(name: &str) -> BoardResult<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() || trimmed.len() > 255 {
        return Err(BoardError::bad_request(
            "attachment filename must be 1 to 255 characters",
        ));
    }
    if trimmed.contains('/') || trimmed.contains('\\') || trimmed.chars().any(|c| c.is_control()) {
        return Err(BoardError::bad_request(
            "attachment filename cannot contain path separators or control characters",
        ));
    }
    Ok(trimmed.to_string())
}

fn validate_content_type(raw: Option<&str>) -> String {
    raw.map(str::trim)
        .filter(|s| !s.is_empty() && s.len() <= 255 && !s.chars().any(|c| c.is_control()))
        .unwrap_or("application/octet-stream")
        .to_string()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex
}

async fn require_can_upload(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
) -> BoardResult<()> {
    let board_grants = grants::load_grants(db, board_id).await?;
    let can_post_anywhere = grants::caller_permissions_at_pattern(&board_grants, caller, "*")
        .can_post_new_thread()
        || board_grants.iter().any(|g| {
            g.can.keys().any(|pat| {
                grants::caller_permissions_at_pattern(&board_grants, caller, pat)
                    .can_post_new_thread()
            })
        })
        || caller.narrowing.as_ref().is_some_and(|n| {
            n.rules.keys().any(|pat| {
                grants::caller_permissions_at_pattern(&board_grants, caller, pat)
                    .can_post_new_thread()
            })
        });
    if !can_post_anywhere {
        return Err(BoardError::forbidden(
            "uploading attachments requires 'post' permission in at least one channel",
        ));
    }
    Ok(())
}

/// Streams an Axum HTTP `Body` directly to `dest_path` while computing SHA-256 and enforcing `max_bytes`.
/// Never buffers the full payload in RAM.
async fn stream_body_to_file(
    mut body: Body,
    dest_path: &Path,
    max_bytes: u64,
    too_large_hint: Option<&str>,
) -> BoardResult<(u64, String)> {
    if let Some(parent) = dest_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            BoardError::internal(format!("failed to create blob staging directory: {e}"))
        })?;
    }

    let mut file = tokio::fs::File::create(dest_path)
        .await
        .map_err(|e| BoardError::internal(format!("failed to create staging file: {e}")))?;

    let mut hasher = Sha256::new();
    let mut total: u64 = 0;

    while let Some(frame_res) = body.frame().await {
        let frame = match frame_res {
            Ok(f) => f,
            Err(e) => {
                let _ = tokio::fs::remove_file(dest_path).await;
                return Err(BoardError::bad_request(format!(
                    "failed reading upload body: {e}"
                )));
            }
        };
        if let Some(data) = frame.data_ref() {
            total = total.saturating_add(data.len() as u64);
            if total > max_bytes {
                let _ = tokio::fs::remove_file(dest_path).await;
                let mut err = BoardError::bad_request(format!(
                    "attachment exceeds maximum size of {max_bytes} bytes"
                ))
                .with_status(StatusCode::PAYLOAD_TOO_LARGE);
                if let Some(hint) = too_large_hint {
                    err = err.with_hint(hint);
                }
                return Err(err);
            }
            hasher.update(data);
            if let Err(e) = file.write_all(data).await {
                let _ = tokio::fs::remove_file(dest_path).await;
                return Err(BoardError::internal(format!(
                    "failed writing attachment chunk: {e}"
                )));
            }
        }
    }

    if let Err(e) = file.flush().await {
        let _ = tokio::fs::remove_file(dest_path).await;
        return Err(BoardError::internal(format!(
            "failed flushing attachment file: {e}"
        )));
    }
    drop(file);

    let sha256 = hex_encode(&hasher.finalize());
    Ok((total, sha256))
}

/// Moves a staged file to its content-addressed blob path (deduplicating if already present).
async fn commit_staged_blob(data_dir: &Path, part_path: &Path, sha256: &str) -> BoardResult<()> {
    let final_path = content_blob_path(data_dir, sha256);
    if let Some(parent) = final_path.parent() {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            BoardError::internal(format!("failed to create blob directory: {e}"))
        })?;
    }
    if tokio::fs::try_exists(&final_path).await.unwrap_or(false) {
        let _ = tokio::fs::remove_file(part_path).await;
    } else {
        tokio::fs::rename(part_path, &final_path).await.map_err(|e| {
            BoardError::internal(format!("failed committing blob file: {e}"))
        })?;
    }
    Ok(())
}

/// Streams an Axum HTTP `Body` into 1 MiB chunks in memory (used only for <= 4 MiB inline uploads
/// on PostgreSQL) while computing SHA-256 and enforcing `max_bytes`.
async fn stream_body_to_chunks(
    mut body: Body,
    max_bytes: u64,
    too_large_hint: Option<&str>,
) -> BoardResult<(u64, String, Vec<Vec<u8>>)> {
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut current = Vec::with_capacity(BLOB_CHUNK_BYTES.min(64 * 1024));

    while let Some(frame_res) = body.frame().await {
        let frame = frame_res.map_err(|e| {
            BoardError::bad_request(format!("failed reading upload body: {e}"))
        })?;
        if let Some(data) = frame.data_ref() {
            total = total.saturating_add(data.len() as u64);
            if total > max_bytes {
                let mut err = BoardError::bad_request(format!(
                    "attachment exceeds maximum size of {max_bytes} bytes"
                ))
                .with_status(StatusCode::PAYLOAD_TOO_LARGE);
                if let Some(hint) = too_large_hint {
                    err = err.with_hint(hint);
                }
                return Err(err);
            }
            hasher.update(data);
            let mut slice = data.as_ref();
            while !slice.is_empty() {
                let space = BLOB_CHUNK_BYTES - current.len();
                let take = slice.len().min(space);
                current.extend_from_slice(&slice[..take]);
                slice = &slice[take..];
                if current.len() == BLOB_CHUNK_BYTES {
                    chunks.push(std::mem::take(&mut current));
                }
            }
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }

    let sha256 = hex_encode(&hasher.finalize());
    Ok((total, sha256, chunks))
}

/// Streams an Axum HTTP `Body` directly into `blob_staging_chunks` in 1 MiB rows so up to 100 MiB
/// uploads stay bounded to ~1 MiB peak RAM and never hold a long DB transaction across network I/O.
async fn stream_body_to_pg_staging(
    db: &Database,
    att_id: &str,
    part: Option<u32>,
    mut body: Body,
    max_bytes: u64,
) -> BoardResult<(u64, String)> {
    let base_idx = match part {
        None => 0i64,
        Some(p) => i64::from(p) * UPLOAD_PART_CHUNK_STRIDE,
    };

    // Clear any prior staged chunks for this attachment/part so retries are idempotent
    {
        let mut tx = db.begin_tx().await?;
        match part {
            None => {
                tx.execute(
                    "DELETE FROM blob_staging_chunks WHERE attachment_id = ?1",
                    sql_args![att_id],
                )
                .await?;
            }
            Some(_) => {
                tx.execute(
                    "DELETE FROM blob_staging_chunks
                     WHERE attachment_id = ?1 AND chunk_idx >= ?2 AND chunk_idx < ?3",
                    sql_args![att_id, base_idx, base_idx + UPLOAD_PART_CHUNK_STRIDE],
                )
                .await?;
            }
        }
        tx.commit().await?;
    }

    let cleanup = |db: Database, att_id: String, part: Option<u32>, base_idx: i64| async move {
        if let Ok(mut tx) = db.begin_tx().await {
            let _ = match part {
                None => {
                    tx.execute(
                        "DELETE FROM blob_staging_chunks WHERE attachment_id = ?1",
                        sql_args![&att_id],
                    )
                    .await
                }
                Some(_) => {
                    tx.execute(
                        "DELETE FROM blob_staging_chunks
                         WHERE attachment_id = ?1 AND chunk_idx >= ?2 AND chunk_idx < ?3",
                        sql_args![&att_id, base_idx, base_idx + UPLOAD_PART_CHUNK_STRIDE],
                    )
                    .await
                }
            };
            let _ = tx.commit().await;
        }
    };

    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    let mut sub_idx: i64 = 0;
    let mut buf = Vec::with_capacity(BLOB_CHUNK_BYTES);

    while let Some(frame_res) = body.frame().await {
        let frame = match frame_res {
            Ok(f) => f,
            Err(e) => {
                cleanup(db.clone(), att_id.to_string(), part, base_idx).await;
                return Err(BoardError::bad_request(format!(
                    "failed reading upload body: {e}"
                )));
            }
        };
        if let Some(data) = frame.data_ref() {
            total = total.saturating_add(data.len() as u64);
            if total > max_bytes {
                cleanup(db.clone(), att_id.to_string(), part, base_idx).await;
                return Err(BoardError::bad_request(format!(
                    "attachment exceeds maximum size of {max_bytes} bytes"
                ))
                .with_status(StatusCode::PAYLOAD_TOO_LARGE));
            }
            hasher.update(data);
            let mut slice = data.as_ref();
            while !slice.is_empty() {
                let space = BLOB_CHUNK_BYTES - buf.len();
                let take = slice.len().min(space);
                buf.extend_from_slice(&slice[..take]);
                slice = &slice[take..];
                if buf.len() == BLOB_CHUNK_BYTES {
                    let chunk_idx = base_idx + sub_idx;
                    if let Err(err) = insert_pg_staging_chunk(db, att_id, chunk_idx, &buf).await {
                        cleanup(db.clone(), att_id.to_string(), part, base_idx).await;
                        return Err(err);
                    }
                    buf.clear();
                    sub_idx += 1;
                }
            }
        }
    }

    if !buf.is_empty() {
        let chunk_idx = base_idx + sub_idx;
        if let Err(err) = insert_pg_staging_chunk(db, att_id, chunk_idx, &buf).await {
            cleanup(db.clone(), att_id.to_string(), part, base_idx).await;
            return Err(err);
        }
    }

    let sha256 = hex_encode(&hasher.finalize());
    Ok((total, sha256))
}

async fn insert_pg_staging_chunk(
    db: &Database,
    att_id: &str,
    chunk_idx: i64,
    data: &[u8],
) -> BoardResult<()> {
    let mut tx = db.begin_tx().await?;
    tx.execute(
        "INSERT INTO blob_staging_chunks (attachment_id, chunk_idx, data)
         VALUES (?1, ?2, ?3)
         ON CONFLICT (attachment_id, chunk_idx) DO UPDATE SET data = EXCLUDED.data",
        sql_args![att_id, chunk_idx, data],
    )
    .await?;
    tx.commit().await
}

/// Concatenates local multi-part staging files (`<att_id>.part.*`) into a single content-addressed blob.
async fn commit_local_multipart_blob(data_dir: &Path, att_id: &str) -> BoardResult<(u64, String)> {
    let tmp_dir = blobs_dir(data_dir).join("tmp");
    let prefix = format!("{att_id}.part.");
    let mut part_paths: Vec<PathBuf> = Vec::new();
    if let Ok(mut rd) = tokio::fs::read_dir(&tmp_dir).await {
        while let Ok(Some(entry)) = rd.next_entry().await {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&prefix) {
                    part_paths.push(entry.path());
                }
            }
        }
    }
    part_paths.sort();
    if part_paths.is_empty() {
        return Err(BoardError::bad_request(
            "upload bytes must be PUT before completing",
        ));
    }

    let combined = temp_part_path(data_dir, att_id);
    let mut out = tokio::fs::File::create(&combined)
        .await
        .map_err(|e| BoardError::internal(format!("failed creating combined staging file: {e}")))?;
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    let mut buf = vec![0u8; 64 * 1024];

    for p in &part_paths {
        let mut f = tokio::fs::File::open(p)
            .await
            .map_err(|e| BoardError::internal(format!("failed opening part file: {e}")))?;
        loop {
            let n = f
                .read(&mut buf)
                .await
                .map_err(|e| BoardError::internal(format!("failed reading part file: {e}")))?;
            if n == 0 {
                break;
            }
            total = total.saturating_add(n as u64);
            if total > MAX_ATTACHMENT_BYTES {
                drop(out);
                let _ = tokio::fs::remove_file(&combined).await;
                return Err(BoardError::bad_request(format!(
                    "attachment exceeds maximum size of {MAX_ATTACHMENT_BYTES} bytes"
                ))
                .with_status(StatusCode::PAYLOAD_TOO_LARGE));
            }
            hasher.update(&buf[..n]);
            out.write_all(&buf[..n])
                .await
                .map_err(|e| BoardError::internal(format!("failed writing combined blob: {e}")))?;
        }
    }
    out.flush()
        .await
        .map_err(|e| BoardError::internal(format!("failed flushing combined blob: {e}")))?;
    drop(out);

    let sha256 = hex_encode(&hasher.finalize());
    commit_staged_blob(data_dir, &combined, &sha256).await?;
    for p in &part_paths {
        let _ = tokio::fs::remove_file(p).await;
    }
    Ok((total, sha256))
}

/// Commits multi-part `blob_staging_chunks` into contiguous 1 MiB `blob_chunks` inside `tx`.
/// Uses two-pass streaming over staging chunks so peak RAM stays ~1 MiB even for 100 MiB files.
async fn commit_pg_multipart_blob_tx(
    tx: &mut DbTx,
    att_id: &str,
    now: &str,
) -> BoardResult<(u64, String)> {
    let idx_rows = tx
        .fetch_all(
            "SELECT chunk_idx FROM blob_staging_chunks
             WHERE attachment_id = ?1
             ORDER BY chunk_idx ASC",
            sql_args![att_id],
        )
        .await?;
    if idx_rows.is_empty() {
        return Err(BoardError::bad_request(
            "upload bytes must be PUT before completing",
        ));
    }
    let mut indices = Vec::with_capacity(idx_rows.len());
    for r in idx_rows {
        indices.push(r.get_i64(0)?);
    }

    // Pass 1: compute total size and exact SHA-256 one chunk at a time
    let mut hasher = Sha256::new();
    let mut total: u64 = 0;
    for &idx in &indices {
        let row = tx
            .fetch_one(
                "SELECT data FROM blob_staging_chunks WHERE attachment_id = ?1 AND chunk_idx = ?2",
                sql_args![att_id, idx],
            )
            .await?;
        let bytes = row.get_bytes(0)?;
        total = total.saturating_add(bytes.len() as u64);
        if total > MAX_ATTACHMENT_BYTES {
            return Err(BoardError::bad_request(format!(
                "attachment exceeds maximum size of {MAX_ATTACHMENT_BYTES} bytes"
            ))
            .with_status(StatusCode::PAYLOAD_TOO_LARGE));
        }
        hasher.update(&bytes);
    }

    let sha256 = hex_encode(&hasher.finalize());
    let chunk_count = if total == 0 {
        0i64
    } else {
        total.div_ceil(BLOB_CHUNK_BYTES as u64) as i64
    };

    let inserted_obj = tx
        .execute(
            "INSERT INTO blob_objects (sha256, size, chunk_count, created_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (sha256) DO NOTHING",
            sql_args![&sha256, total as i64, chunk_count, now],
        )
        .await?;

    // Pass 2: if not already deduplicated, normalize into contiguous 1 MiB blob_chunks
    if inserted_obj > 0 && total > 0 {
        let mut out_idx: i64 = 0;
        let mut buf = Vec::with_capacity(BLOB_CHUNK_BYTES);
        for &idx in &indices {
            let row = tx
                .fetch_one(
                    "SELECT data FROM blob_staging_chunks WHERE attachment_id = ?1 AND chunk_idx = ?2",
                    sql_args![att_id, idx],
                )
                .await?;
            let bytes = row.get_bytes(0)?;
            let mut slice = bytes.as_slice();
            while !slice.is_empty() {
                let space = BLOB_CHUNK_BYTES - buf.len();
                let take = slice.len().min(space);
                buf.extend_from_slice(&slice[..take]);
                slice = &slice[take..];
                if buf.len() == BLOB_CHUNK_BYTES {
                    tx.execute(
                        "INSERT INTO blob_chunks (sha256, chunk_idx, data)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT (sha256, chunk_idx) DO NOTHING",
                        sql_args![&sha256, out_idx, buf.as_slice()],
                    )
                    .await?;
                    buf.clear();
                    out_idx += 1;
                }
            }
        }
        if !buf.is_empty() {
            tx.execute(
                "INSERT INTO blob_chunks (sha256, chunk_idx, data)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (sha256, chunk_idx) DO NOTHING",
                sql_args![&sha256, out_idx, buf.as_slice()],
            )
            .await?;
        }
    }

    tx.execute(
        "DELETE FROM blob_staging_chunks WHERE attachment_id = ?1",
        sql_args![att_id],
    )
    .await?;

    Ok((total, sha256))
}

/// Inline single-request upload (<= 4 MB).
pub async fn upload_inline(
    db: &Database,
    blobs: &BlobStore,
    board_id: &str,
    caller: &CallerContext,
    filename: &str,
    content_type: Option<&str>,
    body: Body,
) -> BoardResult<AttachmentRef> {
    let name = validate_filename(filename)?;
    let content_type = validate_content_type(content_type);
    require_can_upload(db, board_id, caller).await?;

    let att_id = new_attachment_id();
    let now = Utc::now().to_rfc3339();
    let uploader = caller.actor_id();
    let too_large_hint = Some(
        "Use the two-step streaming upload endpoint (/attachments/uploads) for files up to 100 MB.",
    );

    let size = match blobs {
        BlobStore::LocalFs(data_dir) => {
            let part = temp_part_path(data_dir, &att_id);
            let (size, sha256) =
                stream_body_to_file(body, &part, MAX_INLINE_UPLOAD_BYTES, too_large_hint).await?;
            commit_staged_blob(data_dir, &part, &sha256).await?;

            let mut tx = db.begin_tx().await?;
            tx.execute(
                "INSERT INTO attachments (id, board_id, name, content_type, size, sha256, uploader_actor, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'ready', ?8)",
                sql_args![
                    &att_id,
                    board_id,
                    &name,
                    &content_type,
                    size as i64,
                    &sha256,
                    &uploader,
                    &now
                ],
            )
            .await?;
            tx.commit().await?;
            size
        }
        BlobStore::PostgresSql(_) => {
            let (size, sha256, chunks) =
                stream_body_to_chunks(body, MAX_INLINE_UPLOAD_BYTES, too_large_hint).await?;

            let mut tx = db.begin_tx().await?;
            let inserted_obj = tx
                .execute(
                    "INSERT INTO blob_objects (sha256, size, chunk_count, created_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (sha256) DO NOTHING",
                    sql_args![&sha256, size as i64, chunks.len() as i64, &now],
                )
                .await?;
            if inserted_obj > 0 {
                for (idx, chunk) in chunks.iter().enumerate() {
                    tx.execute(
                        "INSERT INTO blob_chunks (sha256, chunk_idx, data)
                         VALUES (?1, ?2, ?3)
                         ON CONFLICT (sha256, chunk_idx) DO NOTHING",
                        sql_args![&sha256, idx as i64, chunk.as_slice()],
                    )
                    .await?;
                }
            }
            tx.execute(
                "INSERT INTO attachments (id, board_id, name, content_type, size, sha256, uploader_actor, status, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'ready', ?8)",
                sql_args![
                    &att_id,
                    board_id,
                    &name,
                    &content_type,
                    size as i64,
                    &sha256,
                    &uploader,
                    &now
                ],
            )
            .await?;
            tx.commit().await?;
            size
        }
    };

    Ok(AttachmentRef {
        id: att_id,
        name,
        content_type,
        size,
    })
}

/// Step 1 of streaming two-step upload (<= 100 MB): reserves an attachment ID.
pub async fn init_streaming_upload(
    db: &Database,
    board_id: &str,
    board_route_segment: &str,
    caller: &CallerContext,
    input: InitUploadInput,
) -> BoardResult<InitUploadResult> {
    let name = validate_filename(&input.name)?;
    let content_type = validate_content_type(input.content_type.as_deref());
    if let Some(sz) = input.size {
        if sz > MAX_ATTACHMENT_BYTES {
            return Err(BoardError::bad_request(format!(
                "attachment exceeds maximum size of {MAX_ATTACHMENT_BYTES} bytes"
            ))
            .with_status(StatusCode::PAYLOAD_TOO_LARGE));
        }
    }
    require_can_upload(db, board_id, caller).await?;

    let att_id = new_attachment_id();
    let now = Utc::now().to_rfc3339();
    let uploader = caller.actor_id();

    let mut tx = db.begin_tx().await?;
    tx.execute(
        "INSERT INTO attachments (id, board_id, name, content_type, size, sha256, uploader_actor, status, created_at)
         VALUES (?1, ?2, ?3, ?4, 0, '', ?5, 'pending', ?6)",
        sql_args![&att_id, board_id, &name, &content_type, &uploader, &now],
    )
    .await?;
    tx.commit().await?;

    let upload_url =
        format!("/v1/messageboard/boards/{board_route_segment}/attachments/uploads/{att_id}");
    let complete_url = format!("{upload_url}/complete");

    Ok(InitUploadResult {
        id: att_id,
        name,
        content_type,
        upload_url,
        complete_url,
        max_size: MAX_ATTACHMENT_BYTES,
    })
}

/// Step 2 of streaming upload: streams bytes directly to staging storage (<= 100 MB).
/// Supports both single-PUT (`part = None`) and multi-part (`part = Some(idx)`) uploads
/// across arbitrary replicas.
pub async fn put_streaming_upload(
    db: &Database,
    blobs: &BlobStore,
    board_id: &str,
    att_id: &str,
    part: Option<u32>,
    caller: &CallerContext,
    body: Body,
) -> BoardResult<UploadChunkResult> {
    if let Some(p) = part {
        if p > MAX_UPLOAD_PART_INDEX {
            return Err(BoardError::bad_request(format!(
                "upload part index must be <= {MAX_UPLOAD_PART_INDEX}"
            )));
        }
    }

    let rec = {
        let mut conn = db.read_conn();
        load_attachment_tx(&mut conn, board_id, att_id).await?
    }
    .ok_or_else(|| BoardError::not_found(format!("upload '{att_id}' not found")))?;

    if !caller.owns_actor_item(&rec.uploader_actor) {
        return Err(BoardError::forbidden(
            "only the upload initiator can upload bytes for this attachment",
        ));
    }
    if rec.status == "ready" {
        return Err(BoardError::conflict("upload has already been completed"));
    }

    let (size, sha256) = match blobs {
        BlobStore::LocalFs(data_dir) => {
            let dest = match part {
                None => temp_part_path(data_dir, att_id),
                Some(p) => temp_multipart_path(data_dir, att_id, p),
            };
            stream_body_to_file(body, &dest, MAX_ATTACHMENT_BYTES, None).await?
        }
        BlobStore::PostgresSql(pg_db) => {
            stream_body_to_pg_staging(pg_db, att_id, part, body, MAX_ATTACHMENT_BYTES).await?
        }
    };

    let mut tx = db.begin_tx().await?;
    match part {
        None => {
            tx.execute(
                "UPDATE attachments SET size = ?1, sha256 = ?2, status = 'uploaded'
                 WHERE board_id = ?3 AND id = ?4 AND status != 'ready'",
                sql_args![size as i64, &sha256, board_id, att_id],
            )
            .await?;
        }
        Some(_) => {
            tx.execute(
                "UPDATE attachments SET status = 'staging'
                 WHERE board_id = ?1 AND id = ?2 AND status != 'ready'",
                sql_args![board_id, att_id],
            )
            .await?;
        }
    }
    tx.commit().await?;

    Ok(UploadChunkResult {
        id: att_id.to_string(),
        size,
        sha256,
    })
}

/// Step 3 of streaming upload: commits staged bytes into content-addressed storage and marks ready.
/// Can execute on a different replica from `init` and `put`.
pub async fn complete_streaming_upload(
    db: &Database,
    blobs: &BlobStore,
    board_id: &str,
    att_id: &str,
    caller: &CallerContext,
) -> BoardResult<AttachmentRef> {
    let mut tx = db.begin_tx().await?;
    let rec = load_attachment_for_update_tx(&mut tx, board_id, att_id)
        .await?
        .ok_or_else(|| BoardError::not_found(format!("upload '{att_id}' not found")))?;

    if !caller.owns_actor_item(&rec.uploader_actor) {
        return Err(BoardError::forbidden(
            "only the upload initiator can complete this attachment",
        ));
    }
    if rec.status == "ready" {
        tx.commit().await?;
        return Ok(rec.to_ref());
    }

    let now = Utc::now().to_rfc3339();
    let (final_size, final_sha) = if rec.status == "uploaded" && !rec.sha256.is_empty() {
        match blobs {
            BlobStore::LocalFs(data_dir) => {
                let part = temp_part_path(data_dir, att_id);
                commit_staged_blob(data_dir, &part, &rec.sha256).await?;
                (rec.size, rec.sha256.clone())
            }
            BlobStore::PostgresSql(_) => {
                let chunk_count = tx
                    .fetch_one(
                        "SELECT COUNT(*) FROM blob_staging_chunks WHERE attachment_id = ?1",
                        sql_args![att_id],
                    )
                    .await?
                    .get_i64(0)?;
                tx.execute(
                    "INSERT INTO blob_objects (sha256, size, chunk_count, created_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (sha256) DO NOTHING",
                    sql_args![&rec.sha256, rec.size as i64, chunk_count, &now],
                )
                .await?;
                tx.execute(
                    "INSERT INTO blob_chunks (sha256, chunk_idx, data)
                     SELECT ?1, chunk_idx, data
                     FROM blob_staging_chunks
                     WHERE attachment_id = ?2
                     ORDER BY chunk_idx ASC
                     ON CONFLICT (sha256, chunk_idx) DO NOTHING",
                    sql_args![&rec.sha256, att_id],
                )
                .await?;
                tx.execute(
                    "DELETE FROM blob_staging_chunks WHERE attachment_id = ?1",
                    sql_args![att_id],
                )
                .await?;
                (rec.size, rec.sha256.clone())
            }
        }
    } else if rec.status == "staging" {
        match blobs {
            BlobStore::LocalFs(data_dir) => commit_local_multipart_blob(data_dir, att_id).await?,
            BlobStore::PostgresSql(_) => commit_pg_multipart_blob_tx(&mut tx, att_id, &now).await?,
        }
    } else {
        return Err(BoardError::bad_request(
            "upload bytes must be PUT before completing",
        ));
    };

    tx.execute(
        "UPDATE attachments SET size = ?1, sha256 = ?2, status = 'ready'
         WHERE board_id = ?3 AND id = ?4",
        sql_args![final_size as i64, &final_sha, board_id, att_id],
    )
    .await?;
    tx.commit().await?;

    Ok(AttachmentRef {
        id: rec.id,
        name: rec.name,
        content_type: rec.content_type,
        size: final_size,
    })
}

async fn load_attachment_for_update_tx(
    tx: &mut DbTx,
    board_id: &str,
    att_id: &str,
) -> BoardResult<Option<AttachmentRecord>> {
    let sql = format!(
        "SELECT id, board_id, name, content_type, size, sha256, uploader_actor, status, created_at
         FROM attachments
         WHERE board_id = ?1 AND id = ?2{}",
        tx.lock_row_suffix()
    );
    let row = tx.fetch_optional(&sql, sql_args![board_id, att_id]).await?;
    match row {
        None => Ok(None),
        Some(r) => Ok(Some(AttachmentRecord {
            id: r.get_str(0)?,
            board_id: r.get_str(1)?,
            name: r.get_str(2)?,
            content_type: r.get_str(3)?,
            size: r.get_i64(4)? as u64,
            sha256: r.get_str(5)?,
            uploader_actor: r.get_str(6)?,
            status: r.get_str(7)?,
            created_at: r.get_str(8)?,
        })),
    }
}

pub async fn load_attachment_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    att_id: &str,
) -> BoardResult<Option<AttachmentRecord>> {
    let row = conn
        .fetch_optional(
            "SELECT id, board_id, name, content_type, size, sha256, uploader_actor, status, created_at
             FROM attachments
             WHERE board_id = ?1 AND id = ?2",
            sql_args![board_id, att_id],
        )
        .await?;
    match row {
        None => Ok(None),
        Some(r) => Ok(Some(AttachmentRecord {
            id: r.get_str(0)?,
            board_id: r.get_str(1)?,
            name: r.get_str(2)?,
            content_type: r.get_str(3)?,
            size: r.get_i64(4)? as u64,
            sha256: r.get_str(5)?,
            uploader_actor: r.get_str(6)?,
            status: r.get_str(7)?,
            created_at: r.get_str(8)?,
        })),
    }
}

/// Checks whether `caller` is permitted to read/download or attach `att` on `board_id`:
/// - Uploader (`caller.owns_actor_item(&att.uploader_actor)`), OR
/// - Caller can read every channel on the board (`*`-read), OR
/// - At least one NON-deleted message references `att.id` in a channel the caller can `read`.
///
/// On tombstoned messages (where no live message references `att.id`), only uploader or `*`-read
/// callers can access it.
pub async fn caller_can_access_attachment_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    board_grants: &[Grant],
    caller: &CallerContext,
    att: &AttachmentRecord,
) -> BoardResult<bool> {
    if caller.owns_actor_item(&att.uploader_actor) {
        return Ok(true);
    }
    if grants::caller_can_read_all_channels(board_grants, caller) {
        return Ok(true);
    }

    let rows = conn
        .fetch_all(
            "SELECT m.channel
             FROM message_attachments ma
             JOIN messages m ON m.id = ma.message_id
             WHERE ma.board_id = ?1 AND ma.attachment_id = ?2 AND m.deleted_at IS NULL",
            sql_args![board_id, &att.id],
        )
        .await?;
    for r in rows {
        let ch = r.get_str(0)?;
        if grants::caller_permissions_on_channel(board_grants, caller, &ch).can_read() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Validates and links attachments to a message inside a write transaction.
pub async fn validate_and_link_attachments_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    board_grants: &[Grant],
    caller: &CallerContext,
    message_id: &str,
    attachment_ids: &[String],
) -> BoardResult<()> {
    if attachment_ids.len() > MAX_ATTACHMENTS_PER_MESSAGE {
        return Err(BoardError::bad_request(format!(
            "a message can carry at most {MAX_ATTACHMENTS_PER_MESSAGE} attachments"
        )));
    }
    let mut seen: Vec<&str> = Vec::with_capacity(attachment_ids.len());
    for (idx, att_id) in attachment_ids.iter().enumerate() {
        if seen.contains(&att_id.as_str()) {
            continue;
        }
        seen.push(att_id.as_str());

        let att = load_attachment_tx(conn, board_id, att_id)
            .await?
            .ok_or_else(|| BoardError::not_found(format!("attachment '{att_id}' not found")))?;
        if att.status != "ready" {
            return Err(BoardError::bad_request(format!(
                "attachment '{att_id}' is not ready"
            )));
        }
        if !caller_can_access_attachment_tx(conn, board_id, board_grants, caller, &att).await? {
            return Err(BoardError::forbidden(format!(
                "caller cannot access attachment '{att_id}'"
            )));
        }
        conn.execute(
            "INSERT INTO message_attachments (board_id, message_id, attachment_id, idx)
             VALUES (?1, ?2, ?3, ?4)",
            sql_args![board_id, message_id, att_id, idx as i64],
        )
        .await?;
    }
    Ok(())
}

pub async fn load_message_attachments_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_id: &str,
) -> BoardResult<Vec<AttachmentRef>> {
    let rows = conn
        .fetch_all(
            "SELECT a.id, a.name, a.content_type, a.size
             FROM message_attachments ma
             JOIN attachments a ON a.id = ma.attachment_id
             WHERE ma.message_id = ?1
             ORDER BY ma.idx ASC",
            sql_args![message_id],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(AttachmentRef {
            id: r.get_str(0)?,
            name: r.get_str(1)?,
            content_type: r.get_str(2)?,
            size: r.get_i64(3)? as u64,
        });
    }
    Ok(out)
}

pub async fn load_messages_attachments_batch_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_ids: &[&str],
) -> BoardResult<HashMap<String, Vec<AttachmentRef>>> {
    let mut out: HashMap<String, Vec<AttachmentRef>> = HashMap::new();
    if message_ids.is_empty() {
        return Ok(out);
    }
    for chunk in message_ids.chunks(400) {
        let sql = format!(
            "SELECT ma.message_id, a.id, a.name, a.content_type, a.size
             FROM message_attachments ma
             JOIN attachments a ON a.id = ma.attachment_id
             WHERE ma.message_id IN ({})
             ORDER BY ma.message_id ASC, ma.idx ASC",
            sql_placeholders(chunk.len())
        );
        let args: Vec<SqlArg<'_>> = chunk.iter().copied().map(SqlArg::from).collect();
        let rows = conn.fetch_all(&sql, &args).await?;
        for r in rows {
            let mid = r.get_str(0)?;
            let att = AttachmentRef {
                id: r.get_str(1)?,
                name: r.get_str(2)?,
                content_type: r.get_str(3)?,
                size: r.get_i64(4)? as u64,
            };
            out.entry(mid).or_default().push(att);
        }
    }
    Ok(out)
}

/// Resolves an attachment for download, enforcing visibility and tombstone rules.
pub async fn authorize_download(
    db: &Database,
    board_id: &str,
    att_id: &str,
    caller: &CallerContext,
) -> BoardResult<AttachmentRecord> {
    let mut conn = db.read_conn();
    let att = load_attachment_tx(&mut conn, board_id, att_id)
        .await?
        .ok_or_else(|| BoardError::not_found(format!("attachment '{att_id}' not found")))?;
    if att.status != "ready" {
        return Err(BoardError::not_found(format!(
            "attachment '{att_id}' not found"
        )));
    }
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    if !caller_can_access_attachment_tx(&mut conn, board_id, &board_grants, caller, &att).await? {
        return Err(BoardError::forbidden(format!(
            "not permitted to download attachment '{att_id}'"
        )));
    }
    Ok(att)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_attachment_filenames() {
        assert_eq!(validate_filename("trace.txt").unwrap(), "trace.txt");
        assert!(validate_filename("").is_err());
        assert!(validate_filename("../etc/passwd").is_err());
        assert!(validate_filename("a\\b.txt").is_err());
        assert!(validate_filename("bad\nname").is_err());
    }
}
