use std::collections::HashMap;
use std::path::{Path, PathBuf};

use axum::body::Body;
use axum::http::StatusCode;
use chrono::Utc;
use http_body_util::BodyExt;
use rusqlite::{params, params_from_iter, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::actors::CallerContext;
use crate::db::{sql_placeholders, Database};
use crate::error::{BoardError, BoardResult};
use crate::grants::{self, Grant};
use crate::id::new_attachment_id;
use crate::messages::AttachmentRef;

pub const MAX_ATTACHMENTS_PER_MESSAGE: usize = 10;
pub const MAX_INLINE_UPLOAD_BYTES: u64 = 4 * 1024 * 1024;
pub const MAX_ATTACHMENT_BYTES: u64 = 100 * 1024 * 1024;

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
        use std::fmt::Write;
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex
}

fn require_can_upload(db: &Database, board_id: &str, caller: &CallerContext) -> BoardResult<()> {
    let board_grants = grants::load_grants(db, board_id)?;
    // Must have `post` somewhere on the board (checking grant patterns and any narrowing patterns)
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

/// Inline single-request upload (<= 4 MB).
pub async fn upload_inline(
    db: &Database,
    data_dir: &Path,
    board_id: &str,
    caller: &CallerContext,
    filename: &str,
    content_type: Option<&str>,
    body: Body,
) -> BoardResult<AttachmentRef> {
    let name = validate_filename(filename)?;
    let content_type = validate_content_type(content_type);
    require_can_upload(db, board_id, caller)?;

    let att_id = new_attachment_id();
    let part = temp_part_path(data_dir, &att_id);
    let (size, sha256) = stream_body_to_file(
        body,
        &part,
        MAX_INLINE_UPLOAD_BYTES,
        Some("Use the two-step streaming upload endpoint (/attachments/uploads) for files up to 100 MB."),
    )
    .await?;

    commit_staged_blob(data_dir, &part, &sha256).await?;

    let now = Utc::now().to_rfc3339();
    let uploader = caller.actor_id();
    db.with_tx(|tx| {
        tx.execute(
            "INSERT INTO attachments (id, board_id, name, content_type, size, sha256, uploader_actor, status, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'ready', ?8)",
            params![att_id, board_id, name, content_type, size as i64, sha256, uploader, now],
        )?;
        Ok(())
    })?;

    Ok(AttachmentRef {
        id: att_id,
        name,
        content_type,
        size,
    })
}

/// Step 1 of streaming two-step upload (<= 100 MB): reserves an attachment ID.
pub fn init_streaming_upload(
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
    require_can_upload(db, board_id, caller)?;

    let att_id = new_attachment_id();
    let now = Utc::now().to_rfc3339();
    let uploader = caller.actor_id();

    db.with_tx(|tx| {
        tx.execute(
            "INSERT INTO attachments (id, board_id, name, content_type, size, sha256, uploader_actor, status, created_at)
             VALUES (?1, ?2, ?3, ?4, 0, '', ?5, 'pending', ?6)",
            params![att_id, board_id, name, content_type, uploader, now],
        )?;
        Ok(())
    })?;

    let upload_url = format!("/v1/messageboard/boards/{board_route_segment}/attachments/uploads/{att_id}");
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

/// Step 2 of streaming upload: streams bytes directly to staging disk file (<= 100 MB).
pub async fn put_streaming_upload(
    db: &Database,
    data_dir: &Path,
    board_id: &str,
    att_id: &str,
    caller: &CallerContext,
    body: Body,
) -> BoardResult<UploadChunkResult> {
    let rec = db.with_conn(|conn| load_attachment_tx(conn, board_id, att_id))?
        .ok_or_else(|| BoardError::not_found(format!("upload '{att_id}' not found")))?;

    if !caller.owns_actor_item(&rec.uploader_actor) {
        return Err(BoardError::forbidden(
            "only the upload initiator can upload bytes for this attachment",
        ));
    }
    if rec.status == "ready" {
        return Err(BoardError::conflict("upload has already been completed"));
    }

    let part = temp_part_path(data_dir, att_id);
    let (size, sha256) = stream_body_to_file(body, &part, MAX_ATTACHMENT_BYTES, None).await?;

    db.with_tx(|tx| {
        tx.execute(
            "UPDATE attachments SET size = ?1, sha256 = ?2, status = 'uploaded'
             WHERE board_id = ?3 AND id = ?4",
            params![size as i64, sha256, board_id, att_id],
        )?;
        Ok(())
    })?;

    Ok(UploadChunkResult {
        id: att_id.to_string(),
        size,
        sha256,
    })
}

/// Step 3 of streaming upload: commits staged file into content-addressed storage and marks ready.
pub async fn complete_streaming_upload(
    db: &Database,
    data_dir: &Path,
    board_id: &str,
    att_id: &str,
    caller: &CallerContext,
) -> BoardResult<AttachmentRef> {
    let rec = db.with_conn(|conn| load_attachment_tx(conn, board_id, att_id))?
        .ok_or_else(|| BoardError::not_found(format!("upload '{att_id}' not found")))?;

    if !caller.owns_actor_item(&rec.uploader_actor) {
        return Err(BoardError::forbidden(
            "only the upload initiator can complete this attachment",
        ));
    }
    if rec.status == "ready" {
        return Ok(rec.to_ref());
    }
    if rec.status != "uploaded" || rec.sha256.is_empty() {
        return Err(BoardError::bad_request(
            "upload bytes must be PUT before completing",
        ));
    }

    let part = temp_part_path(data_dir, att_id);
    commit_staged_blob(data_dir, &part, &rec.sha256).await?;

    db.with_tx(|tx| {
        tx.execute(
            "UPDATE attachments SET status = 'ready' WHERE board_id = ?1 AND id = ?2",
            params![board_id, att_id],
        )?;
        Ok(())
    })?;

    Ok(rec.to_ref())
}

pub fn load_attachment_tx(
    conn: &Connection,
    board_id: &str,
    att_id: &str,
) -> BoardResult<Option<AttachmentRecord>> {
    conn.query_row(
        "SELECT id, board_id, name, content_type, size, sha256, uploader_actor, status, created_at
         FROM attachments
         WHERE board_id = ?1 AND id = ?2",
        params![board_id, att_id],
        |r| {
            Ok(AttachmentRecord {
                id: r.get(0)?,
                board_id: r.get(1)?,
                name: r.get(2)?,
                content_type: r.get(3)?,
                size: r.get::<_, i64>(4)? as u64,
                sha256: r.get(5)?,
                uploader_actor: r.get(6)?,
                status: r.get(7)?,
                created_at: r.get(8)?,
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

/// Checks whether `caller` is permitted to read/download or attach `att` on `board_id`:
/// - Uploader (`caller.owns_actor_item(&att.uploader_actor)`), OR
/// - Caller can read every channel on the board (`*`-read), OR
/// - At least one NON-deleted message references `att.id` in a channel the caller can `read`.
///
/// On tombstoned messages (where no live message references `att.id`), only uploader or `*`-read
/// callers can access it.
pub fn caller_can_access_attachment_tx(
    conn: &Connection,
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

    let mut stmt = conn.prepare(
        "SELECT m.channel
         FROM message_attachments ma
         JOIN messages m ON m.id = ma.message_id
         WHERE ma.board_id = ?1 AND ma.attachment_id = ?2 AND m.deleted_at IS NULL",
    )?;
    let channels = stmt.query_map(params![board_id, att.id], |r| r.get::<_, String>(0))?;
    for ch_res in channels {
        let ch = ch_res?;
        if grants::caller_permissions_on_channel(board_grants, caller, &ch).can_read() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Validates and links attachments to a message inside a write transaction.
pub fn validate_and_link_attachments_tx(
    conn: &Connection,
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

        let att = load_attachment_tx(conn, board_id, att_id)?.ok_or_else(|| {
            BoardError::not_found(format!("attachment '{att_id}' not found"))
        })?;
        if att.status != "ready" {
            return Err(BoardError::bad_request(format!(
                "attachment '{att_id}' is not ready"
            )));
        }
        if !caller_can_access_attachment_tx(conn, board_id, board_grants, caller, &att)? {
            return Err(BoardError::forbidden(format!(
                "caller cannot access attachment '{att_id}'"
            )));
        }
        conn.execute(
            "INSERT INTO message_attachments (board_id, message_id, attachment_id, idx)
             VALUES (?1, ?2, ?3, ?4)",
            params![board_id, message_id, att_id, idx as i64],
        )?;
    }
    Ok(())
}

pub fn load_message_attachments_tx(
    conn: &Connection,
    message_id: &str,
) -> BoardResult<Vec<AttachmentRef>> {
    let mut stmt = conn.prepare_cached(
        "SELECT a.id, a.name, a.content_type, a.size
         FROM message_attachments ma
         JOIN attachments a ON a.id = ma.attachment_id
         WHERE ma.message_id = ?1
         ORDER BY ma.idx ASC",
    )?;
    let rows = stmt.query_map(params![message_id], |r| {
        Ok(AttachmentRef {
            id: r.get(0)?,
            name: r.get(1)?,
            content_type: r.get(2)?,
            size: r.get::<_, i64>(3)? as u64,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

pub fn load_messages_attachments_batch_tx(
    conn: &Connection,
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
        let mut stmt = conn.prepare_cached(&sql)?;
        let rows = stmt.query_map(params_from_iter(chunk.iter().copied()), |r| {
            Ok((
                r.get::<_, String>(0)?,
                AttachmentRef {
                    id: r.get(1)?,
                    name: r.get(2)?,
                    content_type: r.get(3)?,
                    size: r.get::<_, i64>(4)? as u64,
                },
            ))
        })?;
        for row in rows {
            let (mid, att) = row?;
            out.entry(mid).or_default().push(att);
        }
    }
    Ok(out)
}

/// Resolves an attachment for download, enforcing visibility and tombstone rules.
pub fn authorize_download(
    db: &Database,
    board_id: &str,
    att_id: &str,
    caller: &CallerContext,
) -> BoardResult<AttachmentRecord> {
    db.with_conn(|conn| {
        let att = load_attachment_tx(conn, board_id, att_id)?
            .ok_or_else(|| BoardError::not_found(format!("attachment '{att_id}' not found")))?;
        if att.status != "ready" {
            return Err(BoardError::not_found(format!("attachment '{att_id}' not found")));
        }
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        if !caller_can_access_attachment_tx(conn, board_id, &board_grants, caller, &att)? {
            return Err(BoardError::forbidden(format!(
                "not permitted to download attachment '{att_id}'"
            )));
        }
        Ok(att)
    })
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
