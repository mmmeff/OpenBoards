use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::actors::{self, CallerContext};
use crate::attachments;
use crate::db::{
    sql_placeholders, sql_placeholders_from, Database, DbRow, DbTx, Queryable, SqlArg,
};
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::events::{self, EventAppend, EventType};
use crate::filter::{FilterContext, MessageSnapshot, QueryFilter};
use crate::grants;
use crate::id::new_message_id;
use crate::labels;
use crate::mentions::{self, MentionResolution};
use crate::schedules;
use crate::schemas;
use crate::sql_args;
use crate::sync;
use crate::tasks::{self, TaskView};
use crate::validate::{validate_channel_path, validate_text_length};

pub const DEFAULT_PAGE_LIMIT: usize = 50;
pub const MAX_PAGE_LIMIT: usize = 500;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageAuthor {
    pub actor: String,
    pub principal: String,
    pub profile: Option<String>,
    pub handle: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachmentRef {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub content_type: String,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParsedBlock {
    pub schema: String,
    pub version: i64,
    pub data: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageView {
    pub id: String,
    pub channel: String,
    pub text: String,
    pub re: Option<String>,
    #[serde(rename = "replyTo", skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub by: MessageAuthor,
    pub labels: BTreeMap<String, String>,
    pub reactions: BTreeMap<String, Vec<String>>,
    pub attachments: Vec<AttachmentRef>,
    pub blocks: Vec<ParsedBlock>,
    pub from: Option<String>,
    #[serde(rename = "noSync")]
    pub no_sync: bool,
    pub revision: i64,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    pub deleted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadSummaryView {
    pub message: MessageView,
    pub replies: usize,
    pub last: Option<MessageView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagesPage {
    pub messages: Vec<MessageView>,
    pub cursor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadsPage {
    pub threads: Vec<ThreadSummaryView>,
    pub cursor: String,
    pub older: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadView {
    pub message: MessageView,
    pub replies: Vec<MessageView>,
    pub cursor: String,
    pub schedules: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostedMessageResult {
    #[serde(flatten)]
    pub message: MessageView,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub mentions: Vec<MentionResolution>,
    #[serde(skip_serializing)]
    pub replayed: bool,
    #[serde(skip_serializing)]
    pub event_seq: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct PostMessageInput {
    pub channel: String,
    #[serde(default)]
    pub text: String,
    pub id: Option<String>,
    #[serde(default)]
    pub attachments: Vec<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub schema: Option<String>,
    #[serde(default, alias = "noSync")]
    pub no_sync: bool,
}

#[derive(Debug, Deserialize)]
pub struct ReplyMessageInput {
    #[serde(default)]
    pub text: String,
    pub id: Option<String>,
    pub after: Option<String>,
    #[serde(default)]
    pub attachments: Vec<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub schema: Option<String>,
    #[serde(default, alias = "noSync")]
    pub no_sync: bool,
}

#[derive(Debug, Deserialize)]
pub struct EditMessageInput {
    pub text: String,
    pub revision: Option<i64>,
    pub schema: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DeleteMessageInput {
    pub revision: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ReactInput {
    pub emoji: String,
    #[serde(default)]
    pub remove: bool,
}

#[derive(Debug, Default, Clone)]
pub struct MessageListQuery {
    pub filter: QueryFilter,
    pub after: Option<String>,
    pub before: Option<String>,
    pub limit: Option<usize>,
}

/// Normalizes emoji inputs (short names, `:name:`, or Unicode emoji) to Chat SDK short names.
pub fn normalize_emoji(raw: &str) -> BoardResult<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(BoardError::bad_request("emoji cannot be empty"));
    }
    let stripped = trimmed
        .strip_prefix(':')
        .and_then(|s| s.strip_suffix(':'))
        .unwrap_or(trimmed)
        .trim();

    let mapped: Option<&str> = match stripped {
        "👍" | "+1" | "thumbsup" | "thumbs_up" => Some("thumbs_up"),
        "👎" | "-1" | "thumbsdown" | "thumbs_down" => Some("thumbs_down"),
        "👀" | "eyes" => Some("eyes"),
        "✅" | "white_check_mark" | "check" => Some("check"),
        "❤️" | "❤" | "heart" => Some("heart"),
        "🔥" | "fire" => Some("fire"),
        "🚀" | "rocket" => Some("rocket"),
        "🎉" | "tada" | "party_popper" => Some("party_popper"),
        "😄" | "smile" => Some("smile"),
        "😂" | "joy" => Some("joy"),
        "🤔" | "thinking" | "thinking_face" => Some("thinking"),
        "🙏" | "pray" => Some("pray"),
        "💯" | "100" => Some("100"),
        "⚠️" | "⚠" | "warning" => Some("warning"),
        "❌" | "x" => Some("x"),
        "✨" | "sparkles" => Some("sparkles"),
        "🐛" | "bug" => Some("bug"),
        "👏" | "clap" => Some("clap"),
        "😮" | "open_mouth" => Some("open_mouth"),
        "😢" | "cry" => Some("cry"),
        "😡" | "rage" => Some("rage"),
        _ => None,
    };

    let normalized = if let Some(m) = mapped {
        m.to_string()
    } else {
        stripped.to_ascii_lowercase().replace(['-', ' '], "_")
    };

    if normalized.is_empty()
        || normalized.len() > 64
        || !normalized
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(BoardError::bad_request(format!(
            "invalid emoji '{raw}'; use a normalized short name or supported emoji"
        )));
    }
    Ok(normalized)
}

#[derive(Debug, Clone)]
pub struct StoredMessageRow {
    pub id: String,
    pub board_id: String,
    pub seq: i64,
    pub channel: String,
    pub re: Option<String>,
    pub reply_to: Option<String>,
    pub text: String,
    pub by_actor: String,
    pub by_principal: String,
    pub by_profile: Option<String>,
    pub from_source: Option<String>,
    pub no_sync: bool,
    pub revision: i64,
    pub deleted_at: Option<String>,
    pub created_at: String,
}

pub async fn next_msg_seq_tx(tx: &mut DbTx, board_id: &str) -> BoardResult<i64> {
    let row = tx
        .fetch_one(
            "UPDATE boards
             SET next_msg_seq = next_msg_seq + 1
             WHERE id = ?1
             RETURNING next_msg_seq - 1",
            sql_args![board_id],
        )
        .await?;
    row.get_i64(0)
}

async fn current_board_cursor_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
) -> BoardResult<String> {
    events::current_event_cursor_tx(conn, board_id).await
}

pub async fn load_stored_message_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    message_id: &str,
) -> BoardResult<Option<StoredMessageRow>> {
    let row = conn
        .fetch_optional(
            "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                    by_profile, from_source, no_sync, revision, deleted_at, created_at
             FROM messages
             WHERE board_id = ?1 AND id = ?2",
            sql_args![board_id, message_id],
        )
        .await?;
    row.as_ref().map(map_stored_row).transpose()
}

/// Loads a message row with `FOR UPDATE` on PostgreSQL to serialize CAS mutations
/// (label CAS, edit revision CAS, delete revision CAS).
pub async fn load_stored_message_for_update_tx(
    tx: &mut DbTx,
    board_id: &str,
    message_id: &str,
) -> BoardResult<Option<StoredMessageRow>> {
    let sql = format!(
        "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                by_profile, from_source, no_sync, revision, deleted_at, created_at
         FROM messages
         WHERE board_id = ?1 AND id = ?2{}",
        tx.lock_row_suffix()
    );
    let row = tx.fetch_optional(&sql, sql_args![board_id, message_id]).await?;
    row.as_ref().map(map_stored_row).transpose()
}

/// Maintains the `threads` summary index atomically whenever a root message or reply is inserted.
pub async fn record_thread_message_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    channel: &str,
    msg_id: &str,
    seq: i64,
    re_root_id: Option<&str>,
) -> BoardResult<()> {
    if let Some(root_id) = re_root_id {
        conn.execute(
            "UPDATE threads
             SET last_seq = ?1,
                 last_id = ?2,
                 last_reply_id = ?2,
                 reply_count = reply_count + 1
             WHERE root_id = ?3",
            sql_args![seq, msg_id, root_id],
        )
        .await?;
    } else {
        conn.execute(
            "INSERT INTO threads (
                root_id, board_id, channel, root_seq, last_seq, last_id, last_reply_id, reply_count
             ) VALUES (?1, ?2, ?3, ?4, ?4, ?1, NULL, 0)",
            sql_args![msg_id, board_id, channel, seq],
        )
        .await?;
    }
    Ok(())
}

async fn load_stored_messages_batch_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    message_ids: &[&str],
) -> BoardResult<HashMap<String, StoredMessageRow>> {
    let mut out = HashMap::with_capacity(message_ids.len());
    if message_ids.is_empty() {
        return Ok(out);
    }
    for chunk in message_ids.chunks(400) {
        let sql = format!(
            "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                    by_profile, from_source, no_sync, revision, deleted_at, created_at
             FROM messages
             WHERE board_id = ?1 AND id IN ({})",
            sql_placeholders_from(2, chunk.len())
        );
        let mut args: Vec<SqlArg<'_>> = Vec::with_capacity(chunk.len() + 1);
        args.push(SqlArg::from(board_id));
        for &id in chunk {
            args.push(SqlArg::from(id));
        }
        let rows = conn.fetch_all(&sql, &args).await?;
        for r in &rows {
            let stored = map_stored_row(r)?;
            out.insert(stored.id.clone(), stored);
        }
    }
    Ok(out)
}

fn map_stored_row(r: &DbRow) -> BoardResult<StoredMessageRow> {
    Ok(StoredMessageRow {
        id: r.get_str(0)?,
        board_id: r.get_str(1)?,
        seq: r.get_i64(2)?,
        channel: r.get_str(3)?,
        re: r.get_opt_str(4)?,
        reply_to: r.get_opt_str(5)?,
        text: r.get_str(6)?,
        by_actor: r.get_str(7)?,
        by_principal: r.get_str(8)?,
        by_profile: r.get_opt_str(9)?,
        from_source: r.get_opt_str(10)?,
        no_sync: r.get_i64(11)? != 0,
        revision: r.get_i64(12)?,
        deleted_at: r.get_opt_str(13)?,
        created_at: r.get_str(14)?,
    })
}

pub async fn hydrate_message_view_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    row: &StoredMessageRow,
) -> BoardResult<MessageView> {
    let handle = actors::resolve_actor_view(conn, &row.by_actor)
        .await?
        .map(|a| a.handle)
        .unwrap_or_else(|| row.by_actor.clone());

    let deleted = row.deleted_at.is_some();

    let (labels, reactions, attachments, blocks) = if deleted {
        (BTreeMap::new(), BTreeMap::new(), Vec::new(), Vec::new())
    } else {
        (
            labels::load_message_labels_tx(conn, &row.id).await?,
            load_reactions_tx(conn, &row.id).await?,
            attachments::load_message_attachments_tx(conn, &row.id).await?,
            schemas::load_message_blocks_tx(conn, &row.id).await?,
        )
    };

    let task = if row.re.is_none() {
        tasks::load_task_view_tx(conn, &row.id).await?
    } else {
        None
    };

    Ok(MessageView {
        id: row.id.clone(),
        channel: row.channel.clone(),
        text: if deleted {
            String::new()
        } else {
            row.text.clone()
        },
        re: row.re.clone(),
        reply_to: row.reply_to.clone(),
        by: MessageAuthor {
            actor: row.by_actor.clone(),
            principal: row.by_principal.clone(),
            profile: row.by_profile.clone(),
            handle,
        },
        labels,
        reactions,
        attachments,
        blocks,
        from: row.from_source.clone(),
        no_sync: row.no_sync,
        revision: row.revision,
        created_at: row.created_at.clone(),
        deleted,
        task,
    })
}

pub async fn load_mention_ids_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_id: &str,
) -> BoardResult<Vec<String>> {
    let rows = conn
        .fetch_all(
            "SELECT actor_id FROM message_mentions WHERE message_id = ?1 ORDER BY actor_id ASC",
            sql_args![message_id],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(r.get_str(0)?);
    }
    Ok(out)
}

/// Builds an immutable `MessageSnapshot` (including root thread labels and mentions) for filtering
/// and event-log before/after evaluation.
pub async fn build_message_snapshot_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    row: &StoredMessageRow,
) -> BoardResult<MessageSnapshot> {
    let handle = actors::resolve_actor_view(conn, &row.by_actor)
        .await?
        .map(|a| a.handle)
        .unwrap_or_else(|| row.by_actor.clone());
    let deleted = row.deleted_at.is_some();
    let msg_labels = if deleted {
        BTreeMap::new()
    } else {
        labels::load_message_labels_tx(conn, &row.id).await?
    };
    let root_labels = match &row.re {
        Some(root_id) => Some(labels::load_message_labels_tx(conn, root_id).await?),
        None => None,
    };
    let mention_ids = if deleted {
        Vec::new()
    } else {
        load_mention_ids_tx(conn, &row.id).await?
    };
    Ok(MessageSnapshot {
        id: row.id.clone(),
        channel: row.channel.clone(),
        re: row.re.clone(),
        text: if deleted {
            String::new()
        } else {
            row.text.clone()
        },
        by_actor: row.by_actor.clone(),
        by_principal: row.by_principal.clone(),
        by_handle: handle,
        labels: msg_labels,
        root_labels,
        mention_ids,
        from: row.from_source.clone(),
        deleted,
    })
}

async fn load_reactions_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_id: &str,
) -> BoardResult<BTreeMap<String, Vec<String>>> {
    let rows = conn
        .fetch_all(
            "SELECT emoji, actor_id FROM message_reactions WHERE message_id = ?1 ORDER BY emoji ASC, actor_id ASC",
            sql_args![message_id],
        )
        .await?;
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for r in rows {
        map.entry(r.get_str(0)?).or_default().push(r.get_str(1)?);
    }
    Ok(map)
}

async fn load_reactions_batch_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_ids: &[&str],
) -> BoardResult<HashMap<String, BTreeMap<String, Vec<String>>>> {
    let mut out: HashMap<String, BTreeMap<String, Vec<String>>> = HashMap::new();
    if message_ids.is_empty() {
        return Ok(out);
    }
    for chunk in message_ids.chunks(400) {
        let sql = format!(
            "SELECT message_id, emoji, actor_id
             FROM message_reactions
             WHERE message_id IN ({})
             ORDER BY message_id ASC, emoji ASC, actor_id ASC",
            sql_placeholders(chunk.len())
        );
        let args: Vec<SqlArg<'_>> = chunk.iter().copied().map(SqlArg::from).collect();
        let rows = conn.fetch_all(&sql, &args).await?;
        for r in rows {
            out.entry(r.get_str(0)?)
                .or_default()
                .entry(r.get_str(1)?)
                .or_default()
                .push(r.get_str(2)?);
        }
    }
    Ok(out)
}

/// Batch-hydrates a slice of `StoredMessageRow`s using bulk `IN (...)` queries instead of
/// per-row N+1 lookups. Preserves input ordering and exact `MessageView` semantics.
pub async fn hydrate_message_views_batch_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    rows: &[StoredMessageRow],
) -> BoardResult<Vec<MessageView>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let author_ids: Vec<&str> = rows.iter().map(|r| r.by_actor.as_str()).collect();
    let handles = actors::resolve_actor_handles_batch_tx(conn, &author_ids).await?;

    let active_ids: Vec<&str> = rows
        .iter()
        .filter(|r| r.deleted_at.is_none())
        .map(|r| r.id.as_str())
        .collect();
    let root_ids: Vec<&str> = rows
        .iter()
        .filter(|r| r.re.is_none())
        .map(|r| r.id.as_str())
        .collect();

    let mut labels_map = labels::load_messages_labels_batch_tx(conn, &active_ids).await?;
    let mut reactions_map = load_reactions_batch_tx(conn, &active_ids).await?;
    let mut attachments_map =
        attachments::load_messages_attachments_batch_tx(conn, &active_ids).await?;
    let mut blocks_map = schemas::load_messages_blocks_batch_tx(conn, &active_ids).await?;
    let mut tasks_map = tasks::load_task_views_batch_tx(conn, &root_ids).await?;

    let mut views = Vec::with_capacity(rows.len());
    for row in rows {
        let deleted = row.deleted_at.is_some();
        let handle = handles
            .get(&row.by_actor)
            .cloned()
            .unwrap_or_else(|| row.by_actor.clone());
        let (labels, reactions, attachments, blocks) = if deleted {
            (BTreeMap::new(), BTreeMap::new(), Vec::new(), Vec::new())
        } else {
            (
                labels_map.remove(&row.id).unwrap_or_default(),
                reactions_map.remove(&row.id).unwrap_or_default(),
                attachments_map.remove(&row.id).unwrap_or_default(),
                blocks_map.remove(&row.id).unwrap_or_default(),
            )
        };
        let task = if row.re.is_none() {
            tasks_map.remove(&row.id)
        } else {
            None
        };
        views.push(MessageView {
            id: row.id.clone(),
            channel: row.channel.clone(),
            text: if deleted {
                String::new()
            } else {
                row.text.clone()
            },
            re: row.re.clone(),
            reply_to: row.reply_to.clone(),
            by: MessageAuthor {
                actor: row.by_actor.clone(),
                principal: row.by_principal.clone(),
                profile: row.by_profile.clone(),
                handle,
            },
            labels,
            reactions,
            attachments,
            blocks,
            from: row.from_source.clone(),
            no_sync: row.no_sync,
            revision: row.revision,
            created_at: row.created_at.clone(),
            deleted,
            task,
        });
    }
    Ok(views)
}

async fn visible_actors_for_caller_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    board_grants: &[grants::Grant],
    caller: &CallerContext,
) -> BoardResult<Vec<actors::ActorView>> {
    let has_wildcard_read =
        grants::caller_permissions_at_pattern(board_grants, caller, "*").can_read();
    actors::list_board_actors(conn, board_id, has_wildcard_read, |ch| {
        grants::caller_permissions_on_channel(board_grants, caller, ch).can_read()
    })
    .await
}

async fn check_idempotency_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    caller_actor: &str,
    key: &str,
    expected_scope: &str,
    expected_channel: &str,
    expected_root: Option<&str>,
) -> BoardResult<Option<MessageView>> {
    let row = conn
        .fetch_optional(
            "SELECT scope_kind, channel, thread_root, message_id
             FROM idempotency_keys
             WHERE board_id = ?1 AND caller_actor = ?2 AND key = ?3",
            sql_args![board_id, caller_actor, key],
        )
        .await?;

    let Some(r) = row else {
        let used_in_batch = conn
            .fetch_optional(
                "SELECT 1 FROM task_batches
                 WHERE board_id = ?1 AND caller_actor = ?2 AND idempotency_key = ?3",
                sql_args![board_id, caller_actor, key],
            )
            .await?
            .is_some();
        if used_in_batch {
            return Err(BoardError::new(
                ErrorCode::DuplicateId,
                format!("idempotency key '{key}' was already used for a task batch"),
            ));
        }
        return Ok(None);
    };

    let scope = r.get_str(0)?;
    let ch = r.get_str(1)?;
    let root = r.get_opt_str(2)?;
    let msg_id = r.get_str(3)?;

    if scope != expected_scope || ch != expected_channel || root.as_deref() != expected_root {
        return Err(BoardError::new(
            ErrorCode::DuplicateId,
            format!("idempotency key '{key}' was already used for a different target"),
        ));
    }

    let stored = load_stored_message_tx(conn, board_id, &msg_id)
        .await?
        .ok_or_else(|| BoardError::internal("idempotency key points to missing message"))?;
    Ok(Some(hydrate_message_view_tx(conn, &stored).await?))
}

struct IdempotencyRecord<'a> {
    board_id: &'a str,
    caller_actor: &'a str,
    key: &'a str,
    scope: &'a str,
    channel: &'a str,
    thread_root: Option<&'a str>,
    message_id: &'a str,
    now: &'a str,
}

async fn store_idempotency_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    rec: IdempotencyRecord<'_>,
) -> BoardResult<()> {
    conn.execute(
        "INSERT INTO idempotency_keys (board_id, caller_actor, key, scope_kind, channel, thread_root, message_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        sql_args![
            rec.board_id,
            rec.caller_actor,
            rec.key,
            rec.scope,
            rec.channel,
            rec.thread_root,
            rec.message_id,
            rec.now
        ],
    )
    .await?;
    Ok(())
}

async fn store_mentions_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    message_id: &str,
    actor_ids: &[String],
) -> BoardResult<()> {
    conn.execute(
        "DELETE FROM message_mentions WHERE message_id = ?1",
        sql_args![message_id],
    )
    .await?;
    for aid in actor_ids {
        conn.execute(
            "INSERT INTO message_mentions (board_id, message_id, actor_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT (message_id, actor_id) DO NOTHING",
            sql_args![board_id, message_id, aid],
        )
        .await?;
    }
    Ok(())
}

/// Starts a new thread in `input.channel`.
///
/// - Implicitly creates the channel if it doesn't exist (requires `read` + `post`).
/// - Posting to an existing channel requires only `post` (blind intake).
pub async fn post_message(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: PostMessageInput,
) -> BoardResult<PostedMessageResult> {
    let mut tx = db.begin_tx().await?;
    let res = post_message_in_tx(&mut tx, board_id, caller, input).await?;
    tx.commit().await?;
    Ok(res)
}

/// Transaction-scoped version of `post_message` so callers (e.g., `tasks::create_task`,
/// `tasks::create_many_tasks`, and atomic scheduler) can compose message creation with other writes.
pub async fn post_message_in_tx(
    tx: &mut DbTx,
    board_id: &str,
    caller: &CallerContext,
    input: PostMessageInput,
) -> BoardResult<PostedMessageResult> {
    validate_channel_path(&input.channel)?;
    validate_text_length(&input.text)?;
    if input.text.is_empty() && input.attachments.is_empty() {
        return Err(BoardError::bad_request(
            "message text cannot be empty unless attachments are provided",
        ));
    }
    if !input.labels.is_empty() {
        labels::validate_user_labels(&input.labels)?;
    }

    let board_grants = grants::load_grants_tx(tx, board_id).await?;
    let perms = grants::caller_permissions_on_channel(&board_grants, caller, &input.channel);

    let channel_exists = tx
        .fetch_optional(
            "SELECT 1 FROM channels WHERE board_id = ?1 AND path = ?2",
            sql_args![board_id, &input.channel],
        )
        .await?
        .is_some();

    if channel_exists {
        if !perms.can_post_new_thread() {
            return Err(BoardError::forbidden(format!(
                "posting in '{}' requires 'post' permission",
                input.channel
            )));
        }
    } else if !perms.can_create_channel() {
        return Err(BoardError::forbidden(format!(
            "creating channel '{}' on first post requires 'read' and 'post' permissions",
            input.channel
        )));
    }
    if !input.labels.is_empty() && !perms.can_label() {
        return Err(BoardError::forbidden(format!(
            "setting labels in '{}' requires 'label' permission",
            input.channel
        )));
    }

    let actor_id = caller.actor_id();
    if let Some(idem_key) = &input.id {
        tx.advisory_xact_lock("idem", &format!("{board_id}:{actor_id}:{idem_key}"))
            .await?;
        if let Some(existing) = check_idempotency_tx(
            tx,
            board_id,
            &actor_id,
            idem_key,
            "post",
            &input.channel,
            None,
        )
        .await?
        {
            return Ok(PostedMessageResult {
                message: existing,
                mentions: Vec::new(),
                replayed: true,
                event_seq: None,
            });
        }
    }

    let parsed_blocks =
        schemas::parse_and_validate_blocks_tx(tx, &input.text, input.schema.as_deref()).await?;

    let now = Utc::now().to_rfc3339();
    if !channel_exists {
        tx.execute(
            "INSERT INTO channels (board_id, path, created_by, created_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT (board_id, path) DO NOTHING",
            sql_args![board_id, &input.channel, &actor_id, &now],
        )
        .await?;
    }

    actors::record_actor_write_tx(tx, board_id, &input.channel, caller).await?;

    let visible_actors = visible_actors_for_caller_tx(tx, board_id, &board_grants, caller).await?;
    let resolved_mentions = mentions::resolve_mentions(&input.text, &visible_actors);

    let msg_id = new_message_id();
    let seq = next_msg_seq_tx(tx, board_id).await?;
    let by_principal = caller.principal_id();
    let by_profile = caller.profile.as_ref().map(|p| p.name_lower.clone());

    tx.execute(
        "INSERT INTO messages (
            id, board_id, seq, channel, re, reply_to, text,
            by_actor, by_principal, by_profile, from_source, no_sync,
            revision, deleted_at, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, ?6, ?7, ?8, NULL, ?9, 1, NULL, ?10, ?10)",
        sql_args![
            &msg_id,
            board_id,
            seq,
            &input.channel,
            &resolved_mentions.text,
            &actor_id,
            &by_principal,
            &by_profile,
            i64::from(input.no_sync),
            &now
        ],
    )
    .await?;
    record_thread_message_tx(tx, board_id, &input.channel, &msg_id, seq, None).await?;

    store_mentions_tx(tx, board_id, &msg_id, &resolved_mentions.actor_ids).await?;
    if !input.attachments.is_empty() {
        attachments::validate_and_link_attachments_tx(
            tx,
            board_id,
            &board_grants,
            caller,
            &msg_id,
            &input.attachments,
        )
        .await?;
    }
    if !input.labels.is_empty() {
        labels::insert_labels_tx(tx, board_id, &msg_id, &input.labels).await?;
    }
    if !parsed_blocks.is_empty() {
        schemas::store_message_blocks_tx(tx, board_id, &msg_id, &parsed_blocks).await?;
    }

    if let Some(idem_key) = &input.id {
        store_idempotency_tx(
            tx,
            IdempotencyRecord {
                board_id,
                caller_actor: &actor_id,
                key: idem_key,
                scope: "post",
                channel: &input.channel,
                thread_root: None,
                message_id: &msg_id,
                now: &now,
            },
        )
        .await?;
    }

    let stored = load_stored_message_tx(tx, board_id, &msg_id)
        .await?
        .expect("inserted message must exist");
    let snap = build_message_snapshot_tx(tx, &stored).await?;
    let view = hydrate_message_view_tx(tx, &stored).await?;
    let ev = events::append_event_tx(
        tx,
        EventAppend {
            board_id,
            event_type: EventType::MessagePosted,
            channel: Some(&input.channel),
            message_id: Some(&msg_id),
            actor_id: Some(&actor_id),
            payload: serde_json::json!({ "channel": input.channel, "re": Value::Null }),
            before_snapshot: None,
            after_snapshot: Some(&snap),
            after_message: Some(&view),
        },
    )
    .await?;
    let _ = sync::enqueue_outbound_for_message_tx(tx, board_id, &msg_id, "post", None).await?;
    Ok(PostedMessageResult {
        message: view,
        mentions: resolved_mentions.mentions,
        replayed: false,
        event_seq: Some(ev.seq),
    })
}

/// Replies in a thread identified by `target_message_id` (root or any reply).
///
/// Requires `read` + `post` in the thread's channel.
pub async fn reply_message(
    db: &Database,
    board_id: &str,
    target_message_id: &str,
    caller: &CallerContext,
    input: ReplyMessageInput,
) -> BoardResult<PostedMessageResult> {
    let mut tx = db.begin_tx().await?;
    let res = reply_message_in_tx(&mut tx, board_id, target_message_id, caller, input).await?;
    tx.commit().await?;
    Ok(res)
}

/// Transaction-scoped version of `reply_message` (enables single-tx atomic scheduler firing).
pub async fn reply_message_in_tx(
    tx: &mut DbTx,
    board_id: &str,
    target_message_id: &str,
    caller: &CallerContext,
    input: ReplyMessageInput,
) -> BoardResult<PostedMessageResult> {
    validate_text_length(&input.text)?;
    if input.text.is_empty() && input.attachments.is_empty() {
        return Err(BoardError::bad_request(
            "message text cannot be empty unless attachments are provided",
        ));
    }
    if !input.labels.is_empty() {
        labels::validate_user_labels(&input.labels)?;
    }

    let target = load_stored_message_tx(tx, board_id, target_message_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{target_message_id}' not found"),
            )
        })?;

    let board_grants = grants::load_grants_tx(tx, board_id).await?;
    let perms = grants::caller_permissions_on_channel(&board_grants, caller, &target.channel);
    if !perms.can_read() {
        return Err(BoardError::new(
            ErrorCode::MessageNotFound,
            format!("message '{target_message_id}' not found"),
        ));
    }
    if !perms.can_reply() {
        return Err(BoardError::forbidden(format!(
            "replying in '{}' requires 'read' and 'post' permissions",
            target.channel
        )));
    }
    if !input.labels.is_empty() && !perms.can_label() {
        return Err(BoardError::forbidden(format!(
            "setting labels in '{}' requires 'label' permission",
            target.channel
        )));
    }

    let root_id = target.re.clone().unwrap_or_else(|| target.id.clone());
    let reply_to = if target.re.is_some() {
        Some(target.id.clone())
    } else {
        None
    };

    let actor_id = caller.actor_id();
    if let Some(idem_key) = &input.id {
        tx.advisory_xact_lock("idem", &format!("{board_id}:{actor_id}:{idem_key}"))
            .await?;
        if let Some(existing) = check_idempotency_tx(
            tx,
            board_id,
            &actor_id,
            idem_key,
            "reply",
            &target.channel,
            Some(&root_id),
        )
        .await?
        {
            return Ok(PostedMessageResult {
                message: existing,
                mentions: Vec::new(),
                replayed: true,
                event_seq: None,
            });
        }
    }

    // Lock thread root row on Postgres before checking `--after` and updating `threads` summary.
    let lock_root_sql = format!(
        "SELECT id FROM messages WHERE board_id = ?1 AND id = ?2{}",
        tx.lock_row_suffix()
    );
    let _ = tx.fetch_one(&lock_root_sql, sql_args![board_id, &root_id]).await?;

    // Concurrency guard: `after` must still be the thread's latest message
    if let Some(expected_after) = &input.after {
        let latest_id = tx
            .fetch_one(
                "SELECT id FROM messages
                 WHERE board_id = ?1 AND (id = ?2 OR re = ?2)
                 ORDER BY seq DESC LIMIT 1",
                sql_args![board_id, &root_id],
            )
            .await?
            .get_str(0)?;
        if &latest_id != expected_after {
            return Err(BoardError::new(
                ErrorCode::NewerMessage,
                format!(
                    "thread '{root_id}' has a newer message ('{latest_id}') than '{expected_after}'"
                ),
            ));
        }
    }

    let parsed_blocks =
        schemas::parse_and_validate_blocks_tx(tx, &input.text, input.schema.as_deref()).await?;

    let now = Utc::now().to_rfc3339();
    actors::record_actor_write_tx(tx, board_id, &target.channel, caller).await?;

    let visible_actors = visible_actors_for_caller_tx(tx, board_id, &board_grants, caller).await?;
    let resolved_mentions = mentions::resolve_mentions(&input.text, &visible_actors);

    let msg_id = new_message_id();
    let seq = next_msg_seq_tx(tx, board_id).await?;
    let by_principal = caller.principal_id();
    let by_profile = caller.profile.as_ref().map(|p| p.name_lower.clone());

    tx.execute(
        "INSERT INTO messages (
            id, board_id, seq, channel, re, reply_to, text,
            by_actor, by_principal, by_profile, from_source, no_sync,
            revision, deleted_at, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?11, 1, NULL, ?12, ?12)",
        sql_args![
            &msg_id,
            board_id,
            seq,
            &target.channel,
            &root_id,
            &reply_to,
            &resolved_mentions.text,
            &actor_id,
            &by_principal,
            &by_profile,
            i64::from(input.no_sync),
            &now
        ],
    )
    .await?;
    record_thread_message_tx(tx, board_id, &target.channel, &msg_id, seq, Some(&root_id)).await?;

    store_mentions_tx(tx, board_id, &msg_id, &resolved_mentions.actor_ids).await?;
    if !input.attachments.is_empty() {
        attachments::validate_and_link_attachments_tx(
            tx,
            board_id,
            &board_grants,
            caller,
            &msg_id,
            &input.attachments,
        )
        .await?;
    }
    if !input.labels.is_empty() {
        labels::insert_labels_tx(tx, board_id, &msg_id, &input.labels).await?;
    }
    if !parsed_blocks.is_empty() {
        schemas::store_message_blocks_tx(tx, board_id, &msg_id, &parsed_blocks).await?;
    }

    if let Some(idem_key) = &input.id {
        store_idempotency_tx(
            tx,
            IdempotencyRecord {
                board_id,
                caller_actor: &actor_id,
                key: idem_key,
                scope: "reply",
                channel: &target.channel,
                thread_root: Some(&root_id),
                message_id: &msg_id,
                now: &now,
            },
        )
        .await?;
    }

    let stored = load_stored_message_tx(tx, board_id, &msg_id)
        .await?
        .expect("inserted reply must exist");
    let snap = build_message_snapshot_tx(tx, &stored).await?;
    let view = hydrate_message_view_tx(tx, &stored).await?;
    let ev = events::append_event_tx(
        tx,
        EventAppend {
            board_id,
            event_type: EventType::MessagePosted,
            channel: Some(&target.channel),
            message_id: Some(&msg_id),
            actor_id: Some(&actor_id),
            payload: serde_json::json!({
                "channel": target.channel,
                "re": root_id,
                "replyTo": reply_to
            }),
            before_snapshot: None,
            after_snapshot: Some(&snap),
            after_message: Some(&view),
        },
    )
    .await?;
    let _ = sync::enqueue_outbound_for_message_tx(tx, board_id, &msg_id, "reply", None).await?;
    Ok(PostedMessageResult {
        message: view,
        mentions: resolved_mentions.mentions,
        replayed: false,
        event_seq: Some(ev.seq),
    })
}

/// Edits a message's text. Only the author (or bare principal for its profiles) can edit.
pub async fn edit_message(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: EditMessageInput,
) -> BoardResult<(MessageView, Vec<MentionResolution>, Vec<i64>)> {
    validate_text_length(&input.text)?;

    let mut tx = db.begin_tx().await?;
    let msg = load_stored_message_for_update_tx(&mut tx, board_id, message_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;

    let board_grants = grants::load_grants_tx(&mut tx, board_id).await?;
    let perms = grants::caller_permissions_on_channel(&board_grants, caller, &msg.channel);
    let is_author = caller.owns_actor_item(&msg.by_actor);

    if !(is_author && perms.can_post_new_thread()) {
        if !perms.can_read() {
            return Err(BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            ));
        }
        return Err(BoardError::forbidden(
            "only the message author can edit a message",
        ));
    }

    if let Some(expected_rev) = input.revision {
        if expected_rev != msg.revision {
            return Err(BoardError::new(
                ErrorCode::RevisionConflict,
                format!(
                    "message '{message_id}' is at revision {}, expected {expected_rev}",
                    msg.revision
                ),
            ));
        }
    }

    if msg.deleted_at.is_some() {
        return Err(BoardError::new(
            ErrorCode::MessageDeleted,
            format!("message '{message_id}' has been deleted"),
        ));
    }

    if input.text.is_empty()
        && attachments::load_message_attachments_tx(&mut tx, message_id)
            .await?
            .is_empty()
    {
        return Err(BoardError::bad_request(
            "edited text cannot be empty unless the message has attachments",
        ));
    }

    let before_snap = build_message_snapshot_tx(&mut tx, &msg).await?;
    let parsed_blocks =
        schemas::parse_and_validate_blocks_tx(&mut tx, &input.text, input.schema.as_deref())
            .await?;
    let visible_actors =
        visible_actors_for_caller_tx(&mut tx, board_id, &board_grants, caller).await?;
    let resolved_mentions = mentions::resolve_mentions(&input.text, &visible_actors);
    let now = Utc::now().to_rfc3339();
    let next_rev = msg.revision + 1;

    let updated_rows = tx
        .execute(
            "UPDATE messages SET text = ?1, revision = ?2, updated_at = ?3
             WHERE board_id = ?4 AND id = ?5 AND revision = ?6",
            sql_args![
                &resolved_mentions.text,
                next_rev,
                &now,
                board_id,
                message_id,
                msg.revision
            ],
        )
        .await?;
    if updated_rows == 0 {
        return Err(BoardError::new(
            ErrorCode::RevisionConflict,
            format!("message '{message_id}' was concurrently modified"),
        ));
    }
    store_mentions_tx(&mut tx, board_id, message_id, &resolved_mentions.actor_ids).await?;
    schemas::store_message_blocks_tx(&mut tx, board_id, message_id, &parsed_blocks).await?;

    let updated = load_stored_message_tx(&mut tx, board_id, message_id)
        .await?
        .unwrap();
    let after_snap = build_message_snapshot_tx(&mut tx, &updated).await?;
    let view = hydrate_message_view_tx(&mut tx, &updated).await?;
    let actor_id = caller.actor_id();
    let ev = events::append_event_tx(
        &mut tx,
        EventAppend {
            board_id,
            event_type: EventType::MessageEdited,
            channel: Some(&msg.channel),
            message_id: Some(message_id),
            actor_id: Some(&actor_id),
            payload: serde_json::json!({ "revision": next_rev }),
            before_snapshot: Some(&before_snap),
            after_snapshot: Some(&after_snap),
            after_message: Some(&view),
        },
    )
    .await?;
    let mut seqs = vec![ev.seq];
    if msg.re.is_none() {
        if let Some(task_seq) =
            tasks::on_root_edited_tx(&mut tx, board_id, message_id, caller).await?
        {
            seqs.push(task_seq);
        }
    }
    let _ = sync::enqueue_outbound_for_message_tx(&mut tx, board_id, message_id, "edit", None)
        .await?;
    let final_view = hydrate_message_view_tx(&mut tx, &updated).await?;
    tx.commit().await?;
    Ok((final_view, resolved_mentions.mentions, seqs))
}

/// Deletes (tombstones) a message.
///
/// Allowed for author (or bare principal for its profiles) OR team owner with `manage` in channel.
/// Repeat delete on an already-deleted message is an idempotent no-op.
pub async fn delete_message(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: DeleteMessageInput,
) -> BoardResult<Vec<i64>> {
    let mut tx = db.begin_tx().await?;
    let msg = load_stored_message_for_update_tx(&mut tx, board_id, message_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;

    let board_grants = grants::load_grants_tx(&mut tx, board_id).await?;
    let perms = grants::caller_permissions_on_channel(&board_grants, caller, &msg.channel);
    let is_author = caller.owns_actor_item(&msg.by_actor);
    let owner_can_moderate =
        caller.principal.is_owner() && !caller.is_restricted() && perms.can_manage();

    if !(is_author && perms.can_post_new_thread()) && !owner_can_moderate {
        if !perms.can_read() {
            return Err(BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            ));
        }
        return Err(BoardError::forbidden(
            "only the message author or a team owner with 'manage' in the channel can delete this message",
        ));
    }

    if let Some(expected_rev) = input.revision {
        if expected_rev != msg.revision {
            return Err(BoardError::new(
                ErrorCode::RevisionConflict,
                format!(
                    "message '{message_id}' is at revision {}, expected {expected_rev}",
                    msg.revision
                ),
            ));
        }
    }

    if msg.deleted_at.is_some() {
        tx.commit().await?;
        return Ok(Vec::new());
    }

    let before_snap = build_message_snapshot_tx(&mut tx, &msg).await?;
    let now = Utc::now().to_rfc3339();
    let next_rev = msg.revision + 1;
    let updated_rows = tx
        .execute(
            "UPDATE messages SET text = '', deleted_at = ?1, revision = ?2, updated_at = ?1
             WHERE board_id = ?3 AND id = ?4 AND revision = ?5",
            sql_args![&now, next_rev, board_id, message_id, msg.revision],
        )
        .await?;
    if updated_rows == 0 {
        return Err(BoardError::new(
            ErrorCode::RevisionConflict,
            format!("message '{message_id}' was concurrently modified"),
        ));
    }
    tx.execute(
        "DELETE FROM message_reactions WHERE message_id = ?1",
        sql_args![message_id],
    )
    .await?;
    tx.execute(
        "DELETE FROM message_mentions WHERE message_id = ?1",
        sql_args![message_id],
    )
    .await?;
    labels::delete_message_labels_tx(&mut tx, message_id).await?;
    schemas::delete_message_blocks_tx(&mut tx, message_id).await?;

    let updated = load_stored_message_tx(&mut tx, board_id, message_id)
        .await?
        .unwrap();
    let after_snap = build_message_snapshot_tx(&mut tx, &updated).await?;
    let view = hydrate_message_view_tx(&mut tx, &updated).await?;
    let actor_id = caller.actor_id();
    let ev = events::append_event_tx(
        &mut tx,
        EventAppend {
            board_id,
            event_type: EventType::MessageDeleted,
            channel: Some(&msg.channel),
            message_id: Some(message_id),
            actor_id: Some(&actor_id),
            payload: serde_json::json!({ "revision": next_rev }),
            before_snapshot: Some(&before_snap),
            after_snapshot: Some(&after_snap),
            after_message: Some(&view),
        },
    )
    .await?;
    let mut seqs = vec![ev.seq];
    if msg.re.is_none() {
        if let Some(task_seq) =
            tasks::on_root_deleted_tx(&mut tx, board_id, message_id, &actor_id).await?
        {
            seqs.push(task_seq);
        }
    }
    let _ = sync::enqueue_outbound_for_message_tx(&mut tx, board_id, message_id, "delete", None)
        .await?;
    tx.commit().await?;

    Ok(seqs)
}

/// Adds or removes a normalized emoji reaction on a message.
pub async fn react_message(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: ReactInput,
) -> BoardResult<(String, Option<i64>)> {
    let emoji = normalize_emoji(&input.emoji)?;

    let mut tx = db.begin_tx().await?;
    let msg = load_stored_message_tx(&mut tx, board_id, message_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;

    let board_grants = grants::load_grants_tx(&mut tx, board_id).await?;
    let perms = grants::caller_permissions_on_channel(&board_grants, caller, &msg.channel);
    if !perms.can_read() {
        return Err(BoardError::new(
            ErrorCode::MessageNotFound,
            format!("message '{message_id}' not found"),
        ));
    }
    if !perms.can_react() {
        return Err(BoardError::forbidden(format!(
            "reacting in '{}' requires 'react' permission",
            msg.channel
        )));
    }
    if msg.deleted_at.is_some() {
        return Err(BoardError::new(
            ErrorCode::MessageDeleted,
            format!("cannot react to deleted message '{message_id}'"),
        ));
    }

    let actor_id = caller.actor_id();
    let changed = if input.remove {
        let n = tx
            .execute(
                "DELETE FROM message_reactions WHERE message_id = ?1 AND actor_id = ?2 AND emoji = ?3",
                sql_args![message_id, &actor_id, &emoji],
            )
            .await?;
        n > 0
    } else {
        actors::record_actor_write_tx(&mut tx, board_id, &msg.channel, caller).await?;
        let now = Utc::now().to_rfc3339();
        let n = tx
            .execute(
                "INSERT INTO message_reactions (board_id, message_id, emoji, actor_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (message_id, actor_id, emoji) DO NOTHING",
                sql_args![board_id, message_id, &emoji, &actor_id, &now],
            )
            .await?;
        n > 0
    };

    let event_seq = if changed {
        let snap = build_message_snapshot_tx(&mut tx, &msg).await?;
        let view = hydrate_message_view_tx(&mut tx, &msg).await?;
        let ev_type = if input.remove {
            EventType::ReactionRemoved
        } else {
            EventType::ReactionAdded
        };
        let ev = events::append_event_tx(
            &mut tx,
            EventAppend {
                board_id,
                event_type: ev_type,
                channel: Some(&msg.channel),
                message_id: Some(message_id),
                actor_id: Some(&actor_id),
                payload: serde_json::json!({ "emoji": emoji }),
                before_snapshot: Some(&snap),
                after_snapshot: Some(&snap),
                after_message: Some(&view),
            },
        )
        .await?;
        let kind = if input.remove { "unreact" } else { "react" };
        let _ = sync::enqueue_outbound_for_message_tx(
            &mut tx,
            board_id,
            message_id,
            kind,
            Some(serde_json::json!({ "emoji": emoji })),
        )
        .await?;
        Some(ev.seq)
    } else {
        None
    };
    tx.commit().await?;
    Ok((emoji, event_seq))
}

pub async fn get_posted_by_idempotency_key(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    key: &str,
) -> BoardResult<MessageView> {
    let mut conn = db.read_conn();
    let actor_id = caller.actor_id();
    let row = conn
        .fetch_optional(
            "SELECT message_id FROM idempotency_keys
             WHERE board_id = ?1 AND caller_actor = ?2 AND key = ?3",
            sql_args![board_id, &actor_id, key],
        )
        .await?;
    let Some(r) = row else {
        return Err(BoardError::new(
            ErrorCode::PostNotFound,
            format!("no post found for idempotency key '{key}'"),
        ));
    };
    let msg_id = r.get_str(0)?;
    let stored = load_stored_message_tx(&mut conn, board_id, &msg_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::PostNotFound,
                format!("no post found for idempotency key '{key}'"),
            )
        })?;
    hydrate_message_view_tx(&mut conn, &stored).await
}

pub async fn get_message(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
) -> BoardResult<MessageView> {
    let mut conn = db.read_conn();
    let stored = load_stored_message_tx(&mut conn, board_id, message_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    if !grants::caller_permissions_on_channel(&board_grants, caller, &stored.channel).can_read() {
        return Err(BoardError::new(
            ErrorCode::MessageNotFound,
            format!("message '{message_id}' not found"),
        ));
    }
    hydrate_message_view_tx(&mut conn, &stored).await
}

pub async fn get_thread(
    db: &Database,
    board_id: &str,
    any_message_id: &str,
    caller: &CallerContext,
) -> BoardResult<ThreadView> {
    let mut conn = db.read_conn();
    let target = load_stored_message_tx(&mut conn, board_id, any_message_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{any_message_id}' not found"),
            )
        })?;
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    if !grants::caller_permissions_on_channel(&board_grants, caller, &target.channel).can_read() {
        return Err(BoardError::new(
            ErrorCode::MessageNotFound,
            format!("message '{any_message_id}' not found"),
        ));
    }

    let root_id = target.re.unwrap_or(target.id);
    let root_stored = load_stored_message_tx(&mut conn, board_id, &root_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("thread root '{root_id}' not found"),
            )
        })?;

    let rows = conn
        .fetch_all(
            "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                    by_profile, from_source, no_sync, revision, deleted_at, created_at
             FROM messages
             WHERE board_id = ?1 AND re = ?2
             ORDER BY seq ASC",
            sql_args![board_id, &root_id],
        )
        .await?;
    let mut all_rows = Vec::with_capacity(rows.len() + 1);
    all_rows.push(root_stored);
    for r in &rows {
        all_rows.push(map_stored_row(r)?);
    }

    let mut hydrated = hydrate_message_views_batch_tx(&mut conn, &all_rows).await?;
    let root_view = hydrated.remove(0);
    let replies = hydrated;

    let cursor = current_board_cursor_tx(&mut conn, board_id).await?;
    let pending_schedules =
        schedules::list_pending_thread_schedules_tx(&mut conn, board_id, &root_id)
            .await?
            .into_iter()
            .map(|s| {
                serde_json::to_value(s).map_err(|e| {
                    BoardError::internal(format!("failed to serialize schedule: {e}"))
                })
            })
            .collect::<BoardResult<Vec<Value>>>()?;
    Ok(ThreadView {
        message: root_view,
        replies,
        cursor,
        schedules: pending_schedules,
    })
}

enum ChannelSqlFilter<'a> {
    All,
    Exact(&'a str),
    Subtree {
        exact: &'a str,
        low: String,
        high: String,
    },
}

fn channel_sql_filter(channel: Option<&str>) -> ChannelSqlFilter<'_> {
    match channel {
        None | Some("*") => ChannelSqlFilter::All,
        Some(raw) => {
            if let Some(prefix) = raw.strip_suffix('/') {
                ChannelSqlFilter::Subtree {
                    exact: prefix,
                    low: format!("{prefix}/"),
                    high: format!("{prefix}0"),
                }
            } else {
                ChannelSqlFilter::Exact(raw)
            }
        }
    }
}

fn cached_can_read_channel(
    cache: &mut HashMap<String, bool>,
    board_grants: &[grants::Grant],
    caller: &CallerContext,
    channel: &str,
) -> bool {
    if let Some(&allowed) = cache.get(channel) {
        return allowed;
    }
    let allowed = grants::caller_permissions_on_channel(board_grants, caller, channel).can_read();
    cache.insert(channel.to_string(), allowed);
    allowed
}

/// Evaluates `filter` against `row` while skipping unnecessary DB queries for unused filter axes.
async fn row_matches_filter_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    row: &StoredMessageRow,
    filter: &QueryFilter,
    ctx: &FilterContext,
    handle_cache: &mut HashMap<String, String>,
    root_labels_cache: &mut HashMap<String, BTreeMap<String, String>>,
) -> BoardResult<bool> {
    if !filter.matches_channel(&row.channel) {
        return Ok(false);
    }
    if let Some(re_filter) = &filter.re {
        let root = row.re.as_deref().unwrap_or(&row.id);
        if root != re_filter {
            return Ok(false);
        }
    }
    if let Some(from_filter) = &filter.from {
        if from_filter.eq_ignore_ascii_case("board") {
            if row.from_source.is_some() {
                return Ok(false);
            }
        } else if row.from_source.as_deref() != Some(from_filter.as_str()) {
            return Ok(false);
        }
    }
    let deleted = row.deleted_at.is_some();
    let effective_text = if deleted { "" } else { row.text.as_str() };
    if let Some(needle) = &filter.text {
        let needle_lower = needle.to_ascii_lowercase();
        if !effective_text.to_ascii_lowercase().contains(&needle_lower) {
            return Ok(false);
        }
    }

    let by_handle = if filter.by.is_some() {
        if let Some(h) = handle_cache.get(&row.by_actor) {
            h.clone()
        } else {
            let h = actors::resolve_actor_view(conn, &row.by_actor)
                .await?
                .map(|a| a.handle)
                .unwrap_or_else(|| row.by_actor.clone());
            handle_cache.insert(row.by_actor.clone(), h.clone());
            h
        }
    } else {
        String::new()
    };

    let (msg_labels, root_labels) = if !filter.labels.is_empty() {
        let own = if deleted {
            BTreeMap::new()
        } else {
            labels::load_message_labels_tx(conn, &row.id).await?
        };
        let root_l = match &row.re {
            Some(root_id) => {
                if let Some(cached) = root_labels_cache.get(root_id) {
                    Some(cached.clone())
                } else {
                    let loaded = labels::load_message_labels_tx(conn, root_id).await?;
                    root_labels_cache.insert(root_id.clone(), loaded.clone());
                    Some(loaded)
                }
            }
            None => {
                root_labels_cache
                    .entry(row.id.clone())
                    .or_insert_with(|| own.clone());
                None
            }
        };
        (own, root_l)
    } else {
        (BTreeMap::new(), None)
    };

    let mention_ids = if filter.mentions.is_some() && !deleted {
        load_mention_ids_tx(conn, &row.id).await?
    } else {
        Vec::new()
    };

    let snap = MessageSnapshot {
        id: row.id.clone(),
        channel: row.channel.clone(),
        re: row.re.clone(),
        text: effective_text.to_string(),
        by_actor: row.by_actor.clone(),
        by_principal: row.by_principal.clone(),
        by_handle,
        labels: msg_labels,
        root_labels,
        mention_ids,
        from: row.from_source.clone(),
        deleted,
    };
    Ok(filter.matches_snapshot(&snap, ctx))
}

async fn query_filtered_message_rows_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    filter: &QueryFilter,
    after_seq: Option<i64>,
    before_seq: Option<i64>,
) -> BoardResult<Vec<StoredMessageRow>> {
    let mut sql = String::from(
        "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                by_profile, from_source, no_sync, revision, deleted_at, created_at
         FROM messages
         WHERE board_id = ?1",
    );
    let mut bind: Vec<SqlArg<'_>> = vec![SqlArg::from(board_id)];

    if let Some(re_id) = &filter.re {
        let p = bind.len() + 1;
        let _ = write!(&mut sql, " AND (id = ?{p} OR re = ?{p})");
        bind.push(SqlArg::from(re_id.as_str()));
    } else {
        match channel_sql_filter(filter.channel.as_deref()) {
            ChannelSqlFilter::All => {}
            ChannelSqlFilter::Exact(ch) => {
                let p = bind.len() + 1;
                let _ = write!(&mut sql, " AND channel = ?{p}");
                bind.push(SqlArg::from(ch));
            }
            ChannelSqlFilter::Subtree { exact, low, high } => {
                let p1 = bind.len() + 1;
                let p2 = p1 + 1;
                let p3 = p2 + 1;
                let _ = write!(
                    &mut sql,
                    " AND (channel = ?{p1} OR (channel >= ?{p2} AND channel < ?{p3}))"
                );
                bind.push(SqlArg::from(exact));
                bind.push(SqlArg::from(low));
                bind.push(SqlArg::from(high));
            }
        }
    }

    if let Some(a) = after_seq {
        let p = bind.len() + 1;
        let _ = write!(&mut sql, " AND seq > ?{p}");
        bind.push(SqlArg::from(a));
    }
    if let Some(b) = before_seq {
        let p = bind.len() + 1;
        let _ = write!(&mut sql, " AND seq < ?{p}");
        bind.push(SqlArg::from(b));
    }
    sql.push_str(" ORDER BY seq ASC");

    let rows = conn.fetch_all(&sql, &bind).await?;
    rows.iter().map(map_stored_row).collect()
}

async fn resolve_msg_seq_cursor_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    cursor: &str,
) -> BoardResult<i64> {
    if cursor == "0" {
        return Ok(0);
    }
    if let Some(row) = load_stored_message_tx(conn, board_id, cursor).await? {
        return Ok(row.seq);
    }
    if let Some(ev_seq) = events::parse_event_cursor_seq(cursor) {
        let row = conn
            .fetch_optional(
                "SELECT m.seq
                 FROM events e
                 JOIN messages m ON m.id = e.message_id
                 WHERE e.board_id = ?1 AND e.seq <= ?2 AND e.event_type = 'message.posted'
                 ORDER BY e.seq DESC LIMIT 1",
                sql_args![board_id, ev_seq],
            )
            .await?;
        return match row {
            Some(r) => r.get_i64(0),
            None => Ok(0),
        };
    }
    Err(BoardError::bad_request(format!(
        "invalid cursor or message ID '{cursor}'"
    )))
}

fn validate_limit(limit: Option<usize>) -> BoardResult<usize> {
    let n = limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if n == 0 || n > MAX_PAGE_LIMIT {
        return Err(BoardError::bad_request(format!(
            "limit must be between 1 and {MAX_PAGE_LIMIT}"
        )));
    }
    Ok(n)
}

pub async fn list_messages(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    query: MessageListQuery,
) -> BoardResult<MessagesPage> {
    if query.after.is_some() && query.before.is_some() {
        return Err(BoardError::bad_request(
            "cannot combine 'after' and 'before'",
        ));
    }
    query.filter.validate()?;
    let limit = validate_limit(query.limit)?;

    let mut conn = db.read_conn();
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    if let ChannelSqlFilter::Exact(ch) = channel_sql_filter(query.filter.channel.as_deref()) {
        if !grants::caller_permissions_on_channel(&board_grants, caller, ch).can_read() {
            let cursor = current_board_cursor_tx(&mut conn, board_id).await?;
            return Ok(MessagesPage {
                messages: Vec::new(),
                cursor,
            });
        }
    }
    let visible_actors = if query.filter.mentions.is_some() {
        visible_actors_for_caller_tx(&mut conn, board_id, &board_grants, caller).await?
    } else {
        Vec::new()
    };
    let filter_ctx =
        FilterContext::build(&mut conn, caller, &visible_actors, &query.filter).await?;
    let after_seq = match &query.after {
        Some(c) => Some(resolve_msg_seq_cursor_tx(&mut conn, board_id, c).await?),
        None => None,
    };
    let before_seq = match &query.before {
        Some(c) => Some(resolve_msg_seq_cursor_tx(&mut conn, board_id, c).await?),
        None => None,
    };

    let candidates = query_filtered_message_rows_tx(
        &mut conn,
        board_id,
        &query.filter,
        after_seq,
        before_seq,
    )
    .await?;
    let mut perm_cache: HashMap<String, bool> = HashMap::new();
    let mut handle_cache: HashMap<String, String> = HashMap::new();
    let mut root_labels_cache: HashMap<String, BTreeMap<String, String>> = HashMap::new();
    let mut matched: Vec<StoredMessageRow> = Vec::new();

    for r in candidates {
        if !cached_can_read_channel(&mut perm_cache, &board_grants, caller, &r.channel) {
            continue;
        }
        if !row_matches_filter_tx(
            &mut conn,
            &r,
            &query.filter,
            &filter_ctx,
            &mut handle_cache,
            &mut root_labels_cache,
        )
        .await?
        {
            continue;
        }
        matched.push(r);
        if after_seq.is_some() && matched.len() >= limit {
            break;
        }
    }

    let page_rows: Vec<StoredMessageRow> = if after_seq.is_some() {
        matched.into_iter().take(limit).collect()
    } else {
        let skip = matched.len().saturating_sub(limit);
        matched.into_iter().skip(skip).collect()
    };

    let messages = hydrate_message_views_batch_tx(&mut conn, &page_rows).await?;
    let cursor = current_board_cursor_tx(&mut conn, board_id).await?;
    Ok(MessagesPage { messages, cursor })
}

struct ThreadSummaryRow {
    root_id: String,
    channel: String,
    last_id: String,
    last_reply_id: Option<String>,
    reply_count: usize,
}

struct ThreadPageBounds {
    after_seq: Option<i64>,
    before_seq: Option<i64>,
    limit: usize,
}

async fn list_threads_fast_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    board_grants: &[grants::Grant],
    caller: &CallerContext,
    channel_filter: Option<&str>,
    bounds: ThreadPageBounds,
) -> BoardResult<ThreadsPage> {
    let ThreadPageBounds {
        after_seq,
        before_seq,
        limit,
    } = bounds;
    let ch_sql = channel_sql_filter(channel_filter);
    let exact_verified = match ch_sql {
        ChannelSqlFilter::Exact(ch) => {
            if !grants::caller_permissions_on_channel(board_grants, caller, ch).can_read() {
                let cursor = current_board_cursor_tx(conn, board_id).await?;
                return Ok(ThreadsPage {
                    threads: Vec::new(),
                    cursor,
                    older: None,
                });
            }
            true
        }
        ChannelSqlFilter::All | ChannelSqlFilter::Subtree { .. } => false,
    };

    let mut sql = String::from(
        "SELECT root_id, channel, last_id, last_reply_id, reply_count
         FROM threads
         WHERE board_id = ?1",
    );
    let mut bind: Vec<SqlArg<'_>> = vec![SqlArg::from(board_id)];

    match ch_sql {
        ChannelSqlFilter::All => {}
        ChannelSqlFilter::Exact(ch) => {
            let p = bind.len() + 1;
            let _ = write!(&mut sql, " AND channel = ?{p}");
            bind.push(SqlArg::from(ch));
        }
        ChannelSqlFilter::Subtree { exact, low, high } => {
            let p1 = bind.len() + 1;
            let p2 = p1 + 1;
            let p3 = p2 + 1;
            let _ = write!(
                &mut sql,
                " AND (channel = ?{p1} OR (channel >= ?{p2} AND channel < ?{p3}))"
            );
            bind.push(SqlArg::from(exact));
            bind.push(SqlArg::from(low));
            bind.push(SqlArg::from(high));
        }
    }

    let want_count = if after_seq.is_some() { limit } else { limit + 1 };
    if let Some(a) = after_seq {
        let p = bind.len() + 1;
        let _ = write!(&mut sql, " AND last_seq > ?{p} ORDER BY last_seq ASC");
        bind.push(SqlArg::from(a));
    } else {
        if let Some(b) = before_seq {
            let p = bind.len() + 1;
            let _ = write!(&mut sql, " AND last_seq < ?{p}");
            bind.push(SqlArg::from(b));
        }
        sql.push_str(" ORDER BY last_seq DESC");
    }
    if exact_verified {
        let p = bind.len() + 1;
        let _ = write!(&mut sql, " LIMIT ?{p}");
        bind.push(SqlArg::from(want_count as i64));
    }

    let rows = conn.fetch_all(&sql, &bind).await?;

    let mut perm_cache: HashMap<String, bool> = HashMap::new();
    let mut selected: Vec<ThreadSummaryRow> = Vec::with_capacity(want_count);
    for r in rows {
        let t = ThreadSummaryRow {
            root_id: r.get_str(0)?,
            channel: r.get_str(1)?,
            last_id: r.get_str(2)?,
            last_reply_id: r.get_opt_str(3)?,
            reply_count: r.get_i64(4)? as usize,
        };
        if !exact_verified
            && !cached_can_read_channel(&mut perm_cache, board_grants, caller, &t.channel)
        {
            continue;
        }
        selected.push(t);
        if selected.len() >= want_count {
            break;
        }
    }

    let has_older = if after_seq.is_some() {
        false
    } else {
        let more = selected.len() > limit;
        if more {
            selected.truncate(limit);
        }
        selected.reverse();
        more
    };

    let mut needed_ids: Vec<&str> = Vec::with_capacity(selected.len() * 2);
    for t in &selected {
        needed_ids.push(t.root_id.as_str());
        if let Some(ref rid) = t.last_reply_id {
            needed_ids.push(rid.as_str());
        }
    }

    let stored_map = load_stored_messages_batch_tx(conn, board_id, &needed_ids).await?;
    let mut ordered_rows: Vec<StoredMessageRow> = Vec::with_capacity(needed_ids.len());
    for id in &needed_ids {
        if let Some(r) = stored_map.get(*id) {
            ordered_rows.push(r.clone());
        }
    }
    let hydrated_list = hydrate_message_views_batch_tx(conn, &ordered_rows).await?;
    let mut views_map: HashMap<String, MessageView> = hydrated_list
        .into_iter()
        .map(|v| (v.id.clone(), v))
        .collect();

    let older = if has_older {
        selected.first().map(|t| t.last_id.clone())
    } else {
        None
    };

    let mut threads = Vec::with_capacity(selected.len());
    for t in selected {
        let Some(root_view) = views_map.remove(&t.root_id) else {
            continue;
        };
        let last_view = t.last_reply_id.as_ref().and_then(|rid| views_map.remove(rid));
        threads.push(ThreadSummaryView {
            message: root_view,
            replies: t.reply_count,
            last: last_view,
        });
    }

    let cursor = current_board_cursor_tx(conn, board_id).await?;
    Ok(ThreadsPage {
        threads,
        cursor,
        older,
    })
}

pub async fn list_threads(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    query: MessageListQuery,
) -> BoardResult<ThreadsPage> {
    if query.after.is_some() && query.before.is_some() {
        return Err(BoardError::bad_request(
            "cannot combine 'after' and 'before'",
        ));
    }
    query.filter.validate()?;
    let limit = validate_limit(query.limit)?;

    let mut conn = db.read_conn();
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    let after_seq = match &query.after {
        Some(c) => Some(resolve_msg_seq_cursor_tx(&mut conn, board_id, c).await?),
        None => None,
    };
    let before_seq = match &query.before {
        Some(c) => Some(resolve_msg_seq_cursor_tx(&mut conn, board_id, c).await?),
        None => None,
    };

    // Fast indexed path when only channel/cursor/limit filters are active.
    if !query.filter.has_message_specific_filters() {
        return list_threads_fast_tx(
            &mut conn,
            board_id,
            &board_grants,
            caller,
            query.filter.channel.as_deref(),
            ThreadPageBounds {
                after_seq,
                before_seq,
                limit,
            },
        )
        .await;
    }

    if let ChannelSqlFilter::Exact(ch) = channel_sql_filter(query.filter.channel.as_deref()) {
        if !grants::caller_permissions_on_channel(&board_grants, caller, ch).can_read() {
            let cursor = current_board_cursor_tx(&mut conn, board_id).await?;
            return Ok(ThreadsPage {
                threads: Vec::new(),
                cursor,
                older: None,
            });
        }
    }

    let visible_actors = if query.filter.mentions.is_some() {
        visible_actors_for_caller_tx(&mut conn, board_id, &board_grants, caller).await?
    } else {
        Vec::new()
    };
    let filter_ctx =
        FilterContext::build(&mut conn, caller, &visible_actors, &query.filter).await?;
    let candidates =
        query_filtered_message_rows_tx(&mut conn, board_id, &query.filter, None, None).await?;

    struct ThreadAcc {
        root: Option<StoredMessageRow>,
        replies: usize,
        last: Option<StoredMessageRow>,
        last_seq: i64,
        last_id: String,
        matched: bool,
    }

    let mut perm_cache: HashMap<String, bool> = HashMap::new();
    let mut handle_cache: HashMap<String, String> = HashMap::new();
    let mut root_labels_cache: HashMap<String, BTreeMap<String, String>> = HashMap::new();
    let mut by_root: BTreeMap<String, ThreadAcc> = BTreeMap::new();

    for r in candidates {
        if !cached_can_read_channel(&mut perm_cache, &board_grants, caller, &r.channel) {
            continue;
        }
        if !query.filter.matches_channel(&r.channel) {
            continue;
        }
        let root_id = r.re.clone().unwrap_or_else(|| r.id.clone());
        if let Some(re_f) = &query.filter.re {
            if &root_id != re_f {
                continue;
            }
        }

        let already_matched = by_root.get(&root_id).is_some_and(|a| a.matched);
        let is_match = if already_matched {
            true
        } else {
            row_matches_filter_tx(
                &mut conn,
                &r,
                &query.filter,
                &filter_ctx,
                &mut handle_cache,
                &mut root_labels_cache,
            )
            .await?
        };

        let acc = by_root.entry(root_id).or_insert_with(|| ThreadAcc {
            root: None,
            replies: 0,
            last: None,
            last_seq: r.seq,
            last_id: r.id.clone(),
            matched: false,
        });
        if r.seq >= acc.last_seq {
            acc.last_seq = r.seq;
            acc.last_id = r.id.clone();
        }
        if r.re.is_none() {
            acc.root = Some(r);
        } else {
            acc.replies += 1;
            acc.last = Some(r);
        }
        if is_match {
            acc.matched = true;
        }
    }

    let mut summaries: Vec<ThreadAcc> = by_root
        .into_values()
        .filter(|a| a.matched && a.root.is_some())
        .filter(|a| after_seq.is_none_or(|s| a.last_seq > s))
        .filter(|a| before_seq.is_none_or(|s| a.last_seq < s))
        .collect();

    summaries.sort_by_key(|a| a.last_seq);

    let (page_items, has_older) = if after_seq.is_some() {
        let items: Vec<_> = summaries.into_iter().take(limit).collect();
        (items, false)
    } else {
        let total = summaries.len();
        let skip = total.saturating_sub(limit);
        let items: Vec<_> = summaries.into_iter().skip(skip).collect();
        (items, skip > 0)
    };

    let older = if has_older {
        page_items.first().map(|a| a.last_id.clone())
    } else {
        None
    };

    let mut to_hydrate: Vec<StoredMessageRow> = Vec::with_capacity(page_items.len() * 2);
    let mut page_meta: Vec<(String, Option<String>, usize)> = Vec::with_capacity(page_items.len());
    for a in page_items {
        let root = a.root.unwrap();
        let root_id = root.id.clone();
        to_hydrate.push(root);
        let last_id = a.last.map(|l| {
            let lid = l.id.clone();
            to_hydrate.push(l);
            lid
        });
        page_meta.push((root_id, last_id, a.replies));
    }

    let hydrated = hydrate_message_views_batch_tx(&mut conn, &to_hydrate).await?;
    let mut views_map: HashMap<String, MessageView> = hydrated
        .into_iter()
        .map(|v| (v.id.clone(), v))
        .collect();

    let mut threads = Vec::with_capacity(page_meta.len());
    for (root_id, last_id, replies) in page_meta {
        let Some(root_view) = views_map.remove(&root_id) else {
            continue;
        };
        let last_view = last_id.and_then(|lid| views_map.remove(&lid));
        threads.push(ThreadSummaryView {
            message: root_view,
            replies,
            last: last_view,
        });
    }

    let cursor = current_board_cursor_tx(&mut conn, board_id).await?;
    Ok(ThreadsPage {
        threads,
        cursor,
        older,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_chat_sdk_emojis() {
        assert_eq!(normalize_emoji("+1").unwrap(), "thumbs_up");
        assert_eq!(normalize_emoji(":+1:").unwrap(), "thumbs_up");
        assert_eq!(normalize_emoji("👍").unwrap(), "thumbs_up");
        assert_eq!(normalize_emoji(":eyes:").unwrap(), "eyes");
        assert_eq!(normalize_emoji("👀").unwrap(), "eyes");
        assert_eq!(normalize_emoji("party-parrot").unwrap(), "party_parrot");
        assert!(normalize_emoji("").is_err());
        assert!(normalize_emoji("bad!emoji").is_err());
    }
}
