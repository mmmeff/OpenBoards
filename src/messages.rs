use std::collections::BTreeMap;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::actors::{self, CallerContext};
use crate::attachments;
use crate::db::Database;
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::events::{self, EventAppend, EventType};
use crate::filter::{FilterContext, MessageSnapshot, QueryFilter};
use crate::grants;
use crate::id::new_message_id;
use crate::labels;
use crate::mentions::{self, MentionResolution};
use crate::schedules;
use crate::schemas;
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
        stripped
            .to_ascii_lowercase()
            .replace(['-', ' '], "_")
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

fn next_msg_seq_tx(conn: &Connection, board_id: &str) -> BoardResult<i64> {
    let seq: i64 = conn.query_row(
        "SELECT next_msg_seq FROM boards WHERE id = ?1",
        params![board_id],
        |r| r.get(0),
    )?;
    conn.execute(
        "UPDATE boards SET next_msg_seq = ?1 WHERE id = ?2",
        params![seq + 1, board_id],
    )?;
    Ok(seq)
}

fn current_board_cursor_tx(conn: &Connection, board_id: &str) -> BoardResult<String> {
    events::current_event_cursor_tx(conn, board_id)
}

pub fn load_stored_message_tx(
    conn: &Connection,
    board_id: &str,
    message_id: &str,
) -> BoardResult<Option<StoredMessageRow>> {
    conn.query_row(
        "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                by_profile, from_source, no_sync, revision, deleted_at, created_at
         FROM messages
         WHERE board_id = ?1 AND id = ?2",
        params![board_id, message_id],
        map_stored_row,
    )
    .optional()
    .map_err(Into::into)
}

fn map_stored_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<StoredMessageRow> {
    Ok(StoredMessageRow {
        id: r.get(0)?,
        board_id: r.get(1)?,
        seq: r.get(2)?,
        channel: r.get(3)?,
        re: r.get(4)?,
        reply_to: r.get(5)?,
        text: r.get(6)?,
        by_actor: r.get(7)?,
        by_principal: r.get(8)?,
        by_profile: r.get(9)?,
        from_source: r.get(10)?,
        no_sync: r.get::<_, i64>(11)? != 0,
        revision: r.get(12)?,
        deleted_at: r.get(13)?,
        created_at: r.get(14)?,
    })
}

pub fn hydrate_message_view_tx(conn: &Connection, row: &StoredMessageRow) -> BoardResult<MessageView> {
    let handle = actors::resolve_actor_view(conn, &row.by_actor)?
        .map(|a| a.handle)
        .unwrap_or_else(|| row.by_actor.clone());

    let deleted = row.deleted_at.is_some();

    let (labels, reactions, attachments, blocks) = if deleted {
        (BTreeMap::new(), BTreeMap::new(), Vec::new(), Vec::new())
    } else {
        (
            labels::load_message_labels_tx(conn, &row.id)?,
            load_reactions_tx(conn, &row.id)?,
            attachments::load_message_attachments_tx(conn, &row.id)?,
            schemas::load_message_blocks_tx(conn, &row.id)?,
        )
    };

    let task = if row.re.is_none() {
        tasks::load_task_view_tx(conn, &row.id)?
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

pub fn load_mention_ids_tx(conn: &Connection, message_id: &str) -> BoardResult<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT actor_id FROM message_mentions WHERE message_id = ?1 ORDER BY actor_id ASC",
    )?;
    let rows = stmt.query_map(params![message_id], |r| r.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// Builds an immutable `MessageSnapshot` (including root thread labels and mentions) for filtering
/// and event-log before/after evaluation.
pub fn build_message_snapshot_tx(
    conn: &Connection,
    row: &StoredMessageRow,
) -> BoardResult<MessageSnapshot> {
    let handle = actors::resolve_actor_view(conn, &row.by_actor)?
        .map(|a| a.handle)
        .unwrap_or_else(|| row.by_actor.clone());
    let deleted = row.deleted_at.is_some();
    let msg_labels = if deleted {
        BTreeMap::new()
    } else {
        labels::load_message_labels_tx(conn, &row.id)?
    };
    let root_labels = match &row.re {
        Some(root_id) => Some(labels::load_message_labels_tx(conn, root_id)?),
        None => None,
    };
    let mention_ids = if deleted {
        Vec::new()
    } else {
        load_mention_ids_tx(conn, &row.id)?
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

fn load_reactions_tx(conn: &Connection, message_id: &str) -> BoardResult<BTreeMap<String, Vec<String>>> {
    let mut stmt = conn.prepare(
        "SELECT emoji, actor_id FROM message_reactions WHERE message_id = ?1 ORDER BY emoji ASC, actor_id ASC",
    )?;
    let rows = stmt.query_map(params![message_id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for row in rows {
        let (emoji, actor_id) = row?;
        map.entry(emoji).or_default().push(actor_id);
    }
    Ok(map)
}

fn visible_actors_for_caller_tx(
    conn: &Connection,
    board_id: &str,
    board_grants: &[grants::Grant],
    caller: &CallerContext,
) -> BoardResult<Vec<actors::ActorView>> {
    let has_wildcard_read =
        grants::caller_permissions_at_pattern(board_grants, caller, "*").can_read();
    actors::list_board_actors(conn, board_id, has_wildcard_read, |ch| {
        grants::caller_permissions_on_channel(board_grants, caller, ch).can_read()
    })
}

fn check_idempotency_tx(
    conn: &Connection,
    board_id: &str,
    caller_actor: &str,
    key: &str,
    expected_scope: &str,
    expected_channel: &str,
    expected_root: Option<&str>,
) -> BoardResult<Option<MessageView>> {
    let row: Option<(String, String, Option<String>, String)> = conn
        .query_row(
            "SELECT scope_kind, channel, thread_root, message_id
             FROM idempotency_keys
             WHERE board_id = ?1 AND caller_actor = ?2 AND key = ?3",
            params![board_id, caller_actor, key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;

    let Some((scope, ch, root, msg_id)) = row else {
        // Also reject keys already used for a task batch (`task_batches`) -> 409 duplicate_id
        let used_in_batch: bool = conn
            .query_row(
                "SELECT 1 FROM task_batches
                 WHERE board_id = ?1 AND caller_actor = ?2 AND idempotency_key = ?3",
                params![board_id, caller_actor, key],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false);
        if used_in_batch {
            return Err(BoardError::new(
                ErrorCode::DuplicateId,
                format!("idempotency key '{key}' was already used for a task batch"),
            ));
        }
        return Ok(None);
    };

    if scope != expected_scope || ch != expected_channel || root.as_deref() != expected_root {
        return Err(BoardError::new(
            ErrorCode::DuplicateId,
            format!("idempotency key '{key}' was already used for a different target"),
        ));
    }

    let stored = load_stored_message_tx(conn, board_id, &msg_id)?.ok_or_else(|| {
        BoardError::internal("idempotency key points to missing message")
    })?;
    Ok(Some(hydrate_message_view_tx(conn, &stored)?))
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

fn store_idempotency_tx(conn: &Connection, rec: IdempotencyRecord<'_>) -> BoardResult<()> {
    conn.execute(
        "INSERT INTO idempotency_keys (board_id, caller_actor, key, scope_kind, channel, thread_root, message_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            rec.board_id,
            rec.caller_actor,
            rec.key,
            rec.scope,
            rec.channel,
            rec.thread_root,
            rec.message_id,
            rec.now
        ],
    )?;
    Ok(())
}

fn store_mentions_tx(
    conn: &Connection,
    board_id: &str,
    message_id: &str,
    actor_ids: &[String],
) -> BoardResult<()> {
    conn.execute(
        "DELETE FROM message_mentions WHERE message_id = ?1",
        params![message_id],
    )?;
    for aid in actor_ids {
        conn.execute(
            "INSERT OR IGNORE INTO message_mentions (board_id, message_id, actor_id) VALUES (?1, ?2, ?3)",
            params![board_id, message_id, aid],
        )?;
    }
    Ok(())
}

/// Starts a new thread in `input.channel`.
///
/// - Implicitly creates the channel if it doesn't exist (requires `read` + `post`).
/// - Posting to an existing channel requires only `post` (blind intake).
pub fn post_message(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: PostMessageInput,
) -> BoardResult<PostedMessageResult> {
    db.with_tx(|tx| post_message_in_tx(tx, board_id, caller, input))
}

/// Transaction-scoped version of `post_message` so callers (e.g., `tasks::create_task` and
/// `tasks::create_many_tasks`) can atomically compose message creation with other writes.
pub fn post_message_in_tx(
    tx: &Connection,
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

    let board_grants = grants::load_grants_tx(tx, board_id)?;
    let perms = grants::caller_permissions_on_channel(&board_grants, caller, &input.channel);

    let channel_exists: bool = tx
        .query_row(
            "SELECT 1 FROM channels WHERE board_id = ?1 AND path = ?2",
            params![board_id, input.channel],
            |_| Ok(true),
        )
        .optional()?
        .unwrap_or(false);

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
        if let Some(existing) = check_idempotency_tx(
            tx,
            board_id,
            &actor_id,
            idem_key,
            "post",
            &input.channel,
            None,
        )? {
            return Ok(PostedMessageResult {
                message: existing,
                mentions: Vec::new(),
                replayed: true,
                event_seq: None,
            });
        }
    }

    let parsed_blocks =
        schemas::parse_and_validate_blocks_tx(tx, &input.text, input.schema.as_deref())?;

    let now = Utc::now().to_rfc3339();
    if !channel_exists {
        tx.execute(
            "INSERT INTO channels (board_id, path, created_by, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![board_id, input.channel, actor_id, now],
        )?;
    }

    actors::record_actor_write_tx(tx, board_id, &input.channel, caller)?;

    let visible_actors = visible_actors_for_caller_tx(tx, board_id, &board_grants, caller)?;
    let resolved_mentions = mentions::resolve_mentions(&input.text, &visible_actors);

    let msg_id = new_message_id();
    let seq = next_msg_seq_tx(tx, board_id)?;
    let by_principal = caller.principal_id();
    let by_profile = caller.profile.as_ref().map(|p| p.name_lower.clone());

    tx.execute(
        "INSERT INTO messages (
            id, board_id, seq, channel, re, reply_to, text,
            by_actor, by_principal, by_profile, from_source, no_sync,
            revision, deleted_at, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, ?5, ?6, ?7, ?8, NULL, ?9, 1, NULL, ?10, ?10)",
        params![
            msg_id,
            board_id,
            seq,
            input.channel,
            resolved_mentions.text,
            actor_id,
            by_principal,
            by_profile,
            i64::from(input.no_sync),
            now
        ],
    )?;

    store_mentions_tx(tx, board_id, &msg_id, &resolved_mentions.actor_ids)?;
    if !input.attachments.is_empty() {
        attachments::validate_and_link_attachments_tx(
            tx,
            board_id,
            &board_grants,
            caller,
            &msg_id,
            &input.attachments,
        )?;
    }
    if !input.labels.is_empty() {
        labels::insert_labels_tx(tx, board_id, &msg_id, &input.labels)?;
    }
    if !parsed_blocks.is_empty() {
        schemas::store_message_blocks_tx(tx, board_id, &msg_id, &parsed_blocks)?;
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
        )?;
    }

    let stored = load_stored_message_tx(tx, board_id, &msg_id)?
        .expect("inserted message must exist");
    let snap = build_message_snapshot_tx(tx, &stored)?;
    let view = hydrate_message_view_tx(tx, &stored)?;
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
    )?;
    let _ = sync::enqueue_outbound_for_message_tx(tx, board_id, &msg_id, "post", None)?;
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
pub fn reply_message(
    db: &Database,
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

    db.with_tx(|tx| {
        let target = load_stored_message_tx(tx, board_id, target_message_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{target_message_id}' not found"),
            )
        })?;

        let board_grants = grants::load_grants_tx(tx, board_id)?;
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
            if let Some(existing) = check_idempotency_tx(
                tx,
                board_id,
                &actor_id,
                idem_key,
                "reply",
                &target.channel,
                Some(&root_id),
            )? {
                return Ok(PostedMessageResult {
                    message: existing,
                    mentions: Vec::new(),
                    replayed: true,
                    event_seq: None,
                });
            }
        }

        // Concurrency guard: `after` must still be the thread's latest message
        if let Some(expected_after) = &input.after {
            let latest_id: String = tx.query_row(
                "SELECT id FROM messages
                 WHERE board_id = ?1 AND (id = ?2 OR re = ?2)
                 ORDER BY seq DESC LIMIT 1",
                params![board_id, root_id],
                |r| r.get(0),
            )?;
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
            schemas::parse_and_validate_blocks_tx(tx, &input.text, input.schema.as_deref())?;

        let now = Utc::now().to_rfc3339();
        actors::record_actor_write_tx(tx, board_id, &target.channel, caller)?;

        let visible_actors = visible_actors_for_caller_tx(tx, board_id, &board_grants, caller)?;
        let resolved_mentions = mentions::resolve_mentions(&input.text, &visible_actors);

        let msg_id = new_message_id();
        let seq = next_msg_seq_tx(tx, board_id)?;
        let by_principal = caller.principal_id();
        let by_profile = caller.profile.as_ref().map(|p| p.name_lower.clone());

        tx.execute(
            "INSERT INTO messages (
                id, board_id, seq, channel, re, reply_to, text,
                by_actor, by_principal, by_profile, from_source, no_sync,
                revision, deleted_at, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, NULL, ?11, 1, NULL, ?12, ?12)",
            params![
                msg_id,
                board_id,
                seq,
                target.channel,
                root_id,
                reply_to,
                resolved_mentions.text,
                actor_id,
                by_principal,
                by_profile,
                i64::from(input.no_sync),
                now
            ],
        )?;

        store_mentions_tx(tx, board_id, &msg_id, &resolved_mentions.actor_ids)?;
        if !input.attachments.is_empty() {
            attachments::validate_and_link_attachments_tx(
                tx,
                board_id,
                &board_grants,
                caller,
                &msg_id,
                &input.attachments,
            )?;
        }
        if !input.labels.is_empty() {
            labels::insert_labels_tx(tx, board_id, &msg_id, &input.labels)?;
        }
        if !parsed_blocks.is_empty() {
            schemas::store_message_blocks_tx(tx, board_id, &msg_id, &parsed_blocks)?;
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
            )?;
        }

        let stored = load_stored_message_tx(tx, board_id, &msg_id)?
            .expect("inserted reply must exist");
        let snap = build_message_snapshot_tx(tx, &stored)?;
        let view = hydrate_message_view_tx(tx, &stored)?;
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
        )?;
        let _ = sync::enqueue_outbound_for_message_tx(tx, board_id, &msg_id, "reply", None)?;
        Ok(PostedMessageResult {
            message: view,
            mentions: resolved_mentions.mentions,
            replayed: false,
            event_seq: Some(ev.seq),
        })
    })
}

/// Edits a message's text. Only the author (or bare principal for its profiles) can edit.
pub fn edit_message(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: EditMessageInput,
) -> BoardResult<(MessageView, Vec<MentionResolution>, Vec<i64>)> {
    validate_text_length(&input.text)?;

    db.with_tx(|tx| {
        let msg = load_stored_message_tx(tx, board_id, message_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;

        let board_grants = grants::load_grants_tx(tx, board_id)?;
        let perms = grants::caller_permissions_on_channel(&board_grants, caller, &msg.channel);
        let is_author = caller.owns_actor_item(&msg.by_actor);

        // Blind-intake author with `post` can still edit their own message
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

        if input.text.is_empty() && attachments::load_message_attachments_tx(tx, message_id)?.is_empty() {
            return Err(BoardError::bad_request(
                "edited text cannot be empty unless the message has attachments",
            ));
        }

        let before_snap = build_message_snapshot_tx(tx, &msg)?;
        let parsed_blocks =
            schemas::parse_and_validate_blocks_tx(tx, &input.text, input.schema.as_deref())?;
        let visible_actors = visible_actors_for_caller_tx(tx, board_id, &board_grants, caller)?;
        let resolved_mentions = mentions::resolve_mentions(&input.text, &visible_actors);
        let now = Utc::now().to_rfc3339();
        let next_rev = msg.revision + 1;

        tx.execute(
            "UPDATE messages SET text = ?1, revision = ?2, updated_at = ?3
             WHERE board_id = ?4 AND id = ?5",
            params![resolved_mentions.text, next_rev, now, board_id, message_id],
        )?;
        store_mentions_tx(tx, board_id, message_id, &resolved_mentions.actor_ids)?;
        schemas::store_message_blocks_tx(tx, board_id, message_id, &parsed_blocks)?;

        let updated = load_stored_message_tx(tx, board_id, message_id)?.unwrap();
        let after_snap = build_message_snapshot_tx(tx, &updated)?;
        let view = hydrate_message_view_tx(tx, &updated)?;
        let actor_id = caller.actor_id();
        let ev = events::append_event_tx(
            tx,
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
        )?;
        let mut seqs = vec![ev.seq];
        if msg.re.is_none() {
            if let Some(task_seq) = tasks::on_root_edited_tx(tx, board_id, message_id, caller)? {
                seqs.push(task_seq);
            }
        }
        let _ = sync::enqueue_outbound_for_message_tx(tx, board_id, message_id, "edit", None)?;
        let final_view = hydrate_message_view_tx(tx, &updated)?;
        Ok((final_view, resolved_mentions.mentions, seqs))
    })
}

/// Deletes (tombstones) a message.
///
/// Allowed for author (or bare principal for its profiles) OR team owner with `manage` in channel.
/// Repeat delete on an already-deleted message is an idempotent no-op.
pub fn delete_message(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: DeleteMessageInput,
) -> BoardResult<Vec<i64>> {
    db.with_tx(|tx| {
        let msg = load_stored_message_tx(tx, board_id, message_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;

        let board_grants = grants::load_grants_tx(tx, board_id)?;
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
            return Ok(Vec::new());
        }

        let before_snap = build_message_snapshot_tx(tx, &msg)?;
        let now = Utc::now().to_rfc3339();
        let next_rev = msg.revision + 1;
        tx.execute(
            "UPDATE messages SET text = '', deleted_at = ?1, revision = ?2, updated_at = ?1
             WHERE board_id = ?3 AND id = ?4",
            params![now, next_rev, board_id, message_id],
        )?;
        tx.execute(
            "DELETE FROM message_reactions WHERE message_id = ?1",
            params![message_id],
        )?;
        tx.execute(
            "DELETE FROM message_mentions WHERE message_id = ?1",
            params![message_id],
        )?;
        labels::delete_message_labels_tx(tx, message_id)?;
        schemas::delete_message_blocks_tx(tx, message_id)?;

        let updated = load_stored_message_tx(tx, board_id, message_id)?.unwrap();
        let after_snap = build_message_snapshot_tx(tx, &updated)?;
        let view = hydrate_message_view_tx(tx, &updated)?;
        let actor_id = caller.actor_id();
        let ev = events::append_event_tx(
            tx,
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
        )?;
        let mut seqs = vec![ev.seq];
        if msg.re.is_none() {
            if let Some(task_seq) = tasks::on_root_deleted_tx(tx, board_id, message_id, &actor_id)?
            {
                seqs.push(task_seq);
            }
        }
        let _ = sync::enqueue_outbound_for_message_tx(tx, board_id, message_id, "delete", None)?;

        Ok(seqs)
    })
}

/// Adds or removes a normalized emoji reaction on a message.
pub fn react_message(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: ReactInput,
) -> BoardResult<(String, Option<i64>)> {
    let emoji = normalize_emoji(&input.emoji)?;

    db.with_tx(|tx| {
        let msg = load_stored_message_tx(tx, board_id, message_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;

        let board_grants = grants::load_grants_tx(tx, board_id)?;
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
            let n = tx.execute(
                "DELETE FROM message_reactions WHERE message_id = ?1 AND actor_id = ?2 AND emoji = ?3",
                params![message_id, actor_id, emoji],
            )?;
            n > 0
        } else {
            actors::record_actor_write_tx(tx, board_id, &msg.channel, caller)?;
            let now = Utc::now().to_rfc3339();
            let n = tx.execute(
                "INSERT OR IGNORE INTO message_reactions (board_id, message_id, emoji, actor_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![board_id, message_id, emoji, actor_id, now],
            )?;
            n > 0
        };

        let event_seq = if changed {
            let snap = build_message_snapshot_tx(tx, &msg)?;
            let view = hydrate_message_view_tx(tx, &msg)?;
            let ev_type = if input.remove {
                EventType::ReactionRemoved
            } else {
                EventType::ReactionAdded
            };
            let ev = events::append_event_tx(
                tx,
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
            )?;
            let kind = if input.remove { "unreact" } else { "react" };
            let _ = sync::enqueue_outbound_for_message_tx(
                tx,
                board_id,
                message_id,
                kind,
                Some(serde_json::json!({ "emoji": emoji })),
            )?;
            Some(ev.seq)
        } else {
            None
        };
        Ok((emoji, event_seq))
    })
}

pub fn get_posted_by_idempotency_key(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    key: &str,
) -> BoardResult<MessageView> {
    db.with_conn(|conn| {
        let actor_id = caller.actor_id();
        let msg_id: Option<String> = conn
            .query_row(
                "SELECT message_id FROM idempotency_keys
                 WHERE board_id = ?1 AND caller_actor = ?2 AND key = ?3",
                params![board_id, actor_id, key],
                |r| r.get(0),
            )
            .optional()?;
        let Some(msg_id) = msg_id else {
            return Err(BoardError::new(
                ErrorCode::PostNotFound,
                format!("no post found for idempotency key '{key}'"),
            ));
        };
        let stored = load_stored_message_tx(conn, board_id, &msg_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::PostNotFound,
                format!("no post found for idempotency key '{key}'"),
            )
        })?;
        hydrate_message_view_tx(conn, &stored)
    })
}

pub fn get_message(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
) -> BoardResult<MessageView> {
    db.with_conn(|conn| {
        let stored = load_stored_message_tx(conn, board_id, message_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        if !grants::caller_permissions_on_channel(&board_grants, caller, &stored.channel).can_read()
        {
            return Err(BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            ));
        }
        hydrate_message_view_tx(conn, &stored)
    })
}

pub fn get_thread(
    db: &Database,
    board_id: &str,
    any_message_id: &str,
    caller: &CallerContext,
) -> BoardResult<ThreadView> {
    db.with_conn(|conn| {
        let target = load_stored_message_tx(conn, board_id, any_message_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{any_message_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        if !grants::caller_permissions_on_channel(&board_grants, caller, &target.channel).can_read()
        {
            return Err(BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{any_message_id}' not found"),
            ));
        }

        let root_id = target.re.unwrap_or(target.id);
        let root_stored = load_stored_message_tx(conn, board_id, &root_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("thread root '{root_id}' not found"),
            )
        })?;
        let root_view = hydrate_message_view_tx(conn, &root_stored)?;

        let mut stmt = conn.prepare(
            "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                    by_profile, from_source, no_sync, revision, deleted_at, created_at
             FROM messages
             WHERE board_id = ?1 AND re = ?2
             ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![board_id, root_id], map_stored_row)?;
        let mut replies = Vec::new();
        for row in rows {
            let r = row?;
            replies.push(hydrate_message_view_tx(conn, &r)?);
        }

        let cursor = current_board_cursor_tx(conn, board_id)?;
        let pending_schedules = schedules::list_pending_thread_schedules_tx(conn, board_id, &root_id)?
            .into_iter()
            .map(|s| {
                serde_json::to_value(s)
                    .map_err(|e| BoardError::internal(format!("failed to serialize schedule: {e}")))
            })
            .collect::<BoardResult<Vec<Value>>>()?;
        Ok(ThreadView {
            message: root_view,
            replies,
            cursor,
            schedules: pending_schedules,
        })
    })
}

fn resolve_msg_seq_cursor_tx(conn: &Connection, board_id: &str, cursor: &str) -> BoardResult<i64> {
    if cursor == "0" {
        return Ok(0);
    }
    if let Some(row) = load_stored_message_tx(conn, board_id, cursor)? {
        return Ok(row.seq);
    }
    if let Some(ev_seq) = events::parse_event_cursor_seq(cursor) {
        // Resolve event cursor to the highest message seq posted at or before `ev_seq`
        let msg_seq: Option<i64> = conn
            .query_row(
                "SELECT m.seq
                 FROM events e
                 JOIN messages m ON m.id = e.message_id
                 WHERE e.board_id = ?1 AND e.seq <= ?2 AND e.event_type = 'message.posted'
                 ORDER BY e.seq DESC LIMIT 1",
                params![board_id, ev_seq],
                |r| r.get(0),
            )
            .optional()?;
        return Ok(msg_seq.unwrap_or(0));
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

pub fn list_messages(
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

    db.with_conn(|conn| {
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        let visible_actors = visible_actors_for_caller_tx(conn, board_id, &board_grants, caller)?;
        let filter_ctx = FilterContext::build(conn, caller, &visible_actors, &query.filter)?;
        let after_seq = match &query.after {
            Some(c) => Some(resolve_msg_seq_cursor_tx(conn, board_id, c)?),
            None => None,
        };
        let before_seq = match &query.before {
            Some(c) => Some(resolve_msg_seq_cursor_tx(conn, board_id, c)?),
            None => None,
        };

        let mut stmt = conn.prepare(
            "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                    by_profile, from_source, no_sync, revision, deleted_at, created_at
             FROM messages
             WHERE board_id = ?1
             ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![board_id], map_stored_row)?;
        let mut matched: Vec<(i64, MessageView)> = Vec::new();

        for row in rows {
            let r = row?;
            if let Some(a) = after_seq {
                if r.seq <= a {
                    continue;
                }
            }
            if let Some(b) = before_seq {
                if r.seq >= b {
                    continue;
                }
            }
            if !grants::caller_permissions_on_channel(&board_grants, caller, &r.channel).can_read()
            {
                continue;
            }
            let snap = build_message_snapshot_tx(conn, &r)?;
            if !query.filter.matches_snapshot(&snap, &filter_ctx) {
                continue;
            }
            let view = hydrate_message_view_tx(conn, &r)?;
            matched.push((r.seq, view));
        }

        let messages: Vec<MessageView> = if after_seq.is_some() {
            matched.into_iter().take(limit).map(|(_, v)| v).collect()
        } else {
            let skip = matched.len().saturating_sub(limit);
            matched.into_iter().skip(skip).map(|(_, v)| v).collect()
        };

        let cursor = current_board_cursor_tx(conn, board_id)?;
        Ok(MessagesPage { messages, cursor })
    })
}

pub fn list_threads(
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

    db.with_conn(|conn| {
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        let visible_actors = visible_actors_for_caller_tx(conn, board_id, &board_grants, caller)?;
        let filter_ctx = FilterContext::build(conn, caller, &visible_actors, &query.filter)?;
        let after_seq = match &query.after {
            Some(c) => Some(resolve_msg_seq_cursor_tx(conn, board_id, c)?),
            None => None,
        };
        let before_seq = match &query.before {
            Some(c) => Some(resolve_msg_seq_cursor_tx(conn, board_id, c)?),
            None => None,
        };

        let mut stmt = conn.prepare(
            "SELECT id, board_id, seq, channel, re, reply_to, text, by_actor, by_principal,
                    by_profile, from_source, no_sync, revision, deleted_at, created_at
             FROM messages
             WHERE board_id = ?1
             ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![board_id], map_stored_row)?;

        struct ThreadAcc {
            root: Option<MessageView>,
            replies: usize,
            last: Option<MessageView>,
            last_seq: i64,
            last_id: String,
            matched: bool,
        }

        let mut by_root: BTreeMap<String, ThreadAcc> = BTreeMap::new();
        for row in rows {
            let r = row?;
            if !grants::caller_permissions_on_channel(&board_grants, caller, &r.channel).can_read()
            {
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

            let snap = build_message_snapshot_tx(conn, &r)?;
            let is_match = query.filter.matches_snapshot(&snap, &filter_ctx);
            let view = hydrate_message_view_tx(conn, &r)?;

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
                acc.root = Some(view);
            } else {
                acc.replies += 1;
                acc.last = Some(view);
            }
            if is_match {
                acc.matched = true;
            }
        }

        let mut summaries: Vec<(i64, String, ThreadSummaryView)> = by_root
            .into_values()
            .filter(|a| a.matched && a.root.is_some())
            .filter(|a| after_seq.is_none_or(|s| a.last_seq > s))
            .filter(|a| before_seq.is_none_or(|s| a.last_seq < s))
            .map(|a| {
                (
                    a.last_seq,
                    a.last_id,
                    ThreadSummaryView {
                        message: a.root.unwrap(),
                        replies: a.replies,
                        last: a.last,
                    },
                )
            })
            .collect();

        // Newest activity last
        summaries.sort_by_key(|(seq, _, _)| *seq);

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
            page_items.first().map(|(_, id, _)| id.clone())
        } else {
            None
        };

        let threads = page_items.into_iter().map(|(_, _, t)| t).collect();
        let cursor = current_board_cursor_tx(conn, board_id)?;
        Ok(ThreadsPage {
            threads,
            cursor,
            older,
        })
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
