use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::actors::{self, ActorView, CallerContext};
use crate::db::Database;
use crate::error::{BoardError, BoardResult};
use crate::filter::{FilterContext, MessageSnapshot, QueryFilter};
use crate::grants;
use crate::messages::{MessageView, DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT};

pub const MAX_POLL_WAIT_SECS: f64 = 300.0;
pub const DEFAULT_WAIT_TIMEOUT_SECS: f64 = 1800.0;
pub const MAX_WAIT_TIMEOUT_SECS: f64 = 1800.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventType {
    // Messages
    #[serde(rename = "message.posted")]
    MessagePosted,
    #[serde(rename = "message.edited")]
    MessageEdited,
    #[serde(rename = "message.deleted")]
    MessageDeleted,
    // Reactions, Labels, Channels
    #[serde(rename = "reaction.added")]
    ReactionAdded,
    #[serde(rename = "reaction.removed")]
    ReactionRemoved,
    #[serde(rename = "labels.changed")]
    LabelsChanged,
    #[serde(rename = "channel.created")]
    ChannelCreated,
    // Tasks (Wave 3)
    #[serde(rename = "task.created")]
    TaskCreated,
    #[serde(rename = "task.ready")]
    TaskReady,
    #[serde(rename = "task.claimed")]
    TaskClaimed,
    #[serde(rename = "task.released")]
    TaskReleased,
    #[serde(rename = "task.completed")]
    TaskCompleted,
    #[serde(rename = "task.failed")]
    TaskFailed,
    #[serde(rename = "task.blocked")]
    TaskBlocked,
    #[serde(rename = "task.unblocked")]
    TaskUnblocked,
    #[serde(rename = "task.canceled")]
    TaskCanceled,
    #[serde(rename = "task.reopened")]
    TaskReopened,
    #[serde(rename = "task.assigned")]
    TaskAssigned,
    #[serde(rename = "task.needs_changed")]
    TaskNeedsChanged,
    #[serde(rename = "task.expired")]
    TaskExpired,
    #[serde(rename = "task.renewed")]
    TaskRenewed,
    // Sync (Wave 4)
    #[serde(rename = "thread.synced")]
    ThreadSynced,
    #[serde(rename = "message.synced")]
    MessageSynced,
    #[serde(rename = "message.files_synced")]
    MessageFilesSynced,
    #[serde(rename = "message.sync_failed")]
    MessageSyncFailed,
}

impl EventType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MessagePosted => "message.posted",
            Self::MessageEdited => "message.edited",
            Self::MessageDeleted => "message.deleted",
            Self::ReactionAdded => "reaction.added",
            Self::ReactionRemoved => "reaction.removed",
            Self::LabelsChanged => "labels.changed",
            Self::ChannelCreated => "channel.created",
            Self::TaskCreated => "task.created",
            Self::TaskReady => "task.ready",
            Self::TaskClaimed => "task.claimed",
            Self::TaskReleased => "task.released",
            Self::TaskCompleted => "task.completed",
            Self::TaskFailed => "task.failed",
            Self::TaskBlocked => "task.blocked",
            Self::TaskUnblocked => "task.unblocked",
            Self::TaskCanceled => "task.canceled",
            Self::TaskReopened => "task.reopened",
            Self::TaskAssigned => "task.assigned",
            Self::TaskNeedsChanged => "task.needs_changed",
            Self::TaskExpired => "task.expired",
            Self::TaskRenewed => "task.renewed",
            Self::ThreadSynced => "thread.synced",
            Self::MessageSynced => "message.synced",
            Self::MessageFilesSynced => "message.files_synced",
            Self::MessageSyncFailed => "message.sync_failed",
        }
    }

    pub fn parse(s: &str) -> BoardResult<Self> {
        match s.trim() {
            "message.posted" => Ok(Self::MessagePosted),
            "message.edited" => Ok(Self::MessageEdited),
            "message.deleted" => Ok(Self::MessageDeleted),
            "reaction.added" => Ok(Self::ReactionAdded),
            "reaction.removed" => Ok(Self::ReactionRemoved),
            "labels.changed" => Ok(Self::LabelsChanged),
            "channel.created" => Ok(Self::ChannelCreated),
            "task.created" => Ok(Self::TaskCreated),
            "task.ready" => Ok(Self::TaskReady),
            "task.claimed" => Ok(Self::TaskClaimed),
            "task.released" => Ok(Self::TaskReleased),
            "task.completed" => Ok(Self::TaskCompleted),
            "task.failed" => Ok(Self::TaskFailed),
            "task.blocked" => Ok(Self::TaskBlocked),
            "task.unblocked" => Ok(Self::TaskUnblocked),
            "task.canceled" => Ok(Self::TaskCanceled),
            "task.reopened" => Ok(Self::TaskReopened),
            "task.assigned" => Ok(Self::TaskAssigned),
            "task.needs_changed" => Ok(Self::TaskNeedsChanged),
            "task.expired" => Ok(Self::TaskExpired),
            "task.renewed" => Ok(Self::TaskRenewed),
            "thread.synced" => Ok(Self::ThreadSynced),
            "message.synced" => Ok(Self::MessageSynced),
            "message.files_synced" => Ok(Self::MessageFilesSynced),
            "message.sync_failed" => Ok(Self::MessageSyncFailed),
            other => Err(BoardError::bad_request(format!(
                "unknown event type '{other}'"
            ))),
        }
    }

    /// Bookkeeping events are hidden from `poll`/`tail`/`wait` unless `all=true`.
    pub const fn is_bookkeeping(self) -> bool {
        matches!(
            self,
            Self::TaskRenewed | Self::MessageSynced | Self::MessageFilesSynced
        )
    }
}

pub fn format_event_cursor(seq: i64) -> String {
    if seq <= 0 {
        "0".to_string()
    } else {
        format!("ev_{seq}")
    }
}

pub fn parse_event_cursor_seq(cursor: &str) -> Option<i64> {
    if cursor == "0" {
        return Some(0);
    }
    let rest = cursor.strip_prefix("ev_")?;
    let n: i64 = rest.parse().ok()?;
    (n >= 0).then_some(n)
}

/// Resolves an `after` cursor (either `"0"`, `"ev_<seq>"`, or a `"msg_..."` ID) to its board event `seq`.
pub fn resolve_event_cursor_tx(conn: &Connection, board_id: &str, cursor: &str) -> BoardResult<i64> {
    if let Some(seq) = parse_event_cursor_seq(cursor) {
        return Ok(seq);
    }
    if cursor.starts_with("msg_") {
        let seq: Option<i64> = conn
            .query_row(
                "SELECT seq FROM events
                 WHERE board_id = ?1 AND message_id = ?2 AND event_type = 'message.posted'
                 ORDER BY seq ASC LIMIT 1",
                params![board_id, cursor],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(s) = seq {
            return Ok(s);
        }
        return Err(BoardError::bad_request(format!(
            "message cursor '{cursor}' not found on this board"
        )));
    }
    Err(BoardError::bad_request(format!(
        "invalid event cursor '{cursor}'"
    )))
}

pub fn current_event_seq_tx(conn: &Connection, board_id: &str) -> BoardResult<i64> {
    let seq: i64 = conn.query_row(
        "SELECT COALESCE(MAX(seq), 0) FROM events WHERE board_id = ?1",
        params![board_id],
        |r| r.get(0),
    )?;
    Ok(seq)
}

pub fn current_event_cursor_tx(conn: &Connection, board_id: &str) -> BoardResult<String> {
    Ok(format_event_cursor(current_event_seq_tx(conn, board_id)?))
}

#[derive(Debug, Clone)]
pub struct EventAppend<'a> {
    pub board_id: &'a str,
    pub event_type: EventType,
    pub channel: Option<&'a str>,
    pub message_id: Option<&'a str>,
    pub actor_id: Option<&'a str>,
    pub payload: Value,
    pub before_snapshot: Option<&'a MessageSnapshot>,
    pub after_snapshot: Option<&'a MessageSnapshot>,
    pub after_message: Option<&'a MessageView>,
}

#[derive(Debug, Clone)]
pub struct AppendedEvent {
    pub seq: i64,
    pub cursor: String,
}

/// Atomically appends an event to the board's single total-order event log inside an active tx.
pub fn append_event_tx(conn: &Connection, input: EventAppend<'_>) -> BoardResult<AppendedEvent> {
    let seq: i64 = conn.query_row(
        "SELECT next_event_seq FROM boards WHERE id = ?1",
        params![input.board_id],
        |r| r.get(0),
    )?;
    conn.execute(
        "UPDATE boards SET next_event_seq = ?1 WHERE id = ?2",
        params![seq + 1, input.board_id],
    )?;

    let cursor = format_event_cursor(seq);
    let now = Utc::now().to_rfc3339();
    let payload_json = serde_json::to_string(&input.payload)
        .map_err(|e| BoardError::internal(format!("failed to serialize event payload: {e}")))?;
    let before_json = input
        .before_snapshot
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| BoardError::internal(format!("failed to serialize before snapshot: {e}")))?;
    let after_snap_json = input
        .after_snapshot
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| BoardError::internal(format!("failed to serialize after snapshot: {e}")))?;
    let after_msg_json = input
        .after_message
        .map(serde_json::to_string)
        .transpose()
        .map_err(|e| BoardError::internal(format!("failed to serialize after message: {e}")))?;

    conn.execute(
        "INSERT INTO events (
            board_id, seq, cursor, event_type, channel, message_id, actor_id,
            bookkeeping, payload_json, before_snapshot_json, after_snapshot_json,
            after_message_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            input.board_id,
            seq,
            cursor,
            input.event_type.as_str(),
            input.channel,
            input.message_id,
            input.actor_id,
            i64::from(input.event_type.is_bookkeeping()),
            payload_json,
            before_json,
            after_snap_json,
            after_msg_json,
            now
        ],
    )?;

    Ok(AppendedEvent { seq, cursor })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventView {
    pub cursor: String,
    #[serde(rename = "type")]
    pub event_type: EventType,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<MessageView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<ActorView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsPage {
    pub events: Vec<EventView>,
    pub cursor: String,
}

#[derive(Debug, Default, Clone)]
pub struct EventQuery {
    pub filter: QueryFilter,
    pub after: Option<String>,
    pub limit: Option<usize>,
    pub all: bool,
    pub types: Vec<EventType>,
}

/// Per-board Tokio broadcast hub waking long-polling `events` and `wait` requests immediately.
#[derive(Clone, Default)]
pub struct EventHub {
    channels: Arc<RwLock<HashMap<String, broadcast::Sender<i64>>>>,
}

impl EventHub {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self, board_id: &str) -> broadcast::Receiver<i64> {
        {
            let guard = self.channels.read().expect("EventHub lock poisoned");
            if let Some(tx) = guard.get(board_id) {
                return tx.subscribe();
            }
        }
        let mut guard = self.channels.write().expect("EventHub lock poisoned");
        let tx = guard
            .entry(board_id.to_string())
            .or_insert_with(|| broadcast::channel(1024).0);
        tx.subscribe()
    }

    pub fn notify(&self, board_id: &str, seq: i64) {
        let guard = self.channels.read().expect("EventHub lock poisoned");
        if let Some(tx) = guard.get(board_id) {
            let _ = tx.send(seq);
        }
    }
}

/// Fetches matching events strictly after `after_seq` (with visibility & before-or-after filtering).
pub fn query_events_after(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    after_seq: i64,
    query: &EventQuery,
) -> BoardResult<EventsPage> {
    query.filter.validate()?;
    let limit = query.limit.unwrap_or(DEFAULT_PAGE_LIMIT);
    if limit == 0 || limit > MAX_PAGE_LIMIT {
        return Err(BoardError::bad_request(format!(
            "limit must be between 1 and {MAX_PAGE_LIMIT}"
        )));
    }

    db.with_conn(|conn| {
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        let can_read_all = grants::caller_can_read_all_channels(&board_grants, caller);
        let has_wildcard_read =
            grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_read();
        let visible_actors = actors::list_board_actors(conn, board_id, has_wildcard_read, |ch| {
            grants::caller_permissions_on_channel(&board_grants, caller, ch).can_read()
        })?;
        let filter_ctx = FilterContext::build(conn, caller, &visible_actors, &query.filter)?;

        let mut stmt = conn.prepare(
            "SELECT seq, cursor, event_type, channel, message_id, actor_id, bookkeeping,
                    payload_json, before_snapshot_json, after_snapshot_json, after_message_json,
                    created_at
             FROM events
             WHERE board_id = ?1 AND seq > ?2
             ORDER BY seq ASC",
        )?;

        #[allow(clippy::type_complexity)]
        let rows = stmt.query_map(params![board_id, after_seq], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, i64>(6)? != 0,
                r.get::<_, String>(7)?,
                r.get::<_, Option<String>>(8)?,
                r.get::<_, Option<String>>(9)?,
                r.get::<_, Option<String>>(10)?,
                r.get::<_, String>(11)?,
            ))
        })?;

        let mut matched: Vec<EventView> = Vec::new();
        let mut last_scanned_seq = after_seq;

        for row in rows {
            let (
                seq,
                cursor,
                ev_type_str,
                channel,
                message_id,
                actor_id,
                bookkeeping,
                payload_json,
                before_json,
                after_snap_json,
                after_msg_json,
                created_at,
            ) = row?;

            last_scanned_seq = seq;

            if bookkeeping && !query.all {
                continue;
            }
            let event_type = EventType::parse(&ev_type_str)?;
            if !query.types.is_empty() && !query.types.contains(&event_type) {
                continue;
            }

            if message_id.is_none() {
                // Message-less events (e.g. channel.created) require `*` read across every channel.
                if !can_read_all {
                    continue;
                }
                if query.filter.has_message_specific_filters() {
                    continue;
                }
                if let Some(ch) = &channel {
                    if !query.filter.matches_channel(ch) {
                        continue;
                    }
                }
            } else {
                let ch = channel.as_deref().unwrap_or("");
                if !grants::caller_permissions_on_channel(&board_grants, caller, ch).can_read() {
                    continue;
                }
                let before_snap: Option<MessageSnapshot> = before_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| BoardError::internal(format!("corrupted before snapshot: {e}")))?;
                let after_snap: Option<MessageSnapshot> = after_snap_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|e| BoardError::internal(format!("corrupted after snapshot: {e}")))?;

                let matches_before = before_snap
                    .as_ref()
                    .is_some_and(|b| query.filter.matches_snapshot(b, &filter_ctx));
                let matches_after = after_snap
                    .as_ref()
                    .is_some_and(|a| query.filter.matches_snapshot(a, &filter_ctx));

                if !matches_before && !matches_after {
                    continue;
                }
            }

            let payload: Value = serde_json::from_str(&payload_json)
                .map_err(|e| BoardError::internal(format!("corrupted event payload: {e}")))?;
            let message: Option<MessageView> = after_msg_json
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|e| BoardError::internal(format!("corrupted event message: {e}")))?;
            let actor = match &actor_id {
                Some(aid) => actors::resolve_actor_view(conn, aid)?,
                None => None,
            };

            matched.push(EventView {
                cursor,
                event_type,
                created_at,
                payload,
                message,
                actor,
            });

            if matched.len() >= limit {
                break;
            }
        }

        let cursor = if let Some(last) = matched.last() {
            last.cursor.clone()
        } else {
            let latest = current_event_seq_tx(conn, board_id)?;
            format_event_cursor(latest.max(last_scanned_seq))
        };

        Ok(EventsPage {
            events: matched,
            cursor,
        })
    })
}

/// Parses a duration string (`200ms`, `30s`, `10m`, `1h`) or plain numeric seconds into a `Duration`.
pub fn parse_duration_param(raw: &str, max_secs: f64) -> BoardResult<Duration> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(BoardError::bad_request("duration cannot be empty"));
    }
    let secs = if let Some(ms) = trimmed.strip_suffix("ms") {
        let v: f64 = ms
            .parse()
            .map_err(|_| BoardError::bad_request(format!("invalid duration '{raw}'")))?;
        v / 1000.0
    } else if let Some(s) = trimmed.strip_suffix('s') {
        s.parse::<f64>()
            .map_err(|_| BoardError::bad_request(format!("invalid duration '{raw}'")))?
    } else if let Some(m) = trimmed.strip_suffix('m') {
        let v: f64 = m
            .parse()
            .map_err(|_| BoardError::bad_request(format!("invalid duration '{raw}'")))?;
        v * 60.0
    } else if let Some(h) = trimmed.strip_suffix('h') {
        let v: f64 = h
            .parse()
            .map_err(|_| BoardError::bad_request(format!("invalid duration '{raw}'")))?;
        v * 3600.0
    } else {
        trimmed
            .parse::<f64>()
            .map_err(|_| BoardError::bad_request(format!("invalid duration '{raw}'")))?
    };

    if !secs.is_finite() || secs < 0.0 || secs > max_secs {
        return Err(BoardError::bad_request(format!(
            "duration must be between 0 and {max_secs} seconds"
        )));
    }
    Ok(Duration::from_secs_f64(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_event_types_and_bookkeeping_flags() {
        assert!(!EventType::MessagePosted.is_bookkeeping());
        assert!(!EventType::LabelsChanged.is_bookkeeping());
        assert!(!EventType::TaskCreated.is_bookkeeping());
        assert!(EventType::TaskRenewed.is_bookkeeping());
        assert!(EventType::MessageSynced.is_bookkeeping());
        assert!(EventType::MessageFilesSynced.is_bookkeeping());
        assert!(!EventType::MessageSyncFailed.is_bookkeeping());

        assert_eq!(
            EventType::parse("labels.changed").unwrap(),
            EventType::LabelsChanged
        );
        assert!(EventType::parse("unknown.event").is_err());
    }

    #[test]
    fn parses_durations_and_cursors() {
        assert_eq!(
            parse_duration_param("250ms", 300.0).unwrap(),
            Duration::from_millis(250)
        );
        assert_eq!(
            parse_duration_param("30s", 300.0).unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_duration_param("5m", 1800.0).unwrap(),
            Duration::from_secs(300)
        );
        assert_eq!(
            parse_duration_param("15", 300.0).unwrap(),
            Duration::from_secs(15)
        );
        assert!(parse_duration_param("301s", 300.0).is_err());

        assert_eq!(format_event_cursor(0), "0");
        assert_eq!(format_event_cursor(42), "ev_42");
        assert_eq!(parse_event_cursor_seq("ev_42"), Some(42));
        assert_eq!(parse_event_cursor_seq("0"), Some(0));
        assert_eq!(parse_event_cursor_seq("bad"), None);
    }
}
