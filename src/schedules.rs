use std::collections::BTreeMap;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::actors::{self, CallerContext, ProfileOverlay};
use crate::attachments;
use crate::db::Database;
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::events::EventHub;
use crate::grants;
use crate::id::new_schedule_id;
use crate::labels;
use crate::messages::{self, MessageAuthor, PostMessageInput, ReplyMessageInput};
use crate::schemas;
use crate::validate::{validate_channel_path, validate_channel_pattern, validate_text_length};
use crate::AppState;

pub const MIN_REPEAT_INTERVAL_SECS: i64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleStatus {
    Pending,
    Completed,
    Canceled,
    Failed,
}

impl ScheduleStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Completed => "completed",
            Self::Canceled => "canceled",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "completed" => Self::Completed,
            "canceled" => Self::Canceled,
            "failed" => Self::Failed,
            _ => Self::Failed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleView {
    pub id: String,
    pub channel: String,
    pub re: Option<String>,
    pub text: String,
    pub labels: BTreeMap<String, String>,
    pub attachments: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    #[serde(rename = "noSync")]
    pub no_sync: bool,
    pub by: MessageAuthor,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub every: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub times: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    #[serde(rename = "nextRunAt")]
    pub next_run_at: String,
    #[serde(rename = "runsCompleted")]
    pub runs_completed: i64,
    #[serde(rename = "lastRunAt", skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<String>,
    #[serde(rename = "lastMessageId", skip_serializing_if = "Option::is_none")]
    pub last_message_id: Option<String>,
    pub status: ScheduleStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateScheduleInput {
    pub channel: Option<String>,
    pub re: Option<String>,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub attachments: Vec<String>,
    pub schema: Option<String>,
    #[serde(default, alias = "noSync")]
    pub no_sync: bool,
    #[serde(rename = "in")]
    pub delay_in: Option<String>,
    pub at: Option<String>,
    pub every: Option<String>,
    #[serde(rename = "for")]
    pub repeat_for: Option<String>,
    pub times: Option<i64>,
    pub until: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct ScheduleListQuery {
    pub re: Option<String>,
    pub channel: Option<String>,
    pub by: Option<String>,
    pub all: bool,
}

#[derive(Debug, Clone)]
struct StoredScheduleRow {
    id: String,
    board_id: String,
    channel: String,
    re: Option<String>,
    text: String,
    labels_json: String,
    attachments_json: String,
    schema_id: Option<String>,
    no_sync: bool,
    creator_actor: String,
    creator_principal: String,
    creator_profile: Option<String>,
    creator_avatar: Option<String>,
    every_secs: Option<i64>,
    max_times: Option<i64>,
    until_at: Option<String>,
    next_run_at: String,
    runs_completed: i64,
    last_run_at: Option<String>,
    last_message_id: Option<String>,
    status: ScheduleStatus,
    error_message: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone, Default)]
pub struct FireSummary {
    pub fired: usize,
    pub failed: usize,
    pub posted_message_ids: Vec<String>,
}

pub fn format_utc_ms(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

pub fn parse_rfc3339_utc(raw: &str, field: &str) -> BoardResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw.trim())
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| BoardError::bad_request(format!("invalid RFC 3339 timestamp for '{field}': {e}")))
}

/// Parses a positive duration string (`30s`, `5m`, `12h`, `1d`, or plain integer seconds) into seconds.
pub fn parse_schedule_duration_secs(raw: &str, field: &str) -> BoardResult<i64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(BoardError::bad_request(format!(
            "duration for '{field}' cannot be empty"
        )));
    }
    let (num_str, mult): (&str, i64) = if let Some(s) = trimmed.strip_suffix('s') {
        (s, 1)
    } else if let Some(m) = trimmed.strip_suffix('m') {
        (m, 60)
    } else if let Some(h) = trimmed.strip_suffix('h') {
        (h, 3600)
    } else if let Some(d) = trimmed.strip_suffix('d') {
        (d, 86_400)
    } else {
        (trimmed, 1)
    };

    let value: i64 = num_str.parse().map_err(|_| {
        BoardError::bad_request(format!(
            "invalid duration '{raw}' for '{field}'; expected e.g. '30s', '5m', '12h', '1d'"
        ))
    })?;
    if value <= 0 {
        return Err(BoardError::bad_request(format!(
            "duration for '{field}' must be positive"
        )));
    }
    value
        .checked_mul(mult)
        .ok_or_else(|| BoardError::bad_request(format!("duration '{raw}' for '{field}' overflows")))
}

fn map_schedule_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<StoredScheduleRow> {
    let status_str: String = r.get(20)?;
    Ok(StoredScheduleRow {
        id: r.get(0)?,
        board_id: r.get(1)?,
        channel: r.get(2)?,
        re: r.get(3)?,
        text: r.get(4)?,
        labels_json: r.get(5)?,
        attachments_json: r.get(6)?,
        schema_id: r.get(7)?,
        no_sync: r.get::<_, i64>(8)? != 0,
        creator_actor: r.get(9)?,
        creator_principal: r.get(10)?,
        creator_profile: r.get(11)?,
        creator_avatar: r.get(12)?,
        every_secs: r.get(13)?,
        max_times: r.get(14)?,
        until_at: r.get(15)?,
        next_run_at: r.get(16)?,
        runs_completed: r.get(17)?,
        last_run_at: r.get(18)?,
        last_message_id: r.get(19)?,
        status: ScheduleStatus::parse(&status_str),
        error_message: r.get(21)?,
        created_at: r.get(22)?,
    })
}

fn hydrate_schedule_view_tx(
    conn: &Connection,
    row: &StoredScheduleRow,
) -> BoardResult<ScheduleView> {
    let handle = actors::resolve_actor_view(conn, &row.creator_actor)?
        .map(|a| a.handle)
        .unwrap_or_else(|| row.creator_actor.clone());
    let labels: BTreeMap<String, String> = serde_json::from_str(&row.labels_json)
        .map_err(|e| BoardError::internal(format!("corrupted schedule labels JSON: {e}")))?;
    let attachments: Vec<String> = serde_json::from_str(&row.attachments_json)
        .map_err(|e| BoardError::internal(format!("corrupted schedule attachments JSON: {e}")))?;

    Ok(ScheduleView {
        id: row.id.clone(),
        channel: row.channel.clone(),
        re: row.re.clone(),
        text: row.text.clone(),
        labels,
        attachments,
        schema: row.schema_id.clone(),
        no_sync: row.no_sync,
        by: MessageAuthor {
            actor: row.creator_actor.clone(),
            principal: row.creator_principal.clone(),
            profile: row.creator_profile.clone(),
            handle,
        },
        every: row.every_secs,
        times: row.max_times,
        until: row.until_at.clone(),
        next_run_at: row.next_run_at.clone(),
        runs_completed: row.runs_completed,
        last_run_at: row.last_run_at.clone(),
        last_message_id: row.last_message_id.clone(),
        status: row.status,
        error: row.error_message.clone(),
        created_at: row.created_at.clone(),
    })
}

fn load_schedule_row_tx(
    conn: &Connection,
    board_id: &str,
    schedule_id: &str,
) -> BoardResult<Option<StoredScheduleRow>> {
    conn.query_row(
        "SELECT id, board_id, channel, re, text, labels_json, attachments_json, schema_id,
                no_sync, creator_actor, creator_principal, creator_profile, creator_avatar,
                every_secs, max_times, until_at, next_run_at, runs_completed, last_run_at,
                last_message_id, status, error_message, created_at
         FROM schedules
         WHERE board_id = ?1 AND id = ?2",
        params![board_id, schedule_id],
        map_schedule_row,
    )
    .optional()
    .map_err(Into::into)
}

/// Creates a persisted schedule (`sch_...`) for a deferred post or reply.
pub fn create_schedule(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: CreateScheduleInput,
) -> BoardResult<ScheduleView> {
    create_schedule_at(db, board_id, caller, input, Utc::now())
}

pub fn create_schedule_at(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: CreateScheduleInput,
    now: DateTime<Utc>,
) -> BoardResult<ScheduleView> {
    // Exactly one target: `channel` OR `re`
    match (&input.channel, &input.re) {
        (Some(_), Some(_)) | (None, None) => {
            return Err(BoardError::bad_request(
                "schedule must specify exactly one of 'channel' or 're'",
            ));
        }
        _ => {}
    }

    validate_text_length(&input.text)?;
    if input.text.is_empty() && input.attachments.is_empty() {
        return Err(BoardError::bad_request(
            "scheduled message text cannot be empty unless attachments are provided",
        ));
    }
    if input.attachments.len() > attachments::MAX_ATTACHMENTS_PER_MESSAGE {
        return Err(BoardError::bad_request(format!(
            "a scheduled message can carry at most {} attachments",
            attachments::MAX_ATTACHMENTS_PER_MESSAGE
        )));
    }
    if !input.labels.is_empty() {
        labels::validate_user_labels(&input.labels)?;
    }

    // Validate timing options
    if input.delay_in.is_some() && input.at.is_some() {
        return Err(BoardError::bad_request(
            "cannot combine 'in' and 'at' timing options",
        ));
    }
    if input.delay_in.is_none() && input.at.is_none() && input.every.is_none() {
        return Err(BoardError::bad_request(
            "schedule requires at least one of 'in', 'at', or 'every'",
        ));
    }

    let every_secs: Option<i64> = match &input.every {
        Some(raw) => {
            let secs = parse_schedule_duration_secs(raw, "every")?;
            if secs < MIN_REPEAT_INTERVAL_SECS {
                return Err(BoardError::bad_request(format!(
                    "repeating interval 'every' must be at least {MIN_REPEAT_INTERVAL_SECS}s (1m)"
                )));
            }
            Some(secs)
        }
        None => None,
    };

    if every_secs.is_none()
        && (input.repeat_for.is_some() || input.times.is_some() || input.until.is_some())
    {
        return Err(BoardError::bad_request(
            "'for', 'times', and 'until' require 'every'",
        ));
    }
    if input.repeat_for.is_some() && input.until.is_some() {
        return Err(BoardError::bad_request("cannot combine 'for' and 'until'"));
    }
    if let Some(t) = input.times {
        if t < 1 {
            return Err(BoardError::bad_request("'times' must be at least 1"));
        }
    }

    let first_run: DateTime<Utc> = if let Some(at_raw) = &input.at {
        parse_rfc3339_utc(at_raw, "at")?
    } else if let Some(in_raw) = &input.delay_in {
        let secs = parse_schedule_duration_secs(in_raw, "in")?;
        now + Duration::seconds(secs)
    } else {
        now + Duration::seconds(every_secs.expect("checked above"))
    };

    let until_dt: Option<DateTime<Utc>> = if let Some(until_raw) = &input.until {
        let u = parse_rfc3339_utc(until_raw, "until")?;
        if u < first_run {
            return Err(BoardError::bad_request(
                "'until' must not be earlier than the first scheduled run",
            ));
        }
        Some(u)
    } else if let Some(for_raw) = &input.repeat_for {
        let secs = parse_schedule_duration_secs(for_raw, "for")?;
        Some(first_run + Duration::seconds(secs))
    } else {
        None
    };

    let labels_json = serde_json::to_string(&input.labels)
        .map_err(|e| BoardError::internal(format!("failed to serialize schedule labels: {e}")))?;
    let attachments_json = serde_json::to_string(&input.attachments).map_err(|e| {
        BoardError::internal(format!("failed to serialize schedule attachments: {e}"))
    })?;

    db.with_tx(|tx| {
        let board_grants = grants::load_grants_tx(tx, board_id)?;

        // Resolve target channel + thread root & check permissions
        let (channel, root_re): (String, Option<String>) = if let Some(ch) = &input.channel {
            validate_channel_path(ch)?;
            let perms = grants::caller_permissions_on_channel(&board_grants, caller, ch);
            let channel_exists: bool = tx
                .query_row(
                    "SELECT 1 FROM channels WHERE board_id = ?1 AND path = ?2",
                    params![board_id, ch],
                    |_| Ok(true),
                )
                .optional()?
                .unwrap_or(false);
            if channel_exists {
                if !perms.can_post_new_thread() {
                    return Err(BoardError::forbidden(format!(
                        "scheduling a post in '{ch}' requires 'post' permission"
                    )));
                }
            } else if !perms.can_create_channel() {
                return Err(BoardError::forbidden(format!(
                    "scheduling a post in new channel '{ch}' requires 'read' and 'post' permissions"
                )));
            }
            if !input.labels.is_empty() && !perms.can_label() {
                return Err(BoardError::forbidden(format!(
                    "scheduling with labels in '{ch}' requires 'label' permission"
                )));
            }
            (ch.clone(), None)
        } else {
            let target_id = input.re.as_deref().expect("checked above");
            let target_msg =
                messages::load_stored_message_tx(tx, board_id, target_id)?.ok_or_else(|| {
                    BoardError::new(
                        ErrorCode::MessageNotFound,
                        format!("message '{target_id}' not found"),
                    )
                })?;
            let perms =
                grants::caller_permissions_on_channel(&board_grants, caller, &target_msg.channel);
            if !perms.can_read() {
                return Err(BoardError::new(
                    ErrorCode::MessageNotFound,
                    format!("message '{target_id}' not found"),
                ));
            }
            if !perms.can_reply() {
                return Err(BoardError::forbidden(format!(
                    "scheduling a reply in '{}' requires 'read' and 'post' permissions",
                    target_msg.channel
                )));
            }
            if !input.labels.is_empty() && !perms.can_label() {
                return Err(BoardError::forbidden(format!(
                    "scheduling with labels in '{}' requires 'label' permission",
                    target_msg.channel
                )));
            }
            let root = target_msg.re.unwrap_or(target_msg.id);
            (target_msg.channel, Some(root))
        };

        // Validate structured blocks/schema upfront
        let _ = schemas::parse_and_validate_blocks_tx(tx, &input.text, input.schema.as_deref())?;

        // Validate attachments are ready and accessible to caller
        for att_id in &input.attachments {
            let att = attachments::load_attachment_tx(tx, board_id, att_id)?.ok_or_else(|| {
                BoardError::not_found(format!("attachment '{att_id}' not found"))
            })?;
            if att.status != "ready" {
                return Err(BoardError::bad_request(format!(
                    "attachment '{att_id}' is not ready"
                )));
            }
            if !attachments::caller_can_access_attachment_tx(
                tx,
                board_id,
                &board_grants,
                caller,
                &att,
            )? {
                return Err(BoardError::forbidden(format!(
                    "caller cannot access attachment '{att_id}'"
                )));
            }
        }

        // Preserve creator profile registration
        actors::record_actor_write_tx(tx, board_id, &channel, caller)?;

        let id = new_schedule_id();
        let now_str = format_utc_ms(now);
        let next_run_str = format_utc_ms(first_run);
        let until_str = until_dt.map(format_utc_ms);
        let creator_actor = caller.actor_id();
        let creator_principal = caller.principal_id();
        let creator_profile = caller.profile.as_ref().map(|p| p.name.clone());
        let creator_avatar = caller.profile.as_ref().and_then(|p| p.avatar.clone());

        tx.execute(
            "INSERT INTO schedules (
                id, board_id, channel, re, text, labels_json, attachments_json, schema_id,
                no_sync, creator_actor, creator_principal, creator_profile, creator_avatar,
                every_secs, max_times, until_at, next_run_at, runs_completed, last_run_at,
                last_message_id, status, error_message, created_at, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, 0, NULL,
                NULL, 'pending', NULL, ?18, ?18
             )",
            params![
                id,
                board_id,
                channel,
                root_re,
                input.text,
                labels_json,
                attachments_json,
                input.schema,
                i64::from(input.no_sync),
                creator_actor,
                creator_principal,
                creator_profile,
                creator_avatar,
                every_secs,
                input.times,
                until_str,
                next_run_str,
                now_str,
            ],
        )?;

        let row = load_schedule_row_tx(tx, board_id, &id)?.expect("inserted schedule exists");
        hydrate_schedule_view_tx(tx, &row)
    })
}

fn matches_channel_filter(channel_filter: &str, channel: &str) -> bool {
    if channel_filter == "*" {
        return true;
    }
    if let Some(prefix) = channel_filter.strip_suffix('/') {
        channel == prefix || channel.starts_with(channel_filter)
    } else {
        channel == channel_filter
    }
}

fn matches_by_filter(by_filter: &str, caller: &CallerContext, view: &ScheduleView) -> bool {
    let by_filter = by_filter.trim().trim_start_matches('@');
    if by_filter.eq_ignore_ascii_case("me") {
        if caller.profile.is_some() {
            return view.by.actor == caller.actor_id();
        }
        return view.by.principal == caller.principal_id();
    }
    if by_filter.contains(':') {
        if by_filter.contains('+') {
            return view.by.actor == by_filter;
        }
        return view.by.principal == by_filter;
    }
    if by_filter.contains('/') {
        return view.by.handle.eq_ignore_ascii_case(by_filter);
    }
    let base = view
        .by
        .handle
        .split_once('/')
        .map(|(b, _)| b)
        .unwrap_or(view.by.handle.as_str());
    base.eq_ignore_ascii_case(by_filter)
}

/// Lists schedules on `board_id`, oldest first (`next_run_at ASC`).
/// Pending-only by default unless `query.all == true`. Filtered to caller-readable channels.
pub fn list_schedules(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    query: ScheduleListQuery,
) -> BoardResult<Vec<ScheduleView>> {
    if let Some(ch) = &query.channel {
        if ch != "*" {
            if ch.ends_with('/') {
                validate_channel_pattern(ch)?;
            } else {
                validate_channel_path(ch)?;
            }
        }
    }

    db.with_conn(|conn| {
        let board_grants = grants::load_grants_tx(conn, board_id)?;

        // If `re` names any reply in a thread, normalize to thread root
        let normalized_re = match &query.re {
            Some(msg_id) => match messages::load_stored_message_tx(conn, board_id, msg_id)? {
                Some(m) => Some(m.re.unwrap_or(m.id)),
                None => Some(msg_id.clone()),
            },
            None => None,
        };

        let mut stmt = conn.prepare(
            "SELECT id, board_id, channel, re, text, labels_json, attachments_json, schema_id,
                    no_sync, creator_actor, creator_principal, creator_profile, creator_avatar,
                    every_secs, max_times, until_at, next_run_at, runs_completed, last_run_at,
                    last_message_id, status, error_message, created_at
             FROM schedules
             WHERE board_id = ?1
             ORDER BY next_run_at ASC, created_at ASC",
        )?;
        let rows = stmt.query_map(params![board_id], map_schedule_row)?;

        let mut out = Vec::new();
        for row_res in rows {
            let row = row_res?;
            if !query.all && row.status != ScheduleStatus::Pending {
                continue;
            }
            if !grants::caller_permissions_on_channel(&board_grants, caller, &row.channel)
                .can_read()
            {
                continue;
            }
            if let Some(ch_f) = &query.channel {
                if !matches_channel_filter(ch_f, &row.channel) {
                    continue;
                }
            }
            if let Some(re_f) = &normalized_re {
                if row.re.as_deref() != Some(re_f.as_str()) {
                    continue;
                }
            }
            let view = hydrate_schedule_view_tx(conn, &row)?;
            if let Some(by_f) = &query.by {
                if !matches_by_filter(by_f, caller, &view) {
                    continue;
                }
            }
            out.push(view);
        }
        Ok(out)
    })
}

/// Lists pending schedules attached to thread `root_id` (used to populate `ThreadView.schedules`).
pub fn list_pending_thread_schedules_tx(
    conn: &Connection,
    board_id: &str,
    root_id: &str,
) -> BoardResult<Vec<ScheduleView>> {
    let mut stmt = conn.prepare(
        "SELECT id, board_id, channel, re, text, labels_json, attachments_json, schema_id,
                no_sync, creator_actor, creator_principal, creator_profile, creator_avatar,
                every_secs, max_times, until_at, next_run_at, runs_completed, last_run_at,
                last_message_id, status, error_message, created_at
         FROM schedules
         WHERE board_id = ?1 AND re = ?2 AND status = 'pending'
         ORDER BY next_run_at ASC, created_at ASC",
    )?;
    let rows = stmt.query_map(params![board_id, root_id], map_schedule_row)?;
    let mut out = Vec::new();
    for row_res in rows {
        let row = row_res?;
        out.push(hydrate_schedule_view_tx(conn, &row)?);
    }
    Ok(out)
}

pub fn get_schedule(
    db: &Database,
    board_id: &str,
    schedule_id: &str,
    caller: &CallerContext,
) -> BoardResult<ScheduleView> {
    db.with_conn(|conn| {
        let row = load_schedule_row_tx(conn, board_id, schedule_id)?
            .ok_or_else(|| BoardError::not_found(format!("schedule '{schedule_id}' not found")))?;
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        let can_read =
            grants::caller_permissions_on_channel(&board_grants, caller, &row.channel).can_read();
        if !can_read && !caller.owns_actor_item(&row.creator_actor) {
            return Err(BoardError::not_found(format!(
                "schedule '{schedule_id}' not found"
            )));
        }
        hydrate_schedule_view_tx(conn, &row)
    })
}

/// Cancels a schedule.
///
/// Allowed ONLY for:
/// - The creator actor (or bare principal for any of its profiles' schedules), OR
/// - A team `Owner` (unrestricted) who holds `manage` in the schedule's channel.
///
/// Returns `(ScheduleView, canceled)` where `canceled` is `false` if already done or canceled.
pub fn cancel_schedule(
    db: &Database,
    board_id: &str,
    schedule_id: &str,
    caller: &CallerContext,
) -> BoardResult<(ScheduleView, bool)> {
    db.with_tx(|tx| {
        let mut row = load_schedule_row_tx(tx, board_id, schedule_id)?
            .ok_or_else(|| BoardError::not_found(format!("schedule '{schedule_id}' not found")))?;

        let board_grants = grants::load_grants_tx(tx, board_id)?;
        let perms = grants::caller_permissions_on_channel(&board_grants, caller, &row.channel);
        let is_creator = caller.owns_actor_item(&row.creator_actor);
        let owner_can_cancel =
            caller.principal.is_owner() && !caller.is_restricted() && perms.can_manage();

        if !is_creator && !owner_can_cancel {
            if !perms.can_read() {
                return Err(BoardError::not_found(format!(
                    "schedule '{schedule_id}' not found"
                )));
            }
            return Err(BoardError::forbidden(
                "only the schedule creator or a team owner with 'manage' in the channel can cancel this schedule",
            ));
        }

        if row.status != ScheduleStatus::Pending {
            let view = hydrate_schedule_view_tx(tx, &row)?;
            return Ok((view, false));
        }

        let now_str = format_utc_ms(Utc::now());
        tx.execute(
            "UPDATE schedules SET status = 'canceled', updated_at = ?1 WHERE board_id = ?2 AND id = ?3",
            params![now_str, board_id, schedule_id],
        )?;
        row.status = ScheduleStatus::Canceled;
        let view = hydrate_schedule_view_tx(tx, &row)?;
        Ok((view, true))
    })
}

/// Fires all pending schedules whose `next_run_at <= now` using `state.db` and `state.events`.
pub fn fire_due_schedules(state: &AppState) -> BoardResult<FireSummary> {
    let summary = fire_due_schedules_at(&state.db, &state.events, Utc::now())?;
    if summary.fired > 0 {
        state.sync_wake.notify_one();
    }
    Ok(summary)
}

/// Deterministic helper for firing due schedules at a caller-supplied timestamp `now`.
pub fn fire_due_schedules_at(
    db: &Database,
    events: &EventHub,
    now: DateTime<Utc>,
) -> BoardResult<FireSummary> {
    let now_str = format_utc_ms(now);

    let due: Vec<StoredScheduleRow> = db.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT id, board_id, channel, re, text, labels_json, attachments_json, schema_id,
                    no_sync, creator_actor, creator_principal, creator_profile, creator_avatar,
                    every_secs, max_times, until_at, next_run_at, runs_completed, last_run_at,
                    last_message_id, status, error_message, created_at
             FROM schedules
             WHERE status = 'pending' AND next_run_at <= ?1
             ORDER BY next_run_at ASC, created_at ASC",
        )?;
        let rows = stmt.query_map(params![now_str], map_schedule_row)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    })?;

    let mut summary = FireSummary::default();

    for sched in due {
        // Re-check status in case a concurrent caller canceled it
        let still_pending = db.with_conn(|conn| {
            Ok(load_schedule_row_tx(conn, &sched.board_id, &sched.id)?
                .is_some_and(|r| r.status == ScheduleStatus::Pending))
        })?;
        if !still_pending {
            continue;
        }

        // Reconstruct active creator principal + profile
        let creator_ctx = match reconstruct_creator_context(db, &sched)? {
            Some(ctx) => ctx,
            None => {
                mark_schedule_failed(
                    db,
                    &sched.board_id,
                    &sched.id,
                    "creator principal was revoked or removed",
                    &now_str,
                )?;
                summary.failed += 1;
                continue;
            }
        };

        // Re-check current board-level access before attempting write
        let has_board_access = db.with_conn(|conn| {
            let board_grants = grants::load_grants_tx(conn, &sched.board_id)?;
            Ok(grants::caller_has_any_board_access(&board_grants, &creator_ctx))
        })?;
        if !has_board_access {
            mark_schedule_failed(
                db,
                &sched.board_id,
                &sched.id,
                "creator no longer has access to board",
                &now_str,
            )?;
            summary.failed += 1;
            continue;
        }

        let labels: BTreeMap<String, String> =
            serde_json::from_str(&sched.labels_json).unwrap_or_default();
        let attachments: Vec<String> =
            serde_json::from_str(&sched.attachments_json).unwrap_or_default();

        let post_result = match &sched.re {
            None => messages::post_message(
                db,
                &sched.board_id,
                &creator_ctx,
                PostMessageInput {
                    channel: sched.channel.clone(),
                    text: sched.text.clone(),
                    id: None,
                    attachments,
                    labels,
                    schema: sched.schema_id.clone(),
                    no_sync: sched.no_sync,
                },
            ),
            Some(root_id) => messages::reply_message(
                db,
                &sched.board_id,
                root_id,
                &creator_ctx,
                ReplyMessageInput {
                    text: sched.text.clone(),
                    id: None,
                    after: None,
                    attachments,
                    labels,
                    schema: sched.schema_id.clone(),
                    no_sync: sched.no_sync,
                },
            ),
        };

        match post_result {
            Ok(posted) => {
                if let Some(seq) = posted.event_seq {
                    events.notify(&sched.board_id, seq);
                }

                let next_runs = sched.runs_completed + 1;
                let (next_status, next_run_at) = compute_next_state(&sched, next_runs, now)?;

                db.with_tx(|tx| {
                    tx.execute(
                        "UPDATE schedules
                         SET runs_completed = ?1,
                             last_run_at = ?2,
                             last_message_id = ?3,
                             next_run_at = ?4,
                             status = ?5,
                             updated_at = ?2
                         WHERE board_id = ?6 AND id = ?7 AND status = 'pending'",
                        params![
                            next_runs,
                            now_str,
                            posted.message.id,
                            next_run_at,
                            next_status.as_str(),
                            sched.board_id,
                            sched.id,
                        ],
                    )?;
                    Ok(())
                })?;

                summary.fired += 1;
                summary.posted_message_ids.push(posted.message.id);
            }
            Err(err) => {
                mark_schedule_failed(db, &sched.board_id, &sched.id, &err.message, &now_str)?;
                summary.failed += 1;
            }
        }
    }

    Ok(summary)
}

fn reconstruct_creator_context(
    db: &Database,
    sched: &StoredScheduleRow,
) -> BoardResult<Option<CallerContext>> {
    let principal = db.with_conn(|conn| {
        actors::resolve_active_principal_tx(conn, &sched.creator_principal)
    })?;
    let Some(principal) = principal else {
        return Ok(None);
    };
    let profile = sched.creator_profile.as_ref().map(|name| ProfileOverlay {
        name: name.clone(),
        name_lower: name.to_ascii_lowercase(),
        avatar: sched.creator_avatar.clone(),
    });
    Ok(Some(CallerContext {
        principal,
        profile,
        narrowing: None,
        sandboxed: false,
    }))
}

fn compute_next_state(
    sched: &StoredScheduleRow,
    runs_completed: i64,
    now: DateTime<Utc>,
) -> BoardResult<(ScheduleStatus, String)> {
    let Some(every_secs) = sched.every_secs else {
        return Ok((ScheduleStatus::Completed, sched.next_run_at.clone()));
    };

    if let Some(max_times) = sched.max_times {
        if runs_completed >= max_times {
            return Ok((ScheduleStatus::Completed, sched.next_run_at.clone()));
        }
    }

    let prev_scheduled = parse_rfc3339_utc(&sched.next_run_at, "next_run_at").unwrap_or(now);
    let candidate = prev_scheduled + Duration::seconds(every_secs);

    if let Some(until_raw) = &sched.until_at {
        if let Ok(until_dt) = parse_rfc3339_utc(until_raw, "until") {
            if candidate > until_dt {
                return Ok((ScheduleStatus::Completed, sched.next_run_at.clone()));
            }
        }
    }

    Ok((ScheduleStatus::Pending, format_utc_ms(candidate)))
}

fn mark_schedule_failed(
    db: &Database,
    board_id: &str,
    schedule_id: &str,
    reason: &str,
    now_str: &str,
) -> BoardResult<()> {
    db.with_tx(|tx| {
        tx.execute(
            "UPDATE schedules
             SET status = 'failed', error_message = ?1, updated_at = ?2
             WHERE board_id = ?3 AND id = ?4 AND status = 'pending'",
            params![reason, now_str, board_id, schedule_id],
        )?;
        Ok(())
    })
}

/// Spawns the background tokio scheduler loop ticking once per second.
pub fn spawn_scheduler_loop(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(err) = fire_due_schedules(&state) {
                tracing::warn!("scheduler tick error: {err}");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_schedule_durations_with_units() {
        assert_eq!(parse_schedule_duration_secs("30s", "in").unwrap(), 30);
        assert_eq!(parse_schedule_duration_secs("1m", "every").unwrap(), 60);
        assert_eq!(parse_schedule_duration_secs("12h", "in").unwrap(), 43_200);
        assert_eq!(parse_schedule_duration_secs("1d", "every").unwrap(), 86_400);
        assert_eq!(parse_schedule_duration_secs("90", "in").unwrap(), 90);
        assert!(parse_schedule_duration_secs("0s", "in").is_err());
        assert!(parse_schedule_duration_secs("-5m", "in").is_err());
        assert!(parse_schedule_duration_secs("", "in").is_err());
        assert!(parse_schedule_duration_secs("5w", "in").is_err());
    }
}
