use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::actors::{CallerContext, Principal, ProfileOverlay};
use crate::boards;
use crate::db::Database;
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::events::{self, EventQuery, EventType, EventView};
use crate::filter::{LabelPredicate, QueryFilter};
use crate::grants;
use crate::id::prefixed_id;
use crate::AppState;

pub const MAX_WATCHERS_PER_DEPLOYMENT: usize = 32;
pub const MAX_WATCHER_ID_LEN: usize = 64;
pub const MAX_WEBHOOK_URL_LEN: usize = 2048;

pub const HEADER_SIGNATURE: &str = "X-Board-Signature";
pub const HEADER_DELIVERY_ID: &str = "X-Board-Delivery-Id";
pub const HEADER_WATCHER_ID: &str = "X-Board-Watcher-Id";
pub const HEADER_EVENT_CURSOR: &str = "X-Board-Event-Cursor";
pub const HEADER_EVENT_TYPE: &str = "X-Board-Event-Type";
pub const HEADER_ATTEMPT: &str = "X-Board-Attempt";

pub const STATUS_PENDING: &str = "pending";
pub const STATUS_RETRYING: &str = "retrying";
pub const STATUS_DELIVERED: &str = "delivered";
pub const STATUS_DEAD_LETTER: &str = "dead_letter";

const DISPATCH_BATCH_LIMIT: usize = 100;
const RECENT_DELIVERIES_LIMIT: i64 = 25;

/// Watcher ID regex: `^[a-z0-9_-]+$` (1..=64 ASCII chars).
pub fn validate_watcher_id(id: &str) -> BoardResult<()> {
    let bytes = id.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_WATCHER_ID_LEN {
        return Err(BoardError::bad_request(format!(
            "watcher id must be 1 to {MAX_WATCHER_ID_LEN} characters"
        )));
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_' || *b == b'-')
    {
        return Err(BoardError::bad_request(format!(
            "invalid watcher id '{id}': must match ^[a-z0-9_-]+$"
        )));
    }
    Ok(())
}

/// Validates a watcher webhook URL (http/https, <= 2048 bytes, valid URL with host).
pub fn validate_webhook_url(url: &str) -> BoardResult<()> {
    let trimmed = url.trim();
    if trimmed.is_empty() || trimmed.len() > MAX_WEBHOOK_URL_LEN {
        return Err(BoardError::bad_request(format!(
            "webhook url must be 1 to {MAX_WEBHOOK_URL_LEN} bytes"
        )));
    }
    if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(BoardError::bad_request(
            "webhook url must start with http:// or https://",
        ));
    }
    if trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(BoardError::bad_request(
            "webhook url cannot contain whitespace or control characters",
        ));
    }
    let parsed = reqwest::Url::parse(trimmed)
        .map_err(|e| BoardError::bad_request(format!("invalid webhook url '{trimmed}': {e}")))?;
    if parsed.host_str().is_none() {
        return Err(BoardError::bad_request(format!(
            "webhook url '{trimmed}' must include a host"
        )));
    }
    Ok(())
}

/// Validates that a watcher `mentions` filter names an explicit handle or actor ID (not `me`).
pub fn validate_watcher_mentions(raw: &str) -> BoardResult<String> {
    let trimmed = raw.trim();
    let stripped = trimmed.trim_start_matches('@').trim();
    if stripped.is_empty() || stripped.eq_ignore_ascii_case("me") {
        return Err(BoardError::bad_request(
            "watcher 'mentions' filter requires an explicit handle or actor ID (cannot be empty or 'me')",
        ));
    }
    Ok(trimmed.to_string())
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatcherFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub re: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mentions: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub types: Vec<EventType>,
    #[serde(default)]
    pub all: bool,
}

impl WatcherFilter {
    pub fn validate(&self) -> BoardResult<()> {
        if let Some(mentions) = &self.mentions {
            validate_watcher_mentions(mentions)?;
        }
        let query = self.to_event_query()?;
        query.filter.validate()
    }

    pub fn to_event_query(&self) -> BoardResult<EventQuery> {
        let mut parsed_labels = Vec::with_capacity(self.labels.len());
        for raw in &self.labels {
            parsed_labels.push(LabelPredicate::parse(raw)?);
        }
        let filter = QueryFilter {
            channel: self.channel.clone(),
            re: self.re.clone(),
            by: self.by.clone(),
            mentions: self.mentions.clone(),
            labels: parsed_labels,
            from: self.from.clone(),
            text: self.text.clone(),
        };
        filter.validate()?;
        Ok(EventQuery {
            filter,
            after: None,
            limit: Some(DISPATCH_BATCH_LIMIT),
            all: self.all,
            types: self.types.clone(),
        })
    }

    pub fn to_tail_expression(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if let Some(ch) = &self.channel {
            parts.push(ch.clone());
        }
        if let Some(re) = &self.re {
            parts.push(format!("--re {re}"));
        }
        if let Some(by) = &self.by {
            parts.push(format!("--by {by}"));
        }
        if let Some(mentions) = &self.mentions {
            parts.push(format!("--mentions {mentions}"));
        }
        for l in &self.labels {
            parts.push(format!("--label {l}"));
        }
        if let Some(from) = &self.from {
            parts.push(format!("--from {from}"));
        }
        if let Some(text) = &self.text {
            if text.chars().any(char::is_whitespace) {
                parts.push(format!("--text \"{}\"", text.replace('"', "\\\"")));
            } else {
                parts.push(format!("--text {text}"));
            }
        }
        if !self.types.is_empty() {
            let joined = self
                .types
                .iter()
                .map(EventType::as_str)
                .collect::<Vec<_>>()
                .join(",");
            parts.push(format!("--types {joined}"));
        }
        if self.all {
            parts.push("--all".to_string());
        }
        parts.join(" ")
    }
}

fn tokenize_tail(raw: &str) -> BoardResult<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_token = false;
    let mut chars = raw.chars().peekable();

    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            if in_token {
                tokens.push(std::mem::take(&mut current));
                in_token = false;
            }
            continue;
        }
        in_token = true;
        match c {
            '\'' => {
                let mut closed = false;
                for sc in chars.by_ref() {
                    if sc == '\'' {
                        closed = true;
                        break;
                    }
                    current.push(sc);
                }
                if !closed {
                    return Err(BoardError::bad_request(
                        "unterminated single quote in tail expression",
                    ));
                }
            }
            '"' => {
                let mut closed = false;
                while let Some(dc) = chars.next() {
                    if dc == '"' {
                        closed = true;
                        break;
                    }
                    if dc == '\\' {
                        let next = chars.next().ok_or_else(|| {
                            BoardError::bad_request(
                                "unterminated escape sequence in tail expression",
                            )
                        })?;
                        current.push(next);
                    } else {
                        current.push(dc);
                    }
                }
                if !closed {
                    return Err(BoardError::bad_request(
                        "unterminated double quote in tail expression",
                    ));
                }
            }
            other => current.push(other),
        }
    }

    if in_token {
        tokens.push(current);
    }

    Ok(tokens)
}

fn next_flag_value(
    flag: &str,
    inline: Option<&str>,
    iter: &mut std::iter::Peekable<std::vec::IntoIter<String>>,
) -> BoardResult<String> {
    if let Some(v) = inline {
        if v.is_empty() {
            return Err(BoardError::bad_request(format!(
                "flag '{flag}' requires a non-empty value"
            )));
        }
        return Ok(v.to_string());
    }
    match iter.peek() {
        Some(next) if !next.starts_with("--") => Ok(iter.next().expect("peeked")),
        _ => Err(BoardError::bad_request(format!(
            "flag '{flag}' requires a value"
        ))),
    }
}

/// Parses a `tail` expression string such as `support --label state=open --mentions @ada`
/// into a validated `WatcherFilter`.
///
/// Rejects `--after` (`400 bad_request`) and requires an explicit handle/actor for `--mentions`.
pub fn parse_tail_expression(raw: &str) -> BoardResult<WatcherFilter> {
    let tokens = tokenize_tail(raw)?;
    let mut filter = WatcherFilter::default();
    let mut iter = tokens.into_iter().peekable();

    while let Some(tok) = iter.next() {
        if let Some(flag_body) = tok.strip_prefix("--") {
            let (flag_name, inline_val) = match flag_body.split_once('=') {
                Some((k, v)) => (k, Some(v)),
                None => (flag_body, None),
            };
            match flag_name {
                "after" => {
                    return Err(BoardError::bad_request(
                        "'after' is not allowed in watcher tail filters",
                    ));
                }
                "channel" => {
                    if filter.channel.is_some() {
                        return Err(BoardError::bad_request(
                            "channel specified more than once in tail expression",
                        ));
                    }
                    let val = next_flag_value("--channel", inline_val, &mut iter)?;
                    filter.channel = Some(val);
                }
                "re" => {
                    let val = next_flag_value("--re", inline_val, &mut iter)?;
                    filter.re = Some(val);
                }
                "by" => {
                    let val = next_flag_value("--by", inline_val, &mut iter)?;
                    filter.by = Some(val);
                }
                "mentions" => {
                    let val = next_flag_value("--mentions", inline_val, &mut iter)?;
                    filter.mentions = Some(validate_watcher_mentions(&val)?);
                }
                "label" => {
                    let val = next_flag_value("--label", inline_val, &mut iter)?;
                    LabelPredicate::parse(&val)?;
                    filter.labels.push(val);
                }
                "from" | "source" => {
                    let val = next_flag_value(&format!("--{flag_name}"), inline_val, &mut iter)?;
                    filter.from = Some(val);
                }
                "text" => {
                    let val = next_flag_value("--text", inline_val, &mut iter)?;
                    filter.text = Some(val);
                }
                "types" | "type" => {
                    let val = next_flag_value(&format!("--{flag_name}"), inline_val, &mut iter)?;
                    for part in val.split(',') {
                        let trimmed = part.trim();
                        if !trimmed.is_empty() {
                            let ev_type = EventType::parse(trimmed)?;
                            if !filter.types.contains(&ev_type) {
                                filter.types.push(ev_type);
                            }
                        }
                    }
                }
                "all" => match inline_val {
                    None => filter.all = true,
                    Some(v) => match v.trim().to_ascii_lowercase().as_str() {
                        "true" | "1" => filter.all = true,
                        "false" | "0" => filter.all = false,
                        other => {
                            return Err(BoardError::bad_request(format!(
                                "invalid boolean '{other}' for '--all'"
                            )));
                        }
                    },
                },
                other => {
                    return Err(BoardError::bad_request(format!(
                        "unknown watcher tail flag '--{other}'"
                    )));
                }
            }
        } else {
            if filter.channel.is_some() {
                return Err(BoardError::bad_request(format!(
                    "unexpected extra positional argument '{tok}' in tail expression"
                )));
            }
            filter.channel = Some(tok);
        }
    }

    filter.validate()?;
    Ok(filter)
}

/// Computes RFC 2104 HMAC-SHA256 over `payload` using `secret`, returning lowercase hex.
pub fn hmac_sha256_hex(secret: &[u8], payload: &[u8]) -> String {
    const BLOCK_SIZE: usize = 64;
    let mut key_block = [0u8; BLOCK_SIZE];
    if secret.len() > BLOCK_SIZE {
        let hashed = Sha256::digest(secret);
        key_block[..hashed.len()].copy_from_slice(&hashed);
    } else {
        key_block[..secret.len()].copy_from_slice(secret);
    }

    let mut ipad = [0x36u8; BLOCK_SIZE];
    let mut opad = [0x5cu8; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        ipad[i] ^= key_block[i];
        opad[i] ^= key_block[i];
    }

    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(payload);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_digest);
    let mac = outer.finalize();

    let mut hex = String::with_capacity(64);
    for byte in mac {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

pub fn format_signature_header(secret: &str, payload: &[u8]) -> String {
    format!("sha256={}", hmac_sha256_hex(secret.as_bytes(), payload))
}

/// Deterministic delivery/idempotency seed derived from `(board_id, owner_principal, watcher_id, event_cursor)`.
/// Retries of the same match always carry the exact same `delivery_id`.
pub fn compute_delivery_id(
    board_id: &str,
    owner_principal: &str,
    watcher_id: &str,
    event_cursor: &str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"openboards:watcher-delivery:v1\0");
    hasher.update(board_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(owner_principal.as_bytes());
    hasher.update(b"\0");
    hasher.update(watcher_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(event_cursor.as_bytes());
    let digest = hasher.finalize();

    let mut hex = String::with_capacity(28);
    hex.push_str("dlv_");
    for byte in &digest[..12] {
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

pub fn watcher_actor_id(owner_principal: &str, watcher_id: &str) -> String {
    format!("{owner_principal}+{watcher_id}")
}

/// Checks whether a caller is permitted to register or sync watchers.
/// Narrowed/sandboxed callers are ALWAYS denied (`403 forbidden`), and non-project callers
/// fail with `403 watchers_need_project`.
pub fn require_watcher_registration_access(caller: &CallerContext) -> BoardResult<()> {
    if caller.is_restricted() {
        return Err(BoardError::forbidden(
            "narrowed or sandboxed callers cannot register watchers",
        ));
    }
    if !matches!(caller.principal, Principal::Project { .. }) {
        return Err(BoardError::new(
            ErrorCode::WatchersNeedProject,
            "only a deployment's own project credentials can register watchers",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub enum WatcherOwnerScope {
    Single(String),
    All,
}

pub fn resolve_watcher_access_scope(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    requested_owner: Option<&str>,
) -> BoardResult<WatcherOwnerScope> {
    if caller.is_restricted() {
        return Err(BoardError::forbidden(
            "narrowed or sandboxed callers cannot manage watchers",
        ));
    }

    if matches!(caller.principal, Principal::Project { .. }) {
        let own = caller.principal_id();
        if let Some(req) = requested_owner {
            if req != own {
                return Err(BoardError::forbidden(
                    "project credentials can only access their own watchers",
                ));
            }
        }
        return Ok(WatcherOwnerScope::Single(own));
    }

    let board_grants = grants::load_grants(db, board_id)?;
    if caller.is_unrestricted_owner()
        || grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_manage()
    {
        return Ok(match requested_owner {
            Some(owner) => WatcherOwnerScope::Single(owner.to_string()),
            None => WatcherOwnerScope::All,
        });
    }

    Err(BoardError::forbidden(
        "managing watchers requires project credentials or 'manage' permission at '*'",
    ))
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct WatcherDeliverySummary {
    pub total: i64,
    pub delivered: i64,
    pub retrying: i64,
    #[serde(rename = "deadLetter")]
    pub dead_letter: i64,
    pub pending: i64,
    #[serde(rename = "lastDeliveryAt", skip_serializing_if = "Option::is_none")]
    pub last_delivery_at: Option<String>,
    #[serde(rename = "lastStatus", skip_serializing_if = "Option::is_none")]
    pub last_status: Option<String>,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatcherView {
    pub id: String,
    #[serde(rename = "boardId")]
    pub board_id: String,
    #[serde(rename = "ownerPrincipal")]
    pub owner_principal: String,
    pub actor: String,
    pub tail: String,
    pub filter: WatcherFilter,
    pub url: String,
    #[serde(rename = "hasSecret")]
    pub has_secret: bool,
    #[serde(rename = "lastCursor")]
    pub last_cursor: String,
    #[serde(rename = "lastSeq")]
    pub last_seq: i64,
    pub deliveries: WatcherDeliverySummary,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatcherDeliveryView {
    pub id: String,
    #[serde(rename = "deliveryId")]
    pub delivery_id: String,
    #[serde(rename = "eventSeq")]
    pub event_seq: i64,
    #[serde(rename = "eventCursor")]
    pub event_cursor: String,
    pub status: String,
    pub attempts: u32,
    #[serde(rename = "lastHttpStatus", skip_serializing_if = "Option::is_none")]
    pub last_http_status: Option<u16>,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(rename = "nextAttemptAt", skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<String>,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "completedAt", skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatcherDetailView {
    pub watcher: WatcherView,
    #[serde(rename = "recentDeliveries")]
    pub recent_deliveries: Vec<WatcherDeliveryView>,
}

#[derive(Debug, Clone)]
pub struct UpsertWatcherInput {
    pub board_id: String,
    pub id: String,
    pub tail_expr: String,
    pub filter: WatcherFilter,
    pub webhook_url: String,
    pub secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncWatchersResult {
    pub watchers: Vec<WatcherView>,
    pub created: Vec<String>,
    pub updated: Vec<String>,
    pub deleted: Vec<String>,
}

#[derive(Debug, Clone)]
struct StoredWatcher {
    board_id: String,
    owner_principal: String,
    id: String,
    tail_expr: String,
    filter: WatcherFilter,
    webhook_url: String,
    secret: Option<String>,
    last_cursor: i64,
    created_at: String,
    updated_at: String,
}

fn load_delivery_summary_tx(
    conn: &Connection,
    board_id: &str,
    owner_principal: &str,
    watcher_id: &str,
) -> BoardResult<WatcherDeliverySummary> {
    let (total, delivered, retrying, dead_letter, pending): (i64, i64, i64, i64, i64) = conn
        .query_row(
            "SELECT
                COUNT(*),
                COALESCE(SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'dead_letter' THEN 1 ELSE 0 END), 0),
                COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0)
             FROM watcher_deliveries
             WHERE board_id = ?1 AND owner_principal = ?2 AND watcher_id = ?3",
            params![board_id, owner_principal, watcher_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )?;

    let latest: Option<(String, String, Option<String>)> = conn
        .query_row(
            "SELECT updated_at, status, last_error
             FROM watcher_deliveries
             WHERE board_id = ?1 AND owner_principal = ?2 AND watcher_id = ?3
             ORDER BY event_seq DESC
             LIMIT 1",
            params![board_id, owner_principal, watcher_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;

    let (last_delivery_at, last_status, last_error) = match latest {
        Some((ts, st, err)) => (Some(ts), Some(st), err),
        None => (None, None, None),
    };

    Ok(WatcherDeliverySummary {
        total,
        delivered,
        retrying,
        dead_letter,
        pending,
        last_delivery_at,
        last_status,
        last_error,
    })
}

fn load_recent_deliveries_tx(
    conn: &Connection,
    board_id: &str,
    owner_principal: &str,
    watcher_id: &str,
) -> BoardResult<Vec<WatcherDeliveryView>> {
    let mut stmt = conn.prepare(
        "SELECT id, delivery_id, event_seq, event_cursor, status, attempts,
                last_http_status, last_error, next_attempt_at, created_at, updated_at, completed_at
         FROM watcher_deliveries
         WHERE board_id = ?1 AND owner_principal = ?2 AND watcher_id = ?3
         ORDER BY event_seq DESC
         LIMIT ?4",
    )?;
    let rows = stmt.query_map(
        params![board_id, owner_principal, watcher_id, RECENT_DELIVERIES_LIMIT],
        |r| {
            let status_code: Option<i64> = r.get(6)?;
            Ok(WatcherDeliveryView {
                id: r.get(0)?,
                delivery_id: r.get(1)?,
                event_seq: r.get(2)?,
                event_cursor: r.get(3)?,
                status: r.get(4)?,
                attempts: r.get::<_, i64>(5)?.max(0) as u32,
                last_http_status: status_code.and_then(|c| u16::try_from(c).ok()),
                last_error: r.get(7)?,
                next_attempt_at: r.get(8)?,
                created_at: r.get(9)?,
                updated_at: r.get(10)?,
                completed_at: r.get(11)?,
            })
        },
    )?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

fn stored_to_view_tx(conn: &Connection, stored: StoredWatcher) -> BoardResult<WatcherView> {
    let deliveries =
        load_delivery_summary_tx(conn, &stored.board_id, &stored.owner_principal, &stored.id)?;
    let actor = watcher_actor_id(&stored.owner_principal, &stored.id);
    Ok(WatcherView {
        id: stored.id,
        board_id: stored.board_id,
        owner_principal: stored.owner_principal,
        actor,
        tail: stored.tail_expr,
        filter: stored.filter,
        url: stored.webhook_url,
        has_secret: stored.secret.as_deref().is_some_and(|s| !s.is_empty()),
        last_cursor: events::format_event_cursor(stored.last_cursor),
        last_seq: stored.last_cursor,
        deliveries,
        created_at: stored.created_at,
        updated_at: stored.updated_at,
    })
}

fn parse_stored_watcher_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<StoredWatcher> {
    let filter_json: String = r.get(4)?;
    let filter: WatcherFilter = serde_json::from_str(&filter_json).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(e))
    })?;
    Ok(StoredWatcher {
        board_id: r.get(0)?,
        owner_principal: r.get(1)?,
        id: r.get(2)?,
        tail_expr: r.get(3)?,
        filter,
        webhook_url: r.get(5)?,
        secret: r.get(6)?,
        last_cursor: r.get(7)?,
        created_at: r.get(8)?,
        updated_at: r.get(9)?,
    })
}

fn load_stored_watcher_tx(
    conn: &Connection,
    board_id: &str,
    owner_principal: &str,
    watcher_id: &str,
) -> BoardResult<Option<StoredWatcher>> {
    conn.query_row(
        "SELECT board_id, owner_principal, id, tail_expr, filter_json, webhook_url,
                secret, last_cursor, created_at, updated_at
         FROM watchers
         WHERE board_id = ?1 AND owner_principal = ?2 AND id = ?3",
        params![board_id, owner_principal, watcher_id],
        parse_stored_watcher_row,
    )
    .optional()
    .map_err(Into::into)
}

fn normalize_secret(secret: Option<String>) -> Option<String> {
    secret.filter(|s| !s.is_empty())
}

/// Registers or updates a single watcher for the calling deployment.
/// Preserves `last_cursor` when updating an existing watcher ID; enforces the 32-watcher cap
/// per deployment when inserting a new watcher.
pub fn upsert_watcher(
    db: &Database,
    caller: &CallerContext,
    input: UpsertWatcherInput,
) -> BoardResult<(WatcherView, bool)> {
    require_watcher_registration_access(caller)?;
    validate_watcher_id(&input.id)?;
    validate_webhook_url(&input.webhook_url)?;
    input.filter.validate()?;

    let owner_principal = caller.principal_id();
    let filter_json = serde_json::to_string(&input.filter)
        .map_err(|e| BoardError::internal(format!("failed to serialize watcher filter: {e}")))?;
    let secret = normalize_secret(input.secret);
    let now = Utc::now().to_rfc3339();

    db.with_tx(|tx| {
        let existing = load_stored_watcher_tx(tx, &input.board_id, &owner_principal, &input.id)?;
        let created = if let Some(prev) = existing {
            tx.execute(
                "UPDATE watchers
                 SET tail_expr = ?1, filter_json = ?2, webhook_url = ?3, secret = ?4, updated_at = ?5
                 WHERE board_id = ?6 AND owner_principal = ?7 AND id = ?8",
                params![
                    input.tail_expr,
                    filter_json,
                    input.webhook_url.trim(),
                    secret,
                    now,
                    prev.board_id,
                    prev.owner_principal,
                    prev.id
                ],
            )?;
            false
        } else {
            let count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM watchers WHERE owner_principal = ?1",
                params![owner_principal],
                |r| r.get(0),
            )?;
            if count as usize >= MAX_WATCHERS_PER_DEPLOYMENT {
                return Err(BoardError::new(
                    ErrorCode::TooManyWatchers,
                    format!(
                        "deployment cannot register more than {MAX_WATCHERS_PER_DEPLOYMENT} watchers"
                    ),
                ));
            }
            let initial_seq = events::current_event_seq_tx(tx, &input.board_id)?;
            tx.execute(
                "INSERT INTO watchers (
                    board_id, owner_principal, id, tail_expr, filter_json,
                    webhook_url, secret, last_cursor, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
                params![
                    input.board_id,
                    owner_principal,
                    input.id,
                    input.tail_expr,
                    filter_json,
                    input.webhook_url.trim(),
                    secret,
                    initial_seq,
                    now
                ],
            )?;
            true
        };

        let stored = load_stored_watcher_tx(tx, &input.board_id, &owner_principal, &input.id)?
            .expect("upserted watcher must exist");
        let view = stored_to_view_tx(tx, stored)?;
        Ok((view, created))
    })
}

/// Atomically syncs/replaces a deployment's complete watcher set:
/// - Enforces max 32 watchers per deployment (`400 too_many_watchers`).
/// - Updates existing `(board_id, id)` watchers in place while preserving `last_cursor`.
/// - Inserts new watchers starting at each board's current latest event seq.
/// - Deletes previously registered watchers omitted from `desired`.
pub fn sync_deployment_watchers(
    db: &Database,
    caller: &CallerContext,
    desired: Vec<UpsertWatcherInput>,
) -> BoardResult<SyncWatchersResult> {
    require_watcher_registration_access(caller)?;

    if desired.len() > MAX_WATCHERS_PER_DEPLOYMENT {
        return Err(BoardError::new(
            ErrorCode::TooManyWatchers,
            format!(
                "deployment cannot register more than {MAX_WATCHERS_PER_DEPLOYMENT} watchers (got {})",
                desired.len()
            ),
        ));
    }

    let mut seen_keys: HashSet<(String, String)> = HashSet::with_capacity(desired.len());
    for entry in &desired {
        validate_watcher_id(&entry.id)?;
        validate_webhook_url(&entry.webhook_url)?;
        entry.filter.validate()?;
        if !seen_keys.insert((entry.board_id.clone(), entry.id.clone())) {
            return Err(BoardError::bad_request(format!(
                "duplicate watcher id '{}' on board '{}'",
                entry.id, entry.board_id
            )));
        }
    }

    let owner_principal = caller.principal_id();
    let now = Utc::now().to_rfc3339();

    db.with_tx(|tx| {
        let mut stmt = tx.prepare(
            "SELECT board_id, id FROM watchers WHERE owner_principal = ?1 ORDER BY created_at ASC",
        )?;
        let existing_keys: Vec<(String, String)> = stmt
            .query_map(params![owner_principal], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(stmt);

        let existing_set: HashSet<(String, String)> = existing_keys.iter().cloned().collect();
        let mut created = Vec::new();
        let mut updated = Vec::new();
        let mut deleted = Vec::new();

        for (board_id, id) in &existing_keys {
            if !seen_keys.contains(&(board_id.clone(), id.clone())) {
                tx.execute(
                    "DELETE FROM watchers WHERE board_id = ?1 AND owner_principal = ?2 AND id = ?3",
                    params![board_id, owner_principal, id],
                )?;
                deleted.push(id.clone());
            }
        }

        let mut views = Vec::with_capacity(desired.len());
        for entry in desired {
            let filter_json = serde_json::to_string(&entry.filter).map_err(|e| {
                BoardError::internal(format!("failed to serialize watcher filter: {e}"))
            })?;
            let secret = normalize_secret(entry.secret);
            let key = (entry.board_id.clone(), entry.id.clone());

            if existing_set.contains(&key) {
                tx.execute(
                    "UPDATE watchers
                     SET tail_expr = ?1, filter_json = ?2, webhook_url = ?3, secret = ?4, updated_at = ?5
                     WHERE board_id = ?6 AND owner_principal = ?7 AND id = ?8",
                    params![
                        entry.tail_expr,
                        filter_json,
                        entry.webhook_url.trim(),
                        secret,
                        now,
                        entry.board_id,
                        owner_principal,
                        entry.id
                    ],
                )?;
                updated.push(entry.id.clone());
            } else {
                let initial_seq = events::current_event_seq_tx(tx, &entry.board_id)?;
                tx.execute(
                    "INSERT INTO watchers (
                        board_id, owner_principal, id, tail_expr, filter_json,
                        webhook_url, secret, last_cursor, created_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)",
                    params![
                        entry.board_id,
                        owner_principal,
                        entry.id,
                        entry.tail_expr,
                        filter_json,
                        entry.webhook_url.trim(),
                        secret,
                        initial_seq,
                        now
                    ],
                )?;
                created.push(entry.id.clone());
            }

            let stored = load_stored_watcher_tx(tx, &entry.board_id, &owner_principal, &entry.id)?
                .expect("synced watcher must exist");
            views.push(stored_to_view_tx(tx, stored)?);
        }

        Ok(SyncWatchersResult {
            watchers: views,
            created,
            updated,
            deleted,
        })
    })
}

pub fn list_watchers(
    db: &Database,
    board_id: &str,
    scope: &WatcherOwnerScope,
) -> BoardResult<Vec<WatcherView>> {
    db.with_conn(|conn| {
        let stored_list: Vec<StoredWatcher> = match scope {
            WatcherOwnerScope::Single(owner) => {
                let mut stmt = conn.prepare(
                    "SELECT board_id, owner_principal, id, tail_expr, filter_json, webhook_url,
                            secret, last_cursor, created_at, updated_at
                     FROM watchers
                     WHERE board_id = ?1 AND owner_principal = ?2
                     ORDER BY id ASC",
                )?;
                let rows = stmt
                    .query_map(params![board_id, owner], parse_stored_watcher_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                rows
            }
            WatcherOwnerScope::All => {
                let mut stmt = conn.prepare(
                    "SELECT board_id, owner_principal, id, tail_expr, filter_json, webhook_url,
                            secret, last_cursor, created_at, updated_at
                     FROM watchers
                     WHERE board_id = ?1
                     ORDER BY owner_principal ASC, id ASC",
                )?;
                let rows = stmt
                    .query_map(params![board_id], parse_stored_watcher_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                rows
            }
        };

        let mut out = Vec::with_capacity(stored_list.len());
        for s in stored_list {
            out.push(stored_to_view_tx(conn, s)?);
        }
        Ok(out)
    })
}

pub fn get_watcher_detail(
    db: &Database,
    board_id: &str,
    watcher_id: &str,
    scope: &WatcherOwnerScope,
) -> BoardResult<WatcherDetailView> {
    db.with_conn(|conn| {
        let stored = match scope {
            WatcherOwnerScope::Single(owner) => {
                load_stored_watcher_tx(conn, board_id, owner, watcher_id)?
            }
            WatcherOwnerScope::All => {
                let mut stmt = conn.prepare(
                    "SELECT board_id, owner_principal, id, tail_expr, filter_json, webhook_url,
                            secret, last_cursor, created_at, updated_at
                     FROM watchers
                     WHERE board_id = ?1 AND id = ?2
                     ORDER BY created_at ASC",
                )?;
                let mut matches: Vec<StoredWatcher> = stmt
                    .query_map(params![board_id, watcher_id], parse_stored_watcher_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                if matches.len() > 1 {
                    return Err(BoardError::bad_request(format!(
                        "multiple deployments have watcher '{watcher_id}'; pass ?owner=<principal> to disambiguate"
                    )));
                }
                matches.pop()
            }
        };

        let stored = stored.ok_or_else(|| {
            BoardError::new(
                ErrorCode::WatcherNotFound,
                format!("watcher '{watcher_id}' not found"),
            )
        })?;

        let recent_deliveries = load_recent_deliveries_tx(
            conn,
            &stored.board_id,
            &stored.owner_principal,
            &stored.id,
        )?;
        let watcher = stored_to_view_tx(conn, stored)?;
        Ok(WatcherDetailView {
            watcher,
            recent_deliveries,
        })
    })
}

pub fn delete_watcher(
    db: &Database,
    board_id: &str,
    watcher_id: &str,
    scope: &WatcherOwnerScope,
) -> BoardResult<()> {
    db.with_tx(|tx| {
        let deleted = match scope {
            WatcherOwnerScope::Single(owner) => tx.execute(
                "DELETE FROM watchers WHERE board_id = ?1 AND owner_principal = ?2 AND id = ?3",
                params![board_id, owner, watcher_id],
            )?,
            WatcherOwnerScope::All => {
                let count: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM watchers WHERE board_id = ?1 AND id = ?2",
                    params![board_id, watcher_id],
                    |r| r.get(0),
                )?;
                if count > 1 {
                    return Err(BoardError::bad_request(format!(
                        "multiple deployments have watcher '{watcher_id}'; pass ?owner=<principal> to disambiguate"
                    )));
                }
                tx.execute(
                    "DELETE FROM watchers WHERE board_id = ?1 AND id = ?2",
                    params![board_id, watcher_id],
                )?
            }
        };

        if deleted == 0 {
            return Err(BoardError::new(
                ErrorCode::WatcherNotFound,
                format!("watcher '{watcher_id}' not found"),
            ));
        }
        Ok(())
    })
}

pub fn delete_board_watchers(
    db: &Database,
    board_id: &str,
    scope: &WatcherOwnerScope,
) -> BoardResult<usize> {
    db.with_tx(|tx| {
        let count = match scope {
            WatcherOwnerScope::Single(owner) => tx.execute(
                "DELETE FROM watchers WHERE board_id = ?1 AND owner_principal = ?2",
                params![board_id, owner],
            )?,
            WatcherOwnerScope::All => tx.execute(
                "DELETE FROM watchers WHERE board_id = ?1",
                params![board_id],
            )?,
        };
        Ok(count)
    })
}

// --- Background Webhook Dispatcher ---

#[derive(Debug, Clone)]
pub struct DeliveryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub request_timeout: Duration,
}

impl Default for DeliveryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            request_timeout: Duration::from_secs(10),
        }
    }
}

impl DeliveryPolicy {
    pub fn backoff_for_attempt(&self, failed_attempt: u32) -> Duration {
        let shift = failed_attempt.saturating_sub(1).min(16);
        let factor = 1u32 << shift;
        let delay = self.initial_backoff.saturating_mul(factor);
        delay.min(self.max_backoff)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DispatchReport {
    pub matched: usize,
    pub delivered: usize,
    pub dead_lettered: usize,
    pub skipped_self: usize,
}

impl DispatchReport {
    fn merge(&mut self, other: Self) {
        self.matched += other.matched;
        self.delivered += other.delivered;
        self.dead_lettered += other.dead_lettered;
        self.skipped_self += other.skipped_self;
    }
}

fn resolve_watcher_caller(
    conn: &Connection,
    owner_principal: &str,
    watcher_id: &str,
) -> BoardResult<Option<CallerContext>> {
    let Some(rest) = owner_principal.strip_prefix("project:") else {
        return Ok(None);
    };
    let Some((pid, env)) = rest.split_once(':') else {
        return Ok(None);
    };
    let project_name: Option<String> = conn
        .query_row(
            "SELECT name FROM projects WHERE id = ?1 AND revoked_at IS NULL",
            params![pid],
            |r| r.get(0),
        )
        .optional()?;
    let Some(name) = project_name else {
        return Ok(None);
    };

    Ok(Some(CallerContext {
        principal: Principal::Project {
            id: pid.to_string(),
            name,
            environment: env.to_string(),
        },
        profile: Some(ProfileOverlay {
            name: watcher_id.to_string(),
            name_lower: watcher_id.to_string(),
            avatar: None,
        }),
        narrowing: None,
        sandboxed: false,
    }))
}

fn load_event_raw_actor_id(
    db: &Database,
    board_id: &str,
    event_seq: i64,
) -> BoardResult<Option<String>> {
    db.with_conn(|conn| {
        conn.query_row(
            "SELECT actor_id FROM events WHERE board_id = ?1 AND seq = ?2",
            params![board_id, event_seq],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .map(|opt| opt.flatten())
        .map_err(Into::into)
    })
}

fn advance_watcher_cursor(
    db: &Database,
    board_id: &str,
    owner_principal: &str,
    watcher_id: &str,
    seq: i64,
) -> BoardResult<()> {
    db.with_tx(|tx| {
        tx.execute(
            "UPDATE watchers
             SET last_cursor = MAX(last_cursor, ?1)
             WHERE board_id = ?2 AND owner_principal = ?3 AND id = ?4",
            params![seq, board_id, owner_principal, watcher_id],
        )?;
        Ok(())
    })
}

fn watcher_still_exists(
    db: &Database,
    board_id: &str,
    owner_principal: &str,
    watcher_id: &str,
) -> BoardResult<bool> {
    db.with_conn(|conn| {
        Ok(conn
            .query_row(
                "SELECT 1 FROM watchers WHERE board_id = ?1 AND owner_principal = ?2 AND id = ?3",
                params![board_id, owner_principal, watcher_id],
                |_| Ok(true),
            )
            .optional()?
            .unwrap_or(false))
    })
}

enum DeliveryOutcome {
    Delivered,
    DeadLetter,
    AlreadySettled,
    WatcherDeleted,
}

async fn deliver_event_with_retries(
    db: &Database,
    client: &reqwest::Client,
    watcher: &StoredWatcher,
    board_name: &str,
    event: &EventView,
    event_seq: i64,
    policy: &DeliveryPolicy,
) -> BoardResult<DeliveryOutcome> {
    let delivery_id = compute_delivery_id(
        &watcher.board_id,
        &watcher.owner_principal,
        &watcher.id,
        &event.cursor,
    );
    let max_attempts = policy.max_attempts.max(1);

    let (row_id, existing_attempts) = db.with_tx(|tx| {
        let existing: Option<(String, String, i64)> = tx
            .query_row(
                "SELECT id, status, attempts
                 FROM watcher_deliveries
                 WHERE board_id = ?1 AND owner_principal = ?2 AND watcher_id = ?3 AND event_seq = ?4",
                params![
                    watcher.board_id,
                    watcher.owner_principal,
                    watcher.id,
                    event_seq
                ],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;

        if let Some((id, status, attempts)) = existing {
            if status == STATUS_DELIVERED || status == STATUS_DEAD_LETTER {
                return Ok((None, attempts.max(0) as u32));
            }
            return Ok((Some(id), attempts.max(0) as u32));
        }

        let id = prefixed_id("wdv");
        let now = Utc::now().to_rfc3339();
        tx.execute(
            "INSERT INTO watcher_deliveries (
                id, board_id, owner_principal, watcher_id, event_seq, event_cursor,
                delivery_id, status, attempts, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 0, ?9, ?9)",
            params![
                id,
                watcher.board_id,
                watcher.owner_principal,
                watcher.id,
                event_seq,
                event.cursor,
                delivery_id,
                STATUS_PENDING,
                now
            ],
        )?;
        Ok((Some(id), 0))
    })?;

    let Some(row_id) = row_id else {
        return Ok(DeliveryOutcome::AlreadySettled);
    };

    let actor = watcher_actor_id(&watcher.owner_principal, &watcher.id);
    let payload_body = json!({
        "deliveryId": delivery_id,
        "watcher": {
            "id": watcher.id,
            "boardId": watcher.board_id,
            "board": board_name,
            "actor": actor,
            "tail": watcher.tail_expr,
        },
        "event": event,
    });
    let body_bytes = serde_json::to_vec(&payload_body)
        .map_err(|e| BoardError::internal(format!("failed to serialize webhook body: {e}")))?;

    let signature_header = watcher
        .secret
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(|s| format_signature_header(s, &body_bytes));

    let start_attempt = existing_attempts + 1;
    for attempt in start_attempt..=max_attempts {
        if !watcher_still_exists(db, &watcher.board_id, &watcher.owner_principal, &watcher.id)? {
            return Ok(DeliveryOutcome::WatcherDeleted);
        }

        let mut req = client
            .post(&watcher.webhook_url)
            .timeout(policy.request_timeout)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(HEADER_DELIVERY_ID, &delivery_id)
            .header(HEADER_WATCHER_ID, &watcher.id)
            .header(HEADER_EVENT_CURSOR, &event.cursor)
            .header(HEADER_EVENT_TYPE, event.event_type.as_str())
            .header(HEADER_ATTEMPT, attempt.to_string())
            .body(body_bytes.clone());

        if let Some(sig) = &signature_header {
            req = req.header(HEADER_SIGNATURE, sig);
        }

        let outcome: Result<u16, (Option<u16>, String)> = match req.send().await {
            Ok(resp) => {
                let code = resp.status().as_u16();
                if resp.status().is_success() {
                    Ok(code)
                } else {
                    Err((Some(code), format!("webhook returned HTTP {code}")))
                }
            }
            Err(err) => Err((None, format!("webhook request failed: {err}"))),
        };

        let now = Utc::now();
        let now_str = now.to_rfc3339();

        match outcome {
            Ok(code) => {
                db.with_tx(|tx| {
                    tx.execute(
                        "UPDATE watcher_deliveries
                         SET status = ?1, attempts = ?2, last_http_status = ?3,
                             last_error = NULL, next_attempt_at = NULL,
                             updated_at = ?4, completed_at = ?4
                         WHERE id = ?5",
                        params![STATUS_DELIVERED, i64::from(attempt), i64::from(code), now_str, row_id],
                    )?;
                    Ok(())
                })?;
                return Ok(DeliveryOutcome::Delivered);
            }
            Err((status_opt, err_msg)) => {
                if attempt < max_attempts {
                    let delay = policy.backoff_for_attempt(attempt);
                    let next_at = (now
                        + ChronoDuration::from_std(delay)
                            .unwrap_or_else(|_| ChronoDuration::seconds(30)))
                    .to_rfc3339();
                    db.with_tx(|tx| {
                        tx.execute(
                            "UPDATE watcher_deliveries
                             SET status = ?1, attempts = ?2, last_http_status = ?3,
                                 last_error = ?4, next_attempt_at = ?5, updated_at = ?6
                             WHERE id = ?7",
                            params![
                                STATUS_RETRYING,
                                i64::from(attempt),
                                status_opt.map(i64::from),
                                err_msg,
                                next_at,
                                now_str,
                                row_id
                            ],
                        )?;
                        Ok(())
                    })?;
                    tokio::time::sleep(delay).await;
                } else {
                    db.with_tx(|tx| {
                        tx.execute(
                            "UPDATE watcher_deliveries
                             SET status = ?1, attempts = ?2, last_http_status = ?3,
                                 last_error = ?4, next_attempt_at = NULL,
                                 updated_at = ?5, completed_at = ?5
                             WHERE id = ?6",
                            params![
                                STATUS_DEAD_LETTER,
                                i64::from(attempt),
                                status_opt.map(i64::from),
                                err_msg,
                                now_str,
                                row_id
                            ],
                        )?;
                        Ok(())
                    })?;
                    return Ok(DeliveryOutcome::DeadLetter);
                }
            }
        }
    }

    Ok(DeliveryOutcome::DeadLetter)
}

/// Deterministically processes all pending board events for a single watcher starting after
/// its persisted `last_cursor`, delivering matches with retries and persisting the updated cursor.
pub async fn dispatch_watcher_once(
    db: &Database,
    client: &reqwest::Client,
    board_id: &str,
    owner_principal: &str,
    watcher_id: &str,
    policy: &DeliveryPolicy,
) -> BoardResult<DispatchReport> {
    let loaded = db.with_conn(|conn| {
        let Some(stored) = load_stored_watcher_tx(conn, board_id, owner_principal, watcher_id)? else {
            return Ok(None);
        };
        let Some(caller) = resolve_watcher_caller(conn, owner_principal, watcher_id)? else {
            return Ok(None);
        };
        let board_name = boards::find_board_tx(conn, board_id)?
            .map(|b| b.name)
            .unwrap_or_else(|| board_id.to_string());
        Ok(Some((stored, caller, board_name)))
    })?;

    let Some((watcher, watcher_caller, board_name)) = loaded else {
        return Ok(DispatchReport::default());
    };

    let event_query = watcher.filter.to_event_query()?;
    let self_actor_id = watcher_actor_id(&watcher.owner_principal, &watcher.id);
    let mut cursor_seq = watcher.last_cursor;
    let mut report = DispatchReport::default();

    loop {
        let page = events::query_events_after(
            db,
            &watcher.board_id,
            &watcher_caller,
            cursor_seq,
            &event_query,
        )?;

        if page.events.is_empty() {
            if let Some(tip_seq) = events::parse_event_cursor_seq(&page.cursor) {
                if tip_seq > cursor_seq {
                    advance_watcher_cursor(
                        db,
                        &watcher.board_id,
                        &watcher.owner_principal,
                        &watcher.id,
                        tip_seq,
                    )?;
                }
            }
            break;
        }

        for ev in &page.events {
            let Some(ev_seq) = events::parse_event_cursor_seq(&ev.cursor) else {
                continue;
            };

            let raw_actor_id = load_event_raw_actor_id(db, &watcher.board_id, ev_seq)?;
            let ev_actor_id = raw_actor_id
                .as_deref()
                .or_else(|| ev.actor.as_ref().map(|a| a.id.as_str()));

            if ev_actor_id == Some(self_actor_id.as_str()) {
                report.skipped_self += 1;
                advance_watcher_cursor(
                    db,
                    &watcher.board_id,
                    &watcher.owner_principal,
                    &watcher.id,
                    ev_seq,
                )?;
                cursor_seq = ev_seq;
                continue;
            }

            report.matched += 1;
            match deliver_event_with_retries(
                db,
                client,
                &watcher,
                &board_name,
                ev,
                ev_seq,
                policy,
            )
            .await?
            {
                DeliveryOutcome::Delivered => report.delivered += 1,
                DeliveryOutcome::DeadLetter => report.dead_lettered += 1,
                DeliveryOutcome::AlreadySettled => {}
                DeliveryOutcome::WatcherDeleted => return Ok(report),
            }

            advance_watcher_cursor(
                db,
                &watcher.board_id,
                &watcher.owner_principal,
                &watcher.id,
                ev_seq,
            )?;
            cursor_seq = ev_seq;
        }
    }

    Ok(report)
}

/// Deterministically dispatches pending events for all watchers registered on `board_id`.
pub async fn dispatch_board_once(
    db: &Database,
    client: &reqwest::Client,
    board_id: &str,
    policy: &DeliveryPolicy,
) -> BoardResult<DispatchReport> {
    let keys: Vec<(String, String)> = db.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT owner_principal, id FROM watchers WHERE board_id = ?1 ORDER BY owner_principal ASC, id ASC",
        )?;
        let rows = stmt
            .query_map(params![board_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    })?;

    let mut total = DispatchReport::default();
    for (owner, id) in keys {
        let sub = dispatch_watcher_once(db, client, board_id, &owner, &id, policy).await?;
        total.merge(sub);
    }
    Ok(total)
}

/// Deterministically dispatches pending events across every registered watcher on every board.
pub async fn dispatch_all_once(
    db: &Database,
    client: &reqwest::Client,
    policy: &DeliveryPolicy,
) -> BoardResult<DispatchReport> {
    let keys: Vec<(String, String, String)> = db.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT board_id, owner_principal, id FROM watchers ORDER BY board_id ASC, owner_principal ASC, id ASC",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    })?;

    let mut total = DispatchReport::default();
    for (board_id, owner, id) in keys {
        let sub = dispatch_watcher_once(db, client, &board_id, &owner, &id, policy).await?;
        total.merge(sub);
    }
    Ok(total)
}

type WatcherLockKey = (String, String, String);

#[derive(Clone, Default)]
struct WatcherLocks {
    locks: Arc<Mutex<HashMap<WatcherLockKey, Arc<tokio::sync::Mutex<()>>>>>,
}

impl WatcherLocks {
    fn lock_for(&self, key: WatcherLockKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut guard = self.locks.lock().expect("watcher locks poisoned");
        guard
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

async fn dispatch_board_serialized(
    db: &Database,
    client: &reqwest::Client,
    board_id: &str,
    policy: &DeliveryPolicy,
    locks: &WatcherLocks,
) {
    let keys: Vec<(String, String)> = match db.with_conn(|conn| {
        let mut stmt = conn.prepare(
            "SELECT owner_principal, id FROM watchers WHERE board_id = ?1 ORDER BY owner_principal ASC, id ASC",
        )?;
        let rows = stmt
            .query_map(params![board_id], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }) {
        Ok(k) => k,
        Err(err) => {
            tracing::warn!("failed to list watchers for board {board_id}: {err}");
            return;
        }
    };

    for (owner, id) in keys {
        let db = db.clone();
        let client = client.clone();
        let board_id = board_id.to_string();
        let policy = policy.clone();
        let lock = locks.lock_for((board_id.clone(), owner.clone(), id.clone()));
        tokio::spawn(async move {
            let _guard = lock.lock().await;
            if let Err(err) =
                dispatch_watcher_once(&db, &client, &board_id, &owner, &id, &policy).await
            {
                tracing::warn!(
                    "watcher dispatch failed for board={board_id} owner={owner} id={id}: {err}"
                );
            }
        });
    }
}

/// Spawns the background watcher webhook dispatcher using default delivery retry policy.
pub fn spawn_watcher_dispatcher(state: AppState) -> tokio::task::JoinHandle<()> {
    spawn_watcher_dispatcher_with_policy(state, DeliveryPolicy::default())
}

/// Spawns the background watcher webhook dispatcher with a custom `DeliveryPolicy`.
/// Subscribes to `state.events.subscribe(&board_id)` for each board that has watchers and
/// wakes dynamically whenever new watchers are registered.
pub fn spawn_watcher_dispatcher_with_policy(
    state: AppState,
    policy: DeliveryPolicy,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = reqwest::Client::new();
        let locks = WatcherLocks::default();
        let mut board_tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();

        loop {
            let board_ids: Vec<String> = match state.db.with_conn(|conn| {
                let mut stmt = conn.prepare("SELECT DISTINCT board_id FROM watchers")?;
                let rows = stmt
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            }) {
                Ok(ids) => ids,
                Err(err) => {
                    tracing::warn!("watcher dispatcher failed to query boards: {err}");
                    Vec::new()
                }
            };

            for board_id in board_ids {
                if let std::collections::hash_map::Entry::Vacant(e) =
                    board_tasks.entry(board_id.clone())
                {
                    let task_state = state.clone();
                    let task_client = client.clone();
                    let task_policy = policy.clone();
                    let task_locks = locks.clone();
                    let bid = board_id.clone();

                    // Subscribe BEFORE initial catch-up so concurrent events cannot be missed.
                    let mut rx = task_state.events.subscribe(&bid);

                    let handle = tokio::spawn(async move {
                        dispatch_board_serialized(
                            &task_state.db,
                            &task_client,
                            &bid,
                            &task_policy,
                            &task_locks,
                        )
                        .await;

                        while let Ok(_)
                        | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) =
                            rx.recv().await
                        {
                            dispatch_board_serialized(
                                &task_state.db,
                                &task_client,
                                &bid,
                                &task_policy,
                                &task_locks,
                            )
                            .await;
                        }
                    });
                    e.insert(handle);
                }
            }

            // Wake immediately when a watcher is registered/synced, or reconcile periodically.
            tokio::select! {
                _ = state.watcher_wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_watcher_ids() {
        assert!(validate_watcher_id("triage").is_ok());
        assert!(validate_watcher_id("follow_up-2").is_ok());
        assert!(validate_watcher_id("0").is_ok());
        assert!(validate_watcher_id(&"a".repeat(64)).is_ok());

        assert!(validate_watcher_id("").is_err());
        assert!(validate_watcher_id(&"a".repeat(65)).is_err());
        assert!(validate_watcher_id("Triage").is_err());
        assert!(validate_watcher_id("triage.bot").is_err());
        assert!(validate_watcher_id("triage/bot").is_err());
        assert!(validate_watcher_id("triage bot").is_err());
    }

    #[test]
    fn validates_webhook_urls() {
        assert!(validate_webhook_url("https://example.com/api/board").is_ok());
        assert!(validate_webhook_url("http://127.0.0.1:8080/hook").is_ok());

        assert!(validate_webhook_url("").is_err());
        assert!(validate_webhook_url("ftp://example.com/hook").is_err());
        assert!(validate_webhook_url("/relative/path").is_err());
        assert!(validate_webhook_url("https://example.com/bad url").is_err());
        assert!(validate_webhook_url("https://").is_err());
    }

    #[test]
    fn parses_tail_expressions_and_enforces_watcher_constraints() {
        let f = parse_tail_expression(
            "build/ --label 'board.task=completed|blocked|failed' --by ada --mentions @bob --source slack --text \"deploy failed\" --types message.posted,labels.changed --all",
        )
        .unwrap();
        assert_eq!(f.channel.as_deref(), Some("build/"));
        assert_eq!(f.labels, vec!["board.task=completed|blocked|failed"]);
        assert_eq!(f.by.as_deref(), Some("ada"));
        assert_eq!(f.mentions.as_deref(), Some("@bob"));
        assert_eq!(f.from.as_deref(), Some("slack"));
        assert_eq!(f.text.as_deref(), Some("deploy failed"));
        assert_eq!(
            f.types,
            vec![EventType::MessagePosted, EventType::LabelsChanged]
        );
        assert!(f.all);

        // Channel-less label-only tail
        let f2 = parse_tail_expression("--label state=done").unwrap();
        assert_eq!(f2.channel, None);
        assert_eq!(f2.labels, vec!["state=done"]);
        assert!(!f2.all);

        // Reject --after
        assert!(parse_tail_expression("support --after ev_5").is_err());
        assert!(parse_tail_expression("support --after=msg_123").is_err());

        // Reject implicit / 'me' mentions
        assert!(parse_tail_expression("support --mentions").is_err());
        assert!(parse_tail_expression("support --mentions me").is_err());
        assert!(parse_tail_expression("support --mentions @me").is_err());
        assert!(parse_tail_expression("support --mentions @").is_err());

        // Reject unknown flags & extra positional args
        assert!(parse_tail_expression("support --bogus foo").is_err());
        assert!(parse_tail_expression("support extra").is_err());
        assert!(parse_tail_expression("support --text 'unterminated").is_err());
    }

    #[test]
    fn hmac_sha256_matches_rfc4231_test_vectors() {
        // RFC 4231 Test Case 1
        let key1 = [0x0bu8; 20];
        assert_eq!(
            hmac_sha256_hex(&key1, b"Hi There"),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );

        // RFC 4231 Test Case 2
        assert_eq!(
            hmac_sha256_hex(b"Jefe", b"what do ya want for nothing?"),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );

        // RFC 4231 Test Case 6 (key > 64 bytes)
        let key6 = [0xaau8; 131];
        assert_eq!(
            hmac_sha256_hex(&key6, b"Test Using Larger Than Block-Size Key - Hash Key First"),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn delivery_id_is_deterministic_and_scoped() {
        let a = compute_delivery_id("brd_1", "project:prj_1:production", "triage", "ev_42");
        let b = compute_delivery_id("brd_1", "project:prj_1:production", "triage", "ev_42");
        let diff_ev = compute_delivery_id("brd_1", "project:prj_1:production", "triage", "ev_43");
        let diff_w = compute_delivery_id("brd_1", "project:prj_1:production", "other", "ev_42");

        assert!(a.starts_with("dlv_"));
        assert_eq!(a, b);
        assert_ne!(a, diff_ev);
        assert_ne!(a, diff_w);
    }

    #[test]
    fn backoff_scales_exponentially_and_caps() {
        let p = DeliveryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(500),
            request_timeout: Duration::from_secs(5),
        };
        assert_eq!(p.backoff_for_attempt(1), Duration::from_millis(100));
        assert_eq!(p.backoff_for_attempt(2), Duration::from_millis(200));
        assert_eq!(p.backoff_for_attempt(3), Duration::from_millis(400));
        assert_eq!(p.backoff_for_attempt(4), Duration::from_millis(500));
    }
}
