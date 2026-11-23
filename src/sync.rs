use std::collections::BTreeMap;

use axum::http::HeaderMap;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::watch;

use crate::actors::{CallerContext, Principal};
use crate::db::{Database, DbRow, DbTx, Dialect, Queryable};
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::events::{self, EventAppend, EventType};
use crate::grants;
use crate::id::{new_message_id, prefixed_id};
use crate::labels;
use crate::messages::{self, MessageView, StoredMessageRow, ThreadView};
use crate::sql_args;
use crate::validate::{validate_channel_path, validate_slug};
use crate::watchers::hmac_sha256_hex;
use crate::AppState;

pub const MAX_SYNC_RETRIES: i64 = 5;
pub const MAX_INBOX_PER_SOURCE: i64 = 200;
pub const MAX_TYPING_STATUS_CHARS: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncPlatform {
    Slack,
    Linear,
    Github,
    Custom,
}

impl SyncPlatform {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Slack => "slack",
            Self::Linear => "linear",
            Self::Github => "github",
            Self::Custom => "custom",
        }
    }

    pub fn parse(s: &str) -> BoardResult<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "slack" => Ok(Self::Slack),
            "linear" => Ok(Self::Linear),
            "github" => Ok(Self::Github),
            "custom" => Ok(Self::Custom),
            other => Err(BoardError::bad_request(format!(
                "unknown sync platform '{other}'; expected slack, linear, github, or custom"
            ))),
        }
    }

    pub fn default_direction(self) -> SyncDirection {
        match self {
            Self::Github => SyncDirection::Inbound,
            Self::Slack | Self::Linear | Self::Custom => SyncDirection::Bidirectional,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncDirection {
    Inbound,
    Outbound,
    Bidirectional,
}

impl SyncDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
            Self::Bidirectional => "bidirectional",
        }
    }

    pub fn parse(s: &str) -> BoardResult<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "inbound" => Ok(Self::Inbound),
            "outbound" => Ok(Self::Outbound),
            "bidirectional" => Ok(Self::Bidirectional),
            other => Err(BoardError::bad_request(format!(
                "invalid sync direction '{other}'; expected inbound, outbound, or bidirectional"
            ))),
        }
    }

    pub const fn allows_outbound(self) -> bool {
        matches!(self, Self::Outbound | Self::Bidirectional)
    }

    pub const fn allows_inbound(self) -> bool {
        matches!(self, Self::Inbound | Self::Bidirectional)
    }
}

/// Mapping value for a platform channel/repo/team -> board channel(s).
/// Supports either a single board channel string (`"#prs": "prs"`) or a GitHub split object
/// (`"acme/api": { "pulls": "prs", "issues": "issues" }`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MappingTarget {
    Single(String),
    GitHubSplit {
        pulls: Option<String>,
        issues: Option<String>,
    },
}

impl MappingTarget {
    fn validate(&self) -> BoardResult<()> {
        match self {
            Self::Single(ch) => validate_channel_path(ch),
            Self::GitHubSplit { pulls, issues } => {
                if pulls.is_none() && issues.is_none() {
                    return Err(BoardError::bad_request(
                        "mapping must specify at least one of 'pulls' or 'issues'",
                    ));
                }
                if let Some(p) = pulls {
                    validate_channel_path(p)?;
                }
                if let Some(i) = issues {
                    validate_channel_path(i)?;
                }
                Ok(())
            }
        }
    }

    pub fn board_channel_for(&self, is_pull: bool) -> Option<&str> {
        match self {
            Self::Single(ch) => Some(ch.as_str()),
            Self::GitHubSplit { pulls, issues } => {
                if is_pull {
                    pulls.as_deref()
                } else {
                    issues.as_deref()
                }
            }
        }
    }

    pub fn all_board_channels(&self) -> Vec<&str> {
        match self {
            Self::Single(ch) => vec![ch.as_str()],
            Self::GitHubSplit { pulls, issues } => {
                let mut v = Vec::new();
                if let Some(p) = pulls.as_deref() {
                    v.push(p);
                }
                if let Some(i) = issues.as_deref() {
                    v.push(i);
                }
                v
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoredSyncSource {
    pub id: String,
    pub board_id: String,
    pub key: String,
    pub project_id: String,
    pub project_name: String,
    pub environment: String,
    pub owner_principal: String,
    pub platform: SyncPlatform,
    pub direction: SyncDirection,
    pub mappings: BTreeMap<String, MappingTarget>,
    pub webhook_secret: Option<String>,
    pub outbound_url: Option<String>,
    pub outbound_token: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl StoredSyncSource {
    pub fn qualified_key_for(&self, caller_project_id: Option<&str>) -> String {
        if caller_project_id == Some(self.project_id.as_str()) {
            self.key.clone()
        } else {
            format!("{}/{}", self.project_name, self.key)
        }
    }

    pub fn to_view(&self, caller_project_id: Option<&str>) -> SyncSourceView {
        SyncSourceView {
            id: self.id.clone(),
            key: self.key.clone(),
            qualified_key: self.qualified_key_for(caller_project_id),
            project_id: self.project_id.clone(),
            project_name: self.project_name.clone(),
            environment: self.environment.clone(),
            platform: self.platform,
            direction: self.direction,
            mappings: self.mappings.clone(),
            outbound_url: self.outbound_url.clone(),
            has_webhook_secret: self.webhook_secret.is_some(),
            has_outbound_token: self.outbound_token.is_some(),
            created_at: self.created_at.clone(),
            updated_at: self.updated_at.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncSourceView {
    pub id: String,
    pub key: String,
    #[serde(rename = "qualifiedKey")]
    pub qualified_key: String,
    #[serde(rename = "projectId")]
    pub project_id: String,
    #[serde(rename = "projectName")]
    pub project_name: String,
    pub environment: String,
    pub platform: SyncPlatform,
    pub direction: SyncDirection,
    pub mappings: BTreeMap<String, MappingTarget>,
    #[serde(rename = "outboundUrl", skip_serializing_if = "Option::is_none")]
    pub outbound_url: Option<String>,
    #[serde(rename = "hasWebhookSecret")]
    pub has_webhook_secret: bool,
    #[serde(rename = "hasOutboundToken")]
    pub has_outbound_token: bool,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct RegisterSyncSourceInput {
    pub key: String,
    pub platform: SyncPlatform,
    pub direction: Option<SyncDirection>,
    #[serde(default)]
    pub mappings: BTreeMap<String, MappingTarget>,
    #[serde(alias = "webhookSecret")]
    pub webhook_secret: Option<String>,
    #[serde(alias = "outboundUrl")]
    pub outbound_url: Option<String>,
    #[serde(alias = "outboundToken")]
    pub outbound_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadSyncLinkView {
    pub id: String,
    #[serde(rename = "threadId")]
    pub thread_root_id: String,
    pub channel: String,
    #[serde(rename = "sourceKey")]
    pub source_key: String,
    pub platform: SyncPlatform,
    #[serde(rename = "platformChannel")]
    pub platform_channel: String,
    #[serde(rename = "platformThreadId")]
    pub platform_thread_id: String,
    #[serde(rename = "platformUrl", skip_serializing_if = "Option::is_none")]
    pub platform_url: Option<String>,
    pub direction: SyncDirection,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncInboxItem {
    pub id: i64,
    #[serde(rename = "eventType")]
    pub event_type: String,
    pub headers: Value,
    pub body: Value,
    #[serde(rename = "receivedAt")]
    pub received_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncDeliveryView {
    pub id: String,
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub kind: String,
    pub status: String,
    pub attempts: i64,
    #[serde(rename = "lastHttpStatus", skip_serializing_if = "Option::is_none")]
    pub last_http_status: Option<i64>,
    #[serde(rename = "lastError", skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
}

#[derive(Debug, Deserialize)]
pub struct LinkThreadInput {
    pub target: String,
    #[serde(alias = "platformUrl")]
    pub platform_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TypingInput {
    pub status: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DeliveryTickSummary {
    pub delivered: usize,
    pub retried: usize,
    pub failed: usize,
}

fn format_utc_ms(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn caller_project_info(caller: &CallerContext) -> (String, String, String) {
    match &caller.principal {
        Principal::Project {
            id,
            name,
            environment,
        } => (id.clone(), name.clone(), environment.clone()),
        Principal::User { id, username, .. } => {
            (format!("user_{id}"), username.clone(), "production".to_string())
        }
        Principal::App { id, slug, .. } => {
            (format!("app_{id}"), slug.clone(), "production".to_string())
        }
    }
}

fn caller_project_id_opt(caller: &CallerContext) -> Option<&str> {
    match &caller.principal {
        Principal::Project { id, .. } => Some(id.as_str()),
        _ => None,
    }
}

fn map_source_row(r: &DbRow) -> BoardResult<StoredSyncSource> {
    let platform_str = r.get_str(7)?;
    let dir_str = r.get_str(8)?;
    let mappings_json = r.get_str(9)?;
    let platform = SyncPlatform::parse(&platform_str).unwrap_or(SyncPlatform::Custom);
    let direction = SyncDirection::parse(&dir_str).unwrap_or(platform.default_direction());
    let mappings: BTreeMap<String, MappingTarget> =
        serde_json::from_str(&mappings_json).unwrap_or_default();
    Ok(StoredSyncSource {
        id: r.get_str(0)?,
        board_id: r.get_str(1)?,
        key: r.get_str(2)?,
        project_id: r.get_str(3)?,
        project_name: r.get_str(4)?,
        environment: r.get_str(5)?,
        owner_principal: r.get_str(6)?,
        platform,
        direction,
        mappings,
        webhook_secret: r.get_opt_str(10)?,
        outbound_url: r.get_opt_str(11)?,
        outbound_token: r.get_opt_str(12)?,
        created_at: r.get_str(13)?,
        updated_at: r.get_str(14)?,
    })
}

async fn load_sources_for_board_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
) -> BoardResult<Vec<StoredSyncSource>> {
    let rows = conn
        .fetch_all(
            "SELECT id, board_id, key, project_id, project_name, environment, owner_principal,
                    platform, direction, mappings_json, webhook_secret, outbound_url, outbound_token,
                    created_at, updated_at
             FROM sync_sources
             WHERE board_id = ?1
             ORDER BY created_at ASC",
            sql_args![board_id],
        )
        .await?;
    rows.iter().map(map_source_row).collect()
}

async fn load_source_by_id_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    source_id: &str,
) -> BoardResult<Option<StoredSyncSource>> {
    let row = conn
        .fetch_optional(
            "SELECT id, board_id, key, project_id, project_name, environment, owner_principal,
                    platform, direction, mappings_json, webhook_secret, outbound_url, outbound_token,
                    created_at, updated_at
             FROM sync_sources
             WHERE id = ?1",
            sql_args![source_id],
        )
        .await?;
    row.as_ref().map(map_source_row).transpose()
}

/// Resolves `<key>` (preferring caller's project, or unique key on board) or `<project>/<key>`.
async fn resolve_source_by_key_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    key_spec: &str,
    caller: Option<&CallerContext>,
) -> BoardResult<Option<StoredSyncSource>> {
    let sources = load_sources_for_board_tx(conn, board_id).await?;
    if let Some((proj_part, k_part)) = key_spec.split_once('/') {
        return Ok(sources.into_iter().find(|s| {
            (s.project_name.eq_ignore_ascii_case(proj_part) || s.project_id == proj_part)
                && s.key.eq_ignore_ascii_case(k_part)
        }));
    }
    let caller_proj = caller.and_then(caller_project_id_opt);
    if let Some(pid) = caller_proj {
        if let Some(found) = sources
            .iter()
            .find(|s| s.project_id == pid && s.key.eq_ignore_ascii_case(key_spec))
        {
            return Ok(Some(found.clone()));
        }
    }
    let matches: Vec<StoredSyncSource> = sources
        .into_iter()
        .filter(|s| s.key.eq_ignore_ascii_case(key_spec))
        .collect();
    if matches.len() == 1 {
        return Ok(matches.into_iter().next());
    }
    if matches.len() > 1 {
        return Err(BoardError::bad_request(format!(
            "ambiguous sync key '{key_spec}'; qualify as '<project>/{key_spec}'"
        )));
    }
    Ok(None)
}

fn require_sync_allowed(caller: &CallerContext) -> BoardResult<()> {
    if !caller.can_access_sync() {
        return Err(BoardError::forbidden(
            "narrowed or sandboxed callers cannot access sync operations or records",
        ));
    }
    Ok(())
}

/// Registers or updates a sync source with per-project key namespacing and production precedence.
pub async fn register_source(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: RegisterSyncSourceInput,
) -> BoardResult<(SyncSourceView, bool)> {
    require_sync_allowed(caller)?;
    let key = input.key.trim().to_ascii_lowercase();
    validate_slug("sync key", &key)?;
    for target in input.mappings.values() {
        target.validate()?;
    }
    if let Some(url) = &input.outbound_url {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(BoardError::bad_request(
                "outbound_url must start with http:// or https://",
            ));
        }
    }

    let direction = input
        .direction
        .unwrap_or_else(|| input.platform.default_direction());
    let (project_id, project_name, env) = caller_project_info(caller);

    let mut tx = db.begin_tx().await?;
    tx.advisory_xact_lock("sync_src", &format!("{board_id}:{project_id}:{key}"))
        .await?;

    let board_grants = grants::load_grants_tx(&mut tx, board_id).await?;
    if input.mappings.is_empty() {
        if !caller.is_unrestricted_owner()
            && !grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_sync()
        {
            return Err(BoardError::forbidden(
                "registering a sync source requires 'sync' permission",
            ));
        }
    } else {
        for target in input.mappings.values() {
            for ch in target.all_board_channels() {
                let perms = grants::caller_permissions_on_channel(&board_grants, caller, ch);
                if !caller.is_unrestricted_owner() && !perms.can_sync() {
                    return Err(BoardError::forbidden(format!(
                        "syncing channel '{ch}' requires 'sync' permission in '{ch}'"
                    )));
                }
            }
        }
    }

    let existing = tx
        .fetch_optional(
            "SELECT id, board_id, key, project_id, project_name, environment, owner_principal,
                    platform, direction, mappings_json, webhook_secret, outbound_url, outbound_token,
                    created_at, updated_at
             FROM sync_sources
             WHERE board_id = ?1 AND project_id = ?2 AND key = ?3",
            sql_args![board_id, &project_id, &key],
        )
        .await?
        .as_ref()
        .map(map_source_row)
        .transpose()?;

    let now = format_utc_ms(Utc::now());
    let mappings_json = serde_json::to_string(&input.mappings)
        .map_err(|e| BoardError::internal(format!("failed serializing mappings: {e}")))?;
    let caller_principal = caller.principal_id();

    if let Some(ex) = existing {
        if ex.environment == "production" && env != "production" {
            return Err(BoardError::conflict(format!(
                "production deployment owns sync key '{key}'; non-production environment '{env}' cannot supersede it"
            )));
        }
        if ex.environment != "production" && env != "production" && ex.environment != env {
            return Err(BoardError::conflict(format!(
                "environment '{}' already owns sync key '{key}'; only production may supersede it",
                ex.environment
            )));
        }

        tx.execute(
            "UPDATE sync_sources
             SET environment = ?1, owner_principal = ?2, platform = ?3, direction = ?4,
                 mappings_json = ?5, webhook_secret = COALESCE(?6, webhook_secret),
                 outbound_url = COALESCE(?7, outbound_url),
                 outbound_token = COALESCE(?8, outbound_token), updated_at = ?9
             WHERE id = ?10",
            sql_args![
                &env,
                &caller_principal,
                input.platform.as_str(),
                direction.as_str(),
                &mappings_json,
                &input.webhook_secret,
                &input.outbound_url,
                &input.outbound_token,
                &now,
                &ex.id
            ],
        )
        .await?;
        let updated = load_source_by_id_tx(&mut tx, &ex.id).await?.unwrap();
        tx.commit().await?;
        return Ok((updated.to_view(caller_project_id_opt(caller)), false));
    }

    let id = prefixed_id("syn");
    tx.execute(
        "INSERT INTO sync_sources (
            id, board_id, key, project_id, project_name, environment, owner_principal,
            platform, direction, mappings_json, webhook_secret, outbound_url, outbound_token,
            created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?14)",
        sql_args![
            &id,
            board_id,
            &key,
            &project_id,
            &project_name,
            &env,
            &caller_principal,
            input.platform.as_str(),
            direction.as_str(),
            &mappings_json,
            &input.webhook_secret,
            &input.outbound_url,
            &input.outbound_token,
            &now
        ],
    )
    .await?;
    let created = load_source_by_id_tx(&mut tx, &id).await?.unwrap();
    tx.commit().await?;
    Ok((created.to_view(caller_project_id_opt(caller)), true))
}

pub async fn list_sources(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
) -> BoardResult<Vec<SyncSourceView>> {
    require_sync_allowed(caller)?;
    let pid = caller_project_id_opt(caller);
    let mut conn = db.read_conn();
    let sources = load_sources_for_board_tx(&mut conn, board_id).await?;
    Ok(sources.into_iter().map(|s| s.to_view(pid)).collect())
}

pub async fn delete_source(
    db: &Database,
    board_id: &str,
    key_spec: &str,
    caller: &CallerContext,
) -> BoardResult<String> {
    require_sync_allowed(caller)?;
    let mut tx = db.begin_tx().await?;
    let src = resolve_source_by_key_tx(&mut tx, board_id, key_spec, Some(caller))
        .await?
        .ok_or_else(|| BoardError::not_found(format!("sync source '{key_spec}' not found")))?;
    let board_grants = grants::load_grants_tx(&mut tx, board_id).await?;
    let is_owner_principal = src.owner_principal == caller.principal_id();
    let has_manage_star = caller.is_unrestricted_owner()
        || grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_manage();
    if !is_owner_principal && !has_manage_star {
        return Err(BoardError::forbidden(
            "deleting another project's sync source requires 'manage' at '*'",
        ));
    }
    tx.execute("DELETE FROM sync_sources WHERE id = ?1", sql_args![&src.id])
        .await?;
    tx.commit().await?;
    Ok(src.key)
}

/// Returns raw inbound webhook inbox for `key_spec` (newest first). Requires `*` `sync`.
pub async fn get_inbox(
    db: &Database,
    board_id: &str,
    key_spec: &str,
    caller: &CallerContext,
) -> BoardResult<Vec<SyncInboxItem>> {
    require_sync_allowed(caller)?;
    let mut conn = db.read_conn();
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    let can_sync_all = grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_sync()
        && !board_grants.iter().any(|g| {
            g.can.keys().any(|pat| {
                !grants::caller_permissions_at_pattern(&board_grants, caller, pat).can_sync()
            })
        });
    if !can_sync_all {
        return Err(BoardError::forbidden(
            "reading sync inbox requires 'sync' permission in every channel ('*')",
        ));
    }
    let src = resolve_source_by_key_tx(&mut conn, board_id, key_spec, Some(caller))
        .await?
        .ok_or_else(|| BoardError::not_found(format!("sync source '{key_spec}' not found")))?;

    let rows = conn
        .fetch_all(
            "SELECT id, event_type, headers_json, body_json, received_at
             FROM sync_inbox
             WHERE board_id = ?1 AND source_id = ?2
             ORDER BY id DESC",
            sql_args![board_id, &src.id],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let h_json = r.get_str(2)?;
        let b_json = r.get_str(3)?;
        out.push(SyncInboxItem {
            id: r.get_i64(0)?,
            event_type: r.get_str(1)?,
            headers: serde_json::from_str(&h_json).unwrap_or(Value::Null),
            body: serde_json::from_str(&b_json).unwrap_or(Value::Null),
            received_at: r.get_str(4)?,
        });
    }
    Ok(out)
}

/// Returns sync links and recent deliveries for `key_spec` (`sources <key> read`). Requires `*` `manage`.
pub async fn get_source_records(
    db: &Database,
    board_id: &str,
    key_spec: &str,
    caller: &CallerContext,
) -> BoardResult<(SyncSourceView, Vec<ThreadSyncLinkView>, Vec<SyncDeliveryView>)> {
    require_sync_allowed(caller)?;
    let mut conn = db.read_conn();
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    let can_manage_all = caller.is_unrestricted_owner()
        || (grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_manage()
            && !board_grants.iter().any(|g| {
                g.can.keys().any(|pat| {
                    !grants::caller_permissions_at_pattern(&board_grants, caller, pat).can_manage()
                })
            }));
    if !can_manage_all {
        return Err(BoardError::forbidden(
            "reading sync source records requires 'manage' permission in every channel ('*')",
        ));
    }
    let src = resolve_source_by_key_tx(&mut conn, board_id, key_spec, Some(caller))
        .await?
        .ok_or_else(|| BoardError::not_found(format!("sync source '{key_spec}' not found")))?;

    let link_rows = conn
        .fetch_all(
            "SELECT id, thread_root_id, channel, platform, platform_channel, platform_thread_id,
                    platform_url, direction, created_at
             FROM thread_sync_links
             WHERE board_id = ?1 AND source_id = ?2
             ORDER BY created_at DESC",
            sql_args![board_id, &src.id],
        )
        .await?;
    let mut links = Vec::with_capacity(link_rows.len());
    for r in link_rows {
        let p_str = r.get_str(3)?;
        let d_str = r.get_str(7)?;
        links.push(ThreadSyncLinkView {
            id: r.get_str(0)?,
            thread_root_id: r.get_str(1)?,
            channel: r.get_str(2)?,
            source_key: src.key.clone(),
            platform: SyncPlatform::parse(&p_str).unwrap_or(src.platform),
            platform_channel: r.get_str(4)?,
            platform_thread_id: r.get_str(5)?,
            platform_url: r.get_opt_str(6)?,
            direction: SyncDirection::parse(&d_str).unwrap_or(src.direction),
            created_at: r.get_str(8)?,
        });
    }

    let deliv_rows = conn
        .fetch_all(
            "SELECT id, message_id, kind, status, attempts, last_http_status, last_error, updated_at
             FROM sync_deliveries
             WHERE board_id = ?1 AND source_id = ?2
             ORDER BY created_at DESC
             LIMIT 100",
            sql_args![board_id, &src.id],
        )
        .await?;
    let mut deliveries = Vec::with_capacity(deliv_rows.len());
    for r in deliv_rows {
        deliveries.push(SyncDeliveryView {
            id: r.get_str(0)?,
            message_id: r.get_str(1)?,
            kind: r.get_str(2)?,
            status: r.get_str(3)?,
            attempts: r.get_i64(4)?,
            last_http_status: r.get_opt_i64(5)?,
            last_error: r.get_opt_str(6)?,
            updated_at: r.get_str(7)?,
        });
    }

    Ok((src.to_view(caller_project_id_opt(caller)), links, deliveries))
}

/// Lists sync links for a thread (`GET .../threads/{id}/sync`).
pub async fn list_thread_links(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
) -> BoardResult<Vec<ThreadSyncLinkView>> {
    let mut conn = db.read_conn();
    let msg = messages::load_stored_message_tx(&mut conn, board_id, message_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    if !grants::caller_permissions_on_channel(&board_grants, caller, &msg.channel).can_read() {
        return Err(BoardError::new(
            ErrorCode::MessageNotFound,
            format!("message '{message_id}' not found"),
        ));
    }
    let root_id = msg.re.unwrap_or(msg.id);
    let caller_pid = caller_project_id_opt(caller);

    let rows = conn
        .fetch_all(
            "SELECT l.id, l.thread_root_id, l.channel, l.source_id, l.platform,
                    l.platform_channel, l.platform_thread_id, l.platform_url, l.direction, l.created_at
             FROM thread_sync_links l
             WHERE l.board_id = ?1 AND l.thread_root_id = ?2
             ORDER BY l.created_at ASC",
            sql_args![board_id, &root_id],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let src_id = r.get_str(3)?;
        let plat = r.get_str(4)?;
        let dir = r.get_str(8)?;
        let src_key = load_source_by_id_tx(&mut conn, &src_id)
            .await?
            .map(|s| s.qualified_key_for(caller_pid))
            .unwrap_or(src_id);
        out.push(ThreadSyncLinkView {
            id: r.get_str(0)?,
            thread_root_id: r.get_str(1)?,
            channel: r.get_str(2)?,
            source_key: src_key,
            platform: SyncPlatform::parse(&plat).unwrap_or(SyncPlatform::Custom),
            platform_channel: r.get_str(5)?,
            platform_thread_id: r.get_str(6)?,
            platform_url: r.get_opt_str(7)?,
            direction: SyncDirection::parse(&dir).unwrap_or(SyncDirection::Bidirectional),
            created_at: r.get_str(9)?,
        });
    }
    Ok(out)
}

/// Parses a target string like `slack:#incidents`, `slack:C0123:1727000.001`, `github:acme/www:123`,
/// or `www/slack:#incidents` into `(source_key, platform_channel, platform_thread_id)`.
fn parse_sync_target(target: &str) -> BoardResult<(String, String, String)> {
    let trimmed = target.trim();
    let (source_key, rest) = trimmed.split_once(':').ok_or_else(|| {
        BoardError::bad_request(format!(
            "invalid sync target '{target}'; expected '<source_key>:<platform_channel>[:<thread_id>]'"
        ))
    })?;
    if source_key.is_empty() || rest.is_empty() {
        return Err(BoardError::bad_request(format!(
            "invalid sync target '{target}'"
        )));
    }
    match rest.split_once(':') {
        Some((ch, tid)) if !ch.is_empty() && !tid.is_empty() => {
            Ok((source_key.to_string(), ch.to_string(), tid.to_string()))
        }
        _ => Ok((
            source_key.to_string(),
            rest.to_string(),
            prefixed_id("pth"),
        )),
    }
}

/// Links an existing board thread to a platform target (`POST .../threads/{id}/sync`)
/// and queues backfill of non-`noSync` messages (unless root has `noSync: true`).
pub async fn link_thread(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: LinkThreadInput,
) -> BoardResult<(ThreadSyncLinkView, Vec<i64>)> {
    require_sync_allowed(caller)?;
    let (source_key, platform_channel, platform_thread_id) = parse_sync_target(&input.target)?;

    let mut tx = db.begin_tx().await?;
    let msg = messages::load_stored_message_tx(&mut tx, board_id, message_id)
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
    if !perms.can_sync() {
        return Err(BoardError::forbidden(format!(
            "syncing a thread in '{}' requires 'sync' permission",
            msg.channel
        )));
    }

    let root_id = msg.re.clone().unwrap_or_else(|| msg.id.clone());
    let root_msg = messages::load_stored_message_tx(&mut tx, board_id, &root_id)
        .await?
        .unwrap();
    let src = resolve_source_by_key_tx(&mut tx, board_id, &source_key, Some(caller))
        .await?
        .ok_or_else(|| BoardError::not_found(format!("sync source '{source_key}' not found")))?;

    tx.advisory_xact_lock(
        "sync_link",
        &format!("{board_id}:{}:{platform_channel}:{platform_thread_id}", src.id),
    )
    .await?;

    let existing_root = tx
        .fetch_optional(
            "SELECT thread_root_id FROM thread_sync_links
             WHERE board_id = ?1 AND source_id = ?2 AND platform_channel = ?3 AND platform_thread_id = ?4",
            sql_args![board_id, &src.id, &platform_channel, &platform_thread_id],
        )
        .await?
        .map(|r| r.get_str(0))
        .transpose()?;
    if let Some(other_root) = existing_root {
        if other_root != root_id {
            return Err(BoardError::new(
                ErrorCode::AlreadySynced,
                format!(
                    "platform thread '{platform_channel}:{platform_thread_id}' is already linked to another board thread"
                ),
            ));
        }
        return Err(BoardError::new(
            ErrorCode::AlreadySynced,
            format!(
                "thread '{root_id}' is already linked to '{platform_channel}:{platform_thread_id}'"
            ),
        ));
    }

    let link_id = prefixed_id("lnk");
    let now = format_utc_ms(Utc::now());
    tx.execute(
        "INSERT INTO thread_sync_links (
            id, board_id, thread_root_id, channel, source_id, platform,
            platform_channel, platform_thread_id, platform_url, direction, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        sql_args![
            &link_id,
            board_id,
            &root_id,
            &root_msg.channel,
            &src.id,
            src.platform.as_str(),
            &platform_channel,
            &platform_thread_id,
            &input.platform_url,
            src.direction.as_str(),
            &now
        ],
    )
    .await?;

    let snap = messages::build_message_snapshot_tx(&mut tx, &root_msg).await?;
    let root_view = messages::hydrate_message_view_tx(&mut tx, &root_msg).await?;
    let actor_id = caller.actor_id();
    let ev = events::append_event_tx(
        &mut tx,
        EventAppend {
            board_id,
            event_type: EventType::ThreadSynced,
            channel: Some(&root_msg.channel),
            message_id: Some(&root_id),
            actor_id: Some(&actor_id),
            payload: json!({
                "sourceKey": src.key,
                "platform": src.platform.as_str(),
                "platformChannel": platform_channel,
                "platformThreadId": platform_thread_id,
                "platformUrl": input.platform_url
            }),
            before_snapshot: Some(&snap),
            after_snapshot: Some(&snap),
            after_message: Some(&root_view),
        },
    )
    .await?;

    if src.direction.allows_outbound() && !root_msg.no_sync {
        let id_rows = tx
            .fetch_all(
                "SELECT id FROM messages
                 WHERE board_id = ?1 AND (id = ?2 OR re = ?2) AND deleted_at IS NULL
                 ORDER BY seq ASC",
                sql_args![board_id, &root_id],
            )
            .await?;
        for r in id_rows {
            let mid = r.get_str(0)?;
            if let Some(m) = messages::load_stored_message_tx(&mut tx, board_id, &mid).await? {
                if m.no_sync || m.from_source.as_deref() == Some(src.key.as_str()) {
                    continue;
                }
                let kind = if m.re.is_none() { "post" } else { "reply" };
                insert_delivery_for_link_tx(
                    &mut tx,
                    board_id,
                    &src,
                    &link_id,
                    &platform_channel,
                    &platform_thread_id,
                    &m,
                    kind,
                    None,
                )
                .await?;
            }
        }
    }

    tx.commit().await?;
    let view = ThreadSyncLinkView {
        id: link_id,
        thread_root_id: root_id,
        channel: root_msg.channel,
        source_key: src.qualified_key_for(caller_project_id_opt(caller)),
        platform: src.platform,
        platform_channel,
        platform_thread_id,
        platform_url: input.platform_url,
        direction: src.direction,
        created_at: now,
    };
    Ok((view, vec![ev.seq]))
}

/// Finds the board thread linked to an external URL (`GET .../sync/linked?url=...`),
/// visibility-filtered to caller's readable channels.
pub async fn find_linked_thread(
    db: &Database,
    board_id: &str,
    url: &str,
    caller: &CallerContext,
) -> BoardResult<ThreadView> {
    let trimmed = url.trim();
    if trimmed.is_empty() {
        return Err(BoardError::bad_request("missing 'url' query parameter"));
    }
    let root_id: String = {
        let mut conn = db.read_conn();
        let link = conn
            .fetch_optional(
                "SELECT thread_root_id, channel FROM thread_sync_links
                 WHERE board_id = ?1 AND platform_url = ?2
                 LIMIT 1",
                sql_args![board_id, trimmed],
            )
            .await?;
        let found = match link {
            Some(r) => Some((r.get_str(0)?, r.get_str(1)?)),
            None => conn
                .fetch_optional(
                    "SELECT COALESCE(m.re, m.id), m.channel
                     FROM message_sync_map ms
                     JOIN messages m ON m.id = ms.message_id
                     WHERE ms.board_id = ?1 AND ms.platform_url = ?2
                     LIMIT 1",
                    sql_args![board_id, trimmed],
                )
                .await?
                .map(|r| Ok::<_, BoardError>((r.get_str(0)?, r.get_str(1)?)))
                .transpose()?,
        };
        let (root_id, channel) = found.ok_or_else(|| {
            BoardError::not_found(format!("no synced thread found for URL '{trimmed}'"))
        })?;
        let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
        if !grants::caller_permissions_on_channel(&board_grants, caller, &channel).can_read() {
            return Err(BoardError::not_found(format!(
                "no synced thread found for URL '{trimmed}'"
            )));
        }
        root_id
    };

    messages::get_thread(db, board_id, &root_id, caller).await
}

/// Forwards typing indicator (`status <= 50 chars`) to all linked outbound sync sources.
pub async fn send_typing(
    state: &AppState,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: TypingInput,
) -> BoardResult<Vec<String>> {
    if let Some(status) = &input.status {
        if status.chars().count() > MAX_TYPING_STATUS_CHARS {
            return Err(BoardError::bad_request(format!(
                "typing status must be at most {MAX_TYPING_STATUS_CHARS} characters"
            )));
        }
    }

    struct TypingTarget {
        key: String,
        platform: String,
        outbound_url: Option<String>,
        outbound_token: Option<String>,
        pch: String,
        pth: String,
    }

    let targets: Vec<TypingTarget> = {
        let mut conn = state.db.read_conn();
        let msg = messages::load_stored_message_tx(&mut conn, board_id, message_id)
            .await?
            .ok_or_else(|| {
                BoardError::new(
                    ErrorCode::MessageNotFound,
                    format!("message '{message_id}' not found"),
                )
            })?;
        let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
        let perms = grants::caller_permissions_on_channel(&board_grants, caller, &msg.channel);
        if !perms.can_read() {
            return Err(BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            ));
        }
        if !perms.can_post_new_thread() && !perms.can_sync() {
            return Err(BoardError::forbidden(
                "typing indicator requires 'post' or 'sync' permission",
            ));
        }
        let root_id = msg.re.unwrap_or(msg.id);
        let rows = conn
            .fetch_all(
                "SELECT s.key, s.platform, s.outbound_url, s.outbound_token, l.platform_channel, l.platform_thread_id
                 FROM thread_sync_links l
                 JOIN sync_sources s ON s.id = l.source_id
                 WHERE l.board_id = ?1 AND l.thread_root_id = ?2",
                sql_args![board_id, &root_id],
            )
            .await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            out.push(TypingTarget {
                key: r.get_str(0)?,
                platform: r.get_str(1)?,
                outbound_url: r.get_opt_str(2)?,
                outbound_token: r.get_opt_str(3)?,
                pch: r.get_str(4)?,
                pth: r.get_str(5)?,
            });
        }
        out
    };

    let mut notified = Vec::new();
    for t in targets {
        let label = format!("{}:{}:{}", t.key, t.pch, t.pth);
        if let Some(url) = t.outbound_url {
            let payload = json!({
                "kind": "typing",
                "platform": t.platform,
                "platformChannel": t.pch,
                "platformThreadId": t.pth,
                "status": input.status,
                "actor": caller.handle()
            });
            let mut req = state.http.post(&url).json(&payload);
            if let Some(tok) = t.outbound_token {
                req = req.header("authorization", format!("Bearer {tok}"));
            }
            let _ = req.send().await;
        }
        notified.push(label);
    }
    Ok(notified)
}

// --- Outbound Enqueue Hook (called from messages.rs on post/reply/edit/delete/react) ---

/// Enqueues outbound sync deliveries inside an active message write transaction.
///
/// Enforces:
/// - NEVER syncs `noSync: true` messages or ANY reply inside a `noSync: true` root thread
/// - NEVER syncs labels (labels are board-only)
/// - NEVER echoes back to the originating `from_source`
/// - Only delivers to `bidirectional` / `outbound` sources (skips `inbound`-only like GitHub default)
pub async fn enqueue_outbound_for_message_tx(
    tx: &mut DbTx,
    board_id: &str,
    message_id: &str,
    kind: &str,
    extra: Option<Value>,
) -> BoardResult<bool> {
    let Some(msg) = messages::load_stored_message_tx(tx, board_id, message_id).await? else {
        return Ok(false);
    };
    if msg.no_sync {
        return Ok(false);
    }

    let root_id = msg.re.clone().unwrap_or_else(|| msg.id.clone());
    if msg.re.is_some() {
        if let Some(root) = messages::load_stored_message_tx(tx, board_id, &root_id).await? {
            if root.no_sync {
                return Ok(false);
            }
        }
    }

    let mut queued_any = false;

    if kind == "post" && msg.re.is_none() {
        let sources = load_sources_for_board_tx(tx, board_id).await?;
        for src in sources {
            if !src.direction.allows_outbound() {
                continue;
            }
            if msg.from_source.as_deref() == Some(src.key.as_str()) {
                continue;
            }
            for (platform_ch, mapping) in &src.mappings {
                if mapping
                    .all_board_channels()
                    .iter()
                    .any(|&bc| bc == msg.channel)
                {
                    let exists = tx
                        .fetch_optional(
                            "SELECT 1 FROM thread_sync_links
                             WHERE board_id = ?1 AND thread_root_id = ?2 AND source_id = ?3 AND platform_channel = ?4",
                            sql_args![board_id, &root_id, &src.id, platform_ch],
                        )
                        .await?
                        .is_some();
                    if !exists {
                        let link_id = prefixed_id("lnk");
                        let pth = prefixed_id("pth");
                        let now = format_utc_ms(Utc::now());
                        tx.execute(
                            "INSERT INTO thread_sync_links (
                                id, board_id, thread_root_id, channel, source_id, platform,
                                platform_channel, platform_thread_id, platform_url, direction, created_at
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, ?9, ?10)",
                            sql_args![
                                &link_id,
                                board_id,
                                &root_id,
                                &msg.channel,
                                &src.id,
                                src.platform.as_str(),
                                platform_ch,
                                &pth,
                                src.direction.as_str(),
                                &now
                            ],
                        )
                        .await?;
                    }
                }
            }
        }
    }

    let link_rows = tx
        .fetch_all(
            "SELECT id, source_id, platform_channel, platform_thread_id, direction
             FROM thread_sync_links
             WHERE board_id = ?1 AND thread_root_id = ?2",
            sql_args![board_id, &root_id],
        )
        .await?;

    for r in link_rows {
        let link_id = r.get_str(0)?;
        let src_id = r.get_str(1)?;
        let pch = r.get_str(2)?;
        let pth = r.get_str(3)?;
        let dir_str = r.get_str(4)?;

        let dir = SyncDirection::parse(&dir_str).unwrap_or(SyncDirection::Inbound);
        if !dir.allows_outbound() {
            continue;
        }
        let Some(src) = load_source_by_id_tx(tx, &src_id).await? else {
            continue;
        };
        if !src.direction.allows_outbound() {
            continue;
        }
        if msg.from_source.as_deref() == Some(src.key.as_str()) {
            continue;
        }
        insert_delivery_for_link_tx(
            tx,
            board_id,
            &src,
            &link_id,
            &pch,
            &pth,
            &msg,
            kind,
            extra.clone(),
        )
        .await?;
        queued_any = true;
    }

    Ok(queued_any)
}

#[allow(clippy::too_many_arguments)]
async fn insert_delivery_for_link_tx(
    tx: &mut DbTx,
    board_id: &str,
    src: &StoredSyncSource,
    link_id: &str,
    platform_channel: &str,
    platform_thread_id: &str,
    msg: &StoredMessageRow,
    kind: &str,
    extra: Option<Value>,
) -> BoardResult<()> {
    let view = messages::hydrate_message_view_tx(tx, msg).await?;
    let mut payload = match src.platform {
        SyncPlatform::Slack => json!({
            "kind": kind,
            "platform": "slack",
            "sourceKey": src.key,
            "channel": platform_channel,
            "threadId": platform_thread_id,
            "messageId": view.id,
            "text": gfm_to_slack_mrkdwn(&view.text),
            "author": { "actor": view.by.actor, "handle": view.by.handle },
            "attachments": view.attachments
        }),
        SyncPlatform::Linear => {
            let (title, body) = split_linear_title_body(&view.text);
            json!({
                "kind": kind,
                "platform": "linear",
                "sourceKey": src.key,
                "team": platform_channel,
                "threadId": platform_thread_id,
                "messageId": view.id,
                "title": title,
                "body": body,
                "text": view.text,
                "author": { "actor": view.by.actor, "handle": view.by.handle },
                "attachments": view.attachments
            })
        }
        SyncPlatform::Github | SyncPlatform::Custom => json!({
            "kind": kind,
            "platform": src.platform.as_str(),
            "sourceKey": src.key,
            "channel": platform_channel,
            "threadId": platform_thread_id,
            "messageId": view.id,
            "text": view.text,
            "author": { "actor": view.by.actor, "handle": view.by.handle },
            "attachments": view.attachments
        }),
    };

    if let Some(ex) = extra {
        if let (Some(dst), Some(src_obj)) = (payload.as_object_mut(), ex.as_object()) {
            for (k, v) in src_obj {
                dst.insert(k.clone(), v.clone());
            }
        }
    }

    let id = prefixed_id("sdl");
    let now = format_utc_ms(Utc::now());
    let payload_json = serde_json::to_string(&payload)
        .map_err(|e| BoardError::internal(format!("failed serializing delivery payload: {e}")))?;

    tx.execute(
        "INSERT INTO sync_deliveries (
            id, board_id, source_id, link_id, message_id, kind, payload_json,
            status, attempts, last_http_status, last_error, next_attempt_at, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'pending', 0, NULL, NULL, ?8, ?8, ?8)",
        sql_args![
            &id,
            board_id,
            &src.id,
            link_id,
            &msg.id,
            kind,
            &payload_json,
            &now
        ],
    )
    .await?;
    tx.queue_sync_notify();
    Ok(())
}

// --- Outbound Delivery Worker ---

const SYNC_CLAIM_BATCH_LIMIT: i64 = 25;
const SYNC_LEASE_SECS: i64 = 60;

struct ClaimedSyncDelivery {
    id: String,
    board_id: String,
    source_id: String,
    link_id: String,
    message_id: String,
    kind: String,
    payload_json: String,
    prev_attempts: i64,
}

async fn claim_due_deliveries(
    db: &Database,
    lease_owner: &str,
    now: DateTime<Utc>,
) -> BoardResult<Vec<ClaimedSyncDelivery>> {
    let now_str = format_utc_ms(now);
    let lock_until = format_utc_ms(now + Duration::seconds(SYNC_LEASE_SECS));

    let mut tx = db.begin_tx().await?;
    let rows = match tx.dialect() {
        Dialect::Postgres => {
            tx.fetch_all(
                "UPDATE sync_deliveries
                 SET locked_until = ?1, locked_by = ?2, updated_at = ?3
                 WHERE id IN (
                     SELECT id FROM sync_deliveries
                     WHERE status IN ('pending', 'retrying')
                       AND next_attempt_at <= ?3
                       AND (locked_until IS NULL OR locked_until <= ?3)
                     ORDER BY created_at ASC
                     LIMIT ?4
                     FOR UPDATE SKIP LOCKED
                 )
                 RETURNING id, board_id, source_id, link_id, message_id, kind, payload_json, attempts",
                sql_args![&lock_until, lease_owner, &now_str, SYNC_CLAIM_BATCH_LIMIT],
            )
            .await?
        }
        Dialect::Sqlite => {
            let selected = tx
                .fetch_all(
                    "SELECT id, board_id, source_id, link_id, message_id, kind, payload_json, attempts
                     FROM sync_deliveries
                     WHERE status IN ('pending', 'retrying')
                       AND next_attempt_at <= ?1
                       AND (locked_until IS NULL OR locked_until <= ?1)
                     ORDER BY created_at ASC
                     LIMIT ?2",
                    sql_args![&now_str, SYNC_CLAIM_BATCH_LIMIT],
                )
                .await?;
            for r in &selected {
                let deliv_id = r.get_str(0)?;
                tx.execute(
                    "UPDATE sync_deliveries
                     SET locked_until = ?1, locked_by = ?2, updated_at = ?3
                     WHERE id = ?4",
                    sql_args![&lock_until, lease_owner, &now_str, &deliv_id],
                )
                .await?;
            }
            selected
        }
    };
    tx.commit().await?;

    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(ClaimedSyncDelivery {
            id: r.get_str(0)?,
            board_id: r.get_str(1)?,
            source_id: r.get_str(2)?,
            link_id: r.get_str(3)?,
            message_id: r.get_str(4)?,
            kind: r.get_str(5)?,
            payload_json: r.get_str(6)?,
            prev_attempts: r.get_i64(7)?,
        });
    }
    Ok(out)
}

pub async fn process_pending_deliveries(state: &AppState) -> BoardResult<DeliveryTickSummary> {
    process_pending_deliveries_at(state, Utc::now()).await
}

pub async fn process_pending_deliveries_at(
    state: &AppState,
    now: DateTime<Utc>,
) -> BoardResult<DeliveryTickSummary> {
    let now_str = format_utc_ms(now);
    let lease_owner = format!("{}:{}", state.replica_id, prefixed_id("sl"));
    let due = claim_due_deliveries(&state.db, &lease_owner, now).await?;

    let mut summary = DeliveryTickSummary::default();

    for item in due {
        let ClaimedSyncDelivery {
            id: deliv_id,
            board_id,
            source_id,
            link_id,
            message_id,
            kind,
            payload_json,
            prev_attempts,
        } = item;

        let source = {
            let mut conn = state.db.read_conn();
            load_source_by_id_tx(&mut conn, &source_id).await?
        };
        let Some(src) = source else {
            let mut tx = state.db.begin_tx().await?;
            tx.execute(
                "UPDATE sync_deliveries
                 SET status = 'failed', last_error = 'sync source deleted',
                     locked_until = NULL, locked_by = NULL, updated_at = ?1
                 WHERE id = ?2 AND locked_by = ?3",
                sql_args![&now_str, &deliv_id, &lease_owner],
            )
            .await?;
            tx.commit().await?;
            summary.failed += 1;
            continue;
        };
        let Some(outbound_url) = src.outbound_url.as_deref() else {
            let mut tx = state.db.begin_tx().await?;
            let updated = tx
                .execute(
                    "UPDATE sync_deliveries
                     SET status = 'delivered', attempts = ?1,
                         locked_until = NULL, locked_by = NULL, updated_at = ?2
                     WHERE id = ?3 AND locked_by = ?4",
                    sql_args![prev_attempts + 1, &now_str, &deliv_id, &lease_owner],
                )
                .await?;
            tx.commit().await?;
            if updated > 0 {
                summary.delivered += 1;
            }
            continue;
        };

        let attempt = prev_attempts + 1;
        let mut req = state
            .http
            .post(outbound_url)
            .header("content-type", "application/json")
            .header("x-board-delivery-id", &deliv_id)
            .header("x-board-attempt", attempt.to_string())
            .body(payload_json.clone());

        if let Some(tok) = &src.outbound_token {
            req = req.header("authorization", format!("Bearer {tok}"));
        }
        if let Some(sec) = &src.webhook_secret {
            let sig = hmac_sha256_hex(sec.as_bytes(), payload_json.as_bytes());
            req = req.header("x-board-signature", format!("sha256={sig}"));
        }

        let send_res = req.send().await;
        let (ok, status_code, resp_json, err_msg) = match send_res {
            Ok(resp) => {
                let st = resp.status();
                let code = st.as_u16() as i64;
                let bytes = resp.bytes().await.unwrap_or_default();
                let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                if st.is_success() {
                    (true, Some(code), Some(parsed), None)
                } else {
                    (false, Some(code), None, Some(format!("HTTP {code}")))
                }
            }
            Err(e) => (false, None, None, Some(e.to_string())),
        };

        if ok {
            let mut tx = state.db.begin_tx().await?;
            let updated = tx
                .execute(
                    "UPDATE sync_deliveries
                     SET status = 'delivered', attempts = ?1, last_http_status = ?2,
                         last_error = NULL, locked_until = NULL, locked_by = NULL, updated_at = ?3
                     WHERE id = ?4 AND locked_by = ?5",
                    sql_args![attempt, status_code, &now_str, &deliv_id, &lease_owner],
                )
                .await?;
            if updated == 0 {
                tx.commit().await?;
                continue;
            }

            if let Some(body) = &resp_json {
                if let Some(p_tid) = body["platformThreadId"].as_str() {
                    let _ = tx
                        .execute(
                            "UPDATE thread_sync_links
                             SET platform_thread_id = ?1,
                                 platform_url = COALESCE(?2, platform_url)
                             WHERE id = ?3",
                            sql_args![p_tid, body["platformUrl"].as_str(), &link_id],
                        )
                        .await;
                }
                if let Some(p_mid) = body["platformMessageId"].as_str() {
                    let _ = tx
                        .execute(
                            "INSERT INTO message_sync_map (
                                board_id, source_id, message_id, platform_message_id, platform_url, created_at
                             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                             ON CONFLICT DO NOTHING",
                            sql_args![
                                &board_id,
                                &source_id,
                                &message_id,
                                p_mid,
                                body["platformUrl"].as_str(),
                                &now_str
                            ],
                        )
                        .await;
                }
            }

            let mut seqs = Vec::new();
            if let Some(stored) =
                messages::load_stored_message_tx(&mut tx, &board_id, &message_id).await?
            {
                let snap = messages::build_message_snapshot_tx(&mut tx, &stored).await?;
                let view = messages::hydrate_message_view_tx(&mut tx, &stored).await?;
                let ev = events::append_event_tx(
                    &mut tx,
                    EventAppend {
                        board_id: &board_id,
                        event_type: EventType::MessageSynced,
                        channel: Some(&stored.channel),
                        message_id: Some(&message_id),
                        actor_id: Some(&src.owner_principal),
                        payload: json!({
                            "sourceKey": src.key,
                            "platform": src.platform.as_str(),
                            "kind": kind
                        }),
                        before_snapshot: Some(&snap),
                        after_snapshot: Some(&snap),
                        after_message: Some(&view),
                    },
                )
                .await?;
                seqs.push(ev.seq);

                if !view.attachments.is_empty() && matches!(kind.as_str(), "post" | "reply") {
                    let ev_files = events::append_event_tx(
                        &mut tx,
                        EventAppend {
                            board_id: &board_id,
                            event_type: EventType::MessageFilesSynced,
                            channel: Some(&stored.channel),
                            message_id: Some(&message_id),
                            actor_id: Some(&src.owner_principal),
                            payload: json!({
                                "sourceKey": src.key,
                                "count": view.attachments.len()
                            }),
                            before_snapshot: Some(&snap),
                            after_snapshot: Some(&snap),
                            after_message: Some(&view),
                        },
                    )
                    .await?;
                    seqs.push(ev_files.seq);
                }
            }
            tx.commit().await?;
            for seq in seqs {
                state.events.notify(&board_id, seq);
            }
            summary.delivered += 1;
        } else {
            let is_best_effort = matches!(kind.as_str(), "edit" | "delete" | "react" | "unreact");
            if is_best_effort {
                let mut tx = state.db.begin_tx().await?;
                let updated = tx
                    .execute(
                        "UPDATE sync_deliveries
                         SET status = 'failed', attempts = ?1, last_http_status = ?2,
                             last_error = ?3, locked_until = NULL, locked_by = NULL, updated_at = ?4
                         WHERE id = ?5 AND locked_by = ?6",
                        sql_args![
                            attempt,
                            status_code,
                            &err_msg,
                            &now_str,
                            &deliv_id,
                            &lease_owner
                        ],
                    )
                    .await?;
                tx.commit().await?;
                if updated > 0 {
                    summary.failed += 1;
                }
            } else if attempt < MAX_SYNC_RETRIES {
                let backoff_secs = 1i64 << (attempt - 1).min(5);
                let next_at = format_utc_ms(now + Duration::seconds(backoff_secs));
                let mut tx = state.db.begin_tx().await?;
                let updated = tx
                    .execute(
                        "UPDATE sync_deliveries
                         SET status = 'retrying', attempts = ?1, last_http_status = ?2,
                             last_error = ?3, next_attempt_at = ?4,
                             locked_until = NULL, locked_by = NULL, updated_at = ?5
                         WHERE id = ?6 AND locked_by = ?7",
                        sql_args![
                            attempt,
                            status_code,
                            &err_msg,
                            &next_at,
                            &now_str,
                            &deliv_id,
                            &lease_owner
                        ],
                    )
                    .await?;
                tx.commit().await?;
                if updated > 0 {
                    summary.retried += 1;
                }
            } else {
                let mut tx = state.db.begin_tx().await?;
                let updated = tx
                    .execute(
                        "UPDATE sync_deliveries
                         SET status = 'failed', attempts = ?1, last_http_status = ?2,
                             last_error = ?3, locked_until = NULL, locked_by = NULL, updated_at = ?4
                         WHERE id = ?5 AND locked_by = ?6",
                        sql_args![
                            attempt,
                            status_code,
                            &err_msg,
                            &now_str,
                            &deliv_id,
                            &lease_owner
                        ],
                    )
                    .await?;
                if updated == 0 {
                    tx.commit().await?;
                    continue;
                }
                let emitted_seq = if let Some(stored) =
                    messages::load_stored_message_tx(&mut tx, &board_id, &message_id).await?
                {
                    let snap = messages::build_message_snapshot_tx(&mut tx, &stored).await?;
                    let view = messages::hydrate_message_view_tx(&mut tx, &stored).await?;
                    let ev = events::append_event_tx(
                        &mut tx,
                        EventAppend {
                            board_id: &board_id,
                            event_type: EventType::MessageSyncFailed,
                            channel: Some(&stored.channel),
                            message_id: Some(&message_id),
                            actor_id: Some(&src.owner_principal),
                            payload: json!({
                                "sourceKey": src.key,
                                "platform": src.platform.as_str(),
                                "attempts": attempt,
                                "error": err_msg
                            }),
                            before_snapshot: Some(&snap),
                            after_snapshot: Some(&snap),
                            after_message: Some(&view),
                        },
                    )
                    .await?;
                    Some(ev.seq)
                } else {
                    None
                };
                tx.commit().await?;
                if let Some(seq) = emitted_seq {
                    state.events.notify(&board_id, seq);
                }
                summary.failed += 1;
            }
        }
    }

    Ok(summary)
}

pub fn spawn_sync_worker(state: AppState) -> tokio::task::JoinHandle<()> {
    spawn_sync_worker_with_shutdown(state, crate::never_shutdown_rx())
}

pub fn spawn_sync_worker_with_shutdown(
    state: AppState,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {}
                _ = state.sync_wake.notified() => {}
            }
            if *shutdown.borrow() {
                break;
            }
            if let Err(err) = process_pending_deliveries(&state).await {
                tracing::warn!("sync delivery worker error: {err}");
            }
        }
    })
}

// --- Inbound Webhook Processing (Slack, Linear, GitHub, Custom) ---

pub async fn handle_inbound_webhook(
    state: &AppState,
    board_id_opt: Option<&str>,
    key_spec: &str,
    headers: &HeaderMap,
    raw_body: &[u8],
) -> BoardResult<Value> {
    let src: StoredSyncSource = {
        let mut conn = state.db.read_conn();
        if let Some(bid) = board_id_opt {
            resolve_source_by_key_tx(&mut conn, bid, key_spec, None)
                .await?
                .ok_or_else(|| BoardError::not_found(format!("sync source '{key_spec}' not found")))?
        } else {
            let row = conn
                .fetch_optional(
                    "SELECT id, board_id, key, project_id, project_name, environment, owner_principal,
                            platform, direction, mappings_json, webhook_secret, outbound_url, outbound_token,
                            created_at, updated_at
                     FROM sync_sources
                     WHERE key = ?1
                     ORDER BY CASE WHEN environment = 'production' THEN 0 ELSE 1 END, created_at ASC
                     LIMIT 1",
                    sql_args![key_spec],
                )
                .await?
                .ok_or_else(|| {
                    BoardError::not_found(format!("sync source '{key_spec}' not found"))
                })?;
            map_source_row(&row)?
        }
    };

    verify_webhook_signature(&src, headers, raw_body)?;

    let body: Value = serde_json::from_slice(raw_body)
        .map_err(|e| BoardError::bad_request(format!("invalid webhook JSON body: {e}")))?;

    if src.platform == SyncPlatform::Slack && body["type"].as_str() == Some("url_verification") {
        let challenge = body["challenge"].as_str().unwrap_or("");
        return Ok(json!({ "challenge": challenge }));
    }

    let event_label = detect_inbound_event_label(src.platform, headers, &body);
    let headers_val = headers_to_safe_json(headers);
    let headers_str = headers_val.to_string();
    let body_str = body.to_string();
    let now_str = format_utc_ms(Utc::now());

    {
        let mut tx = state.db.begin_tx().await?;
        tx.execute(
            "INSERT INTO sync_inbox (board_id, source_id, event_type, headers_json, body_json, received_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            sql_args![
                &src.board_id,
                &src.id,
                &event_label,
                &headers_str,
                &body_str,
                &now_str
            ],
        )
        .await?;
        tx.execute(
            "DELETE FROM sync_inbox
             WHERE board_id = ?1 AND source_id = ?2
               AND id NOT IN (
                   SELECT id FROM sync_inbox
                   WHERE board_id = ?1 AND source_id = ?2
                   ORDER BY id DESC LIMIT ?3
               )",
            sql_args![&src.board_id, &src.id, MAX_INBOX_PER_SOURCE],
        )
        .await?;
        tx.commit().await?;
    }

    if !src.direction.allows_inbound() {
        return Ok(json!({ "ok": true, "ignored": true, "reason": "outbound_only" }));
    }

    let (result, emitted_seqs) = {
        let mut tx = state.db.begin_tx().await?;
        let res = match src.platform {
            SyncPlatform::Slack => process_slack_inbound_tx(&mut tx, &src, &body).await?,
            SyncPlatform::Linear => process_linear_inbound_tx(&mut tx, &src, &body).await?,
            SyncPlatform::Github => {
                process_github_inbound_tx(&mut tx, &src, headers, &body).await?
            }
            SyncPlatform::Custom => process_custom_inbound_tx(&mut tx, &src, &body).await?,
        };
        tx.commit().await?;
        res
    };

    for seq in emitted_seqs {
        state.events.notify(&src.board_id, seq);
    }
    state.sync_wake.notify_one();

    Ok(result)
}

fn verify_webhook_signature(
    src: &StoredSyncSource,
    headers: &HeaderMap,
    raw_body: &[u8],
) -> BoardResult<()> {
    let Some(secret) = src.webhook_secret.as_deref() else {
        return Ok(());
    };
    match src.platform {
        SyncPlatform::Slack => {
            let sig = headers
                .get("x-slack-signature")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| BoardError::unauthorized("missing X-Slack-Signature header"))?;
            let ts = headers
                .get("x-slack-request-timestamp")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    BoardError::unauthorized("missing X-Slack-Request-Timestamp header")
                })?;
            let mut base = format!("v0:{ts}:").into_bytes();
            base.extend_from_slice(raw_body);
            let expected = format!("v0={}", hmac_sha256_hex(secret.as_bytes(), &base));
            if !constant_time_eq(sig.as_bytes(), expected.as_bytes()) {
                return Err(BoardError::unauthorized("invalid X-Slack-Signature"));
            }
        }
        SyncPlatform::Linear => {
            let sig = headers
                .get("linear-signature")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| BoardError::unauthorized("missing Linear-Signature header"))?;
            let expected = hmac_sha256_hex(secret.as_bytes(), raw_body);
            if !constant_time_eq(sig.as_bytes(), expected.as_bytes()) {
                return Err(BoardError::unauthorized("invalid Linear-Signature"));
            }
        }
        SyncPlatform::Github => {
            let sig = headers
                .get("x-hub-signature-256")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| BoardError::unauthorized("missing X-Hub-Signature-256 header"))?;
            let expected = format!("sha256={}", hmac_sha256_hex(secret.as_bytes(), raw_body));
            if !constant_time_eq(sig.as_bytes(), expected.as_bytes()) {
                return Err(BoardError::unauthorized("invalid X-Hub-Signature-256"));
            }
        }
        SyncPlatform::Custom => {
            let sig = headers
                .get("x-board-signature")
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| BoardError::unauthorized("missing X-Board-Signature header"))?;
            let expected = format!("sha256={}", hmac_sha256_hex(secret.as_bytes(), raw_body));
            if !constant_time_eq(sig.as_bytes(), expected.as_bytes()) {
                return Err(BoardError::unauthorized("invalid X-Board-Signature"));
            }
        }
    }
    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn detect_inbound_event_label(
    platform: SyncPlatform,
    headers: &HeaderMap,
    body: &Value,
) -> String {
    match platform {
        SyncPlatform::Slack => body["event"]["type"]
            .as_str()
            .or_else(|| body["type"].as_str())
            .unwrap_or("slack.event")
            .to_string(),
        SyncPlatform::Linear => format!(
            "{}.{}",
            body["type"].as_str().unwrap_or("Linear"),
            body["action"].as_str().unwrap_or("event")
        ),
        SyncPlatform::Github => headers
            .get("x-github-event")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("github.event")
            .to_string(),
        SyncPlatform::Custom => body["action"]
            .as_str()
            .unwrap_or("custom.event")
            .to_string(),
    }
}

fn headers_to_safe_json(headers: &HeaderMap) -> Value {
    let mut map = serde_json::Map::new();
    for (k, v) in headers {
        let lower = k.as_str().to_ascii_lowercase();
        if lower == "authorization" || lower.contains("secret") || lower.contains("token") {
            continue;
        }
        if let Ok(s) = v.to_str() {
            map.insert(lower, Value::String(s.to_string()));
        }
    }
    Value::Object(map)
}

/// Upserts a synced platform user profile `<owner_principal>+<source_key>:user:<platform_user_id>`
/// and inserts a synced board message (root or reply), setting `from_source = src.key` so it
/// never echoes back to `src`.
#[allow(clippy::too_many_arguments)]
async fn insert_synced_message_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    board_channel: &str,
    thread_root: Option<&str>,
    platform_msg_id: &str,
    platform_url: Option<&str>,
    text: &str,
    platform_user_id: &str,
    platform_user_name: &str,
    platform_user_avatar: Option<&str>,
    initial_labels: &BTreeMap<String, String>,
) -> BoardResult<(MessageView, Vec<i64>)> {
    let now = format_utc_ms(Utc::now());
    let profile_key = format!(
        "{}:user:{}",
        src.key,
        platform_user_id.trim().to_ascii_lowercase()
    );
    let actor_id = format!("{}+{}", src.owner_principal, profile_key);

    tx.execute(
        "INSERT INTO channels (board_id, path, created_by, created_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT (board_id, path) DO NOTHING",
        sql_args![&src.board_id, board_channel, &actor_id, &now],
    )
    .await?;

    tx.execute(
        "INSERT INTO profiles (principal_id, name_lower, name, avatar, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)
         ON CONFLICT(principal_id, name_lower) DO UPDATE SET
           name = excluded.name,
           avatar = COALESCE(excluded.avatar, profiles.avatar),
           updated_at = excluded.updated_at",
        sql_args![
            &src.owner_principal,
            &profile_key,
            platform_user_name,
            platform_user_avatar,
            &now
        ],
    )
    .await?;
    tx.execute(
        "INSERT INTO actor_activity (board_id, channel, actor_id, first_seen_at, last_seen_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(board_id, channel, actor_id) DO UPDATE SET last_seen_at = excluded.last_seen_at",
        sql_args![&src.board_id, board_channel, &actor_id, &now],
    )
    .await?;

    let msg_id = new_message_id();
    let seq = messages::next_msg_seq_tx(tx, &src.board_id).await?;

    tx.execute(
        "INSERT INTO messages (
            id, board_id, seq, channel, re, reply_to, text,
            by_actor, by_principal, by_profile, from_source, no_sync,
            revision, deleted_at, created_at, updated_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9, ?10, 0, 1, NULL, ?11, ?11)",
        sql_args![
            &msg_id,
            &src.board_id,
            seq,
            board_channel,
            thread_root,
            text,
            &actor_id,
            &src.owner_principal,
            &profile_key,
            &src.key,
            &now
        ],
    )
    .await?;
    messages::record_thread_message_tx(tx, &src.board_id, board_channel, &msg_id, seq, thread_root)
        .await?;

    if !initial_labels.is_empty() {
        for (k, v) in initial_labels {
            let _ = labels::set_system_label_tx(tx, &src.board_id, &msg_id, k, v).await;
        }
    }

    tx.execute(
        "INSERT INTO message_sync_map (
            board_id, source_id, message_id, platform_message_id, platform_url, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT DO NOTHING",
        sql_args![
            &src.board_id,
            &src.id,
            &msg_id,
            platform_msg_id,
            platform_url,
            &now
        ],
    )
    .await?;

    let stored = messages::load_stored_message_tx(tx, &src.board_id, &msg_id)
        .await?
        .unwrap();
    let snap = messages::build_message_snapshot_tx(tx, &stored).await?;
    let view = messages::hydrate_message_view_tx(tx, &stored).await?;
    let ev = events::append_event_tx(
        tx,
        EventAppend {
            board_id: &src.board_id,
            event_type: EventType::MessagePosted,
            channel: Some(board_channel),
            message_id: Some(&msg_id),
            actor_id: Some(&actor_id),
            payload: json!({
                "channel": board_channel,
                "re": thread_root,
                "from": src.key
            }),
            before_snapshot: None,
            after_snapshot: Some(&snap),
            after_message: Some(&view),
        },
    )
    .await?;

    let kind = if thread_root.is_none() { "post" } else { "reply" };
    let _ = enqueue_outbound_for_message_tx(tx, &src.board_id, &msg_id, kind, None).await?;

    Ok((view, vec![ev.seq]))
}

async fn find_linked_root_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    src: &StoredSyncSource,
    platform_channel: &str,
    platform_thread_id: &str,
) -> BoardResult<Option<(String, String)>> {
    let row = conn
        .fetch_optional(
            "SELECT thread_root_id, channel FROM thread_sync_links
             WHERE board_id = ?1 AND source_id = ?2 AND platform_channel = ?3 AND platform_thread_id = ?4",
            sql_args![&src.board_id, &src.id, platform_channel, platform_thread_id],
        )
        .await?;
    match row {
        Some(r) => Ok(Some((r.get_str(0)?, r.get_str(1)?))),
        None => Ok(None),
    }
}

async fn find_mapped_message_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    src: &StoredSyncSource,
    platform_msg_id: &str,
) -> BoardResult<Option<String>> {
    let row = conn
        .fetch_optional(
            "SELECT message_id FROM message_sync_map
             WHERE board_id = ?1 AND source_id = ?2 AND platform_message_id = ?3",
            sql_args![&src.board_id, &src.id, platform_msg_id],
        )
        .await?;
    row.map(|r| r.get_str(0)).transpose()
}

async fn create_link_and_emit_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    root_msg: &StoredMessageRow,
    platform_channel: &str,
    platform_thread_id: &str,
    platform_url: Option<&str>,
) -> BoardResult<i64> {
    let link_id = prefixed_id("lnk");
    let now = format_utc_ms(Utc::now());
    tx.execute(
        "INSERT INTO thread_sync_links (
            id, board_id, thread_root_id, channel, source_id, platform,
            platform_channel, platform_thread_id, platform_url, direction, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT DO NOTHING",
        sql_args![
            &link_id,
            &src.board_id,
            &root_msg.id,
            &root_msg.channel,
            &src.id,
            src.platform.as_str(),
            platform_channel,
            platform_thread_id,
            platform_url,
            src.direction.as_str(),
            &now
        ],
    )
    .await?;
    let snap = messages::build_message_snapshot_tx(tx, root_msg).await?;
    let view = messages::hydrate_message_view_tx(tx, root_msg).await?;
    let ev = events::append_event_tx(
        tx,
        EventAppend {
            board_id: &src.board_id,
            event_type: EventType::ThreadSynced,
            channel: Some(&root_msg.channel),
            message_id: Some(&root_msg.id),
            actor_id: Some(&src.owner_principal),
            payload: json!({
                "sourceKey": src.key,
                "platform": src.platform.as_str(),
                "platformChannel": platform_channel,
                "platformThreadId": platform_thread_id,
                "platformUrl": platform_url
            }),
            before_snapshot: Some(&snap),
            after_snapshot: Some(&snap),
            after_message: Some(&view),
        },
    )
    .await?;
    Ok(ev.seq)
}

// --- Slack Adapter ---

async fn process_slack_inbound_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    body: &Value,
) -> BoardResult<(Value, Vec<i64>)> {
    let ev = &body["event"];
    let ev_type = ev["type"].as_str().unwrap_or("");
    let mut seqs = Vec::new();

    match ev_type {
        "message" => {
            if ev["bot_id"].as_str() == Some("openboards") {
                return Ok((json!({ "ok": true, "ignored": "echo" }), seqs));
            }
            let subtype = ev["subtype"].as_str().unwrap_or("");
            let slack_ch = ev["channel"].as_str().unwrap_or("");

            if subtype == "message_changed" {
                let ts = ev["message"]["ts"].as_str().unwrap_or("");
                let new_text = slack_mrkdwn_to_gfm(ev["message"]["text"].as_str().unwrap_or(""));
                if let Some(mid) = find_mapped_message_tx(tx, src, ts).await? {
                    if let Some(seq) = apply_synced_edit_tx(tx, src, &mid, &new_text).await? {
                        seqs.push(seq);
                    }
                    return Ok((json!({ "ok": true, "edited": mid }), seqs));
                }
                return Ok((json!({ "ok": true, "ignored": "unmapped_edit" }), seqs));
            }

            if subtype == "message_deleted" {
                let ts = ev["deleted_ts"].as_str().unwrap_or("");
                if let Some(mid) = find_mapped_message_tx(tx, src, ts).await? {
                    if let Some(seq) = apply_synced_delete_tx(tx, src, &mid).await? {
                        seqs.push(seq);
                    }
                    return Ok((json!({ "ok": true, "deleted": mid }), seqs));
                }
                return Ok((json!({ "ok": true, "ignored": "unmapped_delete" }), seqs));
            }

            let ts = ev["ts"].as_str().unwrap_or("");
            if ts.is_empty() {
                return Ok((json!({ "ok": true, "ignored": "no_ts" }), seqs));
            }
            tx.advisory_xact_lock("sync_in", &format!("{}:{ts}", src.id))
                .await?;
            if find_mapped_message_tx(tx, src, ts).await?.is_some() {
                return Ok((json!({ "ok": true, "ignored": "duplicate" }), seqs));
            }

            let thread_ts = ev["thread_ts"].as_str().unwrap_or(ts);
            let text = slack_mrkdwn_to_gfm(ev["text"].as_str().unwrap_or(""));
            let user_id = ev["user"].as_str().unwrap_or("unknown");
            let user_name = ev["user_name"].as_str().unwrap_or(user_id);
            let avatar = ev["user_avatar"].as_str();
            let permalink = ev["permalink"].as_str();

            if let Some((root_id, board_ch)) =
                find_linked_root_tx(tx, src, slack_ch, thread_ts).await?
            {
                let (view, mut s) = insert_synced_message_tx(
                    tx,
                    src,
                    &board_ch,
                    Some(&root_id),
                    ts,
                    permalink,
                    &text,
                    user_id,
                    user_name,
                    avatar,
                    &BTreeMap::new(),
                )
                .await?;
                seqs.append(&mut s);
                return Ok((json!({ "ok": true, "messageId": view.id }), seqs));
            }

            let Some(board_ch) = lookup_mapped_channel(src, slack_ch, false) else {
                return Ok((json!({ "ok": true, "ignored": "unmapped_channel" }), seqs));
            };

            let (view, mut s) = insert_synced_message_tx(
                tx,
                src,
                board_ch,
                None,
                ts,
                permalink,
                &text,
                user_id,
                user_name,
                avatar,
                &BTreeMap::new(),
            )
            .await?;
            seqs.append(&mut s);
            let root_stored = messages::load_stored_message_tx(tx, &src.board_id, &view.id)
                .await?
                .unwrap();
            seqs.push(
                create_link_and_emit_tx(tx, src, &root_stored, slack_ch, thread_ts, permalink)
                    .await?,
            );
            Ok((json!({ "ok": true, "messageId": view.id }), seqs))
        }
        "reaction_added" | "reaction_removed" => {
            let ts = ev["item"]["ts"].as_str().unwrap_or("");
            let emoji_raw = ev["reaction"].as_str().unwrap_or("");
            let user_id = ev["user"].as_str().unwrap_or("unknown");
            let remove = ev_type == "reaction_removed";
            if let Some(mid) = find_mapped_message_tx(tx, src, ts).await? {
                let actor_id = format!(
                    "{}+{}:user:{}",
                    src.owner_principal,
                    src.key,
                    user_id.to_ascii_lowercase()
                );
                if let Some(seq) =
                    apply_synced_reaction_tx(tx, src, &mid, &actor_id, emoji_raw, remove).await?
                {
                    seqs.push(seq);
                }
                return Ok((json!({ "ok": true, "messageId": mid }), seqs));
            }
            Ok((json!({ "ok": true, "ignored": "unmapped_reaction" }), seqs))
        }
        _ => Ok((json!({ "ok": true, "ignored": ev_type }), seqs)),
    }
}

// --- Linear Adapter ---

async fn process_linear_inbound_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    body: &Value,
) -> BoardResult<(Value, Vec<i64>)> {
    let kind = body["type"].as_str().unwrap_or("");
    let action = body["action"].as_str().unwrap_or("");
    let data = &body["data"];
    let mut seqs = Vec::new();

    match kind {
        "Issue" => {
            let issue_id = data["id"].as_str().unwrap_or("");
            let identifier = data["identifier"].as_str().unwrap_or(issue_id);
            let team_key = data["team"]["key"]
                .as_str()
                .or_else(|| identifier.split('-').next())
                .unwrap_or("");
            let url = body["url"].as_str().or_else(|| data["url"].as_str());
            let title = data["title"].as_str().unwrap_or("");
            let desc = data["description"].as_str().unwrap_or("");
            let text = if desc.is_empty() {
                title.to_string()
            } else {
                format!("{title}\n{desc}")
            };

            let mut issue_labels = BTreeMap::new();
            if !identifier.is_empty() {
                issue_labels.insert("issue".to_string(), identifier.to_string());
            }
            if let Some(st) = data["state"]["type"]
                .as_str()
                .or_else(|| data["state"]["name"].as_str())
            {
                issue_labels.insert("state".to_string(), st.to_ascii_lowercase());
            }
            if let Some(prio) = data["priority"].as_i64() {
                issue_labels.insert("priority".to_string(), prio.to_string());
            }
            if let Some(assignee) = data["assignee"]["name"].as_str() {
                let slug = sanitize_label_segment(assignee);
                if !slug.is_empty() {
                    issue_labels.insert(format!("assignee.{slug}"), "true".to_string());
                }
            }
            if let Some(arr) = data["labels"].as_array() {
                for l in arr {
                    if let Some(name) = l["name"].as_str() {
                        let slug = sanitize_label_segment(name);
                        if !slug.is_empty() {
                            issue_labels.insert(format!("tag.{slug}"), "true".to_string());
                        }
                    }
                }
            }

            if action == "create" {
                tx.advisory_xact_lock("sync_in", &format!("{}:{issue_id}", src.id))
                    .await?;
                if find_mapped_message_tx(tx, src, issue_id).await?.is_some() {
                    return Ok((json!({ "ok": true, "ignored": "echo" }), seqs));
                }
                let Some(board_ch) = lookup_mapped_channel(src, team_key, false) else {
                    return Ok((json!({ "ok": true, "ignored": "unmapped_team" }), seqs));
                };
                let user_id = data["creator"]["id"].as_str().unwrap_or("linear");
                let user_name = data["creator"]["name"].as_str().unwrap_or(user_id);
                let (view, mut s) = insert_synced_message_tx(
                    tx,
                    src,
                    board_ch,
                    None,
                    issue_id,
                    url,
                    &text,
                    user_id,
                    user_name,
                    None,
                    &issue_labels,
                )
                .await?;
                seqs.append(&mut s);
                let stored = messages::load_stored_message_tx(tx, &src.board_id, &view.id)
                    .await?
                    .unwrap();
                seqs.push(
                    create_link_and_emit_tx(tx, src, &stored, team_key, issue_id, url).await?,
                );
                return Ok((json!({ "ok": true, "messageId": view.id }), seqs));
            }

            if action == "update" {
                if let Some((root_id, _)) = find_linked_root_tx(tx, src, team_key, issue_id).await?
                {
                    if let Some(seq) =
                        apply_synced_labels_tx(tx, src, &root_id, &issue_labels, false).await?
                    {
                        seqs.push(seq);
                    }
                    if !text.is_empty() {
                        if let Some(seq) = apply_synced_edit_tx(tx, src, &root_id, &text).await? {
                            seqs.push(seq);
                        }
                    }
                    return Ok((json!({ "ok": true, "updated": root_id }), seqs));
                }
            }

            if action == "remove" {
                if let Some((root_id, _)) = find_linked_root_tx(tx, src, team_key, issue_id).await?
                {
                    if let Some(seq) = apply_synced_delete_tx(tx, src, &root_id).await? {
                        seqs.push(seq);
                    }
                    return Ok((json!({ "ok": true, "deleted": root_id }), seqs));
                }
            }
            Ok((json!({ "ok": true }), seqs))
        }
        "Comment" => {
            let comment_id = data["id"].as_str().unwrap_or("");
            let issue_id = data["issueId"]
                .as_str()
                .or_else(|| data["issue"]["id"].as_str())
                .unwrap_or("");
            let team_key = data["issue"]["team"]["key"].as_str().unwrap_or("");
            let comment_body = data["body"].as_str().unwrap_or("");
            let url = body["url"].as_str().or_else(|| data["url"].as_str());

            if action == "create" {
                tx.advisory_xact_lock("sync_in", &format!("{}:{comment_id}", src.id))
                    .await?;
                if find_mapped_message_tx(tx, src, comment_id).await?.is_some() {
                    return Ok((json!({ "ok": true, "ignored": "echo" }), seqs));
                }
                let link = tx
                    .fetch_optional(
                        "SELECT thread_root_id, channel FROM thread_sync_links
                         WHERE board_id = ?1 AND source_id = ?2 AND platform_thread_id = ?3
                         LIMIT 1",
                        sql_args![&src.board_id, &src.id, issue_id],
                    )
                    .await?
                    .map(|r| Ok::<_, BoardError>((r.get_str(0)?, r.get_str(1)?)))
                    .transpose()?;
                if let Some((root_id, board_ch)) = link {
                    let uid = data["user"]["id"].as_str().unwrap_or("linear");
                    let uname = data["user"]["name"].as_str().unwrap_or(uid);
                    let (view, mut s) = insert_synced_message_tx(
                        tx,
                        src,
                        &board_ch,
                        Some(&root_id),
                        comment_id,
                        url,
                        comment_body,
                        uid,
                        uname,
                        None,
                        &BTreeMap::new(),
                    )
                    .await?;
                    seqs.append(&mut s);
                    return Ok((json!({ "ok": true, "messageId": view.id }), seqs));
                }
                let _ = team_key;
            } else if action == "update" {
                if let Some(mid) = find_mapped_message_tx(tx, src, comment_id).await? {
                    if let Some(seq) =
                        apply_synced_edit_tx(tx, src, &mid, comment_body).await?
                    {
                        seqs.push(seq);
                    }
                    return Ok((json!({ "ok": true, "edited": mid }), seqs));
                }
            } else if action == "remove" {
                if let Some(mid) = find_mapped_message_tx(tx, src, comment_id).await? {
                    if let Some(seq) = apply_synced_delete_tx(tx, src, &mid).await? {
                        seqs.push(seq);
                    }
                    return Ok((json!({ "ok": true, "deleted": mid }), seqs));
                }
            }
            Ok((json!({ "ok": true }), seqs))
        }
        _ => Ok((json!({ "ok": true, "ignored": kind }), seqs)),
    }
}

// --- GitHub Adapter (inbound-only by default, exact documented labels) ---

async fn process_github_inbound_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    headers: &HeaderMap,
    body: &Value,
) -> BoardResult<(Value, Vec<i64>)> {
    let gh_event = headers
        .get("x-github-event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let repo = body["repository"]["full_name"].as_str().unwrap_or("");
    let action = body["action"].as_str().unwrap_or("");
    let mut seqs = Vec::new();

    match gh_event {
        "issues" => {
            let issue = &body["issue"];
            let num = issue["number"].as_i64().unwrap_or(0);
            let p_tid = num.to_string();
            let p_mid = format!("issue:{repo}#{num}");
            let html_url = issue["html_url"].as_str();
            let title = issue["title"].as_str().unwrap_or("");
            let desc = issue["body"].as_str().unwrap_or("");
            let text = if desc.is_empty() {
                title.to_string()
            } else {
                format!("{title}\n\n{desc}")
            };

            let mut gh_labels = BTreeMap::new();
            gh_labels.insert("issue".to_string(), format!("{repo}#{num}"));
            let st = issue["state"].as_str().unwrap_or("open").to_ascii_lowercase();
            gh_labels.insert("state".to_string(), st);
            collect_github_tags_and_assignees(issue, &mut gh_labels);

            tx.advisory_xact_lock("sync_in", &format!("{}:{p_mid}", src.id))
                .await?;
            if let Some((root_id, _)) = find_linked_root_tx(tx, src, repo, &p_tid).await? {
                if let Some(seq) =
                    apply_synced_labels_tx(tx, src, &root_id, &gh_labels, true).await?
                {
                    seqs.push(seq);
                }
                if action == "edited" {
                    if let Some(seq) = apply_synced_edit_tx(tx, src, &root_id, &text).await? {
                        seqs.push(seq);
                    }
                }
                return Ok((json!({ "ok": true, "updated": root_id }), seqs));
            }

            let Some(board_ch) = lookup_mapped_channel(src, repo, false) else {
                return Ok((json!({ "ok": true, "ignored": "unmapped_repo" }), seqs));
            };
            let login = issue["user"]["login"].as_str().unwrap_or("github");
            let avatar = issue["user"]["avatar_url"].as_str();
            let (view, mut s) = insert_synced_message_tx(
                tx,
                src,
                board_ch,
                None,
                &p_mid,
                html_url,
                &text,
                login,
                login,
                avatar,
                &gh_labels,
            )
            .await?;
            seqs.append(&mut s);
            let stored = messages::load_stored_message_tx(tx, &src.board_id, &view.id)
                .await?
                .unwrap();
            seqs.push(create_link_and_emit_tx(tx, src, &stored, repo, &p_tid, html_url).await?);
            Ok((json!({ "ok": true, "messageId": view.id }), seqs))
        }
        "pull_request" => {
            let pr = &body["pull_request"];
            let num = pr["number"].as_i64().unwrap_or(0);
            let p_tid = num.to_string();
            let p_mid = format!("pr:{repo}#{num}");
            let html_url = pr["html_url"].as_str();
            let title = pr["title"].as_str().unwrap_or("");
            let desc = pr["body"].as_str().unwrap_or("");
            let text = if desc.is_empty() {
                title.to_string()
            } else {
                format!("{title}\n\n{desc}")
            };

            let mut gh_labels = BTreeMap::new();
            gh_labels.insert("pr".to_string(), format!("{repo}#{num}"));
            let state = if pr["merged"].as_bool() == Some(true) {
                "merged"
            } else if pr["draft"].as_bool() == Some(true) && pr["state"].as_str() == Some("open") {
                "draft"
            } else {
                pr["state"].as_str().unwrap_or("open")
            };
            gh_labels.insert("state".to_string(), state.to_ascii_lowercase());
            if let Some(sha) = pr["head"]["sha"].as_str() {
                gh_labels.insert("head".to_string(), sha.to_string());
            }
            collect_github_tags_and_assignees(pr, &mut gh_labels);

            tx.advisory_xact_lock("sync_in", &format!("{}:{p_mid}", src.id))
                .await?;
            if let Some((root_id, _)) = find_linked_root_tx(tx, src, repo, &p_tid).await? {
                if let Some(seq) =
                    apply_synced_labels_tx(tx, src, &root_id, &gh_labels, true).await?
                {
                    seqs.push(seq);
                }
                if action == "edited" {
                    if let Some(seq) = apply_synced_edit_tx(tx, src, &root_id, &text).await? {
                        seqs.push(seq);
                    }
                }
                return Ok((json!({ "ok": true, "updated": root_id }), seqs));
            }

            let Some(board_ch) = lookup_mapped_channel(src, repo, true) else {
                return Ok((json!({ "ok": true, "ignored": "unmapped_repo" }), seqs));
            };
            let login = pr["user"]["login"].as_str().unwrap_or("github");
            let avatar = pr["user"]["avatar_url"].as_str();
            let (view, mut s) = insert_synced_message_tx(
                tx,
                src,
                board_ch,
                None,
                &p_mid,
                html_url,
                &text,
                login,
                login,
                avatar,
                &gh_labels,
            )
            .await?;
            seqs.append(&mut s);
            let stored = messages::load_stored_message_tx(tx, &src.board_id, &view.id)
                .await?
                .unwrap();
            seqs.push(create_link_and_emit_tx(tx, src, &stored, repo, &p_tid, html_url).await?);
            Ok((json!({ "ok": true, "messageId": view.id }), seqs))
        }
        "pull_request_review" => {
            let pr = &body["pull_request"];
            let review = &body["review"];
            let num = pr["number"].as_i64().unwrap_or(0);
            let p_tid = num.to_string();
            let rev_id = format!("review:{}", review["id"].as_i64().unwrap_or(0));
            let html_url = review["html_url"].as_str();
            let rev_state = review["state"].as_str().unwrap_or("").to_ascii_lowercase();
            let rev_body = review["body"].as_str().unwrap_or("");
            let login = review["user"]["login"].as_str().unwrap_or("github");
            let avatar = review["user"]["avatar_url"].as_str();

            tx.advisory_xact_lock("sync_in", &format!("{}:{rev_id}", src.id))
                .await?;
            if let Some((root_id, board_ch)) = find_linked_root_tx(tx, src, repo, &p_tid).await? {
                if matches!(rev_state.as_str(), "approved" | "changes_requested") {
                    let mut rev_labels = BTreeMap::new();
                    rev_labels.insert("review".to_string(), rev_state.clone());
                    if let Some(seq) =
                        apply_synced_labels_tx(tx, src, &root_id, &rev_labels, false).await?
                    {
                        seqs.push(seq);
                    }
                }
                if find_mapped_message_tx(tx, src, &rev_id).await?.is_some() {
                    return Ok((json!({ "ok": true, "ignored": "duplicate" }), seqs));
                }
                let reply_text = if rev_body.is_empty() {
                    format!("Review: {rev_state}")
                } else {
                    rev_body.to_string()
                };
                let (view, mut s) = insert_synced_message_tx(
                    tx,
                    src,
                    &board_ch,
                    Some(&root_id),
                    &rev_id,
                    html_url,
                    &reply_text,
                    login,
                    login,
                    avatar,
                    &BTreeMap::new(),
                )
                .await?;
                seqs.append(&mut s);
                return Ok((json!({ "ok": true, "messageId": view.id }), seqs));
            }
            Ok((json!({ "ok": true, "ignored": "unlinked_pr" }), seqs))
        }
        "issue_comment" | "pull_request_review_comment" => {
            let comment = &body["comment"];
            let num = body["issue"]["number"]
                .as_i64()
                .or_else(|| body["pull_request"]["number"].as_i64())
                .unwrap_or(0);
            let p_tid = num.to_string();
            let cid = format!("comment:{}", comment["id"].as_i64().unwrap_or(0));
            let html_url = comment["html_url"].as_str();
            let c_body = comment["body"].as_str().unwrap_or("");
            let login = comment["user"]["login"].as_str().unwrap_or("github");
            let avatar = comment["user"]["avatar_url"].as_str();

            if action == "edited" {
                if let Some(mid) = find_mapped_message_tx(tx, src, &cid).await? {
                    if let Some(seq) = apply_synced_edit_tx(tx, src, &mid, c_body).await? {
                        seqs.push(seq);
                    }
                    return Ok((json!({ "ok": true, "edited": mid }), seqs));
                }
            } else if action == "deleted" {
                if let Some(mid) = find_mapped_message_tx(tx, src, &cid).await? {
                    if let Some(seq) = apply_synced_delete_tx(tx, src, &mid).await? {
                        seqs.push(seq);
                    }
                    return Ok((json!({ "ok": true, "deleted": mid }), seqs));
                }
            } else if let Some((root_id, board_ch)) =
                find_linked_root_tx(tx, src, repo, &p_tid).await?
            {
                tx.advisory_xact_lock("sync_in", &format!("{}:{cid}", src.id))
                    .await?;
                if find_mapped_message_tx(tx, src, &cid).await?.is_some() {
                    return Ok((json!({ "ok": true, "ignored": "duplicate" }), seqs));
                }
                let (view, mut s) = insert_synced_message_tx(
                    tx,
                    src,
                    &board_ch,
                    Some(&root_id),
                    &cid,
                    html_url,
                    c_body,
                    login,
                    login,
                    avatar,
                    &BTreeMap::new(),
                )
                .await?;
                seqs.append(&mut s);
                return Ok((json!({ "ok": true, "messageId": view.id }), seqs));
            }
            Ok((json!({ "ok": true, "ignored": "unlinked_comment" }), seqs))
        }
        _ => Ok((json!({ "ok": true, "ignored": gh_event }), seqs)),
    }
}

fn collect_github_tags_and_assignees(obj: &Value, out: &mut BTreeMap<String, String>) {
    if let Some(arr) = obj["labels"].as_array() {
        for l in arr {
            if let Some(name) = l["name"].as_str() {
                let slug = sanitize_label_segment(name);
                if !slug.is_empty() {
                    out.insert(format!("tag.{slug}"), "true".to_string());
                }
            }
        }
    }
    if let Some(arr) = obj["assignees"].as_array() {
        for a in arr {
            if let Some(login) = a["login"].as_str() {
                let slug = sanitize_label_segment(login);
                if !slug.is_empty() {
                    out.insert(format!("assignee.{slug}"), "true".to_string());
                }
            }
        }
    }
}

/// Updates synced root labels and emits a `labels.changed` event whenever labels change,
/// so `wait`/`tail`/watchers observing e.g. `--label state=merged` or `--label review=approved` wake up.
async fn apply_synced_labels_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    root_id: &str,
    new_labels: &BTreeMap<String, String>,
    replace_tags_and_assignees: bool,
) -> BoardResult<Option<i64>> {
    let Some(stored) = messages::load_stored_message_tx(tx, &src.board_id, root_id).await? else {
        return Ok(None);
    };
    if stored.deleted_at.is_some() {
        return Ok(None);
    }
    let before_labels = labels::load_message_labels_tx(tx, root_id).await?;
    let before_snap = messages::build_message_snapshot_tx(tx, &stored).await?;

    let mut unset = Vec::new();
    if replace_tags_and_assignees {
        for k in before_labels.keys() {
            if (k.starts_with("tag.") || k.starts_with("assignee.")) && !new_labels.contains_key(k)
            {
                tx.execute(
                    "DELETE FROM message_labels WHERE message_id = ?1 AND key = ?2",
                    sql_args![root_id, k],
                )
                .await?;
                unset.push(k.clone());
            }
        }
    }

    let mut set = BTreeMap::new();
    for (k, v) in new_labels {
        if before_labels.get(k) != Some(v)
            && labels::set_system_label_tx(tx, &src.board_id, root_id, k, v)
                .await
                .is_ok()
        {
            set.insert(k.clone(), v.clone());
        }
    }

    if set.is_empty() && unset.is_empty() {
        return Ok(None);
    }

    let after_snap = messages::build_message_snapshot_tx(tx, &stored).await?;
    let after_msg = messages::hydrate_message_view_tx(tx, &stored).await?;
    let ev = events::append_event_tx(
        tx,
        EventAppend {
            board_id: &src.board_id,
            event_type: EventType::LabelsChanged,
            channel: Some(&stored.channel),
            message_id: Some(root_id),
            actor_id: Some(&src.owner_principal),
            payload: json!({
                "set": set,
                "unset": unset,
                "from": src.key
            }),
            before_snapshot: Some(&before_snap),
            after_snapshot: Some(&after_snap),
            after_message: Some(&after_msg),
        },
    )
    .await?;
    Ok(Some(ev.seq))
}

// --- Custom Normalized Adapter ---

async fn process_custom_inbound_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    body: &Value,
) -> BoardResult<(Value, Vec<i64>)> {
    let action = body["action"].as_str().unwrap_or("post");
    let pch = body["platformChannel"].as_str().unwrap_or("");
    let pth = body["platformThreadId"].as_str().unwrap_or("");
    let pmid = body["platformMessageId"].as_str().unwrap_or(pth);
    let purl = body["platformUrl"].as_str();
    let text = body["text"].as_str().unwrap_or("");
    let uid = body["user"]["id"].as_str().unwrap_or("custom");
    let uname = body["user"]["name"].as_str().unwrap_or(uid);
    let uav = body["user"]["avatar"].as_str();
    let mut seqs = Vec::new();

    match action {
        "post" | "reply" => {
            tx.advisory_xact_lock("sync_in", &format!("{}:{pmid}", src.id))
                .await?;
            if find_mapped_message_tx(tx, src, pmid).await?.is_some() {
                return Ok((json!({ "ok": true, "ignored": "echo" }), seqs));
            }
            if let Some((root_id, board_ch)) = find_linked_root_tx(tx, src, pch, pth).await? {
                let (view, mut s) = insert_synced_message_tx(
                    tx,
                    src,
                    &board_ch,
                    Some(&root_id),
                    pmid,
                    purl,
                    text,
                    uid,
                    uname,
                    uav,
                    &BTreeMap::new(),
                )
                .await?;
                seqs.append(&mut s);
                return Ok((json!({ "ok": true, "messageId": view.id }), seqs));
            }
            let Some(board_ch) = lookup_mapped_channel(src, pch, false) else {
                return Ok((json!({ "ok": true, "ignored": "unmapped_channel" }), seqs));
            };
            let (view, mut s) = insert_synced_message_tx(
                tx,
                src,
                board_ch,
                None,
                pmid,
                purl,
                text,
                uid,
                uname,
                uav,
                &BTreeMap::new(),
            )
            .await?;
            seqs.append(&mut s);
            let stored = messages::load_stored_message_tx(tx, &src.board_id, &view.id)
                .await?
                .unwrap();
            seqs.push(create_link_and_emit_tx(tx, src, &stored, pch, pth, purl).await?);
            Ok((json!({ "ok": true, "messageId": view.id }), seqs))
        }
        "edit" => {
            if let Some(mid) = find_mapped_message_tx(tx, src, pmid).await? {
                if let Some(seq) = apply_synced_edit_tx(tx, src, &mid, text).await? {
                    seqs.push(seq);
                }
                return Ok((json!({ "ok": true, "edited": mid }), seqs));
            }
            Ok((json!({ "ok": true, "ignored": "unmapped_edit" }), seqs))
        }
        "delete" => {
            if let Some(mid) = find_mapped_message_tx(tx, src, pmid).await? {
                if let Some(seq) = apply_synced_delete_tx(tx, src, &mid).await? {
                    seqs.push(seq);
                }
                return Ok((json!({ "ok": true, "deleted": mid }), seqs));
            }
            Ok((json!({ "ok": true, "ignored": "unmapped_delete" }), seqs))
        }
        "react" | "unreact" => {
            let emoji = body["emoji"].as_str().unwrap_or("");
            if let Some(mid) = find_mapped_message_tx(tx, src, pmid).await? {
                let actor_id = format!(
                    "{}+{}:user:{}",
                    src.owner_principal,
                    src.key,
                    uid.to_ascii_lowercase()
                );
                if let Some(seq) = apply_synced_reaction_tx(
                    tx,
                    src,
                    &mid,
                    &actor_id,
                    emoji,
                    action == "unreact",
                )
                .await?
                {
                    seqs.push(seq);
                }
                return Ok((json!({ "ok": true, "messageId": mid }), seqs));
            }
            Ok((json!({ "ok": true, "ignored": "unmapped_reaction" }), seqs))
        }
        _ => Ok((json!({ "ok": true, "ignored": action }), seqs)),
    }
}

fn lookup_mapped_channel<'a>(
    src: &'a StoredSyncSource,
    platform_ch: &str,
    is_pull: bool,
) -> Option<&'a str> {
    if let Some(t) = src.mappings.get(platform_ch) {
        return t.board_channel_for(is_pull);
    }
    let with_hash = format!("#{platform_ch}");
    if let Some(t) = src.mappings.get(&with_hash) {
        return t.board_channel_for(is_pull);
    }
    let without_hash = platform_ch.strip_prefix('#').unwrap_or(platform_ch);
    src.mappings
        .get(without_hash)
        .and_then(|t| t.board_channel_for(is_pull))
}

async fn apply_synced_edit_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    message_id: &str,
    new_text: &str,
) -> BoardResult<Option<i64>> {
    let Some(msg) = messages::load_stored_message_for_update_tx(tx, &src.board_id, message_id)
        .await?
    else {
        return Ok(None);
    };
    if msg.deleted_at.is_some() || msg.text == new_text {
        return Ok(None);
    }
    let before_snap = messages::build_message_snapshot_tx(tx, &msg).await?;
    let next_rev = msg.revision + 1;
    let now = format_utc_ms(Utc::now());
    tx.execute(
        "UPDATE messages SET text = ?1, revision = ?2, updated_at = ?3 WHERE id = ?4",
        sql_args![new_text, next_rev, &now, message_id],
    )
    .await?;
    let updated = messages::load_stored_message_tx(tx, &src.board_id, message_id)
        .await?
        .unwrap();
    let after_snap = messages::build_message_snapshot_tx(tx, &updated).await?;
    let view = messages::hydrate_message_view_tx(tx, &updated).await?;
    let ev = events::append_event_tx(
        tx,
        EventAppend {
            board_id: &src.board_id,
            event_type: EventType::MessageEdited,
            channel: Some(&msg.channel),
            message_id: Some(message_id),
            actor_id: Some(&msg.by_actor),
            payload: json!({ "revision": next_rev, "from": src.key }),
            before_snapshot: Some(&before_snap),
            after_snapshot: Some(&after_snap),
            after_message: Some(&view),
        },
    )
    .await?;
    Ok(Some(ev.seq))
}

async fn apply_synced_delete_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    message_id: &str,
) -> BoardResult<Option<i64>> {
    let Some(msg) = messages::load_stored_message_for_update_tx(tx, &src.board_id, message_id)
        .await?
    else {
        return Ok(None);
    };
    if msg.deleted_at.is_some() {
        return Ok(None);
    }
    let before_snap = messages::build_message_snapshot_tx(tx, &msg).await?;
    let next_rev = msg.revision + 1;
    let now = format_utc_ms(Utc::now());
    tx.execute(
        "UPDATE messages SET text = '', deleted_at = ?1, revision = ?2, updated_at = ?1 WHERE id = ?3",
        sql_args![&now, next_rev, message_id],
    )
    .await?;
    tx.execute(
        "DELETE FROM message_reactions WHERE message_id = ?1",
        sql_args![message_id],
    )
    .await?;
    labels::delete_message_labels_tx(tx, message_id).await?;
    let updated = messages::load_stored_message_tx(tx, &src.board_id, message_id)
        .await?
        .unwrap();
    let after_snap = messages::build_message_snapshot_tx(tx, &updated).await?;
    let view = messages::hydrate_message_view_tx(tx, &updated).await?;
    let ev = events::append_event_tx(
        tx,
        EventAppend {
            board_id: &src.board_id,
            event_type: EventType::MessageDeleted,
            channel: Some(&msg.channel),
            message_id: Some(message_id),
            actor_id: Some(&msg.by_actor),
            payload: json!({ "revision": next_rev, "from": src.key }),
            before_snapshot: Some(&before_snap),
            after_snapshot: Some(&after_snap),
            after_message: Some(&view),
        },
    )
    .await?;
    Ok(Some(ev.seq))
}

async fn apply_synced_reaction_tx(
    tx: &mut DbTx,
    src: &StoredSyncSource,
    message_id: &str,
    actor_id: &str,
    raw_emoji: &str,
    remove: bool,
) -> BoardResult<Option<i64>> {
    let emoji = messages::normalize_emoji(raw_emoji)?;
    let Some(msg) = messages::load_stored_message_tx(tx, &src.board_id, message_id).await? else {
        return Ok(None);
    };
    if msg.deleted_at.is_some() {
        return Ok(None);
    }
    let changed = if remove {
        tx.execute(
            "DELETE FROM message_reactions WHERE message_id = ?1 AND actor_id = ?2 AND emoji = ?3",
            sql_args![message_id, actor_id, &emoji],
        )
        .await?
            > 0
    } else {
        let now = format_utc_ms(Utc::now());
        tx.execute(
            "INSERT INTO message_reactions (board_id, message_id, emoji, actor_id, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT (message_id, actor_id, emoji) DO NOTHING",
            sql_args![&src.board_id, message_id, &emoji, actor_id, &now],
        )
        .await?
            > 0
    };
    if !changed {
        return Ok(None);
    }
    let snap = messages::build_message_snapshot_tx(tx, &msg).await?;
    let view = messages::hydrate_message_view_tx(tx, &msg).await?;
    let ev = events::append_event_tx(
        tx,
        EventAppend {
            board_id: &src.board_id,
            event_type: if remove {
                EventType::ReactionRemoved
            } else {
                EventType::ReactionAdded
            },
            channel: Some(&msg.channel),
            message_id: Some(message_id),
            actor_id: Some(actor_id),
            payload: json!({ "emoji": emoji, "from": src.key }),
            before_snapshot: Some(&snap),
            after_snapshot: Some(&snap),
            after_message: Some(&view),
        },
    )
    .await?;
    Ok(Some(ev.seq))
}

// --- Formatting & Conversion Helpers ---

/// Converts GitHub-flavored Markdown (`**bold**`, `~~strike~~`, `[label](url)`) to Slack mrkdwn.
pub fn gfm_to_slack_mrkdwn(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if i + 1 < chars.len() && chars[i] == '*' && chars[i + 1] == '*' {
            out.push('*');
            i += 2;
            continue;
        }
        if i + 1 < chars.len() && chars[i] == '~' && chars[i + 1] == '~' {
            out.push('~');
            i += 2;
            continue;
        }
        // Markdown link [label](url) -> <url|label>
        if chars[i] == '[' {
            if let Some(close_br) = chars[i + 1..].iter().position(|&c| c == ']') {
                let close_br_idx = i + 1 + close_br;
                if close_br_idx + 1 < chars.len() && chars[close_br_idx + 1] == '(' {
                    if let Some(close_p) = chars[close_br_idx + 2..].iter().position(|&c| c == ')')
                    {
                        let close_p_idx = close_br_idx + 2 + close_p;
                        let label: String = chars[i + 1..close_br_idx].iter().collect();
                        let url: String = chars[close_br_idx + 2..close_p_idx].iter().collect();
                        out.push('<');
                        out.push_str(&url);
                        out.push('|');
                        out.push_str(&label);
                        out.push('>');
                        i = close_p_idx + 1;
                        continue;
                    }
                }
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// Converts Slack mrkdwn (`*bold*`, `~strike~`, `<url|label>`) to GitHub-flavored Markdown.
pub fn slack_mrkdwn_to_gfm(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        // Slack link <url|label> -> [label](url)
        if chars[i] == '<' {
            if let Some(close) = chars[i + 1..].iter().position(|&c| c == '>') {
                let inner: String = chars[i + 1..i + 1 + close].iter().collect();
                if let Some((url, label)) = inner.split_once('|') {
                    if url.starts_with("http://") || url.starts_with("https://") {
                        out.push('[');
                        out.push_str(label);
                        out.push_str("](");
                        out.push_str(url);
                        out.push(')');
                        i += close + 2;
                        continue;
                    }
                }
            }
        }
        // Single *bold* -> **bold**
        if chars[i] == '*' {
            if let Some(end) = chars[i + 1..].iter().position(|&c| c == '*') {
                if end > 0 {
                    out.push_str("**");
                    for &c in &chars[i + 1..i + 1 + end] {
                        out.push(c);
                    }
                    out.push_str("**");
                    i += end + 2;
                    continue;
                }
            }
        }
        // Single ~strike~ -> ~~strike~~
        if chars[i] == '~' {
            if let Some(end) = chars[i + 1..].iter().position(|&c| c == '~') {
                if end > 0 {
                    out.push_str("~~");
                    for &c in &chars[i + 1..i + 1 + end] {
                        out.push(c);
                    }
                    out.push_str("~~");
                    i += end + 2;
                    continue;
                }
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

/// Splits a board message into Linear `(title, body)` where the first line is the issue title.
pub fn split_linear_title_body(text: &str) -> (String, String) {
    match text.split_once('\n') {
        Some((first, rest)) => (first.trim().to_string(), rest.trim_start().to_string()),
        None => (text.trim().to_string(), String::new()),
    }
}

fn sanitize_label_segment(raw: &str) -> String {
    raw.trim()
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_gfm_and_slack_mrkdwn() {
        assert_eq!(
            gfm_to_slack_mrkdwn("**deploy** ~~old~~ [docs](https://acme.dev)"),
            "*deploy* ~old~ <https://acme.dev|docs>"
        );
        assert_eq!(
            slack_mrkdwn_to_gfm("*deploy* ~old~ <https://acme.dev|docs>"),
            "**deploy** ~~old~~ [docs](https://acme.dev)"
        );
    }

    #[test]
    fn splits_linear_title_and_body() {
        let (t, b) = split_linear_title_body("Fix checkout timeout\nDetails here\nLine 2");
        assert_eq!(t, "Fix checkout timeout");
        assert_eq!(b, "Details here\nLine 2");

        let (t2, b2) = split_linear_title_body("Single line issue");
        assert_eq!(t2, "Single line issue");
        assert_eq!(b2, "");
    }
}
