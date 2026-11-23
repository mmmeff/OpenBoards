use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::actors::{self, CallerContext};
use crate::db::Database;
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::events::{self, EventAppend, EventHub, EventType};
use crate::filter::{FilterContext, LabelPredicate, QueryFilter};
use crate::grants::{self, Grant};
use crate::id::{hash_token, new_bearer_token};
use crate::labels;
use crate::messages::{self, MessageView, PostMessageInput, ThreadSummaryView};
use crate::validate::{
    validate_channel_path, validate_channel_pattern, validate_profile_name, validate_text_length,
};
use crate::AppState;

pub const MAX_LEASE_MS: i64 = 300_000;
pub const DEFAULT_LEASE_MS: i64 = 300_000;
pub const MAX_TASKS_PAGE_LIMIT: usize = 50;
pub const MAX_BATCH_TASKS: usize = 15;
pub const MAX_BATCH_TASKS_WITH_ID: usize = 14;
pub const MAX_BATCH_WRITE_BUDGET: usize = 100;
pub const MAX_CLAIM_SUBTREE_CHANNELS: usize = 50;
pub const MAX_CLAIM_PLACES: usize = 200;
pub const MAX_CLAIM_SCAN_PER_PLACE: usize = 25;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskState {
    Waiting,
    Ready,
    Claimed,
    Blocked,
    Completed,
    Failed,
    Canceled,
}

impl TaskState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::Ready => "ready",
            Self::Claimed => "claimed",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    pub fn parse(s: &str) -> BoardResult<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "waiting" => Ok(Self::Waiting),
            "ready" => Ok(Self::Ready),
            "claimed" => Ok(Self::Claimed),
            "blocked" => Ok(Self::Blocked),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "canceled" => Ok(Self::Canceled),
            other => Err(BoardError::bad_request(format!(
                "invalid task state '{other}'"
            ))),
        }
    }

    pub const fn is_open(self) -> bool {
        matches!(
            self,
            Self::Waiting | Self::Ready | Self::Claimed | Self::Blocked
        )
    }

    pub const fn is_finished(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Canceled)
    }
}

/// Public task overlay hydrated onto root `MessageView.task`.
/// Never exposes the secret lease `claim_token`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskView {
    pub state: TaskState,
    pub owner: String,
    #[serde(rename = "for", skip_serializing_if = "Option::is_none")]
    pub assigned_for: Option<String>,
    #[serde(rename = "claimedBy", skip_serializing_if = "Option::is_none")]
    pub claimed_by: Option<String>,
    pub epoch: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub until: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub needs: Vec<String>,
    #[serde(rename = "blockReason", skip_serializing_if = "Option::is_none")]
    pub block_reason: Option<String>,
    #[serde(rename = "blockKind", skip_serializing_if = "Option::is_none")]
    pub block_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(rename = "failReason", skip_serializing_if = "Option::is_none")]
    pub fail_reason: Option<String>,
    #[serde(rename = "cancelReason", skip_serializing_if = "Option::is_none")]
    pub cancel_reason: Option<String>,
}

/// Returned ONLY by `claim` and `renew` responses — includes the capability lease `token`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimedTaskView {
    pub task: MessageView,
    pub epoch: i64,
    pub token: String,
    pub until: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TasksPage {
    pub tasks: Vec<ThreadSummaryView>,
    pub cursor: String,
}

#[derive(Debug, Clone)]
pub struct CreateTaskResult {
    pub task: MessageView,
    pub claim: Option<ClaimedTaskView>,
    pub replayed: bool,
    pub event_seqs: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct BatchCreateResult {
    pub tasks: Vec<MessageView>,
    pub replayed: bool,
    pub event_seqs: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct TransitionResult {
    pub task: MessageView,
    pub readied: Vec<String>,
    pub event_seqs: Vec<i64>,
}

#[derive(Debug, Clone)]
pub struct ClaimResult {
    pub claim: ClaimedTaskView,
    pub event_seqs: Vec<i64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateTaskInput {
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
    pub id: Option<String>,
    #[serde(default)]
    pub needs: Vec<String>,
    #[serde(rename = "for")]
    pub assigned_for: Option<String>,
    #[serde(default)]
    pub claim: bool,
    pub lease: Option<i64>,
    pub lease_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BatchNeedRef {
    Id(String),
    Key { key: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskInBatch {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub attachments: Vec<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub needs: Vec<BatchNeedRef>,
    #[serde(default, rename = "for")]
    pub assigned_for: Option<String>,
    #[serde(default, alias = "noSync")]
    pub no_sync: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateManyTasksInput {
    pub channel: String,
    pub tasks: Vec<TaskInBatch>,
    pub id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ClaimInput {
    /// Lease duration in seconds (<= 300).
    pub lease: Option<i64>,
    /// Optional lease duration in milliseconds (<= 300_000), useful for fast tests.
    pub lease_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum ClaimLabelFilter {
    Map(BTreeMap<String, String>),
    List(Vec<String>),
}

#[derive(Debug, Deserialize)]
pub struct ClaimReadyInput {
    pub channel: String,
    pub label: Option<ClaimLabelFilter>,
    #[serde(default)]
    pub from: Vec<String>,
    pub wait: Option<Value>,
    pub lease: Option<i64>,
    pub lease_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct RenewInput {
    pub epoch: i64,
    pub token: String,
    pub lease: Option<i64>,
    pub lease_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseInput {
    pub epoch: i64,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub struct CompleteInput {
    pub epoch: i64,
    pub token: String,
    pub result: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FailInput {
    pub epoch: i64,
    pub token: String,
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct BlockInput {
    pub reason: String,
    pub kind: Option<String>,
    pub epoch: Option<i64>,
    pub token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CancelInput {
    pub reason: String,
}

#[derive(Debug, Deserialize)]
pub struct AssignInput {
    #[serde(rename = "for", alias = "actor")]
    pub assigned_for: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateNeedsInput {
    pub needs: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub struct TaskListQuery {
    pub channel: Option<String>,
    pub state: Option<String>,
    pub labels: Vec<LabelPredicate>,
    pub by: Option<String>,
    pub owner: Option<String>,
    pub assigned_for: Option<String>,
    pub needs: Option<String>,
    pub after: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct StoredTaskRow {
    message_id: String,
    board_id: String,
    channel: String,
    seq: i64,
    state: TaskState,
    owner_actor: String,
    assigned_for: Option<String>,
    claimed_by: Option<String>,
    claim_epoch: i64,
    claim_token: Option<String>,
    claim_until: Option<String>,
    block_reason: Option<String>,
    block_kind: Option<String>,
    result: Option<String>,
    fail_reason: Option<String>,
    cancel_reason: Option<String>,
}

fn format_utc_ms(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn parse_lease_ms(lease_secs: Option<i64>, lease_ms: Option<i64>) -> BoardResult<i64> {
    let ms = match (lease_secs, lease_ms) {
        (_, Some(ms)) => ms,
        (Some(s), None) => s.saturating_mul(1000),
        (None, None) => DEFAULT_LEASE_MS,
    };
    if ms <= 0 {
        return Err(BoardError::bad_request("lease duration must be positive"));
    }
    Ok(ms.min(MAX_LEASE_MS))
}

fn map_task_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<StoredTaskRow> {
    let state_str: String = r.get(4)?;
    let state = TaskState::parse(&state_str).unwrap_or(TaskState::Failed);
    Ok(StoredTaskRow {
        message_id: r.get(0)?,
        board_id: r.get(1)?,
        channel: r.get(2)?,
        seq: r.get(3)?,
        state,
        owner_actor: r.get(5)?,
        assigned_for: r.get(6)?,
        claimed_by: r.get(7)?,
        claim_epoch: r.get(8)?,
        claim_token: r.get(9)?,
        claim_until: r.get(10)?,
        block_reason: r.get(11)?,
        block_kind: r.get(12)?,
        result: r.get(13)?,
        fail_reason: r.get(14)?,
        cancel_reason: r.get(15)?,
    })
}

fn load_task_row_tx(
    conn: &Connection,
    board_id: &str,
    task_id: &str,
) -> BoardResult<Option<StoredTaskRow>> {
    conn.query_row(
        "SELECT message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                result, fail_reason, cancel_reason
         FROM tasks
         WHERE board_id = ?1 AND message_id = ?2",
        params![board_id, task_id],
        map_task_row,
    )
    .optional()
    .map_err(Into::into)
}

fn load_task_needs_tx(conn: &Connection, task_id: &str) -> BoardResult<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT needs_task_id FROM task_needs WHERE task_id = ?1 ORDER BY needs_task_id ASC",
    )?;
    let rows = stmt.query_map(params![task_id], |r| r.get::<_, String>(0))?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// Hydrates `Option<TaskView>` for a root message (never exposes `claim_token`).
pub fn load_task_view_tx(conn: &Connection, message_id: &str) -> BoardResult<Option<TaskView>> {
    let row: Option<StoredTaskRow> = conn
        .query_row(
            "SELECT message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                    claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                    result, fail_reason, cancel_reason
             FROM tasks
             WHERE message_id = ?1",
            params![message_id],
            map_task_row,
        )
        .optional()?;
    let Some(t) = row else {
        return Ok(None);
    };
    let needs = load_task_needs_tx(conn, message_id)?;
    Ok(Some(TaskView {
        state: t.state,
        owner: t.owner_actor,
        assigned_for: t.assigned_for,
        claimed_by: t.claimed_by,
        epoch: t.claim_epoch,
        until: t.claim_until,
        needs,
        block_reason: t.block_reason,
        block_kind: t.block_kind,
        result: t.result,
        fail_reason: t.fail_reason,
        cancel_reason: t.cancel_reason,
    }))
}

fn resolve_assigned_for(owner_actor: &str, raw_for: Option<&str>) -> BoardResult<Option<String>> {
    let Some(raw) = raw_for.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let owner_principal = owner_actor
        .split_once('+')
        .map(|(p, _)| p)
        .unwrap_or(owner_actor);

    if raw.contains(':') {
        if raw == owner_principal
            || raw
                .strip_prefix(owner_principal)
                .is_some_and(|rest| rest.starts_with('+'))
        {
            return Ok(Some(raw.to_string()));
        }
        return Err(BoardError::bad_request(
            "'for' must name a profile of the task owner's principal",
        ));
    }

    validate_profile_name(raw)?;
    Ok(Some(format!(
        "{owner_principal}+{}",
        raw.to_ascii_lowercase()
    )))
}

fn all_needs_completed_tx(conn: &Connection, task_id: &str) -> BoardResult<bool> {
    let unfinished: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM task_needs tn
         JOIN tasks t ON t.message_id = tn.needs_task_id
         WHERE tn.task_id = ?1 AND t.state != 'completed'",
        params![task_id],
        |r| r.get(0),
    )?;
    Ok(unfinished == 0)
}

fn validate_prereqs_readable_tx(
    conn: &Connection,
    board_id: &str,
    board_grants: &[Grant],
    caller: &CallerContext,
    needs: &[String],
) -> BoardResult<Vec<String>> {
    let mut deduped = Vec::with_capacity(needs.len());
    for nid in needs {
        if deduped.contains(nid) {
            continue;
        }
        let prereq = load_task_row_tx(conn, board_id, nid)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::InvalidNeeds,
                format!("prerequisite '{nid}' is not a task on this board"),
            )
        })?;
        if !grants::caller_permissions_on_channel(board_grants, caller, &prereq.channel).can_read()
        {
            return Err(BoardError::new(
                ErrorCode::InvalidNeeds,
                format!("prerequisite '{nid}' is not readable"),
            ));
        }
        deduped.push(nid.clone());
    }
    Ok(deduped)
}

/// Checks whether setting `task_id` to depend on `new_needs` would introduce a cycle.
fn check_no_cycle_tx(
    conn: &Connection,
    board_id: &str,
    task_id: &str,
    new_needs: &[String],
) -> BoardResult<()> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut stack: Vec<String> = new_needs.to_vec();

    while let Some(curr) = stack.pop() {
        if curr == task_id {
            return Err(BoardError::new(
                ErrorCode::InvalidNeeds,
                "task dependencies cannot form a cycle",
            ));
        }
        if !visited.insert(curr.clone()) {
            continue;
        }
        let mut stmt = conn.prepare(
            "SELECT needs_task_id FROM task_needs WHERE board_id = ?1 AND task_id = ?2",
        )?;
        let rows = stmt.query_map(params![board_id, curr], |r| r.get::<_, String>(0))?;
        for r in rows {
            stack.push(r?);
        }
    }
    Ok(())
}

/// Expires all overdue `claimed` leases on `board_id` (or all boards if `board_id` is `None`)
/// as of `now`, transitioning `claimed -> ready`, updating `board.task=ready`, and emitting
/// `task.expired` + `task.ready`. Returns `(board_id, event_seq)` pairs for post-commit notify.
pub fn expire_due_leases_tx(
    conn: &Connection,
    board_id: Option<&str>,
    now: DateTime<Utc>,
) -> BoardResult<Vec<(String, i64)>> {
    let now_str = format_utc_ms(now);
    let overdue: Vec<StoredTaskRow> = if let Some(bid) = board_id {
        let mut stmt = conn.prepare(
            "SELECT message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                    claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                    result, fail_reason, cancel_reason
             FROM tasks
             WHERE board_id = ?1 AND state = 'claimed' AND claim_until <= ?2
             ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![bid, now_str], map_task_row)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        out
    } else {
        let mut stmt = conn.prepare(
            "SELECT message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                    claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                    result, fail_reason, cancel_reason
             FROM tasks
             WHERE state = 'claimed' AND claim_until <= ?1
             ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![now_str], map_task_row)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        out
    };

    let mut emitted = Vec::new();
    for task in overdue {
        let Some(stored_msg) =
            messages::load_stored_message_tx(conn, &task.board_id, &task.message_id)?
        else {
            continue;
        };
        let before_snap = messages::build_message_snapshot_tx(conn, &stored_msg)?;

        conn.execute(
            "UPDATE tasks
             SET state = 'ready', claimed_by = NULL, claim_token = NULL, claim_until = NULL, updated_at = ?1
             WHERE message_id = ?2",
            params![now_str, task.message_id],
        )?;
        labels::set_system_label_tx(
            conn,
            &task.board_id,
            &task.message_id,
            "board.task",
            TaskState::Ready.as_str(),
        )?;

        let after_snap = messages::build_message_snapshot_tx(conn, &stored_msg)?;
        let after_msg = messages::hydrate_message_view_tx(conn, &stored_msg)?;

        let ev_exp = events::append_event_tx(
            conn,
            EventAppend {
                board_id: &task.board_id,
                event_type: EventType::TaskExpired,
                channel: Some(&task.channel),
                message_id: Some(&task.message_id),
                actor_id: task.claimed_by.as_deref(),
                payload: json!({ "epoch": task.claim_epoch }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&after_msg),
            },
        )?;
        emitted.push((task.board_id.clone(), ev_exp.seq));

        let ev_ready = events::append_event_tx(
            conn,
            EventAppend {
                board_id: &task.board_id,
                event_type: EventType::TaskReady,
                channel: Some(&task.channel),
                message_id: Some(&task.message_id),
                actor_id: None,
                payload: json!({ "from": "expired" }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&after_msg),
            },
        )?;
        emitted.push((task.board_id.clone(), ev_ready.seq));
    }

    Ok(emitted)
}

/// Public reaper called by background tokio loop and deterministic tests.
pub fn reap_expired_leases(state: &AppState) -> BoardResult<usize> {
    reap_expired_leases_at(&state.db, &state.events, Utc::now())
}

pub fn reap_expired_leases_at(
    db: &Database,
    events_hub: &EventHub,
    now: DateTime<Utc>,
) -> BoardResult<usize> {
    let emitted = db.with_tx(|tx| expire_due_leases_tx(tx, None, now))?;
    let count = emitted.len() / 2;
    for (bid, seq) in emitted {
        events_hub.notify(&bid, seq);
    }
    Ok(count)
}

pub fn spawn_lease_reaper_loop(state: AppState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(err) = reap_expired_leases(&state) {
                tracing::warn!("lease reaper tick error: {err}");
            }
        }
    })
}

fn require_owner_or_manage(
    caller: &CallerContext,
    board_grants: &[Grant],
    task: &StoredTaskRow,
) -> BoardResult<()> {
    let perms = grants::caller_permissions_on_channel(board_grants, caller, &task.channel);
    if !perms.can_read() && !caller.owns_actor_item(&task.owner_actor) {
        return Err(BoardError::new(
            ErrorCode::MessageNotFound,
            format!("task '{}' not found", task.message_id),
        ));
    }
    if caller.owns_actor_item(&task.owner_actor) || perms.can_manage() {
        return Ok(());
    }
    Err(BoardError::new(
        ErrorCode::TaskNotOwned,
        "only the task owner or someone with 'manage' in its channel can perform this action",
    ))
}

fn validate_active_lease(
    task: &StoredTaskRow,
    epoch: i64,
    token: &str,
    now_str: &str,
) -> BoardResult<()> {
    if task.state != TaskState::Claimed {
        return Err(BoardError::new(
            ErrorCode::ClaimLost,
            "claim is no longer active",
        ));
    }
    if task.claim_epoch != epoch || task.claim_token.as_deref() != Some(token) {
        return Err(BoardError::new(
            ErrorCode::ClaimLost,
            "claim epoch or token does not match current lease",
        ));
    }
    if task
        .claim_until
        .as_deref()
        .is_none_or(|until| until <= now_str)
    {
        return Err(BoardError::new(
            ErrorCode::ClaimLost,
            "claim lease has expired",
        ));
    }
    Ok(())
}

fn claim_task_inner_tx(
    conn: &Connection,
    board_id: &str,
    task: &StoredTaskRow,
    caller: &CallerContext,
    lease_ms: i64,
    now: DateTime<Utc>,
) -> BoardResult<(ClaimedTaskView, i64)> {
    match task.state {
        TaskState::Claimed => {
            return Err(BoardError::new(
                ErrorCode::TaskClaimed,
                format!("task '{}' is already claimed", task.message_id),
            ));
        }
        TaskState::Waiting => {
            return Err(BoardError::new(
                ErrorCode::TaskWaiting,
                format!("task '{}' is waiting on prerequisites", task.message_id),
            ));
        }
        TaskState::Blocked => {
            return Err(BoardError::new(
                ErrorCode::TaskBlocked,
                format!("task '{}' is blocked", task.message_id),
            ));
        }
        TaskState::Completed | TaskState::Failed | TaskState::Canceled => {
            return Err(BoardError::new(
                ErrorCode::TaskFinished,
                format!("task '{}' is already {}", task.message_id, task.state.as_str()),
            ));
        }
        TaskState::Ready => {}
    }

    let caller_actor = caller.actor_id();
    if let Some(assigned) = &task.assigned_for {
        if assigned != &caller_actor {
            return Err(BoardError::new(
                ErrorCode::TaskAssigned,
                format!("task '{}' is assigned to '{assigned}'", task.message_id),
            ));
        }
    }

    let stored_msg = messages::load_stored_message_tx(conn, board_id, &task.message_id)?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{}' not found", task.message_id),
            )
        })?;
    let before_snap = messages::build_message_snapshot_tx(conn, &stored_msg)?;

    let new_epoch = task.claim_epoch + 1;
    let token = new_bearer_token();
    let now_str = format_utc_ms(now);
    let until_str = format_utc_ms(now + Duration::milliseconds(lease_ms));

    actors::record_actor_write_tx(conn, board_id, &task.channel, caller)?;

    conn.execute(
        "UPDATE tasks
         SET state = 'claimed', claimed_by = ?1, claim_epoch = ?2, claim_token = ?3,
             claim_until = ?4, updated_at = ?5
         WHERE message_id = ?6",
        params![
            caller_actor,
            new_epoch,
            token,
            until_str,
            now_str,
            task.message_id
        ],
    )?;
    labels::set_system_label_tx(
        conn,
        board_id,
        &task.message_id,
        "board.task",
        TaskState::Claimed.as_str(),
    )?;

    let after_snap = messages::build_message_snapshot_tx(conn, &stored_msg)?;
    let after_msg = messages::hydrate_message_view_tx(conn, &stored_msg)?;
    let ev = events::append_event_tx(
        conn,
        EventAppend {
            board_id,
            event_type: EventType::TaskClaimed,
            channel: Some(&task.channel),
            message_id: Some(&task.message_id),
            actor_id: Some(&caller_actor),
            payload: json!({
                "epoch": new_epoch,
                "until": until_str,
                "claimedBy": caller_actor
            }),
            before_snapshot: Some(&before_snap),
            after_snapshot: Some(&after_snap),
            after_message: Some(&after_msg),
        },
    )?;

    Ok((
        ClaimedTaskView {
            task: after_msg,
            epoch: new_epoch,
            token,
            until: until_str,
        },
        ev.seq,
    ))
}

/// Creates a task (either starting a new thread in `channel`, or turning existing thread `re` into a task).
pub fn create_task(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: CreateTaskInput,
) -> BoardResult<CreateTaskResult> {
    match (&input.channel, &input.re) {
        (Some(_), Some(_)) | (None, None) => {
            return Err(BoardError::bad_request(
                "task creation requires exactly one of 'channel' or 're'",
            ));
        }
        _ => {}
    }

    let lease_ms = if input.claim {
        parse_lease_ms(input.lease, input.lease_ms)?
    } else {
        DEFAULT_LEASE_MS
    };

    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let board_grants = grants::load_grants_tx(tx, board_id)?;
        let owner_actor = caller.actor_id();
        let assigned_for = resolve_assigned_for(&owner_actor, input.assigned_for.as_deref())?;

        // Validate prereqs upfront
        let valid_needs =
            validate_prereqs_readable_tx(tx, board_id, &board_grants, caller, &input.needs)?;

        let (root_id, channel, msg_seq, replayed) = if let Some(ch) = input.channel {
            validate_channel_path(&ch)?;
            let perms = grants::caller_permissions_on_channel(&board_grants, caller, &ch);
            if !perms.can_label() {
                return Err(BoardError::forbidden(format!(
                    "creating a task in '{ch}' requires 'label' permission"
                )));
            }
            if input.claim && !perms.can_read() {
                return Err(BoardError::forbidden(format!(
                    "claiming a task in '{ch}' requires 'read' and 'label' permissions"
                )));
            }

            let posted = messages::post_message_in_tx(
                tx,
                board_id,
                caller,
                PostMessageInput {
                    channel: ch.clone(),
                    text: input.text,
                    id: input.id,
                    attachments: input.attachments,
                    labels: input.labels,
                    schema: input.schema,
                    no_sync: input.no_sync,
                },
            )?;
            if let Some(seq) = posted.event_seq {
                event_seqs.push(seq);
            }
            if posted.replayed {
                if posted.message.task.is_none() {
                    return Err(BoardError::new(
                        ErrorCode::DuplicateId,
                        "idempotency key was already used for a non-task post",
                    ));
                }
                // Idempotent replay of an already-created task
                return Ok(CreateTaskResult {
                    task: posted.message,
                    claim: None,
                    replayed: true,
                    event_seqs,
                });
            }
            let stored = messages::load_stored_message_tx(tx, board_id, &posted.message.id)?
                .expect("just inserted");
            (posted.message.id, ch, stored.seq, false)
        } else {
            let target_id = input.re.as_deref().expect("checked");
            let target =
                messages::load_stored_message_tx(tx, board_id, target_id)?.ok_or_else(|| {
                    BoardError::new(
                        ErrorCode::MessageNotFound,
                        format!("message '{target_id}' not found"),
                    )
                })?;
            let perms =
                grants::caller_permissions_on_channel(&board_grants, caller, &target.channel);
            if !perms.can_read() {
                return Err(BoardError::new(
                    ErrorCode::MessageNotFound,
                    format!("message '{target_id}' not found"),
                ));
            }
            if !perms.can_label() {
                return Err(BoardError::forbidden(format!(
                    "creating a task in '{}' requires 'label' permission",
                    target.channel
                )));
            }
            let root_id = target.re.unwrap_or(target.id);
            let root_msg =
                messages::load_stored_message_tx(tx, board_id, &root_id)?.ok_or_else(|| {
                    BoardError::new(
                        ErrorCode::MessageNotFound,
                        format!("thread root '{root_id}' not found"),
                    )
                })?;
            if root_msg.deleted_at.is_some() {
                return Err(BoardError::new(
                    ErrorCode::MessageDeleted,
                    format!("cannot create a task from deleted message '{root_id}'"),
                ));
            }
            if load_task_row_tx(tx, board_id, &root_id)?.is_some() {
                return Err(BoardError::new(
                    ErrorCode::TaskAlreadyExists,
                    format!("thread '{root_id}' is already a task"),
                ));
            }
            actors::record_actor_write_tx(tx, board_id, &root_msg.channel, caller)?;
            (root_id, root_msg.channel, root_msg.seq, false)
        };

        // Self-need check for existing thread converted to task
        if valid_needs.iter().any(|n| n == &root_id) {
            return Err(BoardError::new(
                ErrorCode::InvalidNeeds,
                "a task cannot depend on itself",
            ));
        }

        let stored_msg = messages::load_stored_message_tx(tx, board_id, &root_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored_msg)?;

        let mut all_done = true;
        for nid in &valid_needs {
            if let Some(p) = load_task_row_tx(tx, board_id, nid)? {
                if p.state != TaskState::Completed {
                    all_done = false;
                }
            }
        }
        let initial_state = if all_done {
            TaskState::Ready
        } else {
            TaskState::Waiting
        };

        tx.execute(
            "INSERT INTO tasks (
                message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                result, fail_reason, cancel_reason, created_at, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                NULL, 0, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, ?8, ?8
             )",
            params![
                root_id,
                board_id,
                channel,
                msg_seq,
                initial_state.as_str(),
                owner_actor,
                assigned_for,
                now_str
            ],
        )?;

        for nid in &valid_needs {
            tx.execute(
                "INSERT INTO task_needs (board_id, task_id, needs_task_id) VALUES (?1, ?2, ?3)",
                params![board_id, root_id, nid],
            )?;
        }

        labels::set_system_label_tx(tx, board_id, &root_id, "board.task", initial_state.as_str())?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored_msg)?;
        let after_msg = messages::hydrate_message_view_tx(tx, &stored_msg)?;

        let ev_created = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskCreated,
                channel: Some(&channel),
                message_id: Some(&root_id),
                actor_id: Some(&owner_actor),
                payload: json!({
                    "state": initial_state.as_str(),
                    "needs": valid_needs,
                    "for": assigned_for
                }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&after_msg),
            },
        )?;
        event_seqs.push(ev_created.seq);

        if initial_state == TaskState::Ready {
            let ev_ready = events::append_event_tx(
                tx,
                EventAppend {
                    board_id,
                    event_type: EventType::TaskReady,
                    channel: Some(&channel),
                    message_id: Some(&root_id),
                    actor_id: Some(&owner_actor),
                    payload: json!({ "from": "created" }),
                    before_snapshot: Some(&before_snap),
                    after_snapshot: Some(&after_snap),
                    after_message: Some(&after_msg),
                },
            )?;
            event_seqs.push(ev_ready.seq);
        }

        let (final_msg, claim) = if input.claim {
            let task_row = load_task_row_tx(tx, board_id, &root_id)?.unwrap();
            let (claimed, ev_seq) =
                claim_task_inner_tx(tx, board_id, &task_row, caller, lease_ms, now)?;
            event_seqs.push(ev_seq);
            let m = claimed.task.clone();
            (m, Some(claimed))
        } else {
            (after_msg, None)
        };

        Ok(CreateTaskResult {
            task: final_msg,
            claim,
            replayed,
            event_seqs,
        })
    })
}

/// Atomically creates a DAG batch of tasks in one SQLite transaction (`createMany`).
pub fn create_many_tasks(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: CreateManyTasksInput,
) -> BoardResult<BatchCreateResult> {
    validate_channel_path(&input.channel)?;
    if input.tasks.is_empty() {
        return Err(BoardError::bad_request(
            "batch 'tasks' array cannot be empty",
        ));
    }

    let max_items = if input.id.is_some() {
        MAX_BATCH_TASKS_WITH_ID
    } else {
        MAX_BATCH_TASKS
    };
    if input.tasks.len() > max_items {
        return Err(BoardError::new(
            ErrorCode::BatchTooLarge,
            format!(
                "batch has {} tasks, exceeding maximum of {max_items}",
                input.tasks.len()
            ),
        ));
    }

    let total_labels: usize = input.tasks.iter().map(|t| t.labels.len()).sum();
    let total_attachments: usize = input.tasks.iter().map(|t| t.attachments.len()).sum();
    let total_needs: usize = input.tasks.iter().map(|t| t.needs.len()).sum();
    let weighted_budget = input.tasks.len() * 6
        + total_labels
        + total_attachments
        + total_needs
        + usize::from(input.id.is_some()) * 6;
    if weighted_budget > MAX_BATCH_WRITE_BUDGET {
        return Err(BoardError::new(
            ErrorCode::BatchTooLarge,
            "batch exceeds single-write budget (tasks + labels + attachments + needs); split into smaller chunks",
        ));
    }

    for t in &input.tasks {
        validate_text_length(&t.text)?;
    }

    // Validate internal keys & compute topological order
    let mut key_to_idx: HashMap<String, usize> = HashMap::new();
    for (idx, t) in input.tasks.iter().enumerate() {
        if let Some(k) = &t.key {
            if k.trim().is_empty() {
                return Err(BoardError::new(
                    ErrorCode::InvalidNeeds,
                    "task key cannot be empty",
                ));
            }
            if key_to_idx.insert(k.clone(), idx).is_some() {
                return Err(BoardError::new(
                    ErrorCode::InvalidNeeds,
                    format!("duplicate task key '{k}' in batch"),
                ));
            }
        }
    }

    let n = input.tasks.len();
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (idx, t) in input.tasks.iter().enumerate() {
        let mut seen_internal: HashSet<usize> = HashSet::new();
        for need in &t.needs {
            if let BatchNeedRef::Key { key } = need {
                let Some(&dep_idx) = key_to_idx.get(key) else {
                    return Err(BoardError::new(
                        ErrorCode::InvalidNeeds,
                        format!("unknown internal task key '{key}'"),
                    ));
                };
                if dep_idx == idx {
                    return Err(BoardError::new(
                        ErrorCode::InvalidNeeds,
                        format!("task key '{key}' cannot depend on itself"),
                    ));
                }
                if seen_internal.insert(dep_idx) {
                    dependents[dep_idx].push(idx);
                    in_degree[idx] += 1;
                }
            }
        }
    }

    let mut queue: VecDeque<usize> = VecDeque::new();
    for (idx, &deg) in in_degree.iter().enumerate() {
        if deg == 0 {
            queue.push_back(idx);
        }
    }
    let mut topo_order: Vec<usize> = Vec::with_capacity(n);
    while let Some(u) = queue.pop_front() {
        topo_order.push(u);
        for &v in &dependents[u] {
            in_degree[v] -= 1;
            if in_degree[v] == 0 {
                queue.push_back(v);
            }
        }
    }
    if topo_order.len() != n {
        return Err(BoardError::new(
            ErrorCode::InvalidNeeds,
            "batch task dependencies contain a cycle",
        ));
    }

    let request_hash = {
        let canonical = serde_json::to_string(&(&input.channel, &input.tasks))
            .map_err(|e| BoardError::internal(format!("failed to hash batch: {e}")))?;
        hash_token(&canonical)
    };

    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let owner_actor = caller.actor_id();
        let mut event_seqs: Vec<i64> = Vec::new();

        // Check batch idempotency
        if let Some(idem_key) = &input.id {
            let existing: Option<(String, String, String)> = tx
                .query_row(
                    "SELECT channel, request_hash, task_ids_json
                     FROM task_batches
                     WHERE board_id = ?1 AND caller_actor = ?2 AND idempotency_key = ?3",
                    params![board_id, owner_actor, idem_key],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((ch, hash, ids_json)) = existing {
                if ch != input.channel || hash != request_hash {
                    return Err(BoardError::new(
                        ErrorCode::DuplicateId,
                        format!("idempotency key '{idem_key}' was already used for different batch tasks"),
                    ));
                }
                let ids: Vec<String> = serde_json::from_str(&ids_json)
                    .map_err(|e| BoardError::internal(format!("corrupted batch ids: {e}")))?;
                let mut views = Vec::with_capacity(ids.len());
                for id in ids {
                    let stored = messages::load_stored_message_tx(tx, board_id, &id)?
                        .ok_or_else(|| BoardError::internal("missing batch task message"))?;
                    views.push(messages::hydrate_message_view_tx(tx, &stored)?);
                }
                return Ok(BatchCreateResult {
                    tasks: views,
                    replayed: true,
                    event_seqs,
                });
            }
            let used_in_msg: bool = tx
                .query_row(
                    "SELECT 1 FROM idempotency_keys
                     WHERE board_id = ?1 AND caller_actor = ?2 AND key = ?3",
                    params![board_id, owner_actor, idem_key],
                    |_| Ok(true),
                )
                .optional()?
                .unwrap_or(false);
            if used_in_msg {
                return Err(BoardError::new(
                    ErrorCode::DuplicateId,
                    format!("idempotency key '{idem_key}' was already used for a post or reply"),
                ));
            }
        }

        let board_grants = grants::load_grants_tx(tx, board_id)?;
        let perms = grants::caller_permissions_on_channel(&board_grants, caller, &input.channel);
        if !perms.can_label() {
            return Err(BoardError::forbidden(format!(
                "creating tasks in '{}' requires 'label' permission",
                input.channel
            )));
        }

        let mut created_ids: Vec<Option<String>> = vec![None; n];
        for idx in topo_order {
            let item = &input.tasks[idx];
            let assigned_for =
                resolve_assigned_for(&owner_actor, item.assigned_for.as_deref())?;

            let mut resolved_need_ids: Vec<String> = Vec::with_capacity(item.needs.len());
            for need in &item.needs {
                match need {
                    BatchNeedRef::Id(id) => resolved_need_ids.push(id.clone()),
                    BatchNeedRef::Key { key } => {
                        let dep_idx = key_to_idx[key];
                        let dep_id = created_ids[dep_idx]
                            .clone()
                            .expect("topological order ensures dependency created first");
                        resolved_need_ids.push(dep_id);
                    }
                }
            }
            let valid_needs = validate_prereqs_readable_tx(
                tx,
                board_id,
                &board_grants,
                caller,
                &resolved_need_ids,
            )?;

            let posted = messages::post_message_in_tx(
                tx,
                board_id,
                caller,
                PostMessageInput {
                    channel: input.channel.clone(),
                    text: item.text.clone(),
                    id: None,
                    attachments: item.attachments.clone(),
                    labels: item.labels.clone(),
                    schema: item.schema.clone(),
                    no_sync: item.no_sync,
                },
            )?;
            if let Some(seq) = posted.event_seq {
                event_seqs.push(seq);
            }
            let root_id = posted.message.id;
            let stored = messages::load_stored_message_tx(tx, board_id, &root_id)?.unwrap();
            let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

            let mut all_done = true;
            for nid in &valid_needs {
                if let Some(p) = load_task_row_tx(tx, board_id, nid)? {
                    if p.state != TaskState::Completed {
                        all_done = false;
                    }
                }
            }
            let initial_state = if all_done {
                TaskState::Ready
            } else {
                TaskState::Waiting
            };

            tx.execute(
                "INSERT INTO tasks (
                    message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                    claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                    result, fail_reason, cancel_reason, created_at, updated_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                    NULL, 0, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, ?8, ?8
                 )",
                params![
                    root_id,
                    board_id,
                    input.channel,
                    stored.seq,
                    initial_state.as_str(),
                    owner_actor,
                    assigned_for,
                    now_str
                ],
            )?;
            for nid in &valid_needs {
                tx.execute(
                    "INSERT INTO task_needs (board_id, task_id, needs_task_id) VALUES (?1, ?2, ?3)",
                    params![board_id, root_id, nid],
                )?;
            }
            labels::set_system_label_tx(
                tx,
                board_id,
                &root_id,
                "board.task",
                initial_state.as_str(),
            )?;

            let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
            let after_msg = messages::hydrate_message_view_tx(tx, &stored)?;
            let ev_created = events::append_event_tx(
                tx,
                EventAppend {
                    board_id,
                    event_type: EventType::TaskCreated,
                    channel: Some(&input.channel),
                    message_id: Some(&root_id),
                    actor_id: Some(&owner_actor),
                    payload: json!({
                        "state": initial_state.as_str(),
                        "needs": valid_needs,
                        "for": assigned_for
                    }),
                    before_snapshot: Some(&before_snap),
                    after_snapshot: Some(&after_snap),
                    after_message: Some(&after_msg),
                },
            )?;
            event_seqs.push(ev_created.seq);

            if initial_state == TaskState::Ready {
                let ev_ready = events::append_event_tx(
                    tx,
                    EventAppend {
                        board_id,
                        event_type: EventType::TaskReady,
                        channel: Some(&input.channel),
                        message_id: Some(&root_id),
                        actor_id: Some(&owner_actor),
                        payload: json!({ "from": "created" }),
                        before_snapshot: Some(&before_snap),
                        after_snapshot: Some(&after_snap),
                        after_message: Some(&after_msg),
                    },
                )?;
                event_seqs.push(ev_ready.seq);
            }

            created_ids[idx] = Some(root_id);
        }

        let ordered_ids: Vec<String> = created_ids.into_iter().map(Option::unwrap).collect();
        if let Some(idem_key) = &input.id {
            let ids_json = serde_json::to_string(&ordered_ids)
                .map_err(|e| BoardError::internal(format!("failed to serialize batch ids: {e}")))?;
            tx.execute(
                "INSERT INTO task_batches (board_id, caller_actor, idempotency_key, channel, request_hash, task_ids_json, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    board_id,
                    owner_actor,
                    idem_key,
                    input.channel,
                    request_hash,
                    ids_json,
                    now_str
                ],
            )?;
        }

        let mut views = Vec::with_capacity(ordered_ids.len());
        for id in ordered_ids {
            let stored = messages::load_stored_message_tx(tx, board_id, &id)?.unwrap();
            views.push(messages::hydrate_message_view_tx(tx, &stored)?);
        }

        Ok(BatchCreateResult {
            tasks: views,
            replayed: false,
            event_seqs,
        })
    })
}

pub fn get_task(
    db: &Database,
    events_hub: &EventHub,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
) -> BoardResult<MessageView> {
    let emitted = db.with_tx(|tx| expire_due_leases_tx(tx, Some(board_id), Utc::now()))?;
    for (bid, seq) in emitted {
        events_hub.notify(&bid, seq);
    }

    db.with_conn(|conn| {
        let task = load_task_row_tx(conn, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        if !grants::caller_permissions_on_channel(&board_grants, caller, &task.channel).can_read() {
            return Err(BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            ));
        }
        let stored = messages::load_stored_message_tx(conn, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        messages::hydrate_message_view_tx(conn, &stored)
    })
}

pub fn list_tasks(
    db: &Database,
    events_hub: &EventHub,
    board_id: &str,
    caller: &CallerContext,
    query: TaskListQuery,
) -> BoardResult<TasksPage> {
    let limit = query.limit.unwrap_or(MAX_TASKS_PAGE_LIMIT);
    if limit == 0 || limit > MAX_TASKS_PAGE_LIMIT {
        return Err(BoardError::bad_request(format!(
            "tasks limit must be between 1 and {MAX_TASKS_PAGE_LIMIT}"
        )));
    }

    let state_mode = match query.state.as_deref().map(str::trim) {
        None | Some("") | Some("open") => None, // open states
        Some("all") => Some(None),              // all states
        Some(other) => Some(Some(TaskState::parse(other)?)),
    };

    let qfilter = QueryFilter {
        channel: query.channel.clone(),
        by: query.by.clone(),
        labels: query.labels.clone(),
        ..Default::default()
    };
    qfilter.validate()?;

    let emitted = db.with_tx(|tx| expire_due_leases_tx(tx, Some(board_id), Utc::now()))?;
    for (bid, seq) in emitted {
        events_hub.notify(&bid, seq);
    }

    db.with_conn(|conn| {
        let board_grants = grants::load_grants_tx(conn, board_id)?;
        let has_wildcard_read =
            grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_read();
        let visible_actors = actors::list_board_actors(conn, board_id, has_wildcard_read, |ch| {
            grants::caller_permissions_on_channel(&board_grants, caller, ch).can_read()
        })?;
        let filter_ctx = FilterContext::build(conn, caller, &visible_actors, &qfilter)?;

        let after_seq: i64 = match &query.after {
            None => 0,
            Some(c) if c == "0" => 0,
            Some(c) => {
                if let Some(m) = messages::load_stored_message_tx(conn, board_id, c)? {
                    m.seq
                } else if let Some(ev_seq) = events::parse_event_cursor_seq(c) {
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
                    msg_seq.unwrap_or(0)
                } else {
                    return Err(BoardError::bad_request(format!(
                        "invalid tasks cursor '{c}'"
                    )));
                }
            }
        };

        let mut stmt = conn.prepare(
            "SELECT message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                    claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                    result, fail_reason, cancel_reason
             FROM tasks
             WHERE board_id = ?1 AND seq > ?2
             ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![board_id, after_seq], map_task_row)?;

        let mut summaries = Vec::new();
        let mut last_returned_id: Option<String> = None;

        for row_res in rows {
            let task = row_res?;
            match state_mode {
                None => {
                    if !task.state.is_open() {
                        continue;
                    }
                }
                Some(None) => {}
                Some(Some(want)) => {
                    if task.state != want {
                        continue;
                    }
                }
            }

            if !grants::caller_permissions_on_channel(&board_grants, caller, &task.channel)
                .can_read()
            {
                continue;
            }
            if !qfilter.matches_channel(&task.channel) {
                continue;
            }
            if let Some(owner_f) = &query.owner {
                if !matches_actor_or_principal_filter(owner_f, caller, &task.owner_actor) {
                    continue;
                }
            }
            if let Some(for_f) = &query.assigned_for {
                let Some(assigned) = &task.assigned_for else {
                    continue;
                };
                if !matches_for_filter(for_f, caller, assigned) {
                    continue;
                }
            }
            if let Some(needs_f) = &query.needs {
                let needs = load_task_needs_tx(conn, &task.message_id)?;
                if !needs.iter().any(|n| n == needs_f) {
                    continue;
                }
            }

            let Some(stored) = messages::load_stored_message_tx(conn, board_id, &task.message_id)?
            else {
                continue;
            };
            let snap = messages::build_message_snapshot_tx(conn, &stored)?;
            if !qfilter.matches_snapshot(&snap, &filter_ctx) {
                continue;
            }

            let root_view = messages::hydrate_message_view_tx(conn, &stored)?;
            let reply_count: usize = conn.query_row(
                "SELECT COUNT(*) FROM messages WHERE board_id = ?1 AND re = ?2",
                params![board_id, task.message_id],
                |r| r.get::<_, i64>(0),
            )? as usize;
            let last_reply = conn
                .query_row(
                    "SELECT id FROM messages WHERE board_id = ?1 AND re = ?2 ORDER BY seq DESC LIMIT 1",
                    params![board_id, task.message_id],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
                .and_then(|rid| messages::load_stored_message_tx(conn, board_id, &rid).ok().flatten())
                .map(|r| messages::hydrate_message_view_tx(conn, &r))
                .transpose()?;

            last_returned_id = Some(task.message_id.clone());
            summaries.push(ThreadSummaryView {
                message: root_view,
                replies: reply_count,
                last: last_reply,
            });
            if summaries.len() >= limit {
                break;
            }
        }

        let cursor = last_returned_id
            .or_else(|| query.after.clone())
            .unwrap_or_else(|| "0".to_string());
        Ok(TasksPage {
            tasks: summaries,
            cursor,
        })
    })
}

fn matches_actor_or_principal_filter(filter: &str, caller: &CallerContext, actor_id: &str) -> bool {
    if filter.eq_ignore_ascii_case("me") {
        if caller.profile.is_some() {
            return actor_id == caller.actor_id();
        }
        let p = caller.principal_id();
        return actor_id == p
            || actor_id
                .strip_prefix(&p)
                .is_some_and(|r| r.starts_with('+'));
    }
    if filter.contains('+') {
        actor_id == filter
    } else if filter.contains(':') {
        actor_id == filter
            || actor_id
                .strip_prefix(filter)
                .is_some_and(|r| r.starts_with('+'))
    } else {
        actor_id == filter
    }
}

fn matches_for_filter(filter: &str, caller: &CallerContext, assigned: &str) -> bool {
    if filter.eq_ignore_ascii_case("me") {
        return assigned == caller.actor_id();
    }
    if filter.contains(':') {
        return assigned == filter;
    }
    assigned
        .split_once('+')
        .is_some_and(|(_, prof)| prof.eq_ignore_ascii_case(filter))
}

/// Claims one specific task by ID.
pub fn claim_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: ClaimInput,
) -> BoardResult<ClaimResult> {
    let lease_ms = parse_lease_ms(input.lease, input.lease_ms)?;
    db.with_tx(|tx| {
        let now = Utc::now();
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(tx, board_id)?;
        let perms = grants::caller_permissions_on_channel(&board_grants, caller, &task.channel);
        if !perms.can_read() {
            return Err(BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            ));
        }
        if !perms.can_label() {
            return Err(BoardError::forbidden(format!(
                "claiming a task in '{}' requires 'label' permission",
                task.channel
            )));
        }

        let (claim, seq) = claim_task_inner_tx(tx, board_id, &task, caller, lease_ms, now)?;
        event_seqs.push(seq);
        Ok(ClaimResult { claim, event_seqs })
    })
}

fn parse_claim_labels(raw: Option<&ClaimLabelFilter>) -> BoardResult<Vec<LabelPredicate>> {
    let Some(f) = raw else {
        return Ok(Vec::new());
    };
    let mut preds = Vec::new();
    match f {
        ClaimLabelFilter::Map(map) => {
            for (k, v) in map {
                preds.push(LabelPredicate::parse(&format!("{k}={v}"))?);
            }
        }
        ClaimLabelFilter::List(list) => {
            for item in list {
                preds.push(LabelPredicate::parse(item)?);
            }
        }
    }
    Ok(preds)
}

#[derive(Debug, Clone)]
pub struct ClaimReadyAttempt {
    pub claimed: Option<ClaimResult>,
    pub expired_seqs: Vec<i64>,
    pub next_expiry: Option<DateTime<Utc>>,
}

/// Attempts one immediate queue-claim (`claim --ready`) in `input.channel` (exact or `prefix/` subtree).
///
/// Enforces:
/// - max 50 channels in subtree (`400 channel_tree_too_large`)
/// - max 200 `(channel x owner)` places (`400 claim_too_wide`)
/// - inspects at most 25 oldest ready tasks per `(channel, owner)` place for `--label` matching
/// - priority: assigned (`for`) to caller first, then unassigned owned by caller's principal + `from`
pub fn try_claim_ready_once(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    channel_spec: &str,
    label_preds: &[LabelPredicate],
    from_owners: &[String],
    lease_ms: i64,
) -> BoardResult<ClaimReadyAttempt> {
    if channel_spec != "*" {
        if channel_spec.ends_with('/') {
            validate_channel_pattern(channel_spec)?;
        } else {
            validate_channel_path(channel_spec)?;
        }
    }

    db.with_tx(|tx| {
        let now = Utc::now();
        let mut expired_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            expired_seqs.push(seq);
        }

        let board_grants = grants::load_grants_tx(tx, board_id)?;

        // Resolve matching claimable channels (read + label)
        let mut all_channels: Vec<String> = Vec::new();
        if let Some(prefix) = channel_spec.strip_suffix('/') {
            let mut stmt = tx.prepare(
                "SELECT path FROM channels WHERE board_id = ?1 AND (path = ?2 OR path LIKE ?3) ORDER BY path ASC",
            )?;
            let prefix_like = format!("{prefix}/%");
            let rows = stmt.query_map(params![board_id, prefix, prefix_like], |r| {
                r.get::<_, String>(0)
            })?;
            for r in rows {
                let ch = r?;
                let perms = grants::caller_permissions_on_channel(&board_grants, caller, &ch);
                if perms.can_read() && perms.can_label() {
                    all_channels.push(ch);
                }
            }
            if all_channels.len() > MAX_CLAIM_SUBTREE_CHANNELS {
                return Err(BoardError::new(
                    ErrorCode::ChannelTreeTooLarge,
                    format!(
                        "subtree '{channel_spec}' has {} claimable channels, exceeding maximum of {MAX_CLAIM_SUBTREE_CHANNELS}",
                        all_channels.len()
                    ),
                ));
            }
        } else if channel_spec == "*" {
            let mut stmt =
                tx.prepare("SELECT path FROM channels WHERE board_id = ?1 ORDER BY path ASC")?;
            let rows = stmt.query_map(params![board_id], |r| r.get::<_, String>(0))?;
            for r in rows {
                let ch = r?;
                let perms = grants::caller_permissions_on_channel(&board_grants, caller, &ch);
                if perms.can_read() && perms.can_label() {
                    all_channels.push(ch);
                }
            }
            if all_channels.len() > MAX_CLAIM_SUBTREE_CHANNELS {
                return Err(BoardError::new(
                    ErrorCode::ChannelTreeTooLarge,
                    format!(
                        "channel tree has {} claimable channels, exceeding maximum of {MAX_CLAIM_SUBTREE_CHANNELS}",
                        all_channels.len()
                    ),
                ));
            }
        } else {
            let perms =
                grants::caller_permissions_on_channel(&board_grants, caller, channel_spec);
            if !perms.can_read() || !perms.can_label() {
                return Err(BoardError::forbidden(format!(
                    "claiming ready tasks in '{channel_spec}' requires 'read' and 'label' permissions"
                )));
            }
            all_channels.push(channel_spec.to_string());
        }

        // Owners list: caller's principal + deduplicated `from_owners`
        let caller_principal = caller.principal_id();
        let mut owners: Vec<String> = vec![caller_principal.clone()];
        for extra in from_owners {
            let clean = extra.trim();
            if !clean.is_empty() && !owners.iter().any(|o| o == clean) {
                owners.push(clean.to_string());
            }
        }

        let effective_channels = all_channels.len().max(1);
        let places = effective_channels.saturating_mul(owners.len());
        if places > MAX_CLAIM_PLACES {
            return Err(BoardError::new(
                ErrorCode::ClaimTooWide,
                format!(
                    "claim covers {places} (channel x owner) places, exceeding maximum of {MAX_CLAIM_PLACES}"
                ),
            ));
        }

        let caller_actor = caller.actor_id();
        let mut best_assigned: Option<StoredTaskRow> = None;
        let mut best_unassigned: Option<StoredTaskRow> = None;

        for ch in &all_channels {
            // 1. Assigned to caller (inspect up to 25 oldest ready tasks)
            {
                let mut stmt = tx.prepare(
                    "SELECT message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                            claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                            result, fail_reason, cancel_reason
                     FROM tasks
                     WHERE board_id = ?1 AND channel = ?2 AND state = 'ready' AND assigned_for = ?3
                     ORDER BY seq ASC
                     LIMIT ?4",
                )?;
                let rows = stmt.query_map(
                    params![board_id, ch, caller_actor, MAX_CLAIM_SCAN_PER_PLACE as i64],
                    map_task_row,
                )?;
                for r in rows {
                    let t = r?;
                    if task_matches_labels_tx(tx, &t.message_id, label_preds)? {
                        if best_assigned.as_ref().is_none_or(|b| t.seq < b.seq) {
                            best_assigned = Some(t);
                        }
                        break;
                    }
                }
            }

            // 2. Unassigned tasks per owner place (inspect at most 25 oldest ready tasks per place)
            if best_assigned.is_none() {
                for owner_spec in &owners {
                    let (sql, p_param): (&str, String) = if owner_spec.contains('+') {
                        (
                            "SELECT message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                                    claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                                    result, fail_reason, cancel_reason
                             FROM tasks
                             WHERE board_id = ?1 AND channel = ?2 AND state = 'ready'
                               AND assigned_for IS NULL AND owner_actor = ?3
                             ORDER BY seq ASC
                             LIMIT ?4",
                            owner_spec.clone(),
                        )
                    } else {
                        (
                            "SELECT message_id, board_id, channel, seq, state, owner_actor, assigned_for,
                                    claimed_by, claim_epoch, claim_token, claim_until, block_reason, block_kind,
                                    result, fail_reason, cancel_reason
                             FROM tasks
                             WHERE board_id = ?1 AND channel = ?2 AND state = 'ready'
                               AND assigned_for IS NULL
                               AND (owner_actor = ?3 OR owner_actor LIKE ?5)
                             ORDER BY seq ASC
                             LIMIT ?4",
                            owner_spec.clone(),
                        )
                    };

                    let mut stmt = tx.prepare(sql)?;
                    let owner_like = format!("{p_param}+%");
                    let rows = if owner_spec.contains('+') {
                        stmt.query_map(
                            params![board_id, ch, p_param, MAX_CLAIM_SCAN_PER_PLACE as i64],
                            map_task_row,
                        )?
                    } else {
                        stmt.query_map(
                            params![
                                board_id,
                                ch,
                                p_param,
                                MAX_CLAIM_SCAN_PER_PLACE as i64,
                                owner_like
                            ],
                            map_task_row,
                        )?
                    };
                    for r in rows {
                        let t = r?;
                        if task_matches_labels_tx(tx, &t.message_id, label_preds)? {
                            if best_unassigned.as_ref().is_none_or(|b| t.seq < b.seq) {
                                best_unassigned = Some(t);
                            }
                            break;
                        }
                    }
                }
            }
        }

        if let Some(winner) = best_assigned.or(best_unassigned) {
            let (claim, seq) = claim_task_inner_tx(tx, board_id, &winner, caller, lease_ms, now)?;
            let mut seqs = expired_seqs;
            seqs.push(seq);
            return Ok(ClaimReadyAttempt {
                claimed: Some(ClaimResult {
                    claim,
                    event_seqs: seqs,
                }),
                expired_seqs: Vec::new(),
                next_expiry: None,
            });
        }

        // Compute earliest active claim expiry on this board for lease-expiry wakeup
        let next_expiry: Option<DateTime<Utc>> = tx
            .query_row(
                "SELECT MIN(claim_until) FROM tasks WHERE board_id = ?1 AND state = 'claimed'",
                params![board_id],
                |r| r.get::<_, Option<String>>(0),
            )?
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        Ok(ClaimReadyAttempt {
            claimed: None,
            expired_seqs,
            next_expiry,
        })
    })
}

fn task_matches_labels_tx(
    conn: &Connection,
    task_id: &str,
    preds: &[LabelPredicate],
) -> BoardResult<bool> {
    if preds.is_empty() {
        return Ok(true);
    }
    let map = labels::load_message_labels_tx(conn, task_id)?;
    Ok(preds.iter().all(|p| p.matches_map(&map)))
}

pub fn parse_claim_ready_params(
    input: &ClaimReadyInput,
) -> BoardResult<(Vec<LabelPredicate>, i64, std::time::Duration)> {
    let preds = parse_claim_labels(input.label.as_ref())?;
    let lease_ms = parse_lease_ms(input.lease, input.lease_ms)?;
    let wait_dur = match &input.wait {
        None => std::time::Duration::ZERO,
        Some(Value::Number(n)) => {
            let secs = n
                .as_f64()
                .ok_or_else(|| BoardError::bad_request("invalid wait duration"))?;
            if !secs.is_finite() || secs < 0.0 || secs > events::MAX_WAIT_TIMEOUT_SECS {
                return Err(BoardError::bad_request(format!(
                    "wait must be between 0 and {} seconds",
                    events::MAX_WAIT_TIMEOUT_SECS
                )));
            }
            std::time::Duration::from_secs_f64(secs)
        }
        Some(Value::String(s)) => {
            events::parse_duration_param(s, events::MAX_WAIT_TIMEOUT_SECS)?
        }
        Some(_) => return Err(BoardError::bad_request("invalid wait duration")),
    };
    Ok((preds, lease_ms, wait_dur))
}

pub fn renew_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: RenewInput,
) -> BoardResult<ClaimResult> {
    let lease_ms = parse_lease_ms(input.lease, input.lease_ms)?;
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        validate_active_lease(&task, input.epoch, &input.token, &now_str)?;

        let until_str = format_utc_ms(now + Duration::milliseconds(lease_ms));
        tx.execute(
            "UPDATE tasks SET claim_until = ?1, updated_at = ?2 WHERE message_id = ?3",
            params![until_str, now_str, task_id],
        )?;

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();
        let ev = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskRenewed,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "epoch": task.claim_epoch, "until": until_str }),
                before_snapshot: Some(&snap),
                after_snapshot: Some(&snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev.seq);

        Ok(ClaimResult {
            claim: ClaimedTaskView {
                task: view,
                epoch: task.claim_epoch,
                token: input.token,
                until: until_str,
            },
            event_seqs,
        })
    })
}

pub fn release_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: ReleaseInput,
) -> BoardResult<TransitionResult> {
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        validate_active_lease(&task, input.epoch, &input.token, &now_str)?;

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute(
            "UPDATE tasks
             SET state = 'ready', claimed_by = NULL, claim_token = NULL, claim_until = NULL, updated_at = ?1
             WHERE message_id = ?2",
            params![now_str, task_id],
        )?;
        labels::set_system_label_tx(tx, board_id, task_id, "board.task", TaskState::Ready.as_str())?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev_rel = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskReleased,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "epoch": task.claim_epoch }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev_rel.seq);

        let ev_ready = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskReady,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "from": "released" }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev_ready.seq);

        Ok(TransitionResult {
            task: view,
            readied: Vec::new(),
            event_seqs,
        })
    })
}

pub fn complete_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: CompleteInput,
) -> BoardResult<TransitionResult> {
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        validate_active_lease(&task, input.epoch, &input.token, &now_str)?;

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute(
            "UPDATE tasks
             SET state = 'completed', result = ?1, claimed_by = NULL, claim_token = NULL,
                 claim_until = NULL, updated_at = ?2
             WHERE message_id = ?3",
            params![input.result, now_str, task_id],
        )?;
        labels::set_system_label_tx(
            tx,
            board_id,
            task_id,
            "board.task",
            TaskState::Completed.as_str(),
        )?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev_comp = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskCompleted,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "epoch": task.claim_epoch, "result": input.result }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev_comp.seq);

        // Cascade: check every `waiting` dependent of `task_id`
        let mut stmt = tx.prepare(
            "SELECT tn.task_id
             FROM task_needs tn
             JOIN tasks t ON t.message_id = tn.task_id
             WHERE tn.board_id = ?1 AND tn.needs_task_id = ?2 AND t.state = 'waiting'
             ORDER BY t.seq ASC",
        )?;
        let dep_ids: Vec<String> = stmt
            .query_map(params![board_id, task_id], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;

        let mut readied = Vec::new();
        for dep_id in dep_ids {
            if all_needs_completed_tx(tx, &dep_id)? {
                let dep_stored = messages::load_stored_message_tx(tx, board_id, &dep_id)?.unwrap();
                let dep_before = messages::build_message_snapshot_tx(tx, &dep_stored)?;
                tx.execute(
                    "UPDATE tasks SET state = 'ready', updated_at = ?1 WHERE message_id = ?2",
                    params![now_str, dep_id],
                )?;
                labels::set_system_label_tx(
                    tx,
                    board_id,
                    &dep_id,
                    "board.task",
                    TaskState::Ready.as_str(),
                )?;
                let dep_after = messages::build_message_snapshot_tx(tx, &dep_stored)?;
                let dep_view = messages::hydrate_message_view_tx(tx, &dep_stored)?;
                let ev_r = events::append_event_tx(
                    tx,
                    EventAppend {
                        board_id,
                        event_type: EventType::TaskReady,
                        channel: Some(&dep_stored.channel),
                        message_id: Some(&dep_id),
                        actor_id: Some(&actor_id),
                        payload: json!({ "from": "dependency_completed", "prerequisite": task_id }),
                        before_snapshot: Some(&dep_before),
                        after_snapshot: Some(&dep_after),
                        after_message: Some(&dep_view),
                    },
                )?;
                event_seqs.push(ev_r.seq);
                readied.push(dep_id);
            }
        }

        Ok(TransitionResult {
            task: view,
            readied,
            event_seqs,
        })
    })
}

pub fn fail_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: FailInput,
) -> BoardResult<TransitionResult> {
    if input.reason.trim().is_empty() {
        return Err(BoardError::bad_request("fail reason cannot be empty"));
    }
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        validate_active_lease(&task, input.epoch, &input.token, &now_str)?;

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute(
            "UPDATE tasks
             SET state = 'failed', fail_reason = ?1, claimed_by = NULL, claim_token = NULL,
                 claim_until = NULL, updated_at = ?2
             WHERE message_id = ?3",
            params![input.reason, now_str, task_id],
        )?;
        labels::set_system_label_tx(tx, board_id, task_id, "board.task", TaskState::Failed.as_str())?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskFailed,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "epoch": task.claim_epoch, "reason": input.reason }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev.seq);

        Ok(TransitionResult {
            task: view,
            readied: Vec::new(),
            event_seqs,
        })
    })
}

pub fn block_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: BlockInput,
) -> BoardResult<TransitionResult> {
    if input.reason.trim().is_empty() {
        return Err(BoardError::bad_request("block reason cannot be empty"));
    }
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(tx, board_id)?;

        if let (Some(epoch), Some(token)) = (input.epoch, input.token.as_deref()) {
            validate_active_lease(&task, epoch, token, &now_str)?;
        } else {
            require_owner_or_manage(caller, &board_grants, &task)?;
            match task.state {
                TaskState::Claimed => {
                    return Err(BoardError::new(
                        ErrorCode::TaskClaimed,
                        "cannot block a claimed task without the active lease credentials",
                    ));
                }
                TaskState::Completed | TaskState::Failed | TaskState::Canceled => {
                    return Err(BoardError::new(
                        ErrorCode::TaskFinished,
                        format!("cannot block finished task '{}'", task.message_id),
                    ));
                }
                TaskState::Waiting | TaskState::Ready | TaskState::Blocked => {}
            }
        }

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute(
            "UPDATE tasks
             SET state = 'blocked', block_reason = ?1, block_kind = ?2,
                 claimed_by = NULL, claim_token = NULL, claim_until = NULL, updated_at = ?3
             WHERE message_id = ?4",
            params![input.reason, input.kind, now_str, task_id],
        )?;
        labels::set_system_label_tx(
            tx,
            board_id,
            task_id,
            "board.task",
            TaskState::Blocked.as_str(),
        )?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskBlocked,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "reason": input.reason, "kind": input.kind }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev.seq);

        Ok(TransitionResult {
            task: view,
            readied: Vec::new(),
            event_seqs,
        })
    })
}

pub fn unblock_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
) -> BoardResult<TransitionResult> {
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(tx, board_id)?;
        require_owner_or_manage(caller, &board_grants, &task)?;

        if task.state != TaskState::Blocked {
            return Err(BoardError::conflict(format!(
                "task '{task_id}' is '{}', not blocked",
                task.state.as_str()
            )));
        }

        let next_state = if all_needs_completed_tx(tx, task_id)? {
            TaskState::Ready
        } else {
            TaskState::Waiting
        };

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute(
            "UPDATE tasks
             SET state = ?1, block_reason = NULL, block_kind = NULL, updated_at = ?2
             WHERE message_id = ?3",
            params![next_state.as_str(), now_str, task_id],
        )?;
        labels::set_system_label_tx(tx, board_id, task_id, "board.task", next_state.as_str())?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskUnblocked,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "state": next_state.as_str() }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev.seq);

        if next_state == TaskState::Ready {
            let ev_r = events::append_event_tx(
                tx,
                EventAppend {
                    board_id,
                    event_type: EventType::TaskReady,
                    channel: Some(&task.channel),
                    message_id: Some(task_id),
                    actor_id: Some(&actor_id),
                    payload: json!({ "from": "unblocked" }),
                    before_snapshot: Some(&before_snap),
                    after_snapshot: Some(&after_snap),
                    after_message: Some(&view),
                },
            )?;
            event_seqs.push(ev_r.seq);
        }

        Ok(TransitionResult {
            task: view,
            readied: Vec::new(),
            event_seqs,
        })
    })
}

pub fn reopen_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
) -> BoardResult<TransitionResult> {
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(tx, board_id)?;
        require_owner_or_manage(caller, &board_grants, &task)?;

        if !matches!(task.state, TaskState::Failed | TaskState::Canceled) {
            return Err(BoardError::new(
                ErrorCode::TaskFinished,
                format!(
                    "only failed or canceled tasks can be reopened (task is '{}')",
                    task.state.as_str()
                ),
            ));
        }

        let next_state = if all_needs_completed_tx(tx, task_id)? {
            TaskState::Ready
        } else {
            TaskState::Waiting
        };

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute(
            "UPDATE tasks
             SET state = ?1, fail_reason = NULL, cancel_reason = NULL, updated_at = ?2
             WHERE message_id = ?3",
            params![next_state.as_str(), now_str, task_id],
        )?;
        labels::set_system_label_tx(tx, board_id, task_id, "board.task", next_state.as_str())?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskReopened,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "state": next_state.as_str() }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev.seq);

        if next_state == TaskState::Ready {
            let ev_r = events::append_event_tx(
                tx,
                EventAppend {
                    board_id,
                    event_type: EventType::TaskReady,
                    channel: Some(&task.channel),
                    message_id: Some(task_id),
                    actor_id: Some(&actor_id),
                    payload: json!({ "from": "reopened" }),
                    before_snapshot: Some(&before_snap),
                    after_snapshot: Some(&after_snap),
                    after_message: Some(&view),
                },
            )?;
            event_seqs.push(ev_r.seq);
        }

        Ok(TransitionResult {
            task: view,
            readied: Vec::new(),
            event_seqs,
        })
    })
}

pub fn cancel_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: CancelInput,
) -> BoardResult<TransitionResult> {
    if input.reason.trim().is_empty() {
        return Err(BoardError::bad_request("cancel reason cannot be empty"));
    }
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(tx, board_id)?;
        require_owner_or_manage(caller, &board_grants, &task)?;

        if matches!(task.state, TaskState::Completed | TaskState::Canceled) {
            return Err(BoardError::new(
                ErrorCode::TaskFinished,
                format!("task '{task_id}' is already {}", task.state.as_str()),
            ));
        }

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute(
            "UPDATE tasks
             SET state = 'canceled', cancel_reason = ?1, claimed_by = NULL, claim_token = NULL,
                 claim_until = NULL, updated_at = ?2
             WHERE message_id = ?3",
            params![input.reason, now_str, task_id],
        )?;
        labels::set_system_label_tx(
            tx,
            board_id,
            task_id,
            "board.task",
            TaskState::Canceled.as_str(),
        )?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskCanceled,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "reason": input.reason }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev.seq);

        Ok(TransitionResult {
            task: view,
            readied: Vec::new(),
            event_seqs,
        })
    })
}

pub fn assign_task(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: AssignInput,
) -> BoardResult<TransitionResult> {
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(tx, board_id)?;
        require_owner_or_manage(caller, &board_grants, &task)?;

        if task.state.is_finished() {
            return Err(BoardError::new(
                ErrorCode::TaskFinished,
                format!("cannot assign finished task '{task_id}'"),
            ));
        }

        let assigned_for = resolve_assigned_for(&task.owner_actor, input.assigned_for.as_deref())?;

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute(
            "UPDATE tasks SET assigned_for = ?1, updated_at = ?2 WHERE message_id = ?3",
            params![assigned_for, now_str, task_id],
        )?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskAssigned,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "for": assigned_for }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev.seq);

        Ok(TransitionResult {
            task: view,
            readied: Vec::new(),
            event_seqs,
        })
    })
}

pub fn update_task_needs(
    db: &Database,
    board_id: &str,
    task_id: &str,
    caller: &CallerContext,
    input: UpdateNeedsInput,
) -> BoardResult<TransitionResult> {
    db.with_tx(|tx| {
        let now = Utc::now();
        let now_str = format_utc_ms(now);
        let mut event_seqs: Vec<i64> = Vec::new();
        for (_, seq) in expire_due_leases_tx(tx, Some(board_id), now)? {
            event_seqs.push(seq);
        }

        let task = load_task_row_tx(tx, board_id, task_id)?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("task '{task_id}' not found"),
            )
        })?;
        let board_grants = grants::load_grants_tx(tx, board_id)?;
        require_owner_or_manage(caller, &board_grants, &task)?;

        if task.state == TaskState::Claimed {
            return Err(BoardError::new(
                ErrorCode::TaskClaimed,
                "cannot change needs while task is claimed",
            ));
        }
        if task.state.is_finished() {
            return Err(BoardError::new(
                ErrorCode::TaskFinished,
                format!("cannot change needs on finished task '{task_id}'"),
            ));
        }

        let valid_needs =
            validate_prereqs_readable_tx(tx, board_id, &board_grants, caller, &input.needs)?;
        check_no_cycle_tx(tx, board_id, task_id, &valid_needs)?;

        let stored = messages::load_stored_message_tx(tx, board_id, task_id)?.unwrap();
        let before_snap = messages::build_message_snapshot_tx(tx, &stored)?;

        tx.execute("DELETE FROM task_needs WHERE task_id = ?1", params![task_id])?;
        for nid in &valid_needs {
            tx.execute(
                "INSERT INTO task_needs (board_id, task_id, needs_task_id) VALUES (?1, ?2, ?3)",
                params![board_id, task_id, nid],
            )?;
        }

        // Re-evaluate waiting <-> ready (keeping blocked as blocked)
        let next_state = if task.state == TaskState::Blocked {
            TaskState::Blocked
        } else if all_needs_completed_tx(tx, task_id)? {
            TaskState::Ready
        } else {
            TaskState::Waiting
        };

        tx.execute(
            "UPDATE tasks SET state = ?1, updated_at = ?2 WHERE message_id = ?3",
            params![next_state.as_str(), now_str, task_id],
        )?;
        labels::set_system_label_tx(tx, board_id, task_id, "board.task", next_state.as_str())?;

        let after_snap = messages::build_message_snapshot_tx(tx, &stored)?;
        let view = messages::hydrate_message_view_tx(tx, &stored)?;
        let actor_id = caller.actor_id();

        let ev = events::append_event_tx(
            tx,
            EventAppend {
                board_id,
                event_type: EventType::TaskNeedsChanged,
                channel: Some(&task.channel),
                message_id: Some(task_id),
                actor_id: Some(&actor_id),
                payload: json!({ "needs": valid_needs, "state": next_state.as_str() }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&view),
            },
        )?;
        event_seqs.push(ev.seq);

        if task.state != TaskState::Ready && next_state == TaskState::Ready {
            let ev_r = events::append_event_tx(
                tx,
                EventAppend {
                    board_id,
                    event_type: EventType::TaskReady,
                    channel: Some(&task.channel),
                    message_id: Some(task_id),
                    actor_id: Some(&actor_id),
                    payload: json!({ "from": "needs_changed" }),
                    before_snapshot: Some(&before_snap),
                    after_snapshot: Some(&after_snap),
                    after_message: Some(&view),
                },
            )?;
            event_seqs.push(ev_r.seq);
        }

        Ok(TransitionResult {
            task: view,
            readied: Vec::new(),
            event_seqs,
        })
    })
}

/// Root-message deletion hook: cancels any unfinished task attached to `message_id`.
pub fn on_root_deleted_tx(
    conn: &Connection,
    board_id: &str,
    message_id: &str,
    actor_id: &str,
) -> BoardResult<Option<i64>> {
    let Some(task) = load_task_row_tx(conn, board_id, message_id)? else {
        return Ok(None);
    };
    if matches!(task.state, TaskState::Completed | TaskState::Canceled) {
        return Ok(None);
    }

    let stored = messages::load_stored_message_tx(conn, board_id, message_id)?.unwrap();
    let before_snap = messages::build_message_snapshot_tx(conn, &stored)?;
    let now_str = format_utc_ms(Utc::now());

    conn.execute(
        "UPDATE tasks
         SET state = 'canceled', cancel_reason = 'root message deleted',
             claimed_by = NULL, claim_token = NULL, claim_until = NULL, updated_at = ?1
         WHERE message_id = ?2",
        params![now_str, message_id],
    )?;
    labels::set_system_label_tx(
        conn,
        board_id,
        message_id,
        "board.task",
        TaskState::Canceled.as_str(),
    )?;

    let after_snap = messages::build_message_snapshot_tx(conn, &stored)?;
    let view = messages::hydrate_message_view_tx(conn, &stored)?;
    let ev = events::append_event_tx(
        conn,
        EventAppend {
            board_id,
            event_type: EventType::TaskCanceled,
            channel: Some(&task.channel),
            message_id: Some(message_id),
            actor_id: Some(actor_id),
            payload: json!({ "reason": "root message deleted" }),
            before_snapshot: Some(&before_snap),
            after_snapshot: Some(&after_snap),
            after_message: Some(&view),
        },
    )?;
    Ok(Some(ev.seq))
}

/// Root-message edit hook: if someone other than the task's exact owner actor edits an unfinished
/// task's root message (e.g., bare principal editing a profile's task, or original thread author
/// editing a thread converted into a task by another user), transitions the task to `blocked`.
pub fn on_root_edited_tx(
    conn: &Connection,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
) -> BoardResult<Option<i64>> {
    let Some(task) = load_task_row_tx(conn, board_id, message_id)? else {
        return Ok(None);
    };
    if task.state.is_finished() || task.state == TaskState::Blocked {
        return Ok(None);
    }
    let caller_actor = caller.actor_id();
    if caller_actor == task.owner_actor {
        return Ok(None);
    }

    let stored = messages::load_stored_message_tx(conn, board_id, message_id)?.unwrap();
    let before_snap = messages::build_message_snapshot_tx(conn, &stored)?;
    let now_str = format_utc_ms(Utc::now());

    conn.execute(
        "UPDATE tasks
         SET state = 'blocked', block_reason = 'root message edited by non-owner',
             block_kind = 'root_edited', claimed_by = NULL, claim_token = NULL,
             claim_until = NULL, updated_at = ?1
         WHERE message_id = ?2",
        params![now_str, message_id],
    )?;
    labels::set_system_label_tx(
        conn,
        board_id,
        message_id,
        "board.task",
        TaskState::Blocked.as_str(),
    )?;

    let after_snap = messages::build_message_snapshot_tx(conn, &stored)?;
    let view = messages::hydrate_message_view_tx(conn, &stored)?;
    let ev = events::append_event_tx(
        conn,
        EventAppend {
            board_id,
            event_type: EventType::TaskBlocked,
            channel: Some(&task.channel),
            message_id: Some(message_id),
            actor_id: Some(&caller_actor),
            payload: json!({
                "reason": "root message edited by non-owner",
                "kind": "root_edited"
            }),
            before_snapshot: Some(&before_snap),
            after_snapshot: Some(&after_snap),
            after_message: Some(&view),
        },
    )?;
    Ok(Some(ev.seq))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_task_states_and_caps_lease_at_5_minutes() {
        assert_eq!(TaskState::parse("ready").unwrap(), TaskState::Ready);
        assert!(TaskState::Waiting.is_open());
        assert!(TaskState::Claimed.is_open());
        assert!(TaskState::Completed.is_finished());
        assert!(TaskState::parse("unknown").is_err());

        assert_eq!(parse_lease_ms(None, None).unwrap(), 300_000);
        assert_eq!(parse_lease_ms(Some(60), None).unwrap(), 60_000);
        assert_eq!(parse_lease_ms(Some(999), None).unwrap(), 300_000);
        assert_eq!(parse_lease_ms(None, Some(500)).unwrap(), 500);
        assert!(parse_lease_ms(Some(0), None).is_err());
        assert!(parse_lease_ms(Some(-1), None).is_err());
    }

    #[test]
    fn resolves_for_profile_under_owner_principal() {
        assert_eq!(
            resolve_assigned_for("user:u1+lead", Some("Deployer")).unwrap(),
            Some("user:u1+deployer".to_string())
        );
        assert_eq!(
            resolve_assigned_for("user:u1", Some("user:u1+worker")).unwrap(),
            Some("user:u1+worker".to_string())
        );
        assert!(resolve_assigned_for("user:u1", Some("user:u2+worker")).is_err());
        assert!(resolve_assigned_for("user:u1", Some("1bad")).is_err());
    }
}
