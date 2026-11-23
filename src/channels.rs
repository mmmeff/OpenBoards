use std::collections::BTreeMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::actors::{self, CallerContext};
use crate::db::{escape_like_prefix, Database, Queryable};
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::events::{self, EventAppend, EventType};
use crate::grants::{self, Grant, GrantPrincipal};
use crate::permissions::Permissions;
use crate::sql_args;
use crate::validate::validate_channel_path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelView {
    pub path: String,
    pub created_by: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateChannelInput {
    pub path: String,
    #[serde(default)]
    pub private: bool,
}

#[derive(Debug, Serialize)]
pub struct CreateChannelResult {
    pub channel: ChannelView,
    pub created: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grants: Option<Vec<Grant>>,
    #[serde(skip_serializing)]
    pub event_seq: Option<i64>,
}

/// Lists channels on `board_id` that `caller` has `read` permission for.
pub async fn list_channels(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
) -> BoardResult<Vec<ChannelView>> {
    let mut conn = db.read_conn();
    let board_grants = grants::load_grants_tx(&mut conn, board_id).await?;
    let rows = conn
        .fetch_all(
            "SELECT path, created_by, created_at FROM channels WHERE board_id = ?1 ORDER BY path ASC",
            sql_args![board_id],
        )
        .await?;
    let mut visible = Vec::new();
    for r in rows {
        let ch = ChannelView {
            path: r.get_str(0)?,
            created_by: r.get_str(1)?,
            created_at: r.get_str(2)?,
        };
        if grants::caller_permissions_on_channel(&board_grants, caller, &ch.path).can_read() {
            visible.push(ch);
        }
    }
    Ok(visible)
}

/// Creates a channel explicitly.
///
/// - Regular channel (`private: false`): requires `read` and `post` in `path`; idempotent (`created: false` if existing).
/// - Private channel (`private: true`): requires un-narrowed, un-sandboxed `manage` at `*`; atomically checks
///   `409 channel_exists` (exact or descendant channel) and `409 channel_granted` (existing overlapping non-`*` grant),
///   then creates self-grant + `everyone=none` grant on `<path>/` + channel.
pub async fn create_channel(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: CreateChannelInput,
) -> BoardResult<CreateChannelResult> {
    validate_channel_path(&input.path)?;

    if !input.private {
        let mut tx = db.begin_tx().await?;
        let board_grants = grants::load_grants_tx(&mut tx, board_id).await?;
        let perms = grants::caller_permissions_on_channel(&board_grants, caller, &input.path);
        if !perms.can_create_channel() {
            return Err(BoardError::forbidden(format!(
                "creating channel '{}' requires 'read' and 'post' permissions",
                input.path
            )));
        }

        let now = Utc::now().to_rfc3339();
        let actor_id = caller.actor_id();
        let inserted = tx
            .execute(
                "INSERT INTO channels (board_id, path, created_by, created_at)
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT (board_id, path) DO NOTHING",
                sql_args![board_id, &input.path, &actor_id, &now],
            )
            .await?;

        if inserted == 0 {
            let r = tx
                .fetch_one(
                    "SELECT path, created_by, created_at FROM channels WHERE board_id = ?1 AND path = ?2",
                    sql_args![board_id, &input.path],
                )
                .await?;
            let existing = ChannelView {
                path: r.get_str(0)?,
                created_by: r.get_str(1)?,
                created_at: r.get_str(2)?,
            };
            tx.commit().await?;
            return Ok(CreateChannelResult {
                channel: existing,
                created: false,
                grants: None,
                event_seq: None,
            });
        }

        actors::record_actor_write_tx(&mut tx, board_id, &input.path, caller).await?;
        let ev = events::append_event_tx(
            &mut tx,
            EventAppend {
                board_id,
                event_type: EventType::ChannelCreated,
                channel: Some(&input.path),
                message_id: None,
                actor_id: Some(&actor_id),
                payload: serde_json::json!({ "channel": input.path, "private": false }),
                before_snapshot: None,
                after_snapshot: None,
                after_message: None,
            },
        )
        .await?;
        tx.commit().await?;

        return Ok(CreateChannelResult {
            channel: ChannelView {
                path: input.path,
                created_by: actor_id,
                created_at: now,
            },
            created: true,
            grants: None,
            event_seq: Some(ev.seq),
        });
    }

    // Private channel path
    if !caller.can_mutate_grants() {
        return Err(BoardError::forbidden(
            "narrowed or sandboxed callers cannot create private channels",
        ));
    }

    let mut tx = db.begin_tx().await?;
    // Lock board row on Postgres to serialize private-channel creation against concurrent grant mutations.
    let lock_sql = format!("SELECT id FROM boards WHERE id = ?1{}", tx.lock_row_suffix());
    let _ = tx.fetch_one(&lock_sql, sql_args![board_id]).await?;

    let board_grants = grants::load_grants_tx(&mut tx, board_id).await?;

    if !caller.is_unrestricted_owner()
        && !grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_manage()
    {
        return Err(BoardError::forbidden(
            "creating a private channel requires 'manage' permission at '*'",
        ));
    }

    let ceiling = caller.principal.permission_ceiling();
    if !ceiling.can_create_channel() {
        return Err(BoardError::forbidden(
            "caller's role or scopes do not permit creating a channel",
        ));
    }

    // Fail 409 channel_exists if channel or any descendant under `<path>/` already exists
    let prefix_like = format!("{}/%", escape_like_prefix(&input.path));
    let exists = tx
        .fetch_optional(
            "SELECT 1 FROM channels WHERE board_id = ?1 AND (path = ?2 OR path LIKE ?3 ESCAPE '\\') LIMIT 1",
            sql_args![board_id, &input.path, &prefix_like],
        )
        .await?
        .is_some();
    if exists {
        return Err(BoardError::new(
            ErrorCode::ChannelExists,
            format!(
                "channel '{}' or a channel under '{}/' already exists",
                input.path, input.path
            ),
        ));
    }

    // Fail 409 channel_granted if another non-`*` grant already reaches this path or subtree
    if grants::has_overlapping_grant_for_private_channel(&board_grants, &input.path) {
        return Err(BoardError::new(
            ErrorCode::ChannelGranted,
            format!(
                "another grant already reaches '{}' or its subtree",
                input.path
            ),
        ));
    }

    let subtree_pattern = format!("{}/", input.path);
    let actor_id = caller.actor_id();

    let self_perms = if caller.is_unrestricted_owner() {
        Permissions::ALL
    } else {
        grants::caller_permissions_at_pattern(&board_grants, caller, "*")
    };
    let mut self_can = BTreeMap::new();
    self_can.insert(subtree_pattern.clone(), self_perms);
    let self_grant = grants::insert_grant_tx(
        &mut tx,
        board_id,
        vec![GrantPrincipal::from_caller_principal(&caller.principal)],
        self_can,
        Some(&actor_id),
    )
    .await?;

    let mut deny_can = BTreeMap::new();
    deny_can.insert(subtree_pattern, Permissions::NONE);
    let deny_grant = grants::insert_grant_tx(
        &mut tx,
        board_id,
        vec![GrantPrincipal::Everyone],
        deny_can,
        Some(&actor_id),
    )
    .await?;

    let now = Utc::now().to_rfc3339();
    tx.execute(
        "INSERT INTO channels (board_id, path, created_by, created_at) VALUES (?1, ?2, ?3, ?4)",
        sql_args![board_id, &input.path, &actor_id, &now],
    )
    .await?;
    actors::record_actor_write_tx(&mut tx, board_id, &input.path, caller).await?;
    let ev = events::append_event_tx(
        &mut tx,
        EventAppend {
            board_id,
            event_type: EventType::ChannelCreated,
            channel: Some(&input.path),
            message_id: None,
            actor_id: Some(&actor_id),
            payload: serde_json::json!({ "channel": input.path, "private": true }),
            before_snapshot: None,
            after_snapshot: None,
            after_message: None,
        },
    )
    .await?;
    tx.commit().await?;

    Ok(CreateChannelResult {
        channel: ChannelView {
            path: input.path,
            created_by: actor_id,
            created_at: now,
        },
        created: true,
        grants: Some(vec![self_grant, deny_grant]),
        event_seq: Some(ev.seq),
    })
}
