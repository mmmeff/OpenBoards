use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::actors::CallerContext;
use crate::db::{sql_placeholders, Database, Queryable, SqlArg};
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::events::{self, EventAppend, EventType};
use crate::grants;
use crate::messages;
use crate::sql_args;
use crate::validate::{validate_label_key, validate_label_value, validate_writable_label_key};

#[derive(Debug, Default, Deserialize)]
pub struct UpdateLabelsInput {
    #[serde(default)]
    pub set: BTreeMap<String, String>,
    #[serde(default)]
    pub unset: Vec<String>,
    #[serde(default, rename = "if")]
    pub conditions: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelsChangedOutcome {
    pub set: BTreeMap<String, String>,
    pub unset: Vec<String>,
    pub labels: BTreeMap<String, String>,
    #[serde(skip_serializing)]
    pub event_seq: Option<i64>,
}

/// Validates a map of user-supplied labels (rejects `board.*` with `400 reserved_label`).
pub fn validate_user_labels(labels: &BTreeMap<String, String>) -> BoardResult<()> {
    for (k, v) in labels {
        validate_writable_label_key(k)?;
        validate_label_value(v)?;
    }
    Ok(())
}

pub async fn load_message_labels_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_id: &str,
) -> BoardResult<BTreeMap<String, String>> {
    let rows = conn
        .fetch_all(
            "SELECT key, value FROM message_labels WHERE message_id = ?1 ORDER BY key ASC",
            sql_args![message_id],
        )
        .await?;
    let mut map = BTreeMap::new();
    for r in rows {
        map.insert(r.get_str(0)?, r.get_str(1)?);
    }
    Ok(map)
}

pub async fn load_messages_labels_batch_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_ids: &[&str],
) -> BoardResult<HashMap<String, BTreeMap<String, String>>> {
    let mut out: HashMap<String, BTreeMap<String, String>> = HashMap::new();
    if message_ids.is_empty() {
        return Ok(out);
    }
    for chunk in message_ids.chunks(400) {
        let sql = format!(
            "SELECT message_id, key, value
             FROM message_labels
             WHERE message_id IN ({})
             ORDER BY message_id ASC, key ASC",
            sql_placeholders(chunk.len())
        );
        let args: Vec<SqlArg<'_>> = chunk.iter().copied().map(SqlArg::from).collect();
        let rows = conn.fetch_all(&sql, &args).await?;
        for r in rows {
            out.entry(r.get_str(0)?)
                .or_default()
                .insert(r.get_str(1)?, r.get_str(2)?);
        }
    }
    Ok(out)
}

pub async fn insert_labels_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    message_id: &str,
    labels: &BTreeMap<String, String>,
) -> BoardResult<()> {
    for (k, v) in labels {
        conn.execute(
            "INSERT INTO message_labels (board_id, message_id, key, value)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(message_id, key) DO UPDATE SET value = excluded.value",
            sql_args![board_id, message_id, k, v],
        )
        .await?;
    }
    Ok(())
}

/// System helper for setting reserved `board.*` labels (e.g., `board.task` in Wave 3).
pub async fn set_system_label_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    message_id: &str,
    key: &str,
    value: &str,
) -> BoardResult<()> {
    validate_label_key(key)?;
    validate_label_value(value)?;
    conn.execute(
        "INSERT INTO message_labels (board_id, message_id, key, value)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(message_id, key) DO UPDATE SET value = excluded.value",
        sql_args![board_id, message_id, key, value],
    )
    .await?;
    Ok(())
}

pub async fn delete_message_labels_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_id: &str,
) -> BoardResult<()> {
    conn.execute(
        "DELETE FROM message_labels WHERE message_id = ?1",
        sql_args![message_id],
    )
    .await?;
    Ok(())
}

/// Atomically updates labels on `message_id` with optional CAS `if` conditions.
///
/// - Rejects `board.*` keys with `400 reserved_label`.
/// - Requires `label` permission in the message's channel.
/// - Fails `400 message_deleted` on tombstoned messages.
/// - Fails `409 label_conflict` on CAS mismatch (naming ONLY the key, never the actual value).
pub async fn update_labels(
    db: &Database,
    board_id: &str,
    message_id: &str,
    caller: &CallerContext,
    input: UpdateLabelsInput,
) -> BoardResult<LabelsChangedOutcome> {
    validate_user_labels(&input.set)?;
    for k in &input.unset {
        validate_writable_label_key(k)?;
    }
    for (k, expected) in &input.conditions {
        validate_label_key(k)?;
        if let Some(v) = expected {
            validate_label_value(v)?;
        }
    }

    let mut tx = db.begin_tx().await?;
    let stored = messages::load_stored_message_for_update_tx(&mut tx, board_id, message_id)
        .await?
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;
    let channel = stored.channel.clone();

    let board_grants = grants::load_grants_tx(&mut tx, board_id).await?;
    let perms = grants::caller_permissions_on_channel(&board_grants, caller, &channel);
    if !perms.can_read() {
        return Err(BoardError::new(
            ErrorCode::MessageNotFound,
            format!("message '{message_id}' not found"),
        ));
    }
    if !perms.can_label() {
        return Err(BoardError::forbidden(format!(
            "labeling messages in '{channel}' requires 'label' permission"
        )));
    }
    if stored.deleted_at.is_some() {
        return Err(BoardError::new(
            ErrorCode::MessageDeleted,
            format!("cannot label deleted message '{message_id}'"),
        ));
    }

    let current = load_message_labels_tx(&mut tx, message_id).await?;

    // Atomic CAS check — NEVER include actual label values in the error message.
    for (key, expected) in &input.conditions {
        let actual = current.get(key).map(String::as_str);
        if actual != expected.as_deref() {
            return Err(BoardError::new(
                ErrorCode::LabelConflict,
                format!("label condition failed for key '{key}'"),
            ));
        }
    }

    let before_snap = messages::build_message_snapshot_tx(&mut tx, &stored).await?;

    let mut actually_unset = Vec::new();
    for key in &input.unset {
        if !input.set.contains_key(key) {
            let n = tx
                .execute(
                    "DELETE FROM message_labels WHERE message_id = ?1 AND key = ?2",
                    sql_args![message_id, key],
                )
                .await?;
            if n > 0 {
                actually_unset.push(key.clone());
            }
        }
    }

    if !input.set.is_empty() {
        insert_labels_tx(&mut tx, board_id, message_id, &input.set).await?;
    }

    let labels = load_message_labels_tx(&mut tx, message_id).await?;
    let event_seq = if !input.set.is_empty() || !actually_unset.is_empty() {
        let after_snap = messages::build_message_snapshot_tx(&mut tx, &stored).await?;
        let after_msg = messages::hydrate_message_view_tx(&mut tx, &stored).await?;
        let actor_id = caller.actor_id();
        let ev = events::append_event_tx(
            &mut tx,
            EventAppend {
                board_id,
                event_type: EventType::LabelsChanged,
                channel: Some(&channel),
                message_id: Some(message_id),
                actor_id: Some(&actor_id),
                payload: serde_json::json!({
                    "set": input.set,
                    "unset": actually_unset
                }),
                before_snapshot: Some(&before_snap),
                after_snapshot: Some(&after_snap),
                after_message: Some(&after_msg),
            },
        )
        .await?;
        Some(ev.seq)
    } else {
        None
    };

    tx.commit().await?;

    Ok(LabelsChangedOutcome {
        set: input.set,
        unset: actually_unset,
        labels,
        event_seq,
    })
}
