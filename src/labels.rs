use std::collections::BTreeMap;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::actors::CallerContext;
use crate::db::Database;
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::grants;
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
}

/// Validates a map of user-supplied labels (rejects `board.*` with `400 reserved_label`).
pub fn validate_user_labels(labels: &BTreeMap<String, String>) -> BoardResult<()> {
    for (k, v) in labels {
        validate_writable_label_key(k)?;
        validate_label_value(v)?;
    }
    Ok(())
}

pub fn load_message_labels_tx(
    conn: &Connection,
    message_id: &str,
) -> BoardResult<BTreeMap<String, String>> {
    let mut stmt = conn.prepare(
        "SELECT key, value FROM message_labels WHERE message_id = ?1 ORDER BY key ASC",
    )?;
    let rows = stmt.query_map(params![message_id], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })?;
    let mut map = BTreeMap::new();
    for row in rows {
        let (k, v) = row?;
        map.insert(k, v);
    }
    Ok(map)
}

pub fn insert_labels_tx(
    conn: &Connection,
    board_id: &str,
    message_id: &str,
    labels: &BTreeMap<String, String>,
) -> BoardResult<()> {
    for (k, v) in labels {
        conn.execute(
            "INSERT INTO message_labels (board_id, message_id, key, value)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(message_id, key) DO UPDATE SET value = excluded.value",
            params![board_id, message_id, k, v],
        )?;
    }
    Ok(())
}

/// System helper for setting reserved `board.*` labels (e.g., `board.task` in Wave 3).
pub fn set_system_label_tx(
    conn: &Connection,
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
        params![board_id, message_id, key, value],
    )?;
    Ok(())
}

pub fn delete_message_labels_tx(conn: &Connection, message_id: &str) -> BoardResult<()> {
    conn.execute(
        "DELETE FROM message_labels WHERE message_id = ?1",
        params![message_id],
    )?;
    Ok(())
}

/// Atomically updates labels on `message_id` with optional CAS `if` conditions.
///
/// - Rejects `board.*` keys with `400 reserved_label`.
/// - Requires `label` permission in the message's channel.
/// - Fails `400 message_deleted` on tombstoned messages.
/// - Fails `409 label_conflict` on CAS mismatch (naming ONLY the key, never the actual value).
pub fn update_labels(
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

    db.with_tx(|tx| {
        let msg_row: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT channel, deleted_at FROM messages WHERE board_id = ?1 AND id = ?2",
                params![board_id, message_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;

        let (channel, deleted_at) = msg_row.ok_or_else(|| {
            BoardError::new(
                ErrorCode::MessageNotFound,
                format!("message '{message_id}' not found"),
            )
        })?;

        let board_grants = grants::load_grants_tx(tx, board_id)?;
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
        if deleted_at.is_some() {
            return Err(BoardError::new(
                ErrorCode::MessageDeleted,
                format!("cannot label deleted message '{message_id}'"),
            ));
        }

        let current = load_message_labels_tx(tx, message_id)?;

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

        let mut actually_unset = Vec::new();
        for key in &input.unset {
            if !input.set.contains_key(key) {
                let n = tx.execute(
                    "DELETE FROM message_labels WHERE message_id = ?1 AND key = ?2",
                    params![message_id, key],
                )?;
                if n > 0 {
                    actually_unset.push(key.clone());
                }
            }
        }

        if !input.set.is_empty() {
            insert_labels_tx(tx, board_id, message_id, &input.set)?;
        }

        let labels = load_message_labels_tx(tx, message_id)?;
        Ok(LabelsChangedOutcome {
            set: input.set,
            unset: actually_unset,
            labels,
        })
    })
}
