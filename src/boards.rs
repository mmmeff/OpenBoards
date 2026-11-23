use chrono::Utc;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::actors::CallerContext;
use crate::db::Database;
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::grants;
use crate::id::new_board_id;
use crate::validate::validate_board_name;

pub const DEFAULT_BOARD_NAME: &str = "General";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardView {
    pub id: String,
    pub name: String,
    pub created_at: String,
}

/// Ensures the default `General` board exists with its seeded default grants (`g1`/`g2`/`g3`).
pub fn ensure_default_board(db: &Database) -> BoardResult<BoardView> {
    db.with_tx(|tx| ensure_default_board_tx(tx))
}

pub fn ensure_default_board_tx(conn: &Connection) -> BoardResult<BoardView> {
    if let Some(board) = find_board_tx(conn, DEFAULT_BOARD_NAME)? {
        return Ok(board);
    }
    create_board_tx(conn, DEFAULT_BOARD_NAME)
}

pub fn find_board_tx(conn: &Connection, name_or_id: &str) -> BoardResult<Option<BoardView>> {
    let lower = name_or_id.to_ascii_lowercase();
    let mut stmt = conn.prepare_cached(
        "SELECT id, name, created_at FROM boards WHERE id = ?1 OR name_lower = ?2 LIMIT 1",
    )?;
    stmt.query_row(params![name_or_id, lower], |r| {
        Ok(BoardView {
            id: r.get(0)?,
            name: r.get(1)?,
            created_at: r.get(2)?,
        })
    })
    .optional()
    .map_err(Into::into)
}

fn create_board_tx(conn: &Connection, name: &str) -> BoardResult<BoardView> {
    validate_board_name(name)?;
    let id = new_board_id();
    let name_lower = name.to_ascii_lowercase();
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO boards (id, name, name_lower, next_grant_seq, created_at)
         VALUES (?1, ?2, ?3, 1, ?4)",
        params![id, name, name_lower, now],
    )?;
    grants::seed_default_grants_tx(conn, &id)?;
    Ok(BoardView {
        id,
        name: name.to_string(),
        created_at: now,
    })
}

/// Resolves a board from path parameter `{board}` with optional `ensure` behavior:
/// - Default board `General` (case-insensitive) always exists (auto-seeded if absent).
/// - `ensure == Some(true)`: creates the board if missing (with `g1`/`g2`/`g3`).
/// - `ensure == Some(false)`: returns `404 no_board` if missing.
/// - `ensure == None`: returns `404 board_not_found` if missing.
pub fn resolve_board(
    db: &Database,
    name_or_id: &str,
    ensure: Option<bool>,
    caller: &CallerContext,
) -> BoardResult<BoardView> {
    // Fast read-only path avoids starting a SQLite write transaction when the board already exists.
    if let Some(board) = db.with_conn(|conn| {
        let Some(existing) = find_board_tx(conn, name_or_id)? else {
            return Ok(None);
        };
        let board_grants = grants::load_grants_tx(conn, &existing.id)?;
        if !grants::caller_has_any_board_access(&board_grants, caller) {
            return Err(BoardError::new(
                ErrorCode::BoardNotGranted,
                format!("caller has no access to board '{}'", existing.name),
            ));
        }
        Ok(Some(existing))
    })? {
        return Ok(board);
    }

    let board = db.with_tx(|tx| {
        if let Some(existing) = find_board_tx(tx, name_or_id)? {
            return Ok(existing);
        }

        if name_or_id.eq_ignore_ascii_case(DEFAULT_BOARD_NAME) {
            return create_board_tx(tx, DEFAULT_BOARD_NAME);
        }

        match ensure {
            Some(true) => {
                validate_board_name(name_or_id)?;
                if caller.is_restricted() || !caller.principal.can_create_boards() {
                    return Err(BoardError::forbidden(
                        "caller is not permitted to create boards",
                    ));
                }
                create_board_tx(tx, name_or_id)
            }
            Some(false) => Err(BoardError::new(
                ErrorCode::NoBoard,
                format!("board '{name_or_id}' does not exist"),
            )
            .with_hint("Set ensure=true to create the board on first use.")),
            None => Err(BoardError::new(
                ErrorCode::BoardNotFound,
                format!("board '{name_or_id}' not found"),
            )
            .with_hint("Pass ?ensure=true to create the board.")),
        }
    })?;

    // Check caller has at least some access to this board (unless unrestricted owner).
    let board_grants = grants::load_grants(db, &board.id)?;
    if !grants::caller_has_any_board_access(&board_grants, caller) {
        return Err(BoardError::new(
            ErrorCode::BoardNotGranted,
            format!("caller has no access to board '{}'", board.name),
        ));
    }

    Ok(board)
}

#[derive(Debug, Deserialize)]
pub struct CreateBoardInput {
    pub name: String,
    #[serde(default)]
    pub ensure: bool,
}

pub fn create_board(
    db: &Database,
    caller: &CallerContext,
    input: CreateBoardInput,
) -> BoardResult<(BoardView, bool)> {
    validate_board_name(&input.name)?;

    if !input.ensure && (caller.is_restricted() || !caller.principal.can_create_boards()) {
        return Err(BoardError::forbidden(
            "caller is not permitted to create boards",
        ));
    }

    db.with_tx(|tx| {
        // Ensure General is seeded before checking name collisions with "General"
        let _ = ensure_default_board_tx(tx)?;

        if let Some(existing) = find_board_tx(tx, &input.name)? {
            if input.ensure {
                let board_grants = grants::load_grants_tx(tx, &existing.id)?;
                if !grants::caller_has_any_board_access(&board_grants, caller) {
                    return Err(BoardError::new(
                        ErrorCode::BoardNotGranted,
                        format!("caller has no access to board '{}'", existing.name),
                    ));
                }
                return Ok((existing, false));
            }
            return Err(BoardError::new(
                ErrorCode::BoardNameTaken,
                format!("board name '{}' is already taken", input.name),
            ));
        }

        if caller.is_restricted() || !caller.principal.can_create_boards() {
            return Err(BoardError::forbidden(
                "caller is not permitted to create boards",
            ));
        }

        let created = create_board_tx(tx, &input.name)?;
        Ok((created, true))
    })
}

pub fn list_boards(db: &Database, caller: &CallerContext) -> BoardResult<Vec<BoardView>> {
    db.with_tx(|tx| {
        let _ = ensure_default_board_tx(tx)?;
        let mut stmt =
            tx.prepare("SELECT id, name, created_at FROM boards ORDER BY created_at ASC")?;
        let rows = stmt.query_map([], |r| {
            Ok(BoardView {
                id: r.get(0)?,
                name: r.get(1)?,
                created_at: r.get(2)?,
            })
        })?;
        let mut visible = Vec::new();
        for row in rows {
            let b = row?;
            let g = grants::load_grants_tx(tx, &b.id)?;
            if grants::caller_has_any_board_access(&g, caller) {
                visible.push(b);
            }
        }
        Ok(visible)
    })
}
