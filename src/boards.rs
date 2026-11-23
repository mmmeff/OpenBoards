use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::actors::CallerContext;
use crate::db::{Database, DbTx, Queryable};
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::grants;
use crate::id::new_board_id;
use crate::sql_args;
use crate::validate::validate_board_name;

pub const DEFAULT_BOARD_NAME: &str = "General";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardView {
    pub id: String,
    pub name: String,
    pub created_at: String,
}

/// Ensures the default `General` board exists with its seeded default grants (`g1`/`g2`/`g3`).
pub async fn ensure_default_board(db: &Database) -> BoardResult<BoardView> {
    let mut tx = db.begin_tx().await?;
    let board = ensure_default_board_tx(&mut tx).await?;
    tx.commit().await?;
    Ok(board)
}

pub async fn ensure_default_board_tx(tx: &mut DbTx) -> BoardResult<BoardView> {
    let lower = DEFAULT_BOARD_NAME.to_ascii_lowercase();
    tx.advisory_xact_lock("board", &lower).await?;
    if let Some(board) = find_board_tx(tx, DEFAULT_BOARD_NAME).await? {
        return Ok(board);
    }
    create_board_tx(tx, DEFAULT_BOARD_NAME).await
}

pub async fn find_board_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    name_or_id: &str,
) -> BoardResult<Option<BoardView>> {
    let lower = name_or_id.to_ascii_lowercase();
    let row = conn
        .fetch_optional(
            "SELECT id, name, created_at FROM boards WHERE id = ?1 OR name_lower = ?2 LIMIT 1",
            sql_args![name_or_id, &lower],
        )
        .await?;
    match row {
        None => Ok(None),
        Some(r) => Ok(Some(BoardView {
            id: r.get_str(0)?,
            name: r.get_str(1)?,
            created_at: r.get_str(2)?,
        })),
    }
}

async fn create_board_tx(tx: &mut DbTx, name: &str) -> BoardResult<BoardView> {
    validate_board_name(name)?;
    let id = new_board_id();
    let name_lower = name.to_ascii_lowercase();
    let now = Utc::now().to_rfc3339();
    tx.execute(
        "INSERT INTO boards (id, name, name_lower, next_grant_seq, created_at)
         VALUES (?1, ?2, ?3, 1, ?4)",
        sql_args![&id, name, &name_lower, &now],
    )
    .await?;
    grants::seed_default_grants_tx(tx, &id).await?;
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
pub async fn resolve_board(
    db: &Database,
    name_or_id: &str,
    ensure: Option<bool>,
    caller: &CallerContext,
) -> BoardResult<BoardView> {
    // Fast read-only path avoids starting a SQLite write transaction when the board already exists.
    {
        let mut conn = db.read_conn();
        if let Some(existing) = find_board_tx(&mut conn, name_or_id).await? {
            let board_grants = grants::load_grants_tx(&mut conn, &existing.id).await?;
            if !grants::caller_has_any_board_access(&board_grants, caller) {
                return Err(BoardError::new(
                    ErrorCode::BoardNotGranted,
                    format!("caller has no access to board '{}'", existing.name),
                ));
            }
            return Ok(existing);
        }
    }

    let board = {
        let mut tx = db.begin_tx().await?;
        let lower = name_or_id.to_ascii_lowercase();
        tx.advisory_xact_lock("board", &lower).await?;

        let resolved = if let Some(existing) = find_board_tx(&mut tx, name_or_id).await? {
            existing
        } else if name_or_id.eq_ignore_ascii_case(DEFAULT_BOARD_NAME) {
            create_board_tx(&mut tx, DEFAULT_BOARD_NAME).await?
        } else {
            match ensure {
                Some(true) => {
                    validate_board_name(name_or_id)?;
                    if caller.is_restricted() || !caller.principal.can_create_boards() {
                        return Err(BoardError::forbidden(
                            "caller is not permitted to create boards",
                        ));
                    }
                    create_board_tx(&mut tx, name_or_id).await?
                }
                Some(false) => {
                    return Err(BoardError::new(
                        ErrorCode::NoBoard,
                        format!("board '{name_or_id}' does not exist"),
                    )
                    .with_hint("Set ensure=true to create the board on first use."));
                }
                None => {
                    return Err(BoardError::new(
                        ErrorCode::BoardNotFound,
                        format!("board '{name_or_id}' not found"),
                    )
                    .with_hint("Pass ?ensure=true to create the board."));
                }
            }
        };
        tx.commit().await?;
        resolved
    };

    // Check caller has at least some access to this board (unless unrestricted owner).
    let board_grants = grants::load_grants(db, &board.id).await?;
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

pub async fn create_board(
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

    let mut tx = db.begin_tx().await?;
    let name_lower = input.name.to_ascii_lowercase();
    tx.advisory_xact_lock("board", &name_lower).await?;

    // Ensure General is seeded before checking name collisions with "General"
    let _ = ensure_default_board_tx(&mut tx).await?;

    if let Some(existing) = find_board_tx(&mut tx, &input.name).await? {
        if input.ensure {
            let board_grants = grants::load_grants_tx(&mut tx, &existing.id).await?;
            if !grants::caller_has_any_board_access(&board_grants, caller) {
                return Err(BoardError::new(
                    ErrorCode::BoardNotGranted,
                    format!("caller has no access to board '{}'", existing.name),
                ));
            }
            tx.commit().await?;
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

    let created = create_board_tx(&mut tx, &input.name).await?;
    tx.commit().await?;
    Ok((created, true))
}

pub async fn list_boards(db: &Database, caller: &CallerContext) -> BoardResult<Vec<BoardView>> {
    let mut tx = db.begin_tx().await?;
    let _ = ensure_default_board_tx(&mut tx).await?;
    let rows = tx
        .fetch_all(
            "SELECT id, name, created_at FROM boards ORDER BY created_at ASC",
            sql_args![],
        )
        .await?;
    let mut visible = Vec::new();
    for r in rows {
        let b = BoardView {
            id: r.get_str(0)?,
            name: r.get_str(1)?,
            created_at: r.get_str(2)?,
        };
        let g = grants::load_grants_tx(&mut tx, &b.id).await?;
        if grants::caller_has_any_board_access(&g, caller) {
            visible.push(b);
        }
    }
    tx.commit().await?;
    Ok(visible)
}
