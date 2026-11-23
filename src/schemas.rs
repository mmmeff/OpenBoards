use std::collections::HashMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::actors::CallerContext;
use crate::db::{sql_placeholders, Database, Queryable, SqlArg};
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::grants;
use crate::messages::ParsedBlock;
use crate::sql_args;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaView {
    pub id: String,
    pub name: String,
    pub version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub schema: Value,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateSchemaInput {
    pub name: String,
    pub version: i64,
    pub description: Option<String>,
    pub schema: Value,
}

pub fn validate_schema_name(name: &str) -> BoardResult<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return Err(BoardError::bad_request(
            "schema name must be 1 to 64 characters",
        ));
    }
    if !bytes[0].is_ascii_lowercase() {
        return Err(BoardError::bad_request(
            "schema name must start with a lowercase ASCII letter",
        ));
    }
    if !bytes[1..]
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'_')
    {
        return Err(BoardError::bad_request(
            "schema name must match [a-z][a-z0-9_-]*",
        ));
    }
    Ok(())
}

pub fn parse_schema_id(id: &str) -> BoardResult<(String, i64)> {
    let (name, ver_str) = id.split_once('@').ok_or_else(|| {
        BoardError::bad_request(format!(
            "invalid schema ID '{id}'; expected '<name>@<version>'"
        ))
    })?;
    validate_schema_name(name)?;
    let version: i64 = ver_str
        .parse()
        .map_err(|_| BoardError::bad_request(format!("invalid schema version in '{id}'")))?;
    if version < 1 {
        return Err(BoardError::bad_request("schema version must be >= 1"));
    }
    Ok((name.to_string(), version))
}

/// Seeds built-in team-wide schemas `git-commit@1` and `vercel-deployment@1`.
pub async fn seed_builtin_schemas(db: &Database) -> BoardResult<()> {
    let mut tx = db.begin_tx().await?;
    seed_builtin_schemas_tx(&mut tx).await?;
    tx.commit().await
}

pub async fn seed_builtin_schemas_tx<C: Queryable + ?Sized>(conn: &mut C) -> BoardResult<()> {
    let builtins = [
        (
            "git-commit",
            1i64,
            "Structured Git commit reference",
            json!({
                "type": "object",
                "required": ["sha", "message"],
                "properties": {
                    "sha": { "type": "string", "minLength": 4 },
                    "message": { "type": "string" },
                    "repo": { "type": "string" },
                    "branch": { "type": "string" },
                    "author": { "type": "string" },
                    "url": { "type": "string" },
                    "files": { "type": "array", "items": { "type": "string" } }
                },
                "additionalProperties": true
            }),
        ),
        (
            "vercel-deployment",
            1i64,
            "Structured deployment status record",
            json!({
                "type": "object",
                "required": ["id", "url", "state"],
                "properties": {
                    "id": { "type": "string", "minLength": 1 },
                    "url": { "type": "string", "minLength": 1 },
                    "state": {
                        "type": "string",
                        "enum": ["queued", "building", "ready", "error", "canceled"]
                    },
                    "project": { "type": "string" },
                    "environment": { "type": "string" },
                    "commit": { "type": "string" },
                    "createdAt": { "type": "string" }
                },
                "additionalProperties": true
            }),
        ),
    ];

    let now = Utc::now().to_rfc3339();
    for (name, version, desc, schema_val) in builtins {
        let id = format!("{name}@{version}");
        let schema_json = serde_json::to_string(&schema_val)
            .map_err(|e| BoardError::internal(format!("failed to serialize builtin schema: {e}")))?;
        conn.execute(
            "INSERT INTO schemas (id, name, version, description, schema_json, created_by, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6)
             ON CONFLICT (id) DO NOTHING",
            sql_args![&id, name, version, desc, &schema_json, &now],
        )
        .await?;
    }
    Ok(())
}

pub async fn get_schema_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    id: &str,
) -> BoardResult<Option<SchemaView>> {
    seed_builtin_schemas_tx(conn).await?;
    let row = conn
        .fetch_optional(
            "SELECT id, name, version, description, schema_json, created_at
             FROM schemas WHERE id = ?1",
            sql_args![id],
        )
        .await?;
    match row {
        None => Ok(None),
        Some(r) => {
            let schema_json = r.get_str(4)?;
            let schema: Value = serde_json::from_str(&schema_json)
                .map_err(|e| BoardError::internal(format!("corrupted schema JSON: {e}")))?;
            Ok(Some(SchemaView {
                id: r.get_str(0)?,
                name: r.get_str(1)?,
                version: r.get_i64(2)?,
                description: r.get_opt_str(3)?,
                schema,
                created_at: r.get_str(5)?,
            }))
        }
    }
}

pub async fn list_schemas(db: &Database) -> BoardResult<Vec<SchemaView>> {
    let mut tx = db.begin_tx().await?;
    seed_builtin_schemas_tx(&mut tx).await?;
    let rows = tx
        .fetch_all(
            "SELECT id, name, version, description, schema_json, created_at
             FROM schemas ORDER BY name ASC, version ASC",
            sql_args![],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let schema_json = r.get_str(4)?;
        let schema: Value = serde_json::from_str(&schema_json)
            .map_err(|e| BoardError::internal(format!("corrupted schema JSON: {e}")))?;
        out.push(SchemaView {
            id: r.get_str(0)?,
            name: r.get_str(1)?,
            version: r.get_i64(2)?,
            description: r.get_opt_str(3)?,
            schema,
            created_at: r.get_str(5)?,
        });
    }
    tx.commit().await?;
    Ok(out)
}

pub async fn get_schema(db: &Database, id: &str) -> BoardResult<SchemaView> {
    let mut tx = db.begin_tx().await?;
    let view = get_schema_tx(&mut tx, id).await?.ok_or_else(|| {
        BoardError::new(ErrorCode::SchemaNotFound, format!("schema '{id}' not found"))
            .with_status(axum::http::StatusCode::NOT_FOUND)
    })?;
    tx.commit().await?;
    Ok(view)
}

/// Registers an immutable schema version `name@version`.
///
/// Requires `manage` at `*` on `board_id` (if board-scoped) or unrestricted `owner`.
/// Re-registering an identical definition returns `created: false`; differing definition
/// fails with `409 schema_immutable`.
pub async fn create_schema(
    db: &Database,
    board_id: Option<&str>,
    caller: &CallerContext,
    input: CreateSchemaInput,
) -> BoardResult<(SchemaView, bool)> {
    validate_schema_name(&input.name)?;
    if input.version < 1 {
        return Err(BoardError::bad_request("schema version must be >= 1"));
    }
    if !input.schema.is_object() {
        return Err(BoardError::bad_request(
            "schema definition must be a JSON Schema object",
        ));
    }
    jsonschema::validator_for(&input.schema)
        .map_err(|e| BoardError::bad_request(format!("invalid JSON Schema definition: {e}")))?;

    if caller.is_restricted() {
        return Err(BoardError::forbidden(
            "narrowed or sandboxed callers cannot register schemas",
        ));
    }

    let id = format!("{}@{}", input.name, input.version);
    let mut tx = db.begin_tx().await?;
    tx.advisory_xact_lock("schema", &id).await?;
    seed_builtin_schemas_tx(&mut tx).await?;

    if !caller.is_unrestricted_owner() {
        let Some(bid) = board_id else {
            return Err(BoardError::forbidden(
                "registering schemas without a board requires an unrestricted owner or board-level 'manage' at '*'",
            ));
        };
        let board_grants = grants::load_grants_tx(&mut tx, bid).await?;
        if !grants::caller_permissions_at_pattern(&board_grants, caller, "*").can_manage() {
            return Err(BoardError::forbidden(
                "registering schemas requires 'manage' permission at '*'",
            ));
        }
    }

    if let Some(existing) = get_schema_tx(&mut tx, &id).await? {
        if existing.schema == input.schema && existing.description == input.description {
            tx.commit().await?;
            return Ok((existing, false));
        }
        return Err(BoardError::new(
            ErrorCode::SchemaImmutable,
            format!("schema '{id}' already exists with a different definition"),
        )
        .with_hint(format!(
            "Register '{}@{}' for a new version.",
            input.name,
            input.version + 1
        )));
    }

    let now = Utc::now().to_rfc3339();
    let schema_json = serde_json::to_string(&input.schema)
        .map_err(|e| BoardError::internal(format!("failed to serialize schema: {e}")))?;
    let actor_id = caller.actor_id();

    tx.execute(
        "INSERT INTO schemas (id, name, version, description, schema_json, created_by, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        sql_args![
            &id,
            &input.name,
            input.version,
            &input.description,
            &schema_json,
            &actor_id,
            &now
        ],
    )
    .await?;
    tx.commit().await?;

    Ok((
        SchemaView {
            id,
            name: input.name,
            version: input.version,
            description: input.description,
            schema: input.schema,
            created_at: now,
        },
        true,
    ))
}

/// Extracts and validates structured data blocks from message `text` (and optional explicit `schema`).
///
/// Supported embedded Markdown fence headers:
/// - ```` ```board-block <name>@<version> ````
/// - ```` ```board:<name>@<version> ````
///
/// If `explicit_schema` is provided (e.g. CLI `--schema sandbox@1`), the entire `text` is
/// parsed as JSON and validated against that schema (returning one block).
pub async fn parse_and_validate_blocks_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    text: &str,
    explicit_schema: Option<&str>,
) -> BoardResult<Vec<ParsedBlock>> {
    let raw_blocks: Vec<(String, String)> = if let Some(schema_id) = explicit_schema {
        vec![(schema_id.trim().to_string(), text.trim().to_string())]
    } else {
        extract_fenced_blocks(text)
    };

    if raw_blocks.is_empty() {
        return Ok(Vec::new());
    }

    let mut parsed = Vec::with_capacity(raw_blocks.len());
    for (schema_id, body) in raw_blocks {
        let (schema_name, schema_version) = parse_schema_id(&schema_id).map_err(|_| {
            BoardError::new(
                ErrorCode::SchemaNotFound,
                format!("invalid or unknown schema '{schema_id}'"),
            )
        })?;
        let canonical_id = format!("{schema_name}@{schema_version}");
        let schema_view = get_schema_tx(conn, &canonical_id).await?.ok_or_else(|| {
            BoardError::new(
                ErrorCode::SchemaNotFound,
                format!("schema '{canonical_id}' is not registered"),
            )
        })?;

        let data: Value = serde_json::from_str(&body).map_err(|e| {
            BoardError::new(
                ErrorCode::InvalidBlock,
                format!("block for '{canonical_id}' is not valid JSON: {e}"),
            )
        })?;
        if !data.is_object() {
            return Err(BoardError::new(
                ErrorCode::InvalidBlock,
                format!("block for '{canonical_id}' must be a JSON object"),
            ));
        }

        let validator = jsonschema::validator_for(&schema_view.schema).map_err(|e| {
            BoardError::internal(format!("failed to compile schema '{canonical_id}': {e}"))
        })?;
        if let Err(err) = validator.validate(&data) {
            return Err(BoardError::new(
                ErrorCode::InvalidBlock,
                format!("block does not match schema '{canonical_id}': {err}"),
            ));
        }

        parsed.push(ParsedBlock {
            schema: schema_name,
            version: schema_version,
            data,
        });
    }

    Ok(parsed)
}

fn extract_fenced_blocks(text: &str) -> Vec<(String, String)> {
    let mut blocks = Vec::new();
    let mut current: Option<(char, usize, String, String)> = None;

    for line in text.split('\n') {
        let trimmed_start = line.trim_start_matches(' ');
        let leading_spaces = line.len() - trimmed_start.len();

        if let Some((fence_ch, fence_len, ref schema_id, ref mut buf)) = current {
            if leading_spaces <= 3 {
                let count = trimmed_start.chars().take_while(|&c| c == fence_ch).count();
                if count >= fence_len && trimmed_start[count..].trim().is_empty() {
                    blocks.push((schema_id.clone(), buf.trim().to_string()));
                    current = None;
                    continue;
                }
            }
            if !buf.is_empty() {
                buf.push('\n');
            }
            buf.push_str(line);
            continue;
        }

        if leading_spaces <= 3 {
            if let Some(first_ch) = trimmed_start.chars().next() {
                if first_ch == '`' || first_ch == '~' {
                    let count = trimmed_start.chars().take_while(|&c| c == first_ch).count();
                    if count >= 3 {
                        let info = trimmed_start[count..].trim();
                        let schema_id = info
                            .strip_prefix("board-block ")
                            .or_else(|| info.strip_prefix("board:"))
                            .map(str::trim)
                            .filter(|s| !s.is_empty());
                        if let Some(sid) = schema_id {
                            current = Some((first_ch, count, sid.to_string(), String::new()));
                        }
                    }
                }
            }
        }
    }

    blocks
}

pub async fn store_message_blocks_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    message_id: &str,
    blocks: &[ParsedBlock],
) -> BoardResult<()> {
    conn.execute(
        "DELETE FROM message_blocks WHERE message_id = ?1",
        sql_args![message_id],
    )
    .await?;
    for (idx, b) in blocks.iter().enumerate() {
        let data_json = serde_json::to_string(&b.data)
            .map_err(|e| BoardError::internal(format!("failed to serialize block data: {e}")))?;
        conn.execute(
            "INSERT INTO message_blocks (board_id, message_id, idx, schema_name, schema_version, data_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            sql_args![
                board_id,
                message_id,
                idx as i64,
                &b.schema,
                b.version,
                &data_json
            ],
        )
        .await?;
    }
    Ok(())
}

pub async fn load_message_blocks_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_id: &str,
) -> BoardResult<Vec<ParsedBlock>> {
    let rows = conn
        .fetch_all(
            "SELECT schema_name, schema_version, data_json
             FROM message_blocks
             WHERE message_id = ?1
             ORDER BY idx ASC",
            sql_args![message_id],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let data_json = r.get_str(2)?;
        let data: Value = serde_json::from_str(&data_json)
            .map_err(|e| BoardError::internal(format!("corrupted block JSON: {e}")))?;
        out.push(ParsedBlock {
            schema: r.get_str(0)?,
            version: r.get_i64(1)?,
            data,
        });
    }
    Ok(out)
}

pub async fn load_messages_blocks_batch_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_ids: &[&str],
) -> BoardResult<HashMap<String, Vec<ParsedBlock>>> {
    let mut out: HashMap<String, Vec<ParsedBlock>> = HashMap::new();
    if message_ids.is_empty() {
        return Ok(out);
    }
    for chunk in message_ids.chunks(400) {
        let sql = format!(
            "SELECT message_id, schema_name, schema_version, data_json
             FROM message_blocks
             WHERE message_id IN ({})
             ORDER BY message_id ASC, idx ASC",
            sql_placeholders(chunk.len())
        );
        let args: Vec<SqlArg<'_>> = chunk.iter().copied().map(SqlArg::from).collect();
        let rows = conn.fetch_all(&sql, &args).await?;
        for r in rows {
            let mid = r.get_str(0)?;
            let data_json = r.get_str(3)?;
            let data: Value = serde_json::from_str(&data_json)
                .map_err(|e| BoardError::internal(format!("corrupted block JSON: {e}")))?;
            out.entry(mid).or_default().push(ParsedBlock {
                schema: r.get_str(1)?,
                version: r.get_i64(2)?,
                data,
            });
        }
    }
    Ok(out)
}

pub async fn delete_message_blocks_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    message_id: &str,
) -> BoardResult<()> {
    conn.execute(
        "DELETE FROM message_blocks WHERE message_id = ?1",
        sql_args![message_id],
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn extracts_and_validates_builtin_and_custom_blocks() {
        let db = Database::open_in_memory().await.unwrap();
        let md = "Deployed:\n\n```board-block git-commit@1\n{\"sha\":\"a1b2c3d\",\"message\":\"fix bug\"}\n```\nDone!";
        let mut tx = db.begin_tx().await.unwrap();
        let blocks = parse_and_validate_blocks_tx(&mut tx, md, None)
            .await
            .unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].schema, "git-commit");
        assert_eq!(blocks[0].version, 1);
        assert_eq!(blocks[0].data["sha"], "a1b2c3d");

        let bad = "```board:vercel-deployment@1\n{\"id\":\"d1\",\"url\":\"https://x\",\"state\":\"bogus\"}\n```";
        let err = parse_and_validate_blocks_tx(&mut tx, bad, None)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidBlock);

        let missing = "```board-block unknown@1\n{\"a\":1}\n```";
        let err = parse_and_validate_blocks_tx(&mut tx, missing, None)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaNotFound);
        tx.commit().await.unwrap();
    }
}
