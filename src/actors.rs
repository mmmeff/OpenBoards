use std::collections::{BTreeMap, HashMap};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::db::{Database, DbRow, Queryable};
use crate::error::{is_unique_violation, BoardError, BoardResult};
use crate::id::{
    hash_token, new_app_id, new_bearer_token, new_project_id, new_token_id, new_user_id,
};
use crate::permissions::{NarrowingPolicy, Permission, Permissions};
use crate::sql_args;
use crate::validate::{validate_avatar_url, validate_profile_name, validate_slug};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TeamRole {
    Owner,
    Member,
    Developer,
    Viewer,
    None,
}

impl TeamRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Owner => "owner",
            Self::Member => "member",
            Self::Developer => "developer",
            Self::Viewer => "viewer",
            Self::None => "none",
        }
    }

    pub fn parse(s: &str) -> BoardResult<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "owner" => Ok(Self::Owner),
            "member" => Ok(Self::Member),
            "developer" => Ok(Self::Developer),
            "viewer" => Ok(Self::Viewer),
            "none" => Ok(Self::None),
            other => Err(BoardError::bad_request(format!(
                "invalid team role '{other}'; expected owner, member, developer, viewer, or none"
            ))),
        }
    }

    pub const fn is_owner(self) -> bool {
        matches!(self, Self::Owner)
    }

    /// Maximum permission ceiling imposed by this team role.
    pub const fn permission_ceiling(self) -> Permissions {
        match self {
            Self::Owner | Self::Member | Self::Developer => Permissions::ALL,
            Self::Viewer => Permissions::VIEWER,
            Self::None => Permissions::NONE,
        }
    }

    pub const fn can_create_boards(self) -> bool {
        matches!(self, Self::Owner | Self::Member | Self::Developer)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserRecord {
    pub id: String,
    pub username: String,
    pub email: String,
    pub name: String,
    pub role: TeamRole,
    pub avatar: Option<String>,
    pub revoked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectRecord {
    pub id: String,
    pub name: String,
    pub revoked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppRecord {
    pub id: String,
    pub slug: String,
    pub name: String,
    pub scopes: Permissions,
    pub revoked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenSummary {
    pub id: String,
    pub principal: String,
    pub principal_kind: String,
    pub principal_id: String,
    pub environment: Option<String>,
    pub label: Option<String>,
    pub revoked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatedToken {
    pub id: String,
    pub token: String,
    pub principal: String,
    pub label: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Principal {
    User {
        id: String,
        username: String,
        email: String,
        name: String,
        role: TeamRole,
        avatar: Option<String>,
    },
    Project {
        id: String,
        name: String,
        environment: String,
    },
    App {
        id: String,
        slug: String,
        name: String,
        scopes: Permissions,
    },
}

impl Principal {
    /// Canonical principal ID: `user:<id>`, `project:<id>:<env>`, or `app:<id>`.
    pub fn principal_id(&self) -> String {
        match self {
            Self::User { id, .. } => format!("user:{id}"),
            Self::Project {
                id, environment, ..
            } => format!("project:{id}:{environment}"),
            Self::App { id, .. } => format!("app:{id}"),
        }
    }

    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::User { .. } => "user",
            Self::Project { .. } => "project",
            Self::App { .. } => "app",
        }
    }

    pub fn handle(&self) -> String {
        match self {
            Self::User { username, .. } => username.clone(),
            Self::Project { name, .. } => name.clone(),
            Self::App { slug, .. } => slug.clone(),
        }
    }

    pub fn display_name(&self) -> String {
        match self {
            Self::User { name, .. } => name.clone(),
            Self::Project {
                name, environment, ..
            } => {
                if environment == "production" {
                    name.clone()
                } else {
                    format!("{name} ({environment})")
                }
            }
            Self::App { name, .. } => name.clone(),
        }
    }

    pub fn avatar(&self) -> Option<&str> {
        match self {
            Self::User { avatar, .. } => avatar.as_deref(),
            Self::Project { .. } | Self::App { .. } => None,
        }
    }

    pub fn role(&self) -> Option<TeamRole> {
        match self {
            Self::User { role, .. } => Some(*role),
            Self::Project { .. } | Self::App { .. } => None,
        }
    }

    pub fn scopes(&self) -> Option<Permissions> {
        match self {
            Self::App { scopes, .. } => Some(*scopes),
            Self::User { .. } | Self::Project { .. } => None,
        }
    }

    pub fn is_owner(&self) -> bool {
        matches!(self, Self::User { role, .. } if role.is_owner())
    }

    /// Maximum permission ceiling from team role or app scopes.
    pub fn permission_ceiling(&self) -> Permissions {
        match self {
            Self::User { role, .. } => role.permission_ceiling(),
            Self::Project { .. } => Permissions::ALL,
            Self::App { scopes, .. } => *scopes,
        }
    }

    pub fn can_create_boards(&self) -> bool {
        match self {
            Self::User { role, .. } => role.can_create_boards(),
            Self::Project { .. } => true,
            Self::App { scopes, .. } => {
                scopes.has(Permission::Post) || scopes.has(Permission::Manage)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProfileOverlay {
    /// Original display name as provided (validated).
    pub name: String,
    /// Lowercased canonical key (case-insensitive matching).
    pub name_lower: String,
    pub avatar: Option<String>,
}

/// Authenticated caller context with optional profile and per-request narrowing.
#[derive(Debug, Clone)]
pub struct CallerContext {
    pub principal: Principal,
    pub profile: Option<ProfileOverlay>,
    pub narrowing: Option<NarrowingPolicy>,
    pub sandboxed: bool,
}

impl CallerContext {
    pub fn principal_id(&self) -> String {
        self.principal.principal_id()
    }

    /// Canonical actor ID: `<principal>` or `<principal>+<profile_lower>`.
    pub fn actor_id(&self) -> String {
        match &self.profile {
            Some(p) => format!("{}+{}", self.principal.principal_id(), p.name_lower),
            None => self.principal.principal_id(),
        }
    }

    /// Handle: `<user>`, `<user>/<profile>`, `<project>`, `<project>/<profile>`, etc.
    pub fn handle(&self) -> String {
        match &self.profile {
            Some(p) => format!("{}/{}", self.principal.handle(), p.name_lower),
            None => self.principal.handle(),
        }
    }

    pub fn avatar(&self) -> Option<&str> {
        match &self.profile {
            Some(p) => p.avatar.as_deref(),
            None => self.principal.avatar(),
        }
    }

    pub fn is_narrowed(&self) -> bool {
        self.narrowing.is_some()
    }

    /// True if narrowed (`X-Board-Can`) OR sandboxed (`X-Board-Sandboxed`).
    /// Restricted callers are always denied grant mutations, watcher registration,
    /// and sync record reads/writes.
    pub fn is_restricted(&self) -> bool {
        self.narrowing.is_some() || self.sandboxed
    }

    pub fn is_unrestricted_owner(&self) -> bool {
        self.principal.is_owner() && !self.is_restricted()
    }

    pub fn can_mutate_grants(&self) -> bool {
        !self.is_restricted()
    }

    pub fn can_register_watchers(&self) -> bool {
        !self.is_restricted() && matches!(self.principal, Principal::Project { .. })
    }

    pub fn can_access_sync_records(&self) -> bool {
        !self.is_restricted()
    }

    pub fn can_access_sync(&self) -> bool {
        self.can_access_sync_records()
    }

    /// Intersects `perms` with any per-request `X-Board-Can` narrowing for `channel`.
    pub fn apply_narrowing(&self, channel: &str, perms: Permissions) -> Permissions {
        match &self.narrowing {
            Some(policy) => perms.intersect(policy.cap_for_channel(channel)),
            None => perms,
        }
    }

    /// Intersects `perms` with any per-request `X-Board-Can` narrowing at `pattern`.
    pub fn apply_narrowing_at_pattern(&self, pattern: &str, perms: Permissions) -> Permissions {
        match &self.narrowing {
            Some(policy) => perms.intersect(policy.cap_for_pattern(pattern)),
            None => perms,
        }
    }

    /// Profile edit/delete/cancel isolation rule:
    /// - A profile can modify only what that exact profile wrote.
    /// - A bare principal (no profile) can modify what it or any of its profiles wrote.
    pub fn owns_actor_item(&self, item_actor_id: &str) -> bool {
        match &self.profile {
            Some(_) => self.actor_id() == item_actor_id,
            None => {
                let base = self.principal_id();
                item_actor_id == base
                    || item_actor_id
                        .strip_prefix(&base)
                        .is_some_and(|rest| rest.starts_with('+'))
            }
        }
    }

    pub fn whoami_view(&self) -> WhoAmIView {
        let can = self.narrowing.as_ref().map(|n| n.rules.clone());
        let environment = match &self.principal {
            Principal::Project { environment, .. } => Some(environment.clone()),
            _ => None,
        };
        WhoAmIView {
            actor: self.actor_id(),
            principal: self.principal_id(),
            kind: self.principal.kind_str().to_string(),
            profile: self.profile.as_ref().map(|p| p.name.clone()),
            avatar: self.avatar().map(str::to_string),
            handle: self.handle(),
            name: self.principal.display_name(),
            role: self.principal.role(),
            scopes: self.principal.scopes(),
            environment,
            narrowed: self.is_narrowed(),
            sandboxed: self.sandboxed,
            can,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhoAmIView {
    pub actor: String,
    pub principal: String,
    pub kind: String,
    pub profile: Option<String>,
    pub avatar: Option<String>,
    pub handle: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<TeamRole>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes: Option<Permissions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,
    pub narrowed: bool,
    pub sandboxed: bool,
    pub can: Option<BTreeMap<String, Permissions>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorView {
    pub id: String,
    pub kind: String,
    pub handle: String,
    pub name: String,
    pub avatar: Option<String>,
}

/// Ensures a bootstrap `owner` user and initial bearer token exist on startup.
/// Returns `Some(plaintext_token)` only if a brand-new token was created.
pub async fn bootstrap_owner(
    db: &Database,
    configured_token: Option<&str>,
) -> BoardResult<Option<String>> {
    let mut tx = db.begin_tx().await?;
    tx.advisory_xact_lock("bootstrap", "owner").await?;

    let owner_id: String = match tx
        .fetch_optional(
            "SELECT id FROM users WHERE role = 'owner' AND revoked_at IS NULL ORDER BY created_at ASC LIMIT 1",
            sql_args![],
        )
        .await?
    {
        Some(row) => row.get_str(0)?,
        None => {
            let id = new_user_id();
            let now = Utc::now().to_rfc3339();
            tx.execute(
                "INSERT INTO users (id, username, username_lower, email, email_lower, name, role, avatar, created_at)
                 VALUES (?1, 'admin', 'admin', 'admin@local', 'admin@local', 'Admin', 'owner', NULL, ?2)",
                sql_args![&id, &now],
            )
            .await?;
            id
        }
    };

    if let Some(raw_token) = configured_token {
        if raw_token.trim().is_empty() {
            return Err(BoardError::bad_request(
                "configured admin token cannot be empty",
            ));
        }
        let digest = hash_token(raw_token);
        let exists = tx
            .fetch_optional(
                "SELECT 1 FROM tokens WHERE token_hash = ?1 AND revoked_at IS NULL",
                sql_args![&digest],
            )
            .await?
            .is_some();
        if !exists {
            let tok_id = new_token_id();
            let now = Utc::now().to_rfc3339();
            tx.execute(
                "INSERT INTO tokens (id, token_hash, principal_kind, principal_id, environment, label, created_at)
                 VALUES (?1, ?2, 'user', ?3, NULL, 'bootstrap-owner', ?4)",
                sql_args![&tok_id, &digest, &owner_id, &now],
            )
            .await?;
        }
        tx.commit().await?;
        return Ok(None);
    }

    let active_tokens = tx
        .fetch_one(
            "SELECT COUNT(*) FROM tokens WHERE revoked_at IS NULL",
            sql_args![],
        )
        .await?
        .get_i64(0)?;
    if active_tokens > 0 {
        tx.commit().await?;
        return Ok(None);
    }

    let plaintext = new_bearer_token();
    let digest = hash_token(&plaintext);
    let tok_id = new_token_id();
    let now = Utc::now().to_rfc3339();
    tx.execute(
        "INSERT INTO tokens (id, token_hash, principal_kind, principal_id, environment, label, created_at)
         VALUES (?1, ?2, 'user', ?3, NULL, 'bootstrap-owner', ?4)",
        sql_args![&tok_id, &digest, &owner_id, &now],
    )
    .await?;
    tx.commit().await?;
    Ok(Some(plaintext))
}

/// Resolves a raw bearer token to its active `Principal`.
pub async fn authenticate_bearer_token(db: &Database, raw_token: &str) -> BoardResult<Principal> {
    let digest = hash_token(raw_token);
    let mut conn = db.read_conn();
    let row = conn
        .fetch_optional(
            "SELECT principal_kind, principal_id, environment
             FROM tokens
             WHERE token_hash = ?1 AND revoked_at IS NULL",
            sql_args![&digest],
        )
        .await?
        .ok_or_else(|| BoardError::unauthorized("invalid or revoked bearer token"))?;

    let kind = row.get_str(0)?;
    let pid = row.get_str(1)?;
    let env = row.get_opt_str(2)?;

    match kind.as_str() {
        "user" => {
            let user = get_active_user_by_id(&mut conn, &pid)
                .await?
                .ok_or_else(|| BoardError::unauthorized("user account is revoked or missing"))?;
            Ok(Principal::User {
                id: user.id,
                username: user.username,
                email: user.email,
                name: user.name,
                role: user.role,
                avatar: user.avatar,
            })
        }
        "project" => {
            let project = get_active_project_by_id(&mut conn, &pid)
                .await?
                .ok_or_else(|| BoardError::unauthorized("project is revoked or missing"))?;
            Ok(Principal::Project {
                id: project.id,
                name: project.name,
                environment: env.unwrap_or_else(|| "production".to_string()),
            })
        }
        "app" => {
            let app = get_active_app_by_id(&mut conn, &pid)
                .await?
                .ok_or_else(|| BoardError::unauthorized("app is revoked or missing"))?;
            Ok(Principal::App {
                id: app.id,
                slug: app.slug,
                name: app.name,
                scopes: app.scopes,
            })
        }
        other => Err(BoardError::internal(format!(
            "corrupted token principal_kind '{other}'"
        ))),
    }
}

/// Resolves an active (non-revoked) `Principal` from its canonical principal ID
/// (`user:<id>`, `project:<id>:<env>`, or `app:<id>`).
pub async fn resolve_active_principal_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    principal_id: &str,
) -> BoardResult<Option<Principal>> {
    if let Some(uid) = principal_id.strip_prefix("user:") {
        let Some(user) = get_active_user_by_id(conn, uid).await? else {
            return Ok(None);
        };
        return Ok(Some(Principal::User {
            id: user.id,
            username: user.username,
            email: user.email,
            name: user.name,
            role: user.role,
            avatar: user.avatar,
        }));
    }
    if let Some(rest) = principal_id.strip_prefix("project:") {
        let Some((pid, env)) = rest.split_once(':') else {
            return Ok(None);
        };
        let Some(project) = get_active_project_by_id(conn, pid).await? else {
            return Ok(None);
        };
        return Ok(Some(Principal::Project {
            id: project.id,
            name: project.name,
            environment: env.to_string(),
        }));
    }
    if let Some(aid) = principal_id.strip_prefix("app:") {
        let Some(app) = get_active_app_by_id(conn, aid).await? else {
            return Ok(None);
        };
        return Ok(Some(Principal::App {
            id: app.id,
            slug: app.slug,
            name: app.name,
            scopes: app.scopes,
        }));
    }
    Ok(None)
}

/// Implicitly registers a profile (on first write) and records actor activity on `(board_id, channel)`.
pub async fn record_actor_write_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    channel: &str,
    caller: &CallerContext,
) -> BoardResult<()> {
    let now = Utc::now().to_rfc3339();
    if let Some(profile) = &caller.profile {
        let pid = caller.principal_id();
        conn.execute(
            "INSERT INTO profiles (principal_id, name_lower, name, avatar, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)
             ON CONFLICT(principal_id, name_lower) DO UPDATE SET
               avatar = COALESCE(excluded.avatar, profiles.avatar),
               updated_at = excluded.updated_at",
            sql_args![
                &pid,
                &profile.name_lower,
                &profile.name,
                &profile.avatar,
                &now
            ],
        )
        .await?;
    }

    let aid = caller.actor_id();
    conn.execute(
        "INSERT INTO actor_activity (board_id, channel, actor_id, first_seen_at, last_seen_at)
         VALUES (?1, ?2, ?3, ?4, ?4)
         ON CONFLICT(board_id, channel, actor_id) DO UPDATE SET
           last_seen_at = excluded.last_seen_at",
        sql_args![board_id, channel, &aid, &now],
    )
    .await?;

    Ok(())
}

/// Public helper for registering an actor's profile and channel activity outside an existing tx.
pub async fn record_actor_write(
    db: &Database,
    board_id: &str,
    channel: &str,
    caller: &CallerContext,
) -> BoardResult<()> {
    let mut tx = db.begin_tx().await?;
    record_actor_write_tx(&mut tx, board_id, channel, caller).await?;
    tx.commit().await
}

/// Resolves any canonical actor ID (`user:<id>`, `user:<id>+<profile>`, `project:<id>:<env>[+<profile>]`, `app:<id>[+<profile>]`)
/// into an `ActorView`.
pub async fn resolve_actor_view<C: Queryable + ?Sized>(
    conn: &mut C,
    actor_id: &str,
) -> BoardResult<Option<ActorView>> {
    let (principal_part, profile_part) = match actor_id.split_once('+') {
        Some((p, prof)) => (p, Some(prof)),
        None => (actor_id, None),
    };

    let (base_kind, base_handle, base_name, base_avatar) =
        if let Some(uid) = principal_part.strip_prefix("user:") {
            let Some(row) = conn
                .fetch_optional(
                    "SELECT username, name, avatar FROM users WHERE id = ?1",
                    sql_args![uid],
                )
                .await?
            else {
                return Ok(None);
            };
            ("user", row.get_str(0)?, row.get_str(1)?, row.get_opt_str(2)?)
        } else if let Some(rest) = principal_part.strip_prefix("project:") {
            let Some((pid, env)) = rest.split_once(':') else {
                return Ok(None);
            };
            let Some(row) = conn
                .fetch_optional("SELECT name FROM projects WHERE id = ?1", sql_args![pid])
                .await?
            else {
                return Ok(None);
            };
            let name = row.get_str(0)?;
            let display = if env == "production" {
                name.clone()
            } else {
                format!("{name} ({env})")
            };
            ("project", name, display, None)
        } else if let Some(aid) = principal_part.strip_prefix("app:") {
            let Some(row) = conn
                .fetch_optional(
                    "SELECT slug, name FROM apps WHERE id = ?1",
                    sql_args![aid],
                )
                .await?
            else {
                return Ok(None);
            };
            ("app", row.get_str(0)?, row.get_str(1)?, None)
        } else {
            return Ok(None);
        };

    if let Some(prof_lower) = profile_part {
        let prof_row = conn
            .fetch_optional(
                "SELECT name, avatar FROM profiles WHERE principal_id = ?1 AND name_lower = ?2",
                sql_args![principal_part, prof_lower],
            )
            .await?;
        let (prof_name, prof_avatar) = match prof_row {
            Some(r) => (r.get_str(0)?, r.get_opt_str(1)?),
            None => (prof_lower.to_string(), None),
        };
        if prof_lower.contains(":user:") {
            let handle_seg = prof_name
                .to_ascii_lowercase()
                .chars()
                .map(|c| {
                    if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                        c
                    } else {
                        '-'
                    }
                })
                .collect::<String>();
            let handle_seg = if handle_seg.is_empty() {
                prof_lower.to_string()
            } else {
                handle_seg
            };
            Ok(Some(ActorView {
                id: actor_id.to_string(),
                kind: "synced".to_string(),
                handle: format!("{base_handle}/{handle_seg}"),
                name: prof_name,
                avatar: prof_avatar,
            }))
        } else {
            Ok(Some(ActorView {
                id: actor_id.to_string(),
                kind: "profile".to_string(),
                handle: format!("{base_handle}/{prof_lower}"),
                name: prof_name,
                avatar: prof_avatar,
            }))
        }
    } else {
        Ok(Some(ActorView {
            id: actor_id.to_string(),
            kind: base_kind.to_string(),
            handle: base_handle,
            name: base_name,
            avatar: base_avatar,
        }))
    }
}

/// Resolves display handles for a batch of actor IDs, looking up each unique actor only once.
pub async fn resolve_actor_handles_batch_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    actor_ids: &[&str],
) -> BoardResult<HashMap<String, String>> {
    let mut handles = HashMap::new();
    for &actor_id in actor_ids {
        if handles.contains_key(actor_id) {
            continue;
        }
        let handle = resolve_actor_view(conn, actor_id)
            .await?
            .map(|a| a.handle)
            .unwrap_or_else(|| actor_id.to_string());
        handles.insert(actor_id.to_string(), handle);
    }
    Ok(handles)
}

/// Lists actors that have written on `board_id`, filtered by `can_read_channel`.
/// If `has_wildcard_read` is true, board-level (`channel == ""`) entries are also visible,
/// while channel-scoped entries still verify `can_read_channel(channel)` so private/closed
/// channels never leak hidden actors.
pub async fn list_board_actors<C, F>(
    conn: &mut C,
    board_id: &str,
    has_wildcard_read: bool,
    mut can_read_channel: F,
) -> BoardResult<Vec<ActorView>>
where
    C: Queryable + ?Sized,
    F: FnMut(&str) -> bool,
{
    let rows = conn
        .fetch_all(
            "SELECT actor_id, channel FROM actor_activity WHERE board_id = ?1 ORDER BY actor_id ASC",
            sql_args![board_id],
        )
        .await?;

    let mut visible_ids: Vec<String> = Vec::new();
    for row in rows {
        let actor_id = row.get_str(0)?;
        let channel = row.get_str(1)?;
        let visible = if channel.is_empty() {
            has_wildcard_read
        } else {
            can_read_channel(&channel)
        };
        if visible && !visible_ids.contains(&actor_id) {
            visible_ids.push(actor_id);
        }
    }

    let mut actors = Vec::with_capacity(visible_ids.len());
    for actor_id in visible_ids {
        if let Some(view) = resolve_actor_view(conn, &actor_id).await? {
            actors.push(view);
        }
    }
    actors.sort_by(|a, b| a.handle.cmp(&b.handle).then_with(|| a.id.cmp(&b.id)));
    Ok(actors)
}

// --- Admin CRUD operations ---

#[derive(Debug, Deserialize)]
pub struct CreateUserInput {
    pub username: String,
    pub email: String,
    pub name: Option<String>,
    pub role: Option<TeamRole>,
    pub avatar: Option<String>,
}

pub async fn create_user(db: &Database, input: CreateUserInput) -> BoardResult<UserRecord> {
    let username = input.username.trim().to_ascii_lowercase();
    validate_slug("username", &username)?;
    let email = input.email.trim().to_string();
    if email.is_empty() || !email.contains('@') || email.len() > 254 {
        return Err(BoardError::bad_request("invalid email address"));
    }
    let email_lower = email.to_ascii_lowercase();
    if let Some(avatar) = &input.avatar {
        validate_avatar_url(avatar)?;
    }
    let name = input
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&username)
        .to_string();
    let role = input.role.unwrap_or(TeamRole::Member);
    let id = new_user_id();
    let now = Utc::now().to_rfc3339();

    let mut tx = db.begin_tx().await?;
    let dup = tx
        .fetch_optional(
            "SELECT 1 FROM users WHERE username_lower = ?1 OR email_lower = ?2",
            sql_args![&username, &email_lower],
        )
        .await?
        .is_some();
    if dup {
        return Err(BoardError::conflict(
            "a user with that username or email already exists",
        ));
    }

    if let Err(err) = tx
        .execute_raw_err(
            "INSERT INTO users (id, username, username_lower, email, email_lower, name, role, avatar, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            sql_args![
                &id,
                &username,
                &username,
                &email,
                &email_lower,
                &name,
                role.as_str(),
                &input.avatar,
                &now
            ],
        )
        .await
    {
        if is_unique_violation(&err) {
            return Err(BoardError::conflict(
                "a user with that username or email already exists",
            ));
        }
        return Err(err.into());
    }
    tx.commit().await?;

    Ok(UserRecord {
        id,
        username,
        email,
        name,
        role,
        avatar: input.avatar,
        revoked_at: None,
        created_at: now,
    })
}

pub async fn list_users(db: &Database) -> BoardResult<Vec<UserRecord>> {
    let mut conn = db.read_conn();
    let rows = conn
        .fetch_all(
            "SELECT id, username, email, name, role, avatar, revoked_at, created_at
             FROM users ORDER BY created_at ASC",
            sql_args![],
        )
        .await?;
    rows.iter().map(map_user_row).collect()
}

#[derive(Debug, Deserialize)]
pub struct UpdateUserInput {
    pub name: Option<String>,
    pub role: Option<TeamRole>,
    pub avatar: Option<Option<String>>,
}

pub async fn update_user(
    db: &Database,
    id: &str,
    input: UpdateUserInput,
) -> BoardResult<UserRecord> {
    if let Some(Some(avatar)) = &input.avatar {
        validate_avatar_url(avatar)?;
    }
    let mut tx = db.begin_tx().await?;
    let mut user = get_user_by_id(&mut tx, id)
        .await?
        .ok_or_else(|| BoardError::not_found(format!("user '{id}' not found")))?;
    if let Some(name) = input.name {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(BoardError::bad_request("name cannot be blank"));
        }
        user.name = trimmed.to_string();
    }
    if let Some(role) = input.role {
        user.role = role;
    }
    if let Some(avatar) = input.avatar {
        user.avatar = avatar;
    }
    tx.execute(
        "UPDATE users SET name = ?1, role = ?2, avatar = ?3 WHERE id = ?4",
        sql_args![&user.name, user.role.as_str(), &user.avatar, &user.id],
    )
    .await?;
    tx.commit().await?;
    Ok(user)
}

pub async fn revoke_user(db: &Database, id: &str) -> BoardResult<UserRecord> {
    let now = Utc::now().to_rfc3339();
    let mut tx = db.begin_tx().await?;
    let mut user = get_user_by_id(&mut tx, id)
        .await?
        .ok_or_else(|| BoardError::not_found(format!("user '{id}' not found")))?;
    if user.revoked_at.is_none() {
        tx.execute(
            "UPDATE users SET revoked_at = ?1 WHERE id = ?2",
            sql_args![&now, id],
        )
        .await?;
        tx.execute(
            "UPDATE tokens SET revoked_at = ?1 WHERE principal_kind = 'user' AND principal_id = ?2 AND revoked_at IS NULL",
            sql_args![&now, id],
        )
        .await?;
        user.revoked_at = Some(now);
    }
    tx.commit().await?;
    Ok(user)
}

#[derive(Debug, Deserialize)]
pub struct CreateProjectInput {
    pub name: String,
}

pub async fn create_project(
    db: &Database,
    input: CreateProjectInput,
) -> BoardResult<ProjectRecord> {
    let name = input.name.trim().to_ascii_lowercase();
    validate_slug("project name", &name)?;
    let id = new_project_id();
    let now = Utc::now().to_rfc3339();

    let mut tx = db.begin_tx().await?;
    let dup = tx
        .fetch_optional(
            "SELECT 1 FROM projects WHERE name_lower = ?1",
            sql_args![&name],
        )
        .await?
        .is_some();
    if dup {
        return Err(BoardError::conflict(format!(
            "project '{name}' already exists"
        )));
    }
    if let Err(err) = tx
        .execute_raw_err(
            "INSERT INTO projects (id, name, name_lower, created_at) VALUES (?1, ?2, ?3, ?4)",
            sql_args![&id, &name, &name, &now],
        )
        .await
    {
        if is_unique_violation(&err) {
            return Err(BoardError::conflict(format!(
                "project '{name}' already exists"
            )));
        }
        return Err(err.into());
    }
    tx.commit().await?;
    Ok(ProjectRecord {
        id,
        name,
        revoked_at: None,
        created_at: now,
    })
}

pub async fn list_projects(db: &Database) -> BoardResult<Vec<ProjectRecord>> {
    let mut conn = db.read_conn();
    let rows = conn
        .fetch_all(
            "SELECT id, name, revoked_at, created_at FROM projects ORDER BY created_at ASC",
            sql_args![],
        )
        .await?;
    rows.iter().map(map_project_row).collect()
}

pub async fn revoke_project(db: &Database, id: &str) -> BoardResult<ProjectRecord> {
    let now = Utc::now().to_rfc3339();
    let mut tx = db.begin_tx().await?;
    let mut project = get_project_by_id(&mut tx, id)
        .await?
        .ok_or_else(|| BoardError::not_found(format!("project '{id}' not found")))?;
    if project.revoked_at.is_none() {
        tx.execute(
            "UPDATE projects SET revoked_at = ?1 WHERE id = ?2",
            sql_args![&now, id],
        )
        .await?;
        tx.execute(
            "UPDATE tokens SET revoked_at = ?1 WHERE principal_kind = 'project' AND principal_id = ?2 AND revoked_at IS NULL",
            sql_args![&now, id],
        )
        .await?;
        project.revoked_at = Some(now);
    }
    tx.commit().await?;
    Ok(project)
}

#[derive(Debug, Deserialize)]
pub struct CreateAppInput {
    pub slug: String,
    pub name: Option<String>,
    pub scopes: Permissions,
}

pub async fn create_app(db: &Database, input: CreateAppInput) -> BoardResult<AppRecord> {
    let slug = input.slug.trim().to_ascii_lowercase();
    validate_slug("app slug", &slug)?;
    let name = input
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(&slug)
        .to_string();
    let id = new_app_id();
    let now = Utc::now().to_rfc3339();

    let mut tx = db.begin_tx().await?;
    let dup = tx
        .fetch_optional(
            "SELECT 1 FROM apps WHERE slug_lower = ?1",
            sql_args![&slug],
        )
        .await?
        .is_some();
    if dup {
        return Err(BoardError::conflict(format!("app '{slug}' already exists")));
    }
    let scopes_csv = input.scopes.to_csv();
    if let Err(err) = tx
        .execute_raw_err(
            "INSERT INTO apps (id, slug, slug_lower, name, scopes, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            sql_args![&id, &slug, &slug, &name, &scopes_csv, &now],
        )
        .await
    {
        if is_unique_violation(&err) {
            return Err(BoardError::conflict(format!("app '{slug}' already exists")));
        }
        return Err(err.into());
    }
    tx.commit().await?;
    Ok(AppRecord {
        id,
        slug,
        name,
        scopes: input.scopes,
        revoked_at: None,
        created_at: now,
    })
}

pub async fn list_apps(db: &Database) -> BoardResult<Vec<AppRecord>> {
    let mut conn = db.read_conn();
    let rows = conn
        .fetch_all(
            "SELECT id, slug, name, scopes, revoked_at, created_at FROM apps ORDER BY created_at ASC",
            sql_args![],
        )
        .await?;
    rows.iter().map(map_app_row).collect()
}

#[derive(Debug, Deserialize)]
pub struct UpdateAppInput {
    pub name: Option<String>,
    pub scopes: Option<Permissions>,
}

pub async fn update_app(db: &Database, id: &str, input: UpdateAppInput) -> BoardResult<AppRecord> {
    let mut tx = db.begin_tx().await?;
    let mut app = get_app_by_id(&mut tx, id)
        .await?
        .ok_or_else(|| BoardError::not_found(format!("app '{id}' not found")))?;
    if let Some(name) = input.name {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return Err(BoardError::bad_request("app name cannot be blank"));
        }
        app.name = trimmed.to_string();
    }
    if let Some(scopes) = input.scopes {
        app.scopes = scopes;
    }
    let scopes_csv = app.scopes.to_csv();
    tx.execute(
        "UPDATE apps SET name = ?1, scopes = ?2 WHERE id = ?3",
        sql_args![&app.name, &scopes_csv, &app.id],
    )
    .await?;
    tx.commit().await?;
    Ok(app)
}

pub async fn revoke_app(db: &Database, id: &str) -> BoardResult<AppRecord> {
    let now = Utc::now().to_rfc3339();
    let mut tx = db.begin_tx().await?;
    let mut app = get_app_by_id(&mut tx, id)
        .await?
        .ok_or_else(|| BoardError::not_found(format!("app '{id}' not found")))?;
    if app.revoked_at.is_none() {
        tx.execute(
            "UPDATE apps SET revoked_at = ?1 WHERE id = ?2",
            sql_args![&now, id],
        )
        .await?;
        tx.execute(
            "UPDATE tokens SET revoked_at = ?1 WHERE principal_kind = 'app' AND principal_id = ?2 AND revoked_at IS NULL",
            sql_args![&now, id],
        )
        .await?;
        app.revoked_at = Some(now);
    }
    tx.commit().await?;
    Ok(app)
}

#[derive(Debug, Deserialize)]
pub struct CreateTokenInput {
    pub principal_kind: String,
    pub principal_id: String,
    pub environment: Option<String>,
    pub label: Option<String>,
}

pub async fn create_token(db: &Database, input: CreateTokenInput) -> BoardResult<CreatedToken> {
    let kind = input.principal_kind.trim().to_ascii_lowercase();
    let pid = input.principal_id.trim().to_string();
    let env = match kind.as_str() {
        "project" => {
            let e = input
                .environment
                .as_deref()
                .unwrap_or("production")
                .trim()
                .to_ascii_lowercase();
            validate_slug("environment", &e)?;
            Some(e)
        }
        "user" | "app" => {
            if input.environment.is_some() {
                return Err(BoardError::bad_request(
                    "environment is only valid for project tokens",
                ));
            }
            None
        }
        other => {
            return Err(BoardError::bad_request(format!(
                "invalid principal_kind '{other}'; expected user, project, or app"
            )));
        }
    };

    let plaintext = new_bearer_token();
    let digest = hash_token(&plaintext);
    let tok_id = new_token_id();
    let now = Utc::now().to_rfc3339();

    let mut tx = db.begin_tx().await?;
    let principal_str = match kind.as_str() {
        "user" => {
            if get_active_user_by_id(&mut tx, &pid).await?.is_none() {
                return Err(BoardError::not_found(format!(
                    "active user '{pid}' not found"
                )));
            }
            format!("user:{pid}")
        }
        "project" => {
            if get_active_project_by_id(&mut tx, &pid).await?.is_none() {
                return Err(BoardError::not_found(format!(
                    "active project '{pid}' not found"
                )));
            }
            format!("project:{pid}:{}", env.as_deref().unwrap_or("production"))
        }
        "app" => {
            if get_active_app_by_id(&mut tx, &pid).await?.is_none() {
                return Err(BoardError::not_found(format!(
                    "active app '{pid}' not found"
                )));
            }
            format!("app:{pid}")
        }
        _ => unreachable!(),
    };

    tx.execute(
        "INSERT INTO tokens (id, token_hash, principal_kind, principal_id, environment, label, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        sql_args![&tok_id, &digest, &kind, &pid, &env, &input.label, &now],
    )
    .await?;
    tx.commit().await?;

    Ok(CreatedToken {
        id: tok_id,
        token: plaintext,
        principal: principal_str,
        label: input.label,
        created_at: now,
    })
}

pub async fn list_tokens(db: &Database) -> BoardResult<Vec<TokenSummary>> {
    let mut conn = db.read_conn();
    let rows = conn
        .fetch_all(
            "SELECT id, principal_kind, principal_id, environment, label, revoked_at, created_at
             FROM tokens ORDER BY created_at ASC",
            sql_args![],
        )
        .await?;
    rows.iter().map(map_token_summary_row).collect()
}

pub async fn revoke_token(db: &Database, id: &str) -> BoardResult<TokenSummary> {
    let now = Utc::now().to_rfc3339();
    let mut tx = db.begin_tx().await?;
    let row = tx
        .fetch_optional(
            "SELECT id, principal_kind, principal_id, environment, label, revoked_at, created_at
             FROM tokens WHERE id = ?1",
            sql_args![id],
        )
        .await?
        .ok_or_else(|| BoardError::not_found(format!("token '{id}' not found")))?;
    let mut tok = map_token_summary_row(&row)?;

    if tok.revoked_at.is_none() {
        tx.execute(
            "UPDATE tokens SET revoked_at = ?1 WHERE id = ?2",
            sql_args![&now, id],
        )
        .await?;
        tok.revoked_at = Some(now);
    }
    tx.commit().await?;
    Ok(tok)
}

// --- Lookup helpers used by Grants and Explain ---

pub async fn lookup_user_by_identifier<C: Queryable + ?Sized>(
    conn: &mut C,
    ident: &str,
) -> BoardResult<Option<UserRecord>> {
    let trimmed = ident.trim();
    let id_candidate = trimmed.strip_prefix("user:").unwrap_or(trimmed);
    let lower = id_candidate.to_ascii_lowercase();
    let row = conn
        .fetch_optional(
            "SELECT id, username, email, name, role, avatar, revoked_at, created_at
             FROM users
             WHERE id = ?1 OR email_lower = ?2 OR username_lower = ?2
             LIMIT 1",
            sql_args![id_candidate, &lower],
        )
        .await?;
    row.as_ref().map(map_user_row).transpose()
}

pub async fn lookup_project_by_identifier<C: Queryable + ?Sized>(
    conn: &mut C,
    ident: &str,
) -> BoardResult<Option<ProjectRecord>> {
    let trimmed = ident.trim();
    let lower = trimmed.to_ascii_lowercase();
    let row = conn
        .fetch_optional(
            "SELECT id, name, revoked_at, created_at
             FROM projects
             WHERE id = ?1 OR name_lower = ?2
             LIMIT 1",
            sql_args![trimmed, &lower],
        )
        .await?;
    row.as_ref().map(map_project_row).transpose()
}

pub async fn lookup_app_by_identifier<C: Queryable + ?Sized>(
    conn: &mut C,
    ident: &str,
) -> BoardResult<Option<AppRecord>> {
    let trimmed = ident.trim();
    let id_candidate = trimmed.strip_prefix("app:").unwrap_or(trimmed);
    let lower = id_candidate.to_ascii_lowercase();
    let row = conn
        .fetch_optional(
            "SELECT id, slug, name, scopes, revoked_at, created_at
             FROM apps
             WHERE id = ?1 OR slug_lower = ?2
             LIMIT 1",
            sql_args![id_candidate, &lower],
        )
        .await?;
    row.as_ref().map(map_app_row).transpose()
}

pub fn parse_profile_headers(
    profile_hdr: Option<&str>,
    as_hdr: Option<&str>,
    avatar_hdr: Option<&str>,
) -> BoardResult<Option<ProfileOverlay>> {
    let profile_raw = match (profile_hdr, as_hdr) {
        (Some(p), Some(a)) => {
            if !p.trim().eq_ignore_ascii_case(a.trim()) {
                return Err(BoardError::bad_request(
                    "conflicting X-Board-Profile and X-Board-As headers",
                ));
            }
            Some(p.trim())
        }
        (Some(p), None) => Some(p.trim()),
        (None, Some(a)) => Some(a.trim()),
        (None, None) => None,
    };

    let avatar = match avatar_hdr {
        Some(raw) => {
            let trimmed = raw.trim();
            validate_avatar_url(trimmed)?;
            Some(trimmed.to_string())
        }
        None => None,
    };

    match profile_raw {
        Some(name) => {
            validate_profile_name(name)?;
            Ok(Some(ProfileOverlay {
                name: name.to_string(),
                name_lower: name.to_ascii_lowercase(),
                avatar,
            }))
        }
        None => {
            if avatar.is_some() {
                return Err(BoardError::bad_request(
                    "X-Board-Avatar requires X-Board-Profile or X-Board-As",
                ));
            }
            Ok(None)
        }
    }
}

pub fn parse_sandboxed_header(raw: Option<&str>) -> BoardResult<bool> {
    match raw {
        None => Ok(false),
        Some(v) => match v.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Ok(true),
            "false" | "0" => Ok(false),
            other => Err(BoardError::bad_request(format!(
                "invalid X-Board-Sandboxed value '{other}'; expected true or false"
            ))),
        },
    }
}

fn format_principal_str(kind: &str, pid: &str, env: Option<&str>) -> String {
    match kind {
        "user" => format!("user:{pid}"),
        "project" => format!("project:{pid}:{}", env.unwrap_or("production")),
        "app" => format!("app:{pid}"),
        _ => format!("{kind}:{pid}"),
    }
}

async fn get_user_by_id<C: Queryable + ?Sized>(
    conn: &mut C,
    id: &str,
) -> BoardResult<Option<UserRecord>> {
    let row = conn
        .fetch_optional(
            "SELECT id, username, email, name, role, avatar, revoked_at, created_at
             FROM users WHERE id = ?1",
            sql_args![id],
        )
        .await?;
    row.as_ref().map(map_user_row).transpose()
}

async fn get_active_user_by_id<C: Queryable + ?Sized>(
    conn: &mut C,
    id: &str,
) -> BoardResult<Option<UserRecord>> {
    let row = conn
        .fetch_optional(
            "SELECT id, username, email, name, role, avatar, revoked_at, created_at
             FROM users WHERE id = ?1 AND revoked_at IS NULL",
            sql_args![id],
        )
        .await?;
    row.as_ref().map(map_user_row).transpose()
}

async fn get_project_by_id<C: Queryable + ?Sized>(
    conn: &mut C,
    id: &str,
) -> BoardResult<Option<ProjectRecord>> {
    let row = conn
        .fetch_optional(
            "SELECT id, name, revoked_at, created_at FROM projects WHERE id = ?1",
            sql_args![id],
        )
        .await?;
    row.as_ref().map(map_project_row).transpose()
}

async fn get_active_project_by_id<C: Queryable + ?Sized>(
    conn: &mut C,
    id: &str,
) -> BoardResult<Option<ProjectRecord>> {
    let row = conn
        .fetch_optional(
            "SELECT id, name, revoked_at, created_at FROM projects WHERE id = ?1 AND revoked_at IS NULL",
            sql_args![id],
        )
        .await?;
    row.as_ref().map(map_project_row).transpose()
}

async fn get_app_by_id<C: Queryable + ?Sized>(
    conn: &mut C,
    id: &str,
) -> BoardResult<Option<AppRecord>> {
    let row = conn
        .fetch_optional(
            "SELECT id, slug, name, scopes, revoked_at, created_at FROM apps WHERE id = ?1",
            sql_args![id],
        )
        .await?;
    row.as_ref().map(map_app_row).transpose()
}

async fn get_active_app_by_id<C: Queryable + ?Sized>(
    conn: &mut C,
    id: &str,
) -> BoardResult<Option<AppRecord>> {
    let row = conn
        .fetch_optional(
            "SELECT id, slug, name, scopes, revoked_at, created_at FROM apps WHERE id = ?1 AND revoked_at IS NULL",
            sql_args![id],
        )
        .await?;
    row.as_ref().map(map_app_row).transpose()
}

fn map_user_row(r: &DbRow) -> BoardResult<UserRecord> {
    let role_str = r.get_str(4)?;
    let role = TeamRole::parse(&role_str).unwrap_or(TeamRole::None);
    Ok(UserRecord {
        id: r.get_str(0)?,
        username: r.get_str(1)?,
        email: r.get_str(2)?,
        name: r.get_str(3)?,
        role,
        avatar: r.get_opt_str(5)?,
        revoked_at: r.get_opt_str(6)?,
        created_at: r.get_str(7)?,
    })
}

fn map_project_row(r: &DbRow) -> BoardResult<ProjectRecord> {
    Ok(ProjectRecord {
        id: r.get_str(0)?,
        name: r.get_str(1)?,
        revoked_at: r.get_opt_str(2)?,
        created_at: r.get_str(3)?,
    })
}

fn map_app_row(r: &DbRow) -> BoardResult<AppRecord> {
    let scopes_csv = r.get_str(3)?;
    let scopes = if scopes_csv.is_empty() {
        Permissions::NONE
    } else {
        Permissions::parse_spec_str(&scopes_csv).unwrap_or(Permissions::NONE)
    };
    Ok(AppRecord {
        id: r.get_str(0)?,
        slug: r.get_str(1)?,
        name: r.get_str(2)?,
        scopes,
        revoked_at: r.get_opt_str(4)?,
        created_at: r.get_str(5)?,
    })
}

fn map_token_summary_row(r: &DbRow) -> BoardResult<TokenSummary> {
    let kind = r.get_str(1)?;
    let pid = r.get_str(2)?;
    let env = r.get_opt_str(3)?;
    let principal = format_principal_str(&kind, &pid, env.as_deref());
    Ok(TokenSummary {
        id: r.get_str(0)?,
        principal,
        principal_kind: kind,
        principal_id: pid,
        environment: env,
        label: r.get_opt_str(4)?,
        revoked_at: r.get_opt_str(5)?,
        created_at: r.get_str(6)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_isolation_and_bare_principal_ownership() {
        let user = Principal::User {
            id: "usr_1".to_string(),
            username: "ada".to_string(),
            email: "ada@acme.com".to_string(),
            name: "Ada".to_string(),
            role: TeamRole::Member,
            avatar: None,
        };
        let bare = CallerContext {
            principal: user.clone(),
            profile: None,
            narrowing: None,
            sandboxed: false,
        };
        let fx = CallerContext {
            principal: user,
            profile: Some(ProfileOverlay {
                name: "Fx".to_string(),
                name_lower: "fx".to_string(),
                avatar: Some("https://example.com/fx.png".to_string()),
            }),
            narrowing: None,
            sandboxed: false,
        };

        assert_eq!(bare.actor_id(), "user:usr_1");
        assert_eq!(bare.handle(), "ada");
        assert_eq!(fx.actor_id(), "user:usr_1+fx");
        assert_eq!(fx.handle(), "ada/fx");

        assert!(bare.owns_actor_item("user:usr_1"));
        assert!(bare.owns_actor_item("user:usr_1+fx"));
        assert!(bare.owns_actor_item("user:usr_1+codex"));
        assert!(!bare.owns_actor_item("user:usr_2"));

        assert!(fx.owns_actor_item("user:usr_1+fx"));
        assert!(!fx.owns_actor_item("user:usr_1+codex"));
        assert!(!fx.owns_actor_item("user:usr_1"));
    }

    #[test]
    fn parses_profile_and_sandboxed_headers() {
        let p = parse_profile_headers(Some("Fx"), None, Some("https://a.test/x.png"))
            .unwrap()
            .unwrap();
        assert_eq!(p.name, "Fx");
        assert_eq!(p.name_lower, "fx");
        assert_eq!(p.avatar.as_deref(), Some("https://a.test/x.png"));

        assert!(parse_profile_headers(Some("Fx"), Some("fx"), None).is_ok());
        assert!(parse_profile_headers(Some("fx"), Some("codex"), None).is_err());
        assert!(parse_profile_headers(None, None, Some("https://a.test/x.png")).is_err());
        assert!(parse_profile_headers(Some("1bad"), None, None).is_err());

        assert!(!parse_sandboxed_header(None).unwrap());
        assert!(parse_sandboxed_header(Some("true")).unwrap());
        assert!(parse_sandboxed_header(Some("1")).unwrap());
        assert!(!parse_sandboxed_header(Some("false")).unwrap());
        assert!(parse_sandboxed_header(Some("maybe")).is_err());
    }

    #[tokio::test]
    async fn bootstrap_and_token_auth_lifecycle() {
        let db = Database::open_in_memory().await.unwrap();
        let tok = bootstrap_owner(&db, None)
            .await
            .unwrap()
            .expect("initial token");
        assert!(tok.starts_with("ob_"));

        assert!(bootstrap_owner(&db, None).await.unwrap().is_none());

        let principal = authenticate_bearer_token(&db, &tok).await.unwrap();
        assert!(principal.is_owner());
        assert_eq!(principal.handle(), "admin");

        let u = create_user(
            &db,
            CreateUserInput {
                username: "ada".to_string(),
                email: "ada@acme.com".to_string(),
                name: Some("Ada Lovelace".to_string()),
                role: Some(TeamRole::Developer),
                avatar: None,
            },
        )
        .await
        .unwrap();
        let u_tok = create_token(
            &db,
            CreateTokenInput {
                principal_kind: "user".to_string(),
                principal_id: u.id.clone(),
                environment: None,
                label: Some("cli".to_string()),
            },
        )
        .await
        .unwrap();
        let auth_u = authenticate_bearer_token(&db, &u_tok.token).await.unwrap();
        assert_eq!(auth_u.principal_id(), format!("user:{}", u.id));
        assert_eq!(auth_u.role(), Some(TeamRole::Developer));

        let p = create_project(
            &db,
            CreateProjectInput {
                name: "support".to_string(),
            },
        )
        .await
        .unwrap();
        let p_tok = create_token(
            &db,
            CreateTokenInput {
                principal_kind: "project".to_string(),
                principal_id: p.id.clone(),
                environment: Some("preview".to_string()),
                label: None,
            },
        )
        .await
        .unwrap();
        let auth_p = authenticate_bearer_token(&db, &p_tok.token).await.unwrap();
        assert_eq!(auth_p.principal_id(), format!("project:{}:preview", p.id));
        assert_eq!(auth_p.display_name(), "support (preview)");

        let a = create_app(
            &db,
            CreateAppInput {
                slug: "triage-bot".to_string(),
                name: Some("Triage Bot".to_string()),
                scopes: Permissions::READ.union(Permissions::LABEL),
            },
        )
        .await
        .unwrap();
        let a_tok = create_token(
            &db,
            CreateTokenInput {
                principal_kind: "app".to_string(),
                principal_id: a.id.clone(),
                environment: None,
                label: None,
            },
        )
        .await
        .unwrap();
        let auth_a = authenticate_bearer_token(&db, &a_tok.token).await.unwrap();
        assert_eq!(auth_a.principal_id(), format!("app:{}", a.id));
        assert_eq!(
            auth_a.permission_ceiling(),
            Permissions::READ.union(Permissions::LABEL)
        );

        revoke_token(&db, &u_tok.id).await.unwrap();
        assert!(authenticate_bearer_token(&db, &u_tok.token).await.is_err());

        revoke_project(&db, &p.id).await.unwrap();
        assert!(authenticate_bearer_token(&db, &p_tok.token).await.is_err());
    }
}
