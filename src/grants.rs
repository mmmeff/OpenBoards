use std::collections::BTreeMap;

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::actors::{self, CallerContext, Principal};
use crate::db::{Database, DbTx, Queryable};
use crate::error::{BoardError, BoardResult, ErrorCode};
use crate::id::format_grant_id;
use crate::permissions::{NarrowingPolicy, Permissions};
use crate::sql_args;
use crate::validate::{
    match_channel_pattern, match_pattern_at_pattern, validate_channel_path,
    validate_channel_pattern, validate_slug, PatternSpecificity,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GrantPrincipal {
    User {
        id: String,
    },
    Project {
        id: String,
        environments: Vec<String>,
    },
    App {
        id: String,
    },
    AllUsers,
    AllProjects {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        environments: Option<Vec<String>>,
    },
    AllApps,
    Everyone,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GrantPrincipalInput {
    User {
        id: Option<String>,
        email: Option<String>,
        username: Option<String>,
        user: Option<String>,
    },
    Project {
        id: Option<String>,
        project: Option<String>,
        environments: Option<Vec<String>>,
    },
    App {
        id: Option<String>,
        app: Option<String>,
    },
    AllUsers,
    AllProjects {
        environments: Option<Vec<String>>,
    },
    AllApps,
    Everyone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrincipalTier {
    Everyone = 1,
    Group = 2,
    Specific = 3,
}

impl GrantPrincipal {
    pub fn from_caller_principal(principal: &Principal) -> Self {
        match principal {
            Principal::User { id, .. } => Self::User { id: id.clone() },
            Principal::Project {
                id, environment, ..
            } => Self::Project {
                id: id.clone(),
                environments: vec![environment.clone()],
            },
            Principal::App { id, .. } => Self::App { id: id.clone() },
        }
    }

    pub fn is_owner_only_target(&self) -> bool {
        matches!(self, Self::Everyone | Self::AllUsers)
    }

    pub fn match_tier(&self, principal: &Principal) -> Option<PrincipalTier> {
        match (self, principal) {
            (Self::User { id }, Principal::User { id: pid, .. }) if id == pid => {
                Some(PrincipalTier::Specific)
            }
            (
                Self::Project { id, environments },
                Principal::Project {
                    id: pid,
                    environment,
                    ..
                },
            ) if id == pid && environments.iter().any(|e| e == environment) => {
                Some(PrincipalTier::Specific)
            }
            (Self::App { id }, Principal::App { id: aid, .. }) if id == aid => {
                Some(PrincipalTier::Specific)
            }
            (Self::AllUsers, Principal::User { .. }) => Some(PrincipalTier::Group),
            (
                Self::AllProjects { environments },
                Principal::Project { environment, .. },
            ) => match environments {
                None => Some(PrincipalTier::Group),
                Some(envs) if envs.iter().any(|e| e == environment) => Some(PrincipalTier::Group),
                _ => None,
            },
            (Self::AllApps, Principal::App { .. }) => Some(PrincipalTier::Group),
            (Self::Everyone, _) => Some(PrincipalTier::Everyone),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    pub id: String,
    pub seq: i64,
    pub principals: Vec<GrantPrincipal>,
    pub can: BTreeMap<String, Permissions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl Grant {
    pub fn match_tier(&self, principal: &Principal) -> Option<PrincipalTier> {
        self.principals
            .iter()
            .filter_map(|gp| gp.match_tier(principal))
            .max()
    }

    pub fn targets_owner_only_principal(&self) -> bool {
        self.principals.iter().any(GrantPrincipal::is_owner_only_target)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantExplanation {
    pub channel: String,
    pub principal: String,
    pub pattern: Option<String>,
    pub tier: Option<PrincipalTier>,
    pub grants: Vec<String>,
    pub granted: Permissions,
    pub role_cap: Permissions,
    pub narrowed: bool,
    pub can: Permissions,
}

#[derive(Debug, Clone)]
struct Candidate<'a> {
    grant_id: &'a str,
    pattern: &'a str,
    specificity: PatternSpecificity,
    tier: PrincipalTier,
    perms: Permissions,
}

fn resolve_candidates(
    candidates: Vec<Candidate<'_>>,
    principal: &Principal,
    narrowing: Option<&NarrowingPolicy>,
    target: &str,
    is_pattern_target: bool,
) -> GrantExplanation {
    let role_cap = principal.permission_ceiling();
    let has_non_everyone_access = candidates
        .iter()
        .any(|c| c.tier != PrincipalTier::Everyone && !c.perms.is_empty());

    let max_spec = candidates.iter().map(|c| c.specificity).max();
    let Some(best_spec) = max_spec else {
        return GrantExplanation {
            channel: target.to_string(),
            principal: principal.principal_id(),
            pattern: None,
            tier: None,
            grants: Vec::new(),
            granted: Permissions::NONE,
            role_cap,
            narrowed: narrowing.is_some(),
            can: Permissions::NONE,
        };
    };

    let at_best_spec: Vec<&Candidate<'_>> = candidates
        .iter()
        .filter(|c| c.specificity == best_spec)
        .collect();

    let best_tier = at_best_spec
        .iter()
        .map(|c| c.tier)
        .max()
        .expect("at_best_spec is non-empty");

    let winners: Vec<&Candidate<'_>> = at_best_spec
        .into_iter()
        .filter(|c| c.tier == best_tier)
        .collect();

    let winning_pattern = winners.first().map(|c| c.pattern.to_string());
    let mut winning_grants: Vec<String> = Vec::new();
    let mut granted = Permissions::NONE;

    if best_tier == PrincipalTier::Everyone && !has_non_everyone_access {
        granted = Permissions::NONE;
    } else {
        for w in &winners {
            granted = granted.union(w.perms);
            if !winning_grants.iter().any(|id| id == w.grant_id) {
                winning_grants.push(w.grant_id.to_string());
            }
        }
    }

    let capped = granted.intersect(role_cap);
    let effective = match narrowing {
        Some(policy) => {
            let cap = if is_pattern_target {
                policy.cap_for_pattern(target)
            } else {
                policy.cap_for_channel(target)
            };
            capped.intersect(cap)
        }
        None => capped,
    };

    GrantExplanation {
        channel: target.to_string(),
        principal: principal.principal_id(),
        pattern: winning_pattern,
        tier: Some(best_tier),
        grants: winning_grants,
        granted,
        role_cap,
        narrowed: narrowing.is_some(),
        can: effective,
    }
}

/// 4-step grant resolution for a caller (with optional narrowing) on a specific channel path.
pub fn resolve_for_channel(
    grants: &[Grant],
    principal: &Principal,
    narrowing: Option<&NarrowingPolicy>,
    channel: &str,
) -> GrantExplanation {
    let mut candidates = Vec::new();
    for grant in grants {
        let Some(tier) = grant.match_tier(principal) else {
            continue;
        };
        for (pattern, &perms) in &grant.can {
            if let Some(specificity) = match_channel_pattern(pattern, channel) {
                candidates.push(Candidate {
                    grant_id: &grant.id,
                    pattern,
                    specificity,
                    tier,
                    perms,
                });
            }
        }
    }
    resolve_candidates(candidates, principal, narrowing, channel, false)
}

/// 4-step grant resolution for a caller at a channel pattern (`*`, `prefix/`, or exact).
pub fn resolve_for_pattern(
    grants: &[Grant],
    principal: &Principal,
    narrowing: Option<&NarrowingPolicy>,
    target_pattern: &str,
) -> GrantExplanation {
    let mut candidates = Vec::new();
    for grant in grants {
        let Some(tier) = grant.match_tier(principal) else {
            continue;
        };
        for (pattern, &perms) in &grant.can {
            if let Some(specificity) = match_pattern_at_pattern(pattern, target_pattern) {
                candidates.push(Candidate {
                    grant_id: &grant.id,
                    pattern,
                    specificity,
                    tier,
                    perms,
                });
            }
        }
    }
    resolve_candidates(candidates, principal, narrowing, target_pattern, true)
}

/// Convenience helper for resolving a `CallerContext` on a channel.
pub fn caller_permissions_on_channel(
    grants: &[Grant],
    caller: &CallerContext,
    channel: &str,
) -> Permissions {
    resolve_for_channel(grants, &caller.principal, caller.narrowing.as_ref(), channel).can
}

/// Convenience helper for resolving a `CallerContext` at a pattern.
pub fn caller_permissions_at_pattern(
    grants: &[Grant],
    caller: &CallerContext,
    pattern: &str,
) -> Permissions {
    resolve_for_pattern(grants, &caller.principal, caller.narrowing.as_ref(), pattern).can
}

/// Returns true if the caller has `read` permission on every channel of the board
/// (i.e. `read` at `*` and no grant or narrowing rule denies `read` on any pattern).
pub fn caller_can_read_all_channels(grants: &[Grant], caller: &CallerContext) -> bool {
    if !caller_permissions_at_pattern(grants, caller, "*").can_read() {
        return false;
    }
    for grant in grants {
        for pattern in grant.can.keys() {
            if !caller_permissions_at_pattern(grants, caller, pattern).can_read() {
                return false;
            }
        }
    }
    if let Some(narrowing) = &caller.narrowing {
        for pattern in narrowing.rules.keys() {
            if !caller_permissions_at_pattern(grants, caller, pattern).can_read() {
                return false;
            }
        }
    }
    true
}

/// Returns true if the caller has any non-empty permission anywhere on the board.
pub fn caller_has_any_board_access(grants: &[Grant], caller: &CallerContext) -> bool {
    if caller.is_unrestricted_owner() {
        return true;
    }
    if !caller_permissions_at_pattern(grants, caller, "*").is_empty() {
        return true;
    }
    for grant in grants {
        for pattern in grant.can.keys() {
            if !caller_permissions_at_pattern(grants, caller, pattern).is_empty() {
                return true;
            }
        }
    }
    if let Some(narrowing) = &caller.narrowing {
        for pattern in narrowing.rules.keys() {
            if !caller_permissions_at_pattern(grants, caller, pattern).is_empty() {
                return true;
            }
        }
    }
    false
}

/// Computes the caller's effective `can` map across `*` and all relevant patterns on a board.
pub fn summarize_caller_can(
    grants: &[Grant],
    caller: &CallerContext,
) -> BTreeMap<String, Permissions> {
    let mut patterns: Vec<String> = vec!["*".to_string()];
    for grant in grants {
        for pat in grant.can.keys() {
            if !patterns.contains(pat) {
                patterns.push(pat.clone());
            }
        }
    }
    if let Some(narrowing) = &caller.narrowing {
        for pat in narrowing.rules.keys() {
            if !patterns.contains(pat) {
                patterns.push(pat.clone());
            }
        }
    }
    let mut out = BTreeMap::new();
    for pat in patterns {
        let perms = caller_permissions_at_pattern(grants, caller, &pat);
        out.insert(pat, perms);
    }
    out
}

/// Seeds default grants `g1`, `g2`, `g3` for a newly created board inside a transaction.
pub async fn seed_default_grants_tx(tx: &mut DbTx, board_id: &str) -> BoardResult<Vec<Grant>> {
    let mut seeded = Vec::with_capacity(3);

    let mut g1_can = BTreeMap::new();
    g1_can.insert("*".to_string(), Permissions::ALL);
    seeded.push(
        insert_grant_tx(tx, board_id, vec![GrantPrincipal::AllUsers], g1_can, None).await?,
    );

    let mut g2_can = BTreeMap::new();
    g2_can.insert("*".to_string(), Permissions::DEFAULT_PROJECTS);
    seeded.push(
        insert_grant_tx(
            tx,
            board_id,
            vec![GrantPrincipal::AllProjects { environments: None }],
            g2_can,
            None,
        )
        .await?,
    );

    let mut g3_can = BTreeMap::new();
    g3_can.insert("*".to_string(), Permissions::DEFAULT_APPS);
    seeded.push(
        insert_grant_tx(tx, board_id, vec![GrantPrincipal::AllApps], g3_can, None).await?,
    );

    Ok(seeded)
}

pub async fn insert_grant_tx(
    tx: &mut DbTx,
    board_id: &str,
    principals: Vec<GrantPrincipal>,
    can: BTreeMap<String, Permissions>,
    created_by: Option<&str>,
) -> BoardResult<Grant> {
    let seq = tx
        .fetch_one(
            "UPDATE boards
             SET next_grant_seq = next_grant_seq + 1
             WHERE id = ?1
             RETURNING next_grant_seq - 1",
            sql_args![board_id],
        )
        .await?
        .get_i64(0)?;

    let id = format_grant_id(seq);
    let now = Utc::now().to_rfc3339();
    let principals_json = serde_json::to_string(&principals)
        .map_err(|e| BoardError::internal(format!("failed to serialize grant principals: {e}")))?;
    let can_json = serde_json::to_string(&can)
        .map_err(|e| BoardError::internal(format!("failed to serialize grant permissions: {e}")))?;

    tx.execute(
        "INSERT INTO grants (board_id, seq, id, principals_json, can_json, created_by, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
        sql_args![
            board_id,
            seq,
            &id,
            &principals_json,
            &can_json,
            created_by,
            &now
        ],
    )
    .await?;

    Ok(Grant {
        id,
        seq,
        principals,
        can,
        created_by: created_by.map(str::to_string),
        created_at: now.clone(),
        updated_at: now,
    })
}

pub async fn load_grants_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
) -> BoardResult<Vec<Grant>> {
    let rows = conn
        .fetch_all(
            "SELECT id, seq, principals_json, can_json, created_by, created_at, updated_at
             FROM grants
             WHERE board_id = ?1
             ORDER BY seq ASC",
            sql_args![board_id],
        )
        .await?;
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let id = r.get_str(0)?;
        let seq = r.get_i64(1)?;
        let p_json = r.get_str(2)?;
        let c_json = r.get_str(3)?;
        let created_by = r.get_opt_str(4)?;
        let created_at = r.get_str(5)?;
        let updated_at = r.get_str(6)?;
        let principals: Vec<GrantPrincipal> = serde_json::from_str(&p_json)
            .map_err(|e| BoardError::internal(format!("corrupted grant principals: {e}")))?;
        let can: BTreeMap<String, Permissions> = serde_json::from_str(&c_json)
            .map_err(|e| BoardError::internal(format!("corrupted grant can map: {e}")))?;
        out.push(Grant {
            id,
            seq,
            principals,
            can,
            created_by,
            created_at,
            updated_at,
        });
    }
    Ok(out)
}

pub async fn load_grants(db: &Database, board_id: &str) -> BoardResult<Vec<Grant>> {
    let mut conn = db.read_conn();
    load_grants_tx(&mut conn, board_id).await
}

pub async fn resolve_principal_input<C: Queryable + ?Sized>(
    conn: &mut C,
    input: GrantPrincipalInput,
) -> BoardResult<GrantPrincipal> {
    match input {
        GrantPrincipalInput::User {
            id,
            email,
            username,
            user,
        } => {
            let ident = id
                .or(email)
                .or(username)
                .or(user)
                .ok_or_else(|| {
                    BoardError::bad_request("user principal requires id, email, or username")
                })?;
            let rec = actors::lookup_user_by_identifier(conn, &ident)
                .await?
                .ok_or_else(|| BoardError::bad_request(format!("user '{ident}' not found")))?;
            Ok(GrantPrincipal::User { id: rec.id })
        }
        GrantPrincipalInput::Project {
            id,
            project,
            environments,
        } => {
            let ident = id.or(project).ok_or_else(|| {
                BoardError::bad_request("project principal requires project or id")
            })?;
            let rec = actors::lookup_project_by_identifier(conn, &ident)
                .await?
                .ok_or_else(|| BoardError::bad_request(format!("project '{ident}' not found")))?;
            let envs = match environments {
                Some(list) if !list.is_empty() => {
                    let mut normalized = Vec::with_capacity(list.len());
                    for e in list {
                        let clean = e.trim().to_ascii_lowercase();
                        validate_slug("environment", &clean)?;
                        if !normalized.contains(&clean) {
                            normalized.push(clean);
                        }
                    }
                    normalized
                }
                Some(_) => {
                    return Err(BoardError::bad_request(
                        "project environments list cannot be empty",
                    ));
                }
                None => vec!["production".to_string()],
            };
            Ok(GrantPrincipal::Project {
                id: rec.id,
                environments: envs,
            })
        }
        GrantPrincipalInput::App { id, app } => {
            let ident = id
                .or(app)
                .ok_or_else(|| BoardError::bad_request("app principal requires app or id"))?;
            let rec = actors::lookup_app_by_identifier(conn, &ident)
                .await?
                .ok_or_else(|| BoardError::bad_request(format!("app '{ident}' not found")))?;
            Ok(GrantPrincipal::App { id: rec.id })
        }
        GrantPrincipalInput::AllUsers => Ok(GrantPrincipal::AllUsers),
        GrantPrincipalInput::AllProjects { environments } => {
            let envs = match environments {
                Some(list) if !list.is_empty() => {
                    let mut normalized = Vec::with_capacity(list.len());
                    for e in list {
                        let clean = e.trim().to_ascii_lowercase();
                        validate_slug("environment", &clean)?;
                        if !normalized.contains(&clean) {
                            normalized.push(clean);
                        }
                    }
                    Some(normalized)
                }
                Some(_) => {
                    return Err(BoardError::bad_request(
                        "all-projects environments list cannot be empty",
                    ));
                }
                None => None,
            };
            Ok(GrantPrincipal::AllProjects { environments: envs })
        }
        GrantPrincipalInput::AllApps => Ok(GrantPrincipal::AllApps),
        GrantPrincipalInput::Everyone => Ok(GrantPrincipal::Everyone),
    }
}

fn validate_can_map(can: &BTreeMap<String, Permissions>) -> BoardResult<()> {
    if can.is_empty() {
        return Err(BoardError::bad_request(
            "grant 'can' map must include at least one channel pattern",
        ));
    }
    for pattern in can.keys() {
        validate_channel_pattern(pattern)?;
    }
    Ok(())
}

fn dedup_principals(principals: Vec<GrantPrincipal>) -> Vec<GrantPrincipal> {
    let mut out = Vec::with_capacity(principals.len());
    for p in principals {
        if !out.contains(&p) {
            out.push(p);
        }
    }
    out
}

pub fn require_list_grants_access(grants: &[Grant], caller: &CallerContext) -> BoardResult<()> {
    if caller.is_unrestricted_owner() {
        return Ok(());
    }
    if caller_permissions_at_pattern(grants, caller, "*").can_manage() {
        return Ok(());
    }
    Err(BoardError::forbidden(
        "listing grants requires 'manage' permission at '*'",
    ))
}

#[derive(Debug, Deserialize)]
pub struct CreateGrantInput {
    pub principals: Vec<GrantPrincipalInput>,
    pub can: BTreeMap<String, Permissions>,
}

pub async fn create_grant(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    input: CreateGrantInput,
) -> BoardResult<Grant> {
    if !caller.can_mutate_grants() {
        return Err(BoardError::forbidden(
            "narrowed or sandboxed callers cannot mutate grants",
        ));
    }
    if input.principals.is_empty() {
        return Err(BoardError::bad_request(
            "grant must specify at least one principal",
        ));
    }
    validate_can_map(&input.can)?;

    let mut tx = db.begin_tx().await?;
    let lock_sql = format!("SELECT id FROM boards WHERE id = ?1{}", tx.lock_row_suffix());
    let _ = tx.fetch_one(&lock_sql, sql_args![board_id]).await?;

    let existing = load_grants_tx(&mut tx, board_id).await?;
    let mut principals = Vec::with_capacity(input.principals.len());
    for p in input.principals {
        principals.push(resolve_principal_input(&mut tx, p).await?);
    }
    let principals = dedup_principals(principals);

    if !caller.is_unrestricted_owner() {
        if principals.iter().any(GrantPrincipal::is_owner_only_target) {
            return Err(BoardError::forbidden(
                "only team owners can create grants targeting 'everyone' or 'all-users'",
            ));
        }
        for (pattern, &perms) in &input.can {
            let caller_perms = caller_permissions_at_pattern(&existing, caller, pattern);
            if !caller_perms.can_manage() {
                return Err(BoardError::forbidden(format!(
                    "creating a grant on '{pattern}' requires 'manage' at '{pattern}'"
                )));
            }
            if !caller_perms.contains(perms) {
                return Err(BoardError::forbidden(format!(
                    "cannot grant permissions on '{pattern}' that you do not hold"
                )));
            }
        }
    }

    let actor_id = caller.actor_id();
    let grant = insert_grant_tx(&mut tx, board_id, principals, input.can, Some(&actor_id)).await?;
    tx.commit().await?;
    Ok(grant)
}

#[derive(Debug, Deserialize)]
pub struct UpdateGrantInput {
    pub add: Option<Vec<GrantPrincipalInput>>,
    pub remove: Option<Vec<GrantPrincipalInput>>,
    pub can: Option<BTreeMap<String, Permissions>>,
}

pub async fn update_grant(
    db: &Database,
    board_id: &str,
    grant_id: &str,
    caller: &CallerContext,
    input: UpdateGrantInput,
) -> BoardResult<Grant> {
    if !caller.can_mutate_grants() {
        return Err(BoardError::forbidden(
            "narrowed or sandboxed callers cannot mutate grants",
        ));
    }
    if input.add.is_none() && input.remove.is_none() && input.can.is_none() {
        return Err(BoardError::bad_request(
            "update must specify at least one of 'add', 'remove', or 'can'",
        ));
    }
    if let Some(can) = &input.can {
        validate_can_map(can)?;
    }

    let mut tx = db.begin_tx().await?;
    let lock_sql = format!("SELECT id FROM boards WHERE id = ?1{}", tx.lock_row_suffix());
    let _ = tx.fetch_one(&lock_sql, sql_args![board_id]).await?;

    let existing = load_grants_tx(&mut tx, board_id).await?;
    let mut target = existing
        .iter()
        .find(|g| g.id == grant_id)
        .cloned()
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::GrantNotFound,
                format!("grant '{grant_id}' not found"),
            )
        })?;

    let add_resolved = match input.add {
        Some(list) => {
            let mut v = Vec::with_capacity(list.len());
            for p in list {
                v.push(resolve_principal_input(&mut tx, p).await?);
            }
            v
        }
        None => Vec::new(),
    };
    let remove_resolved = match input.remove {
        Some(list) => {
            let mut v = Vec::with_capacity(list.len());
            for p in list {
                v.push(resolve_principal_input(&mut tx, p).await?);
            }
            v
        }
        None => Vec::new(),
    };

    let mut next_principals = target.principals.clone();
    for p in add_resolved {
        if !next_principals.contains(&p) {
            next_principals.push(p);
        }
    }
    next_principals.retain(|p| !remove_resolved.contains(p));
    if next_principals.is_empty() {
        return Err(BoardError::bad_request(
            "cannot remove all principals from a grant; revoke it instead",
        ));
    }

    let next_can = input.can.unwrap_or_else(|| target.can.clone());

    if !caller.is_unrestricted_owner() {
        if target.id == "g1" {
            return Err(BoardError::forbidden(
                "only team owners can edit default grant 'g1'",
            ));
        }
        if target.targets_owner_only_principal()
            || next_principals
                .iter()
                .any(GrantPrincipal::is_owner_only_target)
        {
            return Err(BoardError::forbidden(
                "only team owners can modify grants targeting 'everyone' or 'all-users'",
            ));
        }
        for pattern in target.can.keys() {
            if !caller_permissions_at_pattern(&existing, caller, pattern).can_manage() {
                return Err(BoardError::forbidden(format!(
                    "modifying this grant requires 'manage' at '{pattern}'"
                )));
            }
        }
        for (pattern, &perms) in &next_can {
            let caller_perms = caller_permissions_at_pattern(&existing, caller, pattern);
            if !caller_perms.can_manage() {
                return Err(BoardError::forbidden(format!(
                    "modifying a grant on '{pattern}' requires 'manage' at '{pattern}'"
                )));
            }
            if !caller_perms.contains(perms) {
                return Err(BoardError::forbidden(format!(
                    "cannot grant permissions on '{pattern}' that you do not hold"
                )));
            }
        }
    }

    let now = Utc::now().to_rfc3339();
    let principals_json = serde_json::to_string(&next_principals)
        .map_err(|e| BoardError::internal(format!("failed to serialize principals: {e}")))?;
    let can_json = serde_json::to_string(&next_can)
        .map_err(|e| BoardError::internal(format!("failed to serialize can map: {e}")))?;

    tx.execute(
        "UPDATE grants SET principals_json = ?1, can_json = ?2, updated_at = ?3
         WHERE board_id = ?4 AND id = ?5",
        sql_args![&principals_json, &can_json, &now, board_id, grant_id],
    )
    .await?;
    tx.commit().await?;

    target.principals = next_principals;
    target.can = next_can;
    target.updated_at = now;
    Ok(target)
}

pub async fn revoke_grant(
    db: &Database,
    board_id: &str,
    grant_id: &str,
    caller: &CallerContext,
) -> BoardResult<()> {
    if !caller.can_mutate_grants() {
        return Err(BoardError::forbidden(
            "narrowed or sandboxed callers cannot mutate grants",
        ));
    }

    let mut tx = db.begin_tx().await?;
    let lock_sql = format!("SELECT id FROM boards WHERE id = ?1{}", tx.lock_row_suffix());
    let _ = tx.fetch_one(&lock_sql, sql_args![board_id]).await?;

    let existing = load_grants_tx(&mut tx, board_id).await?;
    let target = existing
        .iter()
        .find(|g| g.id == grant_id)
        .ok_or_else(|| {
            BoardError::new(
                ErrorCode::GrantNotFound,
                format!("grant '{grant_id}' not found"),
            )
        })?;

    if !caller.is_unrestricted_owner() {
        if target.id == "g1" {
            return Err(BoardError::forbidden(
                "only team owners can revoke default grant 'g1'",
            ));
        }
        if target.targets_owner_only_principal() {
            return Err(BoardError::forbidden(
                "only team owners can revoke grants targeting 'everyone' or 'all-users'",
            ));
        }
        for pattern in target.can.keys() {
            if !caller_permissions_at_pattern(&existing, caller, pattern).can_manage() {
                return Err(BoardError::forbidden(format!(
                    "revoking this grant requires 'manage' at '{pattern}'"
                )));
            }
        }
    }

    tx.execute(
        "DELETE FROM grants WHERE board_id = ?1 AND id = ?2",
        sql_args![board_id, grant_id],
    )
    .await?;
    tx.commit().await
}

#[derive(Debug, Default, Deserialize)]
pub struct ExplainQuery {
    pub channel: String,
    pub user: Option<String>,
    pub project: Option<String>,
    pub environment: Option<String>,
    pub app: Option<String>,
    pub principal: Option<String>,
}

pub async fn explain_access(
    db: &Database,
    board_id: &str,
    caller: &CallerContext,
    query: ExplainQuery,
) -> BoardResult<GrantExplanation> {
    validate_channel_path(&query.channel)?;

    let explaining_other = query.user.is_some()
        || query.project.is_some()
        || query.app.is_some()
        || query.principal.is_some();

    let mut conn = db.read_conn();
    let grants = load_grants_tx(&mut conn, board_id).await?;

    if !explaining_other {
        return Ok(resolve_for_channel(
            &grants,
            &caller.principal,
            caller.narrowing.as_ref(),
            &query.channel,
        ));
    }

    if !caller.is_unrestricted_owner()
        && !caller_permissions_at_pattern(&grants, caller, "*").can_manage()
    {
        return Err(BoardError::forbidden(
            "explaining another actor's access requires 'manage' permission at '*'",
        ));
    }

    let target_principal = resolve_explain_target(&mut conn, &query).await?;
    Ok(resolve_for_channel(
        &grants,
        &target_principal,
        None,
        &query.channel,
    ))
}

async fn resolve_explain_target<C: Queryable + ?Sized>(
    conn: &mut C,
    query: &ExplainQuery,
) -> BoardResult<Principal> {
    let mut specified = 0;
    if query.user.is_some() {
        specified += 1;
    }
    if query.project.is_some() {
        specified += 1;
    }
    if query.app.is_some() {
        specified += 1;
    }
    if query.principal.is_some() {
        specified += 1;
    }
    if specified > 1 {
        return Err(BoardError::bad_request(
            "specify at most one of user, project, app, or principal",
        ));
    }

    if let Some(user_ident) = &query.user {
        let u = actors::lookup_user_by_identifier(conn, user_ident)
            .await?
            .ok_or_else(|| BoardError::not_found(format!("user '{user_ident}' not found")))?;
        return Ok(Principal::User {
            id: u.id,
            username: u.username,
            email: u.email,
            name: u.name,
            role: u.role,
            avatar: u.avatar,
        });
    }

    if let Some(proj_ident) = &query.project {
        let p = actors::lookup_project_by_identifier(conn, proj_ident)
            .await?
            .ok_or_else(|| BoardError::not_found(format!("project '{proj_ident}' not found")))?;
        let env = query
            .environment
            .as_deref()
            .unwrap_or("production")
            .trim()
            .to_ascii_lowercase();
        validate_slug("environment", &env)?;
        return Ok(Principal::Project {
            id: p.id,
            name: p.name,
            environment: env,
        });
    }

    if let Some(app_ident) = &query.app {
        let a = actors::lookup_app_by_identifier(conn, app_ident)
            .await?
            .ok_or_else(|| BoardError::not_found(format!("app '{app_ident}' not found")))?;
        return Ok(Principal::App {
            id: a.id,
            slug: a.slug,
            name: a.name,
            scopes: a.scopes,
        });
    }

    if let Some(raw) = &query.principal {
        if let Some(uid) = raw.strip_prefix("user:") {
            let u = actors::lookup_user_by_identifier(conn, uid)
                .await?
                .ok_or_else(|| BoardError::not_found(format!("user '{uid}' not found")))?;
            return Ok(Principal::User {
                id: u.id,
                username: u.username,
                email: u.email,
                name: u.name,
                role: u.role,
                avatar: u.avatar,
            });
        }
        if let Some(rest) = raw.strip_prefix("project:") {
            let (pid, env) = rest.split_once(':').ok_or_else(|| {
                BoardError::bad_request(
                    "project principal must have format 'project:<id>:<env>'",
                )
            })?;
            let p = actors::lookup_project_by_identifier(conn, pid)
                .await?
                .ok_or_else(|| BoardError::not_found(format!("project '{pid}' not found")))?;
            validate_slug("environment", env)?;
            return Ok(Principal::Project {
                id: p.id,
                name: p.name,
                environment: env.to_string(),
            });
        }
        if let Some(aid) = raw.strip_prefix("app:") {
            let a = actors::lookup_app_by_identifier(conn, aid)
                .await?
                .ok_or_else(|| BoardError::not_found(format!("app '{aid}' not found")))?;
            return Ok(Principal::App {
                id: a.id,
                slug: a.slug,
                name: a.name,
                scopes: a.scopes,
            });
        }
        return Err(BoardError::bad_request(format!(
            "invalid principal '{raw}'; expected user:<id>, project:<id>:<env>, or app:<id>"
        )));
    }

    unreachable!("checked explaining_other")
}

/// Returns true if any existing grant on the board has a non-`*` pattern that equals `path`,
/// equals `path/`, or lies inside `path/` (meaning it would still allow access after a private
/// channel's `everyone=none` grant on `path/`).
pub fn has_overlapping_grant_for_private_channel(grants: &[Grant], path: &str) -> bool {
    let subtree = format!("{path}/");
    for grant in grants {
        for pattern in grant.can.keys() {
            if pattern == path || pattern == &subtree || pattern.starts_with(&subtree) {
                return true;
            }
        }
    }
    false
}

/// Checks if a grant exists by ID on a board.
pub async fn grant_exists_tx<C: Queryable + ?Sized>(
    conn: &mut C,
    board_id: &str,
    grant_id: &str,
) -> BoardResult<bool> {
    Ok(conn
        .fetch_optional(
            "SELECT 1 FROM grants WHERE board_id = ?1 AND id = ?2",
            sql_args![board_id, grant_id],
        )
        .await?
        .is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::TeamRole;

    fn make_grant(id: &str, principals: Vec<GrantPrincipal>, can: &[(&str, Permissions)]) -> Grant {
        let mut map = BTreeMap::new();
        for &(pat, perms) in can {
            map.insert(pat.to_string(), perms);
        }
        Grant {
            id: id.to_string(),
            seq: 1,
            principals,
            can: map,
            created_by: None,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    fn user(id: &str, role: TeamRole) -> Principal {
        Principal::User {
            id: id.to_string(),
            username: id.to_string(),
            email: format!("{id}@acme.com"),
            name: id.to_string(),
            role,
            avatar: None,
        }
    }

    #[test]
    fn resolves_4_step_precedence_ties_roles_and_narrowing() {
        let alice = user("alice", TeamRole::Member);
        let bob = user("bob", TeamRole::Member);
        let viewer = user("eve", TeamRole::Viewer);
        let nobody = user("nope", TeamRole::None);
        let prod_proj = Principal::Project {
            id: "prj_1".to_string(),
            name: "web".to_string(),
            environment: "production".to_string(),
        };
        let preview_proj = Principal::Project {
            id: "prj_1".to_string(),
            name: "web".to_string(),
            environment: "preview".to_string(),
        };
        let app = Principal::App {
            id: "app_1".to_string(),
            slug: "bot".to_string(),
            name: "Bot".to_string(),
            scopes: Permissions::READ.union(Permissions::LABEL),
        };

        let grants = vec![
            make_grant("g1", vec![GrantPrincipal::AllUsers], &[("*", Permissions::ALL)]),
            make_grant(
                "g2",
                vec![GrantPrincipal::AllProjects { environments: None }],
                &[("*", Permissions::DEFAULT_PROJECTS)],
            ),
            make_grant(
                "g3",
                vec![GrantPrincipal::AllApps],
                &[("*", Permissions::DEFAULT_APPS)],
            ),
            make_grant("g4", vec![GrantPrincipal::Everyone], &[("security/", Permissions::NONE)]),
            make_grant(
                "g5",
                vec![GrantPrincipal::User { id: "alice".to_string() }],
                &[("security/", Permissions::READ)],
            ),
            make_grant(
                "g6",
                vec![GrantPrincipal::User { id: "alice".to_string() }],
                &[("security/", Permissions::POST)],
            ),
            make_grant(
                "g7",
                vec![GrantPrincipal::User { id: "bob".to_string() }],
                &[("security/public", Permissions::READ)],
            ),
            make_grant(
                "g8",
                vec![GrantPrincipal::Project {
                    id: "prj_1".to_string(),
                    environments: vec!["production".to_string()],
                }],
                &[("security/", Permissions::ALL)],
            ),
        ];

        let r = resolve_for_channel(&grants, &alice, None, "security/incidents");
        assert_eq!(r.pattern.as_deref(), Some("security/"));
        assert_eq!(r.tier, Some(PrincipalTier::Specific));
        assert_eq!(r.grants, vec!["g5", "g6"]);
        assert_eq!(r.can, Permissions::READ.union(Permissions::POST));
        assert!(r.can.can_reply());

        let r = resolve_for_channel(&grants, &bob, None, "security/incidents");
        assert_eq!(r.pattern.as_deref(), Some("security/"));
        assert_eq!(r.tier, Some(PrincipalTier::Everyone));
        assert_eq!(r.can, Permissions::NONE);

        let r = resolve_for_channel(&grants, &bob, None, "security/public");
        assert_eq!(r.pattern.as_deref(), Some("security/public"));
        assert_eq!(r.can, Permissions::READ);

        let r = resolve_for_channel(&grants, &viewer, None, "dev");
        assert_eq!(r.granted, Permissions::ALL);
        assert_eq!(r.can, Permissions::VIEWER);

        let r = resolve_for_channel(&grants, &nobody, None, "dev");
        assert_eq!(r.can, Permissions::NONE);

        assert_eq!(
            resolve_for_channel(&grants, &prod_proj, None, "security/alerts").can,
            Permissions::ALL
        );
        assert_eq!(
            resolve_for_channel(&grants, &preview_proj, None, "security/alerts").can,
            Permissions::NONE
        );

        let r = resolve_for_channel(&grants, &app, None, "dev");
        assert_eq!(r.can, Permissions::READ.union(Permissions::LABEL));

        let narrow = NarrowingPolicy::parse_header("post").unwrap();
        let r = resolve_for_channel(&grants, &alice, Some(&narrow), "security/incidents");
        assert_eq!(r.can, Permissions::POST);
        assert!(r.can.can_post_new_thread());
        assert!(!r.can.can_reply());
    }

    #[test]
    fn everyone_never_grants_access_alone() {
        let alice = user("alice", TeamRole::Member);
        let grants = vec![make_grant(
            "g4",
            vec![GrantPrincipal::Everyone],
            &[("*", Permissions::ALL)],
        )];
        let r = resolve_for_channel(&grants, &alice, None, "dev");
        assert_eq!(r.can, Permissions::NONE);
    }

    #[test]
    fn longer_subtree_beats_shorter_subtree() {
        let alice = user("alice", TeamRole::Member);
        let grants = vec![
            make_grant("g1", vec![GrantPrincipal::AllUsers], &[("*", Permissions::ALL)]),
            make_grant("g4", vec![GrantPrincipal::Everyone], &[("a/", Permissions::NONE)]),
            make_grant("g5", vec![GrantPrincipal::Everyone], &[("a/b/", Permissions::READ)]),
        ];
        assert_eq!(resolve_for_channel(&grants, &alice, None, "a/x").can, Permissions::NONE);
        assert_eq!(resolve_for_channel(&grants, &alice, None, "a/b/c").can, Permissions::READ);
        assert_eq!(resolve_for_channel(&grants, &alice, None, "a/b").can, Permissions::READ);
    }
}
