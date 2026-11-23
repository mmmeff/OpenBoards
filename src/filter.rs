use std::collections::BTreeMap;

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

use crate::actors::{self, ActorView, CallerContext};
use crate::error::{BoardError, BoardResult};
use crate::validate::{validate_channel_path, validate_channel_pattern, validate_label_key};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LabelPredicate {
    /// Label key must exist with any value (`k`).
    Presence(String),
    /// Label key must exist with one of the listed values (`k=v` or `k=v|w`).
    Equals(String, Vec<String>),
}

impl LabelPredicate {
    pub fn parse(raw: &str) -> BoardResult<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(BoardError::bad_request("label filter cannot be empty"));
        }
        if let Some((key, val_part)) = trimmed.split_once('=') {
            validate_label_key(key)?;
            let values: Vec<String> = val_part.split('|').map(str::to_string).collect();
            if values.is_empty() {
                return Err(BoardError::bad_request(format!(
                    "invalid label filter '{raw}'"
                )));
            }
            Ok(Self::Equals(key.to_string(), values))
        } else {
            validate_label_key(trimmed)?;
            Ok(Self::Presence(trimmed.to_string()))
        }
    }

    pub fn matches_map(&self, map: &BTreeMap<String, String>) -> bool {
        match self {
            Self::Presence(k) => map.contains_key(k),
            Self::Equals(k, vals) => map.get(k).is_some_and(|v| vals.iter().any(|cand| cand == v)),
        }
    }

    /// Matches if the message's own labels OR its thread root's labels satisfy this predicate.
    pub fn matches_with_root(
        &self,
        msg_labels: &BTreeMap<String, String>,
        root_labels: Option<&BTreeMap<String, String>>,
    ) -> bool {
        if self.matches_map(msg_labels) {
            return true;
        }
        root_labels.is_some_and(|rl| self.matches_map(rl))
    }
}

/// Shared query filter reusable across `messages`, `threads`, `events`/`poll`/`wait`, `watchers`, and `tasks`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct QueryFilter {
    pub channel: Option<String>,
    pub re: Option<String>,
    pub by: Option<String>,
    pub mentions: Option<String>,
    pub labels: Vec<LabelPredicate>,
    pub from: Option<String>,
    pub text: Option<String>,
}

/// Immutable snapshot of a message (plus its thread root's labels) for filter evaluation,
/// including before-or-after mutation matching in the event log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageSnapshot {
    pub id: String,
    pub channel: String,
    pub re: Option<String>,
    pub text: String,
    pub by_actor: String,
    pub by_principal: String,
    pub by_handle: String,
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub root_labels: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub mention_ids: Vec<String>,
    pub from: Option<String>,
    pub deleted: bool,
}

#[derive(Debug, Clone)]
pub struct FilterContext {
    pub caller_actor_id: String,
    pub caller_principal_id: String,
    pub caller_has_profile: bool,
    /// Pre-resolved actor IDs matching `filter.mentions` (if `filter.mentions` is set).
    pub mentions_target_ids: Option<Vec<String>>,
}

impl FilterContext {
    pub fn build(
        conn: &Connection,
        caller: &CallerContext,
        visible_actors: &[ActorView],
        filter: &QueryFilter,
    ) -> BoardResult<Self> {
        let mentions_target_ids = match &filter.mentions {
            None => None,
            Some(raw) => Some(resolve_mentions_filter_targets(
                conn,
                caller,
                visible_actors,
                raw,
            )?),
        };
        Ok(Self {
            caller_actor_id: caller.actor_id(),
            caller_principal_id: caller.principal_id(),
            caller_has_profile: caller.profile.is_some(),
            mentions_target_ids,
        })
    }
}

fn resolve_mentions_filter_targets(
    conn: &Connection,
    caller: &CallerContext,
    visible_actors: &[ActorView],
    raw: &str,
) -> BoardResult<Vec<String>> {
    let trimmed = raw.trim().trim_start_matches('@');
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("me") {
        return Ok(vec![caller.actor_id()]);
    }
    if let Some(inner) = trimmed.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        return Ok(vec![inner.to_string()]);
    }
    if trimmed.contains(':') {
        return Ok(vec![trimmed.to_string()]);
    }
    // Check email lookup (only return if actor is visible or is caller)
    if trimmed.contains('@') {
        if let Some(u) = actors::lookup_user_by_identifier(conn, trimmed)? {
            let pid = format!("user:{}", u.id);
            if pid == caller.principal_id() || visible_actors.iter().any(|a| a.id == pid) {
                return Ok(vec![pid]);
            }
        }
        return Ok(Vec::new());
    }
    // Match against visible actors by handle (and also check caller's own handle)
    let mut ids: Vec<String> = visible_actors
        .iter()
        .filter(|a| a.handle.eq_ignore_ascii_case(trimmed))
        .map(|a| a.id.clone())
        .collect();
    if caller.handle().eq_ignore_ascii_case(trimmed) && !ids.contains(&caller.actor_id()) {
        ids.push(caller.actor_id());
    }
    Ok(ids)
}

impl QueryFilter {
    pub fn validate(&self) -> BoardResult<()> {
        if let Some(ch) = &self.channel {
            if ch == "*" {
                // wildcard allowed in event/watcher filters
            } else if ch.ends_with('/') {
                validate_channel_pattern(ch)?;
            } else {
                validate_channel_path(ch)?;
            }
        }
        Ok(())
    }

    pub fn has_message_specific_filters(&self) -> bool {
        self.re.is_some()
            || self.by.is_some()
            || self.mentions.is_some()
            || !self.labels.is_empty()
            || self.from.is_some()
            || self.text.is_some()
    }

    pub fn matches_channel(&self, channel: &str) -> bool {
        let Some(filter_ch) = &self.channel else {
            return true;
        };
        if filter_ch == "*" {
            return true;
        }
        if let Some(prefix) = filter_ch.strip_suffix('/') {
            channel == prefix || channel.starts_with(filter_ch)
        } else {
            channel == filter_ch
        }
    }

    pub fn matches_snapshot(&self, snap: &MessageSnapshot, ctx: &FilterContext) -> bool {
        if !self.matches_channel(&snap.channel) {
            return false;
        }

        if let Some(re_filter) = &self.re {
            let root = snap.re.as_deref().unwrap_or(&snap.id);
            if root != re_filter {
                return false;
            }
        }

        if let Some(by_filter) = &self.by {
            if !matches_by_filter(by_filter, ctx, snap) {
                return false;
            }
        }

        if let Some(targets) = &ctx.mentions_target_ids {
            if targets.is_empty()
                || !snap.mention_ids.iter().any(|mid| targets.contains(mid))
            {
                return false;
            }
        }

        for pred in &self.labels {
            if !pred.matches_with_root(&snap.labels, snap.root_labels.as_ref()) {
                return false;
            }
        }

        if let Some(from_filter) = &self.from {
            if from_filter.eq_ignore_ascii_case("board") {
                if snap.from.is_some() {
                    return false;
                }
            } else if snap.from.as_deref() != Some(from_filter.as_str()) {
                return false;
            }
        }

        if let Some(needle) = &self.text {
            let needle_lower = needle.to_ascii_lowercase();
            if !snap.text.to_ascii_lowercase().contains(&needle_lower) {
                return false;
            }
        }

        true
    }
}

fn matches_by_filter(by_filter: &str, ctx: &FilterContext, snap: &MessageSnapshot) -> bool {
    let by_filter = by_filter.trim().trim_start_matches('@');
    if by_filter.eq_ignore_ascii_case("me") {
        if ctx.caller_has_profile {
            return snap.by_actor == ctx.caller_actor_id;
        }
        return snap.by_principal == ctx.caller_principal_id;
    }
    if by_filter.contains(':') {
        if by_filter.contains('+') {
            return snap.by_actor == by_filter;
        }
        return snap.by_principal == by_filter;
    }
    if by_filter.contains('/') {
        return snap.by_handle.eq_ignore_ascii_case(by_filter);
    }
    let base_handle = snap
        .by_handle
        .split_once('/')
        .map(|(base, _)| base)
        .unwrap_or(snap.by_handle.as_str());
    base_handle.eq_ignore_ascii_case(by_filter)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(channel: &str, labels: &[(&str, &str)], root_labels: Option<&[(&str, &str)]>) -> MessageSnapshot {
        MessageSnapshot {
            id: "msg_2".to_string(),
            channel: channel.to_string(),
            re: root_labels.map(|_| "msg_1".to_string()),
            text: "Hello World from Fixer".to_string(),
            by_actor: "user:u1+fx".to_string(),
            by_principal: "user:u1".to_string(),
            by_handle: "ada/fx".to_string(),
            labels: labels
                .iter()
                .map(|&(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            root_labels: root_labels.map(|rl| {
                rl.iter()
                    .map(|&(k, v)| (k.to_string(), v.to_string()))
                    .collect()
            }),
            mention_ids: vec!["user:u2".to_string()],
            from: None,
            deleted: false,
        }
    }

    fn base_ctx(mentions_targets: Option<Vec<String>>) -> FilterContext {
        FilterContext {
            caller_actor_id: "user:u2".to_string(),
            caller_principal_id: "user:u2".to_string(),
            caller_has_profile: false,
            mentions_target_ids: mentions_targets,
        }
    }

    #[test]
    fn label_predicates_support_presence_exact_alternation_and_root_inheritance() {
        let p_pres = LabelPredicate::parse("status").unwrap();
        let p_exact = LabelPredicate::parse("status=open").unwrap();
        let p_alt = LabelPredicate::parse("status=open|draft").unwrap();
        let p_bug = LabelPredicate::parse("kind=bug").unwrap();

        // Reply carries kind=bug, root carries status=draft
        let s = snap("www/prod", &[("kind", "bug")], Some(&[("status", "draft")]));

        let filter = QueryFilter {
            channel: Some("www/".to_string()),
            labels: vec![p_pres, p_alt, p_bug],
            text: Some("fixer".to_string()),
            from: Some("board".to_string()),
            by: Some("ada".to_string()),
            ..Default::default()
        };
        let ctx = base_ctx(Some(vec!["user:u2".to_string()]));
        assert!(filter.matches_snapshot(&s, &ctx));

        // Exact status=open does NOT match status=draft
        let filter_open = QueryFilter {
            labels: vec![p_exact],
            ..Default::default()
        };
        assert!(!filter_open.matches_snapshot(&s, &ctx));
    }
}
