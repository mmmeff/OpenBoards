use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{BoardError, BoardResult};
use crate::validate::{
    match_channel_pattern, match_pattern_at_pattern, validate_channel_pattern, PatternSpecificity,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    Read,
    Post,
    React,
    Label,
    Sync,
    Manage,
}

impl Permission {
    pub const ALL: [Permission; 6] = [
        Permission::Read,
        Permission::Post,
        Permission::React,
        Permission::Label,
        Permission::Sync,
        Permission::Manage,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Post => "post",
            Self::React => "react",
            Self::Label => "label",
            Self::Sync => "sync",
            Self::Manage => "manage",
        }
    }

    pub fn parse(s: &str) -> BoardResult<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "read" => Ok(Self::Read),
            "post" => Ok(Self::Post),
            "react" => Ok(Self::React),
            "label" => Ok(Self::Label),
            "sync" => Ok(Self::Sync),
            "manage" => Ok(Self::Manage),
            other => Err(BoardError::bad_request(format!(
                "unknown permission '{other}'; expected one of read, post, react, label, sync, manage"
            ))),
        }
    }

    const fn bit(self) -> u8 {
        match self {
            Self::Read => 1 << 0,
            Self::Post => 1 << 1,
            Self::React => 1 << 2,
            Self::Label => 1 << 3,
            Self::Sync => 1 << 4,
            Self::Manage => 1 << 5,
        }
    }
}

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Compact bitset of Board permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Permissions(u8);

impl Permissions {
    pub const NONE: Self = Self(0);
    pub const READ: Self = Self(Permission::Read.bit());
    pub const POST: Self = Self(Permission::Post.bit());
    pub const REACT: Self = Self(Permission::React.bit());
    pub const LABEL: Self = Self(Permission::Label.bit());
    pub const SYNC: Self = Self(Permission::Sync.bit());
    pub const MANAGE: Self = Self(Permission::Manage.bit());
    pub const ALL: Self = Self(
        Permission::Read.bit()
            | Permission::Post.bit()
            | Permission::React.bit()
            | Permission::Label.bit()
            | Permission::Sync.bit()
            | Permission::Manage.bit(),
    );

    pub const VIEWER: Self = Self(Permission::Read.bit() | Permission::React.bit());
    pub const DEFAULT_PROJECTS: Self = Self(
        Permission::Read.bit()
            | Permission::Post.bit()
            | Permission::React.bit()
            | Permission::Label.bit()
            | Permission::Sync.bit(),
    );
    pub const DEFAULT_APPS: Self = Self(
        Permission::Read.bit()
            | Permission::Post.bit()
            | Permission::React.bit()
            | Permission::Label.bit(),
    );

    pub const fn from_perm(p: Permission) -> Self {
        Self(p.bit())
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn has(self, p: Permission) -> bool {
        (self.0 & p.bit()) != 0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn intersect(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    pub fn insert(&mut self, p: Permission) {
        self.0 |= p.bit();
    }

    /// Blind intake is allowed with `post` alone.
    pub const fn can_post_new_thread(self) -> bool {
        self.has(Permission::Post)
    }

    /// Replying in a thread requires both `post` and `read`.
    pub const fn can_reply(self) -> bool {
        self.has(Permission::Post) && self.has(Permission::Read)
    }

    /// Creating a channel requires both `read` and `post`.
    pub const fn can_create_channel(self) -> bool {
        self.has(Permission::Read) && self.has(Permission::Post)
    }

    pub const fn can_read(self) -> bool {
        self.has(Permission::Read)
    }

    pub const fn can_react(self) -> bool {
        self.has(Permission::React)
    }

    pub const fn can_label(self) -> bool {
        self.has(Permission::Label)
    }

    pub const fn can_sync(self) -> bool {
        self.has(Permission::Sync)
    }

    pub const fn can_manage(self) -> bool {
        self.has(Permission::Manage)
    }

    pub fn to_vec(self) -> Vec<Permission> {
        Permission::ALL
            .into_iter()
            .filter(|&p| self.has(p))
            .collect()
    }

    pub fn to_csv(self) -> String {
        self.to_vec()
            .into_iter()
            .map(|p| p.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn parse_spec_str(raw: &str) -> BoardResult<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(BoardError::bad_request(
                "permission list cannot be empty; use 'none' for empty permissions",
            ));
        }
        match trimmed.to_ascii_lowercase().as_str() {
            "all" => return Ok(Self::ALL),
            "none" => return Ok(Self::NONE),
            _ => {}
        }
        let mut perms = Self::NONE;
        for part in trimmed.split(',') {
            let token = part.trim();
            if token.is_empty() {
                return Err(BoardError::bad_request(format!(
                    "invalid permission specification '{raw}'"
                )));
            }
            match token.to_ascii_lowercase().as_str() {
                "all" => perms = perms.union(Self::ALL),
                "none" => {}
                other => perms.insert(Permission::parse(other)?),
            }
        }
        Ok(perms)
    }

    pub fn from_iter_perms<I: IntoIterator<Item = Permission>>(iter: I) -> Self {
        let mut p = Self::NONE;
        for perm in iter {
            p.insert(perm);
        }
        p
    }
}

impl Serialize for Permissions {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_vec().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Permissions {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Str(String),
            List(Vec<Permission>),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Str(s) => Permissions::parse_spec_str(&s).map_err(serde::de::Error::custom),
            Raw::List(list) => Ok(Permissions::from_iter_perms(list)),
        }
    }
}

/// Per-request narrowing policy parsed from `X-Board-Can`.
///
/// Supports either:
/// - Comma-separated permissions (`read,post`, `all`, `none`) applied to `*`
/// - JSON object mapping channel patterns to permission specs (`{"dev/":["read","post"]}`)
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NarrowingPolicy {
    pub rules: BTreeMap<String, Permissions>,
}

impl NarrowingPolicy {
    pub fn parse_header(raw: &str) -> BoardResult<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(BoardError::bad_request(
                "X-Board-Can header cannot be empty",
            ));
        }
        if trimmed.starts_with('{') {
            let map: BTreeMap<String, Permissions> =
                serde_json::from_str(trimmed).map_err(|e| {
                    BoardError::bad_request(format!("invalid X-Board-Can JSON map: {e}"))
                })?;
            if map.is_empty() {
                return Err(BoardError::bad_request(
                    "X-Board-Can pattern map cannot be empty",
                ));
            }
            for pattern in map.keys() {
                validate_channel_pattern(pattern)?;
            }
            Ok(Self { rules: map })
        } else {
            let perms = Permissions::parse_spec_str(trimmed)?;
            let mut rules = BTreeMap::new();
            rules.insert("*".to_string(), perms);
            Ok(Self { rules })
        }
    }

    /// Resolves the narrowing cap for a specific channel using most-specific-pattern-wins.
    /// If no pattern in the narrowing policy matches `channel`, returns `Permissions::NONE`.
    pub fn cap_for_channel(&self, channel: &str) -> Permissions {
        let mut best: Option<(PatternSpecificity, Permissions)> = None;
        for (pattern, &perms) in &self.rules {
            if let Some(spec) = match_channel_pattern(pattern, channel) {
                match best {
                    None => best = Some((spec, perms)),
                    Some((best_spec, _)) if spec > best_spec => best = Some((spec, perms)),
                    Some((best_spec, cur)) if spec == best_spec => {
                        best = Some((spec, cur.union(perms)));
                    }
                    _ => {}
                }
            }
        }
        best.map(|(_, p)| p).unwrap_or(Permissions::NONE)
    }

    /// Resolves the narrowing cap at a channel pattern (e.g. `*` or `dev/`).
    pub fn cap_for_pattern(&self, target_pattern: &str) -> Permissions {
        let mut best: Option<(PatternSpecificity, Permissions)> = None;
        for (pattern, &perms) in &self.rules {
            if let Some(spec) = match_pattern_at_pattern(pattern, target_pattern) {
                match best {
                    None => best = Some((spec, perms)),
                    Some((best_spec, _)) if spec > best_spec => best = Some((spec, perms)),
                    Some((best_spec, cur)) if spec == best_spec => {
                        best = Some((spec, cur.union(perms)));
                    }
                    _ => {}
                }
            }
        }
        best.map(|(_, p)| p).unwrap_or(Permissions::NONE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_permission_specs() {
        assert_eq!(Permissions::parse_spec_str("all").unwrap(), Permissions::ALL);
        assert_eq!(Permissions::parse_spec_str("none").unwrap(), Permissions::NONE);
        assert_eq!(
            Permissions::parse_spec_str("read, post").unwrap(),
            Permissions::READ.union(Permissions::POST)
        );
        assert!(Permissions::parse_spec_str("").is_err());
        assert!(Permissions::parse_spec_str("fly").is_err());
    }

    #[test]
    fn enforces_reply_and_blind_intake_semantics() {
        let post_only = Permissions::POST;
        assert!(post_only.can_post_new_thread());
        assert!(!post_only.can_reply());
        assert!(!post_only.can_create_channel());

        let read_and_post = Permissions::READ.union(Permissions::POST);
        assert!(read_and_post.can_post_new_thread());
        assert!(read_and_post.can_reply());
        assert!(read_and_post.can_create_channel());
    }

    #[test]
    fn parses_and_evaluates_narrowing_headers() {
        let flat = NarrowingPolicy::parse_header("read,post").unwrap();
        assert_eq!(
            flat.cap_for_channel("dev"),
            Permissions::READ.union(Permissions::POST)
        );
        assert_eq!(
            flat.cap_for_pattern("*"),
            Permissions::READ.union(Permissions::POST)
        );

        let scoped =
            NarrowingPolicy::parse_header(r#"{"dev/":["read","post"],"dev/secret":"none"}"#)
                .unwrap();
        assert_eq!(
            scoped.cap_for_channel("dev/alerts"),
            Permissions::READ.union(Permissions::POST)
        );
        assert_eq!(scoped.cap_for_channel("dev/secret"), Permissions::NONE);
        assert_eq!(scoped.cap_for_channel("ops"), Permissions::NONE);
        assert_eq!(scoped.cap_for_pattern("*"), Permissions::NONE);
    }
}
