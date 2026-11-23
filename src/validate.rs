use crate::error::{BoardError, BoardResult, ErrorCode};

pub const MAX_BOARD_NAME_CHARS: usize = 64;
pub const MAX_PROFILE_NAME_LEN: usize = 32;
pub const MAX_LABEL_KEY_LEN: usize = 64;
pub const MAX_LABEL_VALUE_BYTES: usize = 256;
pub const MAX_TEXT_CHARS: usize = 10_000;
pub const MAX_AVATAR_URL_LEN: usize = 2048;

/// Board name: 1-64 chars, no `/`, no control characters, non-blank.
pub fn validate_board_name(name: &str) -> BoardResult<()> {
    let len = name.chars().count();
    if len == 0 || len > MAX_BOARD_NAME_CHARS {
        return Err(BoardError::bad_request(format!(
            "board name must be between 1 and {MAX_BOARD_NAME_CHARS} characters"
        )));
    }
    if name.trim().is_empty() {
        return Err(BoardError::bad_request("board name cannot be blank"));
    }
    if name.contains('/') {
        return Err(BoardError::bad_request("board name cannot contain '/'"));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(BoardError::bad_request(
            "board name cannot contain control characters",
        ));
    }
    Ok(())
}

/// Channel path: one or more `[a-z0-9_-]+` segments joined by `/`.
pub fn validate_channel_path(path: &str) -> BoardResult<()> {
    if path.is_empty() {
        return Err(BoardError::bad_request("channel path cannot be empty"));
    }
    for seg in path.split('/') {
        if seg.is_empty() {
            return Err(BoardError::bad_request(format!(
                "invalid channel path '{path}': empty segment"
            )));
        }
        if !seg
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
        {
            return Err(BoardError::bad_request(format!(
                "invalid channel path '{path}': segments must match [a-z0-9_-]+"
            )));
        }
    }
    Ok(())
}

/// Channel pattern: `*`, `<channel_path>/` (subtree), or `<channel_path>` (exact).
pub fn validate_channel_pattern(pattern: &str) -> BoardResult<()> {
    if pattern == "*" {
        return Ok(());
    }
    if let Some(prefix) = pattern.strip_suffix('/') {
        return validate_channel_path(prefix).map_err(|_| {
            BoardError::bad_request(format!("invalid channel pattern '{pattern}'"))
        });
    }
    validate_channel_path(pattern)
        .map_err(|_| BoardError::bad_request(format!("invalid channel pattern '{pattern}'")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PatternSpecificity {
    Wildcard,
    /// Subtree prefix length (longer prefix = more specific).
    Subtree(usize),
    Exact,
}

/// Returns specificity if `pattern` matches `channel`, or `None` if it does not match.
///
/// Subtree `prefix/` matches both `prefix` itself and any `prefix/<descendant>`,
/// while exact `prefix` matches only `prefix` and outranks `prefix/`.
pub fn match_channel_pattern(pattern: &str, channel: &str) -> Option<PatternSpecificity> {
    if pattern == "*" {
        return Some(PatternSpecificity::Wildcard);
    }
    if let Some(prefix) = pattern.strip_suffix('/') {
        if channel == prefix || channel.starts_with(pattern) {
            return Some(PatternSpecificity::Subtree(prefix.len()));
        }
        return None;
    }
    (pattern == channel).then_some(PatternSpecificity::Exact)
}

/// Returns specificity if grant pattern `grant_pat` applies to target pattern `target_pat`.
/// Used when checking permissions at a pattern level (e.g., `manage` at `*` or `security/`).
pub fn match_pattern_at_pattern(grant_pat: &str, target_pat: &str) -> Option<PatternSpecificity> {
    if target_pat == "*" {
        return (grant_pat == "*").then_some(PatternSpecificity::Wildcard);
    }
    if let Some(target_prefix) = target_pat.strip_suffix('/') {
        if grant_pat == "*" {
            return Some(PatternSpecificity::Wildcard);
        }
        if let Some(grant_prefix) = grant_pat.strip_suffix('/') {
            if target_prefix == grant_prefix || target_prefix.starts_with(grant_pat) {
                return Some(PatternSpecificity::Subtree(grant_prefix.len()));
            }
        }
        return None;
    }
    match_channel_pattern(grant_pat, target_pat)
}

/// Profile name: `^[a-zA-Z][a-zA-Z0-9-]{0,31}$`.
pub fn validate_profile_name(name: &str) -> BoardResult<()> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_PROFILE_NAME_LEN {
        return Err(BoardError::bad_request(format!(
            "profile name must be 1 to {MAX_PROFILE_NAME_LEN} characters"
        )));
    }
    if !bytes[0].is_ascii_alphabetic() {
        return Err(BoardError::bad_request(
            "profile name must start with an ASCII letter",
        ));
    }
    if !bytes[1..]
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
    {
        return Err(BoardError::bad_request(
            "profile name must contain only ASCII letters, digits, and hyphens",
        ));
    }
    Ok(())
}

/// Label key: `^[a-z0-9][a-z0-9_.-]{0,63}$`.
pub fn validate_label_key(key: &str) -> BoardResult<()> {
    let bytes = key.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_LABEL_KEY_LEN {
        return Err(BoardError::bad_request(format!(
            "label key must be 1 to {MAX_LABEL_KEY_LEN} characters"
        )));
    }
    if !(bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit()) {
        return Err(BoardError::bad_request(
            "label key must start with a lowercase letter or digit",
        ));
    }
    if !bytes[1..]
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'_' | b'.' | b'-'))
    {
        return Err(BoardError::bad_request(
            "label key must contain only lowercase letters, digits, '_', '.', and '-'",
        ));
    }
    Ok(())
}

/// Validates a user-writable label key (rejects reserved `board.*` namespace).
pub fn validate_writable_label_key(key: &str) -> BoardResult<()> {
    validate_label_key(key)?;
    if key.starts_with("board.") {
        return Err(BoardError::new(
            ErrorCode::ReservedLabel,
            format!("label key '{key}' is in the reserved 'board.' namespace"),
        ));
    }
    Ok(())
}

/// Label value: <= 256 bytes, no control characters.
pub fn validate_label_value(value: &str) -> BoardResult<()> {
    if value.len() > MAX_LABEL_VALUE_BYTES {
        return Err(BoardError::bad_request(format!(
            "label value must be at most {MAX_LABEL_VALUE_BYTES} bytes"
        )));
    }
    if value.chars().any(|c| c.is_control()) {
        return Err(BoardError::bad_request(
            "label value cannot contain control characters",
        ));
    }
    Ok(())
}

/// Message text length: <= 10,000 characters.
pub fn validate_text_length(text: &str) -> BoardResult<()> {
    if text.chars().count() > MAX_TEXT_CHARS {
        return Err(BoardError::bad_request(format!(
            "message text must be at most {MAX_TEXT_CHARS} characters"
        )));
    }
    Ok(())
}

/// Avatar URL: http(s) URL, <= 2048 bytes, no control chars or whitespace.
pub fn validate_avatar_url(url: &str) -> BoardResult<()> {
    if url.is_empty() || url.len() > MAX_AVATAR_URL_LEN {
        return Err(BoardError::bad_request(format!(
            "avatar URL must be 1 to {MAX_AVATAR_URL_LEN} bytes"
        )));
    }
    if !(url.starts_with("https://") || url.starts_with("http://")) {
        return Err(BoardError::bad_request(
            "avatar URL must start with http:// or https://",
        ));
    }
    if url.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(BoardError::bad_request(
            "avatar URL cannot contain whitespace or control characters",
        ));
    }
    Ok(())
}

/// Simple slug validation for usernames, project names, app slugs, environments.
pub fn validate_slug(kind: &str, value: &str) -> BoardResult<()> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return Err(BoardError::bad_request(format!(
            "{kind} must be 1 to 64 characters"
        )));
    }
    if !bytes
        .iter()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-' || *b == b'_')
    {
        return Err(BoardError::bad_request(format!(
            "{kind} must match [a-z0-9_-]+"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_board_names() {
        assert!(validate_board_name("General").is_ok());
        assert!(validate_board_name("security").is_ok());
        assert!(validate_board_name("team-ops 2026").is_ok());
        assert!(validate_board_name(&"a".repeat(64)).is_ok());

        assert!(validate_board_name("").is_err());
        assert!(validate_board_name("   ").is_err());
        assert!(validate_board_name(&"a".repeat(65)).is_err());
        assert!(validate_board_name("sec/ops").is_err());
        assert!(validate_board_name("bad\nname").is_err());
    }

    #[test]
    fn validates_channel_paths_and_patterns() {
        assert!(validate_channel_path("dev").is_ok());
        assert!(validate_channel_path("www/production").is_ok());
        assert!(validate_channel_path("errors-www/v2_beta").is_ok());

        assert!(validate_channel_path("").is_err());
        assert!(validate_channel_path("/dev").is_err());
        assert!(validate_channel_path("dev/").is_err());
        assert!(validate_channel_path("dev//ops").is_err());
        assert!(validate_channel_path("Dev").is_err());
        assert!(validate_channel_path("dev.ops").is_err());
        assert!(validate_channel_path("*").is_err());

        assert!(validate_channel_pattern("*").is_ok());
        assert!(validate_channel_pattern("security/").is_ok());
        assert!(validate_channel_pattern("security/incidents").is_ok());
        assert!(validate_channel_pattern("security/incidents/").is_ok());
        assert!(validate_channel_pattern("").is_err());
        assert!(validate_channel_pattern("/").is_err());
        assert!(validate_channel_pattern("security//").is_err());
        assert!(validate_channel_pattern("*/foo").is_err());
    }

    #[test]
    fn matches_channel_patterns_with_specificity() {
        assert_eq!(
            match_channel_pattern("*", "security/incidents"),
            Some(PatternSpecificity::Wildcard)
        );
        assert_eq!(
            match_channel_pattern("security/", "security"),
            Some(PatternSpecificity::Subtree(8))
        );
        assert_eq!(
            match_channel_pattern("security/", "security/incidents"),
            Some(PatternSpecificity::Subtree(8))
        );
        assert_eq!(match_channel_pattern("security/", "security2"), None);
        assert_eq!(
            match_channel_pattern("security/incidents", "security/incidents"),
            Some(PatternSpecificity::Exact)
        );
        assert_eq!(
            match_channel_pattern("security/incidents", "security/other"),
            None
        );

        assert!(PatternSpecificity::Exact > PatternSpecificity::Subtree(100));
        assert!(PatternSpecificity::Subtree(10) > PatternSpecificity::Subtree(3));
        assert!(PatternSpecificity::Subtree(1) > PatternSpecificity::Wildcard);
    }

    #[test]
    fn validates_profile_names() {
        assert!(validate_profile_name("fx").is_ok());
        assert!(validate_profile_name("A").is_ok());
        assert!(validate_profile_name("Fixer-1").is_ok());
        assert!(validate_profile_name(&format!("a{}", "b".repeat(31))).is_ok());

        assert!(validate_profile_name("").is_err());
        assert!(validate_profile_name("1fixer").is_err());
        assert!(validate_profile_name("-fixer").is_err());
        assert!(validate_profile_name("fixer_1").is_err());
        assert!(validate_profile_name("fixer.1").is_err());
        assert!(validate_profile_name(&format!("a{}", "b".repeat(32))).is_err());
    }

    #[test]
    fn validates_labels_and_text_length() {
        assert!(validate_label_key("status").is_ok());
        assert!(validate_label_key("0a_b.c-d").is_ok());
        assert!(validate_label_key(&"a".repeat(64)).is_ok());
        assert!(validate_label_key("").is_err());
        assert!(validate_label_key(&"a".repeat(65)).is_err());
        assert!(validate_label_key("Status").is_err());
        assert!(validate_label_key("-status").is_err());
        assert!(validate_label_key("_status").is_err());

        assert!(validate_writable_label_key("status").is_ok());
        assert_eq!(
            validate_writable_label_key("board.task").unwrap_err().code,
            ErrorCode::ReservedLabel
        );

        assert!(validate_label_value("").is_ok());
        assert!(validate_label_value("open").is_ok());
        assert!(validate_label_value(&"v".repeat(256)).is_ok());
        assert!(validate_label_value(&"v".repeat(257)).is_err());
        assert!(validate_label_value("bad\nval").is_err());
        assert!(validate_label_value("bad\x00val").is_err());

        assert!(validate_text_length("").is_ok());
        assert!(validate_text_length(&"x".repeat(10_000)).is_ok());
        assert!(validate_text_length(&"x".repeat(10_001)).is_err());
        // Multi-byte UTF-8 chars count by character count, not bytes
        assert!(validate_text_length(&"é".repeat(10_000)).is_ok());
        assert!(validate_text_length(&"é".repeat(10_001)).is_err());
    }
}
