use rand::rngs::OsRng;
use rand::RngCore;
use sha2::{Digest, Sha256};

const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";

fn random_alnum(len: usize) -> String {
    let mut out = String::with_capacity(len);
    let mut buf = [0u8; 32];
    let mut rng = OsRng;
    while out.len() < len {
        rng.fill_bytes(&mut buf);
        for &b in &buf {
            if out.len() == len {
                break;
            }
            // Rejection sampling to avoid modulo bias (252 = 7 * 36)
            if b < 252 {
                out.push(ALPHABET[(b % 36) as usize] as char);
            }
        }
    }
    out
}

pub fn prefixed_id(prefix: &str) -> String {
    format!("{prefix}_{}", random_alnum(16))
}

pub fn new_message_id() -> String {
    prefixed_id("msg")
}

pub fn new_attachment_id() -> String {
    prefixed_id("att")
}

pub fn new_schedule_id() -> String {
    prefixed_id("sch")
}

pub fn new_board_id() -> String {
    prefixed_id("brd")
}

pub fn new_user_id() -> String {
    prefixed_id("usr")
}

pub fn new_project_id() -> String {
    prefixed_id("prj")
}

pub fn new_app_id() -> String {
    prefixed_id("app")
}

pub fn new_token_id() -> String {
    prefixed_id("tok")
}

pub fn new_bearer_token() -> String {
    format!("ob_{}", random_alnum(40))
}

pub fn hash_token(token: &str) -> String {
    let digest = Sha256::digest(token.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    hex
}

pub fn format_grant_id(seq: i64) -> String {
    format!("g{seq}")
}

pub fn parse_grant_id(id: &str) -> Option<i64> {
    let rest = id.strip_prefix('g')?;
    if rest.is_empty() || !rest.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: i64 = rest.parse().ok()?;
    (n >= 1).then_some(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn generates_prefixed_opaque_ids() {
        assert!(new_message_id().starts_with("msg_"));
        assert!(new_attachment_id().starts_with("att_"));
        assert!(new_schedule_id().starts_with("sch_"));
        assert!(new_board_id().starts_with("brd_"));
        assert!(new_user_id().starts_with("usr_"));
        assert!(new_project_id().starts_with("prj_"));
        assert!(new_app_id().starts_with("app_"));
        assert!(new_token_id().starts_with("tok_"));
        assert!(new_bearer_token().starts_with("ob_"));

        let ids: HashSet<String> = (0..100).map(|_| new_message_id()).collect();
        assert_eq!(ids.len(), 100);
        for id in &ids {
            assert_eq!(id.len(), 20);
            assert!(id[4..].bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()));
        }
    }

    #[test]
    fn formats_and_parses_grant_ids() {
        assert_eq!(format_grant_id(1), "g1");
        assert_eq!(format_grant_id(42), "g42");
        assert_eq!(parse_grant_id("g1"), Some(1));
        assert_eq!(parse_grant_id("g42"), Some(42));
        assert_eq!(parse_grant_id("g0"), None);
        assert_eq!(parse_grant_id("g-1"), None);
        assert_eq!(parse_grant_id("g"), None);
        assert_eq!(parse_grant_id("x1"), None);
        assert_eq!(parse_grant_id("g1a"), None);
    }

    #[test]
    fn hashes_tokens_deterministically_as_sha256_hex() {
        let h1 = hash_token("secret-token");
        let h2 = hash_token("secret-token");
        let h3 = hash_token("other-token");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
        assert_eq!(h1.len(), 64);
        assert!(h1.bytes().all(|b| b.is_ascii_hexdigit()));
    }
}
