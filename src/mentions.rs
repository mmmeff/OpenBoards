use serde::{Deserialize, Serialize};

use crate::actors::ActorView;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MentionActorRef {
    pub id: String,
    pub handle: String,
    pub name: String,
}

impl From<&ActorView> for MentionActorRef {
    fn from(a: &ActorView) -> Self {
        Self {
            id: a.id.clone(),
            handle: a.handle.clone(),
            name: a.name.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MentionResolution {
    pub written: String,
    pub actor: Option<MentionActorRef>,
    pub candidates: Vec<MentionActorRef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedMentions {
    pub text: String,
    pub actor_ids: Vec<String>,
    pub mentions: Vec<MentionResolution>,
}

/// Resolves `@handle` and `@[actor-id]` mentions in Markdown `text` against `visible_actors`,
/// skipping inline code spans and fenced code blocks.
///
/// - Unique match -> stored as `@[actor-id]`
/// - Board actors take precedence over synced platform users (`kind == "synced"`)
/// - Unmatched or ambiguous -> left as plain text with `candidates` populated; never fails writes
pub fn resolve_mentions(text: &str, visible_actors: &[ActorView]) -> ResolvedMentions {
    let mut out = String::with_capacity(text.len());
    let mut actor_ids: Vec<String> = Vec::new();
    let mut mentions: Vec<MentionResolution> = Vec::new();

    let mut in_fence: Option<(char, usize)> = None;
    let mut first_line = true;

    for line in text.split('\n') {
        if !first_line {
            out.push('\n');
        }
        first_line = false;

        let trimmed_start = line.trim_start_matches(' ');
        let leading_spaces = line.len() - trimmed_start.len();

        if let Some((fence_ch, fence_len)) = in_fence {
            if leading_spaces <= 3 {
                let count = trimmed_start
                    .chars()
                    .take_while(|&c| c == fence_ch)
                    .count();
                if count >= fence_len && trimmed_start[count..].trim().is_empty() {
                    in_fence = None;
                }
            }
            out.push_str(line);
            continue;
        }

        if leading_spaces <= 3 {
            if let Some(first_ch) = trimmed_start.chars().next() {
                if first_ch == '`' || first_ch == '~' {
                    let count = trimmed_start
                        .chars()
                        .take_while(|&c| c == first_ch)
                        .count();
                    let rest = &trimmed_start[count..];
                    // Backtick info strings cannot contain backticks in CommonMark
                    if count >= 3 && (first_ch == '~' || !rest.contains('`')) {
                        in_fence = Some((first_ch, count));
                        out.push_str(line);
                        continue;
                    }
                }
            }
        }

        resolve_line(line, visible_actors, &mut out, &mut actor_ids, &mut mentions);
    }

    ResolvedMentions {
        text: out,
        actor_ids,
        mentions,
    }
}

fn resolve_line(
    line: &str,
    visible_actors: &[ActorView],
    out: &mut String,
    actor_ids: &mut Vec<String>,
    mentions: &mut Vec<MentionResolution>,
) {
    let bytes = line.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        // Inline code span: match exact backtick run length
        if bytes[i] == b'`' {
            let start = i;
            while i < bytes.len() && bytes[i] == b'`' {
                i += 1;
            }
            let tick_len = i - start;
            if let Some(close_end) = find_closing_backticks(bytes, i, tick_len) {
                out.push_str(&line[start..close_end]);
                i = close_end;
            } else {
                // Unclosed backticks are literal text
                out.push_str(&line[start..i]);
            }
            continue;
        }

        if bytes[i] == b'@' && is_valid_mention_boundary(bytes, i) {
            // Explicit @[actor-id]
            if i + 1 < bytes.len() && bytes[i + 1] == b'[' {
                if let Some(rel_close) = line[i + 2..].find(']') {
                    let raw_id = &line[i + 2..i + 2 + rel_close];
                    if !raw_id.is_empty() && !raw_id.chars().any(char::is_whitespace) {
                        let written = &line[i..i + 2 + rel_close + 1];
                        if let Some(found) = visible_actors.iter().find(|a| a.id == raw_id) {
                            out.push_str(written);
                            push_unique(actor_ids, found.id.clone());
                            mentions.push(MentionResolution {
                                written: written.to_string(),
                                actor: Some(MentionActorRef::from(found)),
                                candidates: Vec::new(),
                            });
                        } else {
                            out.push_str(written);
                            mentions.push(MentionResolution {
                                written: written.to_string(),
                                actor: None,
                                candidates: Vec::new(),
                            });
                        }
                        i = i + 2 + rel_close + 1;
                        continue;
                    }
                }
            }

            // Handle @name or @name/profile
            if let Some(end) = scan_handle(bytes, i + 1) {
                let handle = &line[i + 1..end];
                let written = &line[i..end];
                let (resolved, candidates) = match_handle(handle, visible_actors);
                match resolved {
                    Some(actor) => {
                        out.push_str("@[");
                        out.push_str(&actor.id);
                        out.push(']');
                        push_unique(actor_ids, actor.id.clone());
                        mentions.push(MentionResolution {
                            written: written.to_string(),
                            actor: Some(MentionActorRef::from(actor)),
                            candidates: Vec::new(),
                        });
                    }
                    None => {
                        out.push_str(written);
                        mentions.push(MentionResolution {
                            written: written.to_string(),
                            actor: None,
                            candidates,
                        });
                    }
                }
                i = end;
                continue;
            }
        }

        let ch = line[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
}

fn find_closing_backticks(bytes: &[u8], mut pos: usize, tick_len: usize) -> Option<usize> {
    while pos < bytes.len() {
        if bytes[pos] == b'`' {
            let run_start = pos;
            while pos < bytes.len() && bytes[pos] == b'`' {
                pos += 1;
            }
            if pos - run_start == tick_len {
                return Some(pos);
            }
        } else {
            pos += 1;
        }
    }
    None
}

fn is_valid_mention_boundary(bytes: &[u8], at_idx: usize) -> bool {
    if at_idx == 0 {
        return true;
    }
    let prev = bytes[at_idx - 1];
    !(prev.is_ascii_alphanumeric() || matches!(prev, b'_' | b'-' | b'.' | b'/'))
}

fn scan_handle(bytes: &[u8], start: usize) -> Option<usize> {
    if start >= bytes.len() {
        return None;
    }
    if !(bytes[start].is_ascii_alphanumeric() || bytes[start] == b'_') {
        return None;
    }
    let mut pos = start;
    while pos < bytes.len()
        && (bytes[pos].is_ascii_alphanumeric() || matches!(bytes[pos], b'_' | b'-'))
    {
        pos += 1;
    }
    // Optional `/profile` segment
    if pos + 1 < bytes.len()
        && bytes[pos] == b'/'
        && (bytes[pos + 1].is_ascii_alphanumeric() || bytes[pos + 1] == b'_')
    {
        pos += 1;
        while pos < bytes.len()
            && (bytes[pos].is_ascii_alphanumeric() || matches!(bytes[pos], b'_' | b'-'))
        {
            pos += 1;
        }
    }
    Some(pos)
}

fn match_handle<'a>(
    handle: &str,
    visible_actors: &'a [ActorView],
) -> (Option<&'a ActorView>, Vec<MentionActorRef>) {
    let all_matches: Vec<&ActorView> = visible_actors
        .iter()
        .filter(|a| a.handle.eq_ignore_ascii_case(handle))
        .collect();

    if all_matches.is_empty() {
        return (None, Vec::new());
    }

    // Board actors take precedence over synced platform users
    let board_matches: Vec<&ActorView> = all_matches
        .iter()
        .copied()
        .filter(|a| a.kind != "synced")
        .collect();

    let effective = if !board_matches.is_empty() {
        board_matches
    } else {
        all_matches
    };

    if effective.len() == 1 {
        (Some(effective[0]), Vec::new())
    } else {
        (
            None,
            effective.into_iter().map(MentionActorRef::from).collect(),
        )
    }
}

fn push_unique(list: &mut Vec<String>, item: String) {
    if !list.contains(&item) {
        list.push(item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(id: &str, kind: &str, handle: &str, name: &str) -> ActorView {
        ActorView {
            id: id.to_string(),
            kind: kind.to_string(),
            handle: handle.to_string(),
            name: name.to_string(),
            avatar: None,
        }
    }

    #[test]
    fn resolves_unique_ambiguous_precedence_and_skips_code() {
        let visible = vec![
            actor("user:u1", "user", "ada", "Ada"),
            actor("user:u1+fx", "profile", "ada/fx", "Fx"),
            actor("project:p1:production", "project", "support", "support"),
            actor("project:p1:preview", "project", "support", "support (preview)"),
            actor("project:p1:production+slack:user:U1", "synced", "ada", "Ada Slack"),
        ];

        // Board actor `ada` beats synced user `ada`; `@ada/fx` resolves; email ignored
        let res = resolve_mentions("Hi @ada and @ada/fx (mail bob@acme.com)", &visible);
        assert_eq!(
            res.text,
            "Hi @[user:u1] and @[user:u1+fx] (mail bob@acme.com)"
        );
        assert_eq!(res.actor_ids, vec!["user:u1", "user:u1+fx"]);
        assert_eq!(res.mentions.len(), 2);
        assert_eq!(res.mentions[0].actor.as_ref().unwrap().id, "user:u1");

        // Ambiguous `@support` stays plain text with both candidates; explicit `@[...]` works
        let res = resolve_mentions(
            "ping @support or @[project:p1:preview] and @ghost",
            &visible,
        );
        assert_eq!(
            res.text,
            "ping @support or @[project:p1:preview] and @ghost"
        );
        assert_eq!(res.actor_ids, vec!["project:p1:preview"]);
        assert!(res.mentions[0].actor.is_none());
        assert_eq!(res.mentions[0].candidates.len(), 2);
        assert_eq!(
            res.mentions[1].actor.as_ref().unwrap().id,
            "project:p1:preview"
        );
        assert!(res.mentions[2].actor.is_none());
        assert!(res.mentions[2].candidates.is_empty());

        // Code spans and fenced blocks are skipped
        let md = "inline `@ada` and ``@ada/fx``\n```\n@ada\n```\noutside @ada";
        let res = resolve_mentions(md, &visible);
        assert_eq!(
            res.text,
            "inline `@ada` and ``@ada/fx``\n```\n@ada\n```\noutside @[user:u1]"
        );
        assert_eq!(res.mentions.len(), 1);
    }
}
