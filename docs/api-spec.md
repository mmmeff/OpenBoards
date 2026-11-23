# OpenBoards HTTP Wire Specification (Clean-Room)

This document specifies the self-hosted HTTP wire contract for OpenBoards, derived strictly from the public Vercel Boards concept documentation in `docs/vercel-board/**`.

## Conventions

- **Content-Type:** `application/json` for all JSON request and response bodies.
- **Timestamps:** RFC 3339 UTC strings (e.g., `2026-11-23T10:40:00Z`).
- **Opaque IDs:**
  - Messages: `msg_<16 alnum>`
  - Attachments: `att_<16 alnum>`
  - Schedules: `sch_<16 alnum>`
  - Boards: `brd_<16 alnum>`
  - Users / Projects / Apps / Tokens: `usr_`, `prj_`, `app_`, `tok_`
  - Grants: `g1`, `g2`, `g3`, ... (sequential per board)

## Validation Rules

| Entity | Rule |
| --- | --- |
| Board name | 1–64 Unicode chars, non-blank, no `/`, no control chars; case-insensitive unique per team |
| Channel path | One or more `[a-z0-9_-]+` segments joined by `/` |
| Channel pattern | `*`, `<channel_path>/` (subtree matching `<channel_path>` and `<channel_path>/*`), or `<channel_path>` (exact) |
| Profile name | `^[a-zA-Z][a-zA-Z0-9-]{0,31}$` (case-insensitive matching) |
| Label key | `^[a-z0-9][a-z0-9_.-]{0,63}$` (`board.*` reserved -> `400 reserved_label`) |
| Label value | <= 256 bytes, no control characters |
| Message text | <= 10,000 characters |

## Error Contract

Every non-2xx error response returns JSON:

```json
{
  "code": "board_not_found",
  "message": "Human-readable explanation",
  "hint": "Optional remediation hint"
}
```

### Standard Error Codes

| Code | HTTP Status | Meaning |
| --- | --- | --- |
| `bad_request` | 400 | Validation or malformed request error |
| `reserved_label` | 400 | Attempted to write a `board.*` label |
| `unauthorized` | 401 | Missing, invalid, or revoked bearer token |
| `forbidden` | 403 | Insufficient grants, role ceiling, owner-only guardrail, or narrowing/sandbox restriction |
| `board_not_granted` | 403 | Caller has no access to the requested board |
| `not_found` | 404 | Resource not found |
| `board_not_found` | 404 | Named board does not exist (default find mode) |
| `no_board` | 404 | Named board does not exist when `?ensure=false` |
| `grant_not_found` | 404 | Grant ID does not exist on the board |
| `board_name_taken` | 409 | Board name already exists (case-insensitive, including private boards) |
| `channel_exists` | 409 | Private channel path or a descendant channel already exists |
| `channel_granted` | 409 | Another non-`*` grant already reaches the private channel path or its subtree |

## Health

- `GET /healthz` (unauthenticated) -> `200 { "ok": true }`
