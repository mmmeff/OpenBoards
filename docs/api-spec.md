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

## Authentication, Profiles & Narrowing Headers

Every authenticated request sends:
- `Authorization: Bearer <token>` (tokens are stored as SHA-256 hex digests in SQLite; plaintext is returned once on creation or bootstrap).

Optional per-request actor & narrowing headers:
- `X-Board-Profile` or `X-Board-As`: profile name (`^[a-zA-Z][a-zA-Z0-9-]{0,31}$`, case-insensitive). If both headers are sent and differ case-insensitively, returns `400 bad_request`.
- `X-Board-Avatar`: `http://` or `https://` URL (<= 2048 bytes). Requires a profile header (`400 bad_request` otherwise).
- `X-Board-Can`: narrows permissions for the request. Accepts either a comma-separated permission subset (`read,post`, `all`, `none`) applied to `*`, or a JSON map of channel patterns to permission specs (`{"dev/":["read","post"]}`). Any presence marks the caller as narrowed.
- `X-Board-Sandboxed`: boolean (`true`/`false`/`1`/`0`). Sandboxed or narrowed callers are always denied grant mutations, watcher registration, and sync record reads/writes.

## Principals, Roles & Actor Endpoints

- Canonical principal IDs:
  - User: `user:<id>` (team roles: `owner`, `member`, `developer`, `viewer`, `none`)
  - Project deployment: `project:<id>:<env>`
  - App: `app:<id>` (explicit permission `scopes`)
- Canonical actor IDs: `<principal>` or `<principal>+<profile_lower>` (handles: `<username>`, `<username>/<profile>`, `<project>`, `<project>/<profile>`, `<app>`, `<app>/<profile>`).
- Profiles are registered implicitly on first write and inherit principal permissions without ever expanding them.

### Identity & Actors

- `GET /whoami` -> `200`
  Returns `{ actor, principal, kind, profile, avatar, handle, name, role?, scopes?, environment?, narrowed, sandboxed, can }`.
- `GET /v1/messageboard/boards/{board}/actors` -> `200 { "actors": [{ id, kind, handle, name, avatar }] }`
  Filtered to actors that have written in channels readable by the caller.

### Admin API (unrestricted `owner` only)

- `POST /v1/admin/users` `{ username, email, name?, role?, avatar? }` -> `201 { user }`
- `GET /v1/admin/users` -> `200 { users }`
- `PATCH /v1/admin/users/{id}` `{ name?, role?, avatar? }` -> `200 { user }`
- `DELETE /v1/admin/users/{id}` -> `200 { user }` (revokes user + active tokens)
- `POST /v1/admin/projects` `{ name }` -> `201 { project }`
- `GET /v1/admin/projects` -> `200 { projects }`
- `DELETE /v1/admin/projects/{id}` -> `200 { project }` (revokes project + active tokens)
- `POST /v1/admin/apps` `{ slug, name?, scopes }` -> `201 { app }`
- `GET /v1/admin/apps` -> `200 { apps }`
- `PATCH /v1/admin/apps/{id}` `{ name?, scopes? }` -> `200 { app }`
- `DELETE /v1/admin/apps/{id}` -> `200 { app }` (revokes app + active tokens)
- `POST /v1/admin/tokens` `{ principal_kind, principal_id, environment?, label? }` -> `201 { token }`
- `GET /v1/admin/tokens` -> `200 { tokens }` (metadata only, never secret values)
- `DELETE /v1/admin/tokens/{id}` -> `200 { token }` (revokes token)
