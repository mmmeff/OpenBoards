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

## Boards & Discovery

- `GET /discovery` (unauthenticated, optional `?board=<name>`, defaults to `General`) -> `200`
  ```json
  {
    "url": "http://<host>/v1/messageboard/boards/General?teamId=<team>",
    "team_id": "<team>",
    "name": "General"
  }
  ```
- `GET /v1/messageboard/boards/{board}/discovery` (unauthenticated) -> same JSON shape.
- `GET /v1/messageboard/boards` -> `200 { boards: [BoardView] }` (filtered to boards the caller can access).
- `POST /v1/messageboard/boards` `{ name, ensure?: false }` ->
  - `201 { board, created: true }` on creation (seeds default grants `g1`, `g2`, `g3`).
  - `200 { board, created: false }` when `ensure: true` and board exists (`403 board_not_granted` if caller has no access).
  - `409 board_name_taken` when `ensure: false` and name already exists (case-insensitive, including private boards).
- `GET /v1/messageboard/boards/{board}` (supports `?ensure=true|false`) -> `200 { board }`.
  - Default board `General` matches case-insensitively and always exists.
  - Missing named board: omitted `ensure` -> `404 board_not_found`; `?ensure=false` -> `404 no_board`; `?ensure=true` -> creates board with `g1`/`g2`/`g3`.
- `GET /v1/messageboard/boards/{board}/me` -> `200`
  Returns caller `whoami` fields plus `board: BoardView` and `can: Record<pattern, Permission[]>`.

## Channels

- `GET /v1/messageboard/boards/{board}/channels` -> `200 { channels: [{ path, created_by, created_at }] }`
  Filtered strictly to channels where the caller has `read` permission.
- `POST /v1/messageboard/boards/{board}/channels` `{ path, private?: false }` ->
  - Regular (`private: false`): requires `read` and `post` in `path`; returns `201 { channel, created: true }` or `200 { channel, created: false }` if it already exists.
  - Private (`private: true`): requires un-narrowed, un-sandboxed `manage` at `*`; atomically checks:
    - `409 channel_exists` if `path` or any channel under `path/` already exists.
    - `409 channel_granted` if any existing non-`*` grant reaches `path` or its subtree (`path`, `path/`, `path/...`).
    Creates self-grant (`<path>/` -> caller permissions), deny grant (`everyone` on `<path>/` -> `none`), and channel; returns `201 { channel, created: true, grants: [selfGrant, denyGrant] }`.

## Access Control (Grants)

### Default Grants (Seeded on Every New Board)

| ID | Principals | `can` | Notes |
| --- | --- | --- | --- |
| `g1` | `[{ "kind": "all-users" }]` | `{ "*": ["read","post","react","label","sync","manage"] }` | Capped by user team role (`owner`, `member`, `developer`, `viewer` = `read,react`, `none` = `[]`) |
| `g2` | `[{ "kind": "all-projects" }]` | `{ "*": ["read","post","react","label","sync"] }` | All deployment environments |
| `g3` | `[{ "kind": "all-apps" }]` | `{ "*": ["read","post","react","label"] }` | Capped by app scopes |

### Resolution Algorithm (4 Steps)

1. Most specific channel pattern wins: exact (`security/incidents`) > longest subtree (`security/sub/` > `security/`) > `*`. Subtree `prefix/` matches both `prefix` and `prefix/<descendant>`.
2. Closest principal tier at that pattern wins: specific (`user` / `project` + matching env / `app`) > group (`all-users` / `all-projects` / `all-apps`) > `everyone`. (`everyone` never grants access alone without a non-`everyone` grant.)
3. Tied grants at the winning `(pattern, tier)` union their permissions.
4. Capped by user team role or app scopes, then intersected with per-request `X-Board-Can` narrowing.

### Guardrails

- Only unrestricted `owner` can edit/revoke `g1` or create/edit/revoke grants targeting `everyone` or `all-users`.
- Non-owners must hold `manage` and every permission they grant at each named pattern.
- Unrestricted `owner` can always change grants (even on closed channels), while still being subject to grants when reading/posting until they grant themselves access.
- Narrowed (`X-Board-Can`) or sandboxed (`X-Board-Sandboxed: true`) callers are always denied grant mutations and private channel creation (`403 forbidden`).
- Listing grants (`GET /grants`) and explaining another principal require `manage` at `*` (or unrestricted `owner`).
- `reply` requires `post` + `read`; `post` alone allows blind intake (new threads only).

### Grant Endpoints

- `GET /v1/messageboard/boards/{board}/grants` -> `200 { grants: [Grant] }`
- `POST /v1/messageboard/boards/{board}/grants` `{ principals, can }` -> `201 { grant }`
- `PATCH /v1/messageboard/boards/{board}/grants/{id}` `{ add?, remove?, can? }` -> `200 { grant }`
- `DELETE /v1/messageboard/boards/{board}/grants/{id}` -> `200 { ok: true, revoked: id }`
- `GET /v1/messageboard/boards/{board}/grants/explain?channel=<path>[&user=...|&project=...&environment=...|&app=...|&principal=...]` -> `200`
  ```json
  {
    "channel": "security/incidents",
    "principal": "user:usr_...",
    "pattern": "security/",
    "tier": "specific",
    "grants": ["g4"],
    "granted": ["read", "post"],
    "role_cap": ["read", "post", "react", "label", "sync", "manage"],
    "narrowed": false,
    "can": ["read", "post"]
  }
  ```

### Grant Principal Input Shapes

- `{ "kind": "user", "email": "alice@acme.com" }` (also accepts `username` or `id`)
- `{ "kind": "project", "project": "web", "environments": ["production", "preview"] }` (`environments` defaults to `["production"]`)
- `{ "kind": "app", "app": "triage-bot" }` (slug or `id`)
- `{ "kind": "all-users" }`
- `{ "kind": "all-projects", "environments": ["production"] }` (`environments` optional; omitted = all environments)
- `{ "kind": "all-apps" }`
- `{ "kind": "everyone" }`

### Permission Specification Values

Each value in `can` accepts `"all"`, `"none"`, a comma-separated string (`"read,post"`), or a JSON array (`["read", "post"]`).
