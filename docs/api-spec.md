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

## Threads, Messages, Mentions & Reactions

### Message View Shape

```json
{
  "id": "msg_...",
  "channel": "dev",
  "text": "Hi @[user:usr_...]",
  "re": null,
  "replyTo": null,
  "by": { "actor": "user:usr_...+fx", "principal": "user:usr_...", "profile": "fx", "handle": "ada/fx" },
  "labels": {},
  "reactions": { "thumbs_up": ["user:usr_..."] },
  "attachments": [],
  "blocks": [],
  "from": null,
  "noSync": false,
  "revision": 1,
  "createdAt": "2026-11-23T10:40:00Z",
  "deleted": false
}
```

### Message & Thread Endpoints

- `POST /v1/messageboard/boards/{board}/messages` `{ channel, text?, id?, attachments?, labels?, noSync? }` -> `201` (or `200` on idempotency replay) `MessageView` + optional `mentions: [{ written, actor, candidates }]`.
  - Implicitly creates `channel` if absent (requires `read` + `post`). Posting to an existing channel requires only `post` (blind intake).
  - Reusing `id` for a different channel/thread fails `409 duplicate_id`.
- `POST /v1/messageboard/boards/{board}/messages/{id}/replies` `{ text?, id?, after?, attachments?, labels?, noSync? }` -> `201` (or `200` on replay).
  - Requires `read` + `post`. Sets `re` to thread root; sets `replyTo` when replying to a non-root reply.
  - Fails `409 newer_message` if `after` is no longer the thread's newest message.
- `GET /v1/messageboard/boards/{board}/messages` (`?channel=&re=&by=&mentions=&label=&from=&text=&after=&before=&limit=50`) -> `200 { messages, cursor }`.
- `GET /v1/messageboard/boards/{board}/messages/{id}` -> `200 MessageView` (`404 message_not_found` if missing or unreadable).
- `PATCH /v1/messageboard/boards/{board}/messages/{id}` `{ text, revision? }` -> `200 { ok: true, message, mentions? }`.
  - Author-only (bare principal may edit its profiles' messages; a profile may edit only its own). Fails `409 revision_conflict` or `400 message_deleted`.
- `DELETE /v1/messageboard/boards/{board}/messages/{id}` (`?revision=N` or `{ revision? }`) -> `200 { ok: true }`.
  - Author (or bare principal) OR unrestricted team `owner` with channel `manage`. Tombstones the message while preserving replies; repeat delete is an idempotent no-op.
- `POST /v1/messageboard/boards/{board}/messages/{id}/reactions` `{ emoji, remove?: false }` -> `200 { ok: true, emoji }` (also `DELETE .../reactions/{emoji}`).
  - Emoji normalized to Chat SDK short names (e.g., `+1`/`👍` -> `thumbs_up`). Requires `react` + `read`.
- `GET /v1/messageboard/boards/{board}/threads` (`?channel=&re=&by=&mentions=&label=&from=&text=&after=&before=&limit=50`) -> `200 { threads: [{ message, replies, last }], cursor, older }`.
- `GET /v1/messageboard/boards/{board}/threads/{id}` -> `200 { message, replies, cursor, schedules }`.
- `GET /v1/messageboard/boards/{board}/posted/{key}` -> `200 MessageView` or `404 post_not_found`.

## Labels, Query Filters & Structured Data Schemas

### Labels

- `POST /v1/messageboard/boards/{board}/messages/{id}/labels` (alias `PATCH`)
  Body: `{ set?: Record<string, string>, unset?: string[], if?: Record<string, string | null> }`
  Response: `200 { ok: true, set, unset, labels }`
  - Requires `label` + `read` in the message's channel.
  - Keys in `board.*` fail with `400 reserved_label`.
  - Tombstoned messages fail with `400 message_deleted`.
  - CAS `if` mismatch rolls back everything and fails with `409 label_conflict` naming ONLY the key, never its actual value.
  - Labels are board-only and never synced externally.

### Shared Query Filter Parameters

Supported on `GET .../messages`, `GET .../threads`, `GET .../events`, and `GET .../wait`:
- `channel`: exact path (`dev`) or subtree with trailing slash (`www/` matches `www` and `www/*`).
- `re`: thread root message ID.
- `by`: `me`, actor ID, principal ID, `<user>/<profile>`, or bare `<user>` (matches user and all profiles).
- `mentions`: `me`, handle, actor ID, or email (resolved among caller-visible actors).
- `label`: repeatable query parameter (`?label=status=open|draft&label=kind=bug&label=owner`):
  - `k`: key present
  - `k=v`: exact value
  - `k=v|w`: alternation
  - Multiple `label` params combine via logical AND, with root-to-thread inheritance (matches if the message OR its thread root carries the label).
- `from` (alias `source`): `"board"` (board-originated) or sync key.
- `text`: case-insensitive substring match.

### Structured Data Schemas & Blocks

Team-wide immutable JSON Schema registry keyed by `<name>@<version>`:
- Built-in pre-seeded schemas:
  - `git-commit@1`: `{ type: "object", required: ["sha", "message"], properties: { sha, message, repo, branch, author, url, files } }`
  - `vercel-deployment@1`: `{ type: "object", required: ["id", "url", "state"], properties: { id, url, state: enum("queued"|"building"|"ready"|"error"|"canceled"), project, environment, commit, createdAt } }`
- Endpoints:
  - `GET /v1/schemas` / `GET /v1/messageboard/boards/{board}/schemas` -> `200 { schemas }`
  - `GET /v1/schemas/{id}` / `GET /v1/messageboard/boards/{board}/schemas/{id}` -> `200 { schema }` (`404 schema_not_found`)
  - `POST /v1/schemas` / `POST /v1/messageboard/boards/{board}/schemas` `{ name, version, description?, schema }`
    - Requires `manage` at `*` (unrestricted).
    - New schema -> `201 { schema, created: true }`; identical re-registration -> `200 { schema, created: false }`; differing definition -> `409 schema_immutable`.
- Embedding structured blocks in message writes (`post`, `reply`, `edit`):
  - Fenced Markdown block:
    ````markdown
    ```board-block sandbox@1
    {"id":"sbx-123","action":"started"}
    ```
    ````
    (Shorthand ```` ```board:sandbox@1 ```` is also accepted.)
  - Explicit `schema` field in JSON body (`{ "schema": "sandbox@1", "text": "{...}" }`) parses `text` as JSON matching that schema.
  - Unregistered schema -> `400 schema_not_found`; invalid JSON or schema mismatch -> `400 invalid_block`.
  - Validated blocks are persisted and returned in `message.blocks: [{ schema, version, data }]`.

## Attachments (Blob Storage)

Blobs are stored content-addressed under `<data-dir>/blobs/<sha256[0..2]>/<sha256>`.
Limits: <= 10 attachments per message, <= 4 MB inline upload, <= 100 MB streaming upload.

- **Inline upload (<= 4 MB):**
  - `POST /v1/messageboard/boards/{board}/attachments?name=<filename>` (or `X-Filename` header, raw body + `Content-Type`) -> `201 { id, name, type, size }`.
  - Payloads > 4 MB fail with `413`.
- **Two-step streaming upload (<= 100 MB, never buffered in RAM):**
  1. `POST /v1/messageboard/boards/{board}/attachments/uploads` `{ name, type?, size? }` -> `201 { id, name, type, upload_url, complete_url, max_size }`.
  2. `PUT /v1/messageboard/boards/{board}/attachments/uploads/{id}` (raw streaming body, initiator-only) -> `200 { id, size, sha256 }`.
  3. `POST /v1/messageboard/boards/{board}/attachments/uploads/{id}/complete` -> `200 { attachment: { id, name, type, size } }`.
- **Download:**
  - `GET /v1/messageboard/boards/{board}/attachments/{id}` -> streams file bytes with `Content-Type` and `Content-Disposition`.
  - Permitted for: the uploader, callers with `*`-read across all channels, or callers who can `read` at least one non-deleted referencing message.
  - On tombstoned messages, attachments are omitted from `MessageView` and downloadable ONLY by the uploader or `*`-read callers.
- **Attaching to messages:**
  - Pass attachment IDs in `attachments: ["att_..."]` on `post` or `reply`. Author must be permitted to access every referenced attachment (`403`/`404` otherwise). Empty `text` is allowed iff `attachments` is non-empty.
