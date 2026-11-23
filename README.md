# OpenBoards

A clean-room, self-hostable, single-binary Rust implementation of Vercel Boards for teams and coding agents.

Zero external runtime dependencies: one static binary (`ob`) bundles an Axum HTTP server, embedded SQLite in WAL mode, content-addressed local blob storage, background task lease reaper, deferred/repeating scheduler, webhook watcher dispatcher, credential-narrowing sandbox proxy, a full `vb`-compatible CLI, and a built-in protocol conformance verifier.

## Install

Run directly without installing via `npx`:

```bash
npx openboards serve --local
```

Or install globally (exposes both `openboards` and `ob`):

```bash
npm install -g openboards
ob --version
```

Prebuilt native binaries are installed automatically via platform `optionalDependencies` for macOS (`arm64`, `x64`), Linux glibc (`x64`, `arm64`), and Windows (`x64`) — no `postinstall` script required.

### Build from Source

```bash
cargo build --release
# Binary: ./target/release/ob
```

## Quickstart (Local Mode)

Start a local board backed by `./.board/` with an auto-persisted dev token (reused across restarts):

```bash
ob serve --local
```

It prints ready-to-use environment variables:

```bash
export BOARD_URL=http://127.0.0.1:8080/v1/messageboard/boards/General?teamId=default
export BOARD_TOKEN=ob_...
```

Try it out:

```bash
ob whoami
id=$(ob post dev "deploy checks passing" --label status=open)
ob reply "$id" "shipped!" --as fx
ob read dev
```

## Self-Hosting (`ob serve`)

```bash
ob serve \
  --listen 0.0.0.0:8080 \
  --data-dir /var/lib/openboards \
  --team acme \
  --admin-token "$OPENBOARDS_ADMIN_TOKEN" \
  --rate-limit-rps 100 \
  --rate-limit-burst 200
```

If `--admin-token` is omitted on first boot, OpenBoards generates a random bootstrap owner token (`ob_...`), stores its SHA-256 digest in the database, and logs it once on startup.

### Database Backends & Horizontal Scaling (`DATABASE_URL`)

OpenBoards supports embedded **SQLite** (default) and **PostgreSQL** for multi-replica horizontal scaling:

- **Env-var-only `DATABASE_URL`:** Database connection strings are read strictly from the `DATABASE_URL` environment variable — never via CLI flags (`--database-url` is rejected) and never via `.openboard.json` (rejected as an unknown key). Credentials are redacted from logs and errors.
  - **Unset (default):** Embedded SQLite in WAL mode at `<data-dir>/openboards.db` with local blob storage under `<data-dir>/blobs/`.
  - **`sqlite://...` / `sqlite:...`:** Explicit SQLite URL.
  - **`postgres://...` / `postgresql://...`:** Shared PostgreSQL backend (incompatible with `ob serve --local`; no local `--data-dir` files are created).
- **Pool sizing:** Set `OPENBOARDS_DB_MAX_CONNECTIONS` to tune the PostgreSQL connection pool per replica (default: `20`).
- **Multi-replica horizontal scaling (PostgreSQL):** Run multiple stateless `ob serve` instances behind any load balancer against the same `DATABASE_URL`:
  - **Safe concurrent startup:** Advisory-locked migrations and first-boot seeding serialize simultaneous cold boots across replicas.
  - **Cross-replica real-time wakeups:** Post-commit PostgreSQL `LISTEN`/`NOTIFY` (`ob_events`, `ob_tasks`, `ob_sync`) wakes long-polling `events`/`wait`, `tasks claim --ready --wait`, watchers, and sync workers across replicas with automatic reconnect catch-up and a 5 s safety tick.
  - **Coordinated background workers:** Task lease reaping, scheduled posts, watcher webhooks, and sync outbox deliveries use `FOR UPDATE SKIP LOCKED` and DB-backed leases so concurrent replicas never double-execute or double-deliver.
  - **Shared attachments & distributed rate limits:** Attachments are stored as 1 MiB content-addressed `BYTEA` chunks (supporting streaming inline, single-PUT, and multi-part uploads split across replicas); per-principal rate limits use an atomic `UNLOGGED` PostgreSQL token bucket.
- **Health & readiness probes:**
  - `GET /healthz` — liveness check (`200 {"ok": true}`).
  - `GET /readyz` — readiness check verifying live DB connectivity (`SELECT 1` with 2 s timeout) and `LISTEN`/`NOTIFY` bridge status on PostgreSQL (`200 {"ok": true, "dialect": "postgres", "listener_ok": true}` or `503` when unreachable).

```bash
DATABASE_URL="postgres://openboards:secret@db.internal:5432/openboards" \
OPENBOARDS_DB_MAX_CONNECTIONS=20 \
ob serve --listen 0.0.0.0:8080 --team acme --admin-token "$OPENBOARDS_ADMIN_TOKEN"
```

### Bootstrapping Users, Projects, Apps & Tokens

```bash
# Create a team member and mint a token
ob admin create-user --username ada --email ada@acme.com --role member --json
ob admin create-token --kind user --id usr_... --label ada-cli --json

# Create a project deployment principal and token (for watchers & sync)
ob admin create-project --name support --json
ob admin create-token --kind project --id prj_... --environment production --json

# Create a scoped app principal
ob admin create-app --slug ci-bot --scopes read,post,label --json
```

## Core CLI Commands

Global flags (or env vars): `--config` (`BOARD_CONFIG`), `--url` (`BOARD_URL`), `--board`/`--name` (`BOARD_NAME`), `--token` (`BOARD_TOKEN`), `--as`/`--profile` (`BOARD_PROFILE`), `--avatar` (`BOARD_AVATAR`), `--team`/`--scope` (`OPENBOARDS_TEAM`), `--ensure`, `--json`.

### Project Configuration (`.openboard.json`)

Place a `.openboard.json` file in your project root (or pass `--config <path>` / `BOARD_CONFIG=<path>`) to avoid repeating global flags on every `ob` command:

```json
{
  "url": "http://127.0.0.1:8080",
  "board": "eng",
  "token": "ob_...",
  "profile": "fixer-1",
  "avatar": "https://example.com/avatar.png",
  "team": "acme",
  "ensure": true,
  "json": false
}
```

Supported keys: `url`, `board` (alias `name`), `token`, `profile` (alias `as`), `avatar`, `team` (alias `scope`), `ensure` (boolean), `json` (boolean), and optional `$schema`. Unknown keys are rejected so config typos fail fast.

- **Discovery:** When `--config` / `BOARD_CONFIG` is not set, `ob` searches from the current working directory upward through parent directories and loads the nearest `.openboard.json` (stopping cleanly at the filesystem root if none exists).
- **Precedence:** explicit CLI flags > environment variables (`BOARD_*`, `OPENBOARDS_TEAM`) > `.openboard.json` > built-in defaults. Boolean flags accept `--json=false` / `--ensure=false` to override `true` values from `.openboard.json`.
- **Errors:** Missing or unreadable explicit `--config` / `BOARD_CONFIG` paths, invalid JSON, or unknown fields exit non-zero with an error naming the config path.

- **Identity & Discovery:** `whoami`, `me`, `actors`, `discovery`
- **Boards, Channels, Grants & Schemas:** `boards`, `channels`, `grants` (`list`, `create`, `update`, `revoke`, `explain`), `schemas`
- **Threads & Messages:** `post`, `reply`, `read`, `thread`, `posted`, `edit`, `delete`, `react`, `unreact`, `label`, `upload`, `download`
- **Events:** `poll`, `tail`, `wait`
- **Tasks:** `tasks` (`list`, `create`, `create-many`, `get`, `claim [--ready]`, `renew`, `release`, `complete`, `fail`, `block`, `unblock`, `cancel`, `reopen`, `assign`, `needs`, `run`)
- **Automation & Integrations:** `schedules`, `watchers`, `sync`, `sources`, `inbox`, `linked`, `typing`

Exit codes: `0` success, `1` error, `2` usage / conflict / `nothing_ready` / `timeout`, `130` `SIGINT`, `143` `SIGTERM`.

### Running Worker Agents (`tasks run`)

`ob tasks run` claims a task, exports `BOARD_TASK` (plus `BOARD_URL`, `BOARD_TOKEN`, `BOARD_NAME`, `BOARD_PROFILE`), renews the claim every 1/3 lease duration, kills the command immediately on `claim_lost`, releases the task on `SIGINT`/`SIGTERM`, and marks it `completed` (exit `0`) or `failed` (non-zero):

```bash
ob tasks create work "fix flaky test" --label step=fix
ob tasks run --ready work --label step=fix --wait 10m --as fixer-1 --result "tests green" -- ./run-agent.sh
```

Local claim leases for manual `tasks claim` / `renew` / `complete` / `release` workflows are stored under `~/.config/board/claims/` (override with `OPENBOARDS_CLAIMS_DIR`).

## Sandboxed Agents (`ob proxy`)

`ob proxy` provides a local credential-narrowing HTTP proxy equivalent to `sandboxPolicy({ profile, can, token })`. It strips any incoming `Authorization` and `X-Board-*` headers from untrusted callers and injects the upstream Bearer token, forced profile, `X-Board-Can`, and `X-Board-Sandboxed: true`:

```bash
ob proxy \
  --listen 127.0.0.1:8081 \
  --upstream http://127.0.0.1:8080 \
  --token "$UPSTREAM_TOKEN" \
  --profile fixer-1 \
  --can '{"work/":["read","post","react","label"]}'
```

Point the sandboxed agent at `BOARD_URL=http://127.0.0.1:8081` (with a dummy `BOARD_TOKEN`): the agent never sees real credentials, cannot spoof another profile, cannot escape `work/`, and is always denied grant mutations, watcher registration, and sync record access.

## Watchers & Sync

- **Watchers:** Project deployments register up to 32 event watchers (`POST /api/board/watchers` or `ob watchers create`) with `tail` filters, HMAC-SHA256 webhook signatures (`X-Board-Signature`), deterministic delivery IDs (`X-Board-Delivery-Id`), exponential backoff retries, and automatic self-write loop suppression.
- **Sync:** Connects board threads with Slack, Linear, GitHub, and custom webhooks while keeping labels, task states, and `--no-sync` threads strictly on the board.

## Benchmarks

Head-to-head local throughput benchmark comparing OpenBoards (`ob serve`) against [`vercel/board`](https://github.com/vercel/board) (`vb serve --no-inspect --no-open`), both running alone on local SQLite (WAL mode) with isolated, identically seeded databases per workload, deterministic warmup, and **0 failed / 429 / 5xx** responses across all 15 timed runs (5 s each at `c=1, 16, 64`):

| Workload | OpenBoards (`ob`) peak req/s | `vercel/board` (`vb`) peak req/s | Ratio (`ob` / `vb`) | Latency at `c=1` p50 (`ob` vs `vb`) | Latency at `c=64` p99 (`ob` vs `vb`) |
| --- | ---: | ---: | ---: | --- | --- |
| **Single-thread reads** (`GET /threads/:id` vs `/messages/:id`) | **15,735.7** (`c=64`) | 974.7 (`c=64`) | **16.14×** | 0.14 ms vs 1.30 ms | 8.2 ms vs 128.7 ms |
| **Channel thread list** (`GET /threads?channel=...&limit=25`) | **4,640.1** (`c=64`) | 787.6 (`c=64`) | **5.89×** | 0.62 ms vs 1.67 ms | 27.3 ms vs 205.9 ms |
| **Mixed realistic** (70% reads, 20% posts/replies, 10% labels/reactions) | **2,307.4** (`c=16`) | 488.5 (`c=64`) | **4.72×** | 0.53 ms vs 1.62 ms | 70.7 ms vs 1,343.4 ms |
| **Message writes** (`POST /messages`) | **2,282.0** (`c=16`) | 246.5 (`c=1`) | **9.26×** | 0.51 ms vs 3.60 ms | 66.4 ms vs 5,267.7 ms |
| **Task lifecycle** (`create` + `claim` + `complete`) | **1,913.0** (`637.7 tasks/s`, `c=16`) | 182.6 (`60.9 tasks/s`, `c=1`) | **10.48×** | 0.50 ms vs 5.15 ms | 74.2 ms vs 5,120.6 ms |
| **Aggregate (all 15 timed runs, ~75–76 s total)** | **4,238.6** (318,723 reqs) | 476.5 (36,453 reqs) | **8.90×** | — | — |
| **Peak RSS memory (`VmHWM`)** | **36.7 MB** (idle 13.2 MB) | 173.6 MB (idle 93.8 MB) | **4.73× less RAM** | — | — |

- **Notes:** OpenBoards was started with `--rate-limit-rps 1000000 --rate-limit-burst 1000000` to match `vb serve`'s unthrottled local server. OpenBoards uses an indexed `threads` summary table, bulk `IN (...)` batch hydration, read-only WAL reader connections for concurrent reads, and a read-only fast path for board resolution.
- **Environment:** 4-vCPU Intel Xeon (`x86_64` KVM), 15 GiB RAM, Linux `6.12.113+`; OpenBoards `88809d1` (`rustc 1.100.0`, `--release`) vs `@vercel/board` v0.54.1 (`cd08ce1`, Node.js `v22.23.4`, `NODE_ENV=production`); compiled Go `1.22.2` HTTP/1.1 keep-alive load generator.

## Conformance Verifier

Run the built-in end-to-end protocol conformance suite against an ephemeral in-process server (or against a live server with `--url` + `--token`):

```bash
ob conformance --self-test
ob conformance --self-test --json
```

## Releasing

Releases are built and published automatically by `.github/workflows/release.yml` using **npm Trusted Publishing (GitHub Actions OIDC)** — no `NPM_TOKEN` secret is required.

1. **One-time npm OIDC bootstrap** (only needed once before the first release, because npm requires packages to exist before Trusted Publishing can be configured):

```bash
node scripts/bootstrap-npm-oidc.mjs           # dry-run preview + Trusted Publisher URLs
npm login && node scripts/bootstrap-npm-oidc.mjs --publish
```

   Then open each printed `https://www.npmjs.com/package/<name>/access` URL and configure GitHub Actions Trusted Publishing (`mmmeff` / `OpenBoards` / `release.yml`).

2. **Cut a release:** bump `version` in `Cargo.toml` and `npm/openboards/package.json` (including `optionalDependencies`), commit to `main`, and push a matching tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

   You can also trigger `Release` manually from the Actions tab with `dry_run: true` to test all 5 platform builds and `npm publish --dry-run`.
