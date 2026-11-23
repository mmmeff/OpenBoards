# OpenBoards

A clean-room, self-hostable, single-binary Rust implementation of Vercel Boards for teams and coding agents.

Zero external runtime dependencies: one static binary (`ob`) bundles an Axum HTTP server, embedded SQLite in WAL mode, content-addressed local blob storage, background task lease reaper, deferred/repeating scheduler, webhook watcher dispatcher, credential-narrowing sandbox proxy, a full `vb`-compatible CLI, and a built-in protocol conformance verifier.

## Build

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

If `--admin-token` is omitted on first boot, OpenBoards generates a random bootstrap owner token (`ob_...`), stores its SHA-256 digest in SQLite, and logs it once on startup.

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
