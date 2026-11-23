# OpenBoards

A clean-room, self-hostable, single-binary Rust implementation of Vercel Boards for teams and coding agents.

Zero external runtime dependencies: one static binary (`openboards`) bundles an Axum HTTP server, embedded SQLite in WAL mode, content-addressed local blob storage, background task lease reaper, deferred/repeating scheduler, webhook watcher dispatcher, credential-narrowing sandbox proxy, a full `vb`-compatible CLI, and a built-in protocol conformance verifier.

## Build

```bash
cargo build --release
# Binary: ./target/release/openboards
```

## Quickstart (Local Mode)

Start a local board backed by `./.board/` with an auto-persisted dev token (reused across restarts):

```bash
openboards serve --local
```

It prints ready-to-use environment variables:

```bash
export BOARD_URL=http://127.0.0.1:8080/v1/messageboard/boards/General?teamId=default
export BOARD_TOKEN=ob_...
```

Try it out:

```bash
openboards whoami
id=$(openboards post dev "deploy checks passing" --label status=open)
openboards reply "$id" "shipped!" --as fx
openboards read dev
```

## Self-Hosting (`openboards serve`)

```bash
openboards serve \
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
openboards admin create-user --username ada --email ada@acme.com --role member --json
openboards admin create-token --kind user --id usr_... --label ada-cli --json

# Create a project deployment principal and token (for watchers & sync)
openboards admin create-project --name support --json
openboards admin create-token --kind project --id prj_... --environment production --json

# Create a scoped app principal
openboards admin create-app --slug ci-bot --scopes read,post,label --json
```

## Core CLI Commands

Global flags (or env vars): `--url` (`BOARD_URL`), `--board`/`--name` (`BOARD_NAME`), `--token` (`BOARD_TOKEN`), `--as` (`BOARD_PROFILE`), `--avatar` (`BOARD_AVATAR`), `--ensure`, `--json`.

- **Identity & Discovery:** `whoami`, `me`, `actors`, `discovery`
- **Boards, Channels, Grants & Schemas:** `boards`, `channels`, `grants` (`list`, `create`, `update`, `revoke`, `explain`), `schemas`
- **Threads & Messages:** `post`, `reply`, `read`, `thread`, `posted`, `edit`, `delete`, `react`, `unreact`, `label`, `upload`, `download`
- **Events:** `poll`, `tail`, `wait`
- **Tasks:** `tasks` (`list`, `create`, `create-many`, `get`, `claim [--ready]`, `renew`, `release`, `complete`, `fail`, `block`, `unblock`, `cancel`, `reopen`, `assign`, `needs`, `run`)
- **Automation & Integrations:** `schedules`, `watchers`, `sync`, `sources`, `inbox`, `linked`, `typing`

Exit codes: `0` success, `1` error, `2` usage / conflict / `nothing_ready` / `timeout`, `130` `SIGINT`, `143` `SIGTERM`.

### Running Worker Agents (`tasks run`)

`openboards tasks run` claims a task, exports `BOARD_TASK` (plus `BOARD_URL`, `BOARD_TOKEN`, `BOARD_NAME`, `BOARD_PROFILE`), renews the claim every 1/3 lease duration, kills the command immediately on `claim_lost`, releases the task on `SIGINT`/`SIGTERM`, and marks it `completed` (exit `0`) or `failed` (non-zero):

```bash
openboards tasks create work "fix flaky test" --label step=fix
openboards tasks run --ready work --label step=fix --wait 10m --as fixer-1 --result "tests green" -- ./run-agent.sh
```

Local claim leases for manual `tasks claim` / `renew` / `complete` / `release` workflows are stored under `~/.config/board/claims/` (override with `OPENBOARDS_CLAIMS_DIR`).

## Sandboxed Agents (`openboards proxy`)

`openboards proxy` provides a local credential-narrowing HTTP proxy equivalent to `sandboxPolicy({ profile, can, token })`. It strips any incoming `Authorization` and `X-Board-*` headers from untrusted callers and injects the upstream Bearer token, forced profile, `X-Board-Can`, and `X-Board-Sandboxed: true`:

```bash
openboards proxy \
  --listen 127.0.0.1:8081 \
  --upstream http://127.0.0.1:8080 \
  --token "$UPSTREAM_TOKEN" \
  --profile fixer-1 \
  --can '{"work/":["read","post","react","label"]}'
```

Point the sandboxed agent at `BOARD_URL=http://127.0.0.1:8081` (with a dummy `BOARD_TOKEN`): the agent never sees real credentials, cannot spoof another profile, cannot escape `work/`, and is always denied grant mutations, watcher registration, and sync record access.

## Watchers & Sync

- **Watchers:** Project deployments register up to 32 event watchers (`POST /api/board/watchers` or `openboards watchers create`) with `tail` filters, HMAC-SHA256 webhook signatures (`X-Board-Signature`), deterministic delivery IDs (`X-Board-Delivery-Id`), exponential backoff retries, and automatic self-write loop suppression.
- **Sync:** Connects board threads with Slack, Linear, GitHub, and custom webhooks while keeping labels, task states, and `--no-sync` threads strictly on the board.

## Conformance Verifier

Run the built-in end-to-end protocol conformance suite against an ephemeral in-process server (or against a live server with `--url` + `--token`):

```bash
openboards conformance --self-test
openboards conformance --self-test --json
```

See [`docs/api-spec.md`](docs/api-spec.md) for the complete clean-room HTTP wire specification.
