use std::collections::BTreeMap;
use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::client::{parse_board_url, BoardClient, ClientConfig, ClientError};
use crate::config::{load_cli_config, CliFileConfig, ServeArgs, ServerConfig};
use crate::conformance;
use crate::proxy::{self, ProxyConfig};

#[derive(Debug, Parser)]
#[command(
    name = "ob",
    version,
    about = "Self-hostable message board and CLI for teams and coding agents"
)]
pub struct Cli {
    /// Path to .openboard.json config file (defaults to upward search from cwd)
    #[arg(long, global = true, env = "BOARD_CONFIG")]
    pub config: Option<PathBuf>,

    /// Board server URL or full board URL
    #[arg(long, global = true, env = "BOARD_URL")]
    pub url: Option<String>,

    /// Board name (defaults to General)
    #[arg(long, alias = "name", global = true, env = "BOARD_NAME")]
    pub board: Option<String>,

    /// Bearer API token
    #[arg(long, global = true, env = "BOARD_TOKEN")]
    pub token: Option<String>,

    /// Agent profile name (--as <profile>)
    #[arg(long = "as", alias = "profile", global = true, env = "BOARD_PROFILE")]
    pub profile: Option<String>,

    /// Optional avatar URL for profile
    #[arg(long, global = true, env = "BOARD_AVATAR")]
    pub avatar: Option<String>,

    /// Team identifier
    #[arg(long, alias = "scope", global = true, env = "OPENBOARDS_TEAM")]
    pub team: Option<String>,

    /// Create named board if it does not exist
    #[arg(
        long,
        global = true,
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = true
    )]
    pub ensure: Option<bool>,

    /// Emit machine-readable JSON output
    #[arg(
        long,
        global = true,
        num_args = 0..=1,
        default_missing_value = "true",
        require_equals = true
    )]
    pub json: Option<bool>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedGlobalOptions {
    pub url: Option<String>,
    pub board: Option<String>,
    pub token: Option<String>,
    pub profile: Option<String>,
    pub avatar: Option<String>,
    pub team: Option<String>,
    pub ensure: bool,
    pub json: bool,
}

/// Merges parsed CLI/env options over `.openboard.json` values with precedence:
/// explicit CLI flags > environment variables > `.openboard.json` > defaults.
pub fn resolve_global_options(cli: &Cli, file_cfg: &CliFileConfig) -> ResolvedGlobalOptions {
    let (cli_url_board, cli_url_team) = match cli.url.as_deref() {
        Some(u) => {
            let (_, b, t) = parse_board_url(u);
            (b, t)
        }
        None => (None, None),
    };

    ResolvedGlobalOptions {
        url: cli.url.clone().or_else(|| file_cfg.url.clone()),
        board: cli
            .board
            .clone()
            .or(cli_url_board)
            .or_else(|| file_cfg.board.clone()),
        token: cli.token.clone().or_else(|| file_cfg.token.clone()),
        profile: cli.profile.clone().or_else(|| file_cfg.profile.clone()),
        avatar: cli.avatar.clone().or_else(|| file_cfg.avatar.clone()),
        team: cli
            .team
            .clone()
            .or(cli_url_team)
            .or_else(|| file_cfg.team.clone()),
        ensure: cli.ensure.or(file_cfg.ensure).unwrap_or(false),
        json: cli.json.or(file_cfg.json).unwrap_or(false),
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the OpenBoards HTTP server
    Serve(ServeArgs),
    /// Run a local credential-narrowing sandbox proxy (sandboxPolicy equivalent)
    Proxy(ProxyArgs),
    /// Run the built-in Board protocol conformance verifier
    Conformance(ConformanceArgs),
    /// Show caller identity, profile, role, and narrowing
    Whoami,
    /// Show caller identity and effective permissions on the active board
    Me,
    /// List visible actors on the board
    Actors,
    /// Fetch unauthenticated board discovery metadata
    Discovery,
    /// Self-hosted admin operations (users, projects, apps, tokens)
    Admin {
        #[command(subcommand)]
        cmd: AdminCmd,
    },
    /// Manage boards
    Boards {
        #[command(subcommand)]
        cmd: Option<BoardsCmd>,
    },
    /// Manage channels
    Channels {
        #[command(subcommand)]
        cmd: Option<ChannelsCmd>,
    },
    /// Manage access control grants
    Grants {
        #[command(subcommand)]
        cmd: Option<GrantsCmd>,
    },
    /// Manage structured data JSON schemas
    Schemas {
        #[command(subcommand)]
        cmd: Option<SchemasCmd>,
    },
    /// Post a new thread in a channel
    Post(PostArgs),
    /// Reply in an existing thread
    Reply(ReplyArgs),
    /// Read threads or messages with filters
    Read(ReadArgs),
    /// Read a full thread from any message ID in it
    Thread { id: String },
    /// Look up a message by idempotency key
    Posted { key: String },
    /// Edit a message
    Edit(EditArgs),
    /// Delete (tombstone) a message
    #[command(visible_aliases = ["rm", "del"])]
    Delete(DeleteArgs),
    /// Add a reaction to a message
    React {
        id: String,
        emoji: String,
        #[arg(long, default_value_t = false)]
        remove: bool,
    },
    /// Remove a reaction from a message
    Unreact { id: String, emoji: String },
    /// Set or unset labels on a message (supports CAS --if / --if-unset)
    Label(LabelArgs),
    /// Upload a file attachment
    Upload(UploadArgs),
    /// Download an attachment by ID
    Download {
        id: String,
        /// Destination file path (or '-' for stdout; defaults to attachment filename)
        path: Option<String>,
    },
    /// Poll the board event log once
    Poll(EventFilterArgs),
    /// Follow live board events until Ctrl-C
    Tail(EventFilterArgs),
    /// Block until the next matching event arrives (exit 1 on timeout, 130 on Ctrl-C)
    Wait(WaitArgs),
    /// Manage tasks, claims, DAG dependencies, and run worker commands
    Tasks {
        /// Channel or subtree to list tasks from (e.g. `tasks work/`)
        channel: Option<String>,
        #[arg(long)]
        state: Option<String>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long)]
        by: Option<String>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long = "for")]
        assigned_for: Option<String>,
        #[arg(long)]
        needs: Option<String>,
        #[arg(long)]
        after: Option<String>,
        #[arg(short = 'n', long)]
        limit: Option<usize>,
        #[command(subcommand)]
        cmd: Option<TasksCmd>,
    },
    /// Schedule a post or reply for later (or cancel via `schedule cancel <id>`)
    Schedule(Box<ScheduleSingularArgs>),
    /// List or inspect scheduled posts and replies
    Schedules {
        /// Optional schedule ID to inspect (`schedules <id>`)
        id: Option<String>,
        #[arg(long)]
        re: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        by: Option<String>,
        #[arg(short = 'a', long, default_value_t = false)]
        all: bool,
        #[command(subcommand)]
        cmd: Option<SchedulesCmd>,
    },
    /// Manage webhook watchers
    Watchers {
        #[command(subcommand)]
        cmd: Option<WatchersCmd>,
    },
    /// Show where a thread is synced, or sync it with an external platform target
    Sync {
        id: String,
        target: Option<String>,
    },
    /// List configured sync sources (or `sources <key> read` to inspect records)
    Sources {
        key: Option<String>,
        action: Option<String>,
    },
    /// Inspect raw inbound sync events for a source key
    Inbox { key: String },
    /// Find the board thread linked to an external URL
    Linked { url: String },
    /// Send a typing indicator for a synced thread
    Typing {
        id: String,
        status: Option<String>,
    },
}

#[derive(Debug, Args)]
pub struct ProxyArgs {
    #[arg(long, default_value = "127.0.0.1:8081")]
    pub listen: SocketAddr,
    #[arg(long, env = "OPENBOARDS_UPSTREAM")]
    pub upstream: String,
    #[arg(long, env = "OPENBOARDS_PROXY_TOKEN")]
    pub token: String,
    #[arg(long)]
    pub profile: String,
    #[arg(long, default_value = "read,post,react,label")]
    pub can: String,
    #[arg(long)]
    pub avatar: Option<String>,
}

#[derive(Debug, Args)]
pub struct ConformanceArgs {
    /// Spin up an ephemeral in-process server and run all checks against it
    #[arg(long, default_value_t = false)]
    pub self_test: bool,
}

#[derive(Debug, Subcommand)]
pub enum AdminCmd {
    CreateUser {
        #[arg(long)]
        username: String,
        #[arg(long)]
        email: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value = "member")]
        role: String,
    },
    ListUsers,
    CreateProject {
        #[arg(long)]
        name: String,
    },
    CreateApp {
        #[arg(long)]
        slug: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value = "read,post,react,label")]
        scopes: String,
    },
    CreateToken {
        #[arg(long = "kind")]
        principal_kind: String,
        #[arg(long = "id")]
        principal_id: String,
        #[arg(long)]
        environment: Option<String>,
        #[arg(long)]
        label: Option<String>,
    },
    RevokeToken {
        id: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum BoardsCmd {
    List,
    Create { name: String },
    Ensure { name: String },
}

#[derive(Debug, Subcommand)]
pub enum ChannelsCmd {
    List,
    Create {
        path: String,
        #[arg(long, default_value_t = false)]
        private: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum GrantsCmd {
    List,
    #[command(visible_aliases = ["add", "set"])]
    Create {
        #[arg(long = "user")]
        users: Vec<String>,
        #[arg(long = "project", short = 'p')]
        projects: Vec<String>,
        #[arg(long = "environment", short = 'e')]
        environments: Vec<String>,
        #[arg(long = "app")]
        apps: Vec<String>,
        #[arg(long, default_value_t = false)]
        all_users: bool,
        #[arg(long, default_value_t = false)]
        all_projects: bool,
        #[arg(long, default_value_t = false)]
        all_apps: bool,
        #[arg(long, default_value_t = false)]
        everyone: bool,
        /// Pattern=perms (repeatable, e.g. 'security/=all')
        #[arg(long = "can", required = true)]
        can: Vec<String>,
    },
    #[command(visible_alias = "edit")]
    Update {
        id: String,
        #[arg(long = "add-user")]
        add_users: Vec<String>,
        #[arg(long = "remove-user")]
        remove_users: Vec<String>,
        #[arg(long = "add-project")]
        add_projects: Vec<String>,
        #[arg(long = "remove-project")]
        remove_projects: Vec<String>,
        #[arg(long = "environment", short = 'e')]
        environments: Vec<String>,
        #[arg(long = "add-app")]
        add_apps: Vec<String>,
        #[arg(long = "remove-app")]
        remove_apps: Vec<String>,
        #[arg(long = "can")]
        can: Vec<String>,
    },
    #[command(visible_alias = "delete")]
    Revoke {
        id: String,
    },
    Explain {
        channel: String,
        #[arg(long)]
        user: Option<String>,
        #[arg(long)]
        project: Option<String>,
        #[arg(long)]
        environment: Option<String>,
        #[arg(long)]
        app: Option<String>,
        #[arg(long)]
        principal: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum SchemasCmd {
    List,
    #[command(visible_alias = "inspect")]
    Get {
        id: String,
    },
    Create {
        /// Schema identifier `<name>@<version>`
        id: String,
        #[arg(long)]
        file: Option<String>,
        #[arg(long)]
        data: Option<String>,
        #[arg(long)]
        description: Option<String>,
    },
}

#[derive(Debug, Args)]
pub struct PostArgs {
    /// Channel path, or existing message ID (`msg_...`) to reply in that thread
    pub target: String,
    pub text: Option<String>,
    #[arg(long)]
    pub file: Option<String>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long = "attach")]
    pub attachments: Vec<String>,
    #[arg(long)]
    pub schema: Option<String>,
    #[arg(long = "id", alias = "idempotency-key")]
    pub id: Option<String>,
    #[arg(long = "sync")]
    pub sync: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub no_sync: bool,
}

#[derive(Debug, Args)]
pub struct ReplyArgs {
    pub id: String,
    pub text: Option<String>,
    #[arg(long)]
    pub file: Option<String>,
    #[arg(long)]
    pub after: Option<String>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long = "attach")]
    pub attachments: Vec<String>,
    #[arg(long)]
    pub schema: Option<String>,
    #[arg(long = "idempotency-key")]
    pub idem_key: Option<String>,
    #[arg(long, default_value_t = false)]
    pub no_sync: bool,
}

#[derive(Debug, Args)]
pub struct ReadArgs {
    /// Channel path, subtree (`dev/`), or message ID (`msg_...`)
    pub target: Option<String>,
    #[arg(long, alias = "thread")]
    pub re: Option<String>,
    #[arg(long)]
    pub by: Option<String>,
    #[arg(long, num_args = 0..=1, default_missing_value = "me")]
    pub mentions: Option<String>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long, alias = "source")]
    pub from: Option<String>,
    #[arg(long)]
    pub text: Option<String>,
    #[arg(long)]
    pub after: Option<String>,
    #[arg(long, alias = "older")]
    pub before: Option<String>,
    #[arg(short = 'n', long)]
    pub limit: Option<usize>,
    /// Return flat messages instead of thread summaries
    #[arg(long, default_value_t = false)]
    pub messages: bool,
}

#[derive(Debug, Args)]
pub struct EditArgs {
    pub id: String,
    pub text: Option<String>,
    #[arg(long)]
    pub file: Option<String>,
    #[arg(long = "if-revision", alias = "revision")]
    pub revision: Option<i64>,
    #[arg(long)]
    pub schema: Option<String>,
}

#[derive(Debug, Args)]
pub struct DeleteArgs {
    pub id: String,
    #[arg(long = "if-revision", alias = "revision")]
    pub revision: Option<i64>,
}

#[derive(Debug, Args)]
pub struct LabelArgs {
    pub id: String,
    /// `key=value` pairs to set
    pub set: Vec<String>,
    #[arg(long = "unset")]
    pub unset: Vec<String>,
    #[arg(long = "if")]
    pub if_eq: Vec<String>,
    #[arg(long = "if-unset")]
    pub if_unset: Vec<String>,
}

#[derive(Debug, Args)]
pub struct UploadArgs {
    pub file: PathBuf,
    #[arg(long)]
    pub name: Option<String>,
    #[arg(long = "type")]
    pub content_type: Option<String>,
}

#[derive(Debug, Args)]
pub struct EventFilterArgs {
    pub channel: Option<String>,
    #[arg(long)]
    pub re: Option<String>,
    #[arg(long)]
    pub by: Option<String>,
    #[arg(long, num_args = 0..=1, default_missing_value = "me")]
    pub mentions: Option<String>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long, alias = "source")]
    pub from: Option<String>,
    #[arg(long)]
    pub text: Option<String>,
    #[arg(long)]
    pub after: Option<String>,
    #[arg(long)]
    pub types: Option<String>,
    #[arg(long)]
    pub limit: Option<usize>,
    #[arg(long)]
    pub wait: Option<String>,
    #[arg(short = 'a', long, default_value_t = false)]
    pub all: bool,
}

#[derive(Debug, Args)]
pub struct WaitArgs {
    /// Channel, subtree (`dev/`), or message ID (`msg_...`)
    pub target: Option<String>,
    #[arg(long)]
    pub re: Option<String>,
    #[arg(long)]
    pub by: Option<String>,
    #[arg(long, num_args = 0..=1, default_missing_value = "me")]
    pub mentions: Option<String>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long, alias = "source")]
    pub from: Option<String>,
    #[arg(long)]
    pub text: Option<String>,
    #[arg(long)]
    pub after: Option<String>,
    #[arg(long)]
    pub types: Option<String>,
    #[arg(long, default_value = "30m")]
    pub timeout: String,
    #[arg(short = 'a', long, default_value_t = false)]
    pub all: bool,
}

#[derive(Debug, Subcommand)]
pub enum TasksCmd {
    List {
        channel: Option<String>,
        #[arg(long)]
        state: Option<String>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long)]
        by: Option<String>,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long = "for")]
        assigned_for: Option<String>,
        #[arg(long)]
        needs: Option<String>,
        #[arg(long)]
        after: Option<String>,
        #[arg(short = 'n', long)]
        limit: Option<usize>,
    },
    Create {
        /// Channel name or existing message ID (`msg_...`)
        target: String,
        text: Option<String>,
        /// JSON task plan file (or '-' for stdin) to create multiple tasks in `target` channel
        #[arg(long)]
        file: Option<String>,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long = "needs")]
        needs: Vec<String>,
        #[arg(long = "for")]
        assigned_for: Option<String>,
        #[arg(long, default_value_t = false)]
        claim: bool,
        #[arg(long)]
        lease: Option<i64>,
        #[arg(long = "id", alias = "idempotency-key")]
        id: Option<String>,
        #[arg(long, default_value_t = false)]
        no_sync: bool,
    },
    CreateMany {
        channel: String,
        #[arg(long)]
        file: String,
        #[arg(long = "id", alias = "idempotency-key")]
        id: Option<String>,
    },
    Get {
        id: String,
    },
    Claim {
        /// Task ID (when not using --ready) or channel/subtree (with --ready)
        target: String,
        #[arg(long, default_value_t = false)]
        ready: bool,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long = "from")]
        from: Vec<String>,
        #[arg(long)]
        wait: Option<String>,
        #[arg(long)]
        lease: Option<i64>,
        #[arg(long)]
        lease_ms: Option<i64>,
    },
    Renew {
        id: String,
        #[arg(long)]
        epoch: Option<i64>,
        #[arg(long = "claim-token")]
        claim_token: Option<String>,
        #[arg(long)]
        lease: Option<i64>,
        #[arg(long)]
        lease_ms: Option<i64>,
    },
    Release {
        id: String,
        #[arg(long)]
        epoch: Option<i64>,
        #[arg(long = "claim-token")]
        claim_token: Option<String>,
    },
    Complete {
        id: String,
        result: Option<String>,
        #[arg(long)]
        epoch: Option<i64>,
        #[arg(long = "claim-token")]
        claim_token: Option<String>,
    },
    Fail {
        id: String,
        reason: String,
        #[arg(long)]
        epoch: Option<i64>,
        #[arg(long = "claim-token")]
        claim_token: Option<String>,
    },
    Block {
        id: String,
        reason: String,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        epoch: Option<i64>,
        #[arg(long = "claim-token")]
        claim_token: Option<String>,
    },
    Unblock {
        id: String,
    },
    Cancel {
        id: String,
        reason: String,
    },
    Reopen {
        id: String,
    },
    Assign {
        id: String,
        profile: Option<String>,
        #[arg(long, default_value_t = false)]
        pool: bool,
    },
    Needs {
        id: String,
        needs: Vec<String>,
        #[arg(long, default_value_t = false)]
        none: bool,
    },
    /// Claim a task, run a command with BOARD_TASK set, renew at 1/3 lease, and settle on exit
    Run {
        /// Task ID (default) or channel/subtree (with --ready)
        target: String,
        #[arg(long, default_value_t = false)]
        ready: bool,
        #[arg(long = "label")]
        labels: Vec<String>,
        #[arg(long = "from")]
        from: Vec<String>,
        #[arg(long)]
        wait: Option<String>,
        #[arg(long)]
        lease: Option<i64>,
        #[arg(long)]
        lease_ms: Option<i64>,
        #[arg(long)]
        result: Option<String>,
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
}

#[derive(Debug, Args)]
pub struct ScheduleSingularArgs {
    /// Channel path, message ID (`msg_...`), or `cancel`
    pub target: String,
    /// Message text (or schedule ID when target is `cancel`)
    pub text: Option<String>,
    #[arg(long)]
    pub file: Option<String>,
    #[arg(long = "attach")]
    pub attachments: Vec<String>,
    #[arg(long)]
    pub schema: Option<String>,
    #[arg(long = "in")]
    pub delay_in: Option<String>,
    #[arg(long)]
    pub at: Option<String>,
    #[arg(long)]
    pub every: Option<String>,
    #[arg(long = "for")]
    pub repeat_for: Option<String>,
    #[arg(long)]
    pub times: Option<i64>,
    #[arg(short = 'u', long)]
    pub until: Option<String>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    #[arg(long, default_value_t = false)]
    pub no_sync: bool,
}

#[derive(Debug, Subcommand)]
pub enum SchedulesCmd {
    List {
        #[arg(long)]
        re: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        by: Option<String>,
        #[arg(short = 'a', long, default_value_t = false)]
        all: bool,
    },
    Get {
        id: String,
    },
    Create(Box<ScheduleSingularArgs>),
    Cancel {
        id: String,
    },
}

#[derive(Debug, Subcommand)]
pub enum WatchersCmd {
    List,
    #[command(visible_alias = "register")]
    Create {
        #[arg(long)]
        id: String,
        #[arg(long)]
        tail: String,
        #[arg(long = "webhook")]
        webhook_url: String,
        #[arg(long)]
        secret: Option<String>,
    },
    Sync {
        #[arg(long)]
        file: PathBuf,
    },
    Delete {
        id: String,
    },
}

// --- Local claim persistence (~/.config/board/claims/ or OPENBOARDS_CLAIMS_DIR) ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedClaim {
    pub task_id: String,
    pub epoch: i64,
    pub token: String,
    pub until: String,
    pub profile: Option<String>,
}

fn claims_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("OPENBOARDS_CLAIMS_DIR") {
        if !custom.trim().is_empty() {
            return PathBuf::from(custom);
        }
    }
    if let Ok(home) = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")) {
        if !home.trim().is_empty() {
            return PathBuf::from(home)
                .join(".config")
                .join("board")
                .join("claims");
        }
    }
    std::env::temp_dir().join("openboards-claims")
}

fn claim_file_path(board: &str, profile: Option<&str>, task_id: &str) -> PathBuf {
    let clean = |s: &str| {
        s.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect::<String>()
    };
    let prof_part = profile
        .map(|p| clean(&p.to_ascii_lowercase()))
        .unwrap_or_else(|| "default".to_string());
    claims_dir().join(format!(
        "{}__{}__{}.json",
        clean(&board.to_ascii_lowercase()),
        prof_part,
        clean(task_id)
    ))
}

pub fn save_claim(board: &str, profile: Option<&str>, claim: &SavedClaim) -> Result<(), ClientError> {
    let path = claim_file_path(board, profile, &claim.task_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| ClientError::transport(format!("failed creating claims dir: {e}")))?;
    }
    let data = serde_json::to_string_pretty(claim)
        .map_err(|e| ClientError::transport(format!("failed serializing claim: {e}")))?;
    std::fs::write(&path, data)
        .map_err(|e| ClientError::transport(format!("failed saving claim: {e}")))
}

pub fn load_claim(board: &str, profile: Option<&str>, task_id: &str) -> Option<SavedClaim> {
    let path = claim_file_path(board, profile, task_id);
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

pub fn remove_claim(board: &str, profile: Option<&str>, task_id: &str) {
    let path = claim_file_path(board, profile, task_id);
    let _ = std::fs::remove_file(path);
}

fn resolve_claim_creds(
    board: &str,
    profile: Option<&str>,
    task_id: &str,
    epoch: Option<i64>,
    token: Option<String>,
) -> Result<(i64, String), ClientError> {
    if let (Some(e), Some(t)) = (epoch, token) {
        return Ok((e, t));
    }
    if let Some(saved) = load_claim(board, profile, task_id) {
        return Ok((saved.epoch, saved.token));
    }
    Err(ClientError {
        status: 400,
        code: "claim_not_held".to_string(),
        message: format!("no locally held claim found for task '{task_id}'"),
        hint: Some("Claim the task first or pass --epoch and --claim-token.".to_string()),
        raw_body: None,
    })
}

/// Top-level CLI runner returning the exact process exit code.
pub async fn run_cli(cli: Cli) -> i32 {
    let cli_json_hint = cli.json.unwrap_or(false);
    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            let err = ClientError::usage(format!("failed to determine current directory: {e}"));
            print_error(&err, cli_json_hint);
            return err.exit_code();
        }
    };
    let file_cfg = match load_cli_config(cli.config.as_deref(), &cwd) {
        Ok(Some((_, cfg))) => cfg,
        Ok(None) => CliFileConfig::default(),
        Err(msg) => {
            let err = ClientError::usage(msg);
            print_error(&err, cli_json_hint);
            return err.exit_code();
        }
    };
    let opts = resolve_global_options(&cli, &file_cfg);
    let json_mode = opts.json;

    // Handle local-execution commands first (`serve`, `proxy`, `conformance`)
    match cli.command {
        Command::Serve(args) => {
            let local = args.local;
            let cfg = match ServerConfig::from_serve_args(args) {
                Ok(cfg) => cfg,
                Err(msg) => {
                    eprintln!("error: {msg}");
                    return 2;
                }
            };
            let res = if local {
                crate::serve_local(cfg).await
            } else {
                crate::serve(cfg).await
            };
            return match res {
                Ok(()) => 0,
                Err(err) => {
                    eprintln!("error: {err}");
                    1
                }
            };
        }
        Command::Proxy(args) => {
            let res = proxy::run_proxy(ProxyConfig {
                listen: args.listen,
                upstream: args.upstream,
                token: args.token,
                profile: args.profile,
                can: args.can,
                avatar: args.avatar,
            })
            .await;
            return match res {
                Ok(()) => 0,
                Err(err) => {
                    eprintln!("error: {err}");
                    1
                }
            };
        }
        Command::Conformance(args) => {
            match conformance::run_conformance(
                opts.url.as_deref(),
                opts.token.clone(),
                opts.board.as_deref(),
                args.self_test,
            )
            .await
            {
                Ok(report) => {
                    if json_mode {
                        println!("{}", serde_json::to_string_pretty(&report).unwrap());
                    } else {
                        for c in &report.checks {
                            if c.passed {
                                println!("[PASS] {} ({} ms)", c.name, c.duration_ms);
                            } else {
                                println!(
                                    "[FAIL] {} ({} ms): {}",
                                    c.name,
                                    c.duration_ms,
                                    c.error.as_deref().unwrap_or("unknown error")
                                );
                            }
                        }
                        println!(
                            "\nConformance: {}/{} passed",
                            report.passed,
                            report.passed + report.failed
                        );
                    }
                    return if report.ok { 0 } else { 1 };
                }
                Err(err) => {
                    if json_mode {
                        eprintln!("{}", json!({ "reason": err.code.as_str(), "message": err.message }));
                    } else {
                        eprintln!("error: {err}");
                    }
                    return 1;
                }
            }
        }
        _ => {}
    }

    let raw_url = opts
        .url
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string());
    let cfg = ClientConfig::from_raw(
        &raw_url,
        opts.board.as_deref(),
        opts.token,
        opts.profile,
        opts.avatar,
        opts.team,
        opts.ensure,
    );
    let client = match BoardClient::new(cfg) {
        Ok(c) => c,
        Err(e) => {
            print_error(&e, json_mode);
            return e.exit_code();
        }
    };

    match dispatch_client_command(client, cli.command, json_mode).await {
        Ok(code) => code,
        Err(err) => {
            print_error(&err, json_mode);
            err.exit_code()
        }
    }
}

fn print_error(err: &ClientError, json_mode: bool) {
    if json_mode {
        eprintln!("{}", serde_json::to_string(&err.to_json()).unwrap());
    } else {
        eprintln!("error: {err}");
    }
}

fn print_output(val: &Value, json_mode: bool, human: impl FnOnce(&Value) -> String) {
    if json_mode {
        println!("{}", serde_json::to_string_pretty(val).unwrap());
    } else {
        println!("{}", human(val));
    }
}

fn read_text_arg(text: Option<String>, file: Option<String>) -> Result<String, ClientError> {
    if let Some(f) = file {
        if f == "-" {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| ClientError::usage(format!("failed reading stdin: {e}")))?;
            return Ok(buf);
        }
        return std::fs::read_to_string(&f)
            .map_err(|e| ClientError::usage(format!("failed reading '{f}': {e}")));
    }
    match text {
        Some(t) if t == "-" => {
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| ClientError::usage(format!("failed reading stdin: {e}")))?;
            Ok(buf)
        }
        Some(t) => Ok(t),
        None => Ok(String::new()),
    }
}

fn parse_kv_pairs(raw: &[String]) -> Result<BTreeMap<String, String>, ClientError> {
    let mut map = BTreeMap::new();
    for entry in raw {
        // Support comma-separated k=v pairs as well as repeated flags
        for part in entry.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let (k, v) = trimmed.split_once('=').ok_or_else(|| {
                ClientError::usage(format!("expected 'key=value', got '{trimmed}'"))
            })?;
            map.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    Ok(map)
}

/// Resolves `--attach` arguments: existing `att_...` IDs pass straight through, while local
/// file paths are uploaded via inline/streaming attachment upload and replaced with their IDs.
async fn resolve_attachment_args(
    client: &BoardClient,
    raw: &[String],
) -> Result<Vec<String>, ClientError> {
    let mut ids = Vec::with_capacity(raw.len());
    for item in raw {
        if item.starts_with("att_") && !std::path::Path::new(item).exists() {
            ids.push(item.clone());
            continue;
        }
        let path = std::path::Path::new(item);
        let bytes = std::fs::read(path)
            .map_err(|e| ClientError::usage(format!("failed reading attachment '{item}': {e}")))?;
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file.bin");
        let uploaded = client.upload_bytes(filename, None, bytes).await?;
        let att_id = uploaded["id"]
            .as_str()
            .ok_or_else(|| ClientError::transport("upload response missing attachment id"))?
            .to_string();
        ids.push(att_id);
    }
    Ok(ids)
}

async fn dispatch_client_command(
    client: BoardClient,
    cmd: Command,
    json_mode: bool,
) -> Result<i32, ClientError> {
    match cmd {
        Command::Serve(_) | Command::Proxy(_) | Command::Conformance(_) => unreachable!(),

        Command::Discovery => {
            let val = client.discovery().await?;
            print_output(&val, json_mode, |v| {
                format!(
                    "{} (team: {}, name: {})",
                    v["url"].as_str().unwrap_or(""),
                    v["team_id"].as_str().unwrap_or(""),
                    v["name"].as_str().unwrap_or("")
                )
            });
            Ok(0)
        }

        Command::Whoami => {
            let val = client.whoami().await?;
            print_output(&val, json_mode, |v| {
                format!(
                    "{} ({}, actor: {})",
                    v["handle"].as_str().unwrap_or(""),
                    v["kind"].as_str().unwrap_or(""),
                    v["actor"].as_str().unwrap_or("")
                )
            });
            Ok(0)
        }

        Command::Me => {
            let val = client.me().await?;
            print_output(&val, json_mode, |v| {
                format!(
                    "{} on board '{}'",
                    v["handle"].as_str().unwrap_or(""),
                    v["board"]["name"].as_str().unwrap_or("")
                )
            });
            Ok(0)
        }

        Command::Actors => {
            let val = client.actors().await?;
            print_output(&val, json_mode, |v| {
                v["actors"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .map(|a| {
                                format!(
                                    "{}\t{}\t{}",
                                    a["handle"].as_str().unwrap_or(""),
                                    a["kind"].as_str().unwrap_or(""),
                                    a["id"].as_str().unwrap_or("")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            });
            Ok(0)
        }

        Command::Admin { cmd } => {
            let val = match cmd {
                AdminCmd::CreateUser {
                    username,
                    email,
                    name,
                    role,
                } => {
                    client
                        .admin_create_user(&json!({
                            "username": username,
                            "email": email,
                            "name": name,
                            "role": role
                        }))
                        .await?
                }
                AdminCmd::ListUsers => client.admin_list_users().await?,
                AdminCmd::CreateProject { name } => client.admin_create_project(&name).await?,
                AdminCmd::CreateApp { slug, name, scopes } => {
                    client
                        .admin_create_app(&json!({ "slug": slug, "name": name, "scopes": scopes }))
                        .await?
                }
                AdminCmd::CreateToken {
                    principal_kind,
                    principal_id,
                    environment,
                    label,
                } => {
                    client
                        .admin_create_token(&json!({
                            "principal_kind": principal_kind,
                            "principal_id": principal_id,
                            "environment": environment,
                            "label": label
                        }))
                        .await?
                }
                AdminCmd::RevokeToken { id } => client.admin_revoke_token(&id).await?,
            };
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }

        Command::Boards { cmd } => {
            let val = match cmd.unwrap_or(BoardsCmd::List) {
                BoardsCmd::List => client.list_boards().await?,
                BoardsCmd::Create { name } => client.create_board(&name, false).await?,
                BoardsCmd::Ensure { name } => client.create_board(&name, true).await?,
            };
            print_output(&val, json_mode, |v| {
                if let Some(arr) = v["boards"].as_array() {
                    arr.iter()
                        .filter_map(|b| b["name"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                } else {
                    v["board"]["name"].as_str().unwrap_or("ok").to_string()
                }
            });
            Ok(0)
        }

        Command::Channels { cmd } => {
            let val = match cmd.unwrap_or(ChannelsCmd::List) {
                ChannelsCmd::List => client.list_channels().await?,
                ChannelsCmd::Create { path, private } => {
                    client.create_channel(&path, private).await?
                }
            };
            print_output(&val, json_mode, |v| {
                if let Some(arr) = v["channels"].as_array() {
                    arr.iter()
                        .filter_map(|c| c["path"].as_str())
                        .collect::<Vec<_>>()
                        .join("\n")
                } else {
                    v["channel"]["path"].as_str().unwrap_or("ok").to_string()
                }
            });
            Ok(0)
        }

        Command::Grants { cmd } => {
            let val = match cmd.unwrap_or(GrantsCmd::List) {
                GrantsCmd::List => client.list_grants().await?,
                GrantsCmd::Create {
                    users,
                    projects,
                    environments,
                    apps,
                    all_users,
                    all_projects,
                    all_apps,
                    everyone,
                    can,
                } => {
                    let mut principals: Vec<Value> = Vec::new();
                    for u in users {
                        principals.push(json!({ "kind": "user", "email": u }));
                    }
                    for p in projects {
                        if environments.is_empty() {
                            principals.push(json!({ "kind": "project", "project": p }));
                        } else {
                            principals.push(
                                json!({ "kind": "project", "project": p, "environments": environments }),
                            );
                        }
                    }
                    for a in apps {
                        principals.push(json!({ "kind": "app", "app": a }));
                    }
                    if all_users {
                        principals.push(json!({ "kind": "all-users" }));
                    }
                    if all_projects {
                        if environments.is_empty() {
                            principals.push(json!({ "kind": "all-projects" }));
                        } else {
                            principals.push(
                                json!({ "kind": "all-projects", "environments": environments }),
                            );
                        }
                    }
                    if all_apps {
                        principals.push(json!({ "kind": "all-apps" }));
                    }
                    if everyone {
                        principals.push(json!({ "kind": "everyone" }));
                    }
                    let mut can_map = serde_json::Map::new();
                    for spec in &can {
                        let (pat, perms) = spec.split_once('=').ok_or_else(|| {
                            ClientError::usage(format!("expected 'pattern=perms', got '{spec}'"))
                        })?;
                        can_map.insert(pat.to_string(), json!(perms));
                    }
                    client
                        .create_grant(&json!({ "principals": principals, "can": can_map }))
                        .await?
                }
                GrantsCmd::Update {
                    id,
                    add_users,
                    remove_users,
                    add_projects,
                    remove_projects,
                    environments,
                    add_apps,
                    remove_apps,
                    can,
                } => {
                    let mut body = serde_json::Map::new();
                    let mut add_list: Vec<Value> = add_users
                        .into_iter()
                        .map(|u| json!({ "kind": "user", "email": u }))
                        .collect();
                    for p in add_projects {
                        if environments.is_empty() {
                            add_list.push(json!({ "kind": "project", "project": p }));
                        } else {
                            add_list.push(json!({ "kind": "project", "project": p, "environments": environments }));
                        }
                    }
                    for a in add_apps {
                        add_list.push(json!({ "kind": "app", "app": a }));
                    }
                    if !add_list.is_empty() {
                        body.insert("add".into(), Value::Array(add_list));
                    }

                    let mut remove_list: Vec<Value> = remove_users
                        .into_iter()
                        .map(|u| json!({ "kind": "user", "email": u }))
                        .collect();
                    for p in remove_projects {
                        if environments.is_empty() {
                            remove_list.push(json!({ "kind": "project", "project": p }));
                        } else {
                            remove_list.push(json!({ "kind": "project", "project": p, "environments": environments }));
                        }
                    }
                    for a in remove_apps {
                        remove_list.push(json!({ "kind": "app", "app": a }));
                    }
                    if !remove_list.is_empty() {
                        body.insert("remove".into(), Value::Array(remove_list));
                    }
                    if !can.is_empty() {
                        let mut can_map = serde_json::Map::new();
                        for spec in &can {
                            let (pat, perms) = spec.split_once('=').ok_or_else(|| {
                                ClientError::usage(format!("expected 'pattern=perms', got '{spec}'"))
                            })?;
                            can_map.insert(pat.to_string(), json!(perms));
                        }
                        body.insert("can".into(), Value::Object(can_map));
                    }
                    client.update_grant(&id, &Value::Object(body)).await?
                }
                GrantsCmd::Revoke { id } => client.revoke_grant(&id).await?,
                GrantsCmd::Explain {
                    channel,
                    user,
                    project,
                    environment,
                    app,
                    principal,
                } => {
                    let mut q = vec![("channel".to_string(), channel)];
                    if let Some(v) = user {
                        q.push(("user".into(), v));
                    }
                    if let Some(v) = project {
                        q.push(("project".into(), v));
                    }
                    if let Some(v) = environment {
                        q.push(("environment".into(), v));
                    }
                    if let Some(v) = app {
                        q.push(("app".into(), v));
                    }
                    if let Some(v) = principal {
                        q.push(("principal".into(), v));
                    }
                    client.explain_grant(&q).await?
                }
            };
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }

        Command::Schemas { cmd } => {
            let val = match cmd.unwrap_or(SchemasCmd::List) {
                SchemasCmd::List => client.list_schemas().await?,
                SchemasCmd::Get { id } => client.get_schema(&id).await?,
                SchemasCmd::Create {
                    id,
                    file,
                    data,
                    description,
                } => {
                    let (name, ver_str) = id.split_once('@').ok_or_else(|| {
                        ClientError::usage("schema id must be '<name>@<version>'")
                    })?;
                    let version: i64 = ver_str
                        .parse()
                        .map_err(|_| ClientError::usage("invalid schema version"))?;
                    let raw = match (file, data) {
                        (Some(f), _) => read_text_arg(None, Some(f))?,
                        (None, Some(d)) => d,
                        (None, None) => {
                            return Err(ClientError::usage("provide --file or --data for schema"));
                        }
                    };
                    let schema: Value = serde_json::from_str(&raw)
                        .map_err(|e| ClientError::usage(format!("invalid JSON schema: {e}")))?;
                    client
                        .create_schema(&json!({
                            "name": name,
                            "version": version,
                            "description": description,
                            "schema": schema
                        }))
                        .await?
                }
            };
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }

        Command::Post(args) => {
            if args.no_sync && !args.sync.is_empty() {
                return Err(ClientError::usage(
                    "cannot combine --no-sync and --sync",
                ));
            }
            let text = read_text_arg(args.text, args.file)?;
            let labels = parse_kv_pairs(&args.labels)?;
            let attachments = resolve_attachment_args(&client, &args.attachments).await?;
            let val = if args.target.starts_with("msg_") {
                if !args.sync.is_empty() {
                    return Err(ClientError::usage(
                        "--sync is only valid when starting a new thread in a channel",
                    ));
                }
                client
                    .reply_message(
                        &args.target,
                        &json!({
                            "text": text,
                            "labels": labels,
                            "attachments": attachments,
                            "schema": args.schema,
                            "id": args.id,
                            "noSync": args.no_sync
                        }),
                    )
                    .await?
            } else {
                let posted = client
                    .post_message(&json!({
                        "channel": args.target,
                        "text": text,
                        "labels": labels,
                        "attachments": attachments,
                        "schema": args.schema,
                        "id": args.id,
                        "noSync": args.no_sync
                    }))
                    .await?;
                if let Some(mid) = posted["id"].as_str() {
                    for sync_target in &args.sync {
                        let _ = client.sync_link_thread(mid, sync_target).await?;
                    }
                }
                posted
            };
            print_output(&val, json_mode, |v| {
                v["id"].as_str().unwrap_or("").to_string()
            });
            Ok(0)
        }

        Command::Reply(args) => {
            let text = read_text_arg(args.text, args.file)?;
            let labels = parse_kv_pairs(&args.labels)?;
            let attachments = resolve_attachment_args(&client, &args.attachments).await?;
            let val = client
                .reply_message(
                    &args.id,
                    &json!({
                        "text": text,
                        "after": args.after,
                        "labels": labels,
                        "attachments": attachments,
                        "schema": args.schema,
                        "id": args.idem_key,
                        "noSync": args.no_sync
                    }),
                )
                .await?;
            print_output(&val, json_mode, |v| {
                v["id"].as_str().unwrap_or("").to_string()
            });
            Ok(0)
        }

        Command::Read(args) => {
            if let Some(ref t) = args.target {
                if t.starts_with("http://") || t.starts_with("https://") {
                    let val = client.sync_linked(t).await?;
                    print_output(&val, json_mode, format_thread_human);
                    return Ok(0);
                }
                if t.starts_with("msg_") && !args.messages {
                    let val = client.get_thread(t).await?;
                    print_output(&val, json_mode, format_thread_human);
                    return Ok(0);
                }
            }
            let mut q = Vec::new();
            if let Some(ch) = args.target {
                if !ch.starts_with("msg_") {
                    q.push(("channel".into(), ch));
                } else {
                    q.push(("re".into(), ch));
                }
            }
            if let Some(re) = args.re {
                q.push(("re".into(), re));
            }
            if let Some(by) = args.by {
                q.push(("by".into(), by));
            }
            if let Some(m) = args.mentions {
                q.push(("mentions".into(), m));
            }
            for l in args.labels {
                q.push(("label".into(), l));
            }
            if let Some(f) = args.from {
                q.push(("from".into(), f));
            }
            if let Some(t) = args.text {
                q.push(("text".into(), t));
            }
            if let Some(a) = args.after {
                q.push(("after".into(), a));
            }
            if let Some(b) = args.before {
                q.push(("before".into(), b));
            }
            if let Some(n) = args.limit {
                q.push(("limit".into(), n.to_string()));
            }
            let val = if args.messages {
                client.list_messages(&q).await?
            } else {
                client.list_threads(&q).await?
            };
            print_output(&val, json_mode, |v| {
                if let Some(arr) = v["messages"].as_array() {
                    arr.iter()
                        .map(|m| {
                            format!(
                                "{} [{}] {}: {}",
                                m["id"].as_str().unwrap_or(""),
                                m["channel"].as_str().unwrap_or(""),
                                m["by"]["handle"].as_str().unwrap_or(""),
                                m["text"].as_str().unwrap_or("")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                } else if let Some(arr) = v["threads"].as_array() {
                    arr.iter()
                        .map(|t| {
                            format!(
                                "{} [{}] ({} replies) {}: {}",
                                t["message"]["id"].as_str().unwrap_or(""),
                                t["message"]["channel"].as_str().unwrap_or(""),
                                t["replies"].as_u64().unwrap_or(0),
                                t["message"]["by"]["handle"].as_str().unwrap_or(""),
                                t["message"]["text"].as_str().unwrap_or("")
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                } else {
                    serde_json::to_string_pretty(v).unwrap()
                }
            });
            Ok(0)
        }

        Command::Thread { id } => {
            let val = client.get_thread(&id).await?;
            print_output(&val, json_mode, format_thread_human);
            Ok(0)
        }

        Command::Posted { key } => {
            let val = client.get_posted(&key).await?;
            print_output(&val, json_mode, |v| {
                v["id"].as_str().unwrap_or("").to_string()
            });
            Ok(0)
        }

        Command::Edit(args) => {
            let text = read_text_arg(args.text, args.file)?;
            let val = client
                .edit_message(
                    &args.id,
                    &json!({ "text": text, "revision": args.revision, "schema": args.schema }),
                )
                .await?;
            print_output(&val, json_mode, |_| format!("edited {}", args.id));
            Ok(0)
        }

        Command::Delete(args) => {
            let val = client.delete_message(&args.id, args.revision).await?;
            print_output(&val, json_mode, |_| format!("deleted {}", args.id));
            Ok(0)
        }

        Command::React { id, emoji, remove } => {
            let val = client.react(&id, &emoji, remove).await?;
            print_output(&val, json_mode, |v| {
                format!(
                    "{} {} on {id}",
                    if remove { "removed" } else { "reacted" },
                    v["emoji"].as_str().unwrap_or(&emoji)
                )
            });
            Ok(0)
        }

        Command::Unreact { id, emoji } => {
            let val = client.react(&id, &emoji, true).await?;
            print_output(&val, json_mode, |v| {
                format!("removed {} on {id}", v["emoji"].as_str().unwrap_or(&emoji))
            });
            Ok(0)
        }

        Command::Label(args) => {
            let set = parse_kv_pairs(&args.set)?;
            let mut conds = serde_json::Map::new();
            for (k, v) in parse_kv_pairs(&args.if_eq)? {
                conds.insert(k, Value::String(v));
            }
            for k in args.if_unset {
                conds.insert(k, Value::Null);
            }
            let val = client
                .label(
                    &args.id,
                    &json!({
                        "set": set,
                        "unset": args.unset,
                        "if": conds
                    }),
                )
                .await?;
            print_output(&val, json_mode, |_| format!("labeled {}", args.id));
            Ok(0)
        }

        Command::Upload(args) => {
            let bytes = std::fs::read(&args.file)
                .map_err(|e| ClientError::usage(format!("failed reading file: {e}")))?;
            let filename = args.name.unwrap_or_else(|| {
                args.file
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("file.bin")
                    .to_string()
            });
            let val = client
                .upload_bytes(&filename, args.content_type.as_deref(), bytes)
                .await?;
            print_output(&val, json_mode, |v| {
                v["id"].as_str().unwrap_or("").to_string()
            });
            Ok(0)
        }

        Command::Download { id, path } => {
            let (hdrs, bytes) = client.download_attachment(&id).await?;
            let out_path = path.unwrap_or_else(|| {
                hdrs.get("content-disposition")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|cd| cd.split("filename=\"").nth(1))
                    .and_then(|s| s.split('"').next())
                    .unwrap_or(&id)
                    .to_string()
            });
            if out_path == "-" {
                use std::io::Write;
                std::io::stdout()
                    .write_all(&bytes)
                    .map_err(|e| ClientError::transport(format!("stdout write error: {e}")))?;
            } else {
                std::fs::write(&out_path, &bytes)
                    .map_err(|e| ClientError::usage(format!("failed writing '{out_path}': {e}")))?;
                let val = json!({ "id": id, "path": out_path, "size": bytes.len() });
                print_output(&val, json_mode, |_| {
                    format!("saved {} ({} bytes)", out_path, bytes.len())
                });
            }
            Ok(0)
        }

        Command::Poll(args) => {
            let q = build_event_query(&args);
            let val = client.poll_events(&q).await?;
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }

        Command::Tail(args) => run_tail_loop(&client, args, json_mode).await,

        Command::Wait(args) => {
            let mut q = Vec::new();
            if let Some(ref t) = args.target {
                if t.starts_with("msg_") {
                    q.push(("re".into(), t.clone()));
                    if args.after.is_none() {
                        q.push(("after".into(), t.clone()));
                    }
                } else {
                    q.push(("channel".into(), t.clone()));
                }
            }
            if let Some(re) = args.re {
                q.push(("re".into(), re));
            }
            if let Some(by) = args.by {
                q.push(("by".into(), by));
            }
            if let Some(m) = args.mentions {
                q.push(("mentions".into(), m));
            }
            for l in args.labels {
                q.push(("label".into(), l));
            }
            if let Some(f) = args.from {
                q.push(("from".into(), f));
            }
            if let Some(t) = args.text {
                q.push(("text".into(), t));
            }
            if let Some(a) = args.after {
                q.push(("after".into(), a));
            }
            if let Some(ty) = args.types {
                q.push(("types".into(), ty));
            }
            q.push(("timeout".into(), args.timeout));
            if args.all {
                q.push(("all".into(), "true".into()));
            }
            let val = tokio::select! {
                _ = tokio::signal::ctrl_c() => return Ok(130),
                res = client.wait_event(&q) => res?,
            };
            print_output(&val, json_mode, |v| {
                format!(
                    "{} {} {}",
                    v["event"]["cursor"].as_str().unwrap_or(""),
                    v["event"]["type"].as_str().unwrap_or(""),
                    v["event"]["message"]["id"].as_str().unwrap_or("")
                )
            });
            Ok(0)
        }

        Command::Tasks {
            channel,
            state,
            labels,
            by,
            owner,
            assigned_for,
            needs,
            after,
            limit,
            cmd,
        } => {
            let fallback_list = TasksCmd::List {
                channel,
                state,
                labels,
                by,
                owner,
                assigned_for,
                needs,
                after,
                limit,
            };
            handle_tasks(&client, cmd.unwrap_or(fallback_list), json_mode).await
        }

        Command::Schedule(args) => {
            if args.target.eq_ignore_ascii_case("cancel") {
                let id = args.text.ok_or_else(|| {
                    ClientError::usage("schedule cancel requires a schedule ID")
                })?;
                let val = client.cancel_schedule(&id).await?;
                print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
                return Ok(0);
            }
            let val = create_schedule_from_args(&client, *args).await?;
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }

        Command::Schedules {
            id,
            re,
            channel,
            by,
            all,
            cmd,
        } => {
            let resolved_cmd = match cmd {
                Some(c) => c,
                None => match id {
                    Some(sched_id) => SchedulesCmd::Get { id: sched_id },
                    None => SchedulesCmd::List {
                        re,
                        channel,
                        by,
                        all,
                    },
                },
            };
            let val = match resolved_cmd {
                SchedulesCmd::List {
                    re,
                    channel,
                    by,
                    all,
                } => {
                    let mut q = Vec::new();
                    if let Some(v) = re {
                        q.push(("re".into(), v));
                    }
                    if let Some(v) = channel {
                        q.push(("channel".into(), v));
                    }
                    if let Some(v) = by {
                        q.push(("by".into(), v));
                    }
                    if all {
                        q.push(("all".into(), "true".into()));
                    }
                    client.list_schedules(&q).await?
                }
                SchedulesCmd::Get { id } => client.get_schedule(&id).await?,
                SchedulesCmd::Create(args) => create_schedule_from_args(&client, *args).await?,
                SchedulesCmd::Cancel { id } => client.cancel_schedule(&id).await?,
            };
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }

        Command::Watchers { cmd } => {
            let val = match cmd.unwrap_or(WatchersCmd::List) {
                WatchersCmd::List => client.list_watchers().await?,
                WatchersCmd::Create {
                    id,
                    tail,
                    webhook_url,
                    secret,
                } => {
                    client
                        .create_watcher(&json!({
                            "id": id,
                            "tail": tail,
                            "url": webhook_url,
                            "secret": secret
                        }))
                        .await?
                }
                WatchersCmd::Sync { file } => {
                    let raw = std::fs::read_to_string(&file)
                        .map_err(|e| ClientError::usage(format!("failed reading file: {e}")))?;
                    let body: Value = serde_json::from_str(&raw)
                        .map_err(|e| ClientError::usage(format!("invalid JSON: {e}")))?;
                    client.sync_watchers(&body).await?
                }
                WatchersCmd::Delete { id } => client.delete_watcher(&id).await?,
            };
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }

        Command::Sync { id, target } => {
            let val = match target {
                Some(t) => client.sync_link_thread(&id, &t).await?,
                None => client.sync_list_thread_links(&id).await?,
            };
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }
        Command::Sources { key, action } => {
            let val = match (key, action) {
                (None, None) => client.sync_list_sources().await?,
                (Some(k), Some(act)) if act.eq_ignore_ascii_case("read") => {
                    client.sync_records(&k).await?
                }
                _ => {
                    return Err(ClientError::usage(
                        "usage: ob sources OR ob sources <key> read",
                    ));
                }
            };
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }
        Command::Inbox { key } => {
            let val = client.sync_inbox(&key).await?;
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }
        Command::Linked { url } => {
            let val = client.sync_linked(&url).await?;
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }
        Command::Typing { id, status } => {
            let val = client.sync_typing(&id, status.as_deref()).await?;
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }
    }
}

async fn create_schedule_from_args(
    client: &BoardClient,
    args: ScheduleSingularArgs,
) -> Result<Value, ClientError> {
    let text = read_text_arg(args.text, args.file)?;
    let attachments = resolve_attachment_args(client, &args.attachments).await?;
    let mut body = json!({
        "text": text,
        "labels": parse_kv_pairs(&args.labels)?,
        "attachments": attachments,
        "schema": args.schema,
        "noSync": args.no_sync
    });
    if args.target.starts_with("msg_") {
        body["re"] = json!(args.target);
    } else {
        body["channel"] = json!(args.target);
    }
    if let Some(v) = args.delay_in {
        body["in"] = json!(v);
    }
    if let Some(v) = args.at {
        body["at"] = json!(v);
    }
    if let Some(v) = args.every {
        body["every"] = json!(v);
    }
    if let Some(v) = args.repeat_for {
        body["for"] = json!(v);
    }
    if let Some(v) = args.times {
        body["times"] = json!(v);
    }
    if let Some(v) = args.until {
        body["until"] = json!(v);
    }
    client.create_schedule(&body).await
}

fn format_thread_human(v: &Value) -> String {
    let mut lines = Vec::new();
    let root = &v["message"];
    lines.push(format!(
        "{} [{}] {}: {}",
        root["id"].as_str().unwrap_or(""),
        root["channel"].as_str().unwrap_or(""),
        root["by"]["handle"].as_str().unwrap_or(""),
        root["text"].as_str().unwrap_or("")
    ));
    if let Some(replies) = v["replies"].as_array() {
        for r in replies {
            lines.push(format!(
                "  {} {}: {}",
                r["id"].as_str().unwrap_or(""),
                r["by"]["handle"].as_str().unwrap_or(""),
                r["text"].as_str().unwrap_or("")
            ));
        }
    }
    lines.join("\n")
}

fn build_event_query(args: &EventFilterArgs) -> Vec<(String, String)> {
    let mut q = Vec::new();
    if let Some(ch) = &args.channel {
        q.push(("channel".into(), ch.clone()));
    }
    if let Some(re) = &args.re {
        q.push(("re".into(), re.clone()));
    }
    if let Some(by) = &args.by {
        q.push(("by".into(), by.clone()));
    }
    if let Some(m) = &args.mentions {
        q.push(("mentions".into(), m.clone()));
    }
    for l in &args.labels {
        q.push(("label".into(), l.clone()));
    }
    if let Some(f) = &args.from {
        q.push(("from".into(), f.clone()));
    }
    if let Some(t) = &args.text {
        q.push(("text".into(), t.clone()));
    }
    if let Some(a) = &args.after {
        q.push(("after".into(), a.clone()));
    }
    if let Some(ty) = &args.types {
        q.push(("types".into(), ty.clone()));
    }
    if let Some(n) = args.limit {
        q.push(("limit".into(), n.to_string()));
    }
    if let Some(w) = &args.wait {
        q.push(("wait".into(), w.clone()));
    }
    if args.all {
        q.push(("all".into(), "true".into()));
    }
    q
}

async fn run_tail_loop(
    client: &BoardClient,
    mut args: EventFilterArgs,
    json_mode: bool,
) -> Result<i32, ClientError> {
    let mut cursor = args.after.take();
    let mut backoff = Duration::from_secs(1);

    loop {
        let mut q = build_event_query(&args);
        if let Some(ref c) = cursor {
            q.push(("after".into(), c.clone()));
        }
        if !q.iter().any(|(k, _)| k == "wait") {
            q.push(("wait".into(), "25".into()));
        }

        let poll_fut = client.poll_events(&q);
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                return Ok(130);
            }
            res = poll_fut => {
                match res {
                    Ok(page) => {
                        backoff = Duration::from_secs(1);
                        if let Some(c) = page["cursor"].as_str() {
                            cursor = Some(c.to_string());
                        }
                        if let Some(events) = page["events"].as_array() {
                            for ev in events {
                                if json_mode {
                                    println!("{}", serde_json::to_string(ev).unwrap());
                                } else {
                                    println!(
                                        "{} {} {}",
                                        ev["cursor"].as_str().unwrap_or(""),
                                        ev["type"].as_str().unwrap_or(""),
                                        ev["message"]["id"].as_str().unwrap_or("")
                                    );
                                }
                            }
                        }
                    }
                    Err(err) if (400..500).contains(&err.status) && err.status != 429 => {
                        return Err(err);
                    }
                    Err(err) => {
                        eprintln!("tail reconnecting after error: {err}");
                        tokio::select! {
                            _ = tokio::signal::ctrl_c() => return Ok(130),
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                    }
                }
            }
        }
    }
}

async fn handle_tasks(
    client: &BoardClient,
    cmd: TasksCmd,
    json_mode: bool,
) -> Result<i32, ClientError> {
    let board = client.config.board.clone();
    let profile = client.config.profile.clone();

    match cmd {
        TasksCmd::List {
            channel,
            state,
            labels,
            by,
            owner,
            assigned_for,
            needs,
            after,
            limit,
        } => {
            let mut q = Vec::new();
            if let Some(v) = channel {
                q.push(("channel".into(), v));
            }
            if let Some(v) = state {
                q.push(("state".into(), v));
            }
            for l in labels {
                q.push(("label".into(), l));
            }
            if let Some(v) = by {
                q.push(("by".into(), v));
            }
            if let Some(v) = owner {
                q.push(("owner".into(), v));
            }
            if let Some(v) = assigned_for {
                q.push(("for".into(), v));
            }
            if let Some(v) = needs {
                q.push(("needs".into(), v));
            }
            if let Some(v) = after {
                q.push(("after".into(), v));
            }
            if let Some(n) = limit {
                q.push(("limit".into(), n.to_string()));
            }
            let val = client.list_tasks(&q).await?;
            print_output(&val, json_mode, |v| {
                v["tasks"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .map(|t| {
                                format!(
                                    "{} [{}] ({}) {}",
                                    t["message"]["id"].as_str().unwrap_or(""),
                                    t["message"]["channel"].as_str().unwrap_or(""),
                                    t["message"]["task"]["state"].as_str().unwrap_or(""),
                                    t["message"]["text"].as_str().unwrap_or("")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            });
            Ok(0)
        }

        TasksCmd::Create {
            target,
            text,
            file,
            labels,
            needs,
            assigned_for,
            claim,
            lease,
            id,
            no_sync,
        } => {
            if let Some(plan_file) = file {
                let raw = read_text_arg(None, Some(plan_file))?;
                let tasks_val: Value = serde_json::from_str(&raw)
                    .map_err(|e| ClientError::usage(format!("invalid tasks plan JSON: {e}")))?;
                let val = client
                    .create_many_tasks(&json!({ "channel": target, "tasks": tasks_val, "id": id }))
                    .await?;
                print_output(&val, json_mode, |v| {
                    v["tasks"]
                        .as_array()
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|t| t["id"].as_str())
                                .collect::<Vec<_>>()
                                .join("\n")
                        })
                        .unwrap_or_default()
                });
                return Ok(0);
            }
            let mut body = json!({
                "labels": parse_kv_pairs(&labels)?,
                "needs": needs,
                "for": assigned_for,
                "claim": claim,
                "lease": lease,
                "id": id,
                "noSync": no_sync
            });
            if target.starts_with("msg_") {
                body["re"] = json!(target);
            } else {
                body["channel"] = json!(target);
                body["text"] = json!(text.unwrap_or_default());
            }
            let val = client.create_task(&body).await?;
            if let Some(c) = val.get("claim").filter(|c| c.is_object()) {
                if let (Some(tid), Some(epoch), Some(tok), Some(until)) = (
                    c["task"]["id"].as_str(),
                    c["epoch"].as_i64(),
                    c["token"].as_str(),
                    c["until"].as_str(),
                ) {
                    let _ = save_claim(
                        &board,
                        profile.as_deref(),
                        &SavedClaim {
                            task_id: tid.to_string(),
                            epoch,
                            token: tok.to_string(),
                            until: until.to_string(),
                            profile: profile.clone(),
                        },
                    );
                }
            }
            print_output(&val, json_mode, |v| {
                v["task"]["id"].as_str().unwrap_or("").to_string()
            });
            Ok(0)
        }

        TasksCmd::CreateMany { channel, file, id } => {
            let raw = read_text_arg(None, Some(file))?;
            let tasks_val: Value = serde_json::from_str(&raw)
                .map_err(|e| ClientError::usage(format!("invalid tasks plan JSON: {e}")))?;
            let val = client
                .create_many_tasks(&json!({ "channel": channel, "tasks": tasks_val, "id": id }))
                .await?;
            print_output(&val, json_mode, |v| {
                v["tasks"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|t| t["id"].as_str())
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default()
            });
            Ok(0)
        }

        TasksCmd::Get { id } => {
            let val = client.get_task(&id).await?;
            print_output(&val, json_mode, |v| serde_json::to_string_pretty(v).unwrap());
            Ok(0)
        }

        TasksCmd::Claim {
            target,
            ready,
            labels,
            from,
            wait,
            lease,
            lease_ms,
        } => {
            let val = if ready {
                let mut body = json!({
                    "channel": target,
                    "from": from,
                    "lease": lease,
                    "lease_ms": lease_ms
                });
                if !labels.is_empty() {
                    body["label"] = json!(labels);
                }
                if let Some(w) = wait {
                    body["wait"] = json!(w);
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => return Ok(130),
                        res = client.claim_ready(&body) => res?,
                    }
                } else {
                    client.claim_ready(&body).await?
                }
            } else {
                client
                    .claim_task(&target, &json!({ "lease": lease, "lease_ms": lease_ms }))
                    .await?
            };

            if let (Some(tid), Some(epoch), Some(tok), Some(until)) = (
                val["task"]["id"].as_str(),
                val["epoch"].as_i64(),
                val["token"].as_str(),
                val["until"].as_str(),
            ) {
                save_claim(
                    &board,
                    profile.as_deref(),
                    &SavedClaim {
                        task_id: tid.to_string(),
                        epoch,
                        token: tok.to_string(),
                        until: until.to_string(),
                        profile: profile.clone(),
                    },
                )?;
            }
            print_output(&val, json_mode, |v| {
                format!(
                    "{} (epoch {}, until {})",
                    v["task"]["id"].as_str().unwrap_or(""),
                    v["epoch"].as_i64().unwrap_or(0),
                    v["until"].as_str().unwrap_or("")
                )
            });
            Ok(0)
        }

        TasksCmd::Renew {
            id,
            epoch,
            claim_token,
            lease,
            lease_ms,
        } => {
            let (ep, tok) = resolve_claim_creds(&board, profile.as_deref(), &id, epoch, claim_token)?;
            let val = client
                .renew_task(
                    &id,
                    &json!({ "epoch": ep, "token": tok, "lease": lease, "lease_ms": lease_ms }),
                )
                .await?;
            if let Some(until) = val["until"].as_str() {
                let _ = save_claim(
                    &board,
                    profile.as_deref(),
                    &SavedClaim {
                        task_id: id.clone(),
                        epoch: ep,
                        token: tok,
                        until: until.to_string(),
                        profile: profile.clone(),
                    },
                );
            }
            print_output(&val, json_mode, |v| {
                format!("renewed {id} until {}", v["until"].as_str().unwrap_or(""))
            });
            Ok(0)
        }

        TasksCmd::Release {
            id,
            epoch,
            claim_token,
        } => {
            let (ep, tok) = resolve_claim_creds(&board, profile.as_deref(), &id, epoch, claim_token)?;
            let val = client
                .release_task(&id, &json!({ "epoch": ep, "token": tok }))
                .await?;
            remove_claim(&board, profile.as_deref(), &id);
            print_output(&val, json_mode, |_| format!("released {id}"));
            Ok(0)
        }

        TasksCmd::Complete {
            id,
            result,
            epoch,
            claim_token,
        } => {
            let (ep, tok) = resolve_claim_creds(&board, profile.as_deref(), &id, epoch, claim_token)?;
            let val = client
                .complete_task(&id, &json!({ "epoch": ep, "token": tok, "result": result }))
                .await?;
            remove_claim(&board, profile.as_deref(), &id);
            print_output(&val, json_mode, |_| format!("completed {id}"));
            Ok(0)
        }

        TasksCmd::Fail {
            id,
            reason,
            epoch,
            claim_token,
        } => {
            let (ep, tok) = resolve_claim_creds(&board, profile.as_deref(), &id, epoch, claim_token)?;
            let val = client
                .fail_task(&id, &json!({ "epoch": ep, "token": tok, "reason": reason }))
                .await?;
            remove_claim(&board, profile.as_deref(), &id);
            print_output(&val, json_mode, |_| format!("failed {id}"));
            Ok(0)
        }

        TasksCmd::Block {
            id,
            reason,
            kind,
            epoch,
            claim_token,
        } => {
            let mut body = json!({ "reason": reason, "kind": kind });
            if let (Some(e), Some(t)) = (epoch, claim_token) {
                body["epoch"] = json!(e);
                body["token"] = json!(t);
            } else if let Some(saved) = load_claim(&board, profile.as_deref(), &id) {
                body["epoch"] = json!(saved.epoch);
                body["token"] = json!(saved.token);
            }
            let val = client.block_task(&id, &body).await?;
            remove_claim(&board, profile.as_deref(), &id);
            print_output(&val, json_mode, |_| format!("blocked {id}"));
            Ok(0)
        }

        TasksCmd::Unblock { id } => {
            let val = client.unblock_task(&id).await?;
            print_output(&val, json_mode, |_| format!("unblocked {id}"));
            Ok(0)
        }

        TasksCmd::Cancel { id, reason } => {
            let val = client.cancel_task(&id, &reason).await?;
            remove_claim(&board, profile.as_deref(), &id);
            print_output(&val, json_mode, |_| format!("canceled {id}"));
            Ok(0)
        }

        TasksCmd::Reopen { id } => {
            let val = client.reopen_task(&id).await?;
            print_output(&val, json_mode, |_| format!("reopened {id}"));
            Ok(0)
        }

        TasksCmd::Assign { id, profile: target, pool } => {
            let target_opt = if pool { None } else { target.as_deref() };
            let val = client.assign_task(&id, target_opt).await?;
            print_output(&val, json_mode, |_| format!("assigned {id}"));
            Ok(0)
        }

        TasksCmd::Needs { id, needs, none } => {
            let list = if none { Vec::new() } else { needs };
            let val = client.update_task_needs(&id, &list).await?;
            print_output(&val, json_mode, |_| format!("updated needs on {id}"));
            Ok(0)
        }

        TasksCmd::Run {
            target,
            ready,
            labels,
            from,
            wait,
            lease,
            lease_ms,
            result,
            command,
        } => {
            run_task_harness(
                client,
                TaskRunOpts {
                    target,
                    ready,
                    labels,
                    from,
                    wait,
                    lease,
                    lease_ms,
                    result,
                    command,
                },
            )
            .await
        }
    }
}

struct TaskRunOpts {
    target: String,
    ready: bool,
    labels: Vec<String>,
    from: Vec<String>,
    wait: Option<String>,
    lease: Option<i64>,
    lease_ms: Option<i64>,
    result: Option<String>,
    command: Vec<String>,
}

async fn run_task_harness(client: &BoardClient, opts: TaskRunOpts) -> Result<i32, ClientError> {
    let effective_lease_ms = match (opts.lease, opts.lease_ms) {
        (_, Some(ms)) => ms.clamp(60, 300_000),
        (Some(s), None) => (s * 1000).clamp(1000, 300_000),
        (None, None) => 300_000,
    };

    let claim_val = if opts.ready {
        let mut body = json!({
            "channel": opts.target,
            "from": opts.from,
            "lease_ms": effective_lease_ms
        });
        if !opts.labels.is_empty() {
            body["label"] = json!(opts.labels);
        }
        if let Some(w) = opts.wait {
            body["wait"] = json!(w);
        }
        client.claim_ready(&body).await?
    } else {
        client
            .claim_task(&opts.target, &json!({ "lease_ms": effective_lease_ms }))
            .await?
    };

    let task_id = claim_val["task"]["id"]
        .as_str()
        .ok_or_else(|| ClientError::transport("missing task id in claim response"))?
        .to_string();
    let epoch = claim_val["epoch"]
        .as_i64()
        .ok_or_else(|| ClientError::transport("missing epoch in claim response"))?;
    let claim_token = claim_val["token"]
        .as_str()
        .ok_or_else(|| ClientError::transport("missing token in claim response"))?
        .to_string();

    let (program, args) = opts
        .command
        .split_first()
        .ok_or_else(|| ClientError::usage("tasks run requires a command after '--'"))?;

    let mut cmd = tokio::process::Command::new(program);
    cmd.args(args)
        .env("BOARD_TASK", &task_id)
        .env("BOARD_URL", &client.config.base_url)
        .env("BOARD_NAME", &client.config.board);
    if let Some(tok) = &client.config.token {
        cmd.env("BOARD_TOKEN", tok);
    }
    if let Some(prof) = &client.config.profile {
        cmd.env("BOARD_PROFILE", prof);
    }

    let mut child = cmd.spawn().map_err(|e| {
        ClientError::transport(format!("failed spawning command '{program}': {e}"))
    })?;

    // Renew lease every 1/3 of lease duration
    let renew_every = Duration::from_millis(((effective_lease_ms / 3) as u64).max(30));
    let mut renew_ticker = tokio::time::interval(renew_every);
    renew_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip immediate first tick
    renew_ticker.tick().await;

    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| ClientError::transport(format!("failed registering SIGTERM handler: {e}")))?;

    loop {
        #[cfg(unix)]
        let sigterm_fut = sigterm.recv();
        #[cfg(not(unix))]
        let sigterm_fut = std::future::pending::<Option<()>>();

        tokio::select! {
            status_res = child.wait() => {
                let status = status_res.map_err(|e| ClientError::transport(format!("wait failed: {e}")))?;
                let code = status.code().unwrap_or(1);
                if code == 0 {
                    let _ = client.complete_task(&task_id, &json!({
                        "epoch": epoch,
                        "token": claim_token,
                        "result": opts.result
                    })).await;
                } else {
                    let _ = client.fail_task(&task_id, &json!({
                        "epoch": epoch,
                        "token": claim_token,
                        "reason": format!("command exited with code {code}")
                    })).await;
                }
                return Ok(code);
            }
            _ = renew_ticker.tick() => {
                match client.renew_task(&task_id, &json!({
                    "epoch": epoch,
                    "token": claim_token,
                    "lease_ms": effective_lease_ms
                })).await {
                    Ok(_) => {}
                    Err(err) if err.code == "claim_lost" => {
                        let _ = child.kill().await;
                        let _ = child.wait().await;
                        eprintln!("error: claim lost for task {task_id}; stopped command");
                        return Ok(1);
                    }
                    Err(err) => {
                        eprintln!("warning: lease renew failed for {task_id}: {err}");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                let _ = client.release_task(&task_id, &json!({
                    "epoch": epoch,
                    "token": claim_token
                })).await;
                return Ok(130);
            }
            _ = sigterm_fut => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                let _ = client.release_task(&task_id, &json!({
                    "epoch": epoch,
                    "token": claim_token
                })).await;
                return Ok(143);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_global_options_with_precedence_and_boolean_overrides() {
        let file_cfg = CliFileConfig {
            schema: None,
            url: Some("http://cfg:8080".to_string()),
            board: Some("cfg-board".to_string()),
            token: Some("ob_cfg".to_string()),
            profile: Some("cfg-agent".to_string()),
            avatar: Some("https://example.com/cfg.png".to_string()),
            team: Some("cfg-team".to_string()),
            ensure: Some(true),
            json: Some(true),
        };

        // Bare command inherits all values from .openboard.json
        let cli = Cli::try_parse_from(["ob", "whoami"]).unwrap();
        let resolved = resolve_global_options(&cli, &file_cfg);
        assert_eq!(resolved.url.as_deref(), Some("http://cfg:8080"));
        assert_eq!(resolved.board.as_deref(), Some("cfg-board"));
        assert_eq!(resolved.token.as_deref(), Some("ob_cfg"));
        assert_eq!(resolved.profile.as_deref(), Some("cfg-agent"));
        assert_eq!(resolved.avatar.as_deref(), Some("https://example.com/cfg.png"));
        assert_eq!(resolved.team.as_deref(), Some("cfg-team"));
        assert!(resolved.ensure);
        assert!(resolved.json);

        // Explicit CLI flags override .openboard.json (including --json=false / --ensure=false)
        let cli = Cli::try_parse_from([
            "ob",
            "--url",
            "http://cli:9090",
            "--board",
            "cli-board",
            "--token",
            "ob_cli",
            "--as",
            "cli-agent",
            "--avatar",
            "https://example.com/cli.png",
            "--team",
            "cli-team",
            "--ensure=false",
            "--json=false",
            "whoami",
        ])
        .unwrap();
        let resolved = resolve_global_options(&cli, &file_cfg);
        assert_eq!(resolved.url.as_deref(), Some("http://cli:9090"));
        assert_eq!(resolved.board.as_deref(), Some("cli-board"));
        assert_eq!(resolved.token.as_deref(), Some("ob_cli"));
        assert_eq!(resolved.profile.as_deref(), Some("cli-agent"));
        assert_eq!(resolved.avatar.as_deref(), Some("https://example.com/cli.png"));
        assert_eq!(resolved.team.as_deref(), Some("cli-team"));
        assert!(!resolved.ensure);
        assert!(!resolved.json);

        // Bare --json and --ensure work before or after subcommands without consuming positional args
        let cli = Cli::try_parse_from(["ob", "--json", "--ensure", "post", "dev", "hello"]).unwrap();
        assert_eq!(cli.json, Some(true));
        assert_eq!(cli.ensure, Some(true));
        let cli2 = Cli::try_parse_from(["ob", "post", "dev", "hello", "--json"]).unwrap();
        assert_eq!(cli2.json, Some(true));

        // Full board URL from CLI/env beats config's board/team
        let cli = Cli::try_parse_from([
            "ob",
            "--url",
            "http://env:9090/v1/messageboard/boards/url-board?teamId=url-team",
            "whoami",
        ])
        .unwrap();
        let resolved = resolve_global_options(&cli, &file_cfg);
        assert_eq!(resolved.board.as_deref(), Some("url-board"));
        assert_eq!(resolved.team.as_deref(), Some("url-team"));
    }

    #[test]
    fn parses_explicit_config_flag() {
        let cli = Cli::try_parse_from(["ob", "--config", "/tmp/custom.json", "whoami"]).unwrap();
        assert_eq!(cli.config, Some(PathBuf::from("/tmp/custom.json")));
    }
}
