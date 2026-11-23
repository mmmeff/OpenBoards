pub mod actors;
pub mod api;
pub mod attachments;
pub mod boards;
pub mod channels;
pub mod cli;
pub mod client;
pub mod config;
pub mod conformance;
pub mod db;
pub mod error;
pub mod events;
pub mod filter;
pub mod grants;
pub mod id;
pub mod labels;
pub mod mentions;
pub mod messages;
pub mod permissions;
pub mod proxy;
pub mod ratelimit;
pub mod schedules;
pub mod schemas;
pub mod sync;
pub mod tasks;
pub mod validate;
pub mod watchers;

use std::sync::Arc;

use axum::Router;
use tokio::sync::Notify;

use crate::config::ServerConfig;
use crate::db::Database;
use crate::error::BoardResult;
use crate::events::EventHub;
use crate::id::new_bearer_token;
use crate::ratelimit::RateLimiter;

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub config: Arc<ServerConfig>,
    pub events: EventHub,
    pub rate_limiter: RateLimiter,
    pub watcher_wake: Arc<Notify>,
    pub sync_wake: Arc<Notify>,
}

impl AppState {
    pub fn new(db: Database, config: ServerConfig) -> Self {
        let rate_limiter = RateLimiter::new(config.rate_limit_rps, config.rate_limit_burst);
        Self {
            db,
            config: Arc::new(config),
            events: EventHub::new(),
            rate_limiter,
            watcher_wake: Arc::new(Notify::new()),
            sync_wake: Arc::new(Notify::new()),
        }
    }
}

pub fn build_router(state: AppState) -> Router {
    api::router(state)
}

pub async fn serve(config: ServerConfig) -> BoardResult<()> {
    serve_inner(config, false).await
}

/// Local development server mode (`openboards serve --local`):
/// - Defaults data dir to `./.board`
/// - Reuses or generates and persists a local owner token in `<data_dir>/token`
/// - Prints ready-to-paste `BOARD_URL` and `BOARD_TOKEN` exports
pub async fn serve_local(mut config: ServerConfig) -> BoardResult<()> {
    std::fs::create_dir_all(&config.data_dir).map_err(|e| {
        error::BoardError::internal(format!(
            "failed to create local data dir '{}': {e}",
            config.data_dir.display()
        ))
    })?;
    let tok_path = config.local_token_path();
    let local_token = if let Some(explicit) = config.admin_token.clone() {
        explicit
    } else if tok_path.exists() {
        std::fs::read_to_string(&tok_path)
            .map_err(|e| error::BoardError::internal(format!("failed reading local token: {e}")))?
            .trim()
            .to_string()
    } else {
        let generated = new_bearer_token();
        std::fs::write(&tok_path, format!("{generated}\n")).map_err(|e| {
            error::BoardError::internal(format!("failed persisting local token: {e}"))
        })?;
        generated
    };
    config.admin_token = Some(local_token);
    serve_inner(config, true).await
}

async fn serve_inner(config: ServerConfig, print_local_exports: bool) -> BoardResult<()> {
    let db = Database::open(&config.db_path())?;
    let local_token_for_print = config.admin_token.clone();
    if let Some(generated_token) = actors::bootstrap_owner(&db, config.admin_token.as_deref())? {
        tracing::warn!("bootstrapped initial owner token (shown once): {generated_token}");
    }
    boards::ensure_default_board(&db)?;
    schemas::seed_builtin_schemas(&db)?;

    let listen = config.listen;
    let team = config.team.clone();
    let data_dir = config.data_dir.clone();
    let state = AppState::new(db, config);
    let _scheduler_handle = schedules::spawn_scheduler_loop(state.clone());
    let _lease_reaper_handle = tasks::spawn_lease_reaper_loop(state.clone());
    let _watcher_dispatcher = watchers::spawn_watcher_dispatcher(state.clone());
    let _sync_worker = sync::spawn_sync_worker(state.clone());
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| error::BoardError::internal(format!("failed to bind {listen}: {e}")))?;
    let bound = listener.local_addr().unwrap_or(listen);

    if print_local_exports {
        println!("openboards local server ready (data dir: {})", data_dir.display());
        println!("export BOARD_URL=http://{bound}/v1/messageboard/boards/General?teamId={team}");
        if let Some(tok) = local_token_for_print {
            println!("export BOARD_TOKEN={tok}");
        }
    }

    tracing::info!("openboards listening on http://{bound}");

    axum::serve(listener, app)
        .await
        .map_err(|e| error::BoardError::internal(format!("server error: {e}")))
}
