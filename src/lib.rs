pub mod actors;
pub mod api;
pub mod attachments;
pub mod boards;
pub mod channels;
pub mod config;
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
pub mod ratelimit;
pub mod schemas;
pub mod validate;
pub mod watchers;

use std::sync::Arc;

use axum::Router;
use tokio::sync::Notify;

use crate::config::ServerConfig;
use crate::db::Database;
use crate::error::BoardResult;
use crate::events::EventHub;
use crate::ratelimit::RateLimiter;

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub config: Arc<ServerConfig>,
    pub events: EventHub,
    pub rate_limiter: RateLimiter,
    pub watcher_wake: Arc<Notify>,
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
        }
    }
}

pub fn build_router(state: AppState) -> Router {
    api::router(state)
}

pub async fn serve(config: ServerConfig) -> BoardResult<()> {
    let db = Database::open(&config.db_path())?;
    if let Some(generated_token) =
        actors::bootstrap_owner(&db, config.admin_token.as_deref())?
    {
        tracing::warn!(
            "bootstrapped initial owner token (shown once): {generated_token}"
        );
    }
    boards::ensure_default_board(&db)?;
    schemas::seed_builtin_schemas(&db)?;

    let listen = config.listen;
    let state = AppState::new(db, config);
    let _watcher_dispatcher = watchers::spawn_watcher_dispatcher(state.clone());
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| error::BoardError::internal(format!("failed to bind {listen}: {e}")))?;

    tracing::info!("openboards listening on http://{listen}");

    axum::serve(listener, app)
        .await
        .map_err(|e| error::BoardError::internal(format!("server error: {e}")))
}
