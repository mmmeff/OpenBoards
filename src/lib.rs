#![forbid(unsafe_code)]

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

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::Router;
use tokio::sync::{watch, Notify};

use crate::attachments::BlobStore;
use crate::config::ServerConfig;
use crate::db::Database;
use crate::error::{BoardError, BoardResult};
use crate::events::EventHub;
use crate::id::{new_bearer_token, prefixed_id};
use crate::ratelimit::RateLimiter;

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub blobs: BlobStore,
    pub config: Arc<ServerConfig>,
    pub events: EventHub,
    pub rate_limiter: RateLimiter,
    pub tasks_notify: Arc<Notify>,
    pub watcher_wake: Arc<Notify>,
    pub sync_wake: Arc<Notify>,
    pub pg_listener_healthy: Arc<AtomicBool>,
    pub http: reqwest::Client,
    pub replica_id: String,
}

impl AppState {
    pub fn new(db: Database, config: ServerConfig) -> Self {
        let blobs = BlobStore::for_database(&db, config.data_dir.clone());
        let rate_limiter =
            RateLimiter::for_database(&db, config.rate_limit_rps, config.rate_limit_burst);
        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .pool_idle_timeout(Duration::from_secs(60))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            db,
            blobs,
            config: Arc::new(config),
            events: EventHub::new(),
            rate_limiter,
            tasks_notify: Arc::new(Notify::new()),
            watcher_wake: Arc::new(Notify::new()),
            sync_wake: Arc::new(Notify::new()),
            pg_listener_healthy: Arc::new(AtomicBool::new(true)),
            http,
            replica_id: prefixed_id("rep"),
        }
    }
}

pub(crate) fn never_shutdown_rx() -> watch::Receiver<bool> {
    static NEVER_TX: OnceLock<watch::Sender<bool>> = OnceLock::new();
    NEVER_TX.get_or_init(|| watch::channel(false).0).subscribe()
}

/// Manages all background worker tasks (and the PostgreSQL `PgListener` bridge when on Postgres)
/// with coordinated graceful shutdown via a `tokio::sync::watch` channel.
pub struct BackgroundWorkers {
    shutdown_tx: watch::Sender<bool>,
    handles: Vec<tokio::task::JoinHandle<()>>,
}

impl BackgroundWorkers {
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        let handles = std::mem::take(&mut self.handles);
        for handle in handles {
            let abort_handle = handle.abort_handle();
            if tokio::time::timeout(Duration::from_secs(3), handle)
                .await
                .is_err()
            {
                abort_handle.abort();
            }
        }
    }
}

impl Drop for BackgroundWorkers {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// Spawns all background loops (`PgListener` bridge on Postgres, scheduler, lease reaper,
/// watcher dispatcher, sync outbox worker) wired to one coordinated shutdown channel.
pub fn spawn_background_workers(state: AppState) -> BackgroundWorkers {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut handles = Vec::with_capacity(5);
    if let Some(pg_bridge) = events::spawn_pg_notify_bridge(state.clone(), shutdown_rx.clone()) {
        handles.push(pg_bridge);
    }
    handles.push(schedules::spawn_scheduler_loop_with_shutdown(
        state.clone(),
        shutdown_rx.clone(),
    ));
    handles.push(tasks::spawn_lease_reaper_loop_with_shutdown(
        state.clone(),
        shutdown_rx.clone(),
    ));
    handles.push(watchers::spawn_watcher_dispatcher_with_policy_and_shutdown(
        state.clone(),
        watchers::DeliveryPolicy::default(),
        shutdown_rx.clone(),
    ));
    handles.push(sync::spawn_sync_worker_with_shutdown(state, shutdown_rx));
    BackgroundWorkers {
        shutdown_tx,
        handles,
    }
}

pub fn build_router(state: AppState) -> Router {
    api::router(state)
}

pub async fn serve(config: ServerConfig) -> BoardResult<()> {
    serve_inner(config, false).await
}

/// Local development server mode (`ob serve --local`):
/// - Defaults data dir to `./.board`
/// - Reuses or generates and persists a local owner token in `<data_dir>/token`
/// - Prints ready-to-paste `BOARD_URL` and `BOARD_TOKEN` exports
pub async fn serve_local(mut config: ServerConfig) -> BoardResult<()> {
    if config.db_target.is_postgres() {
        return Err(BoardError::bad_request(
            "--local cannot be combined with a Postgres DATABASE_URL; unset DATABASE_URL or run without --local",
        ));
    }
    std::fs::create_dir_all(&config.data_dir).map_err(|e| {
        BoardError::internal(format!(
            "failed to create local data dir '{}': {e}",
            config.data_dir.display()
        ))
    })?;
    let tok_path = config.local_token_path();
    let local_token = if let Some(explicit) = config.admin_token.clone() {
        explicit
    } else if tok_path.exists() {
        std::fs::read_to_string(&tok_path)
            .map_err(|e| BoardError::internal(format!("failed reading local token: {e}")))?
            .trim()
            .to_string()
    } else {
        let generated = new_bearer_token();
        std::fs::write(&tok_path, format!("{generated}\n"))
            .map_err(|e| BoardError::internal(format!("failed persisting local token: {e}")))?;
        generated
    };
    config.admin_token = Some(local_token);
    serve_inner(config, true).await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                let _ = sig.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received; draining in-flight requests");
}

async fn serve_inner(config: ServerConfig, print_local_exports: bool) -> BoardResult<()> {
    let db = Database::connect_raw(&config.db_target, config.db_max_connections).await?;
    let local_token_for_print = config.admin_token.clone();
    let configured_admin_token = config.admin_token.clone();

    let generated_token = db
        .with_startup_lock(|| async {
            db.run_migrations().await?;
            let tok = actors::bootstrap_owner(&db, configured_admin_token.as_deref()).await?;
            boards::ensure_default_board(&db).await?;
            schemas::seed_builtin_schemas(&db).await?;
            Ok(tok)
        })
        .await?;

    if let Some(tok) = generated_token {
        tracing::warn!("bootstrapped initial owner token (shown once): {tok}");
    }

    tracing::info!("database ready: {}", config.db_target.redacted_display());

    let listen = config.listen;
    let team = config.team.clone();
    let data_dir = config.data_dir.clone();
    let state = AppState::new(db, config);
    let workers = spawn_background_workers(state.clone());
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| BoardError::internal(format!("failed to bind {listen}: {e}")))?;
    let bound = listener.local_addr().unwrap_or(listen);

    if print_local_exports {
        println!("ob local server ready (data dir: {})", data_dir.display());
        println!("export BOARD_URL=http://{bound}/v1/messageboard/boards/General?teamId={team}");
        if let Some(tok) = local_token_for_print {
            println!("export BOARD_TOKEN={tok}");
        }
    }

    tracing::info!("ob listening on http://{bound}");

    let serve_res = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| BoardError::internal(format!("server error: {e}")));

    workers.shutdown().await;

    serve_res
}
