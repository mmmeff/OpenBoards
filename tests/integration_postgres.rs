use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use chrono::{Duration as ChronoDuration, Utc};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use openboards::actors;
use openboards::boards;
use openboards::config::{DbTarget, ServerConfig, DEFAULT_DB_MAX_CONNECTIONS};
use openboards::db::{Database, Dialect};
use openboards::schedules::{self, format_utc_ms};
use openboards::schemas;
use openboards::sync;
use openboards::tasks;
use openboards::watchers::{self, hmac_sha256_hex, DeliveryPolicy};
use openboards::{build_router, AppState};

fn pg_bin_dir() -> PathBuf {
    for ver in ["17", "16", "15", "14"] {
        let candidate = PathBuf::from(format!("/usr/lib/postgresql/{ver}/bin"));
        if candidate.join("initdb").is_file() && candidate.join("pg_ctl").is_file() {
            return candidate;
        }
    }
    panic!("PostgreSQL binaries (initdb/pg_ctl) not found under /usr/lib/postgresql/*/bin");
}

fn free_local_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().unwrap().port()
}

struct EphemeralPostgres {
    _tmp: tempfile::TempDir,
    pgdata: PathBuf,
    bin_dir: PathBuf,
    port: u16,
}

impl EphemeralPostgres {
    fn start() -> Self {
        let bin_dir = pg_bin_dir();
        let tmp = tempfile::tempdir().expect("tempdir for pg");
        let pgdata = tmp.path().join("pgdata");
        let sockdir = tmp.path().join("sock");
        let logfile = tmp.path().join("postgres.log");
        std::fs::create_dir_all(&sockdir).unwrap();

        let init = Command::new(bin_dir.join("initdb"))
            .args([
                "-D",
                pgdata.to_str().unwrap(),
                "-U",
                "postgres",
                "--auth=trust",
                "--no-sync",
                "-E",
                "UTF8",
            ])
            .output()
            .expect("run initdb");
        assert!(
            init.status.success(),
            "initdb failed: stdout={} stderr={}",
            String::from_utf8_lossy(&init.stdout),
            String::from_utf8_lossy(&init.stderr)
        );

        let port = free_local_port();
        let opts = format!("-k {} -h 127.0.0.1 -p {port} -F", sockdir.display());
        let start = Command::new(bin_dir.join("pg_ctl"))
            .args([
                "-D",
                pgdata.to_str().unwrap(),
                "-l",
                logfile.to_str().unwrap(),
                "-o",
                &opts,
                "start",
                "-w",
            ])
            .output()
            .expect("run pg_ctl start");
        assert!(
            start.status.success(),
            "pg_ctl start failed: stdout={} stderr={} log={}",
            String::from_utf8_lossy(&start.stdout),
            String::from_utf8_lossy(&start.stderr),
            std::fs::read_to_string(&logfile).unwrap_or_default()
        );

        Self {
            _tmp: tmp,
            pgdata,
            bin_dir,
            port,
        }
    }

    fn create_database(&self, name: &str) -> String {
        let out = Command::new(self.bin_dir.join("createdb"))
            .args([
                "-h",
                "127.0.0.1",
                "-p",
                &self.port.to_string(),
                "-U",
                "postgres",
                name,
            ])
            .output()
            .expect("run createdb");
        assert!(
            out.status.success(),
            "createdb {name} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        format!("postgres://postgres@127.0.0.1:{}/{name}", self.port)
    }
}

impl Drop for EphemeralPostgres {
    fn drop(&mut self) {
        let _ = Command::new(self.bin_dir.join("pg_ctl"))
            .args([
                "-D",
                self.pgdata.to_str().unwrap(),
                "-m",
                "immediate",
                "stop",
            ])
            .output();
    }
}

struct PgHarness {
    _tmp: tempfile::TempDir,
    state: AppState,
    app: Router,
    owner_token: String,
    http_client: reqwest::Client,
}

impl PgHarness {
    async fn new(pg_url: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let owner_token = "owner-pg-bootstrap-secret".to_string();
        let target = DbTarget::PostgresUrl(pg_url.to_string());
        let db = Database::connect(&target, DEFAULT_DB_MAX_CONNECTIONS)
            .await
            .expect("connect postgres");
        assert_eq!(db.dialect(), Dialect::Postgres);

        actors::bootstrap_owner(&db, Some(&owner_token))
            .await
            .unwrap();
        boards::ensure_default_board(&db).await.unwrap();
        schemas::seed_builtin_schemas(&db).await.unwrap();

        let state = AppState::new(
            db,
            ServerConfig {
                listen: "127.0.0.1:8080".parse().unwrap(),
                data_dir: tmp.path().to_path_buf(),
                team: "pgteam".to_string(),
                admin_token: Some(owner_token.clone()),
                rate_limit_rps: 0,
                rate_limit_burst: 500,
                db_target: target,
                db_max_connections: DEFAULT_DB_MAX_CONNECTIONS,
            },
        );
        let app = build_router(state.clone());
        Self {
            _tmp: tmp,
            state,
            app,
            owner_token,
            http_client: reqwest::Client::new(),
        }
    }

    async fn req(
        &self,
        method: Method,
        uri: &str,
        token: Option<&str>,
        headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            builder = builder.header("authorization", format!("Bearer {t}"));
        }
        for &(k, v) in headers {
            builder = builder.header(k, v);
        }
        let req = match body {
            Some(v) => builder
                .header("content-type", "application/json")
                .body(Body::from(v.to_string()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        };
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, val)
    }

    async fn create_user_token(&self, username: &str, role: &str) -> (String, String) {
        let (st, u) = self
            .req(
                Method::POST,
                "/v1/admin/users",
                Some(&self.owner_token),
                &[],
                Some(json!({
                    "username": username,
                    "email": format!("{username}@pg.local"),
                    "role": role
                })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "user={u}");
        let uid = u["user"]["id"].as_str().unwrap().to_string();
        let (st, t) = self
            .req(
                Method::POST,
                "/v1/admin/tokens",
                Some(&self.owner_token),
                &[],
                Some(json!({ "principal_kind": "user", "principal_id": uid })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "tok={t}");
        (uid, t["token"]["token"].as_str().unwrap().to_string())
    }

    async fn create_project_token(&self, name: &str, env: &str) -> (String, String) {
        let (st, p) = self
            .req(
                Method::POST,
                "/v1/admin/projects",
                Some(&self.owner_token),
                &[],
                Some(json!({ "name": name })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "proj={p}");
        let pid = p["project"]["id"].as_str().unwrap().to_string();
        let (st, t) = self
            .req(
                Method::POST,
                "/v1/admin/tokens",
                Some(&self.owner_token),
                &[],
                Some(json!({
                    "principal_kind": "project",
                    "principal_id": pid,
                    "environment": env
                })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "ptok={t}");
        (pid, t["token"]["token"].as_str().unwrap().to_string())
    }
}

#[tokio::test]
async fn pg_migrations_startup_lock_readyz_discovery_and_env_only_validation() {
    let pg = EphemeralPostgres::start();
    let url = pg.create_database("ob_startup");
    let target = DbTarget::PostgresUrl(url.clone());

    // Simulate two replicas cold-booting simultaneously against the same empty PG database.
    let db_a = Database::connect_raw(&target, 10).await.unwrap();
    let db_b = Database::connect_raw(&target, 10).await.unwrap();

    let boot = |db: Database| async move {
        db.with_startup_lock(|| async {
            db.run_migrations().await?;
            let _ = actors::bootstrap_owner(&db, Some("shared-boot-token")).await?;
            boards::ensure_default_board(&db).await?;
            schemas::seed_builtin_schemas(&db).await?;
            Ok(())
        })
        .await
    };

    let (res_a, res_b) = tokio::join!(boot(db_a.clone()), boot(db_b.clone()));
    res_a.expect("replica A startup failed");
    res_b.expect("replica B startup failed");

    assert_eq!(db_a.schema_version().await.unwrap(), 12);
    assert_eq!(db_b.schema_version().await.unwrap(), 12);
    assert_eq!(actors::list_users(&db_a).await.unwrap().len(), 1);
    assert_eq!(schemas::list_schemas(&db_a).await.unwrap().len(), 2);

    let h = PgHarness::new(&url).await;

    // /healthz and /readyz
    let (st, health) = h.req(Method::GET, "/healthz", None, &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(health["ok"], true);

    let (st, ready) = h.req(Method::GET, "/readyz", None, &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(ready["ok"], true);
    assert_eq!(ready["dialect"], "postgres");

    // Discovery respects X-Forwarded-Proto
    let (st, disc) = h
        .req(
            Method::GET,
            "/discovery",
            None,
            &[("host", "boards.example.com"), ("x-forwarded-proto", "https")],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        disc["url"],
        "https://boards.example.com/v1/messageboard/boards/General?teamId=pgteam"
    );

    // Env-only DATABASE_URL CLI checks on the compiled `ob` binary
    let bin = env!("CARGO_BIN_EXE_ob");

    let cli_flag_rejected = Command::new(bin)
        .args(["serve", "--database-url", &url])
        .output()
        .expect("run ob serve --database-url");
    assert!(!cli_flag_rejected.status.success());

    let local_pg_rejected = Command::new(bin)
        .args(["serve", "--local"])
        .env("DATABASE_URL", "postgres://admin:super_secret_pw@127.0.0.1:5432/ob")
        .output()
        .expect("run ob serve --local with PG DATABASE_URL");
    assert!(!local_pg_rejected.status.success());
    let stderr = String::from_utf8_lossy(&local_pg_rejected.stderr);
    assert!(stderr.contains("--local"), "stderr={stderr}");
    assert!(!stderr.contains("super_secret_pw"), "password leaked: {stderr}");

    let bad_scheme = Command::new(bin)
        .args(["serve"])
        .env("DATABASE_URL", "mysql://root:top_secret_99@127.0.0.1/ob")
        .output()
        .expect("run ob serve with unsupported scheme");
    assert!(!bad_scheme.status.success());
    let stderr = String::from_utf8_lossy(&bad_scheme.stderr);
    assert!(stderr.contains("unsupported DATABASE_URL scheme"), "stderr={stderr}");
    assert!(!stderr.contains("top_secret_99"), "password leaked: {stderr}");
}

#[tokio::test]
async fn pg_auth_boards_channels_grants_messages_threads_labels_cas_and_schemas() {
    let pg = EphemeralPostgres::start();
    let url = pg.create_database("ob_core");
    let h = PgHarness::new(&url).await;

    let (_alice_id, alice_tok) = h.create_user_token("alice", "member").await;
    let (_bob_id, bob_tok) = h.create_user_token("bob", "member").await;

    // Boards uniqueness + ensure
    let (st, created) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards",
            Some(&alice_tok),
            &[],
            Some(json!({ "name": "Engineering" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(created["created"], true);

    let (st, dup) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards",
            Some(&alice_tok),
            &[],
            Some(json!({ "name": "engineering" })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(dup["code"], "board_name_taken");

    // Channels + LIKE ESCAPE '\' verification with `_` in channel slug
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[],
            Some(json!({ "path": "v2xbeta/sub" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // If `_` were unescaped in `LIKE 'v2_beta/%'`, `v2xbeta/sub` would falsely trigger 409 channel_exists!
    let (st, priv_ch) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&h.owner_token),
            &[],
            Some(json!({ "path": "v2_beta", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "priv_ch={priv_ch}");
    assert_eq!(priv_ch["grants"].as_array().unwrap().len(), 2);

    // Creating private channel when exact or descendant exists fails 409 channel_exists
    let (st, conflict_ch) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&h.owner_token),
            &[],
            Some(json!({ "path": "v2_beta", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(conflict_ch["code"], "channel_exists");

    // Messages, schema blocks, idempotency, replies, --after guard, reactions, threads summary index
    let mut root_ids = Vec::new();
    for i in 1..=5 {
        let (st, posted) = h
            .req(
                Method::POST,
                "/v1/messageboard/boards/General/messages",
                Some(&alice_tok),
                &[],
                Some(json!({
                    "channel": "dev",
                    "text": format!("thread {i}\n```board-block git-commit@1\n{{\"sha\":\"abc123{i}\",\"message\":\"commit {i}\"}}\n```"),
                    "id": format!("pg-idem-{i}"),
                    "labels": { "status": "open" }
                })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "posted={posted}");
        assert_eq!(posted["blocks"][0]["schema"], "git-commit");
        root_ids.push(posted["id"].as_str().unwrap().to_string());
    }

    // Idempotency replay returns same ID with 200
    let (st, replay) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "dev",
                "text": "ignored replay text",
                "id": "pg-idem-1"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(replay["id"], root_ids[0]);

    // Reply + --after guard + idempotent reaction
    let (st, reply1) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{}/replies", root_ids[0]),
            Some(&bob_tok),
            &[],
            Some(json!({ "text": "first reply", "after": root_ids[0] })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "reply1={reply1}");

    let (st, stale) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{}/replies", root_ids[0]),
            Some(&bob_tok),
            &[],
            Some(json!({ "text": "stale reply", "after": root_ids[0] })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(stale["code"], "newer_message");

    // Reactions (INSERT ... ON CONFLICT DO NOTHING idempotent)
    for _ in 0..2 {
        let (st, _) = h
            .req(
                Method::POST,
                &format!("/v1/messageboard/boards/General/messages/{}/reactions", root_ids[0]),
                Some(&bob_tok),
                &[],
                Some(json!({ "emoji": "+1" })),
            )
            .await;
        assert_eq!(st, StatusCode::OK);
    }

    // Migration 12 `threads` fast summary index + `older` pagination
    let (st, page1) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/threads?channel=dev&limit=2",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "page1={page1}");
    let t1 = page1["threads"].as_array().unwrap();
    assert_eq!(t1.len(), 2);
    // Most recently active thread is root_ids[0] (due to reply1)
    assert_eq!(t1[1]["message"]["id"], root_ids[0]);
    assert_eq!(t1[1]["replies"], 1);
    assert!(t1[1]["last"]["id"].is_string());
    let older = page1["older"].as_str().expect("older cursor present");

    let (st, page2) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/threads?channel=dev&limit=2&before={older}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(page2["threads"].as_array().unwrap().len(), 2);

    // Concurrent Label CAS race on Postgres: exactly 1 winner (200) and 1 loser (409 label_conflict)
    let target_id = root_ids[1].clone();
    let label_uri = format!("/v1/messageboard/boards/General/messages/{target_id}/labels");
    let (r1, r2) = tokio::join!(
        h.req(
            Method::POST,
            &label_uri,
            Some(&alice_tok),
            &[],
            Some(json!({ "set": { "owner": "alice" }, "if": { "owner": null } })),
        ),
        h.req(
            Method::POST,
            &label_uri,
            Some(&bob_tok),
            &[],
            Some(json!({ "set": { "owner": "bob" }, "if": { "owner": null } })),
        )
    );
    let statuses = [r1.0, r2.0];
    assert!(
        statuses.contains(&StatusCode::OK) && statuses.contains(&StatusCode::CONFLICT),
        "expected exactly 1 OK and 1 CONFLICT, got r1={:?} r2={:?}",
        r1,
        r2
    );
}

#[tokio::test]
async fn pg_tasks_concurrent_claim_dag_leases_and_schedules() {
    let pg = EphemeralPostgres::start();
    let url = pg.create_database("ob_tasks");
    let h = PgHarness::new(&url).await;

    let (_u1, alice_tok) = h.create_user_token("alice_eng", "member").await;

    // Create batch of 4 independent ready tasks + 1 dependent waiting task
    let (st, batch) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "queue",
                "id": "pg-batch-1",
                "tasks": [
                    { "key": "t1", "text": "job 1" },
                    { "key": "t2", "text": "job 2" },
                    { "key": "t3", "text": "job 3" },
                    { "key": "t4", "text": "job 4" },
                    { "key": "final", "text": "final", "needs": [{ "key": "t1" }] }
                ]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "batch={batch}");
    let tasks_arr = batch["tasks"].as_array().unwrap();
    let t1_id = tasks_arr[0]["id"].as_str().unwrap().to_string();
    let final_id = tasks_arr[4]["id"].as_str().unwrap().to_string();
    assert_eq!(tasks_arr[4]["task"]["state"], "waiting");

    // 6 concurrent claim-ready requests racing on 4 ready tasks -> 4 unique claimed, 2 nothing_ready
    let mut futs = Vec::new();
    for worker_idx in 0..6 {
        let tok = alice_tok.clone();
        let prof = format!("w{worker_idx}");
        let app = h.app.clone();
        futs.push(tokio::spawn(async move {
            let req = Request::builder()
                .method(Method::POST)
                .uri("/v1/messageboard/boards/General/tasks/claim-ready")
                .header("authorization", format!("Bearer {tok}"))
                .header("x-board-profile", prof)
                .header("content-type", "application/json")
                .body(Body::from(json!({ "channel": "queue" }).to_string()))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            let st = resp.status();
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            let val: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            (st, val)
        }));
    }

    let mut claimed_ids = Vec::new();
    let mut t1_claim: Option<(i64, String)> = None;
    let mut nothing_ready = 0usize;
    for f in futs {
        let (st, val) = f.await.unwrap();
        if st == StatusCode::OK {
            let tid = val["task"]["id"].as_str().unwrap().to_string();
            if tid == t1_id {
                t1_claim = Some((
                    val["epoch"].as_i64().unwrap(),
                    val["token"].as_str().unwrap().to_string(),
                ));
            }
            claimed_ids.push(tid);
        } else {
            assert_eq!(st, StatusCode::NOT_FOUND, "unexpected error: {val}");
            assert_eq!(val["code"], "nothing_ready");
            nothing_ready += 1;
        }
    }
    assert_eq!(claimed_ids.len(), 4);
    assert_eq!(nothing_ready, 2);
    claimed_ids.sort();
    claimed_ids.dedup();
    assert_eq!(claimed_ids.len(), 4, "duplicate task claim detected on Postgres!");

    // Complete t1 -> cascades `final` to ready
    let (epoch, token) = t1_claim.expect("t1 must have been claimed");
    let (st, comp) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{t1_id}/complete"),
            Some(&alice_tok),
            &[],
            Some(json!({ "epoch": epoch, "token": token, "result": "done" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "comp={comp}");
    assert_eq!(comp["readied"], json!([final_id]));

    // Lease expiration reaps remaining 3 claimed tasks
    let reaped = tasks::reap_expired_leases_at(
        &h.state.db,
        &h.state.events,
        Utc::now() + ChronoDuration::minutes(10),
    )
    .await
    .unwrap();
    assert_eq!(reaped, 3);

    // Schedules on Postgres
    let t0 = Utc::now() + ChronoDuration::minutes(5);
    let (st, sched) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "sched",
                "text": "scheduled pg message",
                "at": format_utc_ms(t0)
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "sched={sched}");

    let fired = schedules::fire_due_schedules_at(
        &h.state.db,
        &h.state.events,
        t0 + ChronoDuration::seconds(1),
    )
    .await
    .unwrap();
    assert_eq!(fired.fired, 1);
    assert_eq!(fired.failed, 0);
}

#[tokio::test]
async fn pg_watchers_and_sync_end_to_end() {
    let pg = EphemeralPostgres::start();
    let url = pg.create_database("ob_watch_sync");
    let h = PgHarness::new(&url).await;

    let (_pid, proj_tok) = h.create_project_token("pg_deploy", "production").await;

    // Mock webhook receiver for watcher + outbound sync
    type Store = Arc<Mutex<Vec<(String, HeaderMap, Value)>>>;
    let received: Store = Arc::new(Mutex::new(Vec::new()));
    let hook_app = Router::new()
        .route(
            "/watch",
            post(
                |State(store): State<Store>,
                 headers: HeaderMap,
                 axum::Json(body): axum::Json<Value>| async move {
                    store.lock().unwrap().push(("watch".into(), headers, body));
                    StatusCode::OK
                },
            ),
        )
        .route(
            "/sync-out",
            post(
                |State(store): State<Store>,
                 headers: HeaderMap,
                 axum::Json(body): axum::Json<Value>| async move {
                    store.lock().unwrap().push(("sync".into(), headers, body));
                    (
                        StatusCode::OK,
                        axum::Json(json!({
                            "platformMessageId": "pm_pg_1",
                            "platformUrl": "https://slack.test/pg/1"
                        })),
                    )
                },
            ),
        )
        .with_state(received.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hook_addr = listener.local_addr().unwrap();
    let hook_srv = tokio::spawn(async move {
        let _ = axum::serve(listener, hook_app).await;
    });

    // 1. Watcher registration, dispatch, cursor advance (CASE WHEN), and detail query (SUM BIGINT)
    let (st, w) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "pg-triage",
                "tail": "alerts",
                "url": format!("http://{hook_addr}/watch"),
                "secret": "whsec_pg"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "w={w}");

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&h.owner_token),
            &[],
            Some(json!({ "channel": "alerts", "text": "pg alert!" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let policy = DeliveryPolicy {
        max_attempts: 2,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(50),
        request_timeout: Duration::from_secs(5),
    };
    let report = watchers::dispatch_all_once(&h.state.db, &h.http_client, &policy)
        .await
        .unwrap();
    assert_eq!(report.delivered, 1);

    let (st, detail) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers/pg-triage",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "detail={detail}");
    assert_eq!(detail["watcher"]["deliveries"]["delivered"], 1);
    assert_eq!(detail["recentDeliveries"].as_array().unwrap().len(), 1);

    // 2. Sync source registration, signed inbound Slack webhook (BIGSERIAL sync_inbox), outbound delivery
    let (st, src) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&proj_tok),
            &[],
            Some(json!({
                "key": "slack",
                "platform": "slack",
                "mappings": { "#pg-sync": "pg-sync" },
                "webhookSecret": "slk_pg_secret",
                "outboundUrl": format!("http://{hook_addr}/sync-out")
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "src={src}");

    let hook_body = json!({
        "type": "event_callback",
        "event": {
            "type": "message",
            "channel": "#pg-sync",
            "ts": "1700001.1",
            "user": "U123",
            "user_name": "Malte",
            "text": "*pg* hello",
            "permalink": "https://acme.slack.com/archives/C9/p17000011"
        }
    })
    .to_string();
    let ts_hdr = "1700000000";
    let sig = hmac_sha256_hex(
        b"slk_pg_secret",
        format!("v0:{ts_hdr}:{hook_body}").as_bytes(),
    );

    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/messageboard/boards/General/webhooks/slack")
        .header("content-type", "application/json")
        .header("x-slack-request-timestamp", ts_hdr)
        .header("x-slack-signature", format!("v0={sig}"))
        .body(Body::from(hook_body))
        .unwrap();
    let resp = h.app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let hook_json: Value = serde_json::from_slice(&body_bytes).unwrap();
    let root_id = hook_json["messageId"].as_str().unwrap().to_string();

    // Verify sync_inbox + linked + outbound reply delivery
    let (st, inbox) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/sources/slack/inbox",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "inbox={inbox}");
    assert_eq!(inbox["inbox"].as_array().unwrap().len(), 1);
    assert!(inbox["inbox"][0]["id"].as_i64().unwrap() >= 1);

    let (st, linked) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/linked?url=https%3A%2F%2Facme.slack.com%2Farchives%2FC9%2Fp17000011",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK, "linked={linked}");
    assert_eq!(linked["message"]["id"], root_id);

    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/replies"),
            Some(&h.owner_token),
            &[],
            Some(json!({ "text": "**reply from pg**" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let sum = sync::process_pending_deliveries(&h.state).await.unwrap();
    assert_eq!(sum.delivered, 1);

    hook_srv.abort();
    let items = received.lock().unwrap();
    assert!(items.iter().any(|(k, _, _)| k == "watch"));
    assert!(items
        .iter()
        .any(|(k, _, b)| k == "sync" && b["text"] == "*reply from pg*"));
}
