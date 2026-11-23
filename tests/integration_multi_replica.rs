use std::fmt::Write as _;
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use chrono::{Duration as ChronoDuration, Utc};
use reqwest::Client;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use openboards::actors;
use openboards::boards;
use openboards::config::{DbTarget, ServerConfig, DEFAULT_DB_MAX_CONNECTIONS};
use openboards::db::{Database, Dialect, Queryable};
use openboards::schedules::format_utc_ms;
use openboards::schemas;
use openboards::{build_router, spawn_background_workers, sql_args, AppState, BackgroundWorkers};

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

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut hex = String::with_capacity(digest.len() * 2);
    for b in digest {
        let _ = write!(&mut hex, "{b:02x}");
    }
    hex
}

struct EphemeralPostgres {
    _tmp: tempfile::TempDir,
    pgdata: PathBuf,
    bin_dir: PathBuf,
    port: u16,
    stopped: bool,
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
        let opts = format!(
            "-k {} -h 127.0.0.1 -p {port} -F -c max_connections=200",
            sockdir.display()
        );
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
            stopped: false,
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

    fn stop_now(&mut self) {
        if !self.stopped {
            let _ = Command::new(self.bin_dir.join("pg_ctl"))
                .args([
                    "-D",
                    self.pgdata.to_str().unwrap(),
                    "-m",
                    "immediate",
                    "stop",
                ])
                .output();
            self.stopped = true;
        }
    }
}

impl Drop for EphemeralPostgres {
    fn drop(&mut self) {
        self.stop_now();
    }
}

struct LiveReplica {
    _tmp: tempfile::TempDir,
    data_dir: PathBuf,
    base_url: String,
    state: AppState,
    workers: Option<BackgroundWorkers>,
    server_handle: tokio::task::JoinHandle<()>,
}

impl LiveReplica {
    async fn boot(
        pg_url: &str,
        owner_token: &str,
        rate_limit_rps: u32,
        rate_limit_burst: u32,
    ) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("unused-pg-data-dir");
        let target = DbTarget::PostgresUrl(pg_url.to_string());
        let db = Database::connect_raw(&target, DEFAULT_DB_MAX_CONNECTIONS)
            .await
            .expect("connect_raw postgres");
        assert_eq!(db.dialect(), Dialect::Postgres);

        db.with_startup_lock(|| async {
            db.run_migrations().await?;
            let _ = actors::bootstrap_owner(&db, Some(owner_token)).await?;
            boards::ensure_default_board(&db).await?;
            schemas::seed_builtin_schemas(&db).await?;
            Ok(())
        })
        .await
        .expect("startup lock bootstrap failed");

        let config = ServerConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            data_dir: data_dir.clone(),
            team: "mrteam".to_string(),
            admin_token: Some(owner_token.to_string()),
            rate_limit_rps,
            rate_limit_burst,
            db_target: target,
            db_max_connections: DEFAULT_DB_MAX_CONNECTIONS,
        };
        let state = AppState::new(db, config);
        let workers = spawn_background_workers(state.clone());
        let app = build_router(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind replica");
        let addr: SocketAddr = listener.local_addr().unwrap();
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Self {
            _tmp: tmp,
            data_dir,
            base_url: format!("http://{addr}"),
            state,
            workers: Some(workers),
            server_handle,
        }
    }

    async fn wait_listener_ready(&self, client: &Client) {
        for _ in 0..50 {
            if let Ok(resp) = client.get(format!("{}/readyz", self.base_url)).send().await {
                if resp.status() == StatusCode::OK {
                    if let Ok(v) = resp.json::<Value>().await {
                        if v["ok"] == true && v["listener_ok"] == true {
                            return;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("replica {} did not become ready with listener_ok=true", self.base_url);
    }

    async fn shutdown(mut self) {
        self.server_handle.abort();
        if let Some(w) = self.workers.take() {
            w.shutdown().await;
        }
    }
}

impl Drop for LiveReplica {
    fn drop(&mut self) {
        self.server_handle.abort();
    }
}

async fn json_req(
    client: &Client,
    method: reqwest::Method,
    url: &str,
    token: Option<&str>,
    headers: &[(&str, &str)],
    body: Option<Value>,
) -> (StatusCode, Value) {
    let mut rb = client.request(method, url);
    if let Some(t) = token {
        rb = rb.header("authorization", format!("Bearer {t}"));
    }
    for &(k, v) in headers {
        rb = rb.header(k, v);
    }
    if let Some(b) = body {
        rb = rb.json(&b);
    }
    let resp = rb.send().await.expect("http request failed");
    let st = resp.status();
    let bytes = resp.bytes().await.unwrap_or_default();
    let val: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (st, val)
}

async fn create_user_token(
    client: &Client,
    base_url: &str,
    owner_token: &str,
    username: &str,
    role: &str,
) -> (String, String) {
    let (st, u) = json_req(
        client,
        reqwest::Method::POST,
        &format!("{base_url}/v1/admin/users"),
        Some(owner_token),
        &[],
        Some(json!({
            "username": username,
            "email": format!("{username}@mr.local"),
            "role": role
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "user={u}");
    let uid = u["user"]["id"].as_str().unwrap().to_string();
    let (st, t) = json_req(
        client,
        reqwest::Method::POST,
        &format!("{base_url}/v1/admin/tokens"),
        Some(owner_token),
        &[],
        Some(json!({ "principal_kind": "user", "principal_id": uid })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "tok={t}");
    (uid, t["token"]["token"].as_str().unwrap().to_string())
}

async fn create_project_token(
    client: &Client,
    base_url: &str,
    owner_token: &str,
    name: &str,
    env: &str,
) -> (String, String) {
    let (st, p) = json_req(
        client,
        reqwest::Method::POST,
        &format!("{base_url}/v1/admin/projects"),
        Some(owner_token),
        &[],
        Some(json!({ "name": name })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "proj={p}");
    let pid = p["project"]["id"].as_str().unwrap().to_string();
    let (st, t) = json_req(
        client,
        reqwest::Method::POST,
        &format!("{base_url}/v1/admin/tokens"),
        Some(owner_token),
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

/// Invariants 1, 2, 3, 4:
/// 1. Simultaneous cold-boot migration race across 2 replicas on an empty Postgres DB
/// 2. Cross-replica LISTEN/NOTIFY wakeups (`GET /events?wait=...`, `GET /wait`, `tasks claim --wait`)
/// 3. Concurrent `tasks claim --ready` race across A and B (exact 1 winner per task)
/// 4. Concurrent CAS & idempotency races across A and B
#[tokio::test]
async fn multi_replica_boot_notify_claim_and_cas_invariants() {
    let pg = EphemeralPostgres::start();
    let pg_url = pg.create_database("ob_mr_core");
    let owner_token = "mr-shared-owner-token";
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    // --- Invariant 1: Simultaneous cold-boot migration race across 2 replicas ---
    let (rep_a, rep_b) = tokio::join!(
        LiveReplica::boot(&pg_url, owner_token, 0, 500),
        LiveReplica::boot(&pg_url, owner_token, 0, 500),
    );
    rep_a.wait_listener_ready(&client).await;
    rep_b.wait_listener_ready(&client).await;

    assert_eq!(rep_a.state.db.schema_version().await.unwrap(), 13);
    assert_eq!(rep_b.state.db.schema_version().await.unwrap(), 13);
    assert_eq!(actors::list_users(&rep_a.state.db).await.unwrap().len(), 1);
    assert_eq!(schemas::list_schemas(&rep_a.state.db).await.unwrap().len(), 2);
    assert!(
        !rep_a.data_dir.exists() && !rep_b.data_dir.exists(),
        "Postgres replicas must not create local data_dir on disk"
    );

    let (_, alice_tok) = create_user_token(&client, &rep_a.base_url, owner_token, "alice", "member").await;
    let (_, bob_tok) = create_user_token(&client, &rep_b.base_url, owner_token, "bob", "member").await;

    // --- Invariant 2a: Cross-replica LISTEN/NOTIFY wakeup on GET /events?wait=... ---
    let poll_url = format!(
        "{}/v1/messageboard/boards/General/events?channel=wake&wait=10s",
        rep_b.base_url
    );
    let wait_url = format!(
        "{}/v1/messageboard/boards/General/wait?channel=wake&timeout=10s",
        rep_b.base_url
    );
    let c1 = client.clone();
    let t1 = alice_tok.clone();
    let poll_task = tokio::spawn(async move {
        let start = Instant::now();
        let res = json_req(&c1, reqwest::Method::GET, &poll_url, Some(&t1), &[], None).await;
        (res, start.elapsed())
    });
    let c2 = client.clone();
    let t2 = bob_tok.clone();
    let wait_task = tokio::spawn(async move {
        let start = Instant::now();
        let res = json_req(&c2, reqwest::Method::GET, &wait_url, Some(&t2), &[], None).await;
        (res, start.elapsed())
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    let (st, posted) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/messages", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "channel": "wake", "text": "hello from replica A" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "posted={posted}");
    let posted_id = posted["id"].as_str().unwrap().to_string();

    let ((poll_st, poll_body), poll_elapsed) = poll_task.await.unwrap();
    assert_eq!(poll_st, StatusCode::OK, "poll={poll_body}");
    assert_eq!(poll_body["events"].as_array().unwrap().len(), 1);
    assert_eq!(poll_body["events"][0]["message"]["id"], posted_id);
    assert!(
        poll_elapsed < Duration::from_secs(2),
        "long-poll on replica B should wake immediately via LISTEN/NOTIFY, took {poll_elapsed:?}"
    );

    let ((wait_st, wait_body), wait_elapsed) = wait_task.await.unwrap();
    assert_eq!(wait_st, StatusCode::OK, "wait={wait_body}");
    assert_eq!(wait_body["event"]["message"]["id"], posted_id);
    assert!(
        wait_elapsed < Duration::from_secs(2),
        "/wait on replica B should wake immediately via LISTEN/NOTIFY, took {wait_elapsed:?}"
    );

    // --- Invariant 2b: Cross-replica LISTEN/NOTIFY wakeup on tasks claim --ready --wait ---
    let claim_wait_url = format!(
        "{}/v1/messageboard/boards/General/tasks/claim-ready",
        rep_b.base_url
    );
    let c3 = client.clone();
    let t3 = alice_tok.clone();
    let claim_wait_task = tokio::spawn(async move {
        let start = Instant::now();
        let res = json_req(
            &c3,
            reqwest::Method::POST,
            &claim_wait_url,
            Some(&t3),
            &[("x-board-profile", "worker-b")],
            Some(json!({ "channel": "wake-tasks", "wait": "10s" })),
        )
        .await;
        (res, start.elapsed())
    });

    tokio::time::sleep(Duration::from_millis(150)).await;
    let (st, created_task) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/tasks", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "channel": "wake-tasks", "text": "cross-replica task" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "created_task={created_task}");
    let created_tid = created_task["task"]["id"].as_str().unwrap().to_string();

    let ((cw_st, cw_body), cw_elapsed) = claim_wait_task.await.unwrap();
    assert_eq!(cw_st, StatusCode::OK, "claim_wait={cw_body}");
    assert_eq!(cw_body["task"]["id"], created_tid);
    assert!(
        cw_elapsed < Duration::from_secs(2),
        "claim-ready --wait on replica B should wake immediately via ob_tasks NOTIFY, took {cw_elapsed:?}"
    );

    // --- Invariant 3: Concurrent tasks claim --ready race across A and B ---
    let batch_tasks: Vec<Value> = (1..=10)
        .map(|i| json!({ "key": format!("j{i}"), "text": format!("job {i}") }))
        .collect();
    let (st, batch) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/tasks/batch", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "channel": "race-q", "id": "race-batch-1", "tasks": batch_tasks })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "batch={batch}");
    assert_eq!(batch["tasks"].as_array().unwrap().len(), 10);

    let mut claim_handles = Vec::new();
    for idx in 0..12 {
        let c = client.clone();
        let tok = alice_tok.clone();
        let url = if idx % 2 == 0 {
            format!("{}/v1/messageboard/boards/General/tasks/claim-ready", rep_a.base_url)
        } else {
            format!("{}/v1/messageboard/boards/General/tasks/claim-ready", rep_b.base_url)
        };
        let prof = format!("w{idx}");
        claim_handles.push(tokio::spawn(async move {
            json_req(
                &c,
                reqwest::Method::POST,
                &url,
                Some(&tok),
                &[("x-board-profile", &prof)],
                Some(json!({ "channel": "race-q" })),
            )
            .await
        }));
    }

    let mut claimed_ids = Vec::new();
    let mut claimed_tokens = Vec::new();
    let mut nothing_ready = 0usize;
    for h in claim_handles {
        let (st, val) = h.await.unwrap();
        if st == StatusCode::OK {
            claimed_ids.push(val["task"]["id"].as_str().unwrap().to_string());
            claimed_tokens.push(val["token"].as_str().unwrap().to_string());
        } else {
            assert_eq!(st, StatusCode::NOT_FOUND, "unexpected error: {val}");
            assert_eq!(val["code"], "nothing_ready");
            nothing_ready += 1;
        }
    }
    assert_eq!(claimed_ids.len(), 10);
    assert_eq!(nothing_ready, 2);
    claimed_ids.sort();
    claimed_ids.dedup();
    assert_eq!(claimed_ids.len(), 10, "duplicate task claim across replicas!");
    claimed_tokens.sort();
    claimed_tokens.dedup();
    assert_eq!(claimed_tokens.len(), 10, "duplicate claim token across replicas!");

    // --- Invariant 4: Concurrent CAS & idempotency races across A and B ---
    let (st, root_msg) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/messages", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "channel": "cas", "text": "initial text" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);
    let root_id = root_msg["id"].as_str().unwrap().to_string();

    // 4a. Label CAS across A & B
    let lbl_url_a = format!(
        "{}/v1/messageboard/boards/General/messages/{root_id}/labels",
        rep_a.base_url
    );
    let lbl_url_b = format!(
        "{}/v1/messageboard/boards/General/messages/{root_id}/labels",
        rep_b.base_url
    );
    let (la, lb) = tokio::join!(
        json_req(
            &client,
            reqwest::Method::POST,
            &lbl_url_a,
            Some(&alice_tok),
            &[],
            Some(json!({ "set": { "owner": "alice" }, "if": { "owner": null } })),
        ),
        json_req(
            &client,
            reqwest::Method::POST,
            &lbl_url_b,
            Some(&bob_tok),
            &[],
            Some(json!({ "set": { "owner": "bob" }, "if": { "owner": null } })),
        ),
    );
    assert!(
        [la.0, lb.0].contains(&StatusCode::OK) && [la.0, lb.0].contains(&StatusCode::CONFLICT),
        "label CAS expected 1 OK + 1 CONFLICT, got la={la:?} lb={lb:?}"
    );

    // 4b. Edit revision CAS across A & B
    let edit_url_a = format!(
        "{}/v1/messageboard/boards/General/messages/{root_id}",
        rep_a.base_url
    );
    let edit_url_b = format!(
        "{}/v1/messageboard/boards/General/messages/{root_id}",
        rep_b.base_url
    );
    let (ea, eb) = tokio::join!(
        json_req(
            &client,
            reqwest::Method::PATCH,
            &edit_url_a,
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "edit from A", "revision": 1 })),
        ),
        json_req(
            &client,
            reqwest::Method::PATCH,
            &edit_url_b,
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "edit from B", "revision": 1 })),
        ),
    );
    assert!(
        [ea.0, eb.0].contains(&StatusCode::OK) && [ea.0, eb.0].contains(&StatusCode::CONFLICT),
        "edit CAS expected 1 OK + 1 CONFLICT, got ea={ea:?} eb={eb:?}"
    );

    // 4c. Reply `--after` guard across A & B
    let rep_url_a = format!(
        "{}/v1/messageboard/boards/General/messages/{root_id}/replies",
        rep_a.base_url
    );
    let rep_url_b = format!(
        "{}/v1/messageboard/boards/General/messages/{root_id}/replies",
        rep_b.base_url
    );
    let (ra, rb) = tokio::join!(
        json_req(
            &client,
            reqwest::Method::POST,
            &rep_url_a,
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "reply from A", "after": root_id })),
        ),
        json_req(
            &client,
            reqwest::Method::POST,
            &rep_url_b,
            Some(&bob_tok),
            &[],
            Some(json!({ "text": "reply from B", "after": root_id })),
        ),
    );
    assert!(
        [ra.0, rb.0].contains(&StatusCode::CREATED) && [ra.0, rb.0].contains(&StatusCode::CONFLICT),
        "reply --after expected 1 CREATED + 1 CONFLICT, got ra={ra:?} rb={rb:?}"
    );

    // 4d. Simultaneous idempotency key retries across A & B
    let post_url_a = format!("{}/v1/messageboard/boards/General/messages", rep_a.base_url);
    let post_url_b = format!("{}/v1/messageboard/boards/General/messages", rep_b.base_url);
    let (ia, ib) = tokio::join!(
        json_req(
            &client,
            reqwest::Method::POST,
            &post_url_a,
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "idem-race", "text": "idempotent msg", "id": "mr-idem-key-1" })),
        ),
        json_req(
            &client,
            reqwest::Method::POST,
            &post_url_b,
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "idem-race", "text": "idempotent msg", "id": "mr-idem-key-1" })),
        ),
    );
    assert!(
        matches!(ia.0, StatusCode::CREATED | StatusCode::OK),
        "ia={ia:?}"
    );
    assert!(
        matches!(ib.0, StatusCode::CREATED | StatusCode::OK),
        "ib={ib:?}"
    );
    assert_eq!(ia.1["id"], ib.1["id"], "idempotent race produced different IDs!");
    let (st, threads) = json_req(
        &client,
        reqwest::Method::GET,
        &format!("{}/v1/messageboard/boards/General/threads?channel=idem-race", rep_b.base_url),
        Some(&alice_tok),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(threads["threads"].as_array().unwrap().len(), 1);

    rep_a.shutdown().await;
    rep_b.shutdown().await;
}

/// Invariants 5, 6, 7, 8:
/// 5. Non-duplicate schedule execution with both replicas running schedulers
/// 6. Non-duplicate task lease reaping across both replicas
/// 7. Non-duplicate watcher webhook delivery across both replicas
/// 8. Non-duplicate sync outbox delivery across both replicas
#[tokio::test]
async fn multi_replica_background_workers_exactly_once() {
    let pg = EphemeralPostgres::start();
    let pg_url = pg.create_database("ob_mr_workers");
    let owner_token = "mr-workers-owner-token";
    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();

    let (rep_a, rep_b) = tokio::join!(
        LiveReplica::boot(&pg_url, owner_token, 0, 500),
        LiveReplica::boot(&pg_url, owner_token, 0, 500),
    );
    rep_a.wait_listener_ready(&client).await;
    rep_b.wait_listener_ready(&client).await;

    let (_, alice_tok) = create_user_token(&client, &rep_a.base_url, owner_token, "alice", "member").await;
    let (_, proj_tok) = create_project_token(&client, &rep_a.base_url, owner_token, "worker-app", "production").await;

    // Local HTTP receiver for both watcher webhooks & sync outbox (adds 80ms delay so replicas genuinely race!)
    type ReceivedLog = Arc<Mutex<Vec<(String, HeaderMap, Value)>>>;
    let received: ReceivedLog = Arc::new(Mutex::new(Vec::new()));
    let hook_app = Router::new()
        .route(
            "/watch",
            post(
                |State(log): State<ReceivedLog>,
                 headers: HeaderMap,
                 axum::Json(body): axum::Json<Value>| async move {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    log.lock().unwrap().push(("watch".to_string(), headers, body));
                    StatusCode::OK
                },
            ),
        )
        .route(
            "/sync-out",
            post(
                |State(log): State<ReceivedLog>,
                 headers: HeaderMap,
                 axum::Json(body): axum::Json<Value>| async move {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    log.lock().unwrap().push(("sync".to_string(), headers, body));
                    (
                        StatusCode::OK,
                        axum::Json(json!({
                            "platformMessageId": "pm_mr_1",
                            "platformUrl": "https://slack.test/mr/1"
                        })),
                    )
                },
            ),
        )
        .with_state(received.clone());
    let hook_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hook_addr = hook_listener.local_addr().unwrap();
    let hook_srv = tokio::spawn(async move {
        let _ = axum::serve(hook_listener, hook_app).await;
    });

    // --- Invariant 5: Non-duplicate schedule execution across both replicas ---
    let fire_at = Utc::now() + ChronoDuration::milliseconds(1100);
    let (st, sched_res) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/schedules", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({
            "channel": "sched-mr",
            "text": "single scheduled post",
            "at": format_utc_ms(fire_at)
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "sched={sched_res}");
    let sched_id = sched_res["schedule"]["id"].as_str().unwrap().to_string();

    // --- Invariant 6: Non-duplicate task lease reaping across both replicas ---
    let (st, created_t) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/tasks", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "channel": "reap-mr", "text": "expiring task" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "task={created_t}");
    let reap_tid = created_t["task"]["id"].as_str().unwrap().to_string();
    let (st, claimed_t) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/tasks/{reap_tid}/claim", rep_a.base_url),
        Some(&alice_tok),
        &[("x-board-profile", "ephemeral-worker")],
        Some(json!({ "lease_ms": 1100 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "claimed={claimed_t}");

    // --- Invariant 7: Non-duplicate watcher webhook delivery across both replicas ---
    let (st, w_res) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/watchers", rep_a.base_url),
        Some(&proj_tok),
        &[],
        Some(json!({
            "id": "mr-alerts",
            "tail": "alerts-mr",
            "url": format!("http://{hook_addr}/watch"),
            "secret": "whsec_mr"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "watcher={w_res}");

    // --- Invariant 8: Non-duplicate sync outbox delivery across both replicas ---
    let (st, s_res) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/sync/sources", rep_a.base_url),
        Some(&proj_tok),
        &[],
        Some(json!({
            "key": "slack",
            "platform": "slack",
            "direction": "outbound",
            "mappings": { "#mr-sync": "sync-mr" },
            "outboundUrl": format!("http://{hook_addr}/sync-out")
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "sync_source={s_res}");

    // Brief pause so replica B's watcher dispatcher subscribes after __watcher__ NOTIFY
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Trigger watcher & sync via Replica B
    let (st, _) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/messages", rep_b.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "channel": "alerts-mr", "text": "alert from B" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, _) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/messages", rep_b.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "channel": "sync-mr", "text": "sync payload from B" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);

    // Wait for schedule, lease reaper, watcher, and sync to all complete, plus grace window for duplicates
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let (_, sched_get) = json_req(
            &client,
            reqwest::Method::GET,
            &format!("{}/v1/messageboard/boards/General/schedules/{sched_id}", rep_b.base_url),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
        let (_, task_get) = json_req(
            &client,
            reqwest::Method::GET,
            &format!("{}/v1/messageboard/boards/General/tasks/{reap_tid}", rep_b.base_url),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
        let (watch_cnt, sync_cnt) = {
            let log = received.lock().unwrap();
            (
                log.iter().filter(|(k, _, _)| k == "watch").count(),
                log.iter().filter(|(k, _, _)| k == "sync").count(),
            )
        };
        if sched_get["schedule"]["status"] == "completed"
            && task_get["task"]["task"]["state"] == "ready"
            && watch_cnt >= 1
            && sync_cnt >= 1
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for background workers: sched={sched_get} task={task_get} watch_cnt={watch_cnt} sync_cnt={sync_cnt}"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // Extra settle window to ensure neither replica double-fires on subsequent ticks
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Verify Invariant 5: exactly 1 scheduled message, runsCompleted == 1
    let (st, sched_threads) = json_req(
        &client,
        reqwest::Method::GET,
        &format!("{}/v1/messageboard/boards/General/threads?channel=sched-mr", rep_a.base_url),
        Some(&alice_tok),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        sched_threads["threads"].as_array().unwrap().len(),
        1,
        "schedule double-posted across replicas!"
    );
    let (st, sched_final) = json_req(
        &client,
        reqwest::Method::GET,
        &format!("{}/v1/messageboard/boards/General/schedules/{sched_id}", rep_a.base_url),
        Some(&alice_tok),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(sched_final["schedule"]["runsCompleted"], 1);
    assert_eq!(sched_final["schedule"]["status"], "completed");

    // Verify Invariant 6: exactly 1 task.expired event across both replicas
    let (st, reap_events) = json_req(
        &client,
        reqwest::Method::GET,
        &format!("{}/v1/messageboard/boards/General/events?channel=reap-mr&after=0", rep_a.base_url),
        Some(&alice_tok),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let expired_count = reap_events["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["type"] == "task.expired")
        .count();
    let ready_count = reap_events["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["type"] == "task.ready")
        .count();
    assert_eq!(expired_count, 1, "duplicate task.expired events across replicas!");
    assert_eq!(ready_count, 2, "expected 1 initial + 1 expired task.ready event!");

    // Verify Invariants 7 & 8: exactly 1 watcher delivery and exactly 1 sync delivery
    {
        let log = received.lock().unwrap();
        let watch_deliveries: Vec<_> = log.iter().filter(|(k, _, _)| k == "watch").collect();
        let sync_deliveries: Vec<_> = log.iter().filter(|(k, _, _)| k == "sync").collect();
        assert_eq!(
            watch_deliveries.len(),
            1,
            "duplicate watcher webhook delivery across replicas: got {}",
            watch_deliveries.len()
        );
        assert_eq!(
            sync_deliveries.len(),
            1,
            "duplicate sync outbox delivery across replicas: got {}",
            sync_deliveries.len()
        );
    }

    hook_srv.abort();
    rep_a.shutdown().await;
    rep_b.shutdown().await;
}

/// Invariant 9: Cross-replica attachments:
/// - Inline upload on A -> download on B
/// - 5 MiB 3-step multi-part upload split across A and B (`init` on A, parts split across A & B,
///   `complete` on B, `download` on A) verifying exact SHA-256 match
/// - 5 MiB standard single-PUT streaming upload split across A and B
/// - Orphan staging cleanup removes abandoned staging chunks
#[tokio::test]
async fn multi_replica_cross_replica_attachments_and_cleanup() {
    let pg = EphemeralPostgres::start();
    let pg_url = pg.create_database("ob_mr_blobs");
    let owner_token = "mr-blobs-owner-token";
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let (rep_a, rep_b) = tokio::join!(
        LiveReplica::boot(&pg_url, owner_token, 0, 500),
        LiveReplica::boot(&pg_url, owner_token, 0, 500),
    );
    rep_a.wait_listener_ready(&client).await;
    rep_b.wait_listener_ready(&client).await;

    let (_, alice_tok) = create_user_token(&client, &rep_a.base_url, owner_token, "alice", "member").await;

    // 9a. Inline upload on Replica A -> download on Replica B
    let inline_bytes: Vec<u8> = (0..(128 * 1024)).map(|i| ((i * 31 + 7) % 251) as u8).collect();
    let inline_sha = sha256_hex(&inline_bytes);

    let resp = client
        .post(format!(
            "{}/v1/messageboard/boards/General/attachments?name=trace.bin",
            rep_a.base_url
        ))
        .header("authorization", format!("Bearer {alice_tok}"))
        .header("content-type", "application/octet-stream")
        .body(inline_bytes.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let inline_att: Value = resp.json().await.unwrap();
    let inline_id = inline_att["id"].as_str().unwrap().to_string();
    assert_eq!(inline_att["size"], inline_bytes.len());

    let dl = client
        .get(format!(
            "{}/v1/messageboard/boards/General/attachments/{inline_id}",
            rep_b.base_url
        ))
        .header("authorization", format!("Bearer {alice_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(dl.status(), StatusCode::OK);
    let dl_bytes = dl.bytes().await.unwrap();
    assert_eq!(dl_bytes.len(), inline_bytes.len());
    assert_eq!(sha256_hex(&dl_bytes), inline_sha);

    // 9b. 5 MiB 3-step multi-part upload split across A and B:
    //     init on A -> parts 0..5 alternating A, B, A, B, A -> complete on B -> download on A
    let five_mib = 5 * 1024 * 1024;
    let payload_5m: Vec<u8> = (0..five_mib).map(|i| ((i * 17 + 3) % 253) as u8).collect();
    let expected_sha_5m = sha256_hex(&payload_5m);

    let (st, init) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/attachments/uploads", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({
            "name": "split-5mib.bin",
            "type": "application/octet-stream",
            "size": five_mib
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED, "init={init}");
    let mp_id = init["id"].as_str().unwrap().to_string();
    let upload_path = init["upload_url"].as_str().unwrap().to_string();
    let complete_path = init["complete_url"].as_str().unwrap().to_string();

    for (part_idx, chunk) in payload_5m.chunks(1024 * 1024).enumerate() {
        let target_base = if part_idx % 2 == 0 {
            &rep_a.base_url
        } else {
            &rep_b.base_url
        };
        let put_resp = client
            .put(format!("{target_base}{upload_path}?part={part_idx}"))
            .header("authorization", format!("Bearer {alice_tok}"))
            .body(chunk.to_vec())
            .send()
            .await
            .unwrap();
        assert_eq!(put_resp.status(), StatusCode::OK);
        let put_json: Value = put_resp.json().await.unwrap();
        assert_eq!(put_json["size"], chunk.len());
    }

    let (st, comp) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}{complete_path}", rep_b.base_url),
        Some(&alice_tok),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "complete={comp}");
    assert_eq!(comp["attachment"]["id"], mp_id);
    assert_eq!(comp["attachment"]["size"], five_mib);

    let dl_5m = client
        .get(format!(
            "{}/v1/messageboard/boards/General/attachments/{mp_id}",
            rep_a.base_url
        ))
        .header("authorization", format!("Bearer {alice_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(dl_5m.status(), StatusCode::OK);
    let dl_5m_bytes = dl_5m.bytes().await.unwrap();
    assert_eq!(dl_5m_bytes.len(), five_mib);
    assert_eq!(
        sha256_hex(&dl_5m_bytes),
        expected_sha_5m,
        "5 MiB multi-part download SHA-256 mismatch!"
    );

    // 9c. Standard single-PUT 5 MiB streaming upload: init on A -> PUT on B -> complete on A -> download on B
    let (st, init2) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/attachments/uploads", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "name": "stream-5mib.bin", "type": "application/octet-stream" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);
    let s2_id = init2["id"].as_str().unwrap().to_string();
    let s2_up = init2["upload_url"].as_str().unwrap().to_string();
    let s2_comp = init2["complete_url"].as_str().unwrap().to_string();

    let put2 = client
        .put(format!("{}{s2_up}", rep_b.base_url))
        .header("authorization", format!("Bearer {alice_tok}"))
        .body(payload_5m.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(put2.status(), StatusCode::OK);

    let (st, comp2) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}{s2_comp}", rep_a.base_url),
        Some(&alice_tok),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK, "comp2={comp2}");
    assert_eq!(comp2["attachment"]["size"], five_mib);

    let dl2 = client
        .get(format!(
            "{}/v1/messageboard/boards/General/attachments/{s2_id}",
            rep_b.base_url
        ))
        .header("authorization", format!("Bearer {alice_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(dl2.status(), StatusCode::OK);
    let dl2_bytes = dl2.bytes().await.unwrap();
    assert_eq!(sha256_hex(&dl2_bytes), expected_sha_5m);

    // 9d. Orphan staging cleanup removes abandoned staging chunks
    let (st, orphan_init) = json_req(
        &client,
        reqwest::Method::POST,
        &format!("{}/v1/messageboard/boards/General/attachments/uploads", rep_a.base_url),
        Some(&alice_tok),
        &[],
        Some(json!({ "name": "abandoned.bin" })),
    )
    .await;
    assert_eq!(st, StatusCode::CREATED);
    let orphan_id = orphan_init["id"].as_str().unwrap().to_string();
    let orphan_up = orphan_init["upload_url"].as_str().unwrap().to_string();
    let put_orphan = client
        .put(format!("{}{orphan_up}?part=0", rep_b.base_url))
        .header("authorization", format!("Bearer {alice_tok}"))
        .body(vec![0xAAu8; 4096])
        .send()
        .await
        .unwrap();
    assert_eq!(put_orphan.status(), StatusCode::OK);

    // Age the abandoned attachment and run cleanup
    let old_ts = (Utc::now() - ChronoDuration::hours(25)).to_rfc3339();
    {
        let mut tx = rep_a.state.db.begin_tx().await.unwrap();
        tx.execute(
            "UPDATE attachments SET created_at = ?1 WHERE id = ?2",
            sql_args![&old_ts, &orphan_id],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let cleaned = rep_b
        .state
        .blobs
        .cleanup_expired_staging(&rep_b.state.db, ChronoDuration::hours(24))
        .await
        .unwrap();
    assert_eq!(cleaned, 1);

    let remaining_chunks = rep_a
        .state
        .db
        .read_conn()
        .fetch_one(
            "SELECT COUNT(*) FROM blob_staging_chunks WHERE attachment_id = ?1",
            sql_args![&orphan_id],
        )
        .await
        .unwrap()
        .get_i64(0)
        .unwrap();
    assert_eq!(remaining_chunks, 0);

    // Confirm zero local disk writes occurred in Postgres mode
    assert!(!rep_a.data_dir.exists() && !rep_b.data_dir.exists());

    rep_a.shutdown().await;
    rep_b.shutdown().await;
}

/// Invariant 10:
/// - Distributed rate limiting enforced across A + B combined (UNLOGGED TABLE atomic bucket)
/// - `GET /readyz` returns 200 when Postgres is healthy and 503 when Postgres is down
/// - Env-only `DATABASE_URL` rejection in `.openboard.json` and CLI flags
#[tokio::test]
async fn multi_replica_distributed_rate_limit_readyz_and_env_only_db_url() {
    let mut pg = EphemeralPostgres::start();
    let pg_url = pg.create_database("ob_mr_rl");
    let owner_token = "mr-rl-owner-token";
    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    // Shared rate limit: 1 req/s, burst 4 across BOTH replicas combined
    let (rep_a, rep_b) = tokio::join!(
        LiveReplica::boot(&pg_url, owner_token, 1, 4),
        LiveReplica::boot(&pg_url, owner_token, 1, 4),
    );
    rep_a.wait_listener_ready(&client).await;
    rep_b.wait_listener_ready(&client).await;

    let (_, alice_tok) = create_user_token(&client, &rep_a.base_url, owner_token, "alice", "member").await;
    let (_, bob_tok) = create_user_token(&client, &rep_a.base_url, owner_token, "bob", "member").await;

    // 4 requests alternating A -> B -> A -> B exhaust the shared burst of 4
    for i in 0..4 {
        let base = if i % 2 == 0 {
            &rep_a.base_url
        } else {
            &rep_b.base_url
        };
        let resp = client
            .get(format!("{base}/whoami"))
            .header("authorization", format!("Bearer {alice_tok}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "request {i} to {base} should succeed within shared burst=4"
        );
    }

    // 5th request on A and 6th request on B must BOTH be rate-limited (429 + Retry-After)
    for base in [&rep_a.base_url, &rep_b.base_url] {
        let resp = client
            .get(format!("{base}/whoami"))
            .header("authorization", format!("Bearer {alice_tok}"))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "expected shared 429 on {base}"
        );
        let retry_after = resp
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        assert!(retry_after >= 1, "missing/invalid Retry-After header");
    }

    // Independent principal (Bob) still has full burst on Replica B
    let bob_resp = client
        .get(format!("{}/whoami", rep_b.base_url))
        .header("authorization", format!("Bearer {bob_tok}"))
        .send()
        .await
        .unwrap();
    assert_eq!(bob_resp.status(), StatusCode::OK);

    // /readyz returns 200 while Postgres is up
    let (st, ready_ok) = json_req(
        &client,
        reqwest::Method::GET,
        &format!("{}/readyz", rep_a.base_url),
        None,
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(ready_ok["ok"], true);
    assert_eq!(ready_ok["dialect"], "postgres");
    assert_eq!(ready_ok["listener_ok"], true);

    // Stop Postgres -> /readyz must return 503
    pg.stop_now();
    let (st, ready_down) = json_req(
        &client,
        reqwest::Method::GET,
        &format!("{}/readyz", rep_b.base_url),
        None,
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "ready_down={ready_down}");
    assert_eq!(ready_down["ok"], false);

    // Env-only DATABASE_URL rejection in `.openboard.json` and CLI
    let bin = env!("CARGO_BIN_EXE_ob");

    let cfg_tmp = tempfile::tempdir().unwrap();
    let cfg_path = cfg_tmp.path().join(".openboard.json");
    std::fs::write(
        &cfg_path,
        json!({
            "url": "http://127.0.0.1:8080",
            "database_url": "postgres://user:secret_pw@127.0.0.1:5432/ob"
        })
        .to_string(),
    )
    .unwrap();
    let cfg_out = Command::new(bin)
        .args(["--config", cfg_path.to_str().unwrap(), "whoami"])
        .output()
        .expect("run ob with database_url in .openboard.json");
    assert!(
        !cfg_out.status.success(),
        ".openboard.json must reject database_url"
    );
    let cfg_stderr = String::from_utf8_lossy(&cfg_out.stderr);
    assert!(
        cfg_stderr.contains("unknown field"),
        "expected unknown field error, got: {cfg_stderr}"
    );

    let cli_flag_out = Command::new(bin)
        .args(["serve", "--database-url", &pg_url])
        .output()
        .expect("run ob serve --database-url");
    assert!(!cli_flag_out.status.success());

    rep_a.shutdown().await;
    rep_b.shutdown().await;
}
