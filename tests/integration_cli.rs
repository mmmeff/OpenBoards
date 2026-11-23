use std::process::Command as StdCommand;
use std::time::Duration;

use serde_json::{json, Value};

use openboards::actors;
use openboards::boards;
use openboards::config::{DbTarget, ServerConfig, DEFAULT_DB_MAX_CONNECTIONS};
use openboards::db::Database;
use openboards::proxy::{build_proxy_router, ProxyConfig};
use openboards::{build_router, schedules, schemas, tasks, watchers, AppState};

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_ob")
}

struct LiveServer {
    _tmp: tempfile::TempDir,
    url: String,
    owner_token: String,
    server_handle: tokio::task::JoinHandle<()>,
    scheduler_handle: tokio::task::JoinHandle<()>,
    reaper_handle: tokio::task::JoinHandle<()>,
    watcher_handle: tokio::task::JoinHandle<()>,
}

impl Drop for LiveServer {
    fn drop(&mut self) {
        self.server_handle.abort();
        self.scheduler_handle.abort();
        self.reaper_handle.abort();
        self.watcher_handle.abort();
    }
}

async fn start_live_server() -> LiveServer {
    let tmp = tempfile::tempdir().unwrap();
    let owner_token = "ob_cli_test_owner_token_123456".to_string();
    let data_dir = tmp.path().to_path_buf();
    let db_path = data_dir.join("openboards.db");
    let cfg = ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir,
        team: "acme".to_string(),
        admin_token: Some(owner_token.clone()),
        rate_limit_rps: 0,
        rate_limit_burst: 500,
        db_target: DbTarget::SqlitePath(db_path.clone()),
        db_max_connections: DEFAULT_DB_MAX_CONNECTIONS,
    };
    let db = Database::open(&db_path).await.unwrap();
    actors::bootstrap_owner(&db, Some(&owner_token)).await.unwrap();
    boards::ensure_default_board(&db).await.unwrap();
    schemas::seed_builtin_schemas(&db).await.unwrap();

    let state = AppState::new(db, cfg);
    let scheduler_handle = schedules::spawn_scheduler_loop(state.clone());
    let reaper_handle = tasks::spawn_lease_reaper_loop(state.clone());
    let watcher_handle = watchers::spawn_watcher_dispatcher(state.clone());
    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    LiveServer {
        _tmp: tmp,
        url: format!("http://{addr}"),
        owner_token,
        server_handle,
        scheduler_handle,
        reaper_handle,
        watcher_handle,
    }
}

fn run_cli_cmd(
    url: &str,
    token: &str,
    claims_dir: &std::path::Path,
    args: &[&str],
) -> (i32, String, String) {
    let out = StdCommand::new(bin_path())
        .args(args)
        .env("BOARD_URL", url)
        .env("BOARD_TOKEN", token)
        .env("OPENBOARDS_CLAIMS_DIR", claims_dir)
        .output()
        .expect("failed executing ob binary");
    let code = out.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    (code, stdout, stderr)
}

#[tokio::test]
async fn cli_conformance_self_test_passes_all_checks() {
    let out = tokio::task::spawn_blocking(|| {
        StdCommand::new(bin_path())
            .args(["conformance", "--self-test", "--json"])
            .output()
            .expect("failed running conformance")
    })
    .await
    .unwrap();

    assert_eq!(
        out.status.code(),
        Some(0),
        "conformance failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let report: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(report["ok"], true);
    assert_eq!(report["failed"], 0);
    assert!(report["passed"].as_u64().unwrap() >= 13);
}

#[tokio::test]
async fn cli_commands_json_exit_codes_and_claim_persistence() {
    let srv = start_live_server().await;
    let claims = tempfile::tempdir().unwrap();

    // 1. whoami --json
    let (code, stdout, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || run_cli_cmd(&url, &tok, &cdir, &["whoami", "--as", "fx", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0);
    let who: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(who["handle"], "admin/fx");

    // 2. post + reply + label + read
    let (code, stdout, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &["post", "dev", "cli hello", "--label", "status=open", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0);
    let posted: Value = serde_json::from_str(&stdout).unwrap();
    let msg_id = posted["id"].as_str().unwrap().to_string();

    let (code, stdout, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let mid = msg_id.clone();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &["label", &mid, "status=triaged", "--if", "status=open", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0);
    let labeled: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(labeled["labels"]["status"], "triaged");

    // 3. Exit code 1 on `wait` timeout and `tasks claim --ready` nothing_ready, and exit code 2 on usage error
    let (code, _, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &["wait", "dev", "--timeout", "40ms", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 1, "expected exit code 1 on wait timeout, stderr={stderr}");
    let err: Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!(err["reason"], "timeout");

    let (code, _, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || run_cli_cmd(&url, &tok, &cdir, &["tasks", "claim", "--ready", "empty-q", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 1, "expected exit code 1 on nothing_ready, stderr={stderr}");
    let err: Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!(err["reason"], "nothing_ready");

    // Usage error (--sync combined with --no-sync) -> exit code 2
    let (code, _, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &["post", "dev", "bad", "--sync", "slack:#ops", "--no-sync", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 2, "expected exit code 2 on usage error, stderr={stderr}");

    // 4. Task create -> claim (persisted locally) -> complete (auto-loads saved claim!)
    let (code, stdout, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || run_cli_cmd(&url, &tok, &cdir, &["tasks", "create", "work", "do job", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0);
    let task_id = serde_json::from_str::<Value>(&stdout).unwrap()["task"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (code, _, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let tid = task_id.clone();
        move || run_cli_cmd(&url, &tok, &cdir, &["tasks", "claim", &tid, "--as", "worker-1"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0);

    // Complete WITHOUT passing --epoch or --claim-token (loaded from OPENBOARDS_CLAIMS_DIR)
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let tid = task_id.clone();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &["tasks", "complete", &tid, "all done", "--as", "worker-1", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "stderr={stderr}");
    let completed: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(completed["task"]["task"]["state"], "completed");
}

#[tokio::test]
async fn cli_tasks_run_renews_lease_handles_claim_lost_and_sigterm_release() {
    let srv = start_live_server().await;
    let claims = tempfile::tempdir().unwrap();

    // 1. `tasks run` exports BOARD_TASK, renews short lease (240ms) across 420ms sleep, and completes!
    let (code, stdout, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &["tasks", "create", "runners", "renew test", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0);
    let tid1 = serde_json::from_str::<Value>(&stdout).unwrap()["task"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let tid = tid1.clone();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &[
                    "tasks",
                    "run",
                    &tid,
                    "--lease-ms",
                    "240",
                    "--result",
                    "renewed ok",
                    "--",
                    "sh",
                    "-c",
                    "echo TASK=$BOARD_TASK && sleep 0.42",
                ],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "tasks run failed: stderr={stderr}");
    assert!(stdout.contains(&format!("TASK={tid1}")));

    let (code, stdout, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let tid = tid1.clone();
        move || run_cli_cmd(&url, &tok, &cdir, &["tasks", "get", &tid, "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0);
    let t1_view: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(t1_view["task"]["task"]["state"], "completed");
    assert_eq!(t1_view["task"]["task"]["result"], "renewed ok");

    // 2. `claim_lost` kills child immediately and exits 1 when task is canceled mid-run!
    let (code, stdout, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &["tasks", "create", "runners", "cancel mid-run", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0);
    let tid2 = serde_json::from_str::<Value>(&stdout).unwrap()["task"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let mut child = tokio::process::Command::new(bin_path())
        .args([
            "tasks",
            "run",
            &tid2,
            "--lease-ms",
            "210",
            "--",
            "sleep",
            "30",
        ])
        .env("BOARD_URL", &srv.url)
        .env("BOARD_TOKEN", &srv.owner_token)
        .env("OPENBOARDS_CLAIMS_DIR", claims.path())
        .spawn()
        .unwrap();

    // Wait briefly for claim, then cancel task so next renew (~70ms) hits claim_lost
    tokio::time::sleep(Duration::from_millis(120)).await;
    let (code, _, _) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let tid = tid2.clone();
        move || run_cli_cmd(&url, &tok, &cdir, &["tasks", "cancel", &tid, "abort"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0);

    let exit = tokio::time::timeout(Duration::from_secs(3), child.wait())
        .await
        .expect("child was not killed on claim_lost")
        .unwrap();
    assert_eq!(exit.code(), Some(1));

    // 3. SIGTERM releases task back to `ready` and exits 143
    #[cfg(unix)]
    {
        let (code, stdout, _) = tokio::task::spawn_blocking({
            let url = srv.url.clone();
            let tok = srv.owner_token.clone();
            let cdir = claims.path().to_path_buf();
            move || {
                run_cli_cmd(
                    &url,
                    &tok,
                    &cdir,
                    &["tasks", "create", "runners", "sigterm release", "--json"],
                )
            }
        })
        .await
        .unwrap();
        assert_eq!(code, 0);
        let tid3 = serde_json::from_str::<Value>(&stdout).unwrap()["task"]["id"]
            .as_str()
            .unwrap()
            .to_string();

        let mut child = tokio::process::Command::new(bin_path())
            .args(["tasks", "run", &tid3, "--", "sleep", "30"])
            .env("BOARD_URL", &srv.url)
            .env("BOARD_TOKEN", &srv.owner_token)
            .env("OPENBOARDS_CLAIMS_DIR", claims.path())
            .spawn()
            .unwrap();
        let pid = child.id().unwrap();

        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = StdCommand::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status();

        let exit = tokio::time::timeout(Duration::from_secs(3), child.wait())
            .await
            .expect("tasks run did not exit on SIGTERM")
            .unwrap();
        assert_eq!(exit.code(), Some(143));

        let (code, stdout, _) = tokio::task::spawn_blocking({
            let url = srv.url.clone();
            let tok = srv.owner_token.clone();
            let cdir = claims.path().to_path_buf();
            let tid = tid3.clone();
            move || run_cli_cmd(&url, &tok, &cdir, &["tasks", "get", &tid, "--json"])
        })
        .await
        .unwrap();
        assert_eq!(code, 0);
        let t3_view: Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(t3_view["task"]["task"]["state"], "ready");
    }
}

#[tokio::test]
async fn proxy_enforces_sandbox_narrowing_and_strips_spoofed_headers() {
    let srv = start_live_server().await;
    let claims = tempfile::tempdir().unwrap();

    let proxy_app = build_proxy_router(ProxyConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        upstream: srv.url.clone(),
        token: srv.owner_token.clone(),
        profile: "sandbox-worker".to_string(),
        can: r#"{"allowed/":["read","post"]}"#.to_string(),
        avatar: Some("https://example.com/sbx.png".to_string()),
    })
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = listener.local_addr().unwrap();
    let proxy_handle = tokio::spawn(async move {
        let _ = axum::serve(listener, proxy_app).await;
    });
    let proxy_url = format!("http://{proxy_addr}");

    // Sandboxed CLI uses a bogus token & attempts to spoof `--as hacker`: proxy overrides both!
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = proxy_url.clone();
        let cdir = claims.path().to_path_buf();
        move || {
            run_cli_cmd(
                &url,
                "not-a-real-token",
                &cdir,
                &["post", "allowed/work", "from sandbox", "--as", "hacker", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "stderr={stderr}");
    let posted: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(posted["by"]["profile"], "sandbox-worker");

    // Posting outside `allowed/` fails with 403 forbidden
    let (code, _, stderr) = tokio::task::spawn_blocking({
        let url = proxy_url.clone();
        let cdir = claims.path().to_path_buf();
        move || run_cli_cmd(&url, "not-a-real-token", &cdir, &["post", "forbidden-ch", "nope", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 1);
    assert_eq!(serde_json::from_str::<Value>(&stderr).unwrap()["code"], "forbidden");

    // Attempting grant mutation through proxy fails 403 forbidden
    let (code, _, stderr) = tokio::task::spawn_blocking({
        let url = proxy_url.clone();
        let cdir = claims.path().to_path_buf();
        move || {
            run_cli_cmd(
                &url,
                "not-a-real-token",
                &cdir,
                &["grants", "create", "--everyone", "--can", "allowed/=none", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 1);
    assert_eq!(serde_json::from_str::<Value>(&stderr).unwrap()["code"], "forbidden");

    proxy_handle.abort();
}

#[tokio::test]
async fn serve_local_persists_token_and_serves_requests() {
    let dir = tempfile::tempdir().unwrap();
    let mut child = tokio::process::Command::new(bin_path())
        .args([
            "serve",
            "--local",
            "--listen",
            "127.0.0.1:0",
            "--data-dir",
            dir.path().to_str().unwrap(),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();

    // Poll until `.board/token` is created
    let tok_path = dir.path().join("token");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        if tok_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    assert!(tok_path.exists(), "expected local token file to be persisted");
    let saved_tok = std::fs::read_to_string(&tok_path).unwrap();
    assert!(saved_tok.trim().starts_with("ob_"));

    let _ = child.kill().await;
    let _ = child.wait().await;
    let _ = json!({}); // keep serde_json used
}

#[tokio::test]
async fn cli_spec_audit_ergonomics_mentions_attach_shorthands_and_schedules() {
    let srv = start_live_server().await;
    let claims = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();

    // 1. `post <channel> --attach <local-file>` uploads local file and attaches it (empty text allowed!)
    let local_file = scratch.path().join("trace.txt");
    std::fs::write(&local_file, "stack trace bytes").unwrap();
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let fpath = local_file.to_str().unwrap().to_string();
        move || run_cli_cmd(&url, &tok, &cdir, &["post", "dev", "--attach", &fpath, "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "post --attach failed: {stderr}");
    let posted: Value = serde_json::from_str(&stdout).unwrap();
    let root_id = posted["id"].as_str().unwrap().to_string();
    assert_eq!(posted["attachments"].as_array().unwrap().len(), 1);
    assert_eq!(posted["attachments"][0]["name"], "trace.txt");

    // 2. `post <msg_id> "reply"` replies in the thread & bare `--mentions` defaults to `me`
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let rid = root_id.clone();
        move || run_cli_cmd(&url, &tok, &cdir, &["post", &rid, "ping @admin", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "post <msg_id> failed: {stderr}");
    let reply: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(reply["re"], root_id);

    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || run_cli_cmd(&url, &tok, &cdir, &["read", "--mentions", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "read --mentions failed: {stderr}");
    let mentioned: Value = serde_json::from_str(&stdout).unwrap();
    assert!(!mentioned["threads"].as_array().unwrap().is_empty());

    // 3. `schemas inspect git-commit@1` alias
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || run_cli_cmd(&url, &tok, &cdir, &["schemas", "inspect", "git-commit@1", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "schemas inspect failed: {stderr}");
    let sch: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(sch["schema"]["id"], "git-commit@1");

    // 4. `tasks create <channel> --file plan.json` and `tasks <channel> --state all` shorthand
    let plan_file = scratch.path().join("plan.json");
    std::fs::write(
        &plan_file,
        r#"[
            {"key":"a","text":"step a"},
            {"key":"b","text":"step b","needs":[{"key":"a"}]}
        ]"#,
    )
    .unwrap();
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let ppath = plan_file.to_str().unwrap().to_string();
        move || {
            run_cli_cmd(
                &url,
                &tok,
                &cdir,
                &["tasks", "create", "build", "--file", &ppath, "--idempotency-key", "plan-1", "--json"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "tasks create --file failed: {stderr}");
    let batch: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(batch["tasks"].as_array().unwrap().len(), 2);

    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || run_cli_cmd(&url, &tok, &cdir, &["tasks", "build", "--state", "all", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "tasks <channel> shorthand failed: {stderr}");
    let listed: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(listed["tasks"].as_array().unwrap().len(), 2);

    // 5. Singular `schedule <channel> "<text>" --in 1h`, `schedules <id>`, `schedule cancel <id>`
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        move || run_cli_cmd(&url, &tok, &cdir, &["schedule", "dev", "reminder", "--in", "1h", "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "schedule failed: {stderr}");
    let sched_id = serde_json::from_str::<Value>(&stdout).unwrap()["schedule"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let sid = sched_id.clone();
        move || run_cli_cmd(&url, &tok, &cdir, &["schedules", &sid, "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "schedules <id> failed: {stderr}");
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap()["schedule"]["id"],
        sched_id
    );

    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let sid = sched_id.clone();
        move || run_cli_cmd(&url, &tok, &cdir, &["schedule", "cancel", &sid, "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "schedule cancel failed: {stderr}");
    assert_eq!(
        serde_json::from_str::<Value>(&stdout).unwrap()["canceled"],
        true
    );

    // 6. `sync <id>` (optional target) and `sources`
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let url = srv.url.clone();
        let tok = srv.owner_token.clone();
        let cdir = claims.path().to_path_buf();
        let rid = root_id.clone();
        move || run_cli_cmd(&url, &tok, &cdir, &["sync", &rid, "--json"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "sync <id> failed: {stderr}");
    assert!(serde_json::from_str::<Value>(&stdout).unwrap()["targets"].is_array());
}

fn run_ob_clean_env(
    cwd: &std::path::Path,
    extra_env: &[(&str, &str)],
    args: &[&str],
) -> (i32, String, String) {
    let mut cmd = StdCommand::new(bin_path());
    cmd.current_dir(cwd)
        .args(args)
        .env_remove("BOARD_CONFIG")
        .env_remove("BOARD_URL")
        .env_remove("BOARD_NAME")
        .env_remove("BOARD_TOKEN")
        .env_remove("BOARD_PROFILE")
        .env_remove("BOARD_AVATAR")
        .env_remove("OPENBOARDS_TEAM");
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("failed executing ob binary");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

#[tokio::test]
async fn cli_binary_help_uses_ob_name() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    let (code, stdout, stderr) =
        tokio::task::spawn_blocking(move || run_ob_clean_env(&dir, &[], &["--help"]))
            .await
            .unwrap();
    assert_eq!(code, 0, "stderr={stderr}");
    assert!(stdout.contains("Usage: ob"), "stdout={stdout}");
    assert!(stdout.contains("--config"), "stdout={stdout}");
}

#[tokio::test]
async fn cli_openboard_json_discovery_precedence_and_errors() {
    let srv = start_live_server().await;
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();
    let mid = root.join("packages");
    let leaf = mid.join("service/src");
    std::fs::create_dir_all(&leaf).unwrap();

    // 1. Root `.openboard.json` discovered recursively from deeply nested `leaf`
    let root_cfg_path = root.join(".openboard.json");
    std::fs::write(
        &root_cfg_path,
        serde_json::to_string_pretty(&json!({
            "url": srv.url,
            "name": "cfg-root-board",
            "token": srv.owner_token,
            "as": "root-agent",
            "avatar": "https://example.com/root.png",
            "ensure": true,
            "json": true
        }))
        .unwrap(),
    )
    .unwrap();

    // `ob me` in `leaf` picks up url, token, board (with ensure=true creating it), profile, avatar, and json=true!
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let cwd = leaf.clone();
        move || run_ob_clean_env(&cwd, &[], &["me"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "ob me via parent .openboard.json failed: stderr={stderr}");
    let me: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(me["handle"], "admin/root-agent");
    assert_eq!(me["board"]["name"], "cfg-root-board");

    // 2. Nearest-ancestor `.openboard.json` overrides outer parent
    let mid_cfg_path = mid.join(".openboard.json");
    std::fs::write(
        &mid_cfg_path,
        serde_json::to_string_pretty(&json!({
            "url": srv.url,
            "board": "cfg-mid-board",
            "token": srv.owner_token,
            "profile": "mid-agent",
            "ensure": true,
            "json": true
        }))
        .unwrap(),
    )
    .unwrap();

    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let cwd = leaf.clone();
        move || run_ob_clean_env(&cwd, &[], &["me"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "nearest config failed: stderr={stderr}");
    let me: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(me["handle"], "admin/mid-agent");
    assert_eq!(me["board"]["name"], "cfg-mid-board");

    // 3. Precedence: env vars (`BOARD_*`) override `.openboard.json`
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let cwd = leaf.clone();
        move || {
            run_ob_clean_env(
                &cwd,
                &[
                    ("BOARD_PROFILE", "env-agent"),
                    ("BOARD_NAME", "env-board"),
                ],
                &["me"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "env override failed: stderr={stderr}");
    let me: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(me["handle"], "admin/env-agent");
    assert_eq!(me["board"]["name"], "env-board");

    // 4. Precedence: explicit CLI flags override BOTH env vars and `.openboard.json` (including `--json=false`)
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let cwd = leaf.clone();
        move || {
            run_ob_clean_env(
                &cwd,
                &[
                    ("BOARD_PROFILE", "env-agent"),
                    ("BOARD_NAME", "env-board"),
                ],
                &["me", "--as", "cli-agent", "--board", "cli-board", "--json=false"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "cli override failed: stderr={stderr}");
    assert!(
        stdout.contains("admin/cli-agent on board 'cli-board'"),
        "expected human output with CLI overrides, got: {stdout}"
    );

    // 5. Explicit `--config` and `BOARD_CONFIG` override upward discovery (and `--config` beats `BOARD_CONFIG`)
    let explicit_cfg = root.join("explicit.json");
    std::fs::write(
        &explicit_cfg,
        serde_json::to_string_pretty(&json!({
            "url": srv.url,
            "board": "explicit-board",
            "token": srv.owner_token,
            "profile": "explicit-agent",
            "ensure": true,
            "json": true
        }))
        .unwrap(),
    )
    .unwrap();

    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let cwd = leaf.clone();
        let cfg_str = explicit_cfg.to_string_lossy().to_string();
        move || run_ob_clean_env(&cwd, &[("BOARD_CONFIG", &cfg_str)], &["me"])
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "BOARD_CONFIG failed: stderr={stderr}");
    let me: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(me["handle"], "admin/explicit-agent");
    assert_eq!(me["board"]["name"], "explicit-board");

    // `--config` beats `BOARD_CONFIG`
    let (code, stdout, stderr) = tokio::task::spawn_blocking({
        let cwd = leaf.clone();
        let env_cfg = explicit_cfg.to_string_lossy().to_string();
        let flag_cfg = root_cfg_path.to_string_lossy().to_string();
        move || {
            run_ob_clean_env(
                &cwd,
                &[("BOARD_CONFIG", &env_cfg)],
                &["--config", &flag_cfg, "me"],
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(code, 0, "--config over BOARD_CONFIG failed: stderr={stderr}");
    let me: Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(me["handle"], "admin/root-agent");

    // 6. Error case: missing explicit `--config` names the path
    let missing = root.join("missing.openboard.json");
    let (code, _, stderr) = tokio::task::spawn_blocking({
        let cwd = leaf.clone();
        let m = missing.to_string_lossy().to_string();
        move || run_ob_clean_env(&cwd, &[], &["--config", &m, "whoami"])
    })
    .await
    .unwrap();
    assert_ne!(code, 0);
    assert!(
        stderr.contains(&missing.display().to_string()),
        "expected missing path in error, stderr={stderr}"
    );

    // 7. Error case: invalid JSON in discovered `.openboard.json` names the path
    let bad_dir = tempfile::tempdir().unwrap();
    let bad_cfg = bad_dir.path().join(".openboard.json");
    std::fs::write(&bad_cfg, "{ invalid json !!!").unwrap();
    let (code, _, stderr) = tokio::task::spawn_blocking({
        let cwd = bad_dir.path().to_path_buf();
        move || run_ob_clean_env(&cwd, &[], &["whoami"])
    })
    .await
    .unwrap();
    assert_ne!(code, 0);
    assert!(
        stderr.contains(&bad_cfg.display().to_string()),
        "expected invalid config path in error, stderr={stderr}"
    );

    // 8. Error case: unknown field in `.openboard.json` fails fast and names the path
    let unknown_dir = tempfile::tempdir().unwrap();
    let unknown_cfg = unknown_dir.path().join(".openboard.json");
    std::fs::write(&unknown_cfg, r#"{"url":"http://127.0.0.1:8080","typo_key":true}"#).unwrap();
    let (code, _, stderr) = tokio::task::spawn_blocking({
        let cwd = unknown_dir.path().to_path_buf();
        move || run_ob_clean_env(&cwd, &[], &["whoami"])
    })
    .await
    .unwrap();
    assert_ne!(code, 0);
    assert!(
        stderr.contains(&unknown_cfg.display().to_string()) && stderr.contains("typo_key"),
        "expected path and unknown key in error, stderr={stderr}"
    );
}
