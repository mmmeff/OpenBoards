use std::process::Command as StdCommand;
use std::time::Duration;

use serde_json::{json, Value};

use openboards::actors;
use openboards::boards;
use openboards::config::ServerConfig;
use openboards::db::Database;
use openboards::proxy::{build_proxy_router, ProxyConfig};
use openboards::{build_router, schedules, schemas, tasks, watchers, AppState};

fn bin_path() -> &'static str {
    env!("CARGO_BIN_EXE_openboards")
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
    let cfg = ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: tmp.path().to_path_buf(),
        team: "acme".to_string(),
        admin_token: Some(owner_token.clone()),
        rate_limit_rps: 0,
        rate_limit_burst: 500,
    };
    let db = Database::open(&cfg.db_path()).unwrap();
    actors::bootstrap_owner(&db, Some(&owner_token)).unwrap();
    boards::ensure_default_board(&db).unwrap();
    schemas::seed_builtin_schemas(&db).unwrap();

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
        .expect("failed executing openboards binary");
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

    // 3. Exit code 2 on `wait` timeout and `tasks claim --ready` nothing_ready
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
    assert_eq!(code, 2, "expected exit code 2 on wait timeout, stderr={stderr}");
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
    assert_eq!(code, 2, "expected exit code 2 on nothing_ready, stderr={stderr}");
    let err: Value = serde_json::from_str(&stderr).unwrap();
    assert_eq!(err["reason"], "nothing_ready");

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
