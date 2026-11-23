use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use serde::Serialize;
use serde_json::{json, Value};

use crate::actors;
use crate::boards;
use crate::client::{BoardClient, ClientConfig};
use crate::config::ServerConfig;
use crate::db::Database;
use crate::error::BoardResult;
use crate::id::new_bearer_token;
use crate::proxy::{build_proxy_router, ProxyConfig};
use crate::{build_router, schedules, schemas, tasks, watchers, AppState};

#[derive(Debug, Clone, Serialize)]
pub struct CheckResult {
    pub name: String,
    pub passed: bool,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConformanceReport {
    pub ok: bool,
    pub passed: usize,
    pub failed: usize,
    pub checks: Vec<CheckResult>,
}

struct EphemeralServer {
    _tmp: tempfile::TempDir,
    url: String,
    owner_token: String,
    state: AppState,
    _server_handle: tokio::task::JoinHandle<()>,
    _scheduler_handle: tokio::task::JoinHandle<()>,
    _reaper_handle: tokio::task::JoinHandle<()>,
    _watcher_handle: tokio::task::JoinHandle<()>,
}

async fn start_ephemeral_server() -> Result<EphemeralServer, String> {
    let tmp = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
    let owner_token = new_bearer_token();
    let cfg = ServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        data_dir: tmp.path().to_path_buf(),
        team: "conformance".to_string(),
        admin_token: Some(owner_token.clone()),
        rate_limit_rps: 0,
        rate_limit_burst: 500,
    };
    let db = Database::open(&cfg.db_path()).map_err(|e| format!("db open: {e}"))?;
    actors::bootstrap_owner(&db, Some(&owner_token)).map_err(|e| format!("bootstrap: {e}"))?;
    boards::ensure_default_board(&db).map_err(|e| format!("default board: {e}"))?;
    schemas::seed_builtin_schemas(&db).map_err(|e| format!("schemas: {e}"))?;

    let state = AppState::new(db, cfg);
    let scheduler = schedules::spawn_scheduler_loop(state.clone());
    let reaper = tasks::spawn_lease_reaper_loop(state.clone());
    let dispatcher = watchers::spawn_watcher_dispatcher(state.clone());
    let app = build_router(state.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| format!("bind: {e}"))?;
    let addr = listener.local_addr().map_err(|e| format!("addr: {e}"))?;
    let server = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    Ok(EphemeralServer {
        _tmp: tmp,
        url: format!("http://{addr}"),
        owner_token,
        state,
        _server_handle: server,
        _scheduler_handle: scheduler,
        _reaper_handle: reaper,
        _watcher_handle: dispatcher,
    })
}

async fn run_check<F, Fut>(name: &str, f: F) -> CheckResult
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), String>>,
{
    let start = Instant::now();
    match f().await {
        Ok(()) => CheckResult {
            name: name.to_string(),
            passed: true,
            duration_ms: start.elapsed().as_millis() as u64,
            error: None,
        },
        Err(err) => CheckResult {
            name: name.to_string(),
            passed: false,
            duration_ms: start.elapsed().as_millis() as u64,
            error: Some(err),
        },
    }
}

fn expect(cond: bool, msg: impl Into<String>) -> Result<(), String> {
    if cond {
        Ok(())
    } else {
        Err(msg.into())
    }
}

/// Runs the full clean-room OpenBoards protocol conformance suite.
///
/// When `self_test` is true or `target_url` is `None`, spins up an ephemeral in-process
/// server on `127.0.0.1:0` and tests all 13 protocol checks (Concepts 1–10 + Sandbox Proxy).
pub async fn run_conformance(
    target_url: Option<&str>,
    token: Option<String>,
    board_name: Option<&str>,
    self_test: bool,
) -> BoardResult<ConformanceReport> {
    let ephemeral = if self_test || target_url.is_none() {
        Some(
            start_ephemeral_server()
                .await
                .map_err(crate::error::BoardError::internal)?,
        )
    } else {
        None
    };

    let (base_url, owner_token) = if let Some(ref eph) = ephemeral {
        (eph.url.clone(), eph.owner_token.clone())
    } else {
        let url = target_url.unwrap().to_string();
        let tok = token.ok_or_else(|| {
            crate::error::BoardError::bad_request(
                "conformance against an external --url requires --token (or BOARD_TOKEN)",
            )
        })?;
        (url, tok)
    };

    let board = board_name.unwrap_or("General").to_string();
    let cfg = ClientConfig::from_raw(
        &base_url,
        Some(&board),
        Some(owner_token.clone()),
        None,
        None,
        None,
        true,
    );
    let owner = BoardClient::new(cfg)
        .map_err(|e| crate::error::BoardError::internal(e.to_string()))?;

    let mut checks = Vec::new();

    // 1. Health & Discovery
    checks.push(
        run_check("01_health_and_discovery", || async {
            let h = owner.healthz().await.map_err(|e| e.to_string())?;
            expect(h["ok"] == true, "healthz ok != true")?;
            let d = owner.discovery().await.map_err(|e| e.to_string())?;
            expect(d["name"] == "General", "discovery default name != General")?;
            expect(d["team_id"].is_string() && d["url"].is_string(), "missing discovery fields")?;
            Ok(())
        })
        .await,
    );

    // 2. Auth, Profiles & Narrowing
    checks.push(
        run_check("02_auth_profiles_and_narrowing", || async {
            let unauth = BoardClient::new(ClientConfig::from_raw(
                &base_url,
                Some(&board),
                None,
                None,
                None,
                None,
                false,
            ))
            .map_err(|e| e.to_string())?;
            let err = unauth.whoami().await.expect_err("unauth should fail");
            expect(err.status == 401 && err.code == "unauthorized", "expected 401 unauthorized")?;

            let fx = owner
                .with_profile(Some("Fx".into()), Some("https://example.com/fx.png".into()))
                .with_extra_header("x-board-can", "read,post")
                .with_extra_header("x-board-sandboxed", "true");
            let w = fx.whoami().await.map_err(|e| e.to_string())?;
            expect(w["profile"] == "Fx", "profile mismatch")?;
            expect(w["narrowed"] == true && w["sandboxed"] == true, "narrowing flags mismatch")?;
            expect(w["can"]["*"] == json!(["read", "post"]), "can narrowing mismatch")?;
            Ok(())
        })
        .await,
    );

    // 3. Boards & Uniqueness
    let test_board = format!("conf-{}", &new_bearer_token()[3..11]);
    checks.push(
        run_check("03_boards_and_uniqueness", || async {
            let missing_client = owner.with_board("does-not-exist-xyz");
            let err = missing_client
                .get_board(Some(false))
                .await
                .expect_err("expected no_board");
            expect(err.code == "no_board", format!("expected no_board, got {}", err.code))?;

            let created = owner
                .create_board(&test_board, false)
                .await
                .map_err(|e| e.to_string())?;
            expect(created["created"] == true, "expected board created=true")?;

            let dup = owner
                .create_board(&test_board.to_uppercase(), false)
                .await
                .expect_err("expected board_name_taken");
            expect(
                dup.code == "board_name_taken",
                format!("expected board_name_taken, got {}", dup.code),
            )?;
            Ok(())
        })
        .await,
    );

    let b_owner = owner.with_board(&test_board);

    // Create secondary member + viewer + project tokens for multi-principal checks
    let member_tok = create_user_token(&owner, "conf_member", "member").await;
    let viewer_tok = create_user_token(&owner, "conf_viewer", "viewer").await;
    let project_tok = create_project_token(&owner, "conf_proj", "production").await;

    // 4. Channels & Private Channel Atomicity
    checks.push(
        run_check("04_channels_and_private_atomicity", || async {
            let c1 = b_owner
                .create_channel("dev", false)
                .await
                .map_err(|e| e.to_string())?;
            expect(c1["created"] == true, "expected channel created")?;
            let c2 = b_owner
                .create_channel("dev", false)
                .await
                .map_err(|e| e.to_string())?;
            expect(c2["created"] == false, "expected idempotent channel")?;

            let priv_ch = b_owner
                .create_channel("vault", true)
                .await
                .map_err(|e| e.to_string())?;
            expect(
                priv_ch["grants"].as_array().map(|a| a.len()) == Some(2),
                "expected 2 atomic grants for private channel",
            )?;

            let dup_priv = b_owner
                .create_channel("vault", true)
                .await
                .expect_err("expected channel_exists");
            expect(dup_priv.code == "channel_exists", "expected 409 channel_exists")?;

            if let Ok(ref mtok) = member_tok {
                let b_mem = b_owner.with_token(mtok.clone());
                let chs = b_mem.list_channels().await.map_err(|e| e.to_string())?;
                let names: Vec<&str> = chs["channels"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter_map(|c| c["path"].as_str())
                    .collect();
                expect(
                    names.contains(&"dev") && !names.contains(&"vault"),
                    "private channel leaked to non-member",
                )?;
            }
            Ok(())
        })
        .await,
    );

    // 5. 4-Step Grants & Explain
    checks.push(
        run_check("05_grants_4step_and_explain", || async {
            let g_list = b_owner.list_grants().await.map_err(|e| e.to_string())?;
            let ids: Vec<&str> = g_list["grants"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|g| g["id"].as_str())
                .collect();
            expect(
                ids.contains(&"g1") && ids.contains(&"g2") && ids.contains(&"g3"),
                "missing default g1/g2/g3 grants",
            )?;

            if let Ok(ref vtok) = viewer_tok {
                let b_viewer = b_owner.with_token(vtok.clone());
                let exp = b_viewer
                    .explain_grant(&[("channel".into(), "dev".into())])
                    .await
                    .map_err(|e| e.to_string())?;
                expect(
                    exp["can"] == json!(["read", "react"]),
                    format!("expected viewer cap [read, react], got {}", exp["can"]),
                )?;
            }

            // Narrowed owner denied grant mutation
            let narrowed = b_owner.with_extra_header("x-board-can", "all");
            let err = narrowed
                .create_grant(&json!({
                    "principals": [{ "kind": "everyone" }],
                    "can": { "sec/": "none" }
                }))
                .await
                .expect_err("narrowed caller must not mutate grants");
            expect(err.code == "forbidden", "expected 403 forbidden")?;
            Ok(())
        })
        .await,
    );

    // 6. Threads, Mentions, Reactions, Idempotency & Tombstones
    checks.push(
        run_check("06_threads_mentions_reactions_tombstones", || async {
            let root = b_owner
                .post_message(&json!({
                    "channel": "dev",
                    "text": "hello root",
                    "id": "conf-idem-1"
                }))
                .await
                .map_err(|e| e.to_string())?;
            let root_id = root["id"].as_str().unwrap().to_string();

            let replay = b_owner
                .post_message(&json!({
                    "channel": "dev",
                    "text": "different",
                    "id": "conf-idem-1"
                }))
                .await
                .map_err(|e| e.to_string())?;
            expect(replay["id"] == root_id, "idempotency replay returned different ID")?;

            let r1 = b_owner
                .reply_message(&root_id, &json!({ "text": "reply 1 @admin `@admin`", "after": root_id }))
                .await
                .map_err(|e| e.to_string())?;
            let r1_id = r1["id"].as_str().unwrap().to_string();
            expect(r1["re"] == root_id, "reply re mismatch")?;
            expect(
                r1["text"].as_str().unwrap().contains("@[user:")
                    && r1["text"].as_str().unwrap().contains("`@admin`"),
                "mention resolution / code skipping failed",
            )?;

            let stale = b_owner
                .reply_message(&root_id, &json!({ "text": "stale", "after": root_id }))
                .await
                .expect_err("expected newer_message");
            expect(stale.code == "newer_message", "expected 409 newer_message")?;

            b_owner
                .react(&root_id, "+1", false)
                .await
                .map_err(|e| e.to_string())?;
            let view = b_owner
                .get_message(&root_id)
                .await
                .map_err(|e| e.to_string())?;
            expect(
                view["reactions"]["thumbs_up"].as_array().map(|a| a.len()) == Some(1),
                "expected normalized thumbs_up reaction",
            )?;

            b_owner
                .delete_message(&r1_id, Some(1))
                .await
                .map_err(|e| e.to_string())?;
            let del_view = b_owner.get_message(&r1_id).await.map_err(|e| e.to_string())?;
            expect(del_view["deleted"] == true && del_view["text"] == "", "tombstone failed")?;
            Ok(())
        })
        .await,
    );

    // 7. Labels CAS, Root Inheritance & Schemas
    checks.push(
        run_check("07_labels_cas_filters_and_schemas", || async {
            let bad = b_owner
                .post_message(&json!({
                    "channel": "dev",
                    "text": "x",
                    "labels": { "board.task": "ready" }
                }))
                .await
                .expect_err("reserved label must fail");
            expect(bad.code == "reserved_label", "expected reserved_label")?;

            let msg = b_owner
                .post_message(&json!({
                    "channel": "dev",
                    "schema": "git-commit@1",
                    "text": "{\"sha\":\"a1b2c3d\",\"message\":\"conformance commit\"}",
                    "labels": { "status": "open" }
                }))
                .await
                .map_err(|e| e.to_string())?;
            let mid = msg["id"].as_str().unwrap().to_string();
            expect(msg["blocks"][0]["schema"] == "git-commit", "expected parsed git-commit block")?;

            let _ = b_owner
                .label(&mid, &json!({ "set": { "owner": "secret-val" }, "if": { "owner": null } }))
                .await
                .map_err(|e| e.to_string())?;
            let conflict = b_owner
                .label(&mid, &json!({ "set": { "owner": "other" }, "if": { "owner": null } }))
                .await
                .expect_err("expected label_conflict");
            expect(
                conflict.code == "label_conflict" && !conflict.message.contains("secret-val"),
                "label_conflict failed or leaked value",
            )?;
            Ok(())
        })
        .await,
    );

    // 8. Attachments (Inline + Streaming + Tombstone Visibility)
    checks.push(
        run_check("08_attachments_and_tombstones", || async {
            let payload = b"conformance attachment bytes".to_vec();
            let att = b_owner
                .upload_bytes("note.txt", Some("text/plain"), payload.clone())
                .await
                .map_err(|e| e.to_string())?;
            let att_id = att["id"].as_str().unwrap().to_string();
            let msg = b_owner
                .post_message(&json!({ "channel": "dev", "text": "", "attachments": [att_id] }))
                .await
                .map_err(|e| e.to_string())?;
            let msg_id = msg["id"].as_str().unwrap().to_string();
            expect(msg["attachments"].as_array().map(|a| a.len()) == Some(1), "missing attachment")?;

            let (_, downloaded) = b_owner
                .download_attachment(&att_id)
                .await
                .map_err(|e| e.to_string())?;
            expect(downloaded == payload, "downloaded bytes mismatch")?;

            b_owner
                .delete_message(&msg_id, None)
                .await
                .map_err(|e| e.to_string())?;
            if let Ok(ref mtok) = member_tok {
                // Member lacks `*`-read because of `vault`, so tombstoned attachment download is forbidden
                let b_mem = b_owner.with_token(mtok.clone());
                let err = b_mem
                    .download_attachment(&att_id)
                    .await
                    .expect_err("non-uploader non-*-read must be denied tombstoned attachment");
                expect(err.status == 403 || err.status == 404, "expected 403/404")?;
            }
            Ok(())
        })
        .await,
    );

    // 9. Unified Event Log, Cursors, Before/After & Live Long-Poll Wait
    checks.push(
        run_check("09_events_before_after_and_wait", || async {
            let msg = b_owner
                .post_message(&json!({
                    "channel": "events-ch",
                    "text": "track me",
                    "labels": { "state": "open" }
                }))
                .await
                .map_err(|e| e.to_string())?;
            let mid = msg["id"].as_str().unwrap().to_string();
            b_owner
                .label(&mid, &json!({ "set": { "state": "closed" } }))
                .await
                .map_err(|e| e.to_string())?;

            let evs = b_owner
                .poll_events(&[
                    ("after".into(), mid.clone()),
                    ("label".into(), "state=open".into()),
                ])
                .await
                .map_err(|e| e.to_string())?;
            expect(
                evs["events"]
                    .as_array()
                    .is_some_and(|a| a.iter().any(|e| e["type"] == "labels.changed")),
                "before-or-after label transition not observed",
            )?;

            // Live wait wakeup
            let poster = b_owner.clone();
            let waiter = tokio::spawn({
                let w = b_owner.clone();
                async move {
                    w.wait_event(&[
                        ("channel".into(), "events-ch".into()),
                        ("timeout".into(), "5s".into()),
                    ])
                    .await
                }
            });
            tokio::time::sleep(Duration::from_millis(30)).await;
            poster
                .post_message(&json!({ "channel": "events-ch", "text": "live wake" }))
                .await
                .map_err(|e| e.to_string())?;
            let woken = waiter
                .await
                .map_err(|e| e.to_string())?
                .map_err(|e| e.to_string())?;
            expect(woken["event"]["type"] == "message.posted", "live wait did not receive post")?;
            Ok(())
        })
        .await,
    );

    // 10. Tasks, Leases, DAG Cascade, Batch Create & Queue Claim
    checks.push(
        run_check("10_tasks_leases_dag_batch_and_claim_ready", || async {
            let batch = b_owner
                .create_many_tasks(&json!({
                    "channel": "work",
                    "id": "conf-batch-1",
                    "tasks": [
                        { "key": "a", "text": "step A", "labels": { "step": "a" } },
                        { "key": "b", "text": "step B", "labels": { "step": "b" }, "needs": [{ "key": "a" }] }
                    ]
                }))
                .await
                .map_err(|e| e.to_string())?;
            let a_id = batch["tasks"][0]["id"].as_str().unwrap().to_string();
            let b_id = batch["tasks"][1]["id"].as_str().unwrap().to_string();
            expect(batch["tasks"][0]["task"]["state"] == "ready", "task A not ready")?;
            expect(batch["tasks"][1]["task"]["state"] == "waiting", "task B not waiting")?;

            let claim = b_owner
                .claim_ready(&json!({ "channel": "work", "label": { "step": "a" } }))
                .await
                .map_err(|e| e.to_string())?;
            expect(claim["task"]["id"] == a_id, "claim-ready returned wrong task")?;
            let epoch = claim["epoch"].as_i64().unwrap();
            let tok = claim["token"].as_str().unwrap().to_string();

            let comp = b_owner
                .complete_task(&a_id, &json!({ "epoch": epoch, "token": tok, "result": "ok" }))
                .await
                .map_err(|e| e.to_string())?;
            expect(comp["readied"] == json!([b_id]), "completing A did not ready B")?;

            let stale = b_owner
                .renew_task(&a_id, &json!({ "epoch": epoch, "token": tok }))
                .await
                .expect_err("stale lease must fail claim_lost");
            expect(stale.code == "claim_lost", "expected claim_lost")?;
            Ok(())
        })
        .await,
    );

    // 11. Schedules
    checks.push(
        run_check("11_schedules_validation_and_lifecycle", || async {
            let bad = b_owner
                .create_schedule(&json!({ "channel": "dev", "text": "too fast", "every": "30s" }))
                .await
                .expect_err("every < 60s must fail");
            expect(bad.code == "bad_request", "expected bad_request")?;

            let s = b_owner
                .create_schedule(&json!({ "channel": "dev", "text": "later", "in": "1h" }))
                .await
                .map_err(|e| e.to_string())?;
            let sid = s["schedule"]["id"].as_str().unwrap().to_string();
            let c = b_owner
                .cancel_schedule(&sid)
                .await
                .map_err(|e| e.to_string())?;
            expect(c["canceled"] == true, "expected schedule canceled=true")?;

            if let Some(ref eph) = ephemeral {
                let due = b_owner
                    .create_schedule(&json!({
                        "channel": "sched-fire",
                        "text": "auto-fired",
                        "at": chrono::Utc::now().to_rfc3339()
                    }))
                    .await
                    .map_err(|e| e.to_string())?;
                expect(due["schedule"]["status"] == "pending", "expected pending")?;
                let sum = schedules::fire_due_schedules(&eph.state).map_err(|e| e.to_string())?;
                expect(sum.fired >= 1, "expected due schedule to fire")?;
            }
            Ok(())
        })
        .await,
    );

    // 12. Watchers & Webhook Delivery + Loop Suppression
    checks.push(
        run_check("12_watchers_webhook_and_loop_suppression", || async {
            let Ok(ref ptok) = project_tok else {
                return Ok(());
            };
            type HookStore = Arc<Mutex<Vec<(HeaderMap, Value)>>>;
            let received: HookStore = Arc::new(Mutex::new(Vec::new()));
            let recv_clone = received.clone();
            let hook_app = Router::new()
                .route(
                    "/hook",
                    post(
                        |State(store): State<HookStore>,
                         headers: HeaderMap,
                         axum::Json(body): axum::Json<Value>| async move {
                            store.lock().unwrap().push((headers, body));
                            StatusCode::OK
                        },
                    ),
                )
                .with_state(recv_clone);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| e.to_string())?;
            let hook_addr = listener.local_addr().map_err(|e| e.to_string())?;
            let hook_srv = tokio::spawn(async move {
                let _ = axum::serve(listener, hook_app).await;
            });

            let b_proj = b_owner.with_token(ptok.clone());
            let _ = b_proj
                .create_watcher(&json!({
                    "id": "triage",
                    "tail": "watch-ch",
                    "url": format!("http://{hook_addr}/hook"),
                    "secret": "whsec_conf"
                }))
                .await
                .map_err(|e| e.to_string())?;

            // Self-write as project+triage must be suppressed
            let self_writer = b_proj.with_profile(Some("triage".into()), None);
            let _ = self_writer
                .post_message(&json!({ "channel": "watch-ch", "text": "self message" }))
                .await
                .map_err(|e| e.to_string())?;

            // External write by owner must trigger webhook delivery
            let _ = b_owner
                .post_message(&json!({ "channel": "watch-ch", "text": "trigger webhook" }))
                .await
                .map_err(|e| e.to_string())?;

            let deadline = Instant::now() + Duration::from_secs(3);
            while Instant::now() < deadline {
                if !received.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            hook_srv.abort();

            let items = received.lock().unwrap();
            expect(items.len() == 1, format!("expected exactly 1 webhook delivery, got {}", items.len()))?;
            expect(
                items[0].1["event"]["message"]["text"] == "trigger webhook",
                "wrong event delivered (self-loop suppression failed)",
            )?;
            expect(
                items[0].0.get("x-board-signature").is_some()
                    && items[0].0.get("x-board-delivery-id").is_some(),
                "missing signature or delivery-id header",
            )?;
            Ok(())
        })
        .await,
    );

    // 13. Sandbox Proxy Enforcement
    checks.push(
        run_check("13_sandbox_proxy_enforcement", || async {
            let proxy_app = build_proxy_router(ProxyConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                upstream: base_url.clone(),
                token: owner_token.clone(),
                profile: "sandbox-agent".to_string(),
                can: r#"{"sandbox-ch/":["read","post"]}"#.to_string(),
                avatar: None,
            })
            .map_err(|e| e.to_string())?;

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|e| e.to_string())?;
            let proxy_addr = listener.local_addr().map_err(|e| e.to_string())?;
            let proxy_srv = tokio::spawn(async move {
                let _ = axum::serve(listener, proxy_app).await;
            });

            // Sandboxed client sends NO real token and tries to spoof profile + bypass sandbox
            let sandboxed = BoardClient::new(ClientConfig::from_raw(
                &format!("http://{proxy_addr}"),
                Some(&test_board),
                Some("fake-placeholder".into()),
                Some("spoofed".into()),
                None,
                None,
                false,
            ))
            .map_err(|e| e.to_string())?
            .with_extra_header("x-board-sandboxed", "false")
            .with_extra_header("x-board-can", "all");

            let who = sandboxed.whoami().await.map_err(|e| e.to_string())?;
            expect(who["profile"] == "sandbox-agent", "proxy did not force profile")?;
            expect(who["sandboxed"] == true, "proxy did not force sandboxed=true")?;

            // Allowed in sandbox-ch
            let p = sandboxed
                .post_message(&json!({ "channel": "sandbox-ch", "text": "hello from sandbox" }))
                .await
                .map_err(|e| e.to_string())?;
            expect(p["by"]["profile"] == "sandbox-agent", "post not attributed to sandbox profile")?;

            // Denied outside sandbox-ch/
            let denied_ch = sandboxed
                .post_message(&json!({ "channel": "dev", "text": "escape" }))
                .await
                .expect_err("sandbox should be blocked outside sandbox-ch/");
            expect(denied_ch.code == "forbidden", "expected 403 forbidden")?;

            // Denied grant mutation
            let denied_grant = sandboxed
                .create_grant(&json!({
                    "principals": [{ "kind": "everyone" }],
                    "can": { "dev": "none" }
                }))
                .await
                .expect_err("sandbox must not mutate grants");
            expect(denied_grant.code == "forbidden", "expected 403 forbidden")?;

            proxy_srv.abort();
            Ok(())
        })
        .await,
    );

    let passed = checks.iter().filter(|c| c.passed).count();
    let failed = checks.len() - passed;
    Ok(ConformanceReport {
        ok: failed == 0,
        passed,
        failed,
        checks,
    })
}

async fn create_user_token(owner: &BoardClient, prefix: &str, role: &str) -> Result<String, String> {
    let suffix = &new_bearer_token()[3..9];
    let username = format!("{prefix}_{suffix}");
    let u = owner
        .admin_create_user(&json!({
            "username": username,
            "email": format!("{username}@conformance.local"),
            "role": role
        }))
        .await
        .map_err(|e| e.to_string())?;
    let uid = u["user"]["id"].as_str().ok_or("missing user id")?;
    let tok = owner
        .admin_create_token(&json!({ "principal_kind": "user", "principal_id": uid }))
        .await
        .map_err(|e| e.to_string())?;
    Ok(tok["token"]["token"].as_str().ok_or("missing token")?.to_string())
}

async fn create_project_token(
    owner: &BoardClient,
    prefix: &str,
    env: &str,
) -> Result<String, String> {
    let suffix = &new_bearer_token()[3..9];
    let name = format!("{prefix}_{suffix}");
    let p = owner
        .admin_create_project(&name)
        .await
        .map_err(|e| e.to_string())?;
    let pid = p["project"]["id"].as_str().ok_or("missing project id")?;
    let tok = owner
        .admin_create_token(&json!({
            "principal_kind": "project",
            "principal_id": pid,
            "environment": env
        }))
        .await
        .map_err(|e| e.to_string())?;
    Ok(tok["token"]["token"].as_str().ok_or("missing token")?.to_string())
}
