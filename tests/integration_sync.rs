use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::State;
use axum::http::{Method, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use chrono::{Duration, Utc};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use openboards::actors;
use openboards::boards;
use openboards::config::{DbTarget, ServerConfig};
use openboards::db::Database;
use openboards::sync;
use openboards::watchers::hmac_sha256_hex;
use openboards::{build_router, AppState};

struct TestHarness {
    _tmp: tempfile::TempDir,
    state: AppState,
    app: Router,
    owner_token: String,
}

impl TestHarness {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let owner_token = "owner-sync-secret".to_string();
        let db = Database::open_in_memory().await.unwrap();
        actors::bootstrap_owner(&db, Some(&owner_token)).await.unwrap();
        boards::ensure_default_board(&db).await.unwrap();
        let state = AppState::new(
            db,
            ServerConfig {
                listen: "127.0.0.1:8080".parse().unwrap(),
                data_dir: tmp.path().to_path_buf(),
                team: "acme".to_string(),
                admin_token: Some(owner_token.clone()),
                rate_limit_rps: 0,
                rate_limit_burst: 500,
                db_target: DbTarget::SqliteUrl("sqlite::memory:".to_string()),
                db_max_connections: 1,
            },
        );
        let app = build_router(state.clone());
        Self {
            _tmp: tmp,
            state,
            app,
            owner_token,
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
        let raw = body.map(|v| v.to_string()).unwrap_or_default();
        self.req_raw_str(method, uri, token, headers, raw).await
    }

    async fn req_raw_str(
        &self,
        method: Method,
        uri: &str,
        token: Option<&str>,
        headers: &[(&str, &str)],
        raw_body: String,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            builder = builder.header("authorization", format!("Bearer {t}"));
        }
        for &(k, v) in headers {
            builder = builder.header(k, v);
        }
        let req = if raw_body.is_empty() {
            builder.body(Body::empty()).unwrap()
        } else {
            builder
                .header("content-type", "application/json")
                .body(Body::from(raw_body))
                .unwrap()
        };
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let st = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (st, val)
    }

    async fn create_user(&self, username: &str, role: &str) -> (String, String) {
        let (st, body) = self
            .req(
                Method::POST,
                "/v1/admin/users",
                Some(&self.owner_token),
                &[],
                Some(json!({ "username": username, "email": format!("{username}@acme.com"), "role": role })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED);
        let uid = body["user"]["id"].as_str().unwrap().to_string();
        let (st, tok) = self
            .req(
                Method::POST,
                "/v1/admin/tokens",
                Some(&self.owner_token),
                &[],
                Some(json!({ "principal_kind": "user", "principal_id": uid })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED);
        (uid, tok["token"]["token"].as_str().unwrap().to_string())
    }

    async fn create_project_token(&self, name: &str, env: &str) -> (String, String) {
        let (st, body) = self
            .req(
                Method::POST,
                "/v1/admin/projects",
                Some(&self.owner_token),
                &[],
                Some(json!({ "name": name })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED);
        let pid = body["project"]["id"].as_str().unwrap().to_string();
        let tok = self.mint_project_token(&pid, env).await;
        (pid, tok)
    }

    async fn mint_project_token(&self, pid: &str, env: &str) -> String {
        let (st, tok) = self
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
        assert_eq!(st, StatusCode::CREATED);
        tok["token"]["token"].as_str().unwrap().to_string()
    }
}

async fn start_mock_platform_server(
    fail_times: usize,
) -> (
    String,
    Arc<Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    #[derive(Clone)]
    struct MockState {
        received: Arc<Mutex<Vec<Value>>>,
        attempts: Arc<AtomicUsize>,
        fail_times: usize,
    }
    let received = Arc::new(Mutex::new(Vec::new()));
    let state = MockState {
        received: received.clone(),
        attempts: Arc::new(AtomicUsize::new(0)),
        fail_times,
    };
    let app = Router::new()
        .route(
            "/deliver",
            post(|State(st): State<MockState>, axum::Json(body): axum::Json<Value>| async move {
                let n = st.attempts.fetch_add(1, Ordering::SeqCst);
                if n < st.fail_times {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(json!({ "error": "upstream boom" })),
                    );
                }
                st.received.lock().unwrap().push(body);
                (
                    StatusCode::OK,
                    axum::Json(json!({
                        "platformMessageId": format!("pm_{n}"),
                        "platformThreadId": "pt_1",
                        "platformUrl": format!("https://platform.test/m/{n}")
                    })),
                )
            }),
        )
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}/deliver"), received, handle)
}

#[tokio::test]
async fn slack_bidirectional_signatures_mrkdwn_files_synced_and_echo_suppression() {
    let h = TestHarness::new().await;
    let (pid, prod_tok) = h.create_project_token("support", "production").await;
    let preview_tok = h.mint_project_token(&pid, "preview").await;
    let (_alice_id, alice_tok) = h.create_user("alice", "member").await;

    let (mock_url, delivered, mock_srv) = start_mock_platform_server(0).await;

    // Preview registers `slack` first, then production supersedes preview, then preview cannot overwrite production!
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&preview_tok),
            &[],
            Some(json!({
                "key": "slack",
                "platform": "slack",
                "mappings": { "#incidents": "incidents" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, prod_src) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&prod_tok),
            &[],
            Some(json!({
                "key": "slack",
                "platform": "slack",
                "mappings": { "#incidents": "incidents" },
                "webhookSecret": "slack_secret_123",
                "outboundUrl": mock_url
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(prod_src["source"]["environment"], "production");

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&preview_tok),
            &[],
            Some(json!({
                "key": "slack",
                "platform": "slack",
                "mappings": { "#incidents": "incidents" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);

    // Slack url_verification challenge with valid HMAC signature; bad signature rejected 401
    let chal = json!({ "type": "url_verification", "challenge": "chal_xyz" }).to_string();
    let (st, _) = h
        .req_raw_str(
            Method::POST,
            "/webhooks/slack",
            None,
            &[
                ("x-slack-request-timestamp", "1700000000"),
                ("x-slack-signature", "v0=bad"),
            ],
            chal.clone(),
        )
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let sig = hmac_sha256_hex(b"slack_secret_123", format!("v0:1700000000:{chal}").as_bytes());
    let (st, resp) = h
        .req_raw_str(
            Method::POST,
            "/webhooks/slack",
            None,
            &[
                ("x-slack-request-timestamp", "1700000000"),
                ("x-slack-signature", &format!("v0={sig}")),
            ],
            chal,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(resp["challenge"], "chal_xyz");

    // Inbound Slack message -> creates thread with synced user profile + GFM conversion
    let msg_hook = json!({
        "type": "event_callback",
        "event": {
            "type": "message",
            "channel": "#incidents",
            "ts": "1727000.001",
            "user": "U0456",
            "user_name": "Malte",
            "text": "*checkout* is ~slow~ <https://status.test|status>",
            "permalink": "https://acme.slack.com/archives/C1/p1727000001"
        }
    })
    .to_string();
    let sig = hmac_sha256_hex(
        b"slack_secret_123",
        format!("v0:1700000001:{msg_hook}").as_bytes(),
    );
    let (st, hook_res) = h
        .req_raw_str(
            Method::POST,
            "/webhooks/slack",
            None,
            &[
                ("x-slack-request-timestamp", "1700000001"),
                ("x-slack-signature", &format!("v0={sig}")),
            ],
            msg_hook.clone(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let root_id = hook_res["messageId"].as_str().unwrap().to_string();

    // Duplicate webhook suppressed (echo/dedup)
    let (st, dup_res) = h
        .req_raw_str(
            Method::POST,
            "/webhooks/slack",
            None,
            &[
                ("x-slack-request-timestamp", "1700000001"),
                ("x-slack-signature", &format!("v0={sig}")),
            ],
            msg_hook,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(dup_res["ignored"], "duplicate");

    // Verify synced user actor ID, handle, and mrkdwn->GFM conversion
    let (st, view) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(view["text"], "**checkout** is ~~slow~~ [status](https://status.test)");
    assert_eq!(view["from"], "slack");
    assert_eq!(view["by"]["handle"], "support/malte");
    assert!(view["by"]["actor"].as_str().unwrap().ends_with("+slack:user:u0456"));

    // Upload attachment & reply from board with a label -> outbound converts GFM->mrkdwn,
    // NEVER includes labels, emits `message.synced` + `message.files_synced`!
    let att_req = Request::builder()
        .method(Method::POST)
        .uri("/v1/messageboard/boards/General/attachments?name=log.txt")
        .header("authorization", format!("Bearer {alice_tok}"))
        .header("content-type", "text/plain")
        .body(Body::from("trace log"))
        .unwrap();
    let att_resp = h.app.clone().oneshot(att_req).await.unwrap();
    let att_json: Value =
        serde_json::from_slice(&att_resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let att_id = att_json["id"].as_str().unwrap().to_string();

    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/replies"),
            Some(&alice_tok),
            &[],
            Some(json!({
                "text": "**investigating** now",
                "labels": { "internal": "secret" },
                "attachments": [att_id]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Also post a `noSync: true` reply -> must NEVER be delivered externally
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/replies"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "private board note", "noSync": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let sum = sync::process_pending_deliveries(&h.state).await.unwrap();
    assert_eq!(sum.delivered, 1);

    {
        let items = delivered.lock().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["text"], "*investigating* now");
        assert!(items[0].get("labels").is_none());
        assert_eq!(items[0]["attachments"].as_array().unwrap().len(), 1);
    }

    // Verify bookkeeping `message.synced` and `message.files_synced` events with `all=true`
    let (st, evs) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/events?after=0&all=true&types=message.synced,message.files_synced",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let types: Vec<&str> = evs["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["type"].as_str().unwrap())
        .collect();
    assert!(types.contains(&"message.synced"));
    assert!(types.contains(&"message.files_synced"));

    mock_srv.abort();
}

#[tokio::test]
async fn github_linear_custom_sync_nosync_root_retries_permissions_and_typing() {
    let h = TestHarness::new().await;
    let (_pid, proj_tok) = h.create_project_token("ops", "production").await;
    let (_alice_id, alice_tok) = h.create_user("alice", "member").await;
    let (_eve_id, eve_tok) = h.create_user("eve", "viewer").await;

    // 1. GitHub inbound-only default: PR + review labels (`pr`, `state`, `head`, `tag.*`, `assignee.*`, `review`)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&proj_tok),
            &[],
            Some(json!({
                "key": "gh",
                "platform": "github",
                "mappings": { "acme/web": { "pulls": "prs", "issues": "issues" } },
                "webhookSecret": "gh_sec"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let pr_body = json!({
        "action": "opened",
        "repository": { "full_name": "acme/web" },
        "pull_request": {
            "number": 42,
            "title": "Add sync engine",
            "body": "Clean-room sync",
            "state": "open",
            "draft": false,
            "merged": false,
            "html_url": "https://github.com/acme/web/pull/42",
            "head": { "sha": "abc1234" },
            "user": { "login": "octocat" },
            "labels": [{ "name": "backend" }],
            "assignees": [{ "login": "ada" }]
        }
    })
    .to_string();
    let gh_sig = format!("sha256={}", hmac_sha256_hex(b"gh_sec", pr_body.as_bytes()));
    let (st, pr_res) = h
        .req_raw_str(
            Method::POST,
            "/v1/messageboard/boards/General/webhooks/gh",
            None,
            &[
                ("x-github-event", "pull_request"),
                ("x-hub-signature-256", &gh_sig),
            ],
            pr_body,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let pr_root_id = pr_res["messageId"].as_str().unwrap().to_string();

    // PR review adds `review=approved` label to root
    let rev_body = json!({
        "action": "submitted",
        "repository": { "full_name": "acme/web" },
        "pull_request": { "number": 42 },
        "review": {
            "id": 99,
            "state": "approved",
            "body": "LGTM!",
            "html_url": "https://github.com/acme/web/pull/42#pullrequestreview-99",
            "user": { "login": "reviewer" }
        }
    })
    .to_string();
    let rev_sig = format!("sha256={}", hmac_sha256_hex(b"gh_sec", rev_body.as_bytes()));
    let (st, _) = h
        .req_raw_str(
            Method::POST,
            "/v1/messageboard/boards/General/webhooks/gh",
            None,
            &[
                ("x-github-event", "pull_request_review"),
                ("x-hub-signature-256", &rev_sig),
            ],
            rev_body,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, pr_msg) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/messages/{pr_root_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(pr_msg["channel"], "prs");
    assert_eq!(pr_msg["labels"]["pr"], "acme/web#42");
    assert_eq!(pr_msg["labels"]["state"], "open");
    assert_eq!(pr_msg["labels"]["head"], "abc1234");
    assert_eq!(pr_msg["labels"]["tag.backend"], "true");
    assert_eq!(pr_msg["labels"]["assignee.ada"], "true");
    assert_eq!(pr_msg["labels"]["review"], "approved");

    // 2. Linear bidirectional sync (first line = title, remainder = body) + `Linear-Signature`
    let (lin_url, lin_delivered, lin_srv) = start_mock_platform_server(0).await;
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&proj_tok),
            &[],
            Some(json!({
                "key": "linear",
                "platform": "linear",
                "mappings": { "ENG": "eng" },
                "webhookSecret": "lin_sec",
                "outboundUrl": lin_url
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, eng_root) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "eng", "text": "Fix login bug\nHappens on Safari" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let _eng_id = eng_root["id"].as_str().unwrap();

    // `noSync: true` root keeps ENTIRE thread (including normal replies!) off Linear
    let (st, nosync_root) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "eng", "text": "Internal draft", "noSync": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let nosync_root_id = nosync_root["id"].as_str().unwrap();
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{nosync_root_id}/replies"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "reply in noSync thread must NOT sync" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    sync::process_pending_deliveries(&h.state).await.unwrap();
    {
        let items = lin_delivered.lock().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["title"], "Fix login bug");
        assert_eq!(items[0]["body"], "Happens on Safari");
    }
    lin_srv.abort();

    // 3. 5-retry failure emits `message.sync_failed` (non-blocking!) on Custom sync
    let (fail_url, _fail_recv, fail_srv) = start_mock_platform_server(10).await;
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&proj_tok),
            &[],
            Some(json!({
                "key": "custom",
                "platform": "custom",
                "mappings": { "room1": "custom-ch" },
                "outboundUrl": fail_url
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, cm) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "custom-ch", "text": "will fail 5x" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let cm_id = cm["id"].as_str().unwrap().to_string();

    let mut t = Utc::now();
    for attempt in 1..=5 {
        let sum = sync::process_pending_deliveries_at(&h.state, t).await.unwrap();
        if attempt < 5 {
            assert_eq!(sum.retried, 1);
        } else {
            assert_eq!(sum.failed, 1);
        }
        t += Duration::seconds(60);
    }
    fail_srv.abort();

    let (st, failed_evs) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/events?after=0&types=message.sync_failed",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(failed_evs["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["message"]["id"] == cm_id));

    // 4. Permissions & guardrails:
    // - Viewer lacks `* sync` (inbox -> 403) and `* manage` (records -> 403)
    // - Project has `* sync` (inbox -> 200) but lacks `* manage` (records -> 403)
    // - Owner has `* manage` (records -> 200)
    // - Sandboxed owner denied all sync endpoints (403)
    let (st, _) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/sources/gh/inbox",
            Some(&eve_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, inbox_resp) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/sources/gh/inbox",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(inbox_resp["inbox"].as_array().unwrap().len(), 2);

    let (st, _) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/sources/gh/records",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, rec_resp) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/sources/gh/records",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(rec_resp["links"].as_array().unwrap().len(), 1);

    let (st, _) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&h.owner_token),
            &[("x-board-sandboxed", "true")],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // 5. `linked(url)` visibility + `typing` status <= 50 chars
    let (st, linked) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/linked?url=https://github.com/acme/web/pull/42",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(linked["message"]["id"], pr_root_id);

    // Close `prs` to everyone -> Alice can no longer see linked thread (404)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "everyone" }],
                "can": { "prs": "none" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, _) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/sync/linked?url=https://github.com/acme/web/pull/42",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Typing validation (> 50 chars rejected 400; <= 50 chars accepted 200)
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/threads/{cm_id}/typing"),
            Some(&alice_tok),
            &[],
            Some(json!({ "status": "a".repeat(51) })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (st, typ_res) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/threads/{cm_id}/typing"),
            Some(&alice_tok),
            &[],
            Some(json!({ "status": "running tests..." })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(typ_res["targets"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn spec_audit_github_label_change_events_and_webhook_dedup() {
    let h = TestHarness::new().await;
    let (_pid, proj_tok) = h.create_project_token("ops", "production").await;
    let (_alice_id, alice_tok) = h.create_user("alice", "member").await;

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/sync/sources",
            Some(&proj_tok),
            &[],
            Some(json!({
                "key": "gh",
                "platform": "github",
                "mappings": { "acme/web": "prs" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Open PR #7
    let open_pr = json!({
        "action": "opened",
        "repository": { "full_name": "acme/web" },
        "pull_request": {
            "number": 7,
            "title": "Ship fix",
            "body": "Body",
            "state": "open",
            "draft": false,
            "merged": false,
            "html_url": "https://github.com/acme/web/pull/7",
            "head": { "sha": "111aaa" },
            "user": { "login": "octocat" }
        }
    })
    .to_string();
    let (st, pr_res) = h
        .req_raw_str(
            Method::POST,
            "/v1/messageboard/boards/General/webhooks/gh",
            None,
            &[("x-github-event", "pull_request")],
            open_pr,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let pr_root = pr_res["messageId"].as_str().unwrap().to_string();

    // Merge PR #7 (no text edit) -> MUST emit `labels.changed` event with `state=merged`
    let merge_pr = json!({
        "action": "closed",
        "repository": { "full_name": "acme/web" },
        "pull_request": {
            "number": 7,
            "title": "Ship fix",
            "body": "Body",
            "state": "closed",
            "draft": false,
            "merged": true,
            "html_url": "https://github.com/acme/web/pull/7",
            "head": { "sha": "222bbb" },
            "user": { "login": "octocat" }
        }
    })
    .to_string();
    let (st, _) = h
        .req_raw_str(
            Method::POST,
            "/v1/messageboard/boards/General/webhooks/gh",
            None,
            &[("x-github-event", "pull_request")],
            merge_pr,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, evs) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/events?after={pr_root}&label=state=merged"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(
        evs["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == "labels.changed" && e["payload"]["set"]["state"] == "merged"),
        "expected labels.changed event for merged PR, got {evs}"
    );

    // Redelivered issue_comment webhook must be deduplicated
    let comment_hook = json!({
        "action": "created",
        "repository": { "full_name": "acme/web" },
        "issue": { "number": 7 },
        "comment": {
            "id": 501,
            "body": "looks good",
            "html_url": "https://github.com/acme/web/pull/7#issuecomment-501",
            "user": { "login": "reviewer" }
        }
    })
    .to_string();
    let (st, r1) = h
        .req_raw_str(
            Method::POST,
            "/v1/messageboard/boards/General/webhooks/gh",
            None,
            &[("x-github-event", "issue_comment")],
            comment_hook.clone(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(r1["messageId"].is_string());

    let (st, r2) = h
        .req_raw_str(
            Method::POST,
            "/v1/messageboard/boards/General/webhooks/gh",
            None,
            &[("x-github-event", "issue_comment")],
            comment_hook,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(r2["ignored"], "duplicate");

    let (st, thread) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/threads/{pr_root}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(thread["replies"].as_array().unwrap().len(), 1);
}
