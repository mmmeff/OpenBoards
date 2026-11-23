use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::routing::post;
use axum::Router;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tokio::sync::Notify;
use tower::ServiceExt;

use openboards::actors;
use openboards::boards;
use openboards::config::ServerConfig;
use openboards::db::Database;
use openboards::watchers::{
    self, DeliveryPolicy, HEADER_ATTEMPT, HEADER_DELIVERY_ID, HEADER_EVENT_CURSOR,
    HEADER_EVENT_TYPE, HEADER_SIGNATURE, HEADER_WATCHER_ID,
};
use openboards::{build_router, AppState};

struct TestHarness {
    _tmp: tempfile::TempDir,
    state: AppState,
    app: Router,
    owner_token: String,
    http_client: reqwest::Client,
}

impl TestHarness {
    fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let owner_token = "owner-bootstrap-secret".to_string();
        let db = Database::open_in_memory().unwrap();
        actors::bootstrap_owner(&db, Some(&owner_token)).unwrap();
        boards::ensure_default_board(&db).unwrap();
        let state = AppState::new(
            db,
            ServerConfig {
                listen: "127.0.0.1:8080".parse().unwrap(),
                data_dir: tmp.path().to_path_buf(),
                team: "acme".to_string(),
                admin_token: Some(owner_token.clone()),
                rate_limit_rps: 0,
                rate_limit_burst: 100,
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

    async fn create_user_with_token(&self, username: &str, email: &str, role: &str) -> (String, String) {
        let (st, body) = self
            .req(
                Method::POST,
                "/v1/admin/users",
                Some(&self.owner_token),
                &[],
                Some(json!({ "username": username, "email": email, "role": role })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED);
        let uid = body["user"]["id"].as_str().unwrap().to_string();

        let (st, body) = self
            .req(
                Method::POST,
                "/v1/admin/tokens",
                Some(&self.owner_token),
                &[],
                Some(json!({ "principal_kind": "user", "principal_id": uid })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED);
        let tok = body["token"]["token"].as_str().unwrap().to_string();
        (uid, tok)
    }

    async fn create_project_with_token(&self, name: &str, env: &str) -> (String, String) {
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

        let (st, body) = self
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
        let tok = body["token"]["token"].as_str().unwrap().to_string();
        (pid, tok)
    }
}

#[derive(Debug, Clone)]
struct CapturedWebhook {
    headers: HeaderMap,
    raw_body: Vec<u8>,
    json_body: Value,
}

#[derive(Clone)]
struct MockWebhookState {
    received: Arc<Mutex<Vec<CapturedWebhook>>>,
    notify: Arc<Notify>,
    flaky_failures_remaining: Arc<Mutex<usize>>,
}

struct MockWebhookServer {
    base_url: String,
    state: MockWebhookState,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for MockWebhookServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl MockWebhookServer {
    async fn start(flaky_failures: usize) -> Self {
        let state = MockWebhookState {
            received: Arc::new(Mutex::new(Vec::new())),
            notify: Arc::new(Notify::new()),
            flaky_failures_remaining: Arc::new(Mutex::new(flaky_failures)),
        };

        let router = Router::new()
            .route("/ok", post(mock_ok_handler))
            .route("/flaky", post(mock_flaky_handler))
            .route("/fail", post(mock_fail_handler))
            .with_state(state.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Self {
            base_url: format!("http://{addr}"),
            state,
            handle,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn captured(&self) -> Vec<CapturedWebhook> {
        self.state.received.lock().unwrap().clone()
    }

    fn clear(&self) {
        self.state.received.lock().unwrap().clear();
    }
}

fn record_webhook(state: &MockWebhookState, headers: HeaderMap, body: axum::body::Bytes) {
    let raw_body = body.to_vec();
    let json_body: Value = serde_json::from_slice(&raw_body).unwrap_or(Value::Null);
    state.received.lock().unwrap().push(CapturedWebhook {
        headers,
        raw_body,
        json_body,
    });
    state.notify.notify_waiters();
}

async fn mock_ok_handler(
    State(state): State<MockWebhookState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> StatusCode {
    record_webhook(&state, headers, body);
    StatusCode::OK
}

async fn mock_flaky_handler(
    State(state): State<MockWebhookState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> StatusCode {
    record_webhook(&state, headers, body);
    let should_fail = {
        let mut remaining = state.flaky_failures_remaining.lock().unwrap();
        if *remaining > 0 {
            *remaining -= 1;
            true
        } else {
            false
        }
    };
    if should_fail {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

async fn mock_fail_handler(
    State(state): State<MockWebhookState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> StatusCode {
    record_webhook(&state, headers, body);
    StatusCode::INTERNAL_SERVER_ERROR
}

fn fast_policy(max_attempts: u32) -> DeliveryPolicy {
    DeliveryPolicy {
        max_attempts,
        initial_backoff: Duration::from_millis(10),
        max_backoff: Duration::from_millis(40),
        request_timeout: Duration::from_secs(5),
    }
}

#[tokio::test]
async fn real_http_webhook_delivery_hmac_signature_and_cursor_persistence() {
    let h = TestHarness::new();
    let mock = MockWebhookServer::start(0).await;
    let (_alice_id, alice_tok) = h
        .create_user_with_token("alice", "alice@acme.com", "member")
        .await;
    let (pid, proj_tok) = h.create_project_with_token("support-app", "production").await;

    let secret = "whsec_super_secret_key";
    let (st, reg) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "triage",
                "tail": "support",
                "url": mock.url("/ok"),
                "secret": secret
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(reg["created"], true);
    assert_eq!(reg["watcher"]["id"], "triage");
    assert_eq!(
        reg["watcher"]["actor"],
        format!("project:{pid}:production+triage")
    );
    assert_eq!(reg["watcher"]["hasSecret"], true);
    assert!(reg["watcher"].get("secret").is_none());

    // Post one matching message in `support` and one non-matching message in `random`
    let (st, msg1) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "support", "text": "customer login broken" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let msg1_id = msg1["id"].as_str().unwrap().to_string();

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "random", "text": "lunch plans" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let report = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(3))
        .await
        .unwrap();
    assert_eq!(report.matched, 1);
    assert_eq!(report.delivered, 1);
    assert_eq!(report.dead_lettered, 0);

    let calls = mock.captured();
    assert_eq!(calls.len(), 1);
    let call = &calls[0];

    // Verify HMAC-SHA256 signature over exact raw body bytes
    let expected_sig = watchers::format_signature_header(secret, &call.raw_body);
    assert_eq!(
        call.headers.get(HEADER_SIGNATURE).unwrap().to_str().unwrap(),
        expected_sig
    );

    // Verify deterministic delivery ID header matches body `deliveryId`
    let hdr_delivery_id = call
        .headers
        .get(HEADER_DELIVERY_ID)
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(call.json_body["deliveryId"], hdr_delivery_id);
    assert_eq!(
        call.headers
            .get(HEADER_WATCHER_ID)
            .unwrap()
            .to_str()
            .unwrap(),
        "triage"
    );
    assert_eq!(
        call.headers
            .get(HEADER_EVENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        "message.posted"
    );
    assert_eq!(
        call.headers.get(HEADER_ATTEMPT).unwrap().to_str().unwrap(),
        "1"
    );
    assert_eq!(call.json_body["event"]["message"]["id"], msg1_id);
    assert_eq!(
        call.headers
            .get(HEADER_EVENT_CURSOR)
            .unwrap()
            .to_str()
            .unwrap(),
        call.json_body["event"]["cursor"].as_str().unwrap()
    );

    // Inspect watcher detail endpoint: cursor advanced and delivery summary updated
    let (st, detail) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers/triage",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(detail["watcher"]["lastSeq"].as_i64().unwrap() >= 2);
    assert_eq!(detail["watcher"]["deliveries"]["total"], 1);
    assert_eq!(detail["watcher"]["deliveries"]["delivered"], 1);
    assert_eq!(detail["watcher"]["deliveries"]["deadLetter"], 0);
    assert_eq!(detail["recentDeliveries"].as_array().unwrap().len(), 1);
    assert_eq!(detail["recentDeliveries"][0]["deliveryId"], hdr_delivery_id);
    assert_eq!(detail["recentDeliveries"][0]["status"], "delivered");
    assert_eq!(detail["recentDeliveries"][0]["lastHttpStatus"], 200);

    // Second dispatch pass delivers nothing (cursor persisted)
    let report2 = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(3))
        .await
        .unwrap();
    assert_eq!(report2.matched, 0);
    assert_eq!(mock.captured().len(), 1);
}

#[tokio::test]
async fn before_and_after_label_triggers_and_channel_visibility() {
    let h = TestHarness::new();
    let mock = MockWebhookServer::start(0).await;
    let (_alice_id, alice_tok) = h
        .create_user_with_token("alice", "alice@acme.com", "member")
        .await;
    let (_pid, proj_tok) = h.create_project_with_token("label-bot", "production").await;

    // Create private channel owned by owner (project cannot read it)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&h.owner_token),
            &[],
            Some(json!({ "path": "secret", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Watcher on `--label state=open` across all readable channels
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "open-tracker",
                "tail": "--label state=open",
                "url": mock.url("/ok")
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // 1. Owner posts in private channel with state=open -> MUST NOT reach project watcher
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "channel": "secret",
                "text": "classified open item",
                "labels": { "state": "open" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let r = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(2))
        .await
        .unwrap();
    assert_eq!(r.delivered, 0);
    assert_eq!(mock.captured().len(), 0);

    // 2. Alice posts in `support` with state=open -> matches AFTER
    let (st, post_msg) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "support",
                "text": "issue #1",
                "labels": { "state": "open" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let root_id = post_msg["id"].as_str().unwrap().to_string();

    // 3. Alice changes state=open -> state=closed -> matches BEFORE (leaving view!)
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/labels"),
            Some(&alice_tok),
            &[],
            Some(json!({ "set": { "state": "closed" } })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // 4. Alice replies while thread is closed -> neither before nor after matches -> NOT delivered
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/replies"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "thanks for closing" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // 5. Alice re-opens state=open -> matches AFTER (entering view!)
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/labels"),
            Some(&alice_tok),
            &[],
            Some(json!({ "set": { "state": "open" } })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let r = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(2))
        .await
        .unwrap();
    assert_eq!(r.delivered, 3);

    let calls = mock.captured();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0].json_body["event"]["type"], "message.posted");
    assert_eq!(calls[1].json_body["event"]["type"], "labels.changed");
    assert_eq!(
        calls[1].json_body["event"]["payload"]["set"]["state"],
        "closed"
    );
    assert_eq!(calls[2].json_body["event"]["type"], "labels.changed");
    assert_eq!(
        calls[2].json_body["event"]["payload"]["set"]["state"],
        "open"
    );
}

#[tokio::test]
async fn self_write_loop_suppression_blocks_own_actor_but_allows_other_watchers() {
    let h = TestHarness::new();
    let mock = MockWebhookServer::start(0).await;
    let (_pid, proj_tok) = h.create_project_with_token("swarm", "production").await;

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "triage",
                "tail": "support",
                "url": mock.url("/ok")
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // 1. Write as `<owner_principal>+triage` -> MUST be suppressed
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&proj_tok),
            &[("x-board-profile", "triage")],
            Some(json!({ "channel": "support", "text": "self message from triage" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let r1 = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(2))
        .await
        .unwrap();
    assert_eq!(r1.skipped_self, 1);
    assert_eq!(r1.delivered, 0);
    assert_eq!(mock.captured().len(), 0);

    // 2. Write as another watcher profile `<owner_principal>+responder` -> MUST trigger `triage`
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&proj_tok),
            &[("x-board-profile", "responder")],
            Some(json!({ "channel": "support", "text": "from another watcher" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // 3. Write as bare deployment `<owner_principal>` (no profile) -> MUST trigger `triage`
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&proj_tok),
            &[],
            Some(json!({ "channel": "support", "text": "from bare deployment" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let r2 = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(2))
        .await
        .unwrap();
    assert_eq!(r2.skipped_self, 0);
    assert_eq!(r2.delivered, 2);

    let calls = mock.captured();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        calls[0].json_body["event"]["message"]["text"],
        "from another watcher"
    );
    assert_eq!(
        calls[1].json_body["event"]["message"]["text"],
        "from bare deployment"
    );
}

#[tokio::test]
async fn exponential_backoff_retries_preserve_delivery_id_and_record_dead_letter() {
    let h = TestHarness::new();
    let mock = MockWebhookServer::start(2).await; // Fail first 2 calls on /flaky, succeed on 3rd
    let (_alice_id, alice_tok) = h
        .create_user_with_token("alice", "alice@acme.com", "member")
        .await;
    let (_pid, proj_tok) = h.create_project_with_token("retries", "production").await;

    // Register flaky watcher (succeeds on attempt 3) and always-failing watcher (dead-letters)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "flaky_w",
                "tail": "alerts",
                "url": mock.url("/flaky")
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "dead_w",
                "tail": "alerts",
                "url": mock.url("/fail")
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "alerts", "text": "cpu 99%" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let report = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(3))
        .await
        .unwrap();
    assert_eq!(report.matched, 2);
    assert_eq!(report.delivered, 1);
    assert_eq!(report.dead_lettered, 1);

    let calls = mock.captured();
    assert_eq!(calls.len(), 6); // 3 attempts for flaky_w + 3 attempts for dead_w

    let flaky_calls: Vec<&CapturedWebhook> = calls
        .iter()
        .filter(|c| c.headers.get(HEADER_WATCHER_ID).unwrap() == "flaky_w")
        .collect();
    assert_eq!(flaky_calls.len(), 3);
    let delivery_id_0 = flaky_calls[0]
        .headers
        .get(HEADER_DELIVERY_ID)
        .unwrap()
        .to_str()
        .unwrap();
    for (idx, c) in flaky_calls.iter().enumerate() {
        assert_eq!(
            c.headers.get(HEADER_DELIVERY_ID).unwrap().to_str().unwrap(),
            delivery_id_0
        );
        assert_eq!(c.json_body["deliveryId"], delivery_id_0);
        assert_eq!(
            c.headers.get(HEADER_ATTEMPT).unwrap().to_str().unwrap(),
            (idx + 1).to_string()
        );
    }

    // Verify detail for flaky_w -> delivered after 3 attempts
    let (st, flaky_detail) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers/flaky_w",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(flaky_detail["watcher"]["deliveries"]["delivered"], 1);
    assert_eq!(flaky_detail["watcher"]["deliveries"]["deadLetter"], 0);
    assert_eq!(flaky_detail["recentDeliveries"][0]["status"], "delivered");
    assert_eq!(flaky_detail["recentDeliveries"][0]["attempts"], 3);
    assert_eq!(flaky_detail["recentDeliveries"][0]["lastHttpStatus"], 200);

    // Verify detail for dead_w -> dead_letter after 3 attempts with HTTP 500
    let (st, dead_detail) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers/dead_w",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(dead_detail["watcher"]["deliveries"]["delivered"], 0);
    assert_eq!(dead_detail["watcher"]["deliveries"]["deadLetter"], 1);
    assert_eq!(dead_detail["recentDeliveries"][0]["status"], "dead_letter");
    assert_eq!(dead_detail["recentDeliveries"][0]["attempts"], 3);
    assert_eq!(dead_detail["recentDeliveries"][0]["lastHttpStatus"], 500);
    assert!(dead_detail["recentDeliveries"][0]["lastError"]
        .as_str()
        .unwrap()
        .contains("500"));
}

#[tokio::test]
async fn background_dispatcher_wakes_on_event_notify_and_delivers() {
    let h = TestHarness::new();
    let mock = MockWebhookServer::start(0).await;
    let (_alice_id, alice_tok) = h
        .create_user_with_token("alice", "alice@acme.com", "member")
        .await;
    let (_pid, proj_tok) = h.create_project_with_token("live-bg", "production").await;

    let bg_handle =
        watchers::spawn_watcher_dispatcher_with_policy(h.state.clone(), fast_policy(2));

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "live_w",
                "tail": "intake",
                "url": mock.url("/ok")
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Give the background loop a brief moment to subscribe to the board
    tokio::time::sleep(Duration::from_millis(50)).await;

    let notified = mock.state.notify.notified();
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "intake", "text": "hello live watcher" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    tokio::time::timeout(Duration::from_secs(3), notified)
        .await
        .expect("background watcher dispatcher should deliver webhook");

    bg_handle.abort();

    let calls = mock.captured();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].json_body["event"]["message"]["text"],
        "hello live watcher"
    );
}

#[tokio::test]
async fn atomic_deployment_sync_preserves_cursors_inserts_new_and_deletes_omitted() {
    let h = TestHarness::new();
    let mock = MockWebhookServer::start(0).await;
    let (_alice_id, alice_tok) = h
        .create_user_with_token("alice", "alice@acme.com", "member")
        .await;
    let (_pid, proj_tok) = h.create_project_with_token("sync-app", "production").await;

    // Initial sync (map form): register `keep` and `remove_me`
    let (st, res1) = h
        .req(
            Method::POST,
            "/api/board/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "url": mock.url("/ok"),
                "watchers": {
                    "keep": { "tail": "dev" },
                    "remove_me": { "tail": "ops" }
                }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(res1["ok"], true);
    assert_eq!(res1["watchers"].as_array().unwrap().len(), 2);

    // Post in `dev` and dispatch so `keep` advances its cursor
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "dev", "text": "first dev post" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let r1 = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(2))
        .await
        .unwrap();
    assert_eq!(r1.delivered, 1);
    mock.clear();

    let (st, before_sync) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers/keep",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let saved_seq = before_sync["watcher"]["lastSeq"].as_i64().unwrap();
    assert!(saved_seq > 0);

    // Post a second message in `dev` BEFORE syncing, without dispatching yet
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "dev", "text": "pending while syncing" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Atomic replace (list form): update `keep`, add `brand_new`, drop `remove_me`
    let (st, res2) = h
        .req(
            Method::POST,
            "/api/board/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "url": mock.url("/ok"),
                "watchers": [
                    { "id": "keep", "tail": "dev/" },
                    { "id": "brand_new", "tail": "dev" }
                ]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(res2["updated"], json!(["keep"]));
    assert_eq!(res2["created"], json!(["brand_new"]));
    assert_eq!(res2["deleted"], json!(["remove_me"]));

    // Verify `keep` preserved its cursor, while `brand_new` started at the latest seq
    let (st, after_keep) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers/keep",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(after_keep["watcher"]["lastSeq"].as_i64().unwrap(), saved_seq);
    assert_eq!(after_keep["watcher"]["tail"], "dev/");

    let (st, after_new) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers/brand_new",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(after_new["watcher"]["lastSeq"].as_i64().unwrap() > saved_seq);

    // `remove_me` is gone
    let (st, err) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers/remove_me",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(err["code"], "watcher_not_found");

    // Dispatching now delivers the pending message ONLY to `keep` (cursor preserved), NOT `brand_new`
    let r2 = watchers::dispatch_all_once(&h.state.db, &h.http_client, &fast_policy(2))
        .await
        .unwrap();
    assert_eq!(r2.delivered, 1);
    let calls = mock.captured();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].headers.get(HEADER_WATCHER_ID).unwrap(),
        "keep"
    );
    assert_eq!(
        calls[0].json_body["event"]["message"]["text"],
        "pending while syncing"
    );
}

#[tokio::test]
async fn enforces_32_watcher_cap_validation_and_sandboxed_caller_denial() {
    let h = TestHarness::new();
    let (_alice_id, alice_tok) = h
        .create_user_with_token("alice", "alice@acme.com", "member")
        .await;
    let (_pid, proj_tok) = h.create_project_with_token("cap-app", "production").await;

    // 1. Sandboxed & narrowed callers are ALWAYS denied (403 forbidden)
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[("x-board-sandboxed", "true")],
            Some(json!({ "id": "w1", "tail": "dev", "url": "https://example.com/hook" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(err["code"], "forbidden");

    let (st, err) = h
        .req(
            Method::POST,
            "/api/board/watchers",
            Some(&proj_tok),
            &[("x-board-can", "all")],
            Some(json!({
                "url": "https://example.com/hook",
                "watchers": [{ "id": "w1", "tail": "dev" }]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(err["code"], "forbidden");

    // 2. Non-project caller denied with 403 watchers_need_project
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&alice_tok),
            &[],
            Some(json!({ "id": "w1", "tail": "dev", "url": "https://example.com/hook" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(err["code"], "watchers_need_project");

    // 3. Validation failures: invalid ID, --after in tail, after in body, mentions=me
    for bad_body in [
        json!({ "id": "BadUpper", "tail": "dev", "url": "https://example.com/hook" }),
        json!({ "id": "bad.dot", "tail": "dev", "url": "https://example.com/hook" }),
        json!({ "id": "w_after", "tail": "dev --after ev_1", "url": "https://example.com/hook" }),
        json!({ "id": "w_after2", "tail": "dev", "after": "ev_1", "url": "https://example.com/hook" }),
        json!({ "id": "w_me", "tail": "dev --mentions me", "url": "https://example.com/hook" }),
        json!({ "id": "w_me2", "tail": "dev --mentions @me", "url": "https://example.com/hook" }),
        json!({ "id": "w_me3", "mentions": "me", "url": "https://example.com/hook" }),
        json!({ "id": "w_url", "tail": "dev", "url": "ftp://example.com/hook" }),
    ] {
        let (st, err) = h
            .req(
                Method::POST,
                "/v1/messageboard/boards/General/watchers",
                Some(&proj_tok),
                &[],
                Some(bad_body),
            )
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "expected 400, got {err}");
        assert_eq!(err["code"], "bad_request");
    }

    // 4. Atomic sync with 33 watchers fails with 400 too_many_watchers
    let too_many: Vec<Value> = (0..33)
        .map(|i| json!({ "id": format!("w_{i}"), "tail": "dev" }))
        .collect();
    let (st, err) = h
        .req(
            Method::POST,
            "/api/board/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "url": "https://example.com/hook",
                "watchers": too_many
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "too_many_watchers");

    // 5. Register exactly 32 watchers incrementally -> all succeed
    for i in 0..32 {
        let (st, _) = h
            .req(
                Method::POST,
                "/v1/messageboard/boards/General/watchers",
                Some(&proj_tok),
                &[],
                Some(json!({
                    "id": format!("w_{i}"),
                    "tail": "dev",
                    "url": "https://example.com/hook"
                })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED);
    }

    // Updating an existing watcher at cap still succeeds (200)
    let (st, upd) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "w_0",
                "tail": "dev/",
                "url": "https://example.com/hook2"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(upd["created"], false);

    // 33rd watcher fails with 400 too_many_watchers
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "w_32",
                "tail": "dev",
                "url": "https://example.com/hook"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "too_many_watchers");

    // Owner can list all 32 watchers
    let (st, listed) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(listed["watchers"].as_array().unwrap().len(), 32);

    // Delete one watcher -> 33rd registration now succeeds
    let (st, del) = h
        .req(
            Method::DELETE,
            "/v1/messageboard/boards/General/watchers/w_0",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(del["deleted"], "w_0");

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "w_32",
                "tail": "dev",
                "url": "https://example.com/hook"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Bulk DELETE clears all watchers on board for this deployment
    let (st, bulk_del) = h
        .req(
            Method::DELETE,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(bulk_del["deleted"], 32);

    let (st, empty_list) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(empty_list["watchers"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn mentions_and_types_filters_with_dispatch_watcher_once() {
    let h = TestHarness::new();
    let mock = MockWebhookServer::start(0).await;
    let (_alice_id, alice_tok) = h
        .create_user_with_token("alice", "alice@acme.com", "member")
        .await;
    let (_bob_id, bob_tok) = h
        .create_user_with_token("bob", "bob@acme.com", "member")
        .await;
    let (pid, proj_tok) = h.create_project_with_token("mentions-bot", "production").await;

    // Seed actor activity so @alice is visible on General
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "dev", "text": "alice online" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, reg) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/watchers",
            Some(&proj_tok),
            &[],
            Some(json!({
                "id": "ping_alice",
                "tail": "dev/ --mentions @alice --types message.posted",
                "url": mock.url("/ok")
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let board_id = reg["watcher"]["boardId"].as_str().unwrap().to_string();
    let owner_principal = format!("project:{pid}:production");

    // Non-mention message -> ignored
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&bob_tok),
            &[],
            Some(json!({ "channel": "dev/api", "text": "general update" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Mentioning @alice -> matched on message.posted
    let (st, ping) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&bob_tok),
            &[],
            Some(json!({ "channel": "dev/api", "text": "hey @alice please check" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let ping_id = ping["id"].as_str().unwrap().to_string();

    // Editing the message produces message.edited -> excluded by `--types message.posted`
    let (st, _) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{ping_id}"),
            Some(&bob_tok),
            &[],
            Some(json!({ "text": "hey @alice please check ASAP" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let report = watchers::dispatch_watcher_once(
        &h.state.db,
        &h.http_client,
        &board_id,
        &owner_principal,
        "ping_alice",
        &fast_policy(2),
    )
    .await
    .unwrap();
    assert_eq!(report.matched, 1);
    assert_eq!(report.delivered, 1);

    let calls = mock.captured();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].json_body["event"]["type"], "message.posted");
    assert_eq!(calls[0].json_body["event"]["message"]["id"], ping_id);
}
