use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use chrono::{Duration, Utc};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use openboards::actors;
use openboards::boards;
use openboards::config::{DbTarget, ServerConfig};
use openboards::db::Database;
use openboards::tasks;
use openboards::{build_router, AppState};

struct TestHarness {
    _tmp: tempfile::TempDir,
    state: AppState,
    app: axum::Router,
    owner_token: String,
}

impl TestHarness {
    async fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let owner_token = "owner-bootstrap-secret".to_string();
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
                rate_limit_burst: 100,
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
}

#[tokio::test]
async fn task_lifecycle_leases_token_secrecy_expiry_and_ownership() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;
    let (_eve_id, eve_tok) = h.create_user_with_token("eve", "eve@acme.com", "viewer").await;

    // Close `work/` manage from Bob so we can test `403 task_not_owned` cleanly:
    // Bob gets read+post+label on `work/` (NO manage), Alice gets all on `work/`.
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "user", "email": "bob@acme.com" }],
                "can": { "work/": ["read", "post", "label"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Viewer cannot create task (lacks post & label)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&eve_tok),
            &[],
            Some(json!({ "channel": "work", "text": "nope" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Alice (as profile `lead`) creates a task -> state=ready, board.task=ready
    let (st, created) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[("x-board-as", "lead")],
            Some(json!({
                "channel": "work",
                "text": "fix CVE-123",
                "labels": { "step": "fix" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let task_id = created["task"]["id"].as_str().unwrap().to_string();
    assert_eq!(created["task"]["labels"]["board.task"], "ready");
    assert_eq!(created["task"]["labels"]["step"], "fix");
    assert_eq!(created["task"]["task"]["state"], "ready");
    assert!(created["task"]["task"].get("token").is_none());

    // Bob (no manage on work/) cannot cancel or block Alice's unclaimed task -> 403 task_not_owned
    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/cancel"),
            Some(&bob_tok),
            &[],
            Some(json!({ "reason": "nope" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(err["code"], "task_not_owned");

    // Another profile of Alice (`alice/other`) also does NOT own `alice/lead`'s task -> 403 task_not_owned
    // (narrowed without manage so profile ownership is specifically verified)
    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/cancel"),
            Some(&alice_tok),
            &[("x-board-as", "other"), ("x-board-can", "read,post,label")],
            Some(json!({ "reason": "wrong profile" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(err["code"], "task_not_owned");

    // Bob (has read+label) claims the task -> receives epoch=1 + secret token
    let (st, claimed) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/claim"),
            Some(&bob_tok),
            &[("x-board-as", "worker-1")],
            Some(json!({ "lease": 60 })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(claimed["epoch"], 1);
    let token1 = claimed["token"].as_str().unwrap().to_string();
    assert!(!token1.is_empty());
    assert_eq!(claimed["task"]["labels"]["board.task"], "claimed");
    assert_eq!(claimed["task"]["task"]["state"], "claimed");

    // GET /messages/{id} and GET /tasks/{id} hydrate `task` but NEVER expose `token`
    let (st, msg_view) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/messages/{task_id}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(msg_view["task"]["state"], "claimed");
    assert!(msg_view["task"].get("token").is_none());

    // Competing worker trying to claim -> 409 task_claimed
    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/claim"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "task_claimed");

    // Renew with valid (epoch, token) succeeds and emits bookkeeping task.renewed
    let (st, renewed) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/renew"),
            Some(&bob_tok),
            &[("x-board-as", "worker-1")],
            Some(json!({ "epoch": 1, "token": token1, "lease": 120 })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(renewed["epoch"], 1);
    assert_eq!(renewed["token"], token1);

    // Wrong token -> 409 claim_lost
    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/complete"),
            Some(&bob_tok),
            &[],
            Some(json!({ "epoch": 1, "token": "bogus-token", "result": "done" })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "claim_lost");

    // Simulate lease expiration at +5 minutes via deterministic reaper -> transitions claimed -> ready
    let reaped = tasks::reap_expired_leases_at(
        &h.state.db,
        &h.state.events,
        Utc::now() + Duration::minutes(5),
    )
    .await
    .unwrap();
    assert_eq!(reaped, 1);

    // Old holder's renew/complete now fails 409 claim_lost
    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/complete"),
            Some(&bob_tok),
            &[],
            Some(json!({ "epoch": 1, "token": token1, "result": "too late" })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "claim_lost");

    // Re-claim -> epoch increments to 2; worker blocks task with active claim; bare Alice unblocks it!
    let (st, claimed2) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/claim"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(claimed2["epoch"], 2);
    let token2 = claimed2["token"].as_str().unwrap().to_string();

    let (st, blocked) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/block"),
            Some(&bob_tok),
            &[],
            Some(json!({
                "epoch": 2,
                "token": token2,
                "reason": "approve migration?",
                "kind": "awaiting_approval"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(blocked["task"]["task"]["state"], "blocked");
    assert_eq!(blocked["task"]["labels"]["board.task"], "blocked");

    // Claiming a blocked task -> 409 task_blocked
    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/claim"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "task_blocked");

    // Bare Alice (owns her profile `alice/lead`'s tasks!) unblocks even without manage
    let (st, unblocked) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{task_id}/unblock"),
            Some(&alice_tok),
            &[("x-board-can", "read,post,label")],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(unblocked["task"]["task"]["state"], "ready");
    assert_eq!(unblocked["task"]["labels"]["board.task"], "ready");

    // Verify `task.renewed` is hidden without `all=true` and visible with `all=true`
    let (st, evs_default) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/events?after=0&types=task.renewed",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(evs_default["events"].as_array().unwrap().is_empty());

    let (st, evs_all) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/events?after=0&types=task.renewed&all=true",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(evs_all["events"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn dag_needs_cascade_reopen_batch_create_and_idempotency() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;

    // 1. Batch create with internal `{ key }` DAG: fix -> review -> ship
    let batch_body = json!({
        "channel": "work",
        "id": "plan-cve-123",
        "tasks": [
            { "key": "fix", "text": "fix CVE-123", "labels": { "step": "fix" } },
            { "key": "review", "text": "review fix", "labels": { "step": "review" }, "needs": [{ "key": "fix" }] },
            { "text": "ship it", "needs": [{ "key": "review" }, { "key": "fix" }], "for": "shipper" }
        ]
    });
    let (st, created) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(batch_body.clone()),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let arr = created["tasks"].as_array().unwrap();
    assert_eq!(arr.len(), 3);
    let fix_id = arr[0]["id"].as_str().unwrap().to_string();
    let review_id = arr[1]["id"].as_str().unwrap().to_string();
    let ship_id = arr[2]["id"].as_str().unwrap().to_string();
    assert_eq!(arr[0]["task"]["state"], "ready");
    assert_eq!(arr[1]["task"]["state"], "waiting");
    assert_eq!(arr[2]["task"]["state"], "waiting");

    // Batch idempotency replay returns same tasks (200 OK); mismatch returns 409 duplicate_id
    let (st, replay) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(batch_body),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(replay["tasks"][0]["id"], fix_id);

    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "work",
                "id": "plan-cve-123",
                "tasks": [{ "text": "different" }]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "duplicate_id");

    // Batch limits: >15 without id, >14 with id -> 400 batch_too_large; cyclic keys -> 400 invalid_needs
    let sixteen: Vec<Value> = (0..16).map(|i| json!({ "text": format!("t{i}") })).collect();
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "work", "tasks": sixteen })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "batch_too_large");

    let fifteen: Vec<Value> = (0..15).map(|i| json!({ "text": format!("t{i}") })).collect();
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "work", "id": "too-big", "tasks": fifteen })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "batch_too_large");

    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "work",
                "tasks": [
                    { "key": "a", "text": "A", "needs": [{ "key": "b" }] },
                    { "key": "b", "text": "B", "needs": [{ "key": "a" }] }
                ]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "invalid_needs");

    // Unknown field in TaskInBatch (e.g., typo `need` instead of `needs`) rejected before creating anything
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "work",
                "tasks": [{ "text": "typo", "need": [] }]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::UNPROCESSABLE_ENTITY);

    // 2. Fail `fix` -> `review` stays waiting; reopen `fix` -> `fix` ready again; complete `fix` -> `review` becomes ready!
    let (st, c_fix) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{fix_id}/claim"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, failed) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{fix_id}/fail"),
            Some(&alice_tok),
            &[],
            Some(json!({
                "epoch": c_fix["epoch"],
                "token": c_fix["token"],
                "reason": "flaky test"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(failed["task"]["task"]["state"], "failed");

    // `review` is still waiting
    let (st, rev_view) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/tasks/{review_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(rev_view["task"]["task"]["state"], "waiting");

    // Reopen `fix`, claim, complete -> `review` transitions waiting -> ready (`ship` still waits on `review`)
    let (st, reopened) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{fix_id}/reopen"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(reopened["task"]["task"]["state"], "ready");

    let (st, c_fix2) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{fix_id}/claim"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, comp) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{fix_id}/complete"),
            Some(&alice_tok),
            &[],
            Some(json!({
                "epoch": c_fix2["epoch"],
                "token": c_fix2["token"],
                "result": "all green"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(comp["readied"], json!([review_id]));

    // Cycle check via PUT /tasks/{review_id}/needs -> depending on `ship_id` (which depends on `review_id`) fails 400 invalid_needs!
    let (st, err) = h
        .req(
            Method::PUT,
            &format!("/v1/messageboard/boards/General/tasks/{review_id}/needs"),
            Some(&alice_tok),
            &[],
            Some(json!({ "needs": [ship_id] })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "invalid_needs");

    // Removing needs on `ship_id` transitions it `waiting -> ready`
    let (st, upd) = h
        .req(
            Method::PUT,
            &format!("/v1/messageboard/boards/General/tasks/{ship_id}/needs"),
            Some(&alice_tok),
            &[],
            Some(json!({ "needs": [] })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(upd["task"]["task"]["state"], "ready");
}

#[tokio::test]
async fn queue_claim_priority_limits_long_poll_and_root_mutation_coupling() {
    let h = TestHarness::new().await;
    let (alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;

    // 1. Priority: assigned (`for: fx`) claimed BEFORE older unassigned task!
    let (st, t_unassigned) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[("x-board-as", "lead")],
            Some(json!({
                "channel": "queue/a",
                "text": "older unassigned",
                "labels": { "role": "worker" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let t_unassigned_id = t_unassigned["task"]["id"].as_str().unwrap().to_string();

    let (st, t_assigned) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[("x-board-as", "lead")],
            Some(json!({
                "channel": "queue/b",
                "text": "newer assigned to fx",
                "labels": { "role": "worker" },
                "for": "fx"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let t_assigned_id = t_assigned["task"]["id"].as_str().unwrap().to_string();

    // Claiming as `alice/fx` on subtree `queue/` gets `t_assigned` FIRST despite `t_unassigned` being older!
    let (st, c1) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/claim-ready",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({ "channel": "queue/", "label": { "role": "worker" } })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(c1["task"]["id"], t_assigned_id);

    // Bob (different owner) gets 404 nothing_ready without `--from`, and gets `t_unassigned` WITH `--from user:<alice_id>`!
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/claim-ready",
            Some(&bob_tok),
            &[],
            Some(json!({ "channel": "queue/", "label": { "role": "worker" } })),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(err["code"], "nothing_ready");

    let (st, c2) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/claim-ready",
            Some(&bob_tok),
            &[],
            Some(json!({
                "channel": "queue/",
                "label": { "role": "worker" },
                "from": [format!("user:{alice_id}")]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(c2["task"]["id"], t_unassigned_id);

    // 2. Claim limits: >200 owners triggers `400 claim_too_wide`
    let too_many_owners: Vec<String> = (0..201).map(|i| format!("user:u_{i}")).collect();
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/claim-ready",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "queue/a", "from": too_many_owners })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "claim_too_wide");

    // 3. Long-poll `claim-ready` wakes immediately when a new task arrives, and times out with 408
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/claim-ready",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "livewait", "wait": "50ms" })),
        )
        .await;
    assert_eq!(st, StatusCode::REQUEST_TIMEOUT);
    assert_eq!(err["code"], "timeout");

    let app_clone = h.app.clone();
    let tok_clone = alice_tok.clone();
    let waiter = tokio::spawn(async move {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/messageboard/boards/General/tasks/claim-ready")
            .header("authorization", format!("Bearer {tok_clone}"))
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "channel": "livewait", "wait": "5s" }).to_string(),
            ))
            .unwrap();
        let resp = app_clone.oneshot(req).await.unwrap();
        let st = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&bytes).unwrap();
        (st, val)
    });

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let (st, live_task) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "livewait", "text": "fresh job" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let live_id = live_task["task"]["id"].as_str().unwrap().to_string();

    let (w_st, w_body) = waiter.await.unwrap();
    assert_eq!(w_st, StatusCode::OK);
    assert_eq!(w_body["task"]["id"], live_id);

    // 4. Root mutation coupling:
    // (a) Bare Alice edits a task created by `alice/fx` -> task transitions to `blocked`!
    let (st, prof_task) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({ "channel": "coupling", "text": "original brief" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let prof_task_id = prof_task["task"]["id"].as_str().unwrap().to_string();

    // `alice/fx` editing her own task does NOT block it
    let (st, edited_self) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{prof_task_id}"),
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({ "text": "clarified by owner" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(edited_self["message"]["task"]["state"], "ready");

    // Bare `alice` (different actor than `alice/fx`) editing root transitions task to `blocked`!
    let (st, edited_non_owner) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{prof_task_id}"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "changed by bare principal" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(edited_non_owner["message"]["task"]["state"], "blocked");
    assert_eq!(edited_non_owner["message"]["labels"]["board.task"], "blocked");

    // (b) Deleting the root message cancels the unfinished task (`board.task=canceled`, emits `task.canceled`)
    let (st, _) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/messages/{prof_task_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, after_del) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/tasks/{prof_task_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(after_del["task"]["deleted"], true);
    assert_eq!(after_del["task"]["task"]["state"], "canceled");

    // Verify `task.canceled` event was emitted
    let (st, evs) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/events?after=0&types=task.canceled",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(evs["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e["message"]["id"] == prof_task_id));
}

#[tokio::test]
async fn existing_thread_task_assign_release_pagination_tree_limit_and_lease_expiry_wakeup() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;

    // 1. Turn an existing thread (authored by Bob) into a task owned by Alice
    let (st, msg) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&bob_tok),
            &[],
            Some(json!({ "channel": "triage", "text": "customer reported crash" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let msg_id = msg["id"].as_str().unwrap().to_string();

    let (st, converted) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[],
            Some(json!({ "re": msg_id, "for": "deployer" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(converted["task"]["id"], msg_id);
    assert_eq!(converted["task"]["task"]["state"], "ready");

    // Duplicate conversion fails 409 task_already_exists
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[],
            Some(json!({ "re": msg_id })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "task_already_exists");

    // Wrong profile cannot claim assigned task -> 409 task_assigned
    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{msg_id}/claim"),
            Some(&alice_tok),
            &[("x-board-as", "other")],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "task_assigned");

    // Assign to pool (`for: null`), claim, then release back to ready
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{msg_id}/assign"),
            Some(&alice_tok),
            &[],
            Some(json!({ "for": null })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, claimed) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{msg_id}/claim"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, released) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/tasks/{msg_id}/release"),
            Some(&bob_tok),
            &[],
            Some(json!({ "epoch": claimed["epoch"], "token": claimed["token"] })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(released["task"]["task"]["state"], "ready");

    // 2. Lease expiry wakeup on `claim-ready` long-poll:
    // Create task with `claim: true` and 80ms lease, then wait for it via `claim-ready` (wait=5s)!
    let (st, short_task) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "fastlease",
                "text": "expiring soon",
                "claim": true,
                "lease_ms": 80
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let short_id = short_task["task"]["id"].as_str().unwrap().to_string();
    assert_eq!(short_task["claim"]["epoch"], 1);

    let (st, claimed_after_expiry) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/claim-ready",
            Some(&alice_tok),
            &[("x-board-as", "worker-2")],
            Some(json!({ "channel": "fastlease", "wait": "5s" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(claimed_after_expiry["task"]["id"], short_id);
    assert_eq!(claimed_after_expiry["epoch"], 2);

    // 3. Pagination & limit > 50 rejection
    let (st, _) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/tasks?limit=51",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (st, p1) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/tasks?state=all&limit=1",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(p1["tasks"].as_array().unwrap().len(), 1);
    let cur1 = p1["cursor"].as_str().unwrap();

    let (st, p2) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/tasks?state=all&limit=1&after={cur1}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(p2["tasks"].as_array().unwrap().len(), 1);
    assert_ne!(p2["tasks"][0]["message"]["id"], p1["tasks"][0]["message"]["id"]);

    // 4. Subtree with >50 channels triggers `400 channel_tree_too_large`
    for i in 0..51 {
        let (st, _) = h
            .req(
                Method::POST,
                "/v1/messageboard/boards/General/channels",
                Some(&alice_tok),
                &[],
                Some(json!({ "path": format!("bigtree/c{i}") })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED);
    }
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/claim-ready",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "bigtree/" })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "channel_tree_too_large");
}

#[tokio::test]
async fn spec_audit_cross_table_idempotency_event_cursor_and_reserved_hint() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;

    // 1. reserved_label for `board.task` names `tasks` commands in hint
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "work", "text": "x", "labels": { "board.task": "ready" } })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "reserved_label");
    assert!(
        err["hint"].as_str().unwrap_or("").contains("tasks"),
        "expected hint naming tasks commands, got {err}"
    );

    // 2. Cross-table `duplicate_id`: post key cannot be reused for tasks.create or tasks.createMany
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "work", "text": "plain post", "id": "shared-key-1" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "work", "text": "task reuse", "id": "shared-key-1" })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "duplicate_id");

    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "work", "id": "shared-key-1", "tasks": [{ "text": "b1" }] })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "duplicate_id");

    // Conversely, a task batch key cannot be reused for a regular post
    let (st, batch) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/tasks/batch",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "work", "id": "batch-key-2", "tasks": [{ "text": "t1" }, { "text": "t2" }] })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(batch["tasks"].as_array().unwrap().len(), 2);

    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "work", "text": "post reuse", "id": "batch-key-2" })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "duplicate_id");

    // 3. `tasks?after=ev_<seq>` accepts event cursors seamlessly
    let (st, evs) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/events?after=0&types=task.created",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let first_task_ev_cursor = evs["events"][0]["cursor"].as_str().unwrap();
    let (st, page) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/tasks?state=all&after={first_task_ev_cursor}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(page["tasks"].as_array().unwrap().len(), 1);
    assert_eq!(page["tasks"][0]["message"]["text"], "t2");

    // 4. `wait?timeout=1h` is accepted (matches documented `--timeout 1h` examples)
    let (st, woken) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/wait?after=0&timeout=1h",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(woken["event"]["cursor"].is_string());
}
