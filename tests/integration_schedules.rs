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
use openboards::schedules::{self, format_utc_ms};
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
async fn one_shot_schedule_fires_preserving_profile_labels_schema_and_thread_embedding() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;

    // Seed Bob as a visible actor in `ops/alerts` so scheduled mention resolves
    let (st, root) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&bob_tok),
            &[],
            Some(json!({ "channel": "ops/alerts", "text": "deploy started" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let root_id = root["id"].as_str().unwrap().to_string();

    // Schedule a one-shot deferred channel post (`in: "5m"`) with profile, avatar, label, schema block, and mention
    let (st, sched_post) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[
                ("x-board-as", "DeployBot"),
                ("x-board-avatar", "https://example.com/bot.png"),
            ],
            Some(json!({
                "channel": "ops/standup",
                "text": "Hi @bob!\n\n```board-block git-commit@1\n{\"sha\":\"a1b2c3d\",\"message\":\"ship\"}\n```",
                "labels": { "kind": "standup" },
                "noSync": true,
                "in": "5m"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let post_sched_id = sched_post["schedule"]["id"].as_str().unwrap().to_string();
    assert!(post_sched_id.starts_with("sch_"));
    assert_eq!(sched_post["schedule"]["status"], "pending");
    assert_eq!(sched_post["schedule"]["by"]["handle"], "alice/deploybot");

    // Schedule a one-shot deferred reply (`at`) in Bob's thread
    let fire_at = Utc::now() + Duration::minutes(2);
    let (st, sched_reply) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({
                "re": root_id,
                "text": "friendly ping @bob",
                "at": format_utc_ms(fire_at)
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let reply_sched_id = sched_reply["schedule"]["id"].as_str().unwrap().to_string();
    assert_eq!(sched_reply["schedule"]["re"], root_id);
    assert_eq!(sched_reply["schedule"]["channel"], "ops/alerts");

    // GET /threads/{id} embeds pending schedules for that thread
    let (st, thread) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/threads/{root_id}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let embedded = thread["schedules"].as_array().unwrap();
    assert_eq!(embedded.len(), 1);
    assert_eq!(embedded[0]["id"], reply_sched_id);

    // GET /schedules with subtree `ops/` returns both pending schedules oldest-first (2m before 5m)
    let (st, listed) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/schedules?channel=ops/",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let ids: Vec<&str> = listed["schedules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![reply_sched_id.as_str(), post_sched_id.as_str()]);

    // Fire at +3m -> ONLY the 2m reply fires
    let sum1 = schedules::fire_due_schedules_at(
        &h.state.db,
        &h.state.events,
        Utc::now() + Duration::minutes(3),
    )
    .await
    .unwrap();
    assert_eq!(sum1.fired, 1);
    assert_eq!(sum1.failed, 0);

    // Thread now has the reply, and pending embedded schedules is empty!
    let (st, thread) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/threads/{root_id}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(thread["replies"].as_array().unwrap().len(), 1);
    assert_eq!(thread["replies"][0]["by"]["handle"], "alice/fx");
    assert!(thread["replies"][0]["text"].as_str().unwrap().contains("@[user:"));
    assert!(thread["schedules"].as_array().unwrap().is_empty());

    // Fire at +6m -> the 5m channel post fires with profile, avatar, label, block, noSync, and event
    let sum2 = schedules::fire_due_schedules_at(
        &h.state.db,
        &h.state.events,
        Utc::now() + Duration::minutes(6),
    )
    .await
    .unwrap();
    assert_eq!(sum2.fired, 1);
    let posted_id = &sum2.posted_message_ids[0];

    let (st, msg) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/messages/{posted_id}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(msg["channel"], "ops/standup");
    assert_eq!(msg["by"]["handle"], "alice/deploybot");
    assert_eq!(msg["labels"]["kind"], "standup");
    assert_eq!(msg["noSync"], true);
    assert_eq!(msg["blocks"][0]["schema"], "git-commit");
    assert_eq!(msg["blocks"][0]["data"]["sha"], "a1b2c3d");

    // Verify profile avatar was preserved in /actors
    let (st, actors_resp) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/actors",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let deploy_actor = actors_resp["actors"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["handle"] == "alice/deploybot")
        .expect("deploybot actor registered");
    assert_eq!(deploy_actor["avatar"], "https://example.com/bot.png");

    // Default GET /schedules hides completed; `all=true` shows them with status=completed
    let (st, pending) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(pending["schedules"].as_array().unwrap().is_empty());

    let (st, all) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/schedules?all=true&by=alice/deploybot",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(all["schedules"].as_array().unwrap().len(), 1);
    assert_eq!(all["schedules"][0]["status"], "completed");
    assert_eq!(all["schedules"][0]["runsCompleted"], 1);
    assert_eq!(all["schedules"][0]["lastMessageId"], posted_id.as_str());
}

#[tokio::test]
async fn repeating_schedules_min_interval_and_times_until_for_bounds() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;

    // Reject `every < 60s` with 400 bad_request
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "ops", "text": "too fast", "every": "30s" })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "bad_request");

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "ops", "text": "still too fast", "every": "59s" })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Reject reserved board.* labels on schedule creation
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "ops",
                "text": "bad",
                "in": "1m",
                "labels": { "board.task": "ready" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "reserved_label");

    // 1. Repeating schedule bounded by `times: 3`, `every: "1m"`
    let t0 = Utc::now();
    let (st, created) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "heartbeat",
                "text": "tick",
                "at": format_utc_ms(t0),
                "every": "1m",
                "times": 3
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let s_times_id = created["schedule"]["id"].as_str().unwrap().to_string();

    // Tick 1 at t0
    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, t0)
            .await
            .unwrap()
            .fired,
        1
    );
    // Tick at t0 + 30s -> nothing due yet
    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, t0 + Duration::seconds(30))
            .await
            .unwrap()
            .fired,
        0
    );
    // Tick 2 at t0 + 1m
    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, t0 + Duration::minutes(1))
            .await
            .unwrap()
            .fired,
        1
    );
    // Tick 3 at t0 + 2m -> reaches times=3 and completes
    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, t0 + Duration::minutes(2))
            .await
            .unwrap()
            .fired,
        1
    );
    // Tick at t0 + 5m -> already completed, does NOT fire again
    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, t0 + Duration::minutes(5))
            .await
            .unwrap()
            .fired,
        0
    );

    let (st, s_view) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/schedules/{s_times_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s_view["schedule"]["status"], "completed");
    assert_eq!(s_view["schedule"]["runsCompleted"], 3);

    // 2. Repeating schedule bounded by `until` (t0 + 90s with every=1m -> fires at t0 and t0+60s, stops after)
    let u0 = Utc::now() + Duration::hours(1);
    let (st, created_u) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "heartbeat",
                "text": "bounded",
                "at": format_utc_ms(u0),
                "every": "1m",
                "until": format_utc_ms(u0 + Duration::seconds(90))
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let s_until_id = created_u["schedule"]["id"].as_str().unwrap().to_string();

    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, u0)
            .await
            .unwrap()
            .fired,
        1
    );
    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, u0 + Duration::minutes(1))
            .await
            .unwrap()
            .fired,
        1
    );
    // Next candidate (u0 + 2m) > until (u0 + 90s), so status is now completed
    let (st, s_u) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/schedules/{s_until_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s_u["schedule"]["status"], "completed");
    assert_eq!(s_u["schedule"]["runsCompleted"], 2);

    // 3. Repeating schedule bounded by `for: "90s"`, `every: "1m"` -> fires twice (f0, f0+60s) then completes
    let f0 = Utc::now() + Duration::hours(2);
    let (st, created_f) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "heartbeat",
                "text": "for-bounded",
                "at": format_utc_ms(f0),
                "every": "1m",
                "for": "90s"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let s_for_id = created_f["schedule"]["id"].as_str().unwrap().to_string();

    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, f0)
            .await
            .unwrap()
            .fired,
        1
    );
    assert_eq!(
        schedules::fire_due_schedules_at(&h.state.db, &h.state.events, f0 + Duration::minutes(1))
            .await
            .unwrap()
            .fired,
        1
    );
    let (st, s_f) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/schedules/{s_for_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s_f["schedule"]["status"], "completed");
    assert_eq!(s_f["schedule"]["runsCompleted"], 2);
}

#[tokio::test]
async fn grant_revocation_stops_schedule_and_cancel_enforces_owner_and_profile_rules() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;

    // 1. Grant revocation stops a pending schedule from firing
    let t0 = Utc::now();
    let (st, s1) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "restricted/updates",
                "text": "should never land",
                "at": format_utc_ms(t0 + Duration::minutes(1))
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let s1_id = s1["schedule"]["id"].as_str().unwrap().to_string();

    // Owner closes `restricted/` to everyone before the schedule fires
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "everyone" }],
                "can": { "restricted/": "none" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let sum = schedules::fire_due_schedules_at(
        &h.state.db,
        &h.state.events,
        t0 + Duration::minutes(2),
    )
    .await
    .unwrap();
    assert_eq!(sum.fired, 0);
    assert_eq!(sum.failed, 1);

    let (st, s1_after) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/schedules/{s1_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(s1_after["schedule"]["status"], "failed");

    // 2. Cancellation permissions:
    // Alice creates two schedules under profile `fx` in `dev`
    let (st, sa) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({ "channel": "dev", "text": "ping A", "in": "1h" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let sa_id = sa["schedule"]["id"].as_str().unwrap().to_string();

    let (st, sb) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schedules",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({ "channel": "dev", "text": "ping B", "in": "1h" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let sb_id = sb["schedule"]["id"].as_str().unwrap().to_string();

    // Another profile (`alice/codex`) CANNOT cancel `alice/fx`'s schedule
    let (st, _) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/schedules/{sa_id}"),
            Some(&alice_tok),
            &[("x-board-as", "codex")],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Non-owner member Bob (even though Bob has `manage` in `dev` via g1!) CANNOT cancel Alice's schedule!
    let (st, _) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/schedules/{sa_id}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Bare principal `alice` (no profile) CAN cancel her profile `alice/fx`'s schedule
    let (st, res) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/schedules/{sa_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(res["canceled"], true);
    assert_eq!(res["schedule"]["status"], "canceled");

    // Repeat cancel returns `canceled: false`
    let (st, res) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/schedules/{sa_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(res["canceled"], false);

    // Team Owner with `manage` in `dev` CAN cancel Alice's other schedule `sb_id`
    let (st, res) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/schedules/{sb_id}"),
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(res["canceled"], true);
    assert_eq!(res["schedule"]["status"], "canceled");
}
