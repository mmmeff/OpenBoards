use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use openboards::actors;
use openboards::boards;
use openboards::config::ServerConfig;
use openboards::db::Database;
use openboards::{build_router, AppState};

struct TestHarness {
    _tmp: tempfile::TempDir,
    app: axum::Router,
    owner_token: String,
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
            },
        );
        let app = build_router(state);
        Self {
            _tmp: tmp,
            app,
            owner_token,
        }
    }

    async fn req_raw(
        &self,
        method: Method,
        uri: &str,
        token: Option<&str>,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            builder = builder.header("authorization", format!("Bearer {t}"));
        }
        for &(k, v) in headers {
            builder = builder.header(k, v);
        }
        let req = builder.body(Body::from(body)).unwrap();
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let resp_headers = resp.headers().clone();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes().to_vec();
        (status, resp_headers, bytes)
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
async fn threads_replies_idempotency_guards_tombstones_and_blind_intake() {
    let h = TestHarness::new();
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;

    // Blind intake (post only) CANNOT create a new channel
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&bob_tok),
            &[("x-board-can", "post")],
            Some(json!({ "channel": "intake", "text": "hello" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // First post implicitly creates channel `intake` (requires read+post)
    let (st, root) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[("x-board-as", "Fx")],
            Some(json!({
                "channel": "intake",
                "text": "root post",
                "id": "idem-1"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let root_id = root["id"].as_str().unwrap().to_string();
    assert!(root_id.starts_with("msg_"));
    assert_eq!(root["re"], Value::Null);
    assert_eq!(root["by"]["handle"], "alice/fx");
    assert_eq!(root["revision"], 1);

    // Idempotent retry returns original message (200 OK)
    let (st, replay) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({
                "channel": "intake",
                "text": "different text ignored on retry",
                "id": "idem-1"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(replay["id"], root_id);
    assert_eq!(replay["text"], "root post");

    // Reusing idempotency key in another channel -> 409 duplicate_id
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({
                "channel": "other",
                "text": "wrong channel",
                "id": "idem-1"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "duplicate_id");

    // GET /posted/{key} returns original post; missing key -> 404 post_not_found
    let (st, posted) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/posted/idem-1",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(posted["id"], root_id);

    let (st, err) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/posted/does-not-exist",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(err["code"], "post_not_found");

    // Blind intake (post only) CAN post to an existing channel and edit its own post, but CANNOT reply
    let (st, blind) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&bob_tok),
            &[("x-board-can", "post")],
            Some(json!({ "channel": "intake", "text": "blind report" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let blind_id = blind["id"].as_str().unwrap().to_string();

    let (st, edited) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{blind_id}"),
            Some(&bob_tok),
            &[("x-board-can", "post")],
            Some(json!({ "text": "blind report updated", "revision": 1 })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(edited["message"]["revision"], 2);

    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/replies"),
            Some(&bob_tok),
            &[("x-board-can", "post")],
            Some(json!({ "text": "cannot reply without read" })),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // 1-level threading: reply to root (replyTo omitted), then reply to that reply (re=root, replyTo=r1)
    let (st, r1) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/replies"),
            Some(&bob_tok),
            &[],
            Some(json!({ "text": "first reply", "after": root_id })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let r1_id = r1["id"].as_str().unwrap().to_string();
    assert_eq!(r1["re"], root_id);
    assert!(r1.get("replyTo").is_none());

    // Stale `after` -> 409 newer_message
    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/replies"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "stale", "after": root_id })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "newer_message");

    let (st, r2) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{r1_id}/replies"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "replying to r1", "after": r1_id })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(r2["re"], root_id);
    assert_eq!(r2["replyTo"], r1_id);

    // Profile edit isolation: alice/codex cannot edit alice/fx's message; bare alice CAN!
    let (st, _) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&alice_tok),
            &[("x-board-as", "codex")],
            Some(json!({ "text": "hijack" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Owner cannot edit another user's message
    let (st, _) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&h.owner_token),
            &[],
            Some(json!({ "text": "owner edit" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Bare alice edits alice/fx's message with revision guard
    let (st, err) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "wrong rev", "revision": 99 })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "revision_conflict");

    let (st, edited) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "updated root", "revision": 1 })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(edited["message"]["revision"], 2);

    // Reactions: normalize `+1` and `👍` to `thumbs_up`, deduplicated per (msg, actor, emoji)
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/reactions"),
            Some(&bob_tok),
            &[],
            Some(json!({ "emoji": "+1" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/reactions"),
            Some(&bob_tok),
            &[],
            Some(json!({ "emoji": "👍" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, msg_view) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(msg_view["reactions"]["thumbs_up"].as_array().unwrap().len(), 1);
    // Reactions leave revision unchanged
    assert_eq!(msg_view["revision"], 2);

    // Owner with manage in channel can delete root (tombstone preserves replies, strips text & reactions)
    let (st, _) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // Repeat delete is idempotent no-op (revision stays 3)
    let (st, _) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, thread) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/threads/{r1_id}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(thread["message"]["deleted"], true);
    assert_eq!(thread["message"]["text"], "");
    assert_eq!(thread["message"]["revision"], 3);
    assert!(thread["message"]["reactions"].as_object().unwrap().is_empty());
    assert_eq!(thread["replies"].as_array().unwrap().len(), 2);

    // Editing or reacting to deleted message fails 400 message_deleted
    let (st, err) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&alice_tok),
            &[],
            Some(json!({ "text": "resurrect" })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "message_deleted");

    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/reactions"),
            Some(&bob_tok),
            &[],
            Some(json!({ "emoji": "eyes" })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "message_deleted");
}

#[tokio::test]
async fn mentions_visibility_isolation_and_by_and_thread_pagination() {
    let h = TestHarness::new();
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;

    // Alice writes in private channel `vault` under profile `secret-bot`
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[("x-board-as", "secret-bot")],
            Some(json!({ "path": "vault", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // Alice also writes in public channel `dev` under bare handle and profile `fx`
    let (st, m1) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "dev", "text": "hello 1" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let m1_id = m1["id"].as_str().unwrap().to_string();

    let (st, m2) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[("x-board-as", "fx")],
            Some(json!({ "channel": "dev", "text": "hello 2 from fx" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let m2_id = m2["id"].as_str().unwrap().to_string();

    // Bob mentions @alice/fx (visible) and @alice/secret-bot (hidden in private channel!)
    let (st, m3) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&bob_tok),
            &[],
            Some(json!({
                "channel": "dev",
                "text": "ping @alice/fx and @alice/secret-bot and `@alice`"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let m3_text = m3["text"].as_str().unwrap();
    assert!(m3_text.contains("@[user:"));
    assert!(m3_text.contains("+fx]"));
    // Hidden actor stays plain text and leaks zero candidates
    assert!(m3_text.contains("@alice/secret-bot"));
    assert!(m3_text.contains("`@alice`"));
    let mentions = m3["mentions"].as_array().unwrap();
    assert_eq!(mentions.len(), 2);
    assert!(mentions[0]["actor"].is_object());
    assert!(mentions[1]["actor"].is_null());
    assert!(mentions[1]["candidates"].as_array().unwrap().is_empty());

    // `--by alice` matches BOTH bare alice and alice/fx; `--by alice/fx` matches only fx
    let (st, page) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/messages?channel=dev&by=alice",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let ids: Vec<&str> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec![m1_id.as_str(), m2_id.as_str()]);

    let (st, page) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/messages?channel=dev&by=alice/fx",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(page["messages"].as_array().unwrap().len(), 1);
    assert_eq!(page["messages"][0]["id"], m2_id);

    // Thread pagination with `limit=2` and `before=older`
    let (st, page1) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/threads?channel=dev&limit=2",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(page1["threads"].as_array().unwrap().len(), 2);
    let older = page1["older"].as_str().expect("should have older cursor");

    let (st, page2) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/threads?channel=dev&limit=2&before={older}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(page2["threads"].as_array().unwrap().len(), 1);
    assert_eq!(page2["threads"][0]["message"]["id"], m1_id);
    assert!(page2["older"].is_null());
}

#[tokio::test]
async fn labels_cas_filters_inheritance_and_structured_schemas() {
    let h = TestHarness::new();
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_eve_id, eve_tok) = h.create_user_with_token("eve", "eve@acme.com", "viewer").await;

    // Built-in schemas git-commit@1 and vercel-deployment@1 are pre-seeded
    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/schemas",
            Some(&eve_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let schema_ids: Vec<&str> = body["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["id"].as_str().unwrap())
        .collect();
    assert!(schema_ids.contains(&"git-commit@1"));
    assert!(schema_ids.contains(&"vercel-deployment@1"));

    // Viewer lacks manage at * -> cannot register schema
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schemas",
            Some(&eve_tok),
            &[],
            Some(json!({
                "name": "sandbox",
                "version": 1,
                "schema": { "type": "object", "required": ["id"], "properties": { "id": { "type": "string" } } }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Member with manage at * registers sandbox@1 -> 201 created: true
    let sandbox_def = json!({
        "name": "sandbox",
        "version": 1,
        "description": "What a sandbox did.",
        "schema": {
            "type": "object",
            "required": ["id", "action"],
            "properties": {
                "id": { "type": "string" },
                "action": { "enum": ["started", "stopped"] }
            }
        }
    });
    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schemas",
            Some(&alice_tok),
            &[],
            Some(sandbox_def.clone()),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(body["created"], true);
    assert_eq!(body["schema"]["id"], "sandbox@1");

    // Identical re-registration -> 200 created: false
    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schemas",
            Some(&alice_tok),
            &[],
            Some(sandbox_def),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["created"], false);

    // Differing definition -> 409 schema_immutable
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/schemas",
            Some(&alice_tok),
            &[],
            Some(json!({
                "name": "sandbox",
                "version": 1,
                "schema": { "type": "object" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "schema_immutable");

    // Reserved board.* label rejected on post with 400 reserved_label
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "www/prod",
                "text": "bad label",
                "labels": { "board.task": "ready" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "reserved_label");

    // Invalid structured block -> 400 invalid_block; unknown schema -> 400 schema_not_found
    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "www/prod",
                "text": "```board-block sandbox@1\n{\"id\":\"sbx-1\",\"action\":\"exploded\"}\n```"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "invalid_block");

    let (st, err) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "www/prod",
                "schema": "missing@1",
                "text": "{\"a\":1}"
            })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(err["code"], "schema_not_found");

    // Post root with embedded fenced block + initial label `status=open`
    let (st, root) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "www/prod",
                "text": "Launch report:\n\n```board-block sandbox@1\n{\"id\":\"sbx-123\",\"action\":\"started\"}\n```",
                "labels": { "status": "open" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let root_id = root["id"].as_str().unwrap().to_string();
    assert_eq!(root["labels"]["status"], "open");
    assert_eq!(root["blocks"].as_array().unwrap().len(), 1);
    assert_eq!(root["blocks"][0]["schema"], "sandbox");
    assert_eq!(root["blocks"][0]["version"], 1);
    assert_eq!(root["blocks"][0]["data"]["id"], "sbx-123");

    // Post reply with explicit `schema` for built-in git-commit@1 and label `kind=bug`
    let (st, reply) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/replies"),
            Some(&alice_tok),
            &[],
            Some(json!({
                "schema": "git-commit@1",
                "text": "{\"sha\":\"a1b2c3d\",\"message\":\"fix timeout\"}",
                "labels": { "kind": "bug" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let reply_id = reply["id"].as_str().unwrap().to_string();
    assert_eq!(reply["blocks"][0]["schema"], "git-commit");

    // Atomic CAS label update: `if: { owner: null }` succeeds, then fails with 409 label_conflict
    // WITHOUT leaking the actual label value ("secret-owner")!
    let (st, body) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/labels"),
            Some(&alice_tok),
            &[],
            Some(json!({
                "set": { "owner": "secret-owner" },
                "if": { "owner": null, "status": "open" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["labels"]["owner"], "secret-owner");

    let (st, err) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/labels"),
            Some(&alice_tok),
            &[],
            Some(json!({
                "set": { "owner": "other" },
                "if": { "owner": null }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err["code"], "label_conflict");
    let err_msg = err["message"].as_str().unwrap();
    assert!(err_msg.contains("owner"));
    assert!(!err_msg.contains("secret-owner"), "must never leak actual value");

    // Viewer cannot set labels (403 forbidden)
    let (st, _) = h
        .req(
            Method::POST,
            &format!("/v1/messageboard/boards/General/messages/{root_id}/labels"),
            Some(&eve_tok),
            &[],
            Some(json!({ "set": { "status": "closed" } })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Filter engine: subtree `www/`, conjunction + alternation + presence + root inheritance + text
    // Reply has `kind=bug`, root has `status=open` -> reply matches BOTH `status=open|draft` AND `kind=bug`!
    let (st, page) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/messages?channel=www/&label=status=open|draft&label=kind=bug&label=owner&text=timeout",
            Some(&eve_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let msgs = page["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["id"], reply_id);

    // Deleting root strips its labels so label filter no longer finds the root
    let (st, _) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/messages/{root_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, page) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/messages?label=status=open",
            Some(&eve_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(page["messages"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn attachments_inline_streaming_limits_and_tombstone_visibility() {
    let h = TestHarness::new();
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;

    // Restrict Bob so he can read ONLY `dev` (not `*`), so we can verify tombstone visibility!
    // Create a private channel `vault` for Alice so Bob does not have `*`-read.
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[],
            Some(json!({ "path": "vault", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    // 1. Inline upload (<= 4 MB)
    let payload = b"hello attachment bytes".to_vec();
    let (st, _, raw) = h
        .req_raw(
            Method::POST,
            "/v1/messageboard/boards/General/attachments?name=trace.txt",
            Some(&alice_tok),
            &[("content-type", "text/plain")],
            payload.clone(),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let att1: Value = serde_json::from_slice(&raw).unwrap();
    let att1_id = att1["id"].as_str().unwrap().to_string();
    assert!(att1_id.starts_with("att_"));
    assert_eq!(att1["name"], "trace.txt");
    assert_eq!(att1["type"], "text/plain");
    assert_eq!(att1["size"], payload.len());

    // Inline upload > 4 MB rejected with 413
    let too_big_inline = vec![0xABu8; 4 * 1024 * 1024 + 1];
    let (st, _, _) = h
        .req_raw(
            Method::POST,
            "/v1/messageboard/boards/General/attachments?name=huge.bin",
            Some(&alice_tok),
            &[("content-type", "application/octet-stream")],
            too_big_inline,
        )
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE);

    // 2. Streaming two-step upload (supports > 4 MB up to 100 MB)
    let (st, init) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/attachments/uploads",
            Some(&alice_tok),
            &[],
            Some(json!({ "name": "artifact.bin", "type": "application/octet-stream" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let att2_id = init["id"].as_str().unwrap().to_string();
    let upload_url = init["upload_url"].as_str().unwrap().to_string();
    let complete_url = init["complete_url"].as_str().unwrap().to_string();

    let big_stream_payload = vec![0x42u8; 4 * 1024 * 1024 + 128];
    let (st, _, raw) = h
        .req_raw(
            Method::PUT,
            &upload_url,
            Some(&alice_tok),
            &[],
            big_stream_payload.clone(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let put_res: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(put_res["size"], big_stream_payload.len());

    let (st, comp) = h
        .req(Method::POST, &complete_url, Some(&alice_tok), &[], None)
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(comp["attachment"]["id"], att2_id);
    assert_eq!(comp["attachment"]["size"], big_stream_payload.len());

    // 3. Before attaching att1 to a readable message, Bob (who lacks `*`-read because of `vault`)
    // cannot download OR attach Alice's unattached file!
    let (st, _, _) = h
        .req_raw(
            Method::GET,
            &format!("/v1/messageboard/boards/General/attachments/{att1_id}"),
            Some(&bob_tok),
            &[],
            Vec::new(),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&bob_tok),
            &[],
            Some(json!({ "channel": "dev", "text": "steal", "attachments": [att1_id] })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // 4. Empty text is allowed when attachments are non-empty; >10 attachments rejected
    let too_many: Vec<String> = (0..11).map(|i| format!("att_{i}")).collect();
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({ "channel": "dev", "text": "", "attachments": too_many })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (st, msg) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/messages",
            Some(&alice_tok),
            &[],
            Some(json!({
                "channel": "dev",
                "text": "",
                "attachments": [att1_id, att2_id]
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let msg_id = msg["id"].as_str().unwrap().to_string();
    assert_eq!(msg["attachments"].as_array().unwrap().len(), 2);

    // Now Bob CAN read `dev`, so Bob CAN download att1 and att2!
    let (st, hdrs, bytes) = h
        .req_raw(
            Method::GET,
            &format!("/v1/messageboard/boards/General/attachments/{att1_id}"),
            Some(&bob_tok),
            &[],
            Vec::new(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs.get("content-type").unwrap(), "text/plain");
    assert_eq!(bytes, payload);

    // 5. Delete (tombstone) the message -> attachments vanish from message view;
    // Bob (non-uploader, non-`-read-all`) can NO LONGER download att1,
    // while uploader Alice AND `*`-read user (after granting owner vault access) CAN still download!
    let (st, _) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/messages/{msg_id}"),
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, tomb) = h
        .req(
            Method::GET,
            &format!("/v1/messageboard/boards/General/messages/{msg_id}"),
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(tomb["deleted"], true);
    assert!(tomb["attachments"].as_array().unwrap().is_empty());

    // Bob denied on tombstoned attachment
    let (st, _, _) = h
        .req_raw(
            Method::GET,
            &format!("/v1/messageboard/boards/General/attachments/{att1_id}"),
            Some(&bob_tok),
            &[],
            Vec::new(),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Uploader Alice still allowed!
    let (st, _, bytes) = h
        .req_raw(
            Method::GET,
            &format!("/v1/messageboard/boards/General/attachments/{att1_id}"),
            Some(&alice_tok),
            &[],
            Vec::new(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(bytes, payload);

    // Owner granted read on `vault/` now has `*`-read across all channels -> can download tombstoned att1!
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "user", "username": "admin" }],
                "can": { "vault/": ["read"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, _, bytes) = h
        .req_raw(
            Method::GET,
            &format!("/v1/messageboard/boards/General/attachments/{att1_id}"),
            Some(&h.owner_token),
            &[],
            Vec::new(),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(bytes, payload);
}
