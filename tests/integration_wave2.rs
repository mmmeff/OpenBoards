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
    app: axum::Router,
    owner_token: String,
}

impl TestHarness {
    fn new() -> Self {
        let owner_token = "owner-bootstrap-secret".to_string();
        let db = Database::open_in_memory().unwrap();
        actors::bootstrap_owner(&db, Some(&owner_token)).unwrap();
        boards::ensure_default_board(&db).unwrap();
        let state = AppState::new(
            db,
            ServerConfig {
                listen: "127.0.0.1:8080".parse().unwrap(),
                data_dir: std::path::PathBuf::from("."),
                team: "acme".to_string(),
                admin_token: Some(owner_token.clone()),
            },
        );
        let app = build_router(state);
        Self { app, owner_token }
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
