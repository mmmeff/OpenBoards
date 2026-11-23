use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

use openboards::actors;
use openboards::boards;
use openboards::config::{DbTarget, ServerConfig};
use openboards::db::Database;
use openboards::{build_router, AppState};

struct TestHarness {
    state: AppState,
    app: axum::Router,
    owner_token: String,
}

impl TestHarness {
    async fn new() -> Self {
        let owner_token = "owner-bootstrap-secret".to_string();
        let db = Database::open_in_memory().await.unwrap();
        actors::bootstrap_owner(&db, Some(&owner_token)).await.unwrap();
        boards::ensure_default_board(&db).await.unwrap();
        let state = AppState::new(
            db,
            ServerConfig {
                listen: "127.0.0.1:8080".parse().unwrap(),
                data_dir: std::path::PathBuf::from("."),
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
        assert_eq!(st, StatusCode::CREATED, "create user failed: {body}");
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
        assert_eq!(st, StatusCode::CREATED, "create project failed: {body}");
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

    async fn create_app_with_token(&self, slug: &str, scopes: Value) -> (String, String) {
        let (st, body) = self
            .req(
                Method::POST,
                "/v1/admin/apps",
                Some(&self.owner_token),
                &[],
                Some(json!({ "slug": slug, "scopes": scopes })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "create app failed: {body}");
        let aid = body["app"]["id"].as_str().unwrap().to_string();

        let (st, body) = self
            .req(
                Method::POST,
                "/v1/admin/tokens",
                Some(&self.owner_token),
                &[],
                Some(json!({ "principal_kind": "app", "principal_id": aid })),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED);
        let tok = body["token"]["token"].as_str().unwrap().to_string();
        (aid, tok)
    }
}

#[tokio::test]
async fn discovery_and_board_lifecycle_and_isolation() {
    let h = TestHarness::new().await;

    // Unauthenticated /discovery
    let (st, body) = h
        .req(Method::GET, "/discovery", None, &[("host", "boards.acme.internal")], None)
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        body["url"],
        "http://boards.acme.internal/v1/messageboard/boards/General?teamId=acme"
    );
    assert_eq!(body["team_id"], "acme");
    assert_eq!(body["name"], "General");

    // Default board General matches case-insensitively and has g1/g2/g3
    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/general",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["board"]["name"], "General");

    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/GENERAL/grants",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let grants = body["grants"].as_array().unwrap();
    assert_eq!(grants.len(), 3);
    assert_eq!(grants[0]["id"], "g1");
    assert_eq!(grants[1]["id"], "g2");
    assert_eq!(grants[2]["id"], "g3");

    // Missing named board: omitted ensure -> board_not_found, ensure=false -> no_board
    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/security",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "board_not_found");

    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/security?ensure=false",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "no_board");

    // ?ensure=true creates the board with seeded g1/g2/g3
    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/security?ensure=true",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["board"]["name"], "security");

    // Case-insensitive duplicate board creation fails 409 board_name_taken even if locked down
    let (_uid, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;
    // Lock security board completely to Bob by revoking g1 on security
    let (st, _) = h
        .req(
            Method::DELETE,
            "/v1/messageboard/boards/security/grants/g1",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    // Bob cannot see 'security' in board listing and gets 403 board_not_granted on access
    let (st, body) = h
        .req(Method::GET, "/v1/messageboard/boards", Some(&bob_tok), &[], None)
        .await;
    assert_eq!(st, StatusCode::OK);
    let board_names: Vec<&str> = body["boards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["name"].as_str().unwrap())
        .collect();
    assert_eq!(board_names, vec!["General"]);

    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/security",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "board_not_granted");

    // Yet creating 'SECURITY' still fails 409 board_name_taken (team-wide uniqueness including private boards)
    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards",
            Some(&bob_tok),
            &[],
            Some(json!({ "name": "SECURITY" })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(body["code"], "board_name_taken");

    // Board isolation: revoking g1 on security did NOT affect Bob's access on General
    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&bob_tok),
            &[],
            Some(json!({ "path": "dev" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(body["channel"]["path"], "dev");
}

#[tokio::test]
async fn grants_precedence_ties_roles_narrowing_guardrails_and_explain() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "developer").await;
    let (_eve_id, eve_tok) = h.create_user_with_token("eve", "eve@acme.com", "viewer").await;
    let (_none_id, none_tok) = h.create_user_with_token("noneuser", "none@acme.com", "none").await;
    let (web_id, web_prod_tok) = h.create_project_with_token("web", "production").await;
    let (_, body) = h
        .req(
            Method::POST,
            "/v1/admin/tokens",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principal_kind": "project",
                "principal_id": web_id,
                "environment": "preview"
            })),
        )
        .await;
    let web_preview_tok = body["token"]["token"].as_str().unwrap().to_string();
    let (_app_id, app_tok) = h
        .create_app_with_token("triage", json!(["read", "label"]))
        .await;

    // 1. Member cannot edit/revoke g1 or close channel to everyone/all-users
    let (st, body) = h
        .req(
            Method::PATCH,
            "/v1/messageboard/boards/General/grants/g1",
            Some(&alice_tok),
            &[],
            Some(json!({ "can": { "*": "read" } })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");

    let (st, _) = h
        .req(
            Method::DELETE,
            "/v1/messageboard/boards/General/grants/g1",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&alice_tok),
            &[],
            Some(json!({
                "principals": [{ "kind": "everyone" }],
                "can": { "security/": "none" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&alice_tok),
            &[],
            Some(json!({
                "principals": [{ "kind": "all-users" }],
                "can": { "security/": "none" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // 2. Narrowed or sandboxed owner is ALWAYS denied grant mutations
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[("x-board-can", "all")],
            Some(json!({
                "principals": [{ "kind": "everyone" }],
                "can": { "security/": "none" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[("x-board-sandboxed", "true")],
            Some(json!({
                "principals": [{ "kind": "everyone" }],
                "can": { "security/": "none" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // 3. Owner opens security/ to Alice (via two tied grants) and closes security/ to everyone
    let (st, g4) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "user", "email": "alice@acme.com" }],
                "can": { "security/": ["read"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(g4["grant"]["id"], "g4");

    let (st, g5) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "user", "email": "alice@acme.com" }],
                "can": { "security/": ["post", "manage"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(g5["grant"]["id"], "g5");

    let (st, g6) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "everyone" }],
                "can": { "security/": "none" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(g6["grant"]["id"], "g6");

    // 4. Explain: Alice gets union of g4 + g5 -> [read, post, manage] on security/incidents
    let (st, exp) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=security/incidents&user=alice@acme.com",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp["pattern"], "security/");
    assert_eq!(exp["tier"], "specific");
    assert_eq!(exp["grants"], json!(["g4", "g5"]));
    assert_eq!(exp["can"], json!(["read", "post", "manage"]));

    // Owner themselves is blocked from reading security/ until they grant themselves access
    let (st, exp) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=security/incidents",
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp["can"], json!([]));

    // Alice can create channel security/incidents, Bob cannot, Owner cannot (until changing grants)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[],
            Some(json!({ "path": "security/incidents" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&bob_tok),
            &[],
            Some(json!({ "path": "security/other" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Alice has manage on security/ but lacks `label` there -> cannot grant `label` to Bob!
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&alice_tok),
            &[],
            Some(json!({
                "principals": [{ "kind": "user", "email": "bob@acme.com" }],
                "can": { "security/": ["read", "label"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Alice CAN grant Bob `read` on exact channel security/incidents (which she holds!)
    let (st, g7) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&alice_tok),
            &[],
            Some(json!({
                "principals": [{ "kind": "user", "email": "bob@acme.com" }],
                "can": { "security/incidents": ["read"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(g7["grant"]["id"], "g7");

    // Bob can now see security/incidents in GET /channels (exact beats security/ everyone=none)
    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/channels",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let chs: Vec<&str> = body["channels"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["path"].as_str().unwrap())
        .collect();
    assert_eq!(chs, vec!["security/incidents"]);

    // 5. Viewer role cap: Eve only gets read+react even via g1=all; cannot list grants or explain others
    let (st, exp) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=dev",
            Some(&eve_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp["can"], json!(["read", "react"]));

    let (st, _) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants",
            Some(&eve_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, _) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=dev&user=alice@acme.com",
            Some(&eve_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // TeamRole::None has zero board access
    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/channels",
            Some(&none_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "board_not_granted");

    // 6. Project env defaults to production; preview doesn't match production-only grant
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "project", "project": "web" }],
                "can": { "security/": ["read", "post"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let (st, exp_prod) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=security/incidents",
            Some(&web_prod_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp_prod["can"], json!(["read", "post"]));

    let (st, exp_prev) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=security/incidents",
            Some(&web_preview_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp_prev["can"], json!([]));

    // 7. App capped by scopes (read, label) even though g3 grants read,post,react,label
    let (st, exp_app) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=dev",
            Some(&app_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp_app["can"], json!(["read", "label"]));

    // 8. Narrowing on self-explain reflects X-Board-Can intersection
    let (st, exp_narrow) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=dev",
            Some(&bob_tok),
            &[("x-board-can", "post")],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(exp_narrow["narrowed"], true);
    assert_eq!(exp_narrow["can"], json!(["post"]));

    // Narrowed post-only caller cannot create a channel (requires read + post)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&bob_tok),
            &[("x-board-can", "post")],
            Some(json!({ "path": "blind-intake" })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn private_channels_atomicity_conflicts_and_actor_visibility() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;
    let (_eve_id, eve_tok) = h.create_user_with_token("eve", "eve@acme.com", "viewer").await;

    // Viewer lacks manage at * -> cannot create private channel
    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&eve_tok),
            &[],
            Some(json!({ "path": "secret", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");

    // Narrowed/sandboxed caller cannot create private channel
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[("x-board-sandboxed", "true")],
            Some(json!({ "path": "secret", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Alice creates private channel `secret` under profile `fx`
    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[
                ("x-board-profile", "Fx"),
                ("x-board-avatar", "https://example.com/fx.png"),
            ],
            Some(json!({ "path": "secret", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(body["created"], true);
    assert_eq!(body["channel"]["path"], "secret");
    let created_grants = body["grants"].as_array().unwrap();
    assert_eq!(created_grants.len(), 2);
    let self_grant_id = created_grants[0]["id"].as_str().unwrap().to_string();

    // Alice sees `secret` and her profile `alice/fx` in actors; Bob sees NEITHER!
    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["channels"].as_array().unwrap().len(), 1);

    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/channels",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["channels"].as_array().unwrap().len(), 0);

    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/actors",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["actors"].as_array().unwrap().len(), 0);

    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/actors",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    let alice_actors = body["actors"].as_array().unwrap();
    assert_eq!(alice_actors.len(), 1);
    assert_eq!(alice_actors[0]["handle"], "alice/fx");
    assert_eq!(alice_actors[0]["avatar"], "https://example.com/fx.png");

    // Alice adds Bob to her private channel grant -> Bob can now see `secret` and `alice/fx`!
    let (st, _) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/grants/{self_grant_id}"),
            Some(&alice_tok),
            &[],
            Some(json!({ "add": [{ "kind": "user", "email": "bob@acme.com" }] })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/channels",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["channels"].as_array().unwrap().len(), 1);

    let (st, body) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/actors",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["actors"].as_array().unwrap().len(), 1);

    // 409 channel_exists when exact channel or a descendant already exists (atomic: no new grants)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[],
            Some(json!({ "path": "ops/alerts" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let grants_before = openboards::grants::load_grants(
        &h.state.db,
        &boards::ensure_default_board(&h.state.db).await.unwrap().id,
    )
    .await
    .unwrap()
    .len();

    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[],
            Some(json!({ "path": "ops", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(body["code"], "channel_exists");

    let grants_after = openboards::grants::load_grants(
        &h.state.db,
        &boards::ensure_default_board(&h.state.db).await.unwrap().id,
    )
    .await
    .unwrap()
    .len();
    assert_eq!(grants_before, grants_after);

    // 409 channel_granted when another grant already reaches the path or subtree (atomic: no channel or grants created)
    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "user", "email": "bob@acme.com" }],
                "can": { "finance/payroll": ["read"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);

    let grants_before = openboards::grants::load_grants(
        &h.state.db,
        &boards::ensure_default_board(&h.state.db).await.unwrap().id,
    )
    .await
    .unwrap()
    .len();

    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&h.owner_token),
            &[],
            Some(json!({ "path": "finance", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(body["code"], "channel_granted");

    let grants_after = openboards::grants::load_grants(
        &h.state.db,
        &boards::ensure_default_board(&h.state.db).await.unwrap().id,
    )
    .await
    .unwrap()
    .len();
    assert_eq!(grants_before, grants_after);

    // Verify /me returns caller's effective `can` map across board patterns
    let (st, me) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/me",
            Some(&alice_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(me["board"]["name"], "General");
    assert!(me["can"]["*"].as_array().unwrap().contains(&json!("read")));
    assert!(me["can"]["secret/"].as_array().unwrap().contains(&json!("manage")));
}

#[tokio::test]
async fn whoami_admin_crud_profiles_and_revocation() {
    let h = TestHarness::new().await;

    // Unauthenticated fails 401
    let (st, body) = h.req(Method::GET, "/whoami", None, &[], None).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");

    // Healthz is unauthenticated
    let (st, body) = h.req(Method::GET, "/healthz", None, &[], None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["ok"], true);

    // Owner whoami with profile, avatar, narrowing, sandboxed
    let (st, body) = h
        .req(
            Method::GET,
            "/whoami",
            Some(&h.owner_token),
            &[
                ("x-board-as", "Fx"),
                ("x-board-avatar", "https://example.com/fx.png"),
                ("x-board-can", "read,post"),
                ("x-board-sandboxed", "true"),
            ],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["handle"], "admin/fx");
    assert_eq!(body["narrowed"], true);
    assert_eq!(body["sandboxed"], true);
    assert_eq!(body["can"]["*"], json!(["read", "post"]));

    // Create user, project, app, and revoke user -> token immediately invalid
    let (ada_id, ada_tok) = h.create_user_with_token("ada", "ada@acme.com", "member").await;
    let (st, _) = h
        .req(Method::GET, "/v1/admin/users", Some(&ada_tok), &[], None)
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, _) = h
        .req(
            Method::DELETE,
            &format!("/v1/admin/users/{ada_id}"),
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _) = h
        .req(Method::GET, "/whoami", Some(&ada_tok), &[], None)
        .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn grant_updates_revocation_project_sync_narrowing_and_idempotent_channels() {
    let h = TestHarness::new().await;
    let (_alice_id, alice_tok) = h.create_user_with_token("alice", "alice@acme.com", "member").await;
    let (_bob_id, bob_tok) = h.create_user_with_token("bob", "bob@acme.com", "member").await;
    let (_sup_id, sup_prod_tok) = h.create_project_with_token("support", "production").await;
    let (_other_id, other_prod_tok) = h.create_project_with_token("other", "production").await;

    // Regular channel creation is idempotent (201 then 200 with created: false)
    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&sup_prod_tok),
            &[],
            Some(json!({ "path": "support" })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(body["created"], true);

    let (st, body) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&sup_prod_tok),
            &[],
            Some(json!({ "path": "support" })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["created"], false);

    // Project lacks `manage` via g2 -> cannot list grants, create private channel, or explain others
    let (st, _) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants",
            Some(&sup_prod_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    let (st, _) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&sup_prod_tok),
            &[],
            Some(json!({ "path": "proj-priv", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Remove `sync` from g2 (all-projects), grant `support` production `all` on `support`
    let (st, _) = h
        .req(
            Method::PATCH,
            "/v1/messageboard/boards/General/grants/g2",
            Some(&h.owner_token),
            &[],
            Some(json!({ "can": { "*": ["read", "post", "react", "label"] } })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);

    let (st, g4) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&h.owner_token),
            &[],
            Some(json!({
                "principals": [{ "kind": "project", "project": "support", "environments": ["production"] }],
                "can": { "support": "all" }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let g4_id = g4["grant"]["id"].as_str().unwrap().to_string();

    let (st, exp_sup) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=support",
            Some(&sup_prod_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(exp_sup["can"].as_array().unwrap().contains(&json!("sync")));

    let (st, exp_other) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=support",
            Some(&other_prod_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(!exp_other["can"].as_array().unwrap().contains(&json!("sync")));

    // Revoking g4 immediately removes `sync` from support project
    let (st, body) = h
        .req(
            Method::DELETE,
            &format!("/v1/messageboard/boards/General/grants/{g4_id}"),
            Some(&h.owner_token),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(body["revoked"], g4_id);

    let (st, exp_sup_after) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=support",
            Some(&sup_prod_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert!(!exp_sup_after["can"].as_array().unwrap().contains(&json!("sync")));

    // Grant remove principal + member cannot add `everyone` to a grant
    let (st, g5) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/grants",
            Some(&alice_tok),
            &[],
            Some(json!({
                "principals": [
                    { "kind": "user", "email": "alice@acme.com" },
                    { "kind": "user", "email": "bob@acme.com" }
                ],
                "can": { "team/": ["read", "post", "manage"] }
            })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    let g5_id = g5["grant"]["id"].as_str().unwrap().to_string();

    // Member cannot add `everyone` via PATCH
    let (st, _) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/grants/{g5_id}"),
            Some(&alice_tok),
            &[],
            Some(json!({ "add": [{ "kind": "everyone" }] })),
        )
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);

    // Member can remove Bob from g5
    let (st, updated) = h
        .req(
            Method::PATCH,
            &format!("/v1/messageboard/boards/General/grants/{g5_id}"),
            Some(&alice_tok),
            &[],
            Some(json!({ "remove": [{ "kind": "user", "email": "bob@acme.com" }] })),
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(updated["grant"]["principals"].as_array().unwrap().len(), 1);

    // Ancestor grant on `team/` does NOT block creating private channel `team/secret`
    let (st, priv_res) = h
        .req(
            Method::POST,
            "/v1/messageboard/boards/General/channels",
            Some(&alice_tok),
            &[],
            Some(json!({ "path": "team/secret", "private": true })),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(priv_res["channel"]["path"], "team/secret");

    // Bob cannot see `team/secret`
    let (st, bob_exp) = h
        .req(
            Method::GET,
            "/v1/messageboard/boards/General/grants/explain?channel=team/secret",
            Some(&bob_tok),
            &[],
            None,
        )
        .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(bob_exp["can"], json!([]));
}
