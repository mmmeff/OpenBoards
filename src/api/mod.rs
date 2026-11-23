pub mod actors;
pub mod admin;

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::{
    authenticate_bearer_token, parse_profile_headers, parse_sandboxed_header, CallerContext,
};
use crate::error::{BoardError, BoardResult};
use crate::permissions::NarrowingPolicy;
use crate::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .merge(admin::routes())
        .merge(actors::routes())
        .with_state(state)
}

async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

fn header_str<'a>(parts: &'a Parts, name: &str) -> BoardResult<Option<&'a str>> {
    match parts.headers.get(name) {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(Some)
            .map_err(|_| BoardError::bad_request(format!("header '{name}' must be valid ASCII"))),
    }
}

impl FromRequestParts<AppState> for CallerContext {
    type Rejection = BoardError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, Self::Rejection> {
        let auth_hdr = parts
            .headers
            .get(AUTHORIZATION)
            .ok_or_else(|| {
                BoardError::unauthorized("missing Authorization header")
                    .with_hint("Send 'Authorization: Bearer <token>'.")
            })?
            .to_str()
            .map_err(|_| BoardError::unauthorized("invalid Authorization header encoding"))?;

        let raw_token = auth_hdr
            .strip_prefix("Bearer ")
            .or_else(|| auth_hdr.strip_prefix("bearer "))
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                BoardError::unauthorized("Authorization header must use Bearer scheme")
            })?;

        let principal = authenticate_bearer_token(&state.db, raw_token)?;

        let profile = parse_profile_headers(
            header_str(parts, "x-board-profile")?,
            header_str(parts, "x-board-as")?,
            header_str(parts, "x-board-avatar")?,
        )?;

        let narrowing = match header_str(parts, "x-board-can")? {
            Some(raw) => Some(NarrowingPolicy::parse_header(raw)?),
            None => None,
        };

        let sandboxed = parse_sandboxed_header(header_str(parts, "x-board-sandboxed")?)?;

        Ok(CallerContext {
            principal,
            profile,
            narrowing,
            sandboxed,
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::actors::{self, CallerContext, ProfileOverlay};
    use crate::config::ServerConfig;
    use crate::db::Database;
    use crate::{build_router, AppState};

    fn test_app(admin_token: &str) -> (AppState, axum::Router) {
        let db = Database::open_in_memory().unwrap();
        actors::bootstrap_owner(&db, Some(admin_token)).unwrap();
        let state = AppState::new(
            db,
            ServerConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                data_dir: std::path::PathBuf::from("."),
                team: "acme".to_string(),
                admin_token: Some(admin_token.to_string()),
            },
        );
        let app = build_router(state.clone());
        (state, app)
    }

    async fn send_json(
        app: &axum::Router,
        method: Method,
        uri: &str,
        token: Option<&str>,
        extra_headers: &[(&str, &str)],
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            builder = builder.header("authorization", format!("Bearer {t}"));
        }
        for &(k, v) in extra_headers {
            builder = builder.header(k, v);
        }
        let req = match body {
            Some(v) => builder
                .header("content-type", "application/json")
                .body(Body::from(v.to_string()))
                .unwrap(),
            None => builder.body(Body::empty()).unwrap(),
        };
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let val: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, val)
    }

    #[tokio::test]
    async fn healthz_returns_ok() {
        let (_state, app) = test_app("owner-secret");
        let (status, body) = send_json(&app, Method::GET, "/healthz", None, &[], None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ok"], true);
    }

    #[tokio::test]
    async fn whoami_auth_profiles_roles_and_narrowing() {
        let (_state, app) = test_app("owner-secret");

        // Unauthenticated fails with 401
        let (status, body) = send_json(&app, Method::GET, "/whoami", None, &[], None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], "unauthorized");

        // Owner whoami with profile, avatar, narrowing, and sandboxed headers
        let (status, body) = send_json(
            &app,
            Method::GET,
            "/whoami",
            Some("owner-secret"),
            &[
                ("x-board-as", "Fx"),
                ("x-board-avatar", "https://example.com/fx.png"),
                ("x-board-can", "read,post"),
                ("x-board-sandboxed", "true"),
            ],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["kind"], "user");
        assert_eq!(body["role"], "owner");
        assert_eq!(body["profile"], "Fx");
        assert_eq!(body["handle"], "admin/fx");
        assert!(body["actor"].as_str().unwrap().ends_with("+fx"));
        assert_eq!(body["avatar"], "https://example.com/fx.png");
        assert_eq!(body["narrowed"], true);
        assert_eq!(body["sandboxed"], true);
        assert_eq!(body["can"]["*"], json!(["read", "post"]));

        // Narrowed owner cannot call admin endpoints
        let (status, body) = send_json(
            &app,
            Method::GET,
            "/v1/admin/users",
            Some("owner-secret"),
            &[("x-board-can", "all")],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["code"], "forbidden");

        // Sandboxed owner cannot call admin endpoints
        let (status, _) = send_json(
            &app,
            Method::GET,
            "/v1/admin/users",
            Some("owner-secret"),
            &[("x-board-sandboxed", "true")],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn admin_crud_and_actor_visibility_filtering() {
        let (state, app) = test_app("owner-secret");

        // Create member Ada
        let (status, body) = send_json(
            &app,
            Method::POST,
            "/v1/admin/users",
            Some("owner-secret"),
            &[],
            Some(json!({
                "username": "ada",
                "email": "ada@acme.com",
                "name": "Ada Lovelace",
                "role": "member"
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let ada_id = body["user"]["id"].as_str().unwrap().to_string();

        // Mint token for Ada
        let (status, body) = send_json(
            &app,
            Method::POST,
            "/v1/admin/tokens",
            Some("owner-secret"),
            &[],
            Some(json!({
                "principal_kind": "user",
                "principal_id": ada_id,
                "label": "ada-cli"
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let ada_tok = body["token"]["token"].as_str().unwrap().to_string();

        // Member cannot access admin endpoints
        let (status, body) =
            send_json(&app, Method::GET, "/v1/admin/users", Some(&ada_tok), &[], None).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["code"], "forbidden");

        // Create project + preview token, and app + scoped token
        let (status, body) = send_json(
            &app,
            Method::POST,
            "/v1/admin/projects",
            Some("owner-secret"),
            &[],
            Some(json!({ "name": "support" })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let prj_id = body["project"]["id"].as_str().unwrap().to_string();

        let (status, body) = send_json(
            &app,
            Method::POST,
            "/v1/admin/tokens",
            Some("owner-secret"),
            &[],
            Some(json!({
                "principal_kind": "project",
                "principal_id": prj_id,
                "environment": "preview"
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let prj_tok = body["token"]["token"].as_str().unwrap().to_string();

        let (status, body) = send_json(
            &app,
            Method::GET,
            "/whoami",
            Some(&prj_tok),
            &[("x-board-profile", "triage")],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["handle"], "support/triage");
        assert_eq!(body["environment"], "preview");

        let (status, body) = send_json(
            &app,
            Method::POST,
            "/v1/admin/apps",
            Some("owner-secret"),
            &[],
            Some(json!({
                "slug": "ci-bot",
                "name": "CI Bot",
                "scopes": ["read", "post"]
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let app_id = body["app"]["id"].as_str().unwrap().to_string();

        let (status, body) = send_json(
            &app,
            Method::POST,
            "/v1/admin/tokens",
            Some("owner-secret"),
            &[],
            Some(json!({
                "principal_kind": "app",
                "principal_id": app_id
            })),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let app_tok = body["token"]["token"].as_str().unwrap().to_string();

        let (status, body) =
            send_json(&app, Method::GET, "/whoami", Some(&app_tok), &[], None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["kind"], "app");
        assert_eq!(body["scopes"], json!(["read", "post"]));

        // Simulate implicit profile registration & writes on two channels: "dev" and "secret"
        let ada_principal = actors::authenticate_bearer_token(&state.db, &ada_tok).unwrap();
        let prj_principal = actors::authenticate_bearer_token(&state.db, &prj_tok).unwrap();
        let ada_fx = CallerContext {
            principal: ada_principal,
            profile: Some(ProfileOverlay {
                name: "Fx".to_string(),
                name_lower: "fx".to_string(),
                avatar: Some("https://example.com/fx.png".to_string()),
            }),
            narrowing: None,
            sandboxed: false,
        };
        let prj_triage = CallerContext {
            principal: prj_principal,
            profile: Some(ProfileOverlay {
                name: "triage".to_string(),
                name_lower: "triage".to_string(),
                avatar: None,
            }),
            narrowing: None,
            sandboxed: false,
        };

        actors::record_actor_write(&state.db, "general", "dev", &ada_fx).unwrap();
        actors::record_actor_write(&state.db, "general", "secret", &prj_triage).unwrap();

        // Unrestricted caller sees both actors
        let (status, body) = send_json(
            &app,
            Method::GET,
            "/v1/messageboard/boards/General/actors",
            Some(&ada_tok),
            &[],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let actors_list = body["actors"].as_array().unwrap();
        assert_eq!(actors_list.len(), 2);
        assert_eq!(actors_list[0]["handle"], "ada/fx");
        assert_eq!(actors_list[0]["avatar"], "https://example.com/fx.png");
        assert_eq!(actors_list[1]["handle"], "support/triage");

        // Caller narrowed to dev/ sees ONLY ada/fx, never leaking support/triage from secret
        let (status, body) = send_json(
            &app,
            Method::GET,
            "/v1/messageboard/boards/General/actors",
            Some(&ada_tok),
            &[("x-board-can", r#"{"dev/":["read"]}"#)],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let actors_list = body["actors"].as_array().unwrap();
        assert_eq!(actors_list.len(), 1);
        assert_eq!(actors_list[0]["handle"], "ada/fx");

        // Revoking Ada denies future requests
        let (status, _) = send_json(
            &app,
            Method::DELETE,
            &format!("/v1/admin/users/{ada_id}"),
            Some("owner-secret"),
            &[],
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let (status, _) =
            send_json(&app, Method::GET, "/whoami", Some(&ada_tok), &[], None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
}
