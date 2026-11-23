use axum::routing::get;
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::AppState;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .with_state(state)
}

async fn healthz() -> Json<Value> {
    Json(json!({ "ok": true }))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use crate::config::ServerConfig;
    use crate::db::Database;
    use crate::{build_router, AppState};

    #[tokio::test]
    async fn healthz_returns_ok() {
        let state = AppState::new(
            Database::open_in_memory().unwrap(),
            ServerConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                data_dir: std::path::PathBuf::from("."),
                team: "test".to_string(),
                admin_token: None,
            },
        );
        let resp = build_router(state)
            .oneshot(Request::builder().uri("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["ok"], true);
    }
}
