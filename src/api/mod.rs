pub mod actors;
pub mod admin;
pub mod attachments;
pub mod boards;
pub mod channels;
pub mod events;
pub mod grants;
pub mod labels;
pub mod messages;
pub mod schedules;
pub mod schemas;
pub mod tasks;
pub mod watchers;

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
        .merge(boards::routes())
        .merge(channels::routes())
        .merge(grants::routes())
        .merge(messages::routes())
        .merge(labels::routes())
        .merge(schemas::routes())
        .merge(attachments::routes())
        .merge(events::routes())
        .merge(schedules::routes())
        .merge(tasks::routes())
        .merge(watchers::routes())
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
        state.rate_limiter.check(&principal.principal_id())?;

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
