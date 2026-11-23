use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, patch, post};
use axum::{Json, Router};
use serde_json::{json, Value};

use crate::actors::{
    self, CallerContext, CreateAppInput, CreateProjectInput, CreateTokenInput, CreateUserInput,
    UpdateAppInput, UpdateUserInput,
};
use crate::error::{BoardError, BoardResult};
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/admin/users", post(create_user).get(list_users))
        .route(
            "/v1/admin/users/{id}",
            patch(update_user).delete(revoke_user),
        )
        .route("/v1/admin/projects", post(create_project).get(list_projects))
        .route("/v1/admin/projects/{id}", delete(revoke_project))
        .route("/v1/admin/apps", post(create_app).get(list_apps))
        .route("/v1/admin/apps/{id}", patch(update_app).delete(revoke_app))
        .route("/v1/admin/tokens", post(create_token).get(list_tokens))
        .route("/v1/admin/tokens/{id}", delete(revoke_token))
}

fn require_admin(caller: &CallerContext) -> BoardResult<()> {
    if !caller.is_unrestricted_owner() {
        return Err(BoardError::forbidden(
            "admin endpoints require an unrestricted owner principal",
        ));
    }
    Ok(())
}

async fn create_user(
    State(state): State<AppState>,
    caller: CallerContext,
    Json(input): Json<CreateUserInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    require_admin(&caller)?;
    let user = actors::create_user(&state.db, input)?;
    Ok((StatusCode::CREATED, Json(json!({ "user": user }))))
}

async fn list_users(
    State(state): State<AppState>,
    caller: CallerContext,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let users = actors::list_users(&state.db)?;
    Ok(Json(json!({ "users": users })))
}

async fn update_user(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(id): Path<String>,
    Json(input): Json<UpdateUserInput>,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let user = actors::update_user(&state.db, &id, input)?;
    Ok(Json(json!({ "user": user })))
}

async fn revoke_user(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(id): Path<String>,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let user = actors::revoke_user(&state.db, &id)?;
    Ok(Json(json!({ "user": user })))
}

async fn create_project(
    State(state): State<AppState>,
    caller: CallerContext,
    Json(input): Json<CreateProjectInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    require_admin(&caller)?;
    let project = actors::create_project(&state.db, input)?;
    Ok((StatusCode::CREATED, Json(json!({ "project": project }))))
}

async fn list_projects(
    State(state): State<AppState>,
    caller: CallerContext,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let projects = actors::list_projects(&state.db)?;
    Ok(Json(json!({ "projects": projects })))
}

async fn revoke_project(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(id): Path<String>,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let project = actors::revoke_project(&state.db, &id)?;
    Ok(Json(json!({ "project": project })))
}

async fn create_app(
    State(state): State<AppState>,
    caller: CallerContext,
    Json(input): Json<CreateAppInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    require_admin(&caller)?;
    let app = actors::create_app(&state.db, input)?;
    Ok((StatusCode::CREATED, Json(json!({ "app": app }))))
}

async fn list_apps(
    State(state): State<AppState>,
    caller: CallerContext,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let apps = actors::list_apps(&state.db)?;
    Ok(Json(json!({ "apps": apps })))
}

async fn update_app(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(id): Path<String>,
    Json(input): Json<UpdateAppInput>,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let app = actors::update_app(&state.db, &id, input)?;
    Ok(Json(json!({ "app": app })))
}

async fn revoke_app(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(id): Path<String>,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let app = actors::revoke_app(&state.db, &id)?;
    Ok(Json(json!({ "app": app })))
}

async fn create_token(
    State(state): State<AppState>,
    caller: CallerContext,
    Json(input): Json<CreateTokenInput>,
) -> BoardResult<(StatusCode, Json<Value>)> {
    require_admin(&caller)?;
    let created = actors::create_token(&state.db, input)?;
    Ok((StatusCode::CREATED, Json(json!({ "token": created }))))
}

async fn list_tokens(
    State(state): State<AppState>,
    caller: CallerContext,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let tokens = actors::list_tokens(&state.db)?;
    Ok(Json(json!({ "tokens": tokens })))
}

async fn revoke_token(
    State(state): State<AppState>,
    caller: CallerContext,
    Path(id): Path<String>,
) -> BoardResult<Json<Value>> {
    require_admin(&caller)?;
    let token = actors::revoke_token(&state.db, &id)?;
    Ok(Json(json!({ "token": token })))
}
