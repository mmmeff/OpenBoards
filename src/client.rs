use std::fmt;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Method, StatusCode};
use serde_json::{json, Value};

use crate::attachments::MAX_INLINE_UPLOAD_BYTES;
use crate::boards::DEFAULT_BOARD_NAME;

pub const MAX_RATE_LIMIT_RETRIES: usize = 4;

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub base_url: String,
    pub board: String,
    pub token: Option<String>,
    pub profile: Option<String>,
    pub avatar: Option<String>,
    pub team: Option<String>,
    pub ensure: bool,
    pub extra_headers: Vec<(String, String)>,
}

impl ClientConfig {
    /// Parses either a root URL (`http://127.0.0.1:8080`) or a full board URL
    /// (`http://127.0.0.1:8080/v1/messageboard/boards/security?teamId=acme`).
    pub fn from_raw(
        raw_url: &str,
        board_override: Option<&str>,
        token: Option<String>,
        profile: Option<String>,
        avatar: Option<String>,
        team: Option<String>,
        ensure: bool,
    ) -> Self {
        let trimmed = raw_url.trim().trim_end_matches('/');
        let mut base_url = trimmed.to_string();
        let mut parsed_board: Option<String> = None;
        let mut parsed_team = team;

        if let Some(idx) = trimmed.find("/v1/messageboard/boards/") {
            base_url = trimmed[..idx].to_string();
            let rest = &trimmed[idx + "/v1/messageboard/boards/".len()..];
            let (path_part, query_part) = match rest.split_once('?') {
                Some((p, q)) => (p, Some(q)),
                None => (rest, None),
            };
            let b = path_part.split('/').next().unwrap_or("");
            if !b.is_empty() {
                parsed_board = Some(b.to_string());
            }
            if parsed_team.is_none() {
                if let Some(q) = query_part {
                    for pair in q.split('&') {
                        if let Some((k, v)) = pair.split_once('=') {
                            if k == "teamId" && !v.is_empty() {
                                parsed_team = Some(v.to_string());
                            }
                        }
                    }
                }
            }
        }

        let board = board_override
            .map(str::to_string)
            .or(parsed_board)
            .unwrap_or_else(|| DEFAULT_BOARD_NAME.to_string());

        Self {
            base_url,
            board,
            token,
            profile,
            avatar,
            team: parsed_team,
            ensure,
            extra_headers: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientError {
    pub status: u16,
    pub code: String,
    pub message: String,
    pub hint: Option<String>,
    pub raw_body: Option<Value>,
}

impl ClientError {
    pub fn transport(msg: impl Into<String>) -> Self {
        Self {
            status: 0,
            code: "network_error".to_string(),
            message: msg.into(),
            hint: None,
            raw_body: None,
        }
    }

    pub fn usage(msg: impl Into<String>) -> Self {
        Self {
            status: 0,
            code: "usage_error".to_string(),
            message: msg.into(),
            hint: None,
            raw_body: None,
        }
    }

    /// CLI exit codes matching `docs/vercel-board/cli-reference.md`:
    /// - `2`: usage errors (invalid arguments/options, missing board)
    /// - `1`: command failures, conflicts, `nothing_ready`, `timeout`, auth/permission errors
    pub fn exit_code(&self) -> i32 {
        match self.code.as_str() {
            "usage_error" => 2,
            _ => 1,
        }
    }

    pub fn to_json(&self) -> Value {
        let mut obj = json!({
            "reason": self.code,
            "code": self.code,
            "status": self.status,
            "message": self.message,
        });
        if let Some(h) = &self.hint {
            obj["hint"] = json!(h);
        }
        obj
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)?;
        if let Some(h) = &self.hint {
            write!(f, " (hint: {h})")?;
        }
        Ok(())
    }
}

impl std::error::Error for ClientError {}

#[derive(Clone)]
pub struct BoardClient {
    http: reqwest::Client,
    pub config: ClientConfig,
}

impl BoardClient {
    pub fn new(config: ClientConfig) -> Result<Self, ClientError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| ClientError::transport(format!("failed to initialize HTTP client: {e}")))?;
        Ok(Self { http, config })
    }

    pub fn with_token(&self, token: impl Into<String>) -> Self {
        let mut cfg = self.config.clone();
        cfg.token = Some(token.into());
        Self {
            http: self.http.clone(),
            config: cfg,
        }
    }

    pub fn with_board(&self, board: impl Into<String>) -> Self {
        let mut cfg = self.config.clone();
        cfg.board = board.into();
        Self {
            http: self.http.clone(),
            config: cfg,
        }
    }

    pub fn with_profile(&self, profile: Option<String>, avatar: Option<String>) -> Self {
        let mut cfg = self.config.clone();
        cfg.profile = profile;
        cfg.avatar = avatar;
        Self {
            http: self.http.clone(),
            config: cfg,
        }
    }

    pub fn with_extra_header(&self, key: impl Into<String>, val: impl Into<String>) -> Self {
        let mut cfg = self.config.clone();
        cfg.extra_headers.push((key.into(), val.into()));
        Self {
            http: self.http.clone(),
            config: cfg,
        }
    }

    pub fn board_path(&self, suffix: &str) -> String {
        let board = url_encode(&self.config.board);
        if suffix.is_empty() {
            format!("/v1/messageboard/boards/{board}")
        } else if suffix.starts_with('/') {
            format!("/v1/messageboard/boards/{board}{suffix}")
        } else {
            format!("/v1/messageboard/boards/{board}/{suffix}")
        }
    }

    fn build_url(&self, path: &str, query: &[(String, String)], include_ensure: bool) -> String {
        let base = self.config.base_url.trim_end_matches('/');
        let mut pairs: Vec<(String, String)> = query.to_vec();
        if include_ensure && self.config.ensure && !pairs.iter().any(|(k, _)| k == "ensure") {
            pairs.push(("ensure".to_string(), "true".to_string()));
        }
        if let Some(team) = &self.config.team {
            if !pairs.iter().any(|(k, _)| k == "teamId") {
                pairs.push(("teamId".to_string(), team.clone()));
            }
        }
        if pairs.is_empty() {
            format!("{base}{path}")
        } else {
            let qs: String = pairs
                .iter()
                .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
                .collect::<Vec<_>>()
                .join("&");
            format!("{base}{path}?{qs}")
        }
    }

    fn auth_headers(&self, include_auth: bool) -> Result<HeaderMap, ClientError> {
        let mut headers = HeaderMap::new();
        if include_auth {
            if let Some(tok) = &self.config.token {
                let val = HeaderValue::from_str(&format!("Bearer {tok}"))
                    .map_err(|_| ClientError::usage("invalid token characters"))?;
                headers.insert(AUTHORIZATION, val);
            }
        }
        if let Some(prof) = &self.config.profile {
            let val = HeaderValue::from_str(prof)
                .map_err(|_| ClientError::usage("invalid profile header characters"))?;
            headers.insert(HeaderName::from_static("x-board-profile"), val);
        }
        if let Some(av) = &self.config.avatar {
            let val = HeaderValue::from_str(av)
                .map_err(|_| ClientError::usage("invalid avatar header characters"))?;
            headers.insert(HeaderName::from_static("x-board-avatar"), val);
        }
        for (k, v) in &self.config.extra_headers {
            let name = HeaderName::from_bytes(k.as_bytes())
                .map_err(|_| ClientError::usage(format!("invalid header name '{k}'")))?;
            let val = HeaderValue::from_str(v)
                .map_err(|_| ClientError::usage(format!("invalid header value for '{k}'")))?;
            headers.insert(name, val);
        }
        Ok(headers)
    }

    pub async fn request_json(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        body: Option<&Value>,
        include_auth: bool,
        include_ensure: bool,
    ) -> Result<Value, ClientError> {
        let url = self.build_url(path, query, include_ensure);
        let headers = self.auth_headers(include_auth)?;

        for attempt in 0..=MAX_RATE_LIMIT_RETRIES {
            let mut req = self.http.request(method.clone(), &url).headers(headers.clone());
            if let Some(b) = body {
                req = req.json(b);
            }
            let resp = req
                .send()
                .await
                .map_err(|e| ClientError::transport(format!("request to {url} failed: {e}")))?;

            let status = resp.status();
            if status == StatusCode::TOO_MANY_REQUESTS && attempt < MAX_RATE_LIMIT_RETRIES {
                let wait = retry_after_delay(resp.headers(), attempt);
                tokio::time::sleep(wait).await;
                continue;
            }

            let bytes = resp
                .bytes()
                .await
                .map_err(|e| ClientError::transport(format!("failed reading response body: {e}")))?;
            let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);

            if status.is_success() {
                return Ok(parsed);
            }

            return Err(parse_error_response(status, parsed));
        }

        Err(ClientError::transport("exceeded rate-limit retries"))
    }

    pub async fn request_raw(
        &self,
        method: Method,
        path: &str,
        query: &[(String, String)],
        content_type: Option<&str>,
        body: Vec<u8>,
        include_ensure: bool,
    ) -> Result<(HeaderMap, Vec<u8>), ClientError> {
        let url = self.build_url(path, query, include_ensure);
        let mut headers = self.auth_headers(true)?;
        if let Some(ct) = content_type {
            if let Ok(val) = HeaderValue::from_str(ct) {
                headers.insert(CONTENT_TYPE, val);
            }
        }

        for attempt in 0..=MAX_RATE_LIMIT_RETRIES {
            let resp = self
                .http
                .request(method.clone(), &url)
                .headers(headers.clone())
                .body(body.clone())
                .send()
                .await
                .map_err(|e| ClientError::transport(format!("request to {url} failed: {e}")))?;

            let status = resp.status();
            if status == StatusCode::TOO_MANY_REQUESTS && attempt < MAX_RATE_LIMIT_RETRIES {
                let wait = retry_after_delay(resp.headers(), attempt);
                tokio::time::sleep(wait).await;
                continue;
            }

            let resp_headers = resp.headers().clone();
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| ClientError::transport(format!("failed reading response body: {e}")))?;

            if status.is_success() {
                return Ok((resp_headers, bytes.to_vec()));
            }
            let parsed: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            return Err(parse_error_response(status, parsed));
        }

        Err(ClientError::transport("exceeded rate-limit retries"))
    }

    // --- High-level API helpers ---

    pub async fn healthz(&self) -> Result<Value, ClientError> {
        self.request_json(Method::GET, "/healthz", &[], None, false, false)
            .await
    }

    pub async fn discovery(&self) -> Result<Value, ClientError> {
        self.request_json(Method::GET, "/discovery", &[], None, false, false)
            .await
    }

    pub async fn whoami(&self) -> Result<Value, ClientError> {
        self.request_json(Method::GET, "/whoami", &[], None, true, false)
            .await
    }

    pub async fn me(&self) -> Result<Value, ClientError> {
        self.request_json(Method::GET, &self.board_path("me"), &[], None, true, true)
            .await
    }

    pub async fn actors(&self) -> Result<Value, ClientError> {
        self.request_json(Method::GET, &self.board_path("actors"), &[], None, true, true)
            .await
    }

    pub async fn list_boards(&self) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            "/v1/messageboard/boards",
            &[],
            None,
            true,
            false,
        )
        .await
    }

    pub async fn create_board(&self, name: &str, ensure: bool) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            "/v1/messageboard/boards",
            &[],
            Some(&json!({ "name": name, "ensure": ensure })),
            true,
            false,
        )
        .await
    }

    pub async fn get_board(&self, ensure_param: Option<bool>) -> Result<Value, ClientError> {
        let mut q = Vec::new();
        if let Some(e) = ensure_param {
            q.push(("ensure".to_string(), e.to_string()));
        }
        self.request_json(Method::GET, &self.board_path(""), &q, None, true, false)
            .await
    }

    pub async fn list_channels(&self) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("channels"),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn create_channel(&self, path: &str, private: bool) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("channels"),
            &[],
            Some(&json!({ "path": path, "private": private })),
            true,
            true,
        )
        .await
    }

    pub async fn list_grants(&self) -> Result<Value, ClientError> {
        self.request_json(Method::GET, &self.board_path("grants"), &[], None, true, true)
            .await
    }

    pub async fn create_grant(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("grants"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn update_grant(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::PATCH,
            &self.board_path(&format!("grants/{id}")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn revoke_grant(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::DELETE,
            &self.board_path(&format!("grants/{id}")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn explain_grant(&self, query: &[(String, String)]) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("grants/explain"),
            query,
            None,
            true,
            true,
        )
        .await
    }

    pub async fn list_schemas(&self) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("schemas"),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn get_schema(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("schemas/{}", url_encode(id))),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn create_schema(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("schemas"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn post_message(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("messages"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn reply_message(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("messages/{id}/replies")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn list_messages(&self, query: &[(String, String)]) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("messages"),
            query,
            None,
            true,
            true,
        )
        .await
    }

    pub async fn list_threads(&self, query: &[(String, String)]) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("threads"),
            query,
            None,
            true,
            true,
        )
        .await
    }

    pub async fn get_thread(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("threads/{id}")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn get_message(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("messages/{id}")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn get_posted(&self, key: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("posted/{}", url_encode(key))),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn edit_message(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::PATCH,
            &self.board_path(&format!("messages/{id}")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn delete_message(
        &self,
        id: &str,
        revision: Option<i64>,
    ) -> Result<Value, ClientError> {
        let mut q = Vec::new();
        if let Some(rev) = revision {
            q.push(("revision".to_string(), rev.to_string()));
        }
        self.request_json(
            Method::DELETE,
            &self.board_path(&format!("messages/{id}")),
            &q,
            None,
            true,
            true,
        )
        .await
    }

    pub async fn react(&self, id: &str, emoji: &str, remove: bool) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("messages/{id}/reactions")),
            &[],
            Some(&json!({ "emoji": emoji, "remove": remove })),
            true,
            true,
        )
        .await
    }

    pub async fn label(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("messages/{id}/labels")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    /// Uploads bytes using inline endpoint (<= 4 MB) or two-step streaming (> 4 MB).
    pub async fn upload_bytes(
        &self,
        filename: &str,
        content_type: Option<&str>,
        data: Vec<u8>,
    ) -> Result<Value, ClientError> {
        if (data.len() as u64) <= MAX_INLINE_UPLOAD_BYTES {
            let (_, bytes) = self
                .request_raw(
                    Method::POST,
                    &self.board_path("attachments"),
                    &[("name".to_string(), filename.to_string())],
                    content_type.or(Some("application/octet-stream")),
                    data,
                    true,
                )
                .await?;
            return serde_json::from_slice(&bytes)
                .map_err(|e| ClientError::transport(format!("invalid upload response: {e}")));
        }

        let init = self
            .request_json(
                Method::POST,
                &self.board_path("attachments/uploads"),
                &[],
                Some(&json!({
                    "name": filename,
                    "type": content_type.unwrap_or("application/octet-stream"),
                    "size": data.len()
                })),
                true,
                true,
            )
            .await?;
        let upload_url = init["upload_url"]
            .as_str()
            .ok_or_else(|| ClientError::transport("missing upload_url"))?;
        let complete_url = init["complete_url"]
            .as_str()
            .ok_or_else(|| ClientError::transport("missing complete_url"))?;

        let _ = self
            .request_raw(Method::PUT, upload_url, &[], content_type, data, true)
            .await?;
        let completed = self
            .request_json(Method::POST, complete_url, &[], None, true, true)
            .await?;
        Ok(completed["attachment"].clone())
    }

    pub async fn download_attachment(&self, id: &str) -> Result<(HeaderMap, Vec<u8>), ClientError> {
        self.request_raw(
            Method::GET,
            &self.board_path(&format!("attachments/{id}")),
            &[],
            None,
            Vec::new(),
            true,
        )
        .await
    }

    pub async fn poll_events(&self, query: &[(String, String)]) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("events"),
            query,
            None,
            true,
            true,
        )
        .await
    }

    pub async fn wait_event(&self, query: &[(String, String)]) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("wait"),
            query,
            None,
            true,
            true,
        )
        .await
    }

    // --- Tasks ---

    pub async fn list_tasks(&self, query: &[(String, String)]) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("tasks"),
            query,
            None,
            true,
            true,
        )
        .await
    }

    pub async fn create_task(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("tasks"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn create_many_tasks(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("tasks/batch"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn get_task(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("tasks/{id}")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn claim_task(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/claim")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn claim_ready(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("tasks/claim-ready"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn renew_task(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/renew")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn release_task(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/release")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn complete_task(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/complete")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn fail_task(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/fail")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn block_task(&self, id: &str, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/block")),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn unblock_task(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/unblock")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn cancel_task(&self, id: &str, reason: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/cancel")),
            &[],
            Some(&json!({ "reason": reason })),
            true,
            true,
        )
        .await
    }

    pub async fn reopen_task(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/reopen")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn assign_task(&self, id: &str, target: Option<&str>) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("tasks/{id}/assign")),
            &[],
            Some(&json!({ "for": target })),
            true,
            true,
        )
        .await
    }

    pub async fn update_task_needs(&self, id: &str, needs: &[String]) -> Result<Value, ClientError> {
        self.request_json(
            Method::PUT,
            &self.board_path(&format!("tasks/{id}/needs")),
            &[],
            Some(&json!({ "needs": needs })),
            true,
            true,
        )
        .await
    }

    // --- Schedules ---

    pub async fn list_schedules(&self, query: &[(String, String)]) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("schedules"),
            query,
            None,
            true,
            true,
        )
        .await
    }

    pub async fn create_schedule(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("schedules"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn get_schedule(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("schedules/{id}")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn cancel_schedule(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::DELETE,
            &self.board_path(&format!("schedules/{id}")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    // --- Watchers ---

    pub async fn list_watchers(&self) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("watchers"),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn create_watcher(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("watchers"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn sync_watchers(&self, body: &Value) -> Result<Value, ClientError> {
        let mut q = vec![("board".to_string(), self.config.board.clone())];
        if self.config.ensure {
            q.push(("ensure".to_string(), "true".to_string()));
        }
        self.request_json(
            Method::POST,
            "/api/board/watchers",
            &q,
            Some(body),
            true,
            false,
        )
        .await
    }

    pub async fn delete_watcher(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::DELETE,
            &self.board_path(&format!("watchers/{id}")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    // --- Sync ---

    pub async fn sync_create_source(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path("sync/sources"),
            &[],
            Some(body),
            true,
            true,
        )
        .await
    }

    pub async fn sync_list_sources(&self) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("sync/sources"),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn sync_delete_source(&self, key: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::DELETE,
            &self.board_path(&format!("sync/sources/{}", url_encode(key))),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn sync_inbox(&self, key: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("sync/sources/{}/inbox", url_encode(key))),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn sync_records(&self, key: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("sync/sources/{}/records", url_encode(key))),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn sync_list_thread_links(&self, thread_id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path(&format!("threads/{thread_id}/sync")),
            &[],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn sync_link_thread(&self, thread_id: &str, target: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("threads/{thread_id}/sync")),
            &[],
            Some(&json!({ "target": target })),
            true,
            true,
        )
        .await
    }

    pub async fn sync_linked(&self, url: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::GET,
            &self.board_path("sync/linked"),
            &[("url".to_string(), url.to_string())],
            None,
            true,
            true,
        )
        .await
    }

    pub async fn sync_typing(
        &self,
        thread_id: &str,
        status: Option<&str>,
    ) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            &self.board_path(&format!("threads/{thread_id}/typing")),
            &[],
            Some(&json!({ "status": status })),
            true,
            true,
        )
        .await
    }

    // --- Admin ---

    pub async fn admin_create_user(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(Method::POST, "/v1/admin/users", &[], Some(body), true, false)
            .await
    }

    pub async fn admin_list_users(&self) -> Result<Value, ClientError> {
        self.request_json(Method::GET, "/v1/admin/users", &[], None, true, false)
            .await
    }

    pub async fn admin_create_project(&self, name: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            "/v1/admin/projects",
            &[],
            Some(&json!({ "name": name })),
            true,
            false,
        )
        .await
    }

    pub async fn admin_create_app(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(Method::POST, "/v1/admin/apps", &[], Some(body), true, false)
            .await
    }

    pub async fn admin_create_token(&self, body: &Value) -> Result<Value, ClientError> {
        self.request_json(
            Method::POST,
            "/v1/admin/tokens",
            &[],
            Some(body),
            true,
            false,
        )
        .await
    }

    pub async fn admin_revoke_token(&self, id: &str) -> Result<Value, ClientError> {
        self.request_json(
            Method::DELETE,
            &format!("/v1/admin/tokens/{id}"),
            &[],
            None,
            true,
            false,
        )
        .await
    }
}

fn parse_error_response(status: StatusCode, parsed: Value) -> ClientError {
    let code = parsed["code"]
        .as_str()
        .unwrap_or(match status {
            StatusCode::UNAUTHORIZED => "unauthorized",
            StatusCode::FORBIDDEN => "forbidden",
            StatusCode::NOT_FOUND => "not_found",
            StatusCode::CONFLICT => "conflict",
            StatusCode::REQUEST_TIMEOUT => "timeout",
            StatusCode::TOO_MANY_REQUESTS => "rate_limited",
            _ => "http_error",
        })
        .to_string();
    let message = parsed["message"]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| format!("HTTP {}", status.as_u16()));
    let hint = parsed["hint"].as_str().map(str::to_string);
    ClientError {
        status: status.as_u16(),
        code,
        message,
        hint,
        raw_body: Some(parsed),
    }
}

fn retry_after_delay(headers: &HeaderMap, attempt: usize) -> Duration {
    if let Some(val) = headers.get("retry-after").and_then(|v| v.to_str().ok()) {
        if let Ok(secs) = val.trim().parse::<u64>() {
            return Duration::from_secs(secs.min(30));
        }
    }
    Duration::from_millis(200u64.saturating_mul(1u64 << attempt))
}

pub fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            use std::fmt::Write;
            let _ = write!(&mut out, "%{b:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_root_and_full_board_urls() {
        let c1 = ClientConfig::from_raw(
            "http://127.0.0.1:8080/v1/messageboard/boards/security?teamId=acme",
            None,
            Some("tok".into()),
            None,
            None,
            None,
            false,
        );
        assert_eq!(c1.base_url, "http://127.0.0.1:8080");
        assert_eq!(c1.board, "security");
        assert_eq!(c1.team.as_deref(), Some("acme"));

        let c2 = ClientConfig::from_raw(
            "http://localhost:9000/",
            Some("ops"),
            None,
            None,
            None,
            None,
            true,
        );
        assert_eq!(c2.base_url, "http://localhost:9000");
        assert_eq!(c2.board, "ops");
        assert!(c2.ensure);
    }

    #[test]
    fn maps_cli_exit_codes() {
        assert_eq!(ClientError::usage("bad").exit_code(), 2);
        assert_eq!(
            ClientError {
                status: 404,
                code: "nothing_ready".into(),
                message: "".into(),
                hint: None,
                raw_body: None,
            }
            .exit_code(),
            1
        );
        assert_eq!(
            ClientError {
                status: 408,
                code: "timeout".into(),
                message: "".into(),
                hint: None,
                raw_body: None,
            }
            .exit_code(),
            1
        );
        assert_eq!(
            ClientError {
                status: 403,
                code: "forbidden".into(),
                message: "".into(),
                hint: None,
                raw_body: None,
            }
            .exit_code(),
            1
        );
    }
}
