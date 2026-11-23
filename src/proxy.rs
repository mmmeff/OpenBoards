use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::{AUTHORIZATION, CONNECTION, CONTENT_LENGTH, HOST, TRANSFER_ENCODING};
use axum::http::{HeaderName, HeaderValue, Request, Response, StatusCode};
use axum::Router;
use http_body_util::BodyExt;

use crate::error::{BoardError, BoardResult};
use crate::permissions::NarrowingPolicy;
use crate::validate::{validate_avatar_url, validate_profile_name};

#[derive(Debug, Clone)]
pub struct ProxyConfig {
    pub listen: SocketAddr,
    pub upstream: String,
    pub token: String,
    pub profile: String,
    pub can: String,
    pub avatar: Option<String>,
}

#[derive(Clone)]
struct ProxyState {
    http: reqwest::Client,
    upstream: String,
    token: String,
    profile: String,
    can: String,
    avatar: Option<String>,
}

/// Validates `ProxyConfig` and constructs a credential-narrowing reverse proxy `Router`.
///
/// Equivalent to `sandboxPolicy({ profile, can, token })`: strips any incoming `Authorization`
/// and `X-Board-*` headers from untrusted callers and forcibly injects the configured upstream
/// Bearer token, `X-Board-Profile`, `X-Board-Can`, and `X-Board-Sandboxed: true`.
pub fn build_proxy_router(config: ProxyConfig) -> BoardResult<Router> {
    if config.token.trim().is_empty() {
        return Err(BoardError::bad_request("proxy --token cannot be empty"));
    }
    if !(config.upstream.starts_with("http://") || config.upstream.starts_with("https://")) {
        return Err(BoardError::bad_request(
            "proxy --upstream must start with http:// or https://",
        ));
    }
    validate_profile_name(&config.profile)?;
    let _ = NarrowingPolicy::parse_header(&config.can)?;
    if let Some(av) = &config.avatar {
        validate_avatar_url(av)?;
    }

    let http = reqwest::Client::builder()
        .build()
        .map_err(|e| BoardError::internal(format!("failed to create proxy HTTP client: {e}")))?;

    let state = Arc::new(ProxyState {
        http,
        upstream: config.upstream.trim_end_matches('/').to_string(),
        token: config.token,
        profile: config.profile,
        can: config.can,
        avatar: config.avatar,
    });

    Ok(Router::new()
        .fallback(proxy_handler)
        .layer(DefaultBodyLimit::disable())
        .with_state(state))
}

pub async fn run_proxy(config: ProxyConfig) -> BoardResult<()> {
    let listen = config.listen;
    let upstream = config.upstream.clone();
    let profile = config.profile.clone();
    let can = config.can.clone();
    let app = build_proxy_router(config)?;

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| BoardError::internal(format!("failed to bind proxy {listen}: {e}")))?;
    let bound = listener.local_addr().unwrap_or(listen);

    println!("openboards sandbox proxy listening on http://{bound}");
    println!("  upstream: {upstream}");
    println!("  profile:  {profile}");
    println!("  can:      {can}");
    println!("export BOARD_URL=http://{bound}");
    println!("export BOARD_TOKEN=sandbox-placeholder");

    axum::serve(listener, app)
        .await
        .map_err(|e| BoardError::internal(format!("proxy server error: {e}")))
}

async fn proxy_handler(
    State(state): State<Arc<ProxyState>>,
    req: Request<Body>,
) -> Result<Response<Body>, BoardError> {
    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let target_url = format!("{}{}", state.upstream, path_and_query);

    let body_bytes: Bytes = body
        .collect()
        .await
        .map_err(|e| BoardError::bad_request(format!("failed to read request body: {e}")))?
        .to_bytes();

    let method = reqwest::Method::from_bytes(parts.method.as_str().as_bytes())
        .map_err(|_| BoardError::bad_request("unsupported HTTP method"))?;

    let mut out_req = state.http.request(method, &target_url);

    // Forward safe headers ONLY; strip Authorization, hop-by-hop, and ALL incoming X-Board-* headers
    for (name, val) in &parts.headers {
        let lower = name.as_str().to_ascii_lowercase();
        if name == AUTHORIZATION
            || name == HOST
            || name == CONTENT_LENGTH
            || name == TRANSFER_ENCODING
            || name == CONNECTION
            || lower.starts_with("x-board-")
        {
            continue;
        }
        out_req = out_req.header(name.as_str(), val.as_bytes());
    }

    // Forcibly inject upstream credential + enforced sandbox profile & narrowing policy
    out_req = out_req
        .header(AUTHORIZATION.as_str(), format!("Bearer {}", state.token))
        .header("x-board-profile", &state.profile)
        .header("x-board-can", &state.can)
        .header("x-board-sandboxed", "true");
    if let Some(av) = &state.avatar {
        out_req = out_req.header("x-board-avatar", av);
    }

    if !body_bytes.is_empty() {
        out_req = out_req.body(body_bytes);
    }

    let upstream_resp = out_req
        .send()
        .await
        .map_err(|e| BoardError::internal(format!("upstream proxy request failed: {e}")))?;

    let status = StatusCode::from_u16(upstream_resp.status().as_u16())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let resp_headers = upstream_resp.headers().clone();
    let resp_bytes = upstream_resp
        .bytes()
        .await
        .map_err(|e| BoardError::internal(format!("failed reading upstream response: {e}")))?;

    let mut builder = Response::builder().status(status);
    for (name, val) in &resp_headers {
        if name == TRANSFER_ENCODING || name == CONNECTION || name == CONTENT_LENGTH {
            continue;
        }
        if let (Ok(hname), Ok(hval)) = (
            HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(val.as_bytes()),
        ) {
            builder = builder.header(hname, hval);
        }
    }

    builder
        .body(Body::from(resp_bytes))
        .map_err(|e| BoardError::internal(format!("failed constructing proxy response: {e}")))
}
