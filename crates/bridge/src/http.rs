//! Remote HTTP MCP server with OAuth support.
//!
//! Exposes RelayFS tools over HTTP (Streamable HTTP / SSE) so AI clients
//! (Cursor, Claude, web agents) can connect directly without needing a local
//! relayfs client binary. Supports OAuth 2.0 / token authentication.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{
    body::Body,
    extract::{Form, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode, Uri},
    middleware::Next,
    response::{Html, IntoResponse, Json, Redirect, Response},
    routing::{get, post},
    Router,
};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::info;

use crate::client::AgentClient;
use crate::mcp::RelayfsServer;

/// Pending OAuth authorization codes.
#[derive(Clone)]
pub struct AuthCodeEntry {
    pub token: String,
    pub expires_at: Instant,
    pub redirect_uri: Option<String>,
}

#[derive(Clone)]
pub struct HttpState {
    pub token: String,
    pub auth_codes: Arc<RwLock<HashMap<String, AuthCodeEntry>>>,
}

impl HttpState {
    pub fn new(token: String) -> Self {
        Self {
            token,
            auth_codes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

/// Build the Axum router for the remote HTTP MCP server and OAuth endpoints.
pub fn make_router(state: HttpState, server: RelayfsServer) -> Router {
    // Disable host and origin restrictions since this is a remote server
    // meant to be accessed across networks and diverse client domains.
    let config = StreamableHttpServerConfig::default()
        .disable_allowed_hosts()
        .disable_allowed_origins()
        .with_sse_keep_alive(Some(Duration::from_secs(15)));

    let mcp_service: StreamableHttpService<RelayfsServer, LocalSessionManager> =
        StreamableHttpService::new(move || Ok(server.clone()), Default::default(), config);

    let mcp_router = Router::new()
        .fallback_service(mcp_service)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    Router::new()
        .nest("/mcp", mcp_router)
        .route("/healthz", get(|| async { "ok" }))
        .route(
            "/.well-known/oauth-protected-resource",
            get(oauth_protected_resource),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            get(oauth_authorization_server),
        )
        .route(
            "/.well-known/openid-configuration",
            get(oauth_authorization_server),
        )
        .route("/oauth/authorize", get(login_page).post(authorize_submit))
        .route("/login", get(login_page).post(authorize_submit))
        .route("/oauth/token", post(token_endpoint))
        .route("/sse", get(legacy_sse_handler))
        .with_state(state)
}

/// Run the remote HTTP MCP server until the process is terminated.
pub async fn run_http(
    listen: &str,
    base_url: &str,
    token: &str,
    id: &str,
    name: &str,
) -> anyhow::Result<()> {
    info!("connecting to relay {base_url} for HTTP MCP bridge...");
    let client = AgentClient::connect(base_url, token, id, name).await?;
    let client = Arc::new(client);

    let state = HttpState::new(token.to_string());
    let server = RelayfsServer::new(client);
    let app = make_router(state.clone(), server);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!("relayfs remote HTTP MCP server listening on http://{listen}/mcp");
    info!("OAuth login available at http://{listen}/login");

    // Clean up expired auth codes periodically.
    let auth_codes = state.auth_codes.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            let now = Instant::now();
            auth_codes
                .write()
                .await
                .retain(|_, entry| entry.expires_at > now);
        }
    });

    axum::serve(listener, app).await?;
    Ok(())
}

fn add_cors_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, DELETE, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(
            "Authorization, Content-Type, Mcp-Session-Id, Mcp-Protocol-Version, Last-Event-Id, X-Requested-With",
        ),
    );
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("Mcp-Session-Id, Mcp-Protocol-Version"),
    );
}

fn cors_preflight_response() -> Response {
    let mut res = StatusCode::NO_CONTENT.into_response();
    add_cors_headers(res.headers_mut());
    res
}

fn extract_token(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    // 1. Authorization: Bearer <token>
    if let Some(auth) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        if let Some(token) = auth.strip_prefix("Bearer ") {
            let token = token.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }
    // 2. Query param: ?token=<token> or ?access_token=<token>
    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == "token" || k == "access_token" {
                    let decoded = decode_percent(v);
                    if !decoded.is_empty() {
                        return Some(decoded);
                    }
                }
            }
        }
    }
    // 3. Cookie: access_token=<token> or token=<token>
    if let Some(cookie) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) {
        for part in cookie.split(';') {
            let part = part.trim();
            if let Some((k, v)) = part.split_once('=') {
                if k == "access_token" || k == "token" {
                    let v = v.trim();
                    if !v.is_empty() {
                        return Some(v.to_string());
                    }
                }
            }
        }
    }
    None
}

fn decode_percent(input: &str) -> String {
    let mut bytes = Vec::new();
    let mut chars = input.bytes();
    while let Some(b) = chars.next() {
        if b == b'%' {
            let h1 = chars.next();
            let h2 = chars.next();
            if let (Some(h1), Some(h2)) = (h1, h2) {
                if let Ok(hex_str) = std::str::from_utf8(&[h1, h2]) {
                    if let Ok(byte) = u8::from_str_radix(hex_str, 16) {
                        bytes.push(byte);
                        continue;
                    }
                }
            }
        } else if b == b'+' {
            bytes.push(b' ');
        } else {
            bytes.push(b);
        }
    }
    String::from_utf8_lossy(&bytes).to_string()
}

fn unauthorized_response() -> Response {
    let mut res = (
        StatusCode::UNAUTHORIZED,
        [
            (
                header::WWW_AUTHENTICATE,
                "Bearer realm=\"relayfs\", error=\"invalid_token\", resource_metadata=\"/.well-known/oauth-protected-resource\"",
            ),
            (
                header::LINK,
                "</.well-known/oauth-protected-resource>; rel=\"describedby\"",
            ),
            (header::CONTENT_TYPE, "application/json"),
        ],
        serde_json::json!({
            "error": "unauthorized",
            "message": "Valid token required. Pass 'Authorization: Bearer <token>', query '?token=<token>', or visit /login to authenticate.",
            "login_url": "/login"
        })
        .to_string(),
    )
        .into_response();

    add_cors_headers(res.headers_mut());
    res
}

async fn auth_middleware(
    State(state): State<HttpState>,
    req: Request,
    next: Next,
) -> Response {
    if req.method() == Method::OPTIONS {
        return cors_preflight_response();
    }

    if state.token.is_empty() {
        let mut res = next.run(req).await;
        add_cors_headers(res.headers_mut());
        return res;
    }

    if let Some(t) = extract_token(req.headers(), req.uri()) {
        if t == state.token {
            let mut res = next.run(req).await;
            add_cors_headers(res.headers_mut());
            return res;
        }
    }

    unauthorized_response()
}

// RFC 9728 OAuth 2.0 Protected Resource Metadata
async fn oauth_protected_resource() -> impl IntoResponse {
    let mut res = Json(serde_json::json!({
        "resource": "/mcp",
        "authorization_servers": ["/"],
        "scopes_supported": ["mcp:tools"]
    }))
    .into_response();
    add_cors_headers(res.headers_mut());
    res
}

// RFC 8414 OAuth 2.0 Authorization Server Metadata
async fn oauth_authorization_server() -> impl IntoResponse {
    let mut res = Json(serde_json::json!({
        "issuer": "/",
        "authorization_endpoint": "/oauth/authorize",
        "token_endpoint": "/oauth/token",
        "response_types_supported": ["code", "token"],
        "grant_types_supported": ["authorization_code", "client_credentials", "password"],
        "token_endpoint_auth_methods_supported": ["client_secret_post", "client_secret_basic", "none"]
    }))
    .into_response();
    add_cors_headers(res.headers_mut());
    res
}

#[derive(Debug, Deserialize)]
pub struct AuthorizeQuery {
    pub client_id: Option<String>,
    pub redirect_uri: Option<String>,
    pub response_type: Option<String>,
    pub state: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AuthorizeForm {
    pub token: String,
    pub redirect_uri: Option<String>,
    pub state: Option<String>,
    pub response_type: Option<String>,
    pub client_id: Option<String>,
}

async fn login_page(
    State(state): State<HttpState>,
    Query(query): Query<AuthorizeQuery>,
    headers: HeaderMap,
    uri: Uri,
) -> impl IntoResponse {
    // If already authorized:
    let is_authed = if state.token.is_empty() {
        true
    } else if let Some(t) = extract_token(&headers, &uri) {
        t == state.token
    } else {
        false
    };

    let redirect_uri_val = query.redirect_uri.as_deref().unwrap_or("");
    let state_val = query.state.as_deref().unwrap_or("");
    let response_type_val = query.response_type.as_deref().unwrap_or("code");
    let client_id_val = query.client_id.as_deref().unwrap_or("");
    let error_msg = query.error.as_deref().unwrap_or("");

    let error_html = if !error_msg.is_empty() {
        format!(r#"<div class="alert error">{error_msg}</div>"#)
    } else {
        String::new()
    };

    let body = if is_authed {
        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>relayfs - Authorized</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #0f1117; color: #e6edf3; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; }}
    .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 32px; width: 440px; box-shadow: 0 8px 24px rgba(0,0,0,0.5); }}
    h2 {{ margin-top: 0; color: #58a6ff; font-size: 20px; }}
    p {{ font-size: 14px; line-height: 1.5; color: #8b949e; }}
    code {{ background: #21262d; padding: 3px 6px; border-radius: 4px; color: #79c0ff; font-family: ui-monospace, SFMono-Regular, monospace; word-break: break-all; }}
    .btn {{ display: inline-block; background: #238636; color: #fff; padding: 10px 16px; border-radius: 6px; text-decoration: none; font-weight: 500; font-size: 14px; margin-top: 16px; }}
    .btn:hover {{ background: #2ea043; }}
  </style>
</head>
<body>
  <div class="card">
    <h2>✓ Authenticated with RelayFS</h2>
    <p>You are authenticated. Configure your AI client with:</p>
    <p><strong>Endpoint:</strong> <code>/mcp</code></p>
    <p><strong>Authorization Header:</strong> <code>Bearer {}</code></p>
  </div>
</body>
</html>"#,
            if state.token.is_empty() { "(none required)" } else { &state.token }
        )
    } else {
        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>relayfs - Remote MCP Login</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #0d1117; color: #c9d1d9; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; padding: 16px; }}
    .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 32px; width: 100%; max-width: 420px; box-shadow: 0 12px 28px rgba(0,0,0,0.6); }}
    .logo {{ display: flex; align-items: center; gap: 10px; margin-bottom: 20px; }}
    .logo-badge {{ background: #1f6feb; color: #fff; font-weight: 700; padding: 4px 10px; border-radius: 6px; font-size: 13px; }}
    h1 {{ font-size: 18px; margin: 0; color: #f0f6fc; }}
    p.desc {{ font-size: 13px; color: #8b949e; margin-top: 4px; margin-bottom: 20px; line-height: 1.4; }}
    .alert.error {{ background: rgba(248,81,73,0.15); border: 1px solid #f85149; color: #ff7b72; padding: 10px 12px; border-radius: 6px; font-size: 13px; margin-bottom: 16px; }}
    label {{ display: block; font-size: 12px; font-weight: 600; color: #8b949e; text-transform: uppercase; margin-bottom: 6px; }}
    input[type="password"], input[type="text"] {{ width: 100%; padding: 10px 12px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; color: #c9d1d9; font-size: 14px; outline: none; margin-bottom: 18px; }}
    input[type="password"]:focus {{ border-color: #58a6ff; box-shadow: 0 0 0 3px rgba(88,166,255,0.2); }}
    button {{ width: 100%; background: #238636; color: #ffffff; border: none; padding: 10px 16px; font-size: 14px; font-weight: 600; border-radius: 6px; cursor: pointer; transition: background 0.15s; }}
    button:hover {{ background: #2ea043; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">
      <span class="logo-badge">relayfs</span>
      <h1>Remote MCP Authentication</h1>
    </div>
    <p class="desc">Enter your pairing token (<code>RELAYFS_TOKEN</code>) to authorize remote MCP tool execution.</p>
    {error_html}
    <form method="POST" action="/oauth/authorize">
      <input type="hidden" name="redirect_uri" value="{redirect_uri_val}">
      <input type="hidden" name="state" value="{state_val}">
      <input type="hidden" name="response_type" value="{response_type_val}">
      <input type="hidden" name="client_id" value="{client_id_val}">
      <label for="token">Pairing Token</label>
      <input type="password" id="token" name="token" placeholder="••••••••••••" autofocus required>
      <button type="submit">Log in & Authorize</button>
    </form>
  </div>
</body>
</html>"#
        )
    };

    Html(body)
}

async fn authorize_submit(
    State(state): State<HttpState>,
    Form(form): Form<AuthorizeForm>,
) -> Response {
    if !state.token.is_empty() && form.token != state.token {
        return Redirect::to(&format!(
            "/oauth/authorize?error=Invalid%20pairing%20token&redirect_uri={}&state={}",
            form.redirect_uri.as_deref().unwrap_or(""),
            form.state.as_deref().unwrap_or("")
        ))
        .into_response();
    }

    // If client supplied a redirect_uri (standard OAuth 2.0 flow):
    if let Some(redirect_uri) = form.redirect_uri {
        if !redirect_uri.is_empty() {
            let code = format!("{:016x}{:016x}", rand::random::<u64>(), rand::random::<u64>());
            state.auth_codes.write().await.insert(
                code.clone(),
                AuthCodeEntry {
                    token: form.token.clone(),
                    expires_at: Instant::now() + Duration::from_secs(600),
                    redirect_uri: Some(redirect_uri.clone()),
                },
            );

            let separator = if redirect_uri.contains('?') { "&" } else { "?" };
            let state_param = form
                .state
                .map(|s| format!("&state={s}"))
                .unwrap_or_default();
            return Redirect::to(&format!("{redirect_uri}{separator}code={code}{state_param}"))
                .into_response();
        }
    }

    // If direct login in browser: set cookie and render success
    let cookie_header = format!(
        "access_token={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400",
        form.token
    );
    let mut res = Html(format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>relayfs - Login Successful</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background: #0d1117; color: #c9d1d9; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; }}
    .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 10px; padding: 32px; width: 480px; box-shadow: 0 12px 28px rgba(0,0,0,0.6); }}
    h2 {{ color: #3fb950; margin-top: 0; }}
    code {{ background: #0d1117; padding: 4px 8px; border-radius: 4px; color: #58a6ff; font-family: monospace; word-break: break-all; }}
    p {{ font-size: 14px; line-height: 1.5; color: #8b949e; }}
  </style>
</head>
<body>
  <div class="card">
    <h2>✓ Login Successful</h2>
    <p>You have authorized this remote MCP server. You can now use the following configuration in your client:</p>
    <p><strong>URL:</strong> <code>http://&lt;this-host&gt;/mcp</code></p>
    <p><strong>Bearer Token:</strong> <code>{}</code></p>
  </div>
</body>
</html>"#,
        form.token
    ))
    .into_response();

    res.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie_header).unwrap_or(HeaderValue::from_static("")),
    );
    res
}

#[derive(Debug, Default, Deserialize)]
pub struct TokenRequest {
    pub grant_type: Option<String>,
    pub code: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
}

async fn token_endpoint(
    State(state): State<HttpState>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let req: TokenRequest = serde_json::from_slice(&body).unwrap_or_else(|_| {
        let text = std::str::from_utf8(&body).unwrap_or("");
        let mut tr = TokenRequest::default();
        for pair in text.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                let val = decode_percent(v);
                match k {
                    "grant_type" => tr.grant_type = Some(val),
                    "code" => tr.code = Some(val),
                    "client_id" => tr.client_id = Some(val),
                    "client_secret" => tr.client_secret = Some(val),
                    "password" => tr.password = Some(val),
                    "token" => tr.token = Some(val),
                    _ => {}
                }
            }
        }
        tr
    });

    let grant = req.grant_type.as_deref().unwrap_or("client_credentials");
    let mut valid = false;

    if grant == "authorization_code" {
        if let Some(code) = req.code.as_deref() {
            let mut codes = state.auth_codes.write().await;
            if let Some(entry) = codes.remove(code) {
                if entry.expires_at > Instant::now() {
                    valid = true;
                }
            }
        }
    } else {
        // Extract client_secret from Basic auth or JSON body
        let secret = req
            .client_secret
            .or(req.password)
            .or(req.token)
            .or_else(|| {
                headers
                    .get(header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|auth| auth.strip_prefix("Bearer "))
                    .map(|s| s.trim().to_string())
            });

        if state.token.is_empty() {
            valid = true;
        } else if let Some(secret) = secret {
            valid = secret == state.token;
        }
    }

    if valid {
        let mut res = Json(serde_json::json!({
            "access_token": state.token,
            "token_type": "Bearer",
            "expires_in": 86400
        }))
        .into_response();
        add_cors_headers(res.headers_mut());
        res
    } else {
        let mut res = (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "invalid_grant",
                "error_description": "Invalid credentials or authorization code"
            })),
        )
            .into_response();
        add_cors_headers(res.headers_mut());
        res
    }
}

// Legacy SSE transport: returns an event pointing to the /mcp endpoint
async fn legacy_sse_handler(
    State(state): State<HttpState>,
    headers: HeaderMap,
    uri: Uri,
) -> Response {
    if !state.token.is_empty() {
        let authed = extract_token(&headers, &uri).map_or(false, |t| t == state.token);
        if !authed {
            return unauthorized_response();
        }
    }

    let token_param = if state.token.is_empty() {
        String::new()
    } else {
        format!("?token={}", state.token)
    };

    let initial = futures::stream::once(async move {
        Ok::<_, std::convert::Infallible>(axum::body::Bytes::from(format!(
            "event: endpoint\ndata: /mcp{token_param}\n\n"
        )))
    });
    let pings = futures::stream::unfold((), |_| async {
        tokio::time::sleep(Duration::from_secs(15)).await;
        Some((
            Ok::<_, std::convert::Infallible>(axum::body::Bytes::from(": ping\n\n")),
            (),
        ))
    });
    use futures::StreamExt;
    let stream = initial.chain(pings);

    let mut res = (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/event-stream"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::CONNECTION, "keep-alive"),
        ],
        Body::from_stream(stream),
    )
        .into_response();

    add_cors_headers(res.headers_mut());
    res
}
