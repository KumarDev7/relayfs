use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TOKEN: &str = "secret-pairing-token";

async fn http_request(
    port: u16,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, HashMap<String, String>, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).await.unwrap();

    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n");
    let mut has_content_length = false;
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
        if k.eq_ignore_ascii_case("content-length") {
            has_content_length = true;
        }
    }
    if !has_content_length && !body.is_empty() {
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");
    req.push_str(body);

    stream.write_all(req.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();

    let mut response_bytes = Vec::new();
    stream.read_to_end(&mut response_bytes).await.unwrap();
    let response_str = String::from_utf8_lossy(&response_bytes);

    let (head, body) = response_str.split_once("\r\n\r\n").unwrap_or(("", ""));
    let mut lines = head.lines();
    let status_line = lines.next().unwrap_or("");
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);

    let mut resp_headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            resp_headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }

    (status_code, resp_headers, body.to_string())
}

fn parse_mcp_response(body: &str) -> serde_json::Value {
    // Check if it's SSE data lines
    for line in body.lines() {
        let line = line.trim();
        if let Some(json_text) = line.strip_prefix("data:") {
            let json_text = json_text.trim();
            if !json_text.is_empty() {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(json_text) {
                    return val;
                }
            }
        }
    }
    // Otherwise direct JSON
    serde_json::from_str(body.trim()).expect("valid JSON response")
}

#[tokio::test]
async fn test_remote_http_mcp_and_oauth() {
    // 1. Start relay on ephemeral port
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_port = relay_listener.local_addr().unwrap().port();
    drop(relay_listener);

    tokio::spawn(async move {
        relayfs_relay::run(&format!("127.0.0.1:{relay_port}"), Some(TOKEN))
            .await
            .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 2. Start target agent connecting to the relay
    tokio::spawn(async move {
        relayfs_agent::run(
            &format!("ws://127.0.0.1:{relay_port}"),
            TOKEN,
            "test-agent",
            "remote-box",
            1,
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. Start remote HTTP MCP server on ephemeral port
    let http_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = http_listener.local_addr().unwrap().port();
    drop(http_listener);

    tokio::spawn(async move {
        relayfs_bridge::run_http(
            &format!("127.0.0.1:{http_port}"),
            &format!("ws://127.0.0.1:{relay_port}"),
            TOKEN,
            "http-bridge",
            "http-mcp",
        )
        .await
        .unwrap();
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    // 4. Test /healthz
    let (status, _, body) = http_request(http_port, "GET", "/healthz", &[], "").await;
    assert_eq!(status, 200);
    assert_eq!(body, "ok");

    // 5. Test OAuth Protected Resource Metadata (RFC 9728)
    let (status, _, body) = http_request(
        http_port,
        "GET",
        "/.well-known/oauth-protected-resource",
        &[],
        "",
    )
    .await;
    assert_eq!(status, 200);
    let meta: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(meta["resource"], "/mcp");

    // 6. Test OAuth Authorization Server Metadata (RFC 8414)
    let (status, _, body) = http_request(
        http_port,
        "GET",
        "/.well-known/oauth-authorization-server",
        &[],
        "",
    )
    .await;
    assert_eq!(status, 200);
    let auth_meta: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(auth_meta["authorization_endpoint"], "/oauth/authorize");
    assert_eq!(auth_meta["token_endpoint"], "/oauth/token");

    // 7. Test /login page
    let (status, _, body) = http_request(http_port, "GET", "/login", &[], "").await;
    assert_eq!(status, 200);
    assert!(body.contains("Remote MCP Authentication"));
    assert!(body.contains("form method=\"POST\" action=\"/oauth/authorize\""));

    // 8. Test /oauth/authorize with invalid token
    let form_data = "token=wrong-token&redirect_uri=http://client.local/callback&state=xyz123";
    let (status, headers, _) = http_request(
        http_port,
        "POST",
        "/oauth/authorize",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        form_data,
    )
    .await;
    assert!(status == 302 || status == 303);
    let location = headers.get("location").cloned().unwrap_or_default();
    assert!(location.contains("Invalid%20pairing%20token"));

    // 9. Test /oauth/authorize with valid token -> redirect with code
    let valid_form = format!("token={TOKEN}&redirect_uri=http://client.local/callback&state=xyz123");
    let (status, headers, _) = http_request(
        http_port,
        "POST",
        "/oauth/authorize",
        &[("Content-Type", "application/x-www-form-urlencoded")],
        &valid_form,
    )
    .await;
    assert!(status == 302 || status == 303);
    let location = headers.get("location").cloned().unwrap_or_default();
    assert!(location.starts_with("http://client.local/callback?code="));
    assert!(location.contains("&state=xyz123"));

    // Extract authorization code
    let code = location
        .split("code=")
        .nth(1)
        .unwrap()
        .split('&')
        .next()
        .unwrap();

    // 10. Exchange authorization code for access token at /oauth/token
    let token_req = serde_json::json!({
        "grant_type": "authorization_code",
        "code": code
    });
    let (status, _, body) = http_request(
        http_port,
        "POST",
        "/oauth/token",
        &[("Content-Type", "application/json")],
        &token_req.to_string(),
    )
    .await;
    assert_eq!(status, 200);
    let token_resp: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(token_resp["access_token"], TOKEN);
    assert_eq!(token_resp["token_type"], "Bearer");

    // 11. Client credentials grant at /oauth/token
    let creds_req = serde_json::json!({
        "grant_type": "client_credentials",
        "client_secret": TOKEN
    });
    let (status, _, _body) = http_request(
        http_port,
        "POST",
        "/oauth/token",
        &[("Content-Type", "application/json")],
        &creds_req.to_string(),
    )
    .await;
    assert_eq!(status, 200);

    // 12. Test unauthenticated request to /mcp -> 401 Unauthorized
    let (status, headers, body) = http_request(http_port, "POST", "/mcp", &[], "{}").await;
    assert_eq!(status, 401);
    let auth_header = headers.get("www-authenticate").cloned().unwrap_or_default();
    assert!(auth_header.contains("Bearer realm=\"relayfs\""));
    assert!(body.contains("unauthorized"));

    // 13. Test authenticated MCP request to /mcp with Bearer token
    let init_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "test-cursor",
                "version": "1.0.0"
            }
        }
    });
    let (status, init_headers, body) = http_request(
        http_port,
        "POST",
        "/mcp",
        &[
            ("Authorization", &format!("Bearer {TOKEN}")),
            ("Content-Type", "application/json"),
            ("Accept", "application/json, text/event-stream"),
        ],
        &init_req.to_string(),
    )
    .await;
    assert_eq!(status, 200);
    let init_resp = parse_mcp_response(&body);
    assert_eq!(init_resp["id"], 1);
    assert!(init_resp["result"]["capabilities"]["tools"].is_object());

    let session_id = init_headers.get("mcp-session-id").cloned().unwrap_or_default();

    // Send notifications/initialized
    let notify_req = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "notifications/initialized"
    });
    let _ = http_request(
        http_port,
        "POST",
        "/mcp",
        &[
            ("Authorization", &format!("Bearer {TOKEN}")),
            ("Mcp-Session-Id", &session_id),
            ("Content-Type", "application/json"),
            ("Accept", "application/json, text/event-stream"),
        ],
        &notify_req.to_string(),
    )
    .await;

    // 14. Test query param authentication ?token=...
    let list_tools_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/list",
        "params": {}
    });
    let (status, _, body) = http_request(
        http_port,
        "POST",
        &format!("/mcp?token={TOKEN}"),
        &[
            ("Mcp-Session-Id", &session_id),
            ("Content-Type", "application/json"),
            ("Accept", "application/json, text/event-stream"),
        ],
        &list_tools_req.to_string(),
    )
    .await;
    assert_eq!(status, 200);
    let tools_resp = parse_mcp_response(&body);
    assert_eq!(tools_resp["id"], 2);
    let tools = tools_resp["result"]["tools"].as_array().unwrap();
    let tool_names: Vec<&str> = tools
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(tool_names.contains(&"run_command"));
    assert!(tool_names.contains(&"read_file"));
    assert!(tool_names.contains(&"write_file"));
    assert!(tool_names.contains(&"ping"));

    // 15. Test tools/call execution over HTTP MCP (calling ping)
    let call_ping_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "ping",
            "arguments": {}
        }
    });
    let (status, _, body) = http_request(
        http_port,
        "POST",
        "/mcp",
        &[
            ("Authorization", &format!("Bearer {TOKEN}")),
            ("Mcp-Session-Id", &session_id),
            ("Content-Type", "application/json"),
            ("Accept", "application/json, text/event-stream"),
        ],
        &call_ping_req.to_string(),
    )
    .await;
    assert_eq!(status, 200);
    let ping_resp = parse_mcp_response(&body);
    assert_eq!(ping_resp["id"], 3);
    assert_eq!(ping_resp["result"]["isError"], false);
    let content = ping_resp["result"]["content"].as_array().unwrap();
    let text = content[0]["text"].as_str().unwrap();
    assert!(text.contains("\"ok\":true"));
    assert!(text.contains("pid"));
}
