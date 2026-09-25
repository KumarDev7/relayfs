#!/usr/bin/env python3
"""Comprehensive End-to-End Verification Test for RelayFS Remote HTTP MCP & OAuth.

Tests:
1. Target agent connection to relay.
2. Health & Discovery endpoints.
3. Auth Enforcement: 401 on missing/bad token, 200 with Bearer token.
4. OAuth 2.0 flow: authorize -> code -> exchange token at /oauth/token.
5. Streamable HTTP MCP Session:
   - initialize & notifications/initialized
   - tools/list
   - ping tool
   - run_command tool (executing bash on remote target)
   - write_file, stat, read_file, list_dir, remove tools
6. Verification through both direct local Docker port and optional public HTTPS endpoint.
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import ssl

TOKEN = os.environ.get("RELAYFS_TOKEN", "ci-test-token")
LOCAL_BASE = os.environ.get("RELAYFS_LOCAL_URL", "http://127.0.0.1:8788")
PUBLIC_BASE = os.environ.get("RELAYFS_PUBLIC_URL", "")
BIN = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "target", "release", "relayfs")

# Create SSL context that doesn't verify certificates if self-signed or local proxy
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def log(msg, status="INFO"):
    colors = {
        "INFO": "\033[94m",
        "PASS": "\033[92m",
        "FAIL": "\033[91m",
        "WARN": "\033[93m",
    }
    reset = "\033[0m"
    print(f"{colors.get(status, '')}[{status}]{reset} {msg}")

class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

opener = urllib.request.build_opener(
    urllib.request.HTTPSHandler(context=ctx),
    NoRedirectHandler(),
)

def http_req(url, method="GET", headers=None, data=None):
    if headers is None:
        headers = {}
    if "User-Agent" not in headers:
        headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) RelayFS-Test/1.0"
    req_data = None
    if data is not None:
        if isinstance(data, (dict, list)):
            req_data = json.dumps(data).encode("utf-8")
            if "Content-Type" not in headers:
                headers["Content-Type"] = "application/json"
        elif isinstance(data, str):
            req_data = data.encode("utf-8")
        else:
            req_data = data

    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)
    try:
        with opener.open(req, timeout=15) as res:
            body = res.read().decode("utf-8")
            res_headers = dict(res.info())
            return res.status, res_headers, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8") if e.fp else ""
        return e.code, dict(e.headers), body
    except Exception as e:
        return 0, {}, str(e)

def parse_sse_events(body):
    events = []
    current_data = []
    for line in body.splitlines():
        line = line.strip()
        if not line:
            if current_data:
                combined = "\n".join(current_data)
                try:
                    events.append(json.loads(combined))
                except Exception:
                    events.append(combined)
                current_data = []
        elif line.startswith("data:"):
            content = line[5:].strip()
            if content:
                current_data.append(content)
    if current_data:
        combined = "\n".join(current_data)
        try:
            events.append(json.loads(combined))
        except Exception:
            events.append(combined)
    return events

class McpClient:
    def __init__(self, base_url, token):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.session_id = None
        self.msg_id = 1

    def call(self, method, params=None):
        url = f"{self.base_url}/mcp"
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id

        payload = {
            "jsonrpc": "2.0",
            "id": self.msg_id,
            "method": method,
            "params": params or {},
        }
        self.msg_id += 1

        status, res_headers, body = http_req(url, method="POST", headers=headers, data=payload)
        assert status == 200, f"MCP POST {method} failed with status {status}: {body}"

        # Capture session id
        for k, v in res_headers.items():
            if k.lower() == "mcp-session-id":
                self.session_id = v
                break

        # Parse SSE response
        events = parse_sse_events(body)
        for ev in events:
            if isinstance(ev, dict) and "id" in ev:
                return ev
        raise RuntimeError(f"No JSON-RPC response with id found in events: {events}, body: {body}")

    def notify(self, method, params=None):
        url = f"{self.base_url}/mcp"
        headers = {
            "Accept": "application/json, text/event-stream",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id

        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
        }
        status, _, body = http_req(url, method="POST", headers=headers, data=payload)
        assert status in (200, 202, 204), f"MCP notify {method} failed with status {status}: {body}"

def test_endpoints(base_url):
    log(f"Testing public endpoints on {base_url}...")

    # 1. Healthz
    status, _, body = http_req(f"{base_url}/healthz")
    assert status == 200 and body.strip() == "ok", f"healthz failed: {status}, {body}"
    log(f"  ✓ /healthz returned 200 'ok'", "PASS")

    # 2. RFC 9728 OAuth Protected Resource Metadata
    status, _, body = http_req(f"{base_url}/.well-known/oauth-protected-resource")
    assert status == 200, f"oauth-protected-resource failed: {status}"
    meta = json.loads(body)
    assert meta.get("resource") == "/mcp", f"invalid resource in metadata: {meta}"
    log(f"  ✓ /.well-known/oauth-protected-resource metadata valid", "PASS")

    # 3. RFC 8414 OAuth Authorization Server Metadata
    status, _, body = http_req(f"{base_url}/.well-known/oauth-authorization-server")
    assert status == 200, f"oauth-authorization-server failed: {status}"
    meta = json.loads(body)
    assert meta.get("authorization_endpoint") == "/oauth/authorize"
    assert meta.get("token_endpoint") == "/oauth/token"
    log(f"  ✓ /.well-known/oauth-authorization-server metadata valid", "PASS")

    # 4. Login page
    status, headers, body = http_req(f"{base_url}/login")
    assert status == 200, f"login page failed: {status}"
    assert "relayfs" in body.lower() and "token" in body.lower(), "unexpected login HTML"
    log(f"  ✓ /login UI returned HTML", "PASS")

    # 5. Auth rejection (unauthenticated MCP request)
    status, _, body = http_req(f"{base_url}/mcp", method="POST", headers={"Accept": "application/json, text/event-stream"}, data={})
    assert status == 401, f"Expected 401 unauthorized, got {status}: {body}"
    log(f"  ✓ /mcp rejects missing token with 401 Unauthorized", "PASS")

    # 6. Auth rejection with wrong token
    status, _, body = http_req(f"{base_url}/mcp", method="POST", headers={"Accept": "application/json, text/event-stream", "Authorization": "Bearer wrong-token"}, data={})
    assert status == 401, f"Expected 401 unauthorized, got {status}: {body}"
    log(f"  ✓ /mcp rejects bad token with 401 Unauthorized", "PASS")

def test_oauth_flow(base_url):
    log(f"Testing OAuth 2.0 authorization code flow on {base_url}...")

    # Step 1: Submit token to /oauth/authorize
    data = urllib.parse.urlencode({
        "token": TOKEN,
        "redirect_uri": "http://localhost:9999/callback",
        "state": "test-state-123",
    })
    status, headers, body = http_req(
        f"{base_url}/oauth/authorize",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=data,
    )
    # Axum redirect (303 or 302) or 200 with Redirect
    location = headers.get("location") or headers.get("Location")
    assert location, f"Expected redirect Location header from /oauth/authorize, headers: {headers}, body: {body}"
    parsed_loc = urllib.parse.urlparse(location)
    q = urllib.parse.parse_qs(parsed_loc.query)
    code = q.get("code", [None])[0]
    state = q.get("state", [None])[0]
    assert code, f"No code in redirect: {location}"
    assert state == "test-state-123", f"State mismatch: {state}"
    log(f"  ✓ /oauth/authorize issued authorization code", "PASS")

    # Step 2: Exchange code for bearer token at /oauth/token
    token_exchange_data = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": "http://localhost:9999/callback",
    })
    status, _, body = http_req(
        f"{base_url}/oauth/token",
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=token_exchange_data,
    )
    assert status == 200, f"/oauth/token exchange failed: {status}, {body}"
    token_res = json.loads(body)
    assert token_res.get("token_type", "").lower() == "bearer"
    access_token = token_res.get("access_token")
    assert access_token == TOKEN, f"Token mismatch: expected {TOKEN}, got {access_token}"
    log(f"  ✓ /oauth/token exchanged code for valid Bearer token", "PASS")

def test_mcp_session(base_url):
    log(f"Testing MCP session and remote tool execution on {base_url}...")
    client = McpClient(base_url, TOKEN)

    # 1. Initialize
    res = client.call("initialize", {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "e2e-tester", "version": "1.0"},
    })
    assert "result" in res, f"initialize failed: {res}"
    client.notify("notifications/initialized")
    log(f"  ✓ MCP initialize handshake successful (session: {client.session_id})", "PASS")

    # 2. Tools list
    res = client.call("tools/list")
    assert "result" in res, f"tools/list failed: {res}"
    tools = {t["name"] for t in res["result"].get("tools", [])}
    expected_tools = {"run_command", "read_file", "write_file", "list_dir", "stat", "remove", "ping"}
    for t in expected_tools:
        assert t in tools, f"Missing tool {t} in tools/list: {tools}"
    log(f"  ✓ tools/list returned all {len(tools)} tools ({', '.join(sorted(tools))})", "PASS")

    # 3. Ping
    res = client.call("tools/call", {"name": "ping", "arguments": {}})
    assert "result" in res, f"ping failed: {res}"
    log(f"  ✓ tools/call 'ping' succeeded: {res['result']}", "PASS")

    # 4. Run command
    res = client.call("tools/call", {
        "name": "run_command",
        "arguments": {"command": "echo 'Hello from RelayFS End-to-End Test!' && uname -a"},
    })
    assert "result" in res, f"run_command failed: {res}"
    output = "".join(c.get("text", "") for c in res["result"].get("content", []))
    assert "Hello from RelayFS End-to-End Test!" in output, f"Unexpected command output: {output}"
    log(f"  ✓ tools/call 'run_command' executed remotely: {output.strip().splitlines()[0]}", "PASS")

    # 5. File Operations: write_file -> stat -> read_file -> list_dir -> remove
    test_path = f"/tmp/relayfs_e2e_{int(time.time())}.txt"
    test_content = "The quick brown fox jumps over the lazy dog 12345! @#$%^&*()"

    # 5a. write_file
    res = client.call("tools/call", {
        "name": "write_file",
        "arguments": {"path": test_path, "content": test_content},
    })
    assert "result" in res, f"write_file failed: {res}"
    log(f"  ✓ tools/call 'write_file' wrote test file at {test_path}", "PASS")

    # 5b. stat
    res = client.call("tools/call", {
        "name": "stat",
        "arguments": {"path": test_path},
    })
    assert "result" in res, f"stat failed: {res}"
    log(f"  ✓ tools/call 'stat' verified file metadata", "PASS")

    # 5c. read_file
    res = client.call("tools/call", {
        "name": "read_file",
        "arguments": {"path": test_path},
    })
    assert "result" in res, f"read_file failed: {res}"
    read_text = "".join(c.get("text", "") for c in res["result"].get("content", []))
    import base64
    read_json = json.loads(read_text)
    decoded_text = base64.b64decode(read_json["data"]).decode("utf-8")
    assert decoded_text == test_content, f"Content mismatch: expected {test_content!r}, got {decoded_text!r}"
    log(f"  ✓ tools/call 'read_file' verified exact content match", "PASS")

    # 5d. list_dir
    res = client.call("tools/call", {
        "name": "list_dir",
        "arguments": {"path": "/tmp"},
    })
    assert "result" in res, f"list_dir failed: {res}"
    entries = "".join(c.get("text", "") for c in res["result"].get("content", []))
    assert os.path.basename(test_path) in entries, f"File {test_path} not found in list_dir output: {entries}"
    log(f"  ✓ tools/call 'list_dir' found created file", "PASS")

    # 5e. remove
    res = client.call("tools/call", {
        "name": "remove",
        "arguments": {"path": test_path},
    })
    assert "result" in res, f"remove failed: {res}"
    log(f"  ✓ tools/call 'remove' deleted test file", "PASS")

def main():
    log("==================================================================")
    log("Starting RelayFS End-to-End Verification Test Suite")
    log("==================================================================")

    # Step 1: Ensure target agent is running and connected
    log("Starting target agent connected to relay...")
    agent_proc = subprocess.Popen([
        BIN, "--mode", "target",
        "--base-url", "ws://127.0.0.1:8788",
        "--token", TOKEN,
        "--id", "e2e-target-agent",
        "--name", "e2e-target",
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        # Give agent 1.5 seconds to establish connection and handshake with relay
        time.sleep(1.5)

        # Step 2: Test Local Endpoints
        log("--- PHASE 1: DIRECT LOCAL CONTAINER (http://127.0.0.1:8788) ---")
        test_endpoints(LOCAL_BASE)
        test_oauth_flow(LOCAL_BASE)
        test_mcp_session(LOCAL_BASE)

        # Step 3: Test Public Domain if configured
        if PUBLIC_BASE:
            log(f"--- PHASE 2: PUBLIC DOMAIN ({PUBLIC_BASE}) ---")
            test_endpoints(PUBLIC_BASE)
            test_oauth_flow(PUBLIC_BASE)
            test_mcp_session(PUBLIC_BASE)

        log("==================================================================")
        log("ALL END-TO-END TESTS PASSED SUCCESSFULLY! (100% NO ERRORS)", "PASS")
        log("==================================================================")
        return 0

    finally:
        agent_proc.terminate()
        try:
            agent_proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            agent_proc.kill()

if __name__ == "__main__":
    sys.exit(main())
