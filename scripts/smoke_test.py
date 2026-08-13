#!/usr/bin/env python3
"""End-to-end smoke test: spawns server + target + bridge, exercises the MCP tools.

Usage: python3 scripts/smoke_test.py [path-to-relayfs-binary]
Default binary: target/release/relayfs (must be built first).
"""
import json
import os
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import time

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "target", "release", "relayfs"
)
TOKEN = "ci-token"
PORT = 18787  # fixed; CI runners are isolated


def wait_port(port, timeout=10):
    import socket
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"port {port} never opened")


def spawn(args):
    return subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def main():
    if not os.path.exists(BIN):
        print(f"binary not found: {BIN} (run `cargo build --release` first)")
        return 1

    procs = []
    try:
        # 1. server
        server = spawn([BIN, "--mode", "server", "--listen", f"127.0.0.1:{PORT}"])
        procs.append(server)
        wait_port(PORT)

        # 2. target
        target = spawn([BIN, "--mode", "target", "--base-url", f"ws://127.0.0.1:{PORT}",
                        "--token", TOKEN, "--id", "ci-agent"])
        procs.append(target)
        time.sleep(1.0)  # handshake

        # 3. bridge (MCP over stdio)
        bridge = subprocess.Popen(
            [BIN, "--mode", "mcp", "--base-url", f"ws://127.0.0.1:{PORT}",
             "--token", TOKEN, "--id", "ci-bridge"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        procs.append(bridge)

        def send(obj):
            bridge.stdin.write((json.dumps(obj) + "\n").encode())
            bridge.stdin.flush()

        def recv(timeout=20):
            r, _, _ = select.select([bridge.stdout], [], [], timeout)
            if not r:
                raise TimeoutError("no response from bridge")
            return json.loads(bridge.stdout.readline())

        def call(method, params=None, timeout=20):
            send({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}})
            return recv(timeout)

        # MCP handshake
        r = call("initialize", {"protocolVersion": "2025-11-25", "capabilities": {},
                                "clientInfo": {"name": "smoke", "version": "0.1"}})
        assert "result" in r, f"initialize failed: {r}"
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        # tools/list
        r = call("tools/list")
        tools = [t["name"] for t in r["result"]["tools"]]
        assert "run_command" in tools and "mount_remote" in tools and "ping" in tools, tools

        # ping
        r = call("tools/call", {"name": "ping", "arguments": {}})
        assert "ok" in r["result"]["content"][0]["text"], r

        # run_command
        r = call("tools/call", {"name": "run_command",
                               "arguments": {"command": "echo ci-ok && uname -s"}})
        out = r["result"]["content"][0]["text"]
        assert "ci-ok" in out and "Linux" in out, out

        # stdin input
        r = call("tools/call", {"name": "run_command",
                               "arguments": {"command": "read name && echo \"hi $name\"",
                                             "input": "ci\n"}})
        assert "hi ci" in r["result"]["content"][0]["text"], r

        # timeout
        t0 = time.time()
        r = call("tools/call", {"name": "run_command",
                               "arguments": {"command": "sleep 30", "timeout_secs": 2}})
        elapsed = time.time() - t0
        assert "timed out" in r["result"]["content"][0]["text"], r
        assert elapsed < 10, f"timeout took {elapsed:.1f}s"

        # file ops in a temp dir
        work = tempfile.mkdtemp(prefix="relayfs-ci-")
        f = os.path.join(work, "hello.txt")
        with open(f, "w") as fh:
            fh.write("hello ci")
        r = call("tools/call", {"name": "read_file", "arguments": {"path": f}})
        assert "aGVsbG8gY2k=" in r["result"]["content"][0]["text"], r  # base64 "hello ci"

        r = call("tools/call", {"name": "list_dir", "arguments": {"path": work}})
        assert "hello.txt" in r["result"]["content"][0]["text"], r

        r = call("tools/call", {"name": "stat", "arguments": {"path": f}})
        assert '"size":8' in r["result"]["content"][0]["text"], r

        w = os.path.join(work, "written.txt")
        r = call("tools/call", {"name": "write_file",
                               "arguments": {"path": w, "content": "written by ci"}})
        assert "wrote" in r["result"]["content"][0]["text"], r
        with open(w) as fh:
            assert fh.read() == "written by ci"

        d = os.path.join(work, "newdir")
        r = call("tools/call", {"name": "mkdir", "arguments": {"path": d}})
        assert os.path.isdir(d)
        r = call("tools/call", {"name": "remove", "arguments": {"path": d}})
        assert not os.path.exists(d)

        r = call("tools/call", {"name": "rename",
                               "arguments": {"from": w, "to": os.path.join(work, "renamed.txt")}})
        assert os.path.exists(os.path.join(work, "renamed.txt"))
        r = call("tools/call", {"name": "copy",
                               "arguments": {"from": os.path.join(work, "renamed.txt"),
                                            "to": os.path.join(work, "copied.txt")}})
        assert os.path.exists(os.path.join(work, "copied.txt"))

        r = call("tools/call", {"name": "list_mounts", "arguments": {}})
        assert "no mounts" in r["result"]["content"][0]["text"], r

        print("ALL SMOKE TESTS PASSED")
        return 0
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    sys.exit(main())
