#!/usr/bin/env python3
"""End-to-End Multi-Target Concurrency Verification Script.

Tests that when multiple target agents connect with the SAME TOKEN and SAME ID:
1. Neither target is displaced; both stay alive and connected simultaneously.
2. The second target receives a unique suffixed ID (e.g. test-node-<timestamp>).
3. Both targets are listed in list_targets (and list_devices).
4. Commands can be routed to each specific target using the target parameter.
5. Disconnecting one target leaves the other target active and operational.
"""
import itertools
import json
import os
import select
import signal
import subprocess
import sys
import time

BIN = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "target", "release", "relayfs")
PORT = 19888
TOKEN = "shared-secret-123"

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

def main():
    if not os.path.exists(BIN):
        print(f"Error: binary not found at {BIN}")
        return 1

    procs = []

    try:
        print("[1/6] Starting RelayFS Server...")
        server = subprocess.Popen(
            [BIN, "--mode", "server", "--listen", f"127.0.0.1:{PORT}", "--token", TOKEN],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        procs.append(server)
        wait_port(PORT)
        print("  -> Server is running and listening on port", PORT)

        print("[2/6] Starting Target 1 (ID: 'test-node')...")
        target1 = subprocess.Popen(
            [BIN, "--mode", "target", "--base-url", f"ws://127.0.0.1:{PORT}",
             "--token", TOKEN, "--id", "test-node", "--name", "node-alpha"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        procs.append(target1)
        time.sleep(1.0)
        assert target1.poll() is None, "Target 1 died unexpectedly"
        print("  -> Target 1 connected and running.")

        print("[3/6] Starting Target 2 (ID: 'test-node' - SAME ID and SAME TOKEN)...")
        target2 = subprocess.Popen(
            [BIN, "--mode", "target", "--base-url", f"ws://127.0.0.1:{PORT}",
             "--token", TOKEN, "--id", "test-node", "--name", "node-beta"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        procs.append(target2)
        time.sleep(1.5)

        # CRITICAL CHECK: Ensure neither target was displaced or killed!
        assert target1.poll() is None, "Target 1 was displaced/killed by Target 2!"
        assert target2.poll() is None, "Target 2 crashed or died on connect!"
        print("  -> VERIFIED: Both Target 1 and Target 2 are alive concurrently!")

        print("[4/6] Connecting MCP Bridge over stdio...")
        bridge = subprocess.Popen(
            [BIN, "--mode", "mcp", "--base-url", f"ws://127.0.0.1:{PORT}",
             "--token", TOKEN, "--id", "test-bridge"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        procs.append(bridge)

        req_id = itertools.count(1)

        def send(msg):
            bridge.stdin.write((json.dumps(msg) + "\n").encode())
            bridge.stdin.flush()

        def recv(timeout=10):
            r, _, _ = select.select([bridge.stdout], [], [], timeout)
            if not r:
                raise TimeoutError("Bridge timeout")
            line = bridge.stdout.readline()
            return json.loads(line)

        def call_tool(name, arguments, timeout=10):
            rid = next(req_id)
            send({
                "jsonrpc": "2.0",
                "id": rid,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            })
            while True:
                msg = recv(timeout)
                if msg.get("id") == rid:
                    return msg

        # Initialize MCP
        rid = next(req_id)
        send({
            "jsonrpc": "2.0",
            "id": rid,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "test-runner", "version": "1.0"},
            },
        })
        while True:
            init_res = recv()
            if init_res.get("id") == rid:
                break
        send({"jsonrpc": "2.0", "method": "notifications/initialized"})

        print("[5/6] Calling list_targets / list_devices...")
        res = call_tool("list_targets", {})
        content = res["result"]["content"][0]["text"]
        print("  -> list_targets output:\n" + "\n".join("     " + l for l in content.splitlines()))

        lines = content.splitlines()
        assert len(lines) == 2, f"Expected exactly 2 targets, got: {lines}"
        assert any(l.startswith("test-node ") for l in lines), "Original 'test-node' not found in listing"
        suffixed_lines = [l for l in lines if l.startswith("test-node-")]
        assert len(suffixed_lines) == 1, f"Expected 1 suffixed target, got: {suffixed_lines}"
        suffixed_id = suffixed_lines[0].split()[0]
        print(f"  -> VERIFIED: Found original 'test-node' and suffixed '{suffixed_id}'!")

        # Also test list_devices alias
        dev_res = call_tool("list_devices", {})
        assert dev_res["result"]["content"][0]["text"] == content, "list_devices output did not match list_targets"
        print("  -> VERIFIED: 'list_devices' alias returns identical output!")

        print("[6/6] Testing targeted execution...")
        # Execute on Target 1
        cmd1 = call_tool("run_command", {"command": "echo HELLO_TARGET_1", "target": "test-node"})
        cmd1_text = cmd1["result"]["content"][0]["text"]
        assert "HELLO_TARGET_1" in cmd1_text, f"Unexpected output from Target 1: {cmd1_text}"
        print("  -> Target 1 execution verified: HELLO_TARGET_1")

        # Execute on Target 2 (suffixed)
        cmd2 = call_tool("run_command", {"command": "echo HELLO_TARGET_2", "target": suffixed_id})
        cmd2_text = cmd2["result"]["content"][0]["text"]
        assert "HELLO_TARGET_2" in cmd2_text, f"Unexpected output from Target 2: {cmd2_text}"
        print(f"  -> Target 2 ({suffixed_id}) execution verified: HELLO_TARGET_2")

        # Disconnect Target 2 and verify Target 1 remains functional
        print("  -> Terminating Target 2...")
        target2.terminate()
        target2.wait()
        time.sleep(0.5)

        assert target1.poll() is None, "Target 1 was affected by Target 2 disconnect!"
        cmd1_again = call_tool("run_command", {"command": "echo STILL_ALIVE", "target": "test-node"})
        assert "STILL_ALIVE" in cmd1_again["result"]["content"][0]["text"]
        print("  -> VERIFIED: Target 1 remains functional after Target 2 disconnected!")

        print("\n=======================================================")
        print("🎉 ALL TESTS PASSED: Multiple targets with same ID/token")
        print("   are kept active, suffixed correctly, and routed properly!")
        print("=======================================================")
        return 0

    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    p.kill()

if __name__ == "__main__":
    sys.exit(main())
