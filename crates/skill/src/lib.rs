//! The relayfs skill document.
//!
//! Printed by the `skill` subcommand of every relayfs binary. Written to be
//! loadable by an AI agent: everything about the app, its working principles,
//! and its caveats.

/// The full skill document.
pub const SKILL: &str = r#"# relayfs — Remote Shell + Filesystem over a Relay

relayfs gives you live shell access and a real filesystem mount of a remote
machine, through a public relay — no open ports, no SSH, no VPN required.

## What it is

One binary, three modes, two outbound WebSocket connections, one shared
pairing token:

    MCP client (Claude / Cursor / any MCP host)
        |  stdio (MCP protocol)
        v
    relayfs --mode mcp      (your machine)     -- MCP server + FUSE mount host
        |  WebSocket (outbound)
        v
    relayfs --mode server   (public VPS)       -- dumb pipe: pairs peers, routes frames
        ^  WebSocket (outbound)
        |
    relayfs --mode target   (remote machine)   -- executes shell commands, serves file ops

Both the mcp and target modes connect OUT to the server, so neither machine
needs a public IP or open inbound ports. The server pairs them by a shared
token and forwards JSON-RPC frames between them without inspecting them.

## Components

| Mode     | Where          | Role                                                        |
|----------|----------------|-------------------------------------------------------------|
| server   | public VPS     | WebSocket hub: pairing, routing, /healthz                   |
| target   | remote machine | runs shell commands, serves file operations                 |
| mcp      | your machine   | MCP server over stdio, hosts the FUSE mount                 |

## Quick start

1. Server (VPS):
       RELAYFS_TOKEN=secret relayfs --mode server --listen 0.0.0.0:8787
2. Target (remote machine):
       relayfs --mode target --base-url ws://relay.example.com:8787 --token secret
3. MCP (your machine):
       relayfs --mode mcp --base-url ws://relay.example.com:8787 --token secret
4. Point your MCP client at the mcp mode (stdio transport), then call its tools.

`--base-url` accepts a base URL (`ws://host:port`); the `/ws` endpoint is
appended automatically. Every flag also has an env-var form (`RELAYFS_RELAY`,
`RELAYFS_TOKEN`, `RELAYFS_AGENT_ID`, `RELAYFS_BRIDGE_ID`, ...). Run
`relayfs --help`, `relayfs --mode <mode> --help`, or `relayfs skill` for the
full reference.

## MCP tools (mcp mode)

15 tools:

| Tool             | Purpose                                                        |
|------------------|----------------------------------------------------------------|
| run_command      | run a shell command remotely; output streams live as it runs   |
| read_file        | read a file (base64, offset/limit)                             |
| write_file       | write a file (UTF-8 text, optional create_dirs)                |
| list_dir         | list a remote directory                                        |
| stat             | metadata: kind, size, mode, uid/gid, symlink target           |
| mkdir            | create a directory (with mode)                                |
| remove           | remove a file or directory (recursive option)                 |
| rename           | rename / move                                                 |
| copy             | copy a file or directory (recursive option)                   |
| mount_remote     | FUSE-mount a remote directory into the local filesystem      |
| unmount_remote   | tear down a mount                                              |
| list_mounts      | list active mounts                                            |
| ping             | target health check                                            |
| list_targets     | list all targets connected to the relay (answered by the relay) |
| get_command_result | fetch a command's result by execution_id (head/tail options)  |

## Working principles

1. **The server is a dumb pipe.** It validates the pairing token, then routes
   JSON-RPC frames between the paired mcp client and target. It never inspects
   command contents, file data, or method bodies. If the target is offline,
   the server answers requests with a `target offline` error.

2. **JSON-RPC 2.0 framing.** Requests carry an id; responses echo it. The
   target streams command output as `command_output` notifications while the
   command runs, then sends the final result. The mcp client matches
   responses to pending requests by id.

3. **Pairing token.** One token pairs one mcp client with one target. The
   server can enforce a single required token (`--token` / `RELAYFS_TOKEN`);
   the target and mcp client must present the same token. The token is the
   only credential — rotate it by restarting all three.

4. **Command execution.** The target runs commands under the user's login
   shell (`$SHELL`), with optional cwd, env overrides, timeout, and stdin
   input. stdout and stderr are streamed live to the mcp client — nothing is
   printed to the target's terminal. Commands that prompt for input (e.g.
   `read`, interactive installers) consume the `input` argument as their
   stdin, then see EOF. On disconnect, all running commands are killed.
   `timeout_secs` kills the command on the target after N seconds;
   `request_timeout_secs` bounds how long the mcp side waits for the response
   (default 5 minutes, 0 = no limit — the command keeps running on the target
   after a wait timeout). `wait: false` returns an `execution_id` immediately
   and the command runs in the background; `get_command_result` fetches the
   result by that id (with optional `head`/`tail` line limits, applied in
   that order). A
   timed-out command is killed and reported. Both sides log every command:
   the mcp side logs the call and its completion (exit code / timeout), the
   target logs what it executes and when it finishes.

5. **FUSE mount semantics.** `mount_remote` mounts a remote directory into
   your local filesystem as a real kernel mount (`/dev/fuse`). Every kernel
   operation — lookup, getattr, read, write, readdir, mkdir, unlink, rename,
   chmod, truncate, symlink — is translated into an RPC to the target. The
   mount point IS the remote directory: there is no copy, no watcher, no
   batch sync. Writes land on the remote machine immediately (on file close
   / fsync, like a normal local disk). The mount lives on the mcp machine;
   the target only serves file operations.

6. **Lifecycle.** The relay pings every connected peer every 30s, so idle
   connections survive intermediary idle timeouts (Cloudflare, nginx). Both
   the target and the mcp client reconnect automatically after a dropped
   connection (default 5s). Requests made during a reconnection window fail
   fast with a `relay connection offline (reconnecting)` error; retry them.
   Mounts are torn down when the mcp process exits.

## Caveats

- **Write timing.** The kernel buffers writes in page cache. Data reaches the
  remote when the file is closed or fsynced — i.e. when your editor saves.
  Not per keystroke. This matches normal local-disk behavior.

- **Remote → local direction.** Changes made directly on the target machine
  (SSH, another process) appear in your mount on next access. Attribute
  lookups are cached for 1 second; reads always hit the remote. Near-real-
  time, but not push-notified: if the remote edits a file you have open
  locally, your editor will not auto-reload.

- **Concurrent edits.** Last-writer-wins per file. No conflict detection.
  Do not edit the same file on both sides simultaneously.

- **Network / target failure.** Operations fail with `EIO` (filesystem error)
  — never silently. Your editor will show a save error, which is correct.

- **Performance.** Every operation is a network round trip. Fine for
  editing, `ls`, small builds. Do NOT run heavy builds or copy large files
  through the mount — run heavy work remotely via `run_command` instead.

- **Security.** The target executes commands as the user it runs under. Run
  it with a dedicated, least-privilege user on the remote machine. Use
  `wss://` (reverse proxy with TLS) in production. The server sees only
  routing metadata, never command or file contents.

- **Platform.** The FUSE mount requires Linux on the mcp machine
  (`/dev/fuse` + `fusermount3`). The server and target are cross-platform.

## Usage guidance

- **Editing a project** → `mount_remote` the project directory, edit locally,
  run builds/tests remotely with `run_command`.
- **Heavy work** (builds, installs, data processing) → `run_command` on the
  remote machine, never inside the mount.
- **One-off file access** → `read_file` / `write_file` / `list_dir` / `stat`
  are cheaper than a mount for single operations.
- **Check health first** → `ping` before a long session; if the target is
  offline, requests fail fast with a clear error.
"#;

/// Print the skill document to stdout.
pub fn print_skill() {
    print!("{SKILL}");
}
