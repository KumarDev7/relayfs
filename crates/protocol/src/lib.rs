//! Wire protocol shared by relay, agent, and bridge.
//!
//! Framing is JSON-RPC 2.0 (see `relayfs-rpc`). The `params` payload of every
//! message is one of the `Request` / `Response` / `Notification` enums below.
//! The relay inspects only the `session` / `target` fields to route; the
//! `method` and `params` body pass through untouched.

use serde::{Deserialize, Serialize};

/// Unique id of a connected agent, chosen by the agent at startup.
pub type AgentId = String;
/// Unique id of a connected bridge, chosen by the bridge at startup.
pub type BridgeId = String;
/// Opaque id of a request, echoed back in the response.
pub type RequestId = u64;

/// First message sent by a peer after the WebSocket opens.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub kind: PeerKind,
    pub id: String,
    /// Human-readable name for logs (e.g. "dev-laptop", "prod-server").
    pub name: String,
    /// Agent only: the pairing token that authorizes it to serve this bridge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerKind {
    Bridge,
    Agent,
}

/// Reply to `Hello`. `session` is present for both peers; `agent_id` only for
/// the bridge (the agent already knows its own id).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloAck {
    pub session: SessionId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<AgentId>,
}

pub type SessionId = String;

/// A request from bridge to agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: RequestId,
    pub method: String,
    pub params: serde_json::Value,
}

/// A response from agent to bridge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl RpcError {
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
    pub fn with_data(code: i64, message: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(data),
        }
    }
}

/// One-way event from agent to bridge (or bridge to agent).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub method: String,
    pub params: serde_json::Value,
}

/// Standard JSON-RPC error codes.
pub mod code {
    pub const PARSE_ERROR: i64 = -32700;
    pub const INVALID_REQUEST: i64 = -32600;
    pub const METHOD_NOT_FOUND: i64 = -32601;
    pub const INVALID_PARAMS: i64 = -32602;
    pub const INTERNAL_ERROR: i64 = -32603;
    /// Agent is not connected to this session.
    pub const AGENT_OFFLINE: i64 = -32000;
    /// The request was cancelled by the caller.
    pub const CANCELLED: i64 = -32001;
}

/// Methods understood by the agent.
pub mod method {
    /// Run a command; params: `run_command` request.
    pub const RUN_COMMAND: &str = "run_command";
    /// Read a file; params: `read_file` request.
    pub const READ_FILE: &str = "read_file";
    /// Write a file; params: `write_file` request.
    pub const WRITE_FILE: &str = "write_file";
    /// List a directory; params: `list_dir` request.
    pub const LIST_DIR: &str = "list_dir";
    /// Stat a path; params: `stat` request.
    pub const STAT: &str = "stat";
    /// Create a directory; params: `mkdir` request.
    pub const MKDIR: &str = "mkdir";
    /// Remove a file or directory; params: `remove` request.
    pub const REMOVE: &str = "remove";
    /// Rename a file or directory; params: `rename` request.
    pub const RENAME: &str = "rename";
    /// Copy a file or directory; params: `copy` request.
    pub const COPY: &str = "copy";
    /// Write bytes at an offset (no truncate); params: `write_at` request.
    pub const WRITE_AT: &str = "write_at";
    /// Truncate a file to a size; params: `truncate` request.
    pub const TRUNCATE: &str = "truncate";
    /// Create a symlink; params: `symlink` request.
    pub const SYMLINK: &str = "symlink";
    /// Change file permissions; params: `chmod` request.
    pub const CHMOD: &str = "chmod";
    /// Stream a file's contents; params: `read_file` request.
    pub const STREAM_FILE: &str = "stream_file";
    /// Begin a FUSE mount; params: `mount` request.
    pub const MOUNT: &str = "mount";
    /// Tear down a FUSE mount; params: `unmount` request.
    pub const UNMOUNT: &str = "unmount";
    /// List active mounts.
    pub const LIST_MOUNTS: &str = "list_mounts";
    /// Agent health check.
    pub const PING: &str = "ping";
    /// List all targets connected to the relay. Answered by the relay
    /// itself (it is the only component that sees every agent), not
    /// forwarded to the paired agent.
    pub const LIST_TARGETS: &str = "list_targets";
}

/// Notifications emitted by the agent.
pub mod notify {
    /// A command produced output; params: `command_output` notification.
    pub const COMMAND_OUTPUT: &str = "command_output";
    /// A command finished; params: `command_finished` notification.
    pub const COMMAND_FINISHED: &str = "command_finished";
    /// A FUSE mount was torn down; params: `mount_gone` notification.
    pub const MOUNT_GONE: &str = "mount_gone";
}

// ---------------------------------------------------------------------------
// Request params
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunCommandParams {
    /// Command line, e.g. `cargo build --release`. Executed via the user's shell.
    pub command: String,
    /// Working directory for the command.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Environment overrides, e.g. `{"FOO": "bar"}`.
    #[serde(default)]
    pub env: Option<serde_json::Map<String, serde_json::Value>>,
    /// Kill the command after this many seconds (0 = no limit).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Text written to the command's stdin before it starts reading.
    /// Commands that need interactive input (prompts, `read`) consume this.
    #[serde(default)]
    pub input: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadFileParams {
    pub path: String,
    /// Byte offset to start reading from.
    #[serde(default)]
    pub offset: Option<u64>,
    /// Maximum number of bytes to read.
    #[serde(default)]
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteFileParams {
    pub path: String,
    /// Base64-encoded contents.
    pub data: String,
    /// Create parent directories if missing.
    #[serde(default)]
    pub create_dirs: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDirParams {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatParams {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MkdirParams {
    pub path: String,
    /// Unix permission bits, e.g. 0o755.
    #[serde(default)]
    pub mode: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveParams {
    pub path: String,
    /// Remove non-empty directories recursively.
    #[serde(default)]
    pub recursive: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameParams {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CopyParams {
    pub from: String,
    pub to: String,
    /// Copy directories recursively.
    #[serde(default)]
    pub recursive: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteAtParams {
    pub path: String,
    /// Byte offset to write at.
    pub offset: u64,
    /// Base64-encoded bytes.
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruncateParams {
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymlinkParams {
    /// Path of the link to create.
    pub link: String,
    /// Target the link points to.
    pub target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChmodParams {
    pub path: String,
    pub mode: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountParams {
    /// Absolute path on the agent of the directory to expose.
    pub remote_dir: String,
    /// Absolute path on the agent where the FUSE filesystem is mounted.
    pub mount_point: String,
    /// Mount read-only.
    #[serde(default)]
    pub read_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnmountParams {
    pub mount_point: String,
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunCommandResult {
    /// Exit status: `Some(code)` if the process exited, `None` if it was
    /// killed by a signal or timed out.
    pub exit_code: Option<i32>,
    /// True if the command was killed by the timeout.
    pub timed_out: bool,
    /// Combined stdout+stderr captured so far (streaming continues via
    /// `command_output` notifications).
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadFileResult {
    /// Base64-encoded bytes.
    pub data: String,
    /// True if fewer bytes were returned than requested (end of file).
    pub eof: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteFileResult {
    pub bytes_written: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDirResult {
    pub entries: Vec<DirEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    pub name: String,
    pub kind: FileKind,
    pub size: u64,
    pub modified: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileKind {
    File,
    Dir,
    Symlink,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatResult {
    pub kind: FileKind,
    pub size: u64,
    pub modified: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    /// Symlink target, when `kind == Symlink`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_target: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountResult {
    pub mount_point: String,
    pub remote_dir: String,
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListMountsResult {
    pub mounts: Vec<MountResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingResult {
    pub ok: bool,
    pub hostname: String,
    pub pid: u32,
}

/// One connected target (agent), as reported by the relay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetInfo {
    /// Agent id chosen at startup.
    pub id: String,
    /// Human-readable name shown in relay logs.
    pub name: String,
    /// Relay-assigned session id.
    pub session: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListTargetsResult {
    pub targets: Vec<TargetInfo>,
}

// ---------------------------------------------------------------------------
// Notifications
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandOutputNotification {
    pub id: RequestId,
    /// stdout or stderr.
    pub stream: String,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandFinishedNotification {
    pub id: RequestId,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountGoneNotification {
    pub mount_point: String,
    pub reason: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_round_trips() {
        let hello = Hello {
            kind: PeerKind::Agent,
            id: "agent-1".into(),
            name: "prod".into(),
            token: Some("secret".into()),
        };
        let json = serde_json::to_string(&hello).unwrap();
        let back: Hello = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, PeerKind::Agent);
        assert_eq!(back.id, "agent-1");
        assert_eq!(back.token.as_deref(), Some("secret"));
    }

    #[test]
    fn hello_without_token_round_trips() {
        let hello = Hello {
            kind: PeerKind::Bridge,
            id: "bridge-1".into(),
            name: "local".into(),
            token: None,
        };
        let json = serde_json::to_string(&hello).unwrap();
        assert!(!json.contains("token"));
        let back: Hello = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kind, PeerKind::Bridge);
        assert!(back.token.is_none());
    }

    #[test]
    fn run_command_params_input_round_trips() {
        let params = RunCommandParams {
            command: "read x && echo $x".into(),
            cwd: Some("/tmp".into()),
            env: None,
            timeout_secs: Some(5),
            input: Some("hello\n".into()),
        };
        let json = serde_json::to_string(&params).unwrap();
        let back: RunCommandParams = serde_json::from_str(&json).unwrap();
        assert_eq!(back.input.as_deref(), Some("hello\n"));
        assert_eq!(back.timeout_secs, Some(5));
    }

    #[test]
    fn run_command_params_input_defaults_to_none() {
        let back: RunCommandParams = serde_json::from_str(r#"{"command": "ls"}"#).unwrap();
        assert_eq!(back.command, "ls");
        assert!(back.input.is_none());
        assert!(back.cwd.is_none());
        assert!(back.timeout_secs.is_none());
    }

    #[test]
    fn rpc_error_serializes_code_and_message() {
        let err = RpcError::new(crate::code::METHOD_NOT_FOUND, "unknown method: foo");
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["code"], crate::code::METHOD_NOT_FOUND);
        assert_eq!(json["message"], "unknown method: foo");
    }
}
