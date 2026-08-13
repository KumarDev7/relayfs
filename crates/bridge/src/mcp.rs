//! MCP server: exposes the remote machine as MCP tools.

use std::sync::Arc;

use rmcp::{
    handler::server::wrapper::Parameters, model::*, schemars, service::RequestContext, tool,
    tool_handler, tool_router, ErrorData as McpError, RoleServer, ServerHandler,
};
use serde::Deserialize;

use crate::client::AgentClient;
use crate::fuse_fs::MountManager;

/// MCP tool arguments.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunCommandArgs {
    /// Command line to execute on the remote machine, e.g. `cargo build`.
    pub command: String,
    /// Working directory on the remote machine.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Kill the command after this many seconds (0 = no limit).
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Text written to the command's stdin before it starts reading.
    /// Use for commands that prompt for input (e.g. `read`, interactive
    /// installers). The command sees this as its stdin, then EOF.
    #[serde(default)]
    pub input: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct PathArgs {
    /// Absolute path on the remote machine.
    pub path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct ReadFileArgs {
    /// Absolute path on the remote machine.
    pub path: String,
    /// Byte offset to start reading from.
    #[serde(default)]
    pub offset: Option<u64>,
    /// Maximum number of bytes to read.
    #[serde(default)]
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct WriteFileArgs {
    /// Absolute path on the remote machine.
    pub path: String,
    /// File contents (UTF-8 text).
    pub content: String,
    /// Create parent directories if missing.
    #[serde(default)]
    pub create_dirs: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MkdirArgs {
    /// Absolute path on the remote machine.
    pub path: String,
    /// Unix permission bits, e.g. 755.
    #[serde(default)]
    pub mode: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RemoveArgs {
    /// Absolute path on the remote machine.
    pub path: String,
    /// Remove non-empty directories recursively.
    #[serde(default)]
    pub recursive: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RenameArgs {
    /// Source path.
    pub from: String,
    /// Destination path.
    pub to: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct CopyArgs {
    /// Source path.
    pub from: String,
    /// Destination path.
    pub to: String,
    /// Copy directories recursively.
    #[serde(default)]
    pub recursive: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct MountArgs {
    /// Absolute path on the remote machine to expose.
    pub remote_dir: String,
    /// Local directory where the remote folder will be mounted.
    pub mount_point: String,
    /// Mount read-only.
    #[serde(default)]
    pub read_only: Option<bool>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UnmountArgs {
    /// Local mount point to tear down.
    pub mount_point: String,
}

/// The relayfs MCP server.
pub struct RelayfsServer {
    client: Arc<AgentClient>,
    mounts: Arc<MountManager>,
}

impl RelayfsServer {
    pub fn new(client: Arc<AgentClient>) -> Self {
        let mounts = Arc::new(MountManager::new(client.clone()));
        Self { client, mounts }
    }

    fn err(e: impl std::fmt::Display) -> McpError {
        McpError::internal_error(e.to_string(), None)
    }
}

#[tool_router]
impl RelayfsServer {
    /// Run a command on the remote machine. Output is streamed back as it is produced.
    #[tool(description = "Run a shell command on the remote machine and return its output")]
    async fn run_command(
        &self,
        ctx: RequestContext<RoleServer>,
        Parameters(args): Parameters<RunCommandArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!(
            "mcp call run_command: {} (cwd={:?}, timeout_secs={:?}, input={} bytes)",
            args.command,
            args.cwd,
            args.timeout_secs,
            args.input.as_ref().map_or(0, |s| s.len())
        );
        let params = serde_json::json!({
            "command": args.command,
            "cwd": args.cwd,
            "timeout_secs": args.timeout_secs,
            "input": args.input,
        });
        let result = self
            .client
            .call(relayfs_protocol::method::RUN_COMMAND, params)
            .await
            .map_err(|e| Self::err(e.message))?;
        let result: relayfs_protocol::RunCommandResult =
            serde_json::from_value(result).map_err(Self::err)?;
        tracing::info!(
            "run_command finished: exit_code={:?}, timed_out={}",
            result.exit_code,
            result.timed_out
        );

        let mut text = result.output;
        if result.timed_out {
            text.push_str("\n[command timed out]");
        } else if let Some(code) = result.exit_code {
            if code != 0 {
                text.push_str(&format!("\n[exit code: {code}]"));
            }
        }
        let _ = ctx;
        Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
    }

    /// Read a file from the remote machine.
    #[tool(description = "Read a file from the remote machine (base64-encoded)")]
    async fn read_file(
        &self,
        Parameters(args): Parameters<ReadFileArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!(
            "mcp call read_file: {} (offset={:?}, limit={:?})",
            args.path,
            args.offset,
            args.limit
        );
        let result = self
            .client
            .call(
                relayfs_protocol::method::READ_FILE,
                serde_json::json!({
                    "path": args.path,
                    "offset": args.offset,
                    "limit": args.limit,
                }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        let result: relayfs_protocol::ReadFileResult =
            serde_json::from_value(result).map_err(Self::err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::json!({ "data": result.data, "eof": result.eof }).to_string(),
        )]))
    }

    /// Write a file on the remote machine.
    #[tool(description = "Write a file on the remote machine")]
    async fn write_file(
        &self,
        Parameters(args): Parameters<WriteFileArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!(
            "mcp call write_file: {} ({} bytes, create_dirs={:?})",
            args.path,
            args.content.len(),
            args.create_dirs
        );
        let data = base64_encode(args.content.as_bytes());
        let result = self
            .client
            .call(
                relayfs_protocol::method::WRITE_FILE,
                serde_json::json!({
                    "path": args.path,
                    "data": data,
                    "create_dirs": args.create_dirs,
                }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        let result: relayfs_protocol::WriteFileResult =
            serde_json::from_value(result).map_err(Self::err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "wrote {} bytes",
            result.bytes_written
        ))]))
    }

    /// List a directory on the remote machine.
    #[tool(description = "List a directory on the remote machine")]
    async fn list_dir(
        &self,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call list_dir: {}", args.path);
        let result = self
            .client
            .call(
                relayfs_protocol::method::LIST_DIR,
                serde_json::json!({ "path": args.path }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        let result: relayfs_protocol::ListDirResult =
            serde_json::from_value(result).map_err(Self::err)?;
        let mut lines = Vec::new();
        for entry in &result.entries {
            let kind = match entry.kind {
                relayfs_protocol::FileKind::Dir => "d",
                relayfs_protocol::FileKind::File => "-",
                relayfs_protocol::FileKind::Symlink => "l",
                relayfs_protocol::FileKind::Other => "?",
            };
            lines.push(format!("{kind} {:>10} {}", entry.size, entry.name));
        }
        Ok(CallToolResult::success(vec![ContentBlock::text(
            lines.join("\n"),
        )]))
    }

    /// Stat a path on the remote machine.
    #[tool(description = "Stat a path on the remote machine")]
    async fn stat(
        &self,
        Parameters(args): Parameters<PathArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call stat: {}", args.path);
        let result = self
            .client
            .call(
                relayfs_protocol::method::STAT,
                serde_json::json!({ "path": args.path }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            result.to_string(),
        )]))
    }

    /// Create a directory on the remote machine.
    #[tool(description = "Create a directory on the remote machine")]
    async fn mkdir(
        &self,
        Parameters(args): Parameters<MkdirArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call mkdir: {} (mode={:?})", args.path, args.mode);
        self.client
            .call(
                relayfs_protocol::method::MKDIR,
                serde_json::json!({ "path": args.path, "mode": args.mode }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        Ok(CallToolResult::success(vec![ContentBlock::text("ok")]))
    }

    /// Remove a file or directory on the remote machine.
    #[tool(description = "Remove a file or directory on the remote machine")]
    async fn remove(
        &self,
        Parameters(args): Parameters<RemoveArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!(
            "mcp call remove: {} (recursive={:?})",
            args.path,
            args.recursive
        );
        self.client
            .call(
                relayfs_protocol::method::REMOVE,
                serde_json::json!({
                    "path": args.path,
                    "recursive": args.recursive,
                }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        Ok(CallToolResult::success(vec![ContentBlock::text("ok")]))
    }

    /// Rename a file or directory on the remote machine.
    #[tool(description = "Rename a file or directory on the remote machine")]
    async fn rename(
        &self,
        Parameters(args): Parameters<RenameArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call rename: {} -> {}", args.from, args.to);
        self.client
            .call(
                relayfs_protocol::method::RENAME,
                serde_json::json!({ "from": args.from, "to": args.to }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        Ok(CallToolResult::success(vec![ContentBlock::text("ok")]))
    }

    /// Copy a file or directory on the remote machine.
    #[tool(description = "Copy a file or directory on the remote machine")]
    async fn copy(
        &self,
        Parameters(args): Parameters<CopyArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!(
            "mcp call copy: {} -> {} (recursive={:?})",
            args.from,
            args.to,
            args.recursive
        );
        self.client
            .call(
                relayfs_protocol::method::COPY,
                serde_json::json!({
                    "from": args.from,
                    "to": args.to,
                    "recursive": args.recursive,
                }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        Ok(CallToolResult::success(vec![ContentBlock::text("ok")]))
    }

    /// Mount a remote directory into the local filesystem (FUSE).
    #[tool(description = "Mount a remote directory into the local filesystem as a real FUSE mount")]
    async fn mount_remote(
        &self,
        Parameters(args): Parameters<MountArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!(
            "mcp call mount_remote: {} at {} (read_only={})",
            args.remote_dir,
            args.mount_point,
            args.read_only.unwrap_or(false)
        );
        self.mounts
            .mount(
                &args.remote_dir,
                &args.mount_point,
                args.read_only.unwrap_or(false),
            )
            .await
            .map_err(Self::err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "mounted {} at {}",
            args.remote_dir, args.mount_point
        ))]))
    }

    /// Unmount a remote directory from the local filesystem.
    #[tool(description = "Unmount a remote directory from the local filesystem")]
    async fn unmount_remote(
        &self,
        Parameters(args): Parameters<UnmountArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call unmount_remote: {}", args.mount_point);
        self.mounts
            .unmount(&args.mount_point)
            .await
            .map_err(Self::err)?;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            "unmounted",
        )]))
    }

    /// List active mounts.
    #[tool(description = "List active remote mounts")]
    async fn list_mounts(&self) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call list_mounts");
        let mounts = self.mounts.list().await;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            if mounts.is_empty() {
                "no mounts".into()
            } else {
                mounts.join("\n")
            },
        )]))
    }

    /// Check the connection to the remote machine.
    #[tool(description = "Check the connection to the remote machine")]
    async fn ping(&self) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call ping");
        let result = self
            .client
            .call(relayfs_protocol::method::PING, serde_json::json!({}))
            .await
            .map_err(|e| Self::err(e.message))?;
        Ok(CallToolResult::success(vec![ContentBlock::text(
            result.to_string(),
        )]))
    }
}

#[tool_handler]
impl ServerHandler for RelayfsServer {}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}
