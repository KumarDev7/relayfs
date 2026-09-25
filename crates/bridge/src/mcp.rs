//! MCP server: exposes the remote machine as MCP tools.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
    /// Maximum time to wait for the command to finish. Defaults to 300s
    /// (5 minutes); 0 = no limit. `timeout_secs` kills the command; this
    /// only bounds how long the bridge waits for the response.
    #[serde(default)]
    pub request_timeout_secs: Option<u64>,
    /// Wait for the command to finish and return its output (default true).
    /// When false, the call returns an `execution_id` immediately and the
    /// command keeps running; fetch the result with `get_command_result`.
    #[serde(default)]
    pub wait: Option<bool>,
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

/// Upload a local file to the connected target without mounting it.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UploadFileArgs {
    /// Source file path on the local MCP machine.
    pub local_path: String,
    /// Destination file path on the remote target.
    pub remote_path: String,
    /// Create the remote parent directory when absent.
    #[serde(default)]
    pub create_dirs: Option<bool>,
}

/// Download a remote file to the local MCP machine without mounting it.
#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DownloadFileArgs {
    /// Source file path on the remote target.
    pub remote_path: String,
    /// Destination file path on the local MCP machine.
    pub local_path: String,
    /// Create the local parent directory when absent.
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct GetCommandResultArgs {
    /// Execution id returned by `run_command` with `wait: false`.
    pub execution_id: u64,
    /// Return only the first N lines of output.
    #[serde(default)]
    pub head: Option<usize>,
    /// Return only the last N lines of output.
    #[serde(default)]
    pub tail: Option<usize>,
}

/// The relayfs MCP server.
#[derive(Clone)]
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

const TRANSFER_CHUNK_BYTES: usize = 1024 * 1024;

fn temporary_path(path: &Path, label: &str) -> PathBuf {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    parent.join(format!(
        ".{file_name}.relayfs-{label}-{:016x}",
        rand::random::<u64>()
    ))
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
            "mcp call run_command: {} (cwd={:?}, timeout_secs={:?}, request_timeout_secs={:?}, input={} bytes)",
            args.command,
            args.cwd,
            args.timeout_secs,
            args.request_timeout_secs,
            args.input.as_ref().map_or(0, |s| s.len())
        );
        let params = serde_json::json!({
            "command": args.command,
            "cwd": args.cwd,
            "timeout_secs": args.timeout_secs,
            "input": args.input,
            "wait": args.wait,
        });
        // Wait bound: default 5 minutes, 0 = no limit. The command itself
        // keeps running on the target either way; this only bounds how long
        // the bridge waits for the response.
        let wait_secs = args.request_timeout_secs.unwrap_or(300);
        let result = if wait_secs > 0 {
            self.client
                .call_timeout(
                    relayfs_protocol::method::RUN_COMMAND,
                    params,
                    std::time::Duration::from_secs(wait_secs),
                )
                .await
                .map_err(|e| Self::err(e.message))?
        } else {
            self.client
                .call(relayfs_protocol::method::RUN_COMMAND, params)
                .await
                .map_err(|e| Self::err(e.message))?
        };
        let result: relayfs_protocol::RunCommandResult =
            serde_json::from_value(result).map_err(Self::err)?;
        tracing::info!(
            "run_command finished: exit_code={:?}, timed_out={}, execution_id={:?}",
            result.exit_code,
            result.timed_out,
            result.execution_id
        );

        // Fire-and-forget mode: return the execution id for get_command_result.
        if let Some(execution_id) = result.execution_id {
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                serde_json::json!({
                    "execution_id": execution_id,
                    "status": "running",
                })
                .to_string(),
            )]));
        }

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

    /// Upload a local file to the remote machine in bounded chunks.
    ///
    /// The target sees either its original file or the complete upload: data
    /// is written to a sibling temporary file and renamed only on success.
    #[tool(description = "Upload a local file to the remote machine atomically")]
    async fn upload_file(
        &self,
        Parameters(args): Parameters<UploadFileArgs>,
    ) -> Result<CallToolResult, McpError> {
        let local_path = PathBuf::from(&args.local_path);
        let remote_temp = format!(
            "{}.relayfs-upload-{:016x}",
            args.remote_path,
            rand::random::<u64>()
        );
        tracing::info!(
            "mcp upload_file: {} -> {} (create_dirs={:?})",
            local_path.display(),
            args.remote_path,
            args.create_dirs
        );

        let mut source = tokio::fs::File::open(&local_path)
            .await
            .map_err(|e| Self::err(format!("open local source {}: {e}", local_path.display())))?;
        let transfer = async {
            self.client
                .call(
                    relayfs_protocol::method::WRITE_FILE,
                    serde_json::json!({
                        "path": remote_temp,
                        "data": "",
                        "create_dirs": args.create_dirs,
                    }),
                )
                .await
                .map_err(|e| Self::err(e.message))?;

            let mut offset = 0u64;
            let mut buffer = vec![0; TRANSFER_CHUNK_BYTES];
            loop {
                let read = source.read(&mut buffer).await.map_err(|e| {
                    Self::err(format!("read local source {}: {e}", local_path.display()))
                })?;
                if read == 0 {
                    break;
                }
                self.client
                    .call(
                        relayfs_protocol::method::WRITE_AT,
                        serde_json::json!({
                            "path": remote_temp,
                            "offset": offset,
                            "data": base64_encode(&buffer[..read]),
                        }),
                    )
                    .await
                    .map_err(|e| Self::err(e.message))?;
                offset += read as u64;
            }
            self.client
                .call(
                    relayfs_protocol::method::TRUNCATE,
                    serde_json::json!({ "path": remote_temp, "size": offset }),
                )
                .await
                .map_err(|e| Self::err(e.message))?;
            Ok::<u64, McpError>(offset)
        }
        .await;

        let bytes = match transfer {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = self
                    .client
                    .call(
                        relayfs_protocol::method::REMOVE,
                        serde_json::json!({ "path": remote_temp, "recursive": false }),
                    )
                    .await;
                return Err(error);
            }
        };
        if let Err(error) = self
            .client
            .call(
                relayfs_protocol::method::RENAME,
                serde_json::json!({ "from": remote_temp, "to": args.remote_path }),
            )
            .await
        {
            let _ = self
                .client
                .call(
                    relayfs_protocol::method::REMOVE,
                    serde_json::json!({ "path": remote_temp, "recursive": false }),
                )
                .await;
            return Err(Self::err(error.message));
        }

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::json!({ "bytes_uploaded": bytes }).to_string(),
        )]))
    }

    /// Download a remote file to the local MCP machine in bounded chunks.
    ///
    /// The destination is replaced only after the complete transfer has been
    /// written and synced locally.
    #[tool(description = "Download a remote file to the local machine atomically")]
    async fn download_file(
        &self,
        Parameters(args): Parameters<DownloadFileArgs>,
    ) -> Result<CallToolResult, McpError> {
        let local_path = PathBuf::from(&args.local_path);
        if args.create_dirs.unwrap_or(false) {
            if let Some(parent) = local_path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
            {
                tokio::fs::create_dir_all(parent).await.map_err(|e| {
                    Self::err(format!("create local directory {}: {e}", parent.display()))
                })?;
            }
        }
        let local_temp = temporary_path(&local_path, "download");
        tracing::info!(
            "mcp download_file: {} -> {} (create_dirs={:?})",
            args.remote_path,
            local_path.display(),
            args.create_dirs
        );

        let transfer = async {
            let mut destination = tokio::fs::File::create(&local_temp).await.map_err(|e| {
                Self::err(format!(
                    "create local destination {}: {e}",
                    local_temp.display()
                ))
            })?;
            let mut offset = 0u64;
            loop {
                let result = self
                    .client
                    .call(
                        relayfs_protocol::method::READ_FILE,
                        serde_json::json!({
                            "path": args.remote_path,
                            "offset": offset,
                            "limit": TRANSFER_CHUNK_BYTES,
                        }),
                    )
                    .await
                    .map_err(|e| Self::err(e.message))?;
                let read: relayfs_protocol::ReadFileResult =
                    serde_json::from_value(result).map_err(Self::err)?;
                let data = base64_decode(&read.data).map_err(Self::err)?;
                if data.is_empty() && !read.eof {
                    return Err(Self::err(
                        "remote transfer returned an empty non-final chunk",
                    ));
                }
                destination.write_all(&data).await.map_err(|e| {
                    Self::err(format!(
                        "write local destination {}: {e}",
                        local_temp.display()
                    ))
                })?;
                offset += data.len() as u64;
                if read.eof {
                    destination.sync_all().await.map_err(|e| {
                        Self::err(format!(
                            "sync local destination {}: {e}",
                            local_temp.display()
                        ))
                    })?;
                    return Ok::<u64, McpError>(offset);
                }
            }
        }
        .await;

        let bytes = match transfer {
            Ok(bytes) => bytes,
            Err(error) => {
                let _ = tokio::fs::remove_file(&local_temp).await;
                return Err(error);
            }
        };
        tokio::fs::rename(&local_temp, &local_path)
            .await
            .map_err(|e| {
                Self::err(format!(
                    "replace local destination {}: {e}",
                    local_path.display()
                ))
            })?;

        Ok(CallToolResult::success(vec![ContentBlock::text(
            serde_json::json!({ "bytes_downloaded": bytes }).to_string(),
        )]))
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

    /// List all targets connected to the relay.
    #[tool(description = "List all targets (agents) currently connected to the relay")]
    async fn list_targets(&self) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call list_targets");
        let result = self
            .client
            .call(
                relayfs_protocol::method::LIST_TARGETS,
                serde_json::json!({}),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        let result: relayfs_protocol::ListTargetsResult =
            serde_json::from_value(result).map_err(Self::err)?;
        if result.targets.is_empty() {
            return Ok(CallToolResult::success(vec![ContentBlock::text(
                "no targets connected",
            )]));
        }
        let lines: Vec<String> = result
            .targets
            .iter()
            .map(|t| format!("{} (name={}, session={})", t.id, t.name, t.session))
            .collect();
        Ok(CallToolResult::success(vec![ContentBlock::text(
            lines.join("\n"),
        )]))
    }

    /// Fetch the result of a command started with `wait: false`.
    #[tool(
        description = "Fetch the result of a command started with wait=false, by its execution_id"
    )]
    async fn get_command_result(
        &self,
        Parameters(args): Parameters<GetCommandResultArgs>,
    ) -> Result<CallToolResult, McpError> {
        tracing::info!(
            "mcp call get_command_result: execution_id={} (head={:?}, tail={:?})",
            args.execution_id,
            args.head,
            args.tail
        );
        let result = self
            .client
            .call(
                relayfs_protocol::method::GET_COMMAND_RESULT,
                serde_json::json!({
                    "execution_id": args.execution_id,
                    "head": args.head,
                    "tail": args.tail,
                }),
            )
            .await
            .map_err(|e| Self::err(e.message))?;
        let result: relayfs_protocol::GetCommandResult =
            serde_json::from_value(result).map_err(Self::err)?;
        let mut text = result.output;
        if result.done {
            if result.timed_out.unwrap_or(false) {
                text.push_str("\n[command timed out]");
            } else if let Some(code) = result.exit_code {
                if code != 0 {
                    text.push_str(&format!("\n[exit code: {code}]"));
                }
            }
        } else {
            text.push_str("\n[still running]");
        }
        Ok(CallToolResult::success(vec![ContentBlock::text(text)]))
    }

    /// Return the relayfs skill document (app overview, principles, caveats).
    #[tool(
        description = "Return the relayfs skill document describing all modes, tools, principles, and caveats"
    )]
    async fn skill(&self) -> Result<CallToolResult, McpError> {
        tracing::info!("mcp call skill");
        Ok(CallToolResult::success(vec![ContentBlock::text(
            relayfs_skill::SKILL,
        )]))
    }
}

#[tool_handler]
impl ServerHandler for RelayfsServer {}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn base64_decode(data: &str) -> Result<Vec<u8>, String> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| format!("invalid base64 returned by target: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn download_replaces_the_destination_from_a_chunked_source() {
        // 1.5 MiB source read through the real 1 MiB chunk size.
        let source_dir = tempfile::tempdir().unwrap();
        let source = source_dir.path().join("remote.bin");
        let payload: Vec<u8> = (0..3 * 512 * 1024u32).map(|i| (i * 31 + 7) as u8).collect();
        tokio::fs::write(&source, &payload).await.unwrap();

        let destination_dir = tempfile::tempdir().unwrap();
        let destination = destination_dir.path().join("nested").join("local.bin");
        // The destination directory must exist before the temporary file and
        // the final rename can land in it.
        tokio::fs::create_dir_all(destination.parent().unwrap())
            .await
            .unwrap();
        let temporary = temporary_path(&destination, "download");

        let mut file = tokio::fs::File::create(&temporary).await.unwrap();
        let mut offset = 0u64;
        loop {
            let end = (offset + TRANSFER_CHUNK_BYTES as u64).min(payload.len() as u64);
            file.write_all(&payload[offset as usize..end as usize])
                .await
                .unwrap();
            offset = end;
            if offset as usize >= payload.len() {
                break;
            }
        }
        file.sync_all().await.unwrap();
        drop(file);

        tokio::fs::rename(&temporary, &destination).await.unwrap();

        let written = tokio::fs::read(&destination).await.unwrap();
        assert_eq!(written.len(), payload.len());
        assert_eq!(written, payload);
        assert!(!temporary.exists(), "temporary file survived the replace");
    }

    #[test]
    fn temporary_path_stays_in_the_destination_directory() {
        let path = Path::new("/srv/app/report.tar.gz");
        let temporary = temporary_path(path, "upload");
        assert_eq!(temporary.parent(), path.parent());
        let name = temporary
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        assert!(name.contains("report.tar.gz"), "{name}");
        assert!(name.starts_with('.'), "{name}");
        assert!(name.contains("relayfs-upload-"), "{name}");
    }

    #[test]
    fn each_temporary_path_is_unique() {
        let path = Path::new("/srv/app/report.tar.gz");
        let first = temporary_path(path, "upload");
        let second = temporary_path(path, "upload");
        assert_ne!(first, second);
    }
}
