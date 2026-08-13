//! relayfs bridge — local MCP server (library).
//!
//! Runs on the developer's machine. Connects out to the relay, pairs with the
//! agent, and exposes the remote machine's shell and filesystem as MCP tools.
//! Also hosts the FUSE mount: `mount_remote` mounts a remote directory into
//! the local filesystem, backed by RPC calls to the agent.

pub mod client;
pub mod fuse_fs;
pub mod mcp;

use std::sync::Arc;

use rmcp::{transport::stdio, ServiceExt};

/// Run the bridge: connect to the relay, pair with the agent, and serve MCP
/// over stdio until the process is terminated.
pub async fn run(base_url: &str, token: &str, id: &str, name: &str) -> anyhow::Result<()> {
    // Connect to the relay and pair with the agent.
    let client = client::AgentClient::connect(base_url, token, id, name).await?;
    let client = Arc::new(client);

    // Serve MCP over stdio.
    let service = mcp::RelayfsServer::new(client)
        .serve(stdio())
        .await
        .inspect_err(|e| tracing::error!("MCP serving error: {e:?}"))?;

    service.waiting().await?;
    Ok(())
}
