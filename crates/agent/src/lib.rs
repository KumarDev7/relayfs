//! relayfs agent — runs on the remote machine (library).
//!
//! Connects out to the relay, authenticates with a pairing token, then serves
//! shell execution and file access to the paired bridge.

pub mod commands;
pub mod conn;
pub mod files;

use std::sync::Arc;

use tokio::sync::Mutex;

/// Shared agent state.
pub struct AgentState {
    /// Pairing token presented to the relay.
    pub token: String,
    /// Running commands: request id -> child.
    pub commands: Mutex<std::collections::HashMap<u64, commands::RunningCommand>>,
}

impl AgentState {
    pub fn new(token: String) -> Self {
        Self {
            token,
            commands: Mutex::new(std::collections::HashMap::new()),
        }
    }
}

/// Run the agent: connect to the relay and serve requests, reconnecting
/// automatically after a dropped connection.
pub async fn run(
    base_url: &str,
    token: &str,
    id: &str,
    name: &str,
    reconnect_secs: u64,
) -> anyhow::Result<()> {
    let state = Arc::new(AgentState::new(token.to_string()));

    loop {
        match conn::run(base_url, token, id, name, state.clone()).await {
            Ok(()) => tracing::info!("connection closed cleanly"),
            Err(e) => tracing::error!("connection error: {e}"),
        }
        tracing::info!("reconnecting in {reconnect_secs}s");
        tokio::time::sleep(std::time::Duration::from_secs(reconnect_secs)).await;
    }
}
