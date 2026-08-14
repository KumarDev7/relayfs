//! Agent connection loop: connect to the relay, handshake, dispatch requests.

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use relayfs_protocol::{Hello, PeerKind};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tracing::{error, info, warn};

use crate::AgentState;

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
/// Shared write half of the WebSocket. Requests run as spawned tasks, so they
/// share the sink instead of owning `&mut WsStream` (which is not `Clone`).
pub type WsSink = Arc<Mutex<futures::stream::SplitSink<WsStream, Message>>>;

pub async fn run(
    base_url: &str,
    token: &str,
    id: &str,
    name: &str,
    state: Arc<AgentState>,
) -> anyhow::Result<()> {
    // rustls may be compiled with both aws-lc-rs and ring (feature
    // unification); pick ring explicitly so wss:// works.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let url = relayfs_rpc::relay_ws_url(base_url);
    let (ws, _) = connect_async(&url).await?;
    info!("connected to relay {url}");
    let mut ws = ws;

    // Handshake.
    let hello = Hello {
        kind: PeerKind::Agent,
        id: id.to_string(),
        name: name.to_string(),
        token: Some(token.to_string()),
    };
    ws.send(Message::Text(serde_json::to_string(&hello)?.into()))
        .await?;

    // Wait for hello_ack.
    loop {
        let Some(msg) = ws.next().await else {
            return Ok(());
        };
        let msg = msg?;
        if let Message::Text(text) = msg {
            let value: serde_json::Value = serde_json::from_str(&text)?;
            if value.get("method").and_then(|m| m.as_str()) == Some("hello_ack") {
                info!("handshake complete: {}", value);
                break;
            }
        }
    }

    // Keepalive is handled by the relay: it pings every connected peer every
    // 30s, and we pong in the read loop below — real traffic on the wire keeps
    // the connection alive through intermediaries (Cloudflare, nginx).

    // Split into reader/writer. The writer is shared: every request runs in
    // its own spawned task so one long-running command (a multi-minute
    // training script, a slow file copy) never blocks pongs, keepalives, or
    // other requests. The read loop only parses frames and spawns; it never
    // awaits handler completion.
    let (sink, mut stream) = ws.split();
    let sink: WsSink = Arc::new(Mutex::new(sink));

    while let Some(msg) = stream.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!("ws error: {e}");
                break;
            }
        };
        match msg {
            Message::Text(text) => {
                let value: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("invalid frame: {e}");
                        continue;
                    }
                };
                let sink = sink.clone();
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = dispatch(&sink, &state, value).await {
                        error!("dispatch error: {e}");
                    }
                });
            }
            Message::Close(_) => break,
            Message::Ping(p) => {
                sink.lock().await.send(Message::Pong(p)).await?;
            }
            _ => {}
        }
    }

    // Kill any commands still running for this session.
    crate::commands::kill_all(&state).await;
    Ok(())
}

/// Dispatch one JSON-RPC request from the bridge. Runs inside a spawned task;
/// long-running handlers must not hold the sink lock while waiting (each
/// `send_*` acquires it only for the single frame write).
async fn dispatch(
    sink: &WsSink,
    state: &AgentState,
    value: serde_json::Value,
) -> anyhow::Result<()> {
    let Some(id) = value.get("id").and_then(|v| v.as_u64()) else {
        // Notifications from the bridge: none defined yet.
        return Ok(());
    };
    let method = value
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or("")
        .to_string();
    let params = value
        .get("params")
        .cloned()
        .unwrap_or(serde_json::Value::Null);

    // Transparency: log every request the agent executes, with its arguments.
    tracing::info!("executing {method}: {}", params);

    let result = match method.as_str() {
        relayfs_protocol::method::RUN_COMMAND => {
            crate::commands::run_command(sink, state, id, params).await
        }
        relayfs_protocol::method::READ_FILE => crate::files::read_file(params).await,
        relayfs_protocol::method::WRITE_FILE => crate::files::write_file(params).await,
        relayfs_protocol::method::LIST_DIR => crate::files::list_dir(params).await,
        relayfs_protocol::method::STAT => crate::files::stat(params).await,
        relayfs_protocol::method::MKDIR => crate::files::mkdir(params).await,
        relayfs_protocol::method::REMOVE => crate::files::remove(params).await,
        relayfs_protocol::method::RENAME => crate::files::rename(params).await,
        relayfs_protocol::method::COPY => crate::files::copy(params).await,
        relayfs_protocol::method::WRITE_AT => crate::files::write_at(params).await,
        relayfs_protocol::method::TRUNCATE => crate::files::truncate(params).await,
        relayfs_protocol::method::SYMLINK => crate::files::symlink(params).await,
        relayfs_protocol::method::CHMOD => crate::files::chmod(params).await,
        relayfs_protocol::method::STREAM_FILE => crate::files::stream_file(sink, id, params).await,
        relayfs_protocol::method::PING => Ok(serde_json::to_value(relayfs_protocol::PingResult {
            ok: true,
            hostname: hostname(),
            pid: std::process::id(),
        })?),
        other => {
            let err = relayfs_protocol::RpcError::new(
                relayfs_protocol::code::METHOD_NOT_FOUND,
                format!("unknown method: {other}"),
            );
            send_response(sink, id, None, Some(err)).await?;
            return Ok(());
        }
    };

    match result {
        Ok(result) => send_response(sink, id, Some(result), None).await?,
        Err(e) => {
            let err = relayfs_protocol::RpcError::new(
                relayfs_protocol::code::INTERNAL_ERROR,
                e.to_string(),
            );
            send_response(sink, id, None, Some(err)).await?;
        }
    }
    Ok(())
}

pub async fn send_response(
    sink: &WsSink,
    id: u64,
    result: Option<serde_json::Value>,
    error: Option<relayfs_protocol::RpcError>,
) -> anyhow::Result<()> {
    let value = relayfs_rpc::response_value(id, result, error);
    sink.lock()
        .await
        .send(Message::Text(value.to_string().into()))
        .await?;
    Ok(())
}

pub async fn send_notification(
    sink: &WsSink,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<()> {
    let value = relayfs_rpc::notification_value(method, params);
    sink.lock()
        .await
        .send(Message::Text(value.to_string().into()))
        .await?;
    Ok(())
}

fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into())
}
