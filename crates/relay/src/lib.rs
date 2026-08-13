//! relayfs relay — public WebSocket hub (library).
//!
//! Both the bridge (local MCP server) and the agent (remote machine) connect
//! *out* to this server, so neither needs a public IP or open ports. The relay
//! pairs them by a shared token and forwards JSON-RPC frames between them
//! without inspecting the method bodies.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{SinkExt, StreamExt};
use relayfs_protocol::{Hello, HelloAck, PeerKind};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info, warn};

/// Pairing key shared by one bridge and one agent.
type Token = String;

#[derive(Clone)]
struct AppState {
    /// agent token -> connected agent
    agents: Arc<RwLock<HashMap<Token, Arc<RelayPeer>>>>,
    /// agent token -> connected bridges
    bridges: Arc<RwLock<HashMap<Token, Vec<Arc<RelayPeer>>>>>,
    /// If set, every peer must present this token.
    required_token: Option<String>,
}

struct RelayPeer {
    sink: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    hello: Hello,
}

impl RelayPeer {
    async fn send_text(&self, text: &str) -> Result<(), axum::Error> {
        let mut sink = self.sink.lock().await;
        sink.send(Message::Text(text.into())).await
    }

    async fn respond(
        &self,
        id: u64,
        result: Option<serde_json::Value>,
        error: Option<relayfs_protocol::RpcError>,
    ) -> Result<(), axum::Error> {
        let mut frame = serde_json::json!({ "jsonrpc": "2.0", "id": id });
        if let Some(result) = result {
            frame["result"] = result;
        }
        if let Some(error) = error {
            frame["error"] = serde_json::to_value(error).unwrap_or_default();
        }
        self.send_text(&frame.to_string()).await
    }
}

/// Run the relay server until the process is terminated.
pub async fn run(listen: &str, token: Option<&str>) -> anyhow::Result<()> {
    let state = AppState {
        agents: Arc::new(RwLock::new(HashMap::new())),
        bridges: Arc::new(RwLock::new(HashMap::new())),
        required_token: token.map(String::from),
    };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!("relayfs relay listening on ws://{listen}/ws");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_handler(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(state, socket))
}

/// Read one text frame; returns `None` on close. Binary frames are rejected.
async fn read_frame(
    stream: &mut futures::stream::SplitStream<WebSocket>,
) -> Option<serde_json::Value> {
    loop {
        let msg = stream.next().await?;
        match msg {
            Ok(Message::Text(text)) => match serde_json::from_str(&text) {
                Ok(value) => return Some(value),
                Err(e) => {
                    warn!("invalid JSON frame: {e}");
                    return None;
                }
            },
            Ok(Message::Binary(_)) => {
                warn!("binary frames not supported");
                return None;
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
            Ok(Message::Close(_)) => return None,
            Err(e) => {
                warn!("ws error: {e}");
                return None;
            }
        }
    }
}

async fn handle_connection(state: AppState, socket: WebSocket) {
    let (sink, mut stream) = socket.split();
    let sink = Arc::new(Mutex::new(sink));

    // First frame must be a hello.
    let Some(value) = read_frame(&mut stream).await else {
        warn!("peer disconnected before hello");
        return;
    };
    let hello: Hello = match serde_json::from_value(value) {
        Ok(h) => h,
        Err(e) => {
            warn!("invalid hello: {e}");
            return;
        }
    };

    // Token validation.
    let token = match hello.token.as_deref() {
        Some(t) if !t.is_empty() => t.to_string(),
        _ => {
            warn!("peer {} presented no token", hello.id);
            return;
        }
    };
    if let Some(required) = &state.required_token {
        if &token != required {
            warn!("peer {} rejected: bad token", hello.id);
            return;
        }
    }

    let session = format!("{}-{}", hello.id, rand::random::<u32>());
    let peer = Arc::new(RelayPeer {
        sink: sink.clone(),
        hello: hello.clone(),
    });

    match hello.kind {
        PeerKind::Agent => register_agent(&state, &token, peer.clone()).await,
        PeerKind::Bridge => register_bridge(&state, &token, peer.clone()).await,
    }

    // Ack.
    let agent_id = if hello.kind == PeerKind::Bridge {
        state
            .agents
            .read()
            .await
            .get(&token)
            .map(|p| p.hello.id.clone())
    } else {
        None
    };
    let ack = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "hello_ack",
        "params": HelloAck { session: session.clone(), agent_id },
    });
    let _ = peer.send_text(&ack.to_string()).await;

    info!(
        "{:?} {} connected (session {})",
        hello.kind, hello.id, session
    );

    // Keepalive: ping every 30s so idle connections survive intermediary
    // idle timeouts (e.g. Cloudflare closes idle WebSockets after ~100s).
    // Both legs (agent and bridge) pass through such intermediaries, and the
    // peer answers with a pong — real traffic on the wire keeps the path open.
    {
        let peer = peer.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                let mut sink = peer.sink.lock().await;
                if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                    break;
                }
            }
        });
    }

    // Read loop.
    loop {
        let Some(value) = read_frame(&mut stream).await else {
            break;
        };

        let is_response = value.get("id").is_some() && value.get("method").is_none();
        let is_notification = value.get("method").is_some() && value.get("id").is_none();

        match hello.kind {
            PeerKind::Agent => {
                // Responses and notifications flow agent -> bridge.
                if is_response || is_notification {
                    forward_to_bridges(&state, &token, &value).await;
                }
            }
            PeerKind::Bridge => {
                if is_response {
                    // Bridges don't answer requests; ignore.
                    continue;
                }
                if is_notification {
                    forward_to_agent(&state, &token, &value).await;
                    continue;
                }
                // Request: route to the agent; if offline, answer with an error.
                let id = value.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
                let agent = state.agents.read().await.get(&token).cloned();
                match agent {
                    Some(agent) => {
                        if let Err(e) = agent.send_text(&value.to_string()).await {
                            error!("forward to agent failed: {e}");
                            let _ = peer
                                .respond(
                                    id,
                                    None,
                                    Some(relayfs_protocol::RpcError::new(
                                        relayfs_protocol::code::INTERNAL_ERROR,
                                        format!("agent forward failed: {e}"),
                                    )),
                                )
                                .await;
                        }
                    }
                    None => {
                        let _ = peer
                            .respond(
                                id,
                                None,
                                Some(relayfs_protocol::RpcError::new(
                                    relayfs_protocol::code::AGENT_OFFLINE,
                                    "agent is not connected",
                                )),
                            )
                            .await;
                    }
                }
            }
        }
    }

    // Cleanup.
    match hello.kind {
        PeerKind::Agent => {
            state.agents.write().await.remove(&token);
            notify_bridges_agent_gone(&state, &token, &hello.id).await;
            info!("agent {} disconnected", hello.id);
        }
        PeerKind::Bridge => {
            if let Some(bridges) = state.bridges.write().await.get_mut(&token) {
                bridges.retain(|b| !Arc::ptr_eq(b, &peer));
            }
            info!("bridge {} disconnected", hello.id);
        }
    }
}

async fn register_agent(state: &AppState, token: &str, peer: Arc<RelayPeer>) {
    let mut agents = state.agents.write().await;
    if let Some(old) = agents.insert(token.to_string(), peer.clone()) {
        warn!("agent {} replaced by {}", old.hello.id, peer.hello.id);
    }
    drop(agents);
    let value = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "agent_connected",
        "params": { "agent_id": peer.hello.id, "name": peer.hello.name },
    });
    forward_to_bridges(state, token, &value).await;
}

async fn register_bridge(state: &AppState, token: &str, peer: Arc<RelayPeer>) {
    let mut bridges = state.bridges.write().await;
    bridges.entry(token.to_string()).or_default().push(peer);
}

async fn forward_to_agent(state: &AppState, token: &str, value: &serde_json::Value) {
    let Some(agent) = state.agents.read().await.get(token).cloned() else {
        return;
    };
    if let Err(e) = agent.send_text(&value.to_string()).await {
        error!("forward to agent failed: {e}");
    }
}

async fn forward_to_bridges(state: &AppState, token: &str, value: &serde_json::Value) {
    let bridges = state
        .bridges
        .read()
        .await
        .get(token)
        .cloned()
        .unwrap_or_default();
    for bridge in bridges {
        if let Err(e) = bridge.send_text(&value.to_string()).await {
            error!("forward to bridge failed: {e}");
        }
    }
}

async fn notify_bridges_agent_gone(state: &AppState, token: &str, agent_id: &str) {
    let value = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "agent_disconnected",
        "params": { "agent_id": agent_id },
    });
    forward_to_bridges(state, token, &value).await;
}
