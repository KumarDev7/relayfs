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
pub struct AppState {
    /// agent token -> connected agents
    pub(crate) agents: Arc<RwLock<HashMap<Token, Vec<Arc<RelayPeer>>>>>,
    /// agent token -> connected bridges
    pub(crate) bridges: Arc<RwLock<HashMap<Token, Vec<Arc<RelayPeer>>>>>,
    /// If set, every peer must present this token.
    pub required_token: Option<String>,
}

impl AppState {
    pub fn new(required_token: Option<String>) -> Self {
        Self {
            agents: Arc::new(RwLock::new(HashMap::new())),
            bridges: Arc::new(RwLock::new(HashMap::new())),
            required_token,
        }
    }
}

pub fn make_router(state: AppState) -> Router {
    Router::new()
        .route("/ws", get(ws_handler))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

/// Run the relay server until the process is terminated.
pub async fn run(listen: &str, token: Option<&str>) -> anyhow::Result<()> {
    let state = AppState::new(token.map(String::from));
    let app = make_router(state);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    info!("relayfs relay listening on ws://{listen}/ws");
    axum::serve(listener, app).await?;
    Ok(())
}

struct RelayPeer {
    sink: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
    hello: Hello,
    /// Relay-assigned session id, reported to bridges via `list_targets`.
    session: String,
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

pub async fn ws_handler(State(state): State<AppState>, ws: WebSocketUpgrade) -> impl IntoResponse {
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

    let (peer, session) = match hello.kind {
        PeerKind::Agent => register_agent(&state, &token, hello.clone(), sink.clone()).await,
        PeerKind::Bridge => {
            let session = format!("{}-{}", hello.id, rand::random::<u32>());
            let peer = Arc::new(RelayPeer {
                sink: sink.clone(),
                hello: hello.clone(),
                session: session.clone(),
            });
            register_bridge(&state, &token, peer.clone()).await;
            (peer, session)
        }
    };

    // Ack.
    let agent_id = if peer.hello.kind == PeerKind::Bridge {
        state
            .agents
            .read()
            .await
            .get(&token)
            .and_then(|list| list.last())
            .map(|p| p.hello.id.clone())
    } else {
        Some(peer.hello.id.clone())
    };
    let ack = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "hello_ack",
        "params": HelloAck { session: session.clone(), agent_id },
    });
    let _ = peer.send_text(&ack.to_string()).await;

    info!(
        "{:?} {} connected (session {})",
        peer.hello.kind, peer.hello.id, session
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

        match peer.hello.kind {
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
                // Relay-local methods: answered by the relay itself, not
                // forwarded to the agent. The relay is the only component
                // that sees every connected target.
                if value.get("method").and_then(|m| m.as_str())
                    == Some(relayfs_protocol::method::LIST_TARGETS)
                {
                    let id = value.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
                    let agents = state.agents.read().await;
                    let targets: Vec<relayfs_protocol::TargetInfo> = agents
                        .values()
                        .flatten()
                        .map(|p| relayfs_protocol::TargetInfo {
                            id: p.hello.id.clone(),
                            name: p.hello.name.clone(),
                            session: p.session.clone(),
                        })
                        .collect();
                    let result =
                        serde_json::to_value(relayfs_protocol::ListTargetsResult { targets })
                            .unwrap_or_default();
                    let _ = peer.respond(id, Some(result), None).await;
                    continue;
                }
                // Request: route to the agent; if offline, answer with an error.
                let id = value.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
                let target_hint = value
                    .get("target")
                    .or_else(|| value.get("params").and_then(|p| p.get("target")))
                    .and_then(|t| t.as_str());
                let session_hint = value
                    .get("session")
                    .or_else(|| value.get("params").and_then(|p| p.get("session")))
                    .and_then(|s| s.as_str());

                let agent = {
                    let agents = state.agents.read().await;
                    agents.get(&token).and_then(|list| {
                        if let Some(target) = target_hint {
                            list.iter()
                                .find(|p| p.hello.id == target || p.session == target)
                                .cloned()
                        } else if let Some(session) = session_hint {
                            list.iter().find(|p| p.session == session).cloned()
                        } else {
                            list.last().cloned()
                        }
                    })
                };
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
    match peer.hello.kind {
        PeerKind::Agent => {
            let removed = {
                let mut agents = state.agents.write().await;
                if let Some(list) = agents.get_mut(&token) {
                    let prev_len = list.len();
                    list.retain(|b| !Arc::ptr_eq(b, &peer));
                    let was_removed = list.len() < prev_len;
                    if list.is_empty() {
                        agents.remove(&token);
                    }
                    was_removed
                } else {
                    false
                }
            };
            if removed {
                notify_bridges_agent_gone(&state, &token, &peer.hello.id).await;
            }
            info!("agent {} disconnected", peer.hello.id);
        }
        PeerKind::Bridge => {
            if let Some(bridges) = state.bridges.write().await.get_mut(&token) {
                bridges.retain(|b| !Arc::ptr_eq(b, &peer));
            }
            info!("bridge {} disconnected", peer.hello.id);
        }
    }
}

async fn register_agent(
    state: &AppState,
    token: &str,
    mut hello: Hello,
    sink: Arc<Mutex<futures::stream::SplitSink<WebSocket, Message>>>,
) -> (Arc<RelayPeer>, String) {
    let mut agents = state.agents.write().await;
    let id_conflict = agents.values().flatten().any(|p| p.hello.id == hello.id);
    if id_conflict {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut suffixed = format!("{}-{timestamp}", hello.id);
        if agents.values().flatten().any(|p| p.hello.id == suffixed) {
            suffixed = format!("{}-{timestamp}-{:04x}", hello.id, rand::random::<u16>());
        }
        info!(
            "target id collision for '{}'; assigning suffixed id '{}'",
            hello.id, suffixed
        );
        hello.id = suffixed;
    }

    let session = format!("{}-{}", hello.id, rand::random::<u32>());
    let peer = Arc::new(RelayPeer {
        sink,
        hello: hello.clone(),
        session: session.clone(),
    });
    agents.entry(token.to_string()).or_default().push(peer.clone());
    drop(agents);

    let value = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "agent_connected",
        "params": { "agent_id": peer.hello.id, "name": peer.hello.name },
    });
    forward_to_bridges(state, token, &value).await;

    (peer, session)
}

async fn register_bridge(state: &AppState, token: &str, peer: Arc<RelayPeer>) {
    let mut bridges = state.bridges.write().await;
    bridges.entry(token.to_string()).or_default().push(peer);
}

async fn forward_to_agent(state: &AppState, token: &str, value: &serde_json::Value) {
    let target_hint = value
        .get("target")
        .or_else(|| value.get("params").and_then(|p| p.get("target")))
        .and_then(|t| t.as_str());
    let agents = state
        .agents
        .read()
        .await
        .get(token)
        .cloned()
        .unwrap_or_default();
    for agent in agents {
        if let Some(target) = target_hint {
            if agent.hello.id != target && agent.session != target {
                continue;
            }
        }
        if let Err(e) = agent.send_text(&value.to_string()).await {
            error!("forward to agent failed: {e}");
        }
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
