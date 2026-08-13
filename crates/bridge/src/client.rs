//! Bridge <-> agent RPC client over the relay.

use std::collections::HashMap;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use relayfs_protocol::{Hello, PeerKind};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A pending request awaiting its response.
struct Pending {
    tx: tokio::sync::oneshot::Sender<Result<serde_json::Value, relayfs_protocol::RpcError>>,
}

/// Client to the paired agent, safe to share across MCP tool calls and
/// across threads (the FUSE layer calls it from its own runtime).
#[derive(Clone)]
pub struct AgentClient {
    /// Sender half, shared. The read loop owns the receiver half.
    sink: Arc<Mutex<futures::stream::SplitSink<WsStream, Message>>>,
    next_id: Arc<std::sync::atomic::AtomicU64>,
    pending: Arc<RwLock<HashMap<u64, Pending>>>,
    /// Agent id from the relay ack.
    pub agent_id: Arc<Mutex<Option<String>>>,
}

impl AgentClient {
    pub async fn connect(
        base_url: &str,
        token: &str,
        id: &str,
        name: &str,
    ) -> anyhow::Result<Self> {
        // rustls may be compiled with both aws-lc-rs and ring (feature
        // unification); pick ring explicitly so wss:// works.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let url = relayfs_rpc::relay_ws_url(base_url);
        let (ws, _) = connect_async(&url).await?;
        info!("connected to relay {url}");
        let (mut sink, mut stream) = ws.split();

        let hello = Hello {
            kind: PeerKind::Bridge,
            id: id.to_string(),
            name: name.to_string(),
            token: Some(token.to_string()),
        };
        sink.send(Message::Text(serde_json::to_string(&hello)?.into()))
            .await?;

        // Wait for hello_ack.
        let agent_id: Option<String> = loop {
            let Some(msg) = stream.next().await else {
                return Err(anyhow::anyhow!("relay closed during handshake"));
            };
            let msg = msg?;
            if let Message::Text(text) = msg {
                let value: serde_json::Value = serde_json::from_str(&text)?;
                if value.get("method").and_then(|m| m.as_str()) == Some("hello_ack") {
                    let id = value
                        .get("params")
                        .and_then(|p| p.get("agent_id"))
                        .and_then(|a| a.as_str())
                        .map(|s| s.to_string());
                    info!("handshake complete, agent: {:?}", id);
                    break id;
                }
            }
        };

        let client = Self {
            sink: Arc::new(Mutex::new(sink)),
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            pending: Arc::new(RwLock::new(HashMap::new())),
            agent_id: Arc::new(Mutex::new(agent_id)),
        };

        // Spawn the read loop; it owns the receiver half.
        let read_client = client.clone();
        tokio::spawn(async move {
            read_client.read_loop(stream).await;
        });

        Ok(client)
    }

    /// Read loop: route responses to pending requests, log notifications.
    async fn read_loop(&self, mut stream: futures::stream::SplitStream<WsStream>) {
        loop {
            let Some(msg) = stream.next().await else {
                break;
            };
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!("ws error: {e}");
                    break;
                }
            };
            let text = match msg {
                Message::Text(t) => t,
                Message::Close(_) => break,
                Message::Ping(p) => {
                    let _ = self.sink.lock().await.send(Message::Pong(p)).await;
                    continue;
                }
                _ => continue,
            };

            let value: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(e) => {
                    warn!("invalid frame: {e}");
                    continue;
                }
            };

            // Response?
            if let Some(id) = value.get("id").and_then(|v| v.as_u64()) {
                if let Some(pending) = self.pending.write().await.remove(&id) {
                    let result = if let Some(error) = value.get("error") {
                        Err(serde_json::from_value(error.clone()).unwrap_or_else(|_| {
                            relayfs_protocol::RpcError::new(
                                relayfs_protocol::code::INTERNAL_ERROR,
                                "malformed error response",
                            )
                        }))
                    } else {
                        Ok(value
                            .get("result")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null))
                    };
                    let _ = pending.tx.send(result);
                }
                continue;
            }

            // Notification.
            if let Some(method) = value.get("method").and_then(|m| m.as_str()) {
                match method {
                    "agent_connected" => {
                        if let Some(id) = value
                            .get("params")
                            .and_then(|p| p.get("agent_id"))
                            .and_then(|a| a.as_str())
                        {
                            *self.agent_id.lock().await = Some(id.to_string());
                            info!("agent connected: {id}");
                        }
                    }
                    "agent_disconnected" => {
                        info!("agent disconnected");
                        *self.agent_id.lock().await = None;
                    }
                    "command_finished" => {
                        if let Some(params) = value.get("params") {
                            let exit_code = params.get("exit_code").and_then(|c| c.as_i64());
                            let timed_out = params
                                .get("timed_out")
                                .and_then(|t| t.as_bool())
                                .unwrap_or(false);
                            match (exit_code, timed_out) {
                                (_, true) => info!("command finished: timed out"),
                                (Some(0), false) => info!("command finished: exit code 0"),
                                (Some(code), false) => {
                                    info!("command finished: exit code {code}")
                                }
                                (None, false) => info!("command finished: killed by signal"),
                            }
                        }
                    }
                    other => {
                        tracing::debug!("notification {other}");
                    }
                }
            }
        }
        // Read loop ended: fail all pending requests.
        let pending = std::mem::take(&mut *self.pending.write().await);
        for (_, p) in pending {
            let _ = p.tx.send(Err(relayfs_protocol::RpcError::new(
                relayfs_protocol::code::AGENT_OFFLINE,
                "connection to agent lost",
            )));
        }
    }

    /// Send a request and await the response.
    pub async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, relayfs_protocol::RpcError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending.write().await.insert(id, Pending { tx });

        let frame = relayfs_rpc::request_value(id, method, params);
        {
            let mut sink = self.sink.lock().await;
            if let Err(e) = sink.send(Message::Text(frame.to_string().into())).await {
                self.pending.write().await.remove(&id);
                return Err(relayfs_protocol::RpcError::new(
                    relayfs_protocol::code::INTERNAL_ERROR,
                    format!("send failed: {e}"),
                ));
            }
        }

        rx.await.map_err(|_| {
            relayfs_protocol::RpcError::new(
                relayfs_protocol::code::AGENT_OFFLINE,
                "connection to agent lost",
            )
        })?
    }

    /// Send a request and await the response, with a timeout.
    pub async fn call_timeout(
        &self,
        method: &str,
        params: serde_json::Value,
        timeout: std::time::Duration,
    ) -> Result<serde_json::Value, relayfs_protocol::RpcError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.pending.write().await.insert(id, Pending { tx });

        let frame = relayfs_rpc::request_value(id, method, params);
        {
            let mut sink = self.sink.lock().await;
            if let Err(e) = sink.send(Message::Text(frame.to_string().into())).await {
                self.pending.write().await.remove(&id);
                return Err(relayfs_protocol::RpcError::new(
                    relayfs_protocol::code::INTERNAL_ERROR,
                    format!("send failed: {e}"),
                ));
            }
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending.write().await.remove(&id);
                Err(relayfs_protocol::RpcError::new(
                    relayfs_protocol::code::AGENT_OFFLINE,
                    "connection to agent lost",
                ))
            }
            Err(_) => {
                self.pending.write().await.remove(&id);
                Err(relayfs_protocol::RpcError::new(
                    relayfs_protocol::code::CANCELLED,
                    "request timed out",
                ))
            }
        }
    }
}
