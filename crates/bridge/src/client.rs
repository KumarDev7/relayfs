//! Bridge <-> agent RPC client over the relay.
//!
//! The client keeps a single WebSocket to the relay and reconnects
//! automatically when the connection drops, so the MCP session never needs to
//! restart. A supervisor task owns the connection lifecycle: connect ->
//! handshake -> install sink -> spawn read loop + keepalive pings; when the
//! read loop ends it clears the sink and retries after a short delay.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use relayfs_protocol::{Hello, PeerKind, RpcError};
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type WsSink = futures::stream::SplitSink<WsStream, Message>;

/// A pending request awaiting its response.
struct Pending {
    tx: oneshot::Sender<Result<serde_json::Value, RpcError>>,
}

/// Client to the paired agent, safe to share across MCP tool calls and
/// across threads (the FUSE layer calls it from its own runtime).
#[derive(Clone)]
pub struct AgentClient {
    base_url: String,
    token: String,
    id: String,
    name: String,
    /// Sink of the current connection; `None` while offline/reconnecting.
    /// The supervisor task is the only writer.
    sink: Arc<Mutex<Option<WsSink>>>,
    next_id: Arc<AtomicU64>,
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

        let client = Self {
            base_url: base_url.to_string(),
            token: token.to_string(),
            id: id.to_string(),
            name: name.to_string(),
            sink: Arc::new(Mutex::new(None)),
            next_id: Arc::new(AtomicU64::new(1)),
            pending: Arc::new(RwLock::new(HashMap::new())),
            agent_id: Arc::new(Mutex::new(None)),
        };

        let supervisor = client.clone();
        tokio::spawn(async move {
            supervisor.supervisor().await;
        });

        // Best-effort wait for the first connection; the supervisor keeps
        // retrying in the background either way.
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if client.sink.lock().await.is_some() {
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        Ok(client)
    }

    /// Connection lifecycle: loop connect -> handshake -> serve until the
    /// read loop ends, then retry.
    async fn supervisor(&self) {
        const RETRY: Duration = Duration::from_secs(5);
        loop {
            match Self::open_connection(self).await {
                Ok((sink, stream)) => {
                    *self.sink.lock().await = Some(sink);
                    info!(
                        "connected to relay, agent: {:?}",
                        self.agent_id.lock().await
                    );

                    let (end_tx, end_rx) = oneshot::channel();
                    {
                        // Read loop: routes responses to pending requests.
                        let read_client = self.clone();
                        tokio::spawn(async move {
                            read_client.read_loop(stream).await;
                            let _ = end_tx.send(());
                        });
                    }
                    {
                        // Keepalive: ping every 25s so intermediaries
                        // (Cloudflare, nginx) don't close the idle connection.
                        let ping_sink = self.sink.clone();
                        tokio::spawn(async move {
                            let mut tick = tokio::time::interval(Duration::from_secs(25));
                            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            loop {
                                tick.tick().await;
                                let mut guard = ping_sink.lock().await;
                                match guard.as_mut() {
                                    Some(sink) => {
                                        if sink
                                            .send(Message::Ping(Vec::new().into()))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                    None => break,
                                }
                            }
                        });
                    }

                    let _ = end_rx.await;
                    // Read loop ended: the connection is dead. Clear the sink
                    // slot, then the outer loop reconnects.
                    *self.sink.lock().await = None;
                    info!("relay connection lost; reconnecting in 5s");
                }
                Err(e) => {
                    warn!("relay connect failed: {e}; retrying in 5s");
                }
            }
            tokio::time::sleep(RETRY).await;
        }
    }

    /// Connect to the relay, handshake, and return the split connection.
    async fn open_connection(
        &self,
    ) -> anyhow::Result<(WsSink, futures::stream::SplitStream<WsStream>)> {
        let url = relayfs_rpc::relay_ws_url(&self.base_url);
        let (ws, _) = connect_async(&url).await?;
        let (mut sink, mut stream) = ws.split();

        let hello = Hello {
            kind: PeerKind::Bridge,
            id: self.id.clone(),
            name: self.name.clone(),
            token: Some(self.token.clone()),
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
                    break id;
                }
            }
        };
        *self.agent_id.lock().await = agent_id;

        Ok((sink, stream))
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
                    let mut guard = self.sink.lock().await;
                    if let Some(sink) = guard.as_mut() {
                        let _ = sink.send(Message::Pong(p)).await;
                    }
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
                            RpcError::new(
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
            let _ = p.tx.send(Err(RpcError::new(
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
    ) -> Result<serde_json::Value, RpcError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.write().await.insert(id, Pending { tx });

        let frame = relayfs_rpc::request_value(id, method, params);
        {
            let mut guard = self.sink.lock().await;
            match guard.as_mut() {
                None => {
                    self.pending.write().await.remove(&id);
                    return Err(RpcError::new(
                        relayfs_protocol::code::AGENT_OFFLINE,
                        "relay connection offline (reconnecting)",
                    ));
                }
                Some(sink) => {
                    if let Err(e) = sink.send(Message::Text(frame.to_string().into())).await {
                        self.pending.write().await.remove(&id);
                        return Err(RpcError::new(
                            relayfs_protocol::code::INTERNAL_ERROR,
                            format!("send failed: {e}"),
                        ));
                    }
                }
            }
        }

        rx.await.map_err(|_| {
            RpcError::new(
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
        timeout: Duration,
    ) -> Result<serde_json::Value, RpcError> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.write().await.insert(id, Pending { tx });

        let frame = relayfs_rpc::request_value(id, method, params);
        {
            let mut guard = self.sink.lock().await;
            match guard.as_mut() {
                None => {
                    self.pending.write().await.remove(&id);
                    return Err(RpcError::new(
                        relayfs_protocol::code::AGENT_OFFLINE,
                        "relay connection offline (reconnecting)",
                    ));
                }
                Some(sink) => {
                    if let Err(e) = sink.send(Message::Text(frame.to_string().into())).await {
                        self.pending.write().await.remove(&id);
                        return Err(RpcError::new(
                            relayfs_protocol::code::INTERNAL_ERROR,
                            format!("send failed: {e}"),
                        ));
                    }
                }
            }
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                self.pending.write().await.remove(&id);
                Err(RpcError::new(
                    relayfs_protocol::code::AGENT_OFFLINE,
                    "connection to agent lost",
                ))
            }
            Err(_) => {
                self.pending.write().await.remove(&id);
                Err(RpcError::new(
                    relayfs_protocol::code::CANCELLED,
                    "request timed out",
                ))
            }
        }
    }
}
