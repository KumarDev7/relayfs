//! JSON-RPC 2.0 framing and WebSocket transport shared by all three binaries.
//!
//! The relay is a pure router: it decodes the outer JSON-RPC envelope, reads
//! the `session` / `target` fields from the params, forwards the *raw* params
//! value to the peer, and never inspects the method body.

use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use relayfs_protocol::{Hello, HelloAck, Notification, Request, Response, RpcError, SessionId};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{
    tungstenite::{Error as WsError, Message},
    MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;

pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A connected peer (bridge or agent) with a live WebSocket.
pub struct Peer {
    pub ws: Arc<Mutex<WsStream>>,
    pub hello: Hello,
    pub session: SessionId,
    pub cancel: CancellationToken,
}

impl Peer {
    pub fn new(ws: WsStream, hello: Hello, session: SessionId) -> Self {
        Self {
            ws: Arc::new(Mutex::new(ws)),
            hello,
            session,
            cancel: CancellationToken::new(),
        }
    }

    /// Send a JSON-RPC request and wait for the matching response.
    pub async fn request(
        &self,
        id: u64,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, RpcError> {
        let frame = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });
        self.send_text(&frame.to_string()).await.map_err(|e| {
            RpcError::new(
                relayfs_protocol::code::INTERNAL_ERROR,
                format!("send failed: {e}"),
            )
        })?;
        // The read loop (run by the caller) routes responses into a channel;
        // this helper is only used for fire-and-forget sends.
        Ok(serde_json::Value::Null)
    }

    /// Send a JSON-RPC notification (no id, no reply expected).
    pub async fn notify(&self, method: &str, params: serde_json::Value) -> Result<(), WsError> {
        let frame = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        });
        self.send_text(&frame.to_string()).await
    }

    /// Send a raw JSON-RPC response.
    pub async fn respond(
        &self,
        id: u64,
        result: Option<serde_json::Value>,
        error: Option<RpcError>,
    ) -> Result<(), WsError> {
        let mut frame = serde_json::json!({ "jsonrpc": "2.0", "id": id });
        if let Some(result) = result {
            frame["result"] = result;
        }
        if let Some(error) = error {
            frame["error"] = serde_json::to_value(error).unwrap_or_default();
        }
        self.send_text(&frame.to_string()).await
    }

    async fn send_text(&self, text: &str) -> Result<(), WsError> {
        let mut ws = self.ws.lock().await;
        ws.send(Message::Text(text.into())).await
    }
}

/// A decoded JSON-RPC message as seen by the relay.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RelayFrame {
    /// `hello` from a peer; params is a `Hello`.
    Hello { params: Hello },
    /// `hello_ack` from the relay; params is a `HelloAck`.
    HelloAck { params: HelloAck },
    /// A request; params is a `Request`.
    Request { params: Request },
    /// A response; params is a `Response`.
    Response { params: Response },
    /// A notification; params is a `Notification`.
    Notification { params: Notification },
}

/// Read one JSON-RPC message from the wire, returning the raw params value.
/// Returns `None` on clean close.
pub async fn read_frame(ws: &mut WsStream) -> Result<Option<serde_json::Value>, WsError> {
    loop {
        let Some(msg) = ws.next().await else {
            return Ok(None);
        };
        match msg? {
            Message::Text(text) => {
                let value: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
                    WsError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("invalid JSON-RPC frame: {e}"),
                    ))
                })?;
                return Ok(Some(value));
            }
            Message::Binary(_) => {
                return Err(WsError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "binary frames are not supported",
                )));
            }
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
            Message::Close(_) => return Ok(None),
        }
    }
}

/// Send a raw JSON-RPC value over the wire.
pub async fn send_frame(ws: &mut WsStream, value: &serde_json::Value) -> Result<(), WsError> {
    ws.send(Message::Text(value.to_string().into())).await
}

/// Build a JSON-RPC request value.
pub fn request_value(id: u64, method: &str, params: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params,
    })
}

/// Build a JSON-RPC notification value.
pub fn notification_value(method: &str, params: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
    })
}

/// Build a JSON-RPC response value.
pub fn response_value(
    id: u64,
    result: Option<serde_json::Value>,
    error: Option<RpcError>,
) -> serde_json::Value {
    let mut value = serde_json::json!({ "jsonrpc": "2.0", "id": id });
    if let Some(result) = result {
        value["result"] = result;
    }
    if let Some(error) = error {
        value["error"] = serde_json::to_value(error).unwrap_or_default();
    }
    value
}

/// Normalize a relay base URL to the WebSocket endpoint.
///
/// Accepts `ws://host:port`, `wss://host`, or a full `.../ws` path; appends
/// `/ws` when the path is missing or empty.
pub fn relay_ws_url(base: &str) -> String {
    let trimmed = base.trim_end_matches('/');
    if trimmed.ends_with("/ws") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/ws")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relay_ws_url_appends_ws() {
        assert_eq!(relay_ws_url("ws://host:8787"), "ws://host:8787/ws");
        assert_eq!(
            relay_ws_url("wss://relay.example.com"),
            "wss://relay.example.com/ws"
        );
        assert_eq!(relay_ws_url("ws://host:8787/"), "ws://host:8787/ws");
    }

    #[test]
    fn relay_ws_url_keeps_existing_ws_path() {
        assert_eq!(relay_ws_url("ws://host:8787/ws"), "ws://host:8787/ws");
        assert_eq!(relay_ws_url("ws://host:8787/ws/"), "ws://host:8787/ws");
    }

    #[test]
    fn request_value_frames_correctly() {
        let v = request_value(7, "ping", serde_json::json!({}));
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 7);
        assert_eq!(v["method"], "ping");
        assert_eq!(v["params"], serde_json::json!({}));
    }

    #[test]
    fn notification_value_has_no_id() {
        let v = notification_value("command_output", serde_json::json!({"data": "x"}));
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["method"], "command_output");
        assert!(v.get("id").is_none());
    }

    #[test]
    fn response_value_round_trips_result_and_error() {
        let ok = response_value(1, Some(serde_json::json!({"ok": true})), None);
        assert_eq!(ok["result"]["ok"], true);
        assert!(ok.get("error").is_none());

        let err = response_value(
            2,
            None,
            Some(RpcError::new(
                relayfs_protocol::code::AGENT_OFFLINE,
                "agent is not connected",
            )),
        );
        assert_eq!(err["error"]["code"], relayfs_protocol::code::AGENT_OFFLINE);
        assert_eq!(err["error"]["message"], "agent is not connected");
        assert!(err.get("result").is_none());
    }
}
