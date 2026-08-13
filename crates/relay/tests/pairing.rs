//! End-to-end relay test: a fake agent and fake bridge pair over the real
//! WebSocket server, and a request is routed agent -> bridge and back.

use futures::{SinkExt, StreamExt};
use relayfs_protocol::{Hello, HelloAck, PeerKind};
use tokio_tungstenite::tungstenite::Message;

const TOKEN: &str = "test-token";

async fn connect(
    port: u16,
    hello: Hello,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://127.0.0.1:{port}/ws");
    // The relay task binds asynchronously; retry until it accepts.
    let mut ws = None;
    for _ in 0..100 {
        match tokio_tungstenite::connect_async(&url).await {
            Ok((w, _)) => {
                ws = Some(w);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    }
    let mut ws = ws.expect("relay did not accept connection");
    ws.send(Message::Text(serde_json::to_string(&hello).unwrap().into()))
        .await
        .unwrap();
    ws
}

async fn recv_json(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
) -> serde_json::Value {
    loop {
        let msg = ws.next().await.unwrap().unwrap();
        if let Message::Text(text) = msg {
            return serde_json::from_str(&text).unwrap();
        }
    }
}

#[tokio::test]
async fn pairs_agent_and_bridge_and_routes_requests() {
    // Bind an ephemeral port, then run the relay on it.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let relay = tokio::spawn(async move {
        relayfs_relay::run(&format!("127.0.0.1:{port}"), Some(TOKEN))
            .await
            .unwrap();
    });

    // Agent connects first.
    let mut agent = connect(
        port,
        Hello {
            kind: PeerKind::Agent,
            id: "test-agent".into(),
            name: "agent".into(),
            token: Some(TOKEN.into()),
        },
    )
    .await;
    let ack: HelloAck =
        serde_json::from_value(recv_json(&mut agent).await["params"].clone()).unwrap();
    assert!(!ack.session.is_empty());

    // Bridge connects; its ack must carry the agent id.
    let mut bridge = connect(
        port,
        Hello {
            kind: PeerKind::Bridge,
            id: "test-bridge".into(),
            name: "bridge".into(),
            token: Some(TOKEN.into()),
        },
    )
    .await;
    let ack: HelloAck =
        serde_json::from_value(recv_json(&mut bridge).await["params"].clone()).unwrap();
    assert_eq!(ack.agent_id.as_deref(), Some("test-agent"));

    // Bridge sends a request; the agent must receive it verbatim.
    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 42,
        "method": "ping",
        "params": {},
    });
    bridge
        .send(Message::Text(request.to_string().into()))
        .await
        .unwrap();
    let received = recv_json(&mut agent).await;
    assert_eq!(received["id"], 42);
    assert_eq!(received["method"], "ping");

    // Agent answers; the bridge must receive the response.
    let response = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 42,
        "result": { "ok": true },
    });
    agent
        .send(Message::Text(response.to_string().into()))
        .await
        .unwrap();
    let received = recv_json(&mut bridge).await;
    assert_eq!(received["id"], 42);
    assert_eq!(received["result"]["ok"], true);

    relay.abort();
}

#[tokio::test]
async fn rejects_wrong_token() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let relay = tokio::spawn(async move {
        relayfs_relay::run(&format!("127.0.0.1:{port}"), Some(TOKEN))
            .await
            .unwrap();
    });

    let mut agent = connect(
        port,
        Hello {
            kind: PeerKind::Agent,
            id: "bad-agent".into(),
            name: "agent".into(),
            token: Some("wrong-token".into()),
        },
    )
    .await;
    // The relay drops the connection without acking — either a close frame
    // or a socket reset.
    let outcome = agent.next().await;
    match outcome {
        Some(Ok(Message::Close(_))) => {}
        Some(Err(_)) => {}
        other => panic!("expected rejection, got {other:?}"),
    }

    relay.abort();
}

#[tokio::test]
async fn replacing_agent_closes_old_and_keeps_new() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let relay = tokio::spawn(async move {
        relayfs_relay::run(&format!("127.0.0.1:{port}"), Some(TOKEN))
            .await
            .unwrap();
    });

    // First agent connects.
    let mut agent_a = connect(
        port,
        Hello {
            kind: PeerKind::Agent,
            id: "agent-a".into(),
            name: "a".into(),
            token: Some(TOKEN.into()),
        },
    )
    .await;
    let _ = recv_json(&mut agent_a).await; // hello_ack

    // A second agent with the same token replaces it: the relay must close
    // the displaced connection so it doesn't linger as a zombie.
    let mut agent_b = connect(
        port,
        Hello {
            kind: PeerKind::Agent,
            id: "agent-b".into(),
            name: "b".into(),
            token: Some(TOKEN.into()),
        },
    )
    .await;
    let _ = recv_json(&mut agent_b).await; // hello_ack
                                           // The relay must close the displaced connection (skipping keepalive
                                           // pings that may arrive first).
    let displaced_closed = loop {
        match agent_a.next().await {
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break true,
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
            other => {
                panic!("expected displaced agent to be closed, got {other:?}")
            }
        }
    };
    assert!(displaced_closed, "expected displaced agent to be closed");
    // Give the relay time to run the displaced connection's cleanup; its
    // stale cleanup must NOT remove the new agent.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // A bridge sees agent-b, and requests route to it.
    let mut bridge = connect(
        port,
        Hello {
            kind: PeerKind::Bridge,
            id: "test-bridge".into(),
            name: "bridge".into(),
            token: Some(TOKEN.into()),
        },
    )
    .await;
    let ack: HelloAck =
        serde_json::from_value(recv_json(&mut bridge).await["params"].clone()).unwrap();
    assert_eq!(ack.agent_id.as_deref(), Some("agent-b"));

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "ping",
        "params": {},
    });
    bridge
        .send(Message::Text(request.to_string().into()))
        .await
        .unwrap();
    let received = recv_json(&mut agent_b).await;
    assert_eq!(received["id"], 1);

    // When agent-b leaves, requests must start failing with AGENT_OFFLINE.
    drop(agent_b);
    let mut saw_offline = false;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        bridge
            .send(Message::Text(request.to_string().into()))
            .await
            .unwrap();
        let response = recv_json(&mut bridge).await;
        if let Some(code) = response.get("error").and_then(|e| e.get("code")) {
            assert_eq!(code, relayfs_protocol::code::AGENT_OFFLINE);
            saw_offline = true;
            break;
        }
    }
    assert!(saw_offline, "expected AGENT_OFFLINE after agent-b left");

    relay.abort();
}

#[tokio::test]
async fn answers_agent_offline_when_no_agent() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let relay = tokio::spawn(async move {
        relayfs_relay::run(&format!("127.0.0.1:{port}"), Some(TOKEN))
            .await
            .unwrap();
    });

    // Only a bridge connects.
    let mut bridge = connect(
        port,
        Hello {
            kind: PeerKind::Bridge,
            id: "lonely-bridge".into(),
            name: "bridge".into(),
            token: Some(TOKEN.into()),
        },
    )
    .await;
    let _ = recv_json(&mut bridge).await; // hello_ack

    let request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "ping",
        "params": {},
    });
    bridge
        .send(Message::Text(request.to_string().into()))
        .await
        .unwrap();
    let response = recv_json(&mut bridge).await;
    assert_eq!(response["id"], 7);
    assert_eq!(
        response["error"]["code"],
        relayfs_protocol::code::AGENT_OFFLINE
    );

    relay.abort();
}
