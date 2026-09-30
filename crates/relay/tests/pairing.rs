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
async fn multiple_agents_stay_active_and_duplicate_ids_are_suffixed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let relay = tokio::spawn(async move {
        relayfs_relay::run(&format!("127.0.0.1:{port}"), Some(TOKEN))
            .await
            .unwrap();
    });

    // First agent connects with id "agent".
    let mut agent_a = connect(
        port,
        Hello {
            kind: PeerKind::Agent,
            id: "agent".into(),
            name: "machine-1".into(),
            token: Some(TOKEN.into()),
        },
    )
    .await;
    let ack_a: HelloAck =
        serde_json::from_value(recv_json(&mut agent_a).await["params"].clone()).unwrap();
    assert_eq!(ack_a.agent_id.as_deref(), Some("agent"));

    // Second agent connects with the SAME id "agent" and SAME token.
    // The relay must NOT displace agent_a; both must remain active.
    // The newly connected agent must receive a suffixed id.
    let mut agent_b = connect(
        port,
        Hello {
            kind: PeerKind::Agent,
            id: "agent".into(),
            name: "machine-2".into(),
            token: Some(TOKEN.into()),
        },
    )
    .await;
    let ack_b: HelloAck =
        serde_json::from_value(recv_json(&mut agent_b).await["params"].clone()).unwrap();
    let suffixed_id = ack_b.agent_id.expect("agent_b should have an assigned id");
    assert!(
        suffixed_id.starts_with("agent-"),
        "expected suffixed id starting with 'agent-', got: {suffixed_id}"
    );

    // Bridge connects.
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
    let _ = recv_json(&mut bridge).await; // hello_ack

    // Query list_targets: both agents must be returned.
    let list_req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 100,
        "method": relayfs_protocol::method::LIST_TARGETS,
        "params": {},
    });
    bridge
        .send(Message::Text(list_req.to_string().into()))
        .await
        .unwrap();
    let list_resp = recv_json(&mut bridge).await;
    let targets: Vec<relayfs_protocol::TargetInfo> =
        serde_json::from_value(list_resp["result"]["targets"].clone()).unwrap();
    assert_eq!(targets.len(), 2, "expected 2 active targets in list_targets");
    let target_ids: Vec<String> = targets.into_iter().map(|t| t.id).collect();
    assert!(target_ids.contains(&"agent".to_string()));
    assert!(target_ids.contains(&suffixed_id));

    // Targeted request to agent_a: agent_a receives it.
    let req_a = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "ping",
        "params": { "target": "agent" },
    });
    bridge
        .send(Message::Text(req_a.to_string().into()))
        .await
        .unwrap();
    let received_a = recv_json(&mut agent_a).await;
    assert_eq!(received_a["id"], 1);

    // Targeted request to agent_b: agent_b receives it.
    let req_b = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "ping",
        "params": { "target": suffixed_id },
    });
    bridge
        .send(Message::Text(req_b.to_string().into()))
        .await
        .unwrap();
    let received_b = recv_json(&mut agent_b).await;
    assert_eq!(received_b["id"], 2);

    // When agent_b disconnects, agent_a must still be able to receive requests.
    drop(agent_b);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let req_a2 = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "ping",
        "params": {},
    });
    bridge
        .send(Message::Text(req_a2.to_string().into()))
        .await
        .unwrap();
    let received_a2 = recv_json(&mut agent_a).await;
    assert_eq!(received_a2["id"], 3);

    // When agent_a also leaves, requests must start failing with AGENT_OFFLINE.
    drop(agent_a);
    let mut saw_offline = false;
    for _ in 0..100 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        bridge
            .send(Message::Text(req_a2.to_string().into()))
            .await
            .unwrap();
        let response = recv_json(&mut bridge).await;
        if let Some(code) = response.get("error").and_then(|e| e.get("code")) {
            assert_eq!(code, relayfs_protocol::code::AGENT_OFFLINE);
            saw_offline = true;
            break;
        }
    }
    assert!(saw_offline, "expected AGENT_OFFLINE after all agents left");

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
