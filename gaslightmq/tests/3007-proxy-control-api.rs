use std::net::SocketAddr;
use std::time::Duration;

use gaslightmq_testing::proxy::*;
use futures::{SinkExt, StreamExt};
use gaslightmq::Proxy;
use gaslightmq::proxy_api::{ApiRow, ClientMsg, NodeState, ServerMsg, Snapshot, catalog::api};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;

async fn proxy_with_control(cluster: &FakeCluster) -> (Proxy, SocketAddr) {
    let proxy = cluster.proxy_with(|c| c.control("127.0.0.1:0".parse().unwrap())).await;
    let addr = proxy.control_addr().unwrap();
    (proxy, addr)
}

/// Minimal HTTP/1.1 client: returns status and body.
async fn http(addr: SocketAddr, method: &str, path: &str, body: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\
         Content-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.unwrap();
    let status = raw[9..12].parse().unwrap();
    let body = raw.split_once("\r\n\r\n").map(|(_, b)| b.to_string()).unwrap_or_default();
    (status, body)
}

#[tokio::test(flavor = "multi_thread")]
async fn rest_api_manages_rows_and_nodes() {
    let cluster = FakeCluster::start(2, &[("orders", 1)]).await;
    let (proxy, control) = proxy_with_control(&cluster).await;

    let (status, body) = http(control, "GET", "/api/state", None).await;
    assert_eq!(status, 200);
    let snapshot: Snapshot = serde_json::from_str(&body).unwrap();
    assert_eq!(snapshot.nodes.len(), 2);
    assert_eq!(snapshot.bootstrap.proxy_port, proxy.bootstrap_addr().port());
    assert_eq!(&snapshot.catalog[0].name, "DescribeConfigs");
    assert!(snapshot.rows.is_empty());

    let (status, _) = http(control, "PUT", "/api/rows/0", Some(r#"{"api_key":0,"delay":{"enabled":true,"ms":10}}"#)).await;
    assert_eq!(status, 200);
    assert_eq!(proxy.handle().rules().rows[0].delay.ms, 10);

    let (status, _) = http(control, "PUT", "/api/rows/0", Some(r#"{"api_key":0,"error":{"enabled":true,"probability":101}}"#)).await;
    assert_eq!(status, 400);

    let (status, _) = http(control, "DELETE", "/api/rows", None).await;
    assert_eq!(status, 204);
    assert!(proxy.handle().rules().rows.is_empty());

    let (status, _) = http(control, "PUT", "/api/nodes/1/state", Some(r#"{"state":"down"}"#)).await;
    assert_eq!(status, 204);
    assert_eq!(proxy.handle().nodes()[1].state, NodeState::Down);
    let (status, _) = http(control, "PUT", "/api/nodes/9/state", Some(r#"{"state":"down"}"#)).await;
    assert_eq!(status, 404);

    let (status, _) = http(control, "GET", "/api/nope", None).await;
    assert_eq!(status, 404);
    let (status, body) = http(control, "GET", "/", None).await;
    assert_eq!(status, 200);
    assert!(body.contains("<"));
    proxy.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_streams_snapshot_changes_and_stats() {
    let cluster = FakeCluster::start(1, &[("orders", 1)]).await;
    let (proxy, control) = proxy_with_control(&cluster).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{control}/api/ws")).await.unwrap();

    let mut next = async || -> ServerMsg {
        loop {
            let msg = tokio::time::timeout(TIMEOUT, ws.next()).await.unwrap().unwrap().unwrap();
            if let Message::Text(text) = msg {
                return serde_json::from_str(&text).unwrap();
            }
        }
    };
    let ServerMsg::Snapshot(snapshot) = next().await else { panic!("expected snapshot first") };
    assert_eq!(snapshot.nodes.len(), 1);
    drop(next);

    let mut row = ApiRow::new(api::METADATA);
    row.blackhole.enabled = true;
    let json = serde_json::to_string(&ClientMsg::SetRow(row.clone())).unwrap();
    ws.send(Message::Text(json.into())).await.unwrap();

    // Generate traffic so stats ticks have something to report.
    let mut client = TestClient::connect(proxy.bootstrap_addr()).await.unwrap();
    produce(&mut client, &[("orders", 0, &["x"])]).await.unwrap();

    let mut saw_row = false;
    let mut saw_produce_tick = false;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    while !(saw_row && saw_produce_tick) && tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(Message::Text(text)))) = tokio::time::timeout(Duration::from_secs(1), ws.next()).await else {
            continue;
        };
        match serde_json::from_str::<ServerMsg>(&text).unwrap() {
            ServerMsg::RowChanged(r) => {
                assert_eq!(r, row);
                saw_row = true;
            }
            ServerMsg::Stats(tick) => {
                saw_produce_tick |= tick.apis.iter().any(|a| a.api_key == api::PRODUCE && a.requests > 0);
                assert_eq!(tick.nodes.len(), 1);
            }
            _ => {}
        }
    }
    assert!(saw_row, "row change broadcast");
    assert!(saw_produce_tick, "stats tick with produce traffic");
    assert_eq!(proxy.handle().rules().rows, vec![row]);
    proxy.shutdown().await;
}
