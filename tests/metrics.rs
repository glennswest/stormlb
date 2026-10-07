//! Integration test: `/metrics` (#12) after real traffic through the real
//! router. A fake apiserver holds two routes — `app.test` to a backend that
//! reads a whole request body before answering 201, `dead.test` to a port
//! nothing listens on — and the metrics listener is scraped over a socket.

use std::net::SocketAddr;
use std::time::{Duration, Instant};
use stormlb::router::{self, RouterCfg};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Reads the head and `content-length` bytes of body, then answers 201 with
/// how many body bytes it got. Answering only after the whole body is what a
/// router that waits for the response before forwarding the body would
/// deadlock on.
async fn sink_backend() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut data = Vec::new();
                let mut buf = vec![0u8; 64 * 1024];
                let end = loop {
                    if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => data.extend_from_slice(&buf[..n]),
                    }
                };
                let head = String::from_utf8_lossy(&data[..end]).to_ascii_lowercase();
                let len: usize = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:").map(|v| v.trim().parse().unwrap()))
                    .unwrap_or(0);
                let mut got = data.len() - end;
                while got < len {
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => got += n,
                    }
                }
                let body = got.to_string();
                let resp = format!("HTTP/1.1 201 Created\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });
    addr
}

async fn fake_api(app: SocketAddr, dead: SocketAddr) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let route = |name: &str, host: &str, be: SocketAddr| {
        format!(r#"{{"metadata":{{"namespace":"t","name":"{name}","annotations":{{"storm.io/backend":"{be}"}}}},"spec":{{"hostnames":["{host}"]}}}}"#)
    };
    let body = format!(r#"{{"items":[{},{}]}}"#, route("app", "app.test", app), route("dead", "dead.test", dead));
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = s.read(&mut buf).await;
                let resp = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port()
}

/// One request; the whole response, or "" if the router isn't up yet.
async fn send(port: u16, req: &[u8]) -> String {
    let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)).await else { return String::new() };
    if c.write_all(req).await.is_err() {
        return String::new();
    }
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(20), c.read_to_end(&mut out)).await;
    String::from_utf8_lossy(&out).into_owned()
}

fn get(host: &str, path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: {host}\r\n\r\n").into_bytes()
}

/// The value of one sample line, if present.
fn sample(text: &str, series: &str) -> Option<f64> {
    text.lines().find_map(|l| l.strip_prefix(series)?.strip_prefix(' ')?.parse().ok())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_count_what_the_router_did() {
    let app = sink_backend().await;
    let dead: SocketAddr = format!("127.0.0.1:{}", free_port().await).parse().unwrap();
    let api = fake_api(app, dead).await;
    let http = free_port().await;
    tokio::spawn(router::run(RouterCfg { listen: format!("127.0.0.1:{http}"), apiserver: api, poll_secs: 1, insecure: true, tls: None }));
    let ml = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mport = ml.local_addr().unwrap().port();
    tokio::spawn(stormlb::metrics::serve(ml, None));

    // Wait for the route table.
    let end = Instant::now() + Duration::from_secs(10);
    while !send(http, &get("app.test", "/")).await.starts_with("HTTP/1.1 201") {
        assert!(Instant::now() < end, "the route never served");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Two more to app.test, one of them an 8 MiB upload the backend reads in
    // full before it answers.
    assert!(send(http, &get("app.test", "/x")).await.starts_with("HTTP/1.1 201"));
    let body = vec![b'x'; 8 << 20];
    let mut up = format!("POST /up HTTP/1.1\r\nHost: app.test\r\ncontent-length: {}\r\n\r\n", body.len()).into_bytes();
    up.extend_from_slice(&body);
    let r = send(http, &up).await;
    assert!(r.starts_with("HTTP/1.1 201") && r.ends_with(&(8 << 20).to_string()), "the upload went through whole: {}", &r[..r.len().min(200)]);

    // The router's own answers, and a dead backend.
    assert!(send(http, &get("nobody.test", "/")).await.starts_with("HTTP/1.1 404"));
    assert!(send(http, b"GET / HTTP/1.1\r\n\r\n").await.starts_with("HTTP/1.1 400"));
    assert!(send(http, &get("127.0.0.1", "/healthz")).await.starts_with("HTTP/1.1 200"));
    assert_eq!(send(http, &get("dead.test", "/")).await, "", "a dead backend closes with no response");

    let m = send(mport, &get("x", "/metrics")).await;
    let (head, text) = m.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("HTTP/1.1 200") && head.contains("text/plain; version=0.0.4"), "{head}");
    let s = |series: &str| sample(text, series).unwrap_or_else(|| panic!("no {series} in:\n{text}"));
    assert!(s(r#"stormlb_router_requests_total{host="app.test",code="201"}"#) >= 3.0, "the backend's own code, per host");
    assert_eq!(s(r#"stormlb_router_requests_total{host="unrouted",code="404"}"#), 1.0);
    assert_eq!(s(r#"stormlb_router_requests_total{host="unrouted",code="400"}"#), 1.0);
    assert_eq!(s(r#"stormlb_router_requests_total{host="unrouted",code="200"}"#), 1.0);
    assert_eq!(s(r#"stormlb_router_requests_total{host="dead.test",code="error"}"#), 1.0);
    assert_eq!(s(r#"stormlb_router_upstream_errors_total{host="dead.test",kind="connect"}"#), 1.0);
    assert!(s(r#"stormlb_router_request_duration_seconds_count{host="app.test"}"#) >= 3.0);
    assert!(s(r#"stormlb_router_request_duration_seconds_bucket{host="app.test",le="+Inf"}"#) >= 3.0);
    assert!(s(r#"stormlb_router_connections_total{listener="http"}"#) >= 9.0);
    assert!(s("stormlb_router_routes") == 2.0);
    assert!(s(r#"stormlb_router_route_refreshes_total{result="ok"}"#) >= 1.0);
    assert!(text.contains("stormlb_build_info{version=\""), "{text}");
    // The client's own Host never becomes a label.
    assert!(!text.contains("nobody.test"), "{text}");
    // Every connection above has ended: nothing active but the scrape's
    // neighbours (the metrics listener doesn't count).
    assert!(until_zero(mport).await, "active connections return to 0");

    // Anything but /metrics and /healthz is a 404; non-GET a 405.
    assert!(send(mport, &get("x", "/")).await.starts_with("HTTP/1.1 404"));
    assert!(send(mport, b"POST /metrics HTTP/1.1\r\ncontent-length: 0\r\n\r\n").await.starts_with("HTTP/1.1 405"));
}

async fn until_zero(mport: u16) -> bool {
    for _ in 0..50 {
        let m = send(mport, &get("x", "/metrics")).await;
        if sample(&m, r#"stormlb_router_connections_active{listener="http"}"#) == Some(0.0) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
