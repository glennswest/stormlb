//! The suites, run hermetically: the real router (`stormlb::router::run`,
//! polling every second) against a small in-memory apiserver, both on
//! loopback. This is how the test container is itself tested in `sc-build`,
//! with no node, no cluster and no network.
//!
//! The apiserver here does only what the router and the suites use:
//! HTTPRoutes, Services and Endpoints by namespace (create, get, list by
//! label, replace, delete), the all-namespaces HTTPRoute list, and one Node.
//! A Service's clusterIP is 127.0.0.1, so the "Service data plane" is the
//! service port itself — which is why the suite gives the Service the
//! backend's own port.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};
use stormlb::router::{self, RouterCfg};
use stormlb_test::env::Env;
use stormlb_test::report::Report;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type Store = Arc<Mutex<BTreeMap<String, Value>>>;

/// Start the apiserver; returns its base URL.
async fn apiserver() -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", l.local_addr().unwrap());
    let store: Store = Arc::default();
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            let store = store.clone();
            tokio::spawn(async move {
                let _ = serve(s, store).await;
            });
        }
    });
    base
}

async fn serve(mut s: TcpStream, store: Store) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
        let n = s.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..end]).into_owned();
    let len = head
        .lines()
        .find_map(|l| l.split_once(':').filter(|(k, _)| k.eq_ignore_ascii_case("content-length")).map(|(_, v)| v.trim().to_string()))
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    while buf.len() < end + len {
        let n = s.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let body: Value = serde_json::from_slice(&buf[end..]).unwrap_or(Value::Null);
    let mut words = head.split_whitespace();
    let (method, target) = (words.next().unwrap_or(""), words.next().unwrap_or(""));
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let (status, v) = handle(method, path, query, body, &store);
    let text = v.to_string();
    let resp = format!(
        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}",
        text.len()
    );
    s.write_all(resp.as_bytes()).await
}

fn handle(method: &str, path: &str, query: &str, body: Value, store: &Store) -> (u16, Value) {
    let mut db = store.lock().unwrap();
    if method == "GET" && path == "/apis/gateway.networking.k8s.io/v1/httproutes" {
        let items: Vec<Value> = db.iter().filter(|(k, _)| k.contains("/httproutes/")).map(|(_, v)| v.clone()).collect();
        return (200, json!({"items": items}));
    }
    if method == "GET" && path == "/api/v1/nodes" {
        return (
            200,
            json!({"items": [{"metadata": {"name": "harness"}, "status": {
                "addresses": [{"type": "InternalIP", "address": "127.0.0.1"}],
                "allocatable": {"cpu": "2"}}}]}),
        );
    }
    let Some((prefix, rest)) = path.split_once("/namespaces/") else {
        return (404, json!({"message": format!("no {path}")}));
    };
    let parts: Vec<&str> = rest.split('/').collect();
    let (coll, kind, name) = match parts.as_slice() {
        [ns, kind] => (format!("{prefix}/namespaces/{ns}/{kind}"), *kind, None),
        [ns, kind, name] => (format!("{prefix}/namespaces/{ns}/{kind}"), *kind, Some(*name)),
        _ => return (404, json!({"message": format!("no {path}")})),
    };
    if !["httproutes", "services", "endpoints"].contains(&kind) {
        return (404, json!({"message": format!("no {kind}")}));
    }
    match (method, name) {
        ("POST", None) => {
            let Some(n) = body["metadata"]["name"].as_str() else {
                return (422, json!({"message": "no metadata.name"}));
            };
            let key = format!("{coll}/{n}");
            if db.contains_key(&key) {
                return (409, json!({"message": format!("{n} exists")}));
            }
            let mut obj = body.clone();
            if kind == "services" && obj["spec"]["clusterIP"].as_str() != Some("None") {
                obj["spec"]["clusterIP"] = json!("127.0.0.1");
            }
            obj["metadata"]["resourceVersion"] = json!(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos().to_string());
            db.insert(key, obj.clone());
            (201, obj)
        }
        ("GET", None) => {
            let sel = query
                .split('&')
                .find_map(|kv| kv.strip_prefix("labelSelector="))
                .map(|s| s.replace("%2F", "/").replace("%3D", "="));
            let items: Vec<Value> = db
                .iter()
                .filter(|(k, _)| k.starts_with(&format!("{coll}/")))
                .map(|(_, v)| v)
                .filter(|v| match sel.as_deref().and_then(|s| s.split_once('=')) {
                    Some((k, want)) => v["metadata"]["labels"][k].as_str() == Some(want),
                    None => true,
                })
                .cloned()
                .collect();
            (200, json!({"items": items}))
        }
        ("GET", Some(n)) => match db.get(&format!("{coll}/{n}")) {
            Some(v) => (200, v.clone()),
            None => (404, json!({"message": "not found"})),
        },
        ("PUT", Some(n)) => {
            let key = format!("{coll}/{n}");
            let Some(old) = db.get(&key) else {
                return (404, json!({"message": "not found"}));
            };
            let mut obj = body.clone();
            if kind == "services" {
                obj["spec"]["clusterIP"] = old["spec"]["clusterIP"].clone();
            }
            db.insert(key, obj.clone());
            (200, obj)
        }
        ("DELETE", Some(n)) => match db.remove(&format!("{coll}/{n}")) {
            Some(v) => (200, v),
            None => (404, json!({"message": "not found"})),
        },
        _ => (405, json!({"message": format!("{method} {path}")})),
    }
}

/// The apiserver and the router on it; returns the Env the suites run with.
async fn stand_up(suite: &str, timeout: Duration) -> Env {
    let api = apiserver().await;
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let cfg = RouterCfg { listen: format!("127.0.0.1:{port}"), apiserver: api.clone(), poll_secs: 1, insecure: true };
    tokio::spawn(async move {
        if let Err(e) = router::run(cfg).await {
            panic!("router: {e}");
        }
    });
    let router = format!("127.0.0.1:{port}");
    let t = Instant::now();
    while TcpStream::connect(&router).await.is_err() {
        assert!(t.elapsed() < Duration::from_secs(5), "the router never listened on {router}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Env {
        suite: suite.into(),
        run_id: format!("harness-{suite}"),
        namespace: "harness".into(),
        api,
        node: "127.0.0.1".into(),
        router,
        stormd: None,
        token: None,
        ca: None,
        timeout,
        started: Instant::now(),
        route_wait: Duration::from_secs(10),
        settle: Duration::from_secs(3),
    }
}

/// Every test's status, against what is expected (anything unnamed: pass).
fn expect(r: &Report, not_pass: &[(&str, &str)]) {
    assert!(!r.outcomes.is_empty(), "nothing ran");
    let mut wrong = Vec::new();
    for (name, status) in &r.outcomes {
        let want = not_pass.iter().find(|(n, _)| n == name).map(|(_, s)| *s).unwrap_or("pass");
        if *status != want {
            wrong.push(format!("{name}: {status}, expected {want}"));
        }
    }
    for (n, _) in not_pass {
        if !r.outcomes.iter().any(|(name, _)| name == n) {
            wrong.push(format!("{n}: never reported"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}\n(the JSON lines above say why)");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn short_passes_against_the_real_router() {
    let env = stand_up("short", Duration::from_secs(120)).await;
    let mut r = Report::new();
    stormlb_test::run(&env, &mut r).await;
    expect(&r, &[("stormd-supervises", "skip")]);
    assert_eq!(r.outcomes.len(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn medium_passes_but_for_the_known_gaps() {
    let env = stand_up("medium", Duration::from_secs(600)).await;
    let mut r = Report::new();
    stormlb_test::run(&env, &mut r).await;
    expect(&r, &[("stormd-no-restarts", "skip"), ("vip-half", "skip")]);
    assert_eq!(r.outcomes.len(), 22);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_runs_waves_and_leaves_nothing_behind() {
    let env = stand_up("long", Duration::from_secs(45)).await;
    let mut r = Report::new();
    stormlb_test::run(&env, &mut r).await;
    // The trend compares timings, and on a shared build box those say more
    // about the box than the router: it must be reported, not pass.
    let trend = r.outcomes.iter().find(|(n, _)| n == "trend").map(|(_, s)| *s);
    assert!(trend.is_some(), "no trend line");
    let others: Vec<_> = r.outcomes.iter().filter(|(n, _)| n != "trend").cloned().collect();
    let waves = others.iter().filter(|(n, _)| n.starts_with("wave-")).count();
    assert!(waves >= 1, "no wave ran: {others:?}");
    let bad: Vec<_> = others.iter().filter(|(_, s)| *s != "pass").collect();
    assert!(bad.is_empty(), "{bad:?}");
}
