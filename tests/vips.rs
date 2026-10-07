//! Integration tests: VIPs made and changed at runtime through the API — the
//! path stormcluster drives as the masters of a cluster change (#16).
//!
//! Everything is on loopback: the VIP is 127.0.0.1 on a free port, the
//! backends are listeners in this process that answer with their id byte, and
//! the API is spoken over a real socket.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use stormlb::api;
use stormlb::config::{Config, HealthSpec};
use stormlb::vips::{Registry, VipSpec};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A backend that writes its `id` byte on connect, then echoes.
async fn backend(id: u8) -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let _ = s.write_all(&[id]).await;
                let mut buf = [0u8; 64];
                while let Ok(n) = s.read(&mut buf).await {
                    if n == 0 || s.write_all(&buf[..n]).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    addr
}

/// A port nothing listens on (bound, then freed).
async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port()
}

/// The id byte of the backend a connection through the VIP reaches; None
/// when it is refused or dropped with no data.
async fn who(vip: SocketAddr) -> Option<u8> {
    let mut c = TcpStream::connect(vip).await.ok()?;
    let mut id = [0u8; 1];
    match tokio::time::timeout(Duration::from_secs(2), c.read(&mut id)).await {
        Ok(Ok(1)) => Some(id[0]),
        _ => None,
    }
}

/// Start the API on a free loopback port.
async fn start_api(reg: Arc<Registry>, token_file: Option<String>) -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(api::serve(l, reg, token_file));
    addr
}

/// One request to the API: (status, JSON body).
async fn call(api: SocketAddr, method: &str, path: &str, body: Option<&str>, token: Option<&str>) -> (u16, serde_json::Value) {
    let mut c = TcpStream::connect(api).await.unwrap();
    let body = body.unwrap_or("");
    let auth = token.map(|t| format!("authorization: Bearer {t}\r\n")).unwrap_or_default();
    let req = format!("{method} {path} HTTP/1.1\r\nhost: x\r\n{auth}content-length: {}\r\n\r\n{body}", body.len());
    c.write_all(req.as_bytes()).await.unwrap();
    let mut out = Vec::new();
    c.read_to_end(&mut out).await.unwrap();
    let text = String::from_utf8(out).unwrap();
    assert!(text.starts_with("HTTP/1.1 "), "{text}");
    let status: u16 = text[9..12].parse().unwrap();
    let (head, json) = text.split_once("\r\n\r\n").unwrap();
    assert!(head.contains("content-type: application/json"), "{head}");
    (status, serde_json::from_str(json).unwrap())
}

fn spec(port: u16, backends: &[SocketAddr]) -> String {
    let b: Vec<String> = backends.iter().map(|a| format!(r#"{{"address":"{}","port":{}}}"#, a.ip(), a.port())).collect();
    format!(r#"{{"address":"127.0.0.1","port":{port},"backends":[{}],"health":{{"mode":"tcp","interval_secs":1,"timeout_secs":1}}}}"#, b.join(","))
}

/// Wait until `f` holds, up to `secs`.
async fn until(secs: u64, mut f: impl AsyncFnMut() -> bool) -> bool {
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_vip_is_created_its_backends_changed_and_removed() {
    let (a, b) = (backend(b'A').await, backend(b'B').await);
    let reg = Arc::new(Registry::new(None));
    let api = start_api(reg.clone(), None).await;
    let port = free_port().await;
    let vip: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

    // Create: 201, and traffic reaches A once it is checked healthy.
    let (s, v) = call(api, "PUT", "/api/v1/vips/api", Some(&spec(port, &[a])), None).await;
    assert_eq!(s, 201, "{v}");
    assert_eq!(v["status"]["source"], "api");
    assert_eq!(v["status"]["listening"], vip.to_string());
    assert!(until(5, async || who(vip).await == Some(b'A')).await, "A serves");
    let (s, v) = call(api, "GET", "/api/v1/vips/api", None, None).await;
    assert_eq!(s, 200);
    assert_eq!(v["status"]["healthy"], 1);
    assert_eq!(v["backends"][0]["port"], a.port());
    assert_eq!(v["status"]["backends"][0]["healthy"], true);

    // A connection made before the change keeps working after it.
    let mut held = TcpStream::connect(vip).await.unwrap();
    let mut id = [0u8; 1];
    held.read_exact(&mut id).await.unwrap();
    assert_eq!(id[0], b'A');

    // Replace A with B: 200, and new connections go to B (probed at once).
    let t = Instant::now();
    let (s, _) = call(api, "PUT", "/api/v1/vips/api", Some(&spec(port, &[b])), None).await;
    assert_eq!(s, 200);
    assert!(until(5, async || who(vip).await == Some(b'B')).await, "B serves");
    assert!(t.elapsed() < Duration::from_millis(900), "a new backend is probed straight away, not after the interval: {:?}", t.elapsed());
    for _ in 0..5 {
        assert_eq!(who(vip).await, Some(b'B'), "A is no longer a member");
    }
    held.write_all(b"still").await.unwrap();
    let mut echo = [0u8; 5];
    held.read_exact(&mut echo).await.unwrap();
    assert_eq!(&echo, b"still", "the connection proxied to A was not cut");

    // Add A back beside B: B, which stays, is healthy at once.
    let (s, v) = call(api, "PUT", "/api/v1/vips/api", Some(&spec(port, &[b, a])), None).await;
    assert_eq!(s, 200);
    assert_eq!(v["status"]["backends"][0]["healthy"], true, "B kept its health: {v}");
    assert!(until(5, async || reg.get("api").unwrap().status.healthy == 2).await);

    // The list.
    let (s, v) = call(api, "GET", "/api/v1/vips", None, None).await;
    assert_eq!(s, 200);
    assert_eq!(v["vips"].as_array().unwrap().len(), 1);

    // Remove: 200, then the listener closes; a second remove is 404.
    let (s, _) = call(api, "DELETE", "/api/v1/vips/api", None, None).await;
    assert_eq!(s, 200);
    assert!(until(3, async || TcpStream::connect(vip).await.is_err()).await, "the listener closed");
    assert_eq!(call(api, "DELETE", "/api/v1/vips/api", None, None).await.0, 404);
    assert_eq!(call(api, "GET", "/api/v1/vips/api", None, None).await.0, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_requests_are_refused_and_change_nothing() {
    let a = backend(b'A').await;
    let reg = Arc::new(Registry::new(None));
    let api = start_api(reg.clone(), None).await;
    let (p1, p2) = (free_port().await, free_port().await);
    assert_eq!(call(api, "PUT", "/api/v1/vips/one", Some(&spec(p1, &[a])), None).await.0, 201);

    let cases = [
        ("/api/v1/vips/two", r#"{"address":"nope","port":1}"#.to_string(), 400, "not an IP"),
        ("/api/v1/vips/two", r#"{"address":"127.0.0.1","port":0}"#.to_string(), 400, "port"),
        ("/api/v1/vips/two", r#"{"address":"127.0.0.1","port":1,"bogus":1}"#.to_string(), 400, "bogus"),
        ("/api/v1/vips/two", r#"{"address":"127.0.0.1","port":1,"backends":[{"address":"master1","port":6443}]}"#.to_string(), 400, "master1"),
        ("/api/v1/vips/two", r#"{"address":"127.0.0.1","port":1,"health":{"mode":"https","ca_file":"/nonexistent/ca.crt"}}"#.to_string(), 400, "/nonexistent/ca.crt"),
        ("/api/v1/vips/two", r#"{"address":"::1","port":1,"vrrp":{"interface":"lo"}}"#.to_string(), 400, "IPv4"),
        ("/api/v1/vips/Two", spec(p2, &[a]), 400, "name"),
        // Another VIP's listener.
        ("/api/v1/vips/two", spec(p1, &[a]), 409, "already VIP one"),
    ];
    for (path, body, want, says) in cases {
        let (s, v) = call(api, "PUT", path, Some(&body), None).await;
        assert_eq!(s, want, "{body}: {v}");
        assert!(v["error"].as_str().unwrap().contains(says), "{body}: {v}");
    }
    // A bad change to an existing VIP leaves it serving as it was.
    let (s, _) = call(api, "PUT", "/api/v1/vips/one", Some(r#"{"address":"127.0.0.1","port":0}"#), None).await;
    assert_eq!(s, 400);
    assert_eq!(reg.get("one").unwrap().spec.port, p1);
    assert_eq!(reg.list().len(), 1, "nothing else was made");

    assert_eq!(call(api, "POST", "/api/v1/vips/one", Some("{}"), None).await.0, 405);
    assert_eq!(call(api, "GET", "/api/v1/other", None, None).await.0, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn moving_the_listener_binds_the_new_port_before_closing_the_old() {
    let a = backend(b'A').await;
    let reg = Arc::new(Registry::new(None));
    let api = start_api(reg, None).await;
    let (p1, p2) = (free_port().await, free_port().await);
    assert_eq!(call(api, "PUT", "/api/v1/vips/m", Some(&spec(p1, &[a])), None).await.0, 201);
    let old: SocketAddr = format!("127.0.0.1:{p1}").parse().unwrap();
    let new: SocketAddr = format!("127.0.0.1:{p2}").parse().unwrap();
    assert!(until(5, async || who(old).await == Some(b'A')).await);

    // A port someone else holds: 409, and the old listener still serves.
    let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (s, v) = call(api, "PUT", "/api/v1/vips/m", Some(&spec(taken.local_addr().unwrap().port(), &[a])), None).await;
    assert_eq!(s, 409, "{v}");
    assert_eq!(who(old).await, Some(b'A'));

    assert_eq!(call(api, "PUT", "/api/v1/vips/m", Some(&spec(p2, &[a])), None).await.0, 200);
    assert_eq!(who(new).await, Some(b'A'), "the backend kept its health across the move");
    assert!(until(3, async || TcpStream::connect(old).await.is_err()).await, "the old listener closed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_file_guards_the_api_but_not_healthz() {
    let dir = tmpdir("token");
    let tf = dir.join("token");
    std::fs::write(&tf, "s3cret\n").unwrap();
    let reg = Arc::new(Registry::new(None));
    let api = start_api(reg, Some(tf.to_str().unwrap().into())).await;
    assert_eq!(call(api, "GET", "/api/v1/vips", None, None).await.0, 401);
    assert_eq!(call(api, "GET", "/api/v1/vips", None, Some("wrong")).await.0, 401);
    assert_eq!(call(api, "GET", "/api/v1/vips", None, Some("s3cret")).await.0, 200);
    assert_eq!(call(api, "GET", "/healthz", None, None).await.0, 200);
    // Rotated in place: the old token stops working at once.
    std::fs::write(&tf, "n3w").unwrap();
    assert_eq!(call(api, "GET", "/api/v1/vips", None, Some("s3cret")).await.0, 401);
    assert_eq!(call(api, "GET", "/api/v1/vips", None, Some("n3w")).await.0, 200);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn off_loopback_the_api_needs_a_token_file() {
    let cfg: Config = toml::from_str("[api]\nlisten = \"0.0.0.0:0\"\n").unwrap();
    let e = api::run(cfg.api.unwrap(), Arc::new(Registry::new(None))).await.unwrap_err();
    assert!(e.to_string().contains("token_file"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vips_made_through_the_api_are_saved_and_served_after_a_restart() {
    let a = backend(b'A').await;
    let dir = tmpdir("state");
    let state = dir.join("sub/vips.json");
    let port = free_port().await;
    let vip: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    {
        let reg = Arc::new(Registry::new(Some(state.clone())));
        let api = start_api(reg.clone(), None).await;
        assert_eq!(call(api, "PUT", "/api/v1/vips/kept", Some(&spec(port, &[a])), None).await.0, 201);
        let saved: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&state).unwrap()).unwrap();
        assert_eq!(saved["vips"]["kept"]["port"], port);
        reg.stop_all();
    }
    // Let the aborted listener task drop its socket.
    tokio::time::sleep(Duration::from_millis(300)).await;
    // A new process: the saved VIP serves again before anyone re-applies it.
    let reg = Registry::new(Some(state.clone()));
    assert_eq!(reg.load_state().unwrap(), 1);
    assert!(until(5, async || who(vip).await == Some(b'A')).await);
    reg.remove("kept").unwrap();
    let saved: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(&state).unwrap()).unwrap();
    assert!(saved["vips"].as_object().unwrap().is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_config_files_vip_is_read_only_through_the_api() {
    let a = backend(b'A').await;
    let port = free_port().await;
    let cfg: Config = toml::from_str(&format!(
        "[vip]\naddress = \"127.0.0.1\"\nport = {port}\nbind = \"127.0.0.1\"\n[[backend]]\naddress = \"{}\"\nport = {}\n",
        a.ip(),
        a.port()
    ))
    .unwrap();
    let reg = Arc::new(Registry::new(None));
    reg.start_config(Registry::config_spec(&cfg).unwrap().unwrap()).unwrap();
    let api = start_api(reg.clone(), None).await;
    let (s, v) = call(api, "GET", "/api/v1/vips/default", None, None).await;
    assert_eq!(s, 200);
    assert_eq!(v["status"]["source"], "config");
    assert_eq!(call(api, "PUT", "/api/v1/vips/default", Some(&spec(free_port().await, &[a])), None).await.0, 409);
    assert_eq!(call(api, "DELETE", "/api/v1/vips/default", None, None).await.0, 409);
    // The TOML health default is tcp at 2 s.
    assert!(until(5, async || who(format!("127.0.0.1:{port}").parse().unwrap()).await == Some(b'A')).await);
}

/// https health checks verify the backend against `ca_file`: a backend whose
/// certificate the CA signed is healthy; the same backend checked against
/// another CA is not. Certificates are made with openssl at test time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_health_checks_verify_against_the_ca_file() {
    let dir = tmpdir("tls");
    if !make_certs(&dir) {
        eprintln!("skipped: no openssl to make test certificates");
        return;
    }
    let readyz = tls_readyz(&dir).await;
    let health = |ca: &str| HealthSpec {
        mode: stormlb::config::HealthMode::Https,
        path: "/readyz".into(),
        interval_secs: 1,
        timeout_secs: 1,
        expect_status: vec![200],
        ca_file: Some(dir.join(ca).to_str().unwrap().into()),
    };
    let reg = Registry::new(None);
    for (name, ca, want) in [("good", "ca.crt", true), ("other", "other-ca.crt", false)] {
        let s = VipSpec {
            address: "127.0.0.1".into(),
            port: free_port().await,
            bind: None,
            backends: vec![stormlb::vips::BackendSpec { address: readyz.ip().to_string(), port: readyz.port() }],
            health: health(ca),
            vrrp: None,
        };
        reg.apply(name, s).unwrap();
    }
    assert!(until(5, async || reg.get("good").unwrap().status.healthy == 1).await, "signed by ca_file: healthy");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(reg.get("other").unwrap().status.healthy, 0, "not signed by ca_file: unhealthy");
    let _ = std::fs::remove_dir_all(dir);
}

fn tmpdir(what: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("stormlb-{what}-{}-{:?}", std::process::id(), std::thread::current().id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// A CA, a server certificate for 127.0.0.1 it signs, and an unrelated CA.
fn make_certs(dir: &std::path::Path) -> bool {
    let sh = |args: &str| {
        std::process::Command::new("sh")
            .arg("-c")
            .arg(args)
            .current_dir(dir)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    };
    sh("command -v openssl")
        && sh("openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=test-ca -keyout ca.key -out ca.crt")
        && sh("openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=other-ca -keyout other-ca.key -out other-ca.crt")
        && sh("openssl req -newkey rsa:2048 -nodes -subj /CN=127.0.0.1 -keyout srv.key -out srv.csr")
        && sh("printf 'subjectAltName=IP:127.0.0.1\\nbasicConstraints=CA:FALSE\\nextendedKeyUsage=serverAuth\\n' > ext \
               && openssl x509 -req -in srv.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 1 -extfile ext -out srv.crt")
}

/// An https server answering 200 to anything, with srv.crt/srv.key.
async fn tls_readyz(dir: &std::path::Path) -> SocketAddr {
    use tokio_rustls::rustls::pki_types::pem::PemObject;
    use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let certs: Vec<CertificateDer> = CertificateDer::pem_file_iter(dir.join("srv.crt")).unwrap().map(|c| c.unwrap()).collect();
    let key = PrivateKeyDer::from_pem_file(dir.join("srv.key")).unwrap();
    let cfg = tokio_rustls::rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(s).await else { return };
                let mut buf = [0u8; 2048];
                let _ = tls.read(&mut buf).await;
                let _ = tls.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok").await;
                let _ = tls.shutdown().await;
            });
        }
    });
    addr
}
