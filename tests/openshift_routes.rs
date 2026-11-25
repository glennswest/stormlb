//! Integration test: OpenShift Routes (`route.openshift.io/v1`, #48). The real
//! router (`router::run`) against a fake apiserver that holds Routes and
//! Services and records `routes/status` patches; backends on loopback (plain,
//! and TLS with their own certificates); certificates made with openssl at
//! test time (skipped without it): a CA, the router's `*.storm1.test`, the
//! reencrypt backend's `secure.mon.svc` and the passthrough backend's
//! `pt.storm1.test`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use stormlb::router::{self, RouterCfg};
use stormlb::tls::{CertPair, TlsCfg};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn tmpdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("stormlb-osroutes-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn sh(dir: &Path, cmd: &str) -> bool {
    std::process::Command::new("sh").arg("-c").arg(cmd).current_dir(dir).output().map(|o| o.status.success()).unwrap_or(false)
}

fn make_ca(dir: &Path) -> bool {
    sh(dir, "command -v openssl") && sh(dir, "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=test-ca -keyout ca.key -out ca.crt")
}

fn make_leaf(dir: &Path, name: &str, san: &str) {
    let cmd = format!(
        "openssl req -newkey rsa:2048 -nodes -subj /CN={name} -keyout {name}.key -out {name}.csr \
         && printf 'subjectAltName={san}\\nbasicConstraints=CA:FALSE\\nextendedKeyUsage=serverAuth\\n' > {name}.ext \
         && openssl x509 -req -in {name}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 1 -extfile {name}.ext -out {name}.crt"
    );
    assert!(sh(dir, &cmd), "openssl leaf {name}");
}

fn path(dir: &Path, f: &str) -> String {
    dir.join(f).to_str().unwrap().to_string()
}

/// The fake apiserver's objects: Routes (status patched in place) and
/// Services, and every status patch it took.
#[derive(Default)]
struct Api {
    routes: Vec<Value>,
    services: Vec<Value>,
    patches: Vec<(String, Value)>,
}

async fn read_request<S: AsyncRead + Unpin>(s: &mut S) -> Option<(String, String, Value)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..end]).to_string();
    let len: usize = head
        .lines()
        .find_map(|l| l.split_once(':').filter(|(k, _)| k.eq_ignore_ascii_case("content-length")).map(|(_, v)| v.trim().parse().unwrap_or(0)))
        .unwrap_or(0);
    while buf.len() < end + len {
        let n = s.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    let mut words = head.split_whitespace();
    let (method, target) = (words.next()?.to_string(), words.next()?.to_string());
    let body = serde_json::from_slice(&buf[end..end + len]).unwrap_or(Value::Null);
    Some((method, target, body))
}

async fn fake_api(state: Arc<Mutex<Api>>) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            let state = state.clone();
            tokio::spawn(async move {
                let Some((method, target, body)) = read_request(&mut s).await else { return };
                let (code, v) = {
                    let mut st = state.lock().unwrap();
                    match (method.as_str(), target.as_str()) {
                        ("GET", "/apis/gateway.networking.k8s.io/v1/httproutes") => (200, json!({"items": []})),
                        ("GET", "/apis/route.openshift.io/v1/routes") => (200, json!({"items": st.routes})),
                        ("GET", "/api/v1/services") => (200, json!({"items": st.services})),
                        ("PATCH", t) if t.starts_with("/apis/route.openshift.io/v1/namespaces/") && t.ends_with("/status") => {
                            let parts: Vec<&str> = t.split('/').collect();
                            let (ns, name) = (parts[5], parts[7]);
                            st.patches.push((format!("{ns}/{name}"), body.clone()));
                            match st.routes.iter_mut().find(|r| r["metadata"]["namespace"] == ns && r["metadata"]["name"] == name) {
                                Some(r) => {
                                    r["status"] = body["status"].clone();
                                    (200, r.clone())
                                }
                                None => (404, json!({})),
                            }
                        }
                        _ => (404, json!({"message": target})),
                    }
                };
                let text = v.to_string();
                let resp = format!("HTTP/1.1 {code} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{text}", text.len());
                let _ = s.write_all(resp.as_bytes()).await;
            });
        }
    });
    format!("http://{addr}")
}

/// Answer one request: `<name>\n` and the request head it got.
async fn answer<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, name: &str) {
    let mut head = Vec::new();
    let mut buf = [0u8; 1024];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        match s.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => head.extend_from_slice(&buf[..n]),
        }
    }
    let body = format!("{name}\n{}", String::from_utf8_lossy(&head));
    let resp = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
    let _ = s.write_all(resp.as_bytes()).await;
    let _ = s.shutdown().await;
}

/// A backend on loopback: plain, or TLS with `<cert>.crt/.key`.
async fn backend(name: &'static str, tls: Option<(&Path, &str)>) -> SocketAddr {
    let acceptor = tls.map(|(dir, cert)| {
        let chain = vec![CertificateDer::from_pem_file(dir.join(format!("{cert}.crt"))).unwrap()];
        let key = PrivateKeyDer::from_pem_file(dir.join(format!("{cert}.key"))).unwrap();
        TlsAcceptor::from(Arc::new(ServerConfig::builder().with_no_client_auth().with_single_cert(chain, key).unwrap()))
    });
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                match acceptor {
                    Some(a) => {
                        if let Ok(t) = a.accept(s).await {
                            answer(t, name).await
                        }
                    }
                    None => answer(s, name).await,
                }
            });
        }
    });
    addr
}

fn service(name: &str, port: u16) -> Value {
    json!({"metadata": {"namespace": "mon", "name": name},
        "spec": {"clusterIP": "127.0.0.1", "ports": [{"name": "web", "port": port, "targetPort": port}]}})
}

fn route(ns: &str, name: &str, created: &str, spec: Value) -> Value {
    json!({"metadata": {"namespace": ns, "name": name, "creationTimestamp": created, "generation": 1}, "spec": spec})
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port()
}

async fn request<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, host: &str, path: &str) -> String {
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer T\r\n\r\n").as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), s.read_to_end(&mut out)).await;
    String::from_utf8_lossy(&out).into_owned()
}

async fn plain(port: u16, host: &str, path: &str) -> String {
    match TcpStream::connect(("127.0.0.1", port)).await {
        Ok(s) => request(s, host, path).await,
        Err(_) => String::new(),
    }
}

/// A TLS connection to the router for `name`, verified against the test CA.
/// Returns the stream and the leaf certificate it was served.
async fn tls(port: u16, ca: &Path, name: &str) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, Vec<u8>), String> {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from_pem_file(ca).unwrap()).unwrap();
    let cfg = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.map_err(|e| e.to_string())?;
    let s = TlsConnector::from(Arc::new(cfg))
        .connect(ServerName::try_from(name.to_string()).unwrap(), tcp)
        .await
        .map_err(|e| e.to_string())?;
    let leaf = s.get_ref().1.peer_certificates().unwrap()[0].to_vec();
    Ok((s, leaf))
}

async fn until<F: AsyncFnMut() -> bool>(secs: u64, mut f: F) -> bool {
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

fn ingress(state: &Arc<Mutex<Api>>, ns: &str, name: &str) -> Value {
    let st = state.lock().unwrap();
    st.routes
        .iter()
        .find(|r| r["metadata"]["namespace"] == ns && r["metadata"]["name"] == name)
        .map(|r| r["status"]["ingress"].clone())
        .unwrap_or(Value::Null)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn openshift_routes_are_served_and_admitted() {
    let dir = tmpdir();
    if !make_ca(&dir) {
        eprintln!("skipped: no openssl to make test certificates");
        return;
    }
    make_leaf(&dir, "wild", "DNS:*.storm1.test");
    make_leaf(&dir, "secure", "DNS:secure.mon.svc");
    make_leaf(&dir, "pt", "DNS:pt.storm1.test");

    let web = backend("web", None).await;
    let secure = backend("secure", Some((&dir, "secure"))).await;
    let pt = backend("passthrough", Some((&dir, "pt"))).await;
    let edge = |policy: &str| json!({"termination": "edge", "insecureEdgeTerminationPolicy": policy});
    let to = |svc: &str| json!({"kind": "Service", "name": svc, "weight": 100});
    let state = Arc::new(Mutex::new(Api {
        services: vec![service("web", web.port()), service("secure", secure.port()), service("pt", pt.port())],
        routes: vec![
            // No host: served as <name>-<namespace>.<the certificate's domain>.
            route("mon", "prom", "2026-11-25T00:00:00Z", json!({"to": to("web"), "port": {"targetPort": "web"}, "tls": edge("Redirect")})),
            route("mon", "allow", "2026-11-25T00:00:01Z", json!({"host": "allow.storm1.test", "to": to("web"), "tls": edge("Allow")})),
            route("mon", "deny", "2026-11-25T00:00:02Z", json!({"host": "deny.storm1.test", "to": to("web"), "tls": {"termination": "edge"}})),
            route("mon", "re", "2026-11-25T00:00:03Z", json!({"host": "re.storm1.test", "to": to("secure"), "tls": {"termination": "reencrypt"}})),
            route("mon", "pt", "2026-11-25T00:00:04Z", json!({"host": "pt.storm1.test", "to": to("pt"), "tls": {"termination": "passthrough"}})),
            // Another namespace, later, on a held host: not admitted.
            route("other", "dup", "2026-11-25T00:00:05Z", json!({"host": "allow.storm1.test", "to": to("web")})),
        ],
        ..Default::default()
    }));
    let api = fake_api(state.clone()).await;
    let (http, https) = (free_port().await, free_port().await);
    let pair = CertPair { cert_file: path(&dir, "wild.crt"), key_file: path(&dir, "wild.key") };
    tokio::spawn(router::run(RouterCfg {
        listen: format!("127.0.0.1:{http}"),
        apiserver: api,
        poll_secs: 1,
        insecure: true,
        // The router itself does not redirect: each Route's policy shows.
        tls: Some(TlsCfg { listen: format!("127.0.0.1:{https}"), certs: vec![pair], redirect: false, reload_secs: 1 }),
        backend_ca_file: Some(path(&dir, "ca.crt")),
        ca_file: None,
        token_file: None,
        route_domain: None,
    }));

    // status.ingress: what `oc` reads.
    let host = "prom-mon.storm1.test";
    assert!(
        until(10, async || ingress(&state, "mon", "prom")[0]["host"] == host).await,
        "status.ingress written with the default host: {}",
        ingress(&state, "mon", "prom")
    );
    let i = ingress(&state, "mon", "prom");
    assert_eq!(i[0]["routerName"], "stormlb");
    assert_eq!(i[0]["wildcardPolicy"], "None");
    assert_eq!((&i[0]["conditions"][0]["type"], &i[0]["conditions"][0]["status"]), (&json!("Admitted"), &json!("True")));
    assert!(until(10, async || ingress(&state, "other", "dup")[0]["conditions"][0]["reason"] == "HostAlreadyClaimed").await);
    assert_eq!(ingress(&state, "other", "dup")[0]["conditions"][0]["status"], "False");

    // Edge: terminated with the router's certificate, the request proxied
    // with its bearer token.
    let ca = dir.join("ca.crt");
    let (s, leaf) = tls(https, &ca, host).await.expect("TLS to the router");
    assert_eq!(leaf, CertificateDer::from_pem_file(dir.join("wild.crt")).unwrap().to_vec());
    let r = request(s, host, "/api/v1/query?query=up").await;
    assert!(r.starts_with("HTTP/1.1 200") && r.contains("\r\n\r\nweb\n"), "{r}");
    assert!(r.contains("GET /api/v1/query?query=up HTTP/1.1") && r.to_ascii_lowercase().contains("authorization: bearer t"), "{r}");
    assert!(r.to_ascii_lowercase().contains("x-forwarded-proto: https"), "{r}");

    // Plain HTTP, per Route: Redirect, Allow, None (no router redirect: 404).
    let r = plain(http, host, "/api/v1/query?query=up").await;
    assert!(r.starts_with("HTTP/1.1 308"), "{r}");
    assert!(r.contains(&format!("location: https://{host}:{https}/api/v1/query?query=up")), "{r}");
    let r = plain(http, "allow.storm1.test", "/").await;
    assert!(r.starts_with("HTTP/1.1 200") && r.contains("\r\n\r\nweb\n"), "{r}");
    let r = plain(http, "deny.storm1.test", "/").await;
    assert!(r.starts_with("HTTP/1.1 404") && r.contains("https only"), "{r}");

    // Reencrypt: TLS to the backend, verified for secure.mon.svc.
    let (s, _) = tls(https, &ca, "re.storm1.test").await.expect("TLS to the router");
    let r = request(s, "re.storm1.test", "/").await;
    assert!(r.starts_with("HTTP/1.1 200") && r.contains("\r\n\r\nsecure\n"), "{r}");

    // Passthrough: the client's TLS reaches the backend, which serves its
    // own certificate.
    let (s, leaf) = tls(https, &ca, "pt.storm1.test").await.expect("TLS through the router");
    assert_eq!(leaf, CertificateDer::from_pem_file(dir.join("pt.crt")).unwrap().to_vec(), "the backend's certificate, not the router's");
    let r = request(s, "pt.storm1.test", "/").await;
    assert!(r.starts_with("HTTP/1.1 200") && r.contains("\r\n\r\npassthrough\n"), "{r}");

    // Status is written once: a steady table patches nothing more.
    let n = state.lock().unwrap().patches.len();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(state.lock().unwrap().patches.len(), n, "{:?}", state.lock().unwrap().patches);
    let _ = std::fs::remove_dir_all(dir);
}
