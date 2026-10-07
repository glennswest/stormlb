//! Integration test: the router terminating TLS (#14). The real router
//! (`router::run`) against a fake apiserver holding one HTTPRoute, a backend
//! that echoes the request head it got, and certificates made with openssl at
//! test time (skipped without it): a CA, `*.storm1.test` (+ IP 127.0.0.1),
//! and `other.example`.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use stormlb::router::{self, RouterCfg};
use stormlb::tls::{CertPair, TlsCfg};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

fn tmpdir(what: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("stormlb-rtls-{what}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn sh(dir: &Path, cmd: &str) -> bool {
    std::process::Command::new("sh").arg("-c").arg(cmd).current_dir(dir).output().map(|o| o.status.success()).unwrap_or(false)
}

/// A CA, and a leaf `<name>.crt/.key` it signs with `san`.
fn make_ca(dir: &Path) -> bool {
    sh(dir, "command -v openssl")
        && sh(dir, "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=test-ca -keyout ca.key -out ca.crt")
}
fn make_leaf(dir: &Path, name: &str, san: &str) {
    let cmd = format!(
        "openssl req -newkey rsa:2048 -nodes -subj /CN={name} -keyout {name}.key -out {name}.csr \
         && printf 'subjectAltName={san}\\nbasicConstraints=CA:FALSE\\nextendedKeyUsage=serverAuth\\n' > {name}.ext \
         && openssl x509 -req -in {name}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 1 -extfile {name}.ext -out {name}.crt.new \
         && mv {name}.key {name}.key.new && mv {name}.crt.new {name}.crt && mv {name}.key.new {name}.key"
    );
    assert!(sh(dir, &cmd), "openssl leaf {name}");
}

/// Answers every request with one HTTPRoute: app.storm1.test -> `backend`.
async fn fake_api(backend: SocketAddr) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    let body = format!(
        r#"{{"items":[{{"metadata":{{"namespace":"t","name":"app","annotations":{{"storm.io/backend":"{backend}"}}}},"spec":{{"hostnames":["app.storm1.test"]}}}}]}}"#
    );
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

/// Answers with the request head it received, as the body.
async fn echo_backend() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0u8; 1024];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let resp = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", head.len());
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.write_all(&head).await;
            });
        }
    });
    addr
}

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port()
}

async fn request<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, host: &str, path: &str) -> String {
    s.write_all(format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nX-Forwarded-Proto: http\r\n\r\n").as_bytes()).await.unwrap();
    let mut out = Vec::new();
    let _ = s.read_to_end(&mut out).await;
    String::from_utf8_lossy(&out).into_owned()
}

/// A TLS connection to the router, verifying against the test CA for `name`
/// (an IP sends no SNI). Returns the stream and the leaf it was served.
async fn tls(port: u16, ca: &Path, name: &str) -> Result<(tokio_rustls::client::TlsStream<TcpStream>, Vec<u8>), String> {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from_pem_file(ca).unwrap()).unwrap();
    let cfg = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.map_err(|e| e.to_string())?;
    let sn = ServerName::try_from(name.to_string()).unwrap();
    let s = TlsConnector::from(Arc::new(cfg)).connect(sn, tcp).await.map_err(|e| e.to_string())?;
    let leaf = s.get_ref().1.peer_certificates().unwrap()[0].to_vec();
    Ok((s, leaf))
}

fn der(path: &Path) -> Vec<u8> {
    CertificateDer::from_pem_file(path).unwrap().to_vec()
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_router_terminates_tls_picks_certificates_by_sni_and_redirects() {
    let dir = tmpdir("main");
    if !make_ca(&dir) {
        eprintln!("skipped: no openssl to make test certificates");
        return;
    }
    // The pairs are configured before they exist: no redirect until they do.
    let pair = |n: &str| CertPair { cert_file: dir.join(format!("{n}.crt")).to_str().unwrap().into(), key_file: dir.join(format!("{n}.key")).to_str().unwrap().into() };
    let backend = echo_backend().await;
    let api = fake_api(backend).await;
    let (http, https) = (free_port().await, free_port().await);
    let cfg = RouterCfg {
        listen: format!("127.0.0.1:{http}"),
        apiserver: api,
        poll_secs: 1,
        insecure: true,
        tls: Some(TlsCfg { listen: format!("127.0.0.1:{https}"), certs: vec![pair("wild"), pair("other")], redirect: true, reload_secs: 1 }),
        backend_ca_file: None,
        ca_file: None,
        token_file: None,
    };
    tokio::spawn(router::run(cfg));
    // An empty answer while the router is still binding: `until` retries.
    let plain = || async {
        match TcpStream::connect(("127.0.0.1", http)).await {
            Ok(c) => request(c, "app.storm1.test", "/x?y=1").await,
            Err(_) => String::new(),
        }
    };
    assert!(until(10, async || plain().await.starts_with("HTTP/1.1 200")).await, "the route serves on plain HTTP while no certificate exists");

    // The certificates appear: picked up on the next reload.
    make_leaf(&dir, "wild", "DNS:*.storm1.test,IP:127.0.0.1");
    make_leaf(&dir, "other", "DNS:other.example");
    let ca = dir.join("ca.crt");
    assert!(until(5, async || tls(https, &ca, "app.storm1.test").await.is_ok()).await, "TLS once the files exist");

    // SNI: the wildcard for its names, the exact certificate for its own.
    let (s, leaf) = tls(https, &ca, "app.storm1.test").await.unwrap();
    assert_eq!(leaf, der(&dir.join("wild.crt")));
    let body = request(s, "app.storm1.test", "/x?y=1").await;
    assert!(body.starts_with("HTTP/1.1 200"), "{body}");
    assert!(body.contains("X-Forwarded-Proto: https\r\n"), "the backend is told https: {body}");
    assert!(!body.to_ascii_lowercase().contains("x-forwarded-proto: http\r\n"), "the client's own claim is dropped: {body}");
    assert_eq!(tls(https, &ca, "other.example").await.unwrap().1, der(&dir.join("other.crt")));
    // No SNI (an IP): the first pair.
    assert_eq!(tls(https, &ca, "127.0.0.1").await.unwrap().1, der(&dir.join("wild.crt")));
    // A name no pair covers gets the first, which the client refuses.
    assert!(tls(https, &ca, "nope.example").await.is_err());
    // A wildcard is one label: a.b.storm1.test is not covered.
    assert!(tls(https, &ca, "a.b.storm1.test").await.is_err());

    // Plain HTTP now redirects, with the path and query, to the TLS port...
    let r = plain().await;
    assert!(r.starts_with("HTTP/1.1 308"), "{r}");
    assert!(r.contains(&format!("location: https://app.storm1.test:{https}/x?y=1\r\n")), "{r}");
    // ...except the health probe.
    let h = request(TcpStream::connect(("127.0.0.1", http)).await.unwrap(), "127.0.0.1", "/healthz").await;
    assert!(h.starts_with("HTTP/1.1 200") && h.ends_with("router alive\n"), "{h}");

    // A renewed certificate in place is served without a restart.
    make_leaf(&dir, "other", "DNS:other.example,DNS:renewed.example");
    assert!(until(5, async || tls(https, &ca, "renewed.example").await.is_ok()).await, "the renewed certificate is picked up");

    // A broken file keeps the last good one.
    std::fs::write(dir.join("other.crt"), "garbage").unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(tls(https, &ca, "renewed.example").await.is_ok(), "the last good certificate stays");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_key_that_is_not_the_certificates_is_refused() {
    let dir = tmpdir("mismatch");
    if !make_ca(&dir) {
        eprintln!("skipped: no openssl to make test certificates");
        return;
    }
    make_leaf(&dir, "a", "DNS:a.test");
    make_leaf(&dir, "b", "DNS:b.test");
    let c = stormlb::tls::Certs::new(vec![CertPair {
        cert_file: dir.join("a.crt").to_str().unwrap().into(),
        key_file: dir.join("b.key").to_str().unwrap().into(),
    }]);
    assert_eq!(c.count(), 0);
    let _ = std::fs::remove_dir_all(dir);
}
