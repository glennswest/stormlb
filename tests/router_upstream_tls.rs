//! Integration test: routes to TLS-only backends (#13). The real router
//! against a fake apiserver; backends that serve TLS with certificates made
//! by openssl at test time (skipped without it): one signed by the CA the
//! router trusts (IP 127.0.0.1 and DNS svc.test), one by another CA.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use stormlb::router::{self, RouterCfg};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

fn tmpdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("stormlb-up-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn sh(dir: &Path, cmd: &str) -> bool {
    std::process::Command::new("sh").arg("-c").arg(cmd).current_dir(dir).output().map(|o| o.status.success()).unwrap_or(false)
}

fn make_certs(dir: &Path) -> bool {
    let leaf = |name: &str, ca: &str, san: &str| {
        format!(
            "openssl req -newkey rsa:2048 -nodes -subj /CN={name} -keyout {name}.key -out {name}.csr \
             && printf 'subjectAltName={san}\\nbasicConstraints=CA:FALSE\\nextendedKeyUsage=serverAuth\\n' > {name}.ext \
             && openssl x509 -req -in {name}.csr -CA {ca}.crt -CAkey {ca}.key -CAcreateserial -days 1 -extfile {name}.ext -out {name}.crt"
        )
    };
    sh(dir, "command -v openssl")
        && sh(dir, "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=cluster-ca -keyout ca.key -out ca.crt")
        && sh(dir, "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=other-ca -keyout other.key -out other.crt")
        && sh(dir, &leaf("be", "ca", "IP:127.0.0.1,DNS:svc.test"))
        && sh(dir, &leaf("rogue", "other", "IP:127.0.0.1,DNS:svc.test"))
}

/// An https backend that answers with the request head it got.
async fn tls_backend(dir: &Path, name: &str) -> SocketAddr {
    let certs: Vec<CertificateDer> = CertificateDer::pem_file_iter(dir.join(format!("{name}.crt"))).unwrap().map(|c| c.unwrap()).collect();
    let key = PrivateKeyDer::from_pem_file(dir.join(format!("{name}.key"))).unwrap();
    let cfg = tokio_rustls::rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut s) = acceptor.accept(s).await else { return };
                let mut head = Vec::new();
                let mut buf = [0u8; 2048];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let resp = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", head.len());
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.write_all(&head).await;
                let _ = s.shutdown().await;
            });
        }
    });
    addr
}

/// A plain backend: 200 "plain".
async fn plain_backend() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = s.read(&mut buf).await;
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\nconnection: close\r\n\r\nplain").await;
            });
        }
    });
    addr
}

/// A route: (host, backend, annotations beyond storm.io/backend).
type Route = (&'static str, SocketAddr, Vec<(&'static str, &'static str)>);

async fn fake_api(routes: Vec<Route>) -> String {
    let items: Vec<String> = routes
        .iter()
        .map(|(host, be, extra)| {
            let mut ann = format!(r#""storm.io/backend":"{be}""#);
            for (k, v) in extra {
                ann.push_str(&format!(r#","{k}":"{v}""#));
            }
            format!(r#"{{"metadata":{{"namespace":"t","name":"{host}","annotations":{{{ann}}}}},"spec":{{"hostnames":["{host}"]}}}}"#)
        })
        .collect();
    let body = format!(r#"{{"items":[{}]}}"#, items.join(","));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
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

async fn get(port: u16, host: &str) -> String {
    let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)).await else { return String::new() };
    let _ = c.write_all(format!("GET /p HTTP/1.1\r\nHost: {host}\r\n\r\n").as_bytes()).await;
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(15), c.read_to_end(&mut out)).await;
    String::from_utf8_lossy(&out).into_owned()
}

fn tls_errors(host: &str) -> f64 {
    let t = stormlb::metrics::global().render(&[]);
    let series = format!(r#"stormlb_router_upstream_errors_total{{host="{host}",kind="tls"}}"#);
    t.lines().find_map(|l| l.strip_prefix(&series)?.strip_prefix(' ')?.parse().ok()).unwrap_or(0.0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn https_backends_are_verified_against_the_ca_or_not_reached() {
    let dir = tmpdir();
    if !make_certs(&dir) {
        eprintln!("skipped: no openssl to make test certificates");
        return;
    }
    let good = tls_backend(&dir, "be").await;
    let rogue = tls_backend(&dir, "rogue").await;
    let plain = plain_backend().await;
    let https = ("storm.io/backend-protocol", "https");
    let api = fake_api(vec![
        ("ip.test", good, vec![https]),
        ("name.test", good, vec![https, ("storm.io/backend-server-name", "svc.test")]),
        ("wrongname.test", good, vec![https, ("storm.io/backend-server-name", "nope.test")]),
        ("rogue.test", rogue, vec![https]),
        ("tlsport.test", good, vec![]),
        ("plain.test", plain, vec![]),
    ])
    .await;
    // The CA file doesn't exist yet: https backends must fail closed.
    let trust = dir.join("trust.crt");
    let port = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
    tokio::spawn(router::run(RouterCfg {
        listen: format!("127.0.0.1:{port}"),
        apiserver: api,
        poll_secs: 1,
        insecure: true,
        tls: None,
        backend_ca_file: Some(trust.to_str().unwrap().into()),
        ca_file: None,
        token_file: None,
    }));
    let end = Instant::now() + Duration::from_secs(10);
    while !get(port, "plain.test").await.ends_with("plain") {
        assert!(Instant::now() < end, "routes never loaded");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(get(port, "ip.test").await, "", "no CA loaded: nothing is sent to an https backend");
    assert!(tls_errors("ip.test") >= 1.0);

    // The CA appears: picked up on the next poll.
    std::fs::copy(dir.join("ca.crt"), &trust).unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    let mut r = String::new();
    while !r.starts_with("HTTP/1.1 200") {
        assert!(Instant::now() < end, "the CA was never picked up: {r:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
        r = get(port, "ip.test").await;
    }
    // Verified by IP SAN, and the head arrived intact over TLS.
    assert!(r.contains("GET /p HTTP/1.1\r\nHost: ip.test\r\n"), "{r}");
    // Verified by the name the route gives (svc.test, also the SNI).
    assert!(get(port, "name.test").await.contains("Host: name.test"));
    // A name the certificate doesn't carry, and a backend signed by another
    // CA: refused, nothing reaches the client.
    let before = tls_errors("rogue.test");
    assert_eq!(get(port, "wrongname.test").await, "");
    assert_eq!(get(port, "rogue.test").await, "");
    assert!(tls_errors("wrongname.test") >= 1.0 && tls_errors("rogue.test") > before);
    // Without the annotation the dial is plain, as before: a TLS port answers
    // nothing usable.
    assert!(!get(port, "tlsport.test").await.starts_with("HTTP/1.1 200"));
    assert!(get(port, "plain.test").await.ends_with("plain"));
    let _ = std::fs::remove_dir_all(dir);
}
