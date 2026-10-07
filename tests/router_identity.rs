//! Integration test: the router's identity toward the apiserver (#9, #10).
//! A fake apiserver over TLS (openssl-made certificate, skipped without
//! openssl) that answers 403 unless the request carries the bearer token it
//! currently expects. The router verifies it against `ca_file` and reads its
//! token from `token_file` on every poll.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use stormlb::router::{self, RouterCfg};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

fn tmpdir() -> PathBuf {
    let d = std::env::temp_dir().join(format!("stormlb-ident-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn sh(dir: &Path, cmd: &str) -> bool {
    std::process::Command::new("sh").arg("-c").arg(cmd).current_dir(dir).output().map(|o| o.status.success()).unwrap_or(false)
}

fn make_certs(dir: &Path) -> bool {
    sh(dir, "command -v openssl")
        && sh(dir, "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=cluster-ca -keyout ca.key -out ca.crt")
        && sh(dir, "openssl req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=other-ca -keyout other.key -out other.crt")
        && sh(
            dir,
            "openssl req -newkey rsa:2048 -nodes -subj /CN=apiserver -keyout api.key -out api.csr \
             && printf 'subjectAltName=IP:127.0.0.1\\nbasicConstraints=CA:FALSE\\nextendedKeyUsage=serverAuth\\n' > api.ext \
             && openssl x509 -req -in api.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 1 -extfile api.ext -out api.crt",
        )
}

/// What the fake apiserver expects and what it last saw.
#[derive(Default)]
struct Api {
    want: String,
    last_auth: Option<String>,
    served: usize,
}

/// An https apiserver holding one route (app.test -> `backend`), 403 unless
/// `Authorization: Bearer <want>`.
async fn fake_api(dir: &Path, backend: SocketAddr, state: Arc<Mutex<Api>>) -> String {
    let certs: Vec<CertificateDer> = CertificateDer::pem_file_iter(dir.join("api.crt")).unwrap().map(|c| c.unwrap()).collect();
    let key = PrivateKeyDer::from_pem_file(dir.join("api.key")).unwrap();
    let cfg = tokio_rustls::rustls::ServerConfig::builder().with_no_client_auth().with_single_cert(certs, key).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(cfg));
    let body = format!(
        r#"{{"items":[{{"metadata":{{"namespace":"t","name":"app","annotations":{{"storm.io/backend":"{backend}"}}}},"spec":{{"hostnames":["app.test"]}}}}]}}"#
    );
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((s, _)) = l.accept().await {
            let (acceptor, state, body) = (acceptor.clone(), state.clone(), body.clone());
            tokio::spawn(async move {
                let Ok(mut s) = acceptor.accept(s).await else { return };
                let mut head = Vec::new();
                let mut buf = [0u8; 4096];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match s.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                let head = String::from_utf8_lossy(&head).to_string();
                let auth = head.lines().find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("authorization").then(|| v.trim().to_string())
                });
                let ok = {
                    let mut st = state.lock().unwrap();
                    st.last_auth = auth.clone();
                    let ok = auth.as_deref() == Some(&format!("Bearer {}", st.want));
                    if ok {
                        st.served += 1;
                    }
                    ok
                };
                let resp = if ok {
                    format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len())
                } else {
                    let b = r#"{"kind":"Status","code":403}"#;
                    format!("HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{b}", b.len())
                };
                let _ = s.write_all(resp.as_bytes()).await;
                let _ = s.shutdown().await;
            });
        }
    });
    format!("https://{addr}")
}

async fn backend() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = s.read(&mut buf).await;
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok").await;
            });
        }
    });
    addr
}

async fn get(port: u16, host: &str) -> String {
    let Ok(mut c) = TcpStream::connect(("127.0.0.1", port)).await else { return String::new() };
    let _ = c.write_all(format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n").as_bytes()).await;
    let mut out = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(10), c.read_to_end(&mut out)).await;
    String::from_utf8_lossy(&out).into_owned()
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

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port()
}

fn cfg(port: u16, api: &str, ca: &Path, token: &Path) -> RouterCfg {
    RouterCfg {
        listen: format!("127.0.0.1:{port}"),
        apiserver: api.into(),
        poll_secs: 1,
        insecure: false,
        tls: None,
        backend_ca_file: None,
        ca_file: Some(ca.to_str().unwrap().into()),
        token_file: Some(token.to_str().unwrap().into()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_router_reads_routes_as_itself_over_a_verified_connection() {
    let dir = tmpdir();
    if !make_certs(&dir) {
        eprintln!("skipped: no openssl to make test certificates");
        return;
    }
    let state = Arc::new(Mutex::new(Api { want: "s3cret".into(), ..Default::default() }));
    let api = fake_api(&dir, backend().await, state.clone()).await;
    let token = dir.join("token");
    let port = free_port().await;
    tokio::spawn(router::run(cfg(port, &api, &dir.join("ca.crt"), &token)));
    assert!(until(5, async || get(port, "app.test").await.starts_with("HTTP/1.1 404")).await, "the router is up");

    // No token file yet: nothing is fetched, the table stays empty.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(get(port, "app.test").await.starts_with("HTTP/1.1 404"));
    assert_eq!(state.lock().unwrap().served, 0);

    // A wrong token: the apiserver refuses it (and sees it was sent).
    std::fs::write(&token, "wrong\n").unwrap();
    assert!(until(5, async || state.lock().unwrap().last_auth.as_deref() == Some("Bearer wrong")).await, "the token is sent as a bearer");
    assert!(get(port, "app.test").await.starts_with("HTTP/1.1 404"), "a refused list leaves the table empty");

    // The right token (read on the next poll, trailing newline trimmed).
    std::fs::write(&token, "s3cret\n").unwrap();
    assert!(until(5, async || get(port, "app.test").await.ends_with("ok")).await, "routes load with the token");

    // Rotated: the apiserver wants a new token; until the file has it the
    // last table keeps serving, then the new one is used.
    state.lock().unwrap().want = "n3w".into();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(get(port, "app.test").await.ends_with("ok"), "a refused refresh keeps the last table");
    std::fs::write(&token, "n3w").unwrap();
    let served = state.lock().unwrap().served;
    assert!(until(5, async || state.lock().unwrap().served > served).await, "the rotated token is read in place");
    assert_eq!(state.lock().unwrap().last_auth.as_deref(), Some("Bearer n3w"));

    // A router that trusts another CA never talks to this apiserver.
    let other = free_port().await;
    tokio::spawn(router::run(cfg(other, &api, &dir.join("other.crt"), &token)));
    assert!(until(5, async || get(other, "app.test").await.starts_with("HTTP/1.1 404")).await);
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(get(other, "app.test").await.starts_with("HTTP/1.1 404"), "an apiserver it can't verify gives it no routes");
    let _ = std::fs::remove_dir_all(dir);
}
