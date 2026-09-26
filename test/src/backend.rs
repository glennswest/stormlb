//! The backend the suites route to: a listener in this container that says
//! who it is. Every answer names the backend, the Host it was asked for, the
//! path, which request on the connection this is and how many body bytes
//! arrived — so a test can tell from the body alone where the router sent
//! it and what it passed through.
//!
//! - `/stream`: sends `first`, waits 1.5 s, sends `second`, closes — a
//!   router that buffers responses holds `first` back.
//! - `Upgrade:` (any path): `101`, then echoes bytes both ways until EOF.
//! - anything else: `200` with a content-length, keep-alive unless asked to
//!   close.

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use crate::http::{head_end, header};

pub struct Backend {
    pub name: String,
    /// `ip:port` the router dials (the route's `storm.io/backend`).
    pub addr: String,
    pub port: u16,
    /// Requests answered, all connections.
    pub hits: Arc<AtomicU64>,
    task: JoinHandle<()>,
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Backend {
    /// Listen on every address, on a port the kernel picks, and advertise
    /// `reach:port`.
    pub async fn start(name: &str, reach: IpAddr) -> std::io::Result<Backend> {
        let any = if reach.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
        let l = TcpListener::bind(any).await?;
        let port = l.local_addr()?.port();
        let hits = Arc::new(AtomicU64::new(0));
        let (n, h) = (name.to_string(), hits.clone());
        let task = tokio::spawn(async move {
            while let Ok((s, _)) = l.accept().await {
                let (n, h) = (n.clone(), h.clone());
                tokio::spawn(async move {
                    let _ = serve(s, &n, &h).await;
                });
            }
        });
        Ok(Backend { name: name.into(), addr: crate::env::join(&reach.to_string(), port), port, hits, task })
    }
}

async fn serve(mut s: TcpStream, name: &str, hits: &AtomicU64) -> std::io::Result<()> {
    let _ = s.set_nodelay(true);
    let mut buf = Vec::new();
    let mut tmp = vec![0u8; 65536];
    let mut nreq = 0u64;
    loop {
        let end = loop {
            if let Some((e, _)) = head_end(&buf) {
                break e;
            }
            let n = s.read(&mut tmp).await?;
            if n == 0 {
                return Ok(());
            }
            buf.extend_from_slice(&tmp[..n]);
        };
        let rest = buf.split_off(end);
        let head = String::from_utf8_lossy(&std::mem::replace(&mut buf, rest)).into_owned();
        nreq += 1;
        hits.fetch_add(1, Ordering::Relaxed);
        let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
        let host = header(&head, "host").unwrap_or_default();

        if header(&head, "upgrade").is_some() {
            s.write_all(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: stormlb-test\r\nConnection: Upgrade\r\n\r\n").await?;
            if !buf.is_empty() {
                s.write_all(&buf).await?;
            }
            let (mut r, mut w) = s.split();
            tokio::io::copy(&mut r, &mut w).await?;
            return Ok(());
        }

        // Count the body through; only its length matters.
        let want = header(&head, "content-length").and_then(|v| v.parse::<usize>().ok()).unwrap_or(0);
        let mut got = buf.len().min(want);
        buf.drain(..got);
        while got < want {
            let n = s.read(&mut tmp).await?;
            if n == 0 {
                return Ok(());
            }
            let take = n.min(want - got);
            got += take;
            buf.extend_from_slice(&tmp[take..n]);
        }

        if path == "/stream" {
            s.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\nconnection: close\r\n\r\nfirst\n").await?;
            s.flush().await?;
            tokio::time::sleep(Duration::from_millis(1500)).await;
            s.write_all(b"second\n").await?;
            return Ok(());
        }

        let close = header(&head, "connection").is_some_and(|v| v.eq_ignore_ascii_case("close"));
        let body = format!("backend={name} host={host} path={path} req={nreq} got={got}\n");
        let resp = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n{}\r\n{body}",
            body.len(),
            if close { "connection: close\r\n" } else { "" }
        );
        s.write_all(resp.as_bytes()).await?;
        if close {
            return Ok(());
        }
    }
}
