//! Integration test: the L4 balancer round-robins across healthy backends and
//! fails over when one goes unhealthy — the core control-plane-VIP behaviour.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use stormlb::balancer;
use stormlb::pool::Pool;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A fake backend that, on connect, writes its `id` byte then echoes input —
/// so a client can tell which backend served it.
async fn spawn_backend(id: u8) -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = l.accept().await {
            tokio::spawn(async move {
                let _ = sock.write_all(&[id]).await;
                let mut buf = [0u8; 64];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if sock.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

/// Connect through the balancer and return the id byte of the backend hit.
async fn who(balancer: SocketAddr) -> u8 {
    let mut c = TcpStream::connect(balancer).await.unwrap();
    let mut id = [0u8; 1];
    c.read_exact(&mut id).await.unwrap();
    id[0]
}

#[tokio::test]
async fn round_robins_and_fails_over() {
    let a = spawn_backend(b'1').await;
    let b = spawn_backend(b'2').await;
    let pool = Arc::new(Pool::new([a, b]));
    for be in &pool.backends {
        be.set_healthy(true);
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let baddr = listener.local_addr().unwrap();
    let p = pool.clone();
    tokio::spawn(async move {
        let _ = balancer::serve(listener, p).await;
    });

    // Round-robin: both backends are hit across several connections.
    let mut seen = HashSet::new();
    for _ in 0..8 {
        seen.insert(who(baddr).await);
    }
    assert!(
        seen.contains(&b'1') && seen.contains(&b'2'),
        "expected traffic to both backends, saw {seen:?}"
    );

    // Failover: mark backend '1' down — everything must go to '2'.
    pool.backends[0].set_healthy(false);
    for _ in 0..6 {
        assert_eq!(who(baddr).await, b'2', "unhealthy backend must be skipped");
    }

    // All down: the balancer accepts then drops with no data.
    pool.backends[1].set_healthy(false);
    let mut c = TcpStream::connect(baddr).await.unwrap();
    let mut buf = [0u8; 1];
    let n = c.read(&mut buf).await.unwrap_or(0);
    assert_eq!(n, 0, "no healthy backend -> connection dropped");
}
