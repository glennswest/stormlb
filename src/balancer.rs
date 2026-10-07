//! L4 (TCP) load balancer: accept on the VIP and proxy to a healthy backend.

use crate::pool::Pool;
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

/// Bind `listen` and serve forever (see [`serve`]).
pub async fn run(listen: SocketAddr, pool: Arc<Pool>) -> Result<()> {
    let listener = bind(listen)?;
    info!("L4 balancer listening on {listen}");
    serve(listener, pool).await
}

/// Bind a listener for a VIP. `IP_FREEBIND` lets a node that does not hold
/// the VIP yet (a VRRP Backup) listen on it, so it serves the moment the
/// address arrives; `SO_REUSEADDR` lets a restart rebind past TIME_WAIT.
pub fn bind(listen: SocketAddr) -> Result<TcpListener> {
    use socket2::{Domain, Socket, Type};
    let sock = Socket::new(Domain::for_address(listen), Type::STREAM, None)
        .with_context(|| format!("socket for {listen}"))?;
    sock.set_reuse_address(true)?;
    if listen.is_ipv4() {
        sock.set_freebind(true)?;
    } else {
        sock.set_freebind_ipv6(true)?;
    }
    sock.bind(&listen.into()).with_context(|| format!("binding L4 balancer on {listen}"))?;
    sock.listen(1024).with_context(|| format!("listening on {listen}"))?;
    sock.set_nonblocking(true)?;
    TcpListener::from_std(sock.into()).with_context(|| format!("registering {listen}"))
}

/// Accept on an already-bound listener and proxy each connection to a healthy
/// backend. Loops until a fatal accept error.
pub async fn serve(listener: TcpListener, pool: Arc<Pool>) -> Result<()> {
    serve_vip(listener, pool, "").await
}

/// [`serve`], counting into the metrics under `vip`'s name (#12).
pub async fn serve_vip(listener: TcpListener, pool: Arc<Pool>, vip: &str) -> Result<()> {
    let vip: Arc<str> = vip.into();
    loop {
        let (client, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("accept error: {e}");
                continue;
            }
        };
        let (pool, vip) = (pool.clone(), vip.clone());
        tokio::spawn(async move {
            let m = crate::metrics::global();
            let l = [("vip", &*vip)];
            m.inc("stormlb_vip_connections_total", &l);
            let _active = m.active("stormlb_vip_connections_active", &l);
            let Some(be) = pool.pick() else {
                m.inc("stormlb_vip_no_healthy_backend_total", &l);
                warn!("no healthy backend for {peer} — dropping");
                return;
            };
            match TcpStream::connect(be.addr).await {
                Ok(upstream) => {
                    debug!("proxy {peer} -> {}", be.addr);
                    if let Err(e) = proxy(client, upstream).await {
                        debug!("proxy {peer} -> {} closed: {e}", be.addr);
                    }
                }
                Err(e) => {
                    m.inc("stormlb_vip_upstream_connect_errors_total", &l);
                    warn!("connect backend {} failed: {e}", be.addr)
                }
            }
        });
    }
}

async fn proxy(mut client: TcpStream, mut upstream: TcpStream) -> Result<()> {
    let _ = client.set_nodelay(true);
    let _ = upstream.set_nodelay(true);
    tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    Ok(())
}
