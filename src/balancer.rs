//! L4 (TCP) load balancer: accept on the VIP and proxy to a healthy backend.

use crate::pool::Pool;
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

/// Bind `listen` and serve forever (see [`serve`]).
pub async fn run(listen: SocketAddr, pool: Arc<Pool>) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding L4 balancer on {listen}"))?;
    info!("L4 balancer listening on {listen}");
    serve(listener, pool).await
}

/// Accept on an already-bound listener and proxy each connection to a healthy
/// backend. Loops until a fatal accept error.
pub async fn serve(listener: TcpListener, pool: Arc<Pool>) -> Result<()> {
    loop {
        let (client, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("accept error: {e}");
                continue;
            }
        };
        let pool = pool.clone();
        tokio::spawn(async move {
            let Some(be) = pool.pick() else {
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
                Err(e) => warn!("connect backend {} failed: {e}", be.addr),
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
