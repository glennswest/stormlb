//! Backend health checks that drive pool membership.

use crate::config::{HealthMode, HealthSpec};
use crate::pool::Pool;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tracing::info;

/// Run the health loop forever: probe every backend each interval and update
/// its healthy flag, logging transitions.
pub async fn run(pool: Arc<Pool>, spec: HealthSpec) {
    // A permissive client (self-signed apiserver certs are the norm on-prem).
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(spec.timeout_secs.max(1)))
        .build()
        .ok();

    loop {
        for be in &pool.backends {
            let ok = check(be.addr, &spec, client.as_ref()).await;
            if ok != be.is_healthy() {
                info!(
                    "backend {} -> {}",
                    be.addr,
                    if ok { "healthy" } else { "unhealthy" }
                );
            }
            be.set_healthy(ok);
        }
        sleep(Duration::from_secs(spec.interval_secs.max(1))).await;
    }
}

/// Probe a single backend per the health spec.
pub async fn check(addr: SocketAddr, spec: &HealthSpec, client: Option<&reqwest::Client>) -> bool {
    let dur = Duration::from_secs(spec.timeout_secs.max(1));
    match spec.mode {
        HealthMode::Tcp => timeout(dur, TcpStream::connect(addr))
            .await
            .map(|r| r.is_ok())
            .unwrap_or(false),
        HealthMode::Http | HealthMode::Https => {
            let Some(client) = client else { return false };
            let scheme = if spec.mode == HealthMode::Https {
                "https"
            } else {
                "http"
            };
            let path = if spec.path.starts_with('/') {
                spec.path.clone()
            } else {
                format!("/{}", spec.path)
            };
            let url = format!("{scheme}://{addr}{path}");
            match client.get(&url).send().await {
                Ok(resp) => spec.expect_status.contains(&resp.status().as_u16()),
                Err(_) => false,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn tcp_check_up_and_down() {
        let spec = HealthSpec {
            mode: HealthMode::Tcp,
            timeout_secs: 1,
            ..Default::default()
        };
        // A listening port is healthy...
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        assert!(check(addr, &spec, None).await);
        // ...and a closed port is not.
        drop(l);
        // Give the OS a moment to release the port, then a fresh unused addr.
        let unused: SocketAddr = "127.0.0.1:1".parse().unwrap();
        assert!(!check(unused, &spec, None).await);
    }
}
