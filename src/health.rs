//! Backend health checks that drive pool membership.
//!
//! The spec can change at runtime ([`Checker::set_spec`]): the loop picks it
//! up on its next round, and a change wakes it at once, so a backend added
//! through the API is probed straight away rather than after an interval.

use crate::config::{HealthMode, HealthSpec};
use crate::pool::Pool;
use anyhow::Context;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::time::timeout;
use tracing::{info, warn};

/// A VIP's health spec and the HTTP client built from it.
pub struct Checker {
    state: RwLock<(HealthSpec, Option<reqwest::Client>)>,
    wake: Notify,
}

impl Checker {
    /// Build the checker; fails when an `https` spec names a CA file that
    /// cannot be read or holds no certificate.
    pub fn new(spec: HealthSpec) -> anyhow::Result<Self> {
        let c = client(&spec)?;
        Ok(Self { state: RwLock::new((spec, c)), wake: Notify::new() })
    }

    pub fn spec(&self) -> HealthSpec {
        self.state.read().unwrap().0.clone()
    }

    /// Replace the spec (validated as in [`Checker::new`]) and wake the loop.
    pub fn set_spec(&self, spec: HealthSpec) -> anyhow::Result<()> {
        let c = client(&spec)?;
        *self.state.write().unwrap() = (spec, c);
        self.wake.notify_one();
        Ok(())
    }

    /// Wake the loop now (e.g. the pool's members changed).
    pub fn wake(&self) {
        self.wake.notify_one();
    }
}

/// The client an http/https check uses. With `ca_file`, an https backend
/// must present a certificate chaining to it (and naming the address dialled);
/// without, any certificate is accepted, as before `ca_file` existed.
pub fn client(spec: &HealthSpec) -> anyhow::Result<Option<reqwest::Client>> {
    if spec.mode == HealthMode::Tcp {
        return Ok(None);
    }
    let mut b = reqwest::Client::builder().timeout(Duration::from_secs(spec.timeout_secs.max(1)));
    match &spec.ca_file {
        Some(path) => {
            let pem = std::fs::read(path).with_context(|| format!("health.ca_file {path}"))?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem)
                .with_context(|| format!("health.ca_file {path}: not PEM"))?;
            anyhow::ensure!(!certs.is_empty(), "health.ca_file {path}: no certificate in it");
            b = b.tls_built_in_root_certs(false);
            for c in certs {
                b = b.add_root_certificate(c);
            }
        }
        // Self-signed apiserver certificates are the norm on-prem.
        None => b = b.danger_accept_invalid_certs(true),
    }
    Ok(Some(b.build().context("building the health-check client")?))
}

/// Run the health loop forever: probe every backend each interval and update
/// its healthy flag, logging transitions.
pub async fn run(pool: Arc<Pool>, checker: Arc<Checker>) {
    loop {
        let (spec, client) = checker.state.read().unwrap().clone();
        for be in pool.backends() {
            let ok = check(be.addr, &spec, client.as_ref()).await;
            if ok != be.is_healthy() {
                info!("backend {} -> {}", be.addr, if ok { "healthy" } else { "unhealthy" });
            }
            be.set_healthy(ok);
        }
        let _ = timeout(Duration::from_secs(spec.interval_secs.max(1)), checker.wake.notified()).await;
    }
}

/// Run the health loop with a fixed spec (the TOML-only path).
pub async fn run_spec(pool: Arc<Pool>, spec: HealthSpec) {
    match Checker::new(spec) {
        Ok(c) => run(pool, Arc::new(c)).await,
        Err(e) => warn!("health checks disabled: {e:#}"),
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

    #[test]
    fn a_missing_or_empty_ca_file_is_refused() {
        let spec = |f: &str| HealthSpec { mode: HealthMode::Https, ca_file: Some(f.into()), ..Default::default() };
        let e = Checker::new(spec("/nonexistent/ca.crt")).err().unwrap();
        assert!(format!("{e:#}").contains("/nonexistent/ca.crt"), "{e:#}");
        let dir = std::env::temp_dir().join(format!("stormlb-ca-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.crt");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        assert!(Checker::new(spec(empty.to_str().unwrap())).is_err());
        let _ = std::fs::remove_dir_all(&dir);
        // tcp never builds a client, so a CA file is not read.
        assert!(Checker::new(HealthSpec { ca_file: Some("/nonexistent".into()), ..Default::default() }).is_ok());
    }
}
