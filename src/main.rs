//! stormlb — pre-cluster control-plane VIP load balancer.

use anyhow::Result;
use clap::Parser;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stormlb::vip::{IpCmd, VipController};
use stormlb::{balancer, bgp, config, health, pool::Pool, vrrp};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "stormlb", about = "Storm control-plane VIP load balancer")]
struct Cli {
    /// Path to the TOML config.
    #[arg(short, long, env = "STORMLB_CONFIG", default_value = "/etc/stormlb/stormlb.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let cfg = config::load(&cli.config)?;

    // Resolve backends up front so a bad address fails loudly at startup.
    let mut addrs = Vec::new();
    for b in &cfg.backends {
        addrs.push(b.socket_addr()?);
    }
    if addrs.is_empty() {
        warn!("no backends configured — the balancer will refuse every connection");
    }
    let pool = Arc::new(Pool::new(addrs));
    let vip: Ipv4Addr = cfg
        .vip
        .address
        .parse()
        .map_err(|_| anyhow::anyhow!("vip.address must be an IPv4 address"))?;

    info!(
        "stormlb starting — vip={}:{} backends={} health={:?}",
        cfg.vip.address,
        cfg.vip.port,
        cfg.backends.len(),
        cfg.health.mode
    );

    // Health loop drives pool membership.
    tokio::spawn(health::run(pool.clone(), cfg.health.clone()));

    // VIP presentation. L2 (VRRP) claims the VIP on the elected Master; L3 (BGP
    // anycast) has every healthy node advertise the /32. Without either, we
    // assume the VIP is already local / bind 0.0.0.0 (single-node / testing).
    if cfg.vrrp.enabled {
        let iface = cfg.vrrp.interface.clone();
        let (vrid, prio, ai) = (
            cfg.vrrp.vrid,
            cfg.vrrp.priority,
            cfg.vrrp.advert_interval_secs,
        );
        // VRRP is a blocking control loop (raw socket + iproute2) — own OS thread.
        std::thread::spawn(move || {
            let ctl: Arc<dyn VipController> = Arc::new(IpCmd);
            if let Err(e) = vrrp::run(vrid, prio, ai, &iface, vip, ctl) {
                warn!("VRRP loop exited: {e}");
            }
        });
    }

    if cfg.bgp.enabled {
        match bgp::preflight(&cfg.bgp) {
            Ok(next_hop) => {
                // Advertise the VIP while this node has healthy backends.
                let advertise = Arc::new(AtomicBool::new(false));
                let p = pool.clone();
                let adv = advertise.clone();
                tokio::spawn(async move {
                    loop {
                        adv.store(p.healthy_count() > 0, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
                bgp::spawn(&cfg.bgp, vip, next_hop, advertise);
            }
            Err(e) => warn!("bgp disabled: {e}"),
        }
    }

    // L4 balancer listens on the configured bind address (default 0.0.0.0).
    let listen = format!("{}:{}", cfg.vip.bind, cfg.vip.port).parse()?;
    balancer::run(listen, pool).await
}
