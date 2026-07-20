//! stormlb — pre-cluster control-plane VIP load balancer.

use anyhow::Result;
use clap::Parser;
use std::sync::Arc;
use stormlb::{balancer, config, health, pool::Pool, vip::VipController, vrrp};
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

    info!(
        "stormlb starting — vip={}:{} backends={} health={:?}",
        cfg.vip.address,
        cfg.vip.port,
        cfg.backends.len(),
        cfg.health.mode
    );

    // Health loop drives pool membership.
    tokio::spawn(health::run(pool.clone(), cfg.health.clone()));

    // VIP ownership. VRRP (L2) claims the VIP on this node when Master; BGP
    // (L3 anycast) is phase 2. Without either, we assume the VIP is already
    // local (or we bind 0.0.0.0) — useful for single-node / testing.
    if cfg.bgp.enabled {
        warn!("bgp.enabled is set but BGP anycast is phase 2 — falling back to local bind");
    }
    if cfg.vrrp.enabled {
        spawn_vrrp(&cfg, pool.clone());
    }

    // L4 balancer listens on the configured bind address (default 0.0.0.0).
    let listen = format!("{}:{}", cfg.vip.bind, cfg.vip.port).parse()?;
    balancer::run(listen, pool).await
}

/// Wire the VRRP virtual router to VIP claim/release. NOTE: the raw-socket
/// advertise/receive loop is the remaining wiring (README "VRRP wiring"); today
/// this drives the state machine + VIP control from the config, which is enough
/// for a single configured Master and exercises the claim path.
fn spawn_vrrp(cfg: &config::Config, _pool: Arc<Pool>) {
    let mut vr = vrrp::VirtualRouter::new(
        cfg.vrrp.vrid,
        cfg.vrrp.priority,
        cfg.vrrp.advert_interval_secs,
    );
    let vip = cfg.vip.address.clone();
    let iface = cfg.vrrp.interface.clone();
    tokio::spawn(async move {
        let ctl = stormlb::vip::IpCmd;
        let state = vr.start();
        info!("VRRP vrid={} priority={} initial state={:?}", vr.vrid, vr.priority, state);
        if state == vrrp::State::Master {
            if let Err(e) = ctl.claim(&vip, &iface) {
                warn!("VRRP could not claim VIP {vip} on {iface}: {e}");
            }
        }
        // TODO(phase-2): join 224.0.0.18, send/receive adverts, drive
        // on_advertisement / on_master_down_timeout, and claim/release on
        // transitions. See README.
    });
}
