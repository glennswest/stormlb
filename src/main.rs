//! stormlb — pre-cluster control-plane VIP load balancer.

use anyhow::Result;
use clap::Parser;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use stormlb::vips::{Registry, CONFIG_VIP};
use stormlb::{bgp, config};
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
    if cfg.vip.is_none() && cfg.router.is_none() && cfg.api.is_none() {
        anyhow::bail!("nothing to do: none of [vip], [router] or [api] is configured");
    }

    let state_file = cfg.api.as_ref().and_then(|a| a.state_file.clone()).map(Into::into);
    let reg = Arc::new(Registry::new(state_file));

    // The TOML [vip] is the VIP `default`: a bad address or a listener that
    // cannot bind fails loudly at startup, as before the API existed.
    if let Some(spec) = Registry::config_spec(&cfg)? {
        if spec.backends.is_empty() {
            warn!("[vip] has no backends — the balancer will refuse every connection");
        }
        info!(
            "stormlb starting — vip={}:{} backends={} health={:?}",
            spec.address,
            spec.port,
            spec.backends.len(),
            spec.health.mode
        );
        reg.start_config(spec)?;
    }

    // L3: every node with a healthy backend advertises the config VIP's /32.
    if let (true, Some(v)) = (cfg.bgp.enabled, &cfg.vip) {
        let vip: Ipv4Addr = v.address.parse().map_err(|_| anyhow::anyhow!("bgp needs vip.address to be IPv4"))?;
        match bgp::preflight(&cfg.bgp) {
            Ok(next_hop) => {
                let advertise = Arc::new(AtomicBool::new(false));
                let pool = reg.pool(CONFIG_VIP).expect("the config VIP was started above");
                let adv = advertise.clone();
                tokio::spawn(async move {
                    loop {
                        adv.store(pool.healthy_count() > 0, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                });
                bgp::spawn(&cfg.bgp, vip, next_hop, advertise);
            }
            Err(e) => warn!("bgp disabled: {e}"),
        }
    }

    if cfg.api.is_some() {
        match reg.load_state() {
            Ok(0) => {}
            Ok(n) => info!("{n} saved VIPs serving again"),
            Err(e) => warn!("saved VIPs not loaded: {e:#}"),
        }
    }

    // The router alone is a complete configuration (single node: the VIP is
    // the node's own address and only the Host demux is wanted). Then its
    // exit is the process's; beside a VIP or the API it is only logged.
    let router_alone = cfg.vip.is_none() && cfg.api.is_none();
    let router = async {
        match cfg.router.clone() {
            Some(rcfg) if router_alone => {
                info!("stormlb starting — router only");
                stormlb::router::run(rcfg).await
            }
            Some(rcfg) => {
                if let Err(e) = stormlb::router::run(rcfg).await {
                    warn!("router exited: {e:#}");
                }
                std::future::pending().await
            }
            None => std::future::pending().await,
        }
    };
    let api = async {
        match cfg.api.clone() {
            Some(a) => stormlb::api::run(a, reg.clone()).await,
            None => std::future::pending().await,
        }
    };
    tokio::select! {
        r = router => r,
        r = api => r,
    }
}
