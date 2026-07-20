//! BGP-anycast advertisement of the VIP (L3, active-active via ECMP).
//!
//! Phase 2. The design and interface are fixed here so the rest of stormlb (and
//! stormcos config) can target it; the BGP finite-state machine + UPDATE
//! encoding land next. Unlike VRRP (one node owns the VIP), BGP-anycast has
//! *every* healthy node advertise the same /32, and the upstream router
//! ECMP-hashes flows across them — true active-active with route-withdraw
//! failover. This is also the bidirectional-peering path (we can import routes,
//! e.g. pod CIDRs, to complement Cilium's BGP).

use crate::config::BgpCfg;
use anyhow::Result;

/// A BGP speaker that advertises the VIP /32 to configured peers and withdraws
/// it when this node's backends go unhealthy.
pub trait Speaker: Send + Sync {
    /// Advertise `vip/32` (start/refresh the anycast announcement).
    fn advertise(&self, vip: &str) -> Result<()>;
    /// Withdraw `vip/32` (stop attracting traffic to this node).
    fn withdraw(&self, vip: &str) -> Result<()>;
}

/// Placeholder speaker until the BGP FSM lands. Constructing it validates config
/// and logs intent; advertise/withdraw are no-ops that return an explicit error
/// so nothing silently believes anycast is active.
pub struct PendingSpeaker {
    pub local_asn: u32,
    pub peers: Vec<(String, u32)>,
}

impl PendingSpeaker {
    pub fn from_config(cfg: &BgpCfg) -> Self {
        Self {
            local_asn: cfg.local_asn,
            peers: cfg
                .peers
                .iter()
                .map(|p| (p.address.clone(), p.asn))
                .collect(),
        }
    }
}

impl Speaker for PendingSpeaker {
    fn advertise(&self, _vip: &str) -> Result<()> {
        anyhow::bail!("BGP anycast is not implemented yet (phase 2) — use VRRP (L2) for now")
    }
    fn withdraw(&self, _vip: &str) -> Result<()> {
        Ok(())
    }
}
