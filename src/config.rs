//! stormlb configuration (TOML).

use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct Config {
    /// The L4 VIP balancer. Optional, because a node can run only the L7
    /// router — on single-node the VIP is the node's own address and there
    /// is nothing for VRRP to float.
    pub vip: Option<Vip>,
    #[serde(default, rename = "backend")]
    pub backends: Vec<BackendCfg>,
    #[serde(default)]
    pub health: HealthSpec,
    #[serde(default)]
    pub vrrp: VrrpCfg,
    #[serde(default)]
    pub bgp: BgpCfg,
    /// The L7 Host-header router over HTTPRoutes. Present = enabled.
    #[serde(default)]
    pub router: Option<crate::router::RouterCfg>,
}

/// The virtual IP fronted by this balancer, and where the L4 proxy listens.
#[derive(Debug, Deserialize)]
pub struct Vip {
    /// The VIP address (owned via VRRP or advertised via BGP).
    pub address: String,
    pub port: u16,
    /// Address the L4 proxy binds. Defaults to `0.0.0.0` so the proxy works
    /// whether or not the VIP is currently local (bind to the VIP once VRRP
    /// ownership is wired).
    #[serde(default = "default_bind")]
    pub bind: String,
}
fn default_bind() -> String {
    "0.0.0.0".to_string()
}

#[derive(Debug, Deserialize)]
pub struct BackendCfg {
    pub address: String,
    pub port: u16,
}
impl BackendCfg {
    pub fn socket_addr(&self) -> anyhow::Result<SocketAddr> {
        Ok(format!("{}:{}", self.address, self.port).parse()?)
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct HealthSpec {
    #[serde(default)]
    pub mode: HealthMode,
    /// Request path for http/https checks (e.g. `/readyz`).
    #[serde(default = "default_path")]
    pub path: String,
    #[serde(default = "default_interval")]
    pub interval_secs: u64,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// HTTP status codes considered healthy.
    #[serde(default = "default_expect")]
    pub expect_status: Vec<u16>,
}
impl Default for HealthSpec {
    fn default() -> Self {
        Self {
            mode: HealthMode::Tcp,
            path: default_path(),
            interval_secs: default_interval(),
            timeout_secs: default_timeout(),
            expect_status: default_expect(),
        }
    }
}
fn default_path() -> String {
    "/readyz".to_string()
}
fn default_interval() -> u64 {
    2
}
fn default_timeout() -> u64 {
    2
}
fn default_expect() -> Vec<u16> {
    vec![200]
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum HealthMode {
    /// A successful TCP connect marks the backend healthy.
    #[default]
    Tcp,
    Http,
    Https,
}

/// VRRP (L2) VIP ownership — one node holds the VIP, sub-second failover.
#[derive(Debug, Deserialize, Default)]
pub struct VrrpCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub interface: String,
    #[serde(default = "default_vrid")]
    pub vrid: u8,
    /// Higher wins. 255 = address owner (starts Master).
    #[serde(default = "default_priority")]
    pub priority: u8,
    #[serde(default = "default_advert")]
    pub advert_interval_secs: u64,
}
fn default_vrid() -> u8 {
    51
}
fn default_priority() -> u8 {
    100
}
fn default_advert() -> u64 {
    1
}

/// BGP-anycast (L3) advertisement — active-active across nodes via ECMP.
#[derive(Debug, Deserialize, Default)]
pub struct BgpCfg {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub local_asn: u32,
    #[serde(default)]
    pub router_id: String,
    #[serde(default)]
    pub peers: Vec<BgpPeer>,
}
#[derive(Debug, Deserialize, Clone)]
pub struct BgpPeer {
    pub address: String,
    pub asn: u32,
}

pub fn load(path: &str) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("reading config {path}: {e}"))?;
    toml::from_str(&text).map_err(|e| anyhow::anyhow!("parsing config {path}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_config() {
        let toml = r#"
[vip]
address = "192.168.8.50"
port = 6443

[[backend]]
address = "192.168.8.98"
port = 6443
[[backend]]
address = "192.168.8.99"
port = 6443

[health]
mode = "https"
path = "/readyz"
interval_secs = 1
expect_status = [200]

[vrrp]
enabled = true
interface = "eth0"
vrid = 51
priority = 200
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        let vip = cfg.vip.expect("the fixture declares a vip");
        assert_eq!(vip.port, 6443);
        assert_eq!(vip.bind, "0.0.0.0"); // default
        assert_eq!(cfg.backends.len(), 2);
        assert_eq!(cfg.backends[1].socket_addr().unwrap().port(), 6443);
        assert_eq!(cfg.health.mode, HealthMode::Https);
        assert!(cfg.vrrp.enabled);
        assert_eq!(cfg.vrrp.priority, 200);
        assert!(!cfg.bgp.enabled); // absent section
    }

    #[test]
    fn health_defaults_to_tcp() {
        let cfg: Config = toml::from_str("[vip]\naddress=\"10.0.0.1\"\nport=443\n").unwrap();
        assert_eq!(cfg.health.mode, HealthMode::Tcp);
        assert_eq!(cfg.health.interval_secs, 2);
    }
}
