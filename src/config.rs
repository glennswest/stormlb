//! stormlb configuration (TOML).

use serde::{Deserialize, Serialize};
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
    /// The VIP API: named VIPs created and changed at runtime. Present =
    /// enabled.
    #[serde(default)]
    pub api: Option<ApiCfg>,
}

/// `[api]`: the HTTP API that creates, changes and removes named VIPs at
/// runtime (`/api/v1/vips/{name}`; README "VIP API").
#[derive(Debug, Deserialize, Clone)]
pub struct ApiCfg {
    /// Where the API listens. Loopback by default: the caller (stormcluster)
    /// runs on the same node. Any other address needs `token_file`.
    #[serde(default = "default_api_listen")]
    pub listen: String,
    /// A file holding the bearer token every `/api/v1` request must carry.
    /// Re-read on each request, so it can be rotated in place. Unset: no
    /// token, which is allowed only on a loopback `listen`.
    #[serde(default)]
    pub token_file: Option<String>,
    /// Where the VIPs made through the API are kept (JSON, written atomically
    /// on each change and loaded at start), so a restart serves them again
    /// before the caller has re-applied them. Unset: kept in memory only.
    #[serde(default)]
    pub state_file: Option<String>,
}
fn default_api_listen() -> String {
    "127.0.0.1:9103".to_string()
}

/// The virtual IP fronted by this balancer, and where the L4 proxy listens.
#[derive(Debug, Deserialize)]
pub struct Vip {
    /// The VIP address (owned via VRRP or advertised via BGP).
    pub address: String,
    pub port: u16,
    /// Address the L4 proxy binds. Defaults to `0.0.0.0` so the proxy accepts
    /// whether or not this node currently holds the VIP.
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

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
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
    /// PEM CA certificate(s) an `https` check verifies the backend against
    /// (the cluster CA for apiservers). Unset: any certificate is accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<String>,
}
impl Default for HealthSpec {
    fn default() -> Self {
        Self {
            mode: HealthMode::Tcp,
            path: default_path(),
            interval_secs: default_interval(),
            timeout_secs: default_timeout(),
            expect_status: default_expect(),
            ca_file: None,
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

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum HealthMode {
    /// A successful TCP connect marks the backend healthy.
    #[default]
    Tcp,
    Http,
    Https,
}

/// VRRP (L2) VIP ownership — one node holds the VIP, sub-second failover.
#[derive(Debug, Deserialize, Serialize, Default, Clone, PartialEq, Eq)]
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
    /// A Backup takes over from a lower-priority Master (RFC 5798
    /// Preempt_Mode, default on).
    #[serde(default = "default_preempt")]
    pub preempt: bool,
    #[serde(default = "default_advert")]
    pub advert_interval_secs: u64,
}
fn default_preempt() -> bool {
    true
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
        assert!(cfg.vrrp.preempt, "preempt defaults on");
        assert!(!cfg.bgp.enabled); // absent section
    }

    #[test]
    fn api_defaults_to_loopback_with_no_token_or_state() {
        let cfg: Config = toml::from_str("[api]\n").unwrap();
        let api = cfg.api.unwrap();
        assert_eq!(api.listen, "127.0.0.1:9103");
        assert!(api.token_file.is_none() && api.state_file.is_none());
        assert!(cfg.vip.is_none() && cfg.router.is_none());
    }

    #[test]
    fn health_takes_a_ca_file() {
        let cfg: Config = toml::from_str("[health]\nmode = \"https\"\nca_file = \"/data/stormcert/ca.crt\"\n").unwrap();
        assert_eq!(cfg.health.ca_file.as_deref(), Some("/data/stormcert/ca.crt"));
    }

    #[test]
    fn health_defaults_to_tcp() {
        let cfg: Config = toml::from_str("[vip]\naddress=\"10.0.0.1\"\nport=443\n").unwrap();
        assert_eq!(cfg.health.mode, HealthMode::Tcp);
        assert_eq!(cfg.health.interval_secs, 2);
    }
}
