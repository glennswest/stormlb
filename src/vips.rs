//! Named VIPs that can be created, changed and removed at runtime.
//!
//! Each VIP is an L4 listener on `bind:port` (default: the VIP's own address,
//! bound with `IP_FREEBIND`), a [`Pool`] of backends, a health loop, and
//! optionally a VRRP instance that owns the address. [`Registry::apply`]
//! validates a spec and binds any new listener *before* touching the running
//! VIP, so a change that cannot be made leaves the old one serving. Backends
//! that stay keep their health, and connections already proxied are never cut.
//!
//! The VIP from the TOML `[vip]` table is `default` and is read-only here; the
//! ones made through the API ([`crate::api`]) are saved to `state_file`.

use crate::config::{Config, HealthSpec};
use crate::health::{self, Checker};
use crate::pool::Pool;
use crate::vip::{Netlink, VipController};
use crate::{balancer, vrrp};
use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn};

/// The name of the VIP the TOML `[vip]` table declares.
pub const CONFIG_VIP: &str = "default";

/// What a VIP is: the body of `PUT /api/v1/vips/{name}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VipSpec {
    /// The VIP (IPv4 or IPv6; VRRP needs IPv4).
    pub address: String,
    pub port: u16,
    /// The address the listener binds. Default: `address` itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bind: Option<String>,
    #[serde(default)]
    pub backends: Vec<BackendSpec>,
    #[serde(default)]
    pub health: HealthSpec,
    /// Own the address with VRRP. Unset: the address is assumed to be
    /// present (or arrive) by other means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vrrp: Option<VrrpSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendSpec {
    /// An IP address (no hostnames: a VIP must not depend on DNS).
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VrrpSpec {
    pub interface: String,
    #[serde(default = "default_vrid")]
    pub vrid: u8,
    /// 1–255, higher wins; 255 = the address owner, starts Master.
    #[serde(default = "default_priority")]
    pub priority: u8,
    /// Take over from a lower-priority Master (RFC 5798 Preempt_Mode).
    #[serde(default = "default_preempt")]
    pub preempt: bool,
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
fn default_preempt() -> bool {
    true
}

/// A VIP as `GET` returns it: the spec, plus where it stands.
#[derive(Debug, Serialize)]
pub struct VipView {
    pub name: String,
    #[serde(flatten)]
    pub spec: VipSpec,
    pub status: Status,
}

#[derive(Debug, Serialize)]
pub struct Status {
    /// Where the listener is bound.
    pub listening: String,
    /// `config` (the TOML `[vip]`, read-only) or `api`.
    pub source: &'static str,
    pub healthy: usize,
    pub backends: Vec<BackendStatus>,
    /// `master`, `backup` or `stopped`; absent without VRRP.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vrrp: Option<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct BackendStatus {
    pub address: String,
    pub port: u16,
    pub healthy: bool,
}

/// Why a change was not made (or not saved). Maps onto an HTTP status.
#[derive(Debug)]
pub enum ApplyError {
    /// The request is wrong: 400.
    Invalid(String),
    /// It conflicts with what runs (a listener in use, a read-only VIP): 409.
    Conflict(String),
    /// No such VIP: 404.
    NotFound(String),
    /// Applied, but `state_file` could not be written: 500.
    NotSaved(String),
}

impl std::fmt::Display for ApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApplyError::Invalid(s) | ApplyError::Conflict(s) | ApplyError::NotFound(s) | ApplyError::NotSaved(s) => f.write_str(s),
        }
    }
}

struct Entry {
    spec: VipSpec,
    from_config: bool,
    listen: SocketAddr,
    pool: Arc<Pool>,
    checker: Arc<Checker>,
    health: JoinHandle<()>,
    serve: JoinHandle<()>,
    vrrp: Option<Arc<vrrp::Handle>>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.health.abort();
        self.serve.abort();
        if let Some(h) = &self.vrrp {
            h.stop();
        }
    }
}

/// Every VIP this process serves.
pub struct Registry {
    vips: Mutex<BTreeMap<String, Entry>>,
    /// Saved VIPs that did not start; kept in `state_file` until applied or
    /// removed.
    pending: Mutex<BTreeMap<String, VipSpec>>,
    state_file: Option<PathBuf>,
    ctl: Arc<dyn VipController>,
}

/// The validated form of a spec.
struct Checked {
    listen: SocketAddr,
    backends: Vec<SocketAddr>,
    vrrp_vip: Option<Ipv4Addr>,
}

impl Registry {
    pub fn new(state_file: Option<PathBuf>) -> Self {
        Self::with_controller(state_file, Arc::new(Netlink))
    }

    /// With a given VIP controller (tests use a recording mock).
    pub fn with_controller(state_file: Option<PathBuf>, ctl: Arc<dyn VipController>) -> Self {
        Self { vips: Mutex::new(BTreeMap::new()), pending: Mutex::new(BTreeMap::new()), state_file, ctl }
    }

    /// The `[vip]` table of a TOML config as a spec, if it has one.
    pub fn config_spec(cfg: &Config) -> anyhow::Result<Option<VipSpec>> {
        let Some(v) = &cfg.vip else { return Ok(None) };
        let vrrp = cfg.vrrp.enabled.then(|| VrrpSpec {
            interface: cfg.vrrp.interface.clone(),
            vrid: cfg.vrrp.vrid,
            priority: cfg.vrrp.priority,
            preempt: cfg.vrrp.preempt,
            advert_interval_secs: cfg.vrrp.advert_interval_secs,
        });
        Ok(Some(VipSpec {
            address: v.address.clone(),
            port: v.port,
            bind: Some(v.bind.clone()),
            backends: cfg.backends.iter().map(|b| BackendSpec { address: b.address.clone(), port: b.port }).collect(),
            health: cfg.health.clone(),
            vrrp,
        }))
    }

    /// Start the TOML `[vip]` as the read-only VIP `default`.
    pub fn start_config(&self, spec: VipSpec) -> anyhow::Result<()> {
        self.put(CONFIG_VIP, spec, true).map(|_| ()).map_err(|e| anyhow::anyhow!("[vip]: {e}"))
    }

    /// Start the VIPs saved in `state_file` (absent file: none). One that no
    /// longer starts (its listener taken, its CA file gone) is skipped and
    /// logged, and stays in the file.
    pub fn load_state(&self) -> anyhow::Result<usize> {
        let Some(path) = &self.state_file else { return Ok(0) };
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
        };
        let saved: State = serde_json::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        let mut n = 0;
        for (name, spec) in saved.vips {
            match self.put(&name, spec.clone(), false) {
                Ok(_) => n += 1,
                Err(e) => {
                    warn!("saved VIP {name} not started: {e}");
                    // Keep it, so a fix to the node and a restart bring it back.
                    self.pending.lock().unwrap().insert(name, spec);
                }
            }
        }
        Ok(n)
    }

    /// Create or replace the API VIP `name`. `Ok(true)` when it was created.
    pub fn apply(&self, name: &str, spec: VipSpec) -> Result<bool, ApplyError> {
        valid_name(name)?;
        if self.vips.lock().unwrap().get(name).is_some_and(|e| e.from_config) {
            return Err(ApplyError::Conflict(format!("{name} is the [vip] of the config file; change it there")));
        }
        let created = self.put(name, spec, false)?;
        self.pending.lock().unwrap().remove(name);
        self.save()?;
        Ok(created)
    }

    /// Remove the API VIP `name`: its listener closes (connections already
    /// proxied run on), its VRRP instance releases the address.
    pub fn remove(&self, name: &str) -> Result<(), ApplyError> {
        {
            let mut vips = self.vips.lock().unwrap();
            match vips.get(name) {
                None => {
                    if self.pending.lock().unwrap().remove(name).is_none() {
                        return Err(ApplyError::NotFound(format!("no VIP {name}")));
                    }
                }
                Some(e) if e.from_config => {
                    return Err(ApplyError::Conflict(format!("{name} is the [vip] of the config file; remove it there")))
                }
                Some(_) => {
                    vips.remove(name);
                    info!("VIP {name} removed");
                }
            }
        }
        self.save()
    }

    /// Stop every VIP (listeners closed, health loops ended, VRRP releasing
    /// its address) without touching `state_file`: what a process exit does.
    pub fn stop_all(&self) {
        std::mem::take(&mut *self.vips.lock().unwrap());
    }

    pub fn get(&self, name: &str) -> Option<VipView> {
        self.vips.lock().unwrap().get(name).map(|e| view(name, e))
    }

    pub fn list(&self) -> Vec<VipView> {
        self.vips.lock().unwrap().iter().map(|(n, e)| view(n, e)).collect()
    }

    /// A VIP's pool (BGP advertises the config VIP while it has a healthy one).
    pub fn pool(&self, name: &str) -> Option<Arc<Pool>> {
        self.vips.lock().unwrap().get(name).map(|e| e.pool.clone())
    }

    fn put(&self, name: &str, spec: VipSpec, from_config: bool) -> Result<bool, ApplyError> {
        let c = check(&spec)?;
        let mut vips = self.vips.lock().unwrap();
        if let Some((other, _)) = vips.iter().find(|(n, e)| n.as_str() != name && e.listen == c.listen) {
            return Err(ApplyError::Conflict(format!("{} is already VIP {other}'s listener", c.listen)));
        }
        match vips.get_mut(name) {
            Some(e) => {
                // Everything that can fail first, so a refusal changes nothing.
                health_ok(&spec.health)?;
                let listener = if c.listen != e.listen { Some(bind(c.listen)?) } else { None };
                if let Some(l) = listener {
                    let old = std::mem::replace(&mut e.serve, spawn_serve(l, e.pool.clone(), name));
                    old.abort();
                    info!("VIP {name}: listener moved {} -> {}", e.listen, c.listen);
                    e.listen = c.listen;
                }
                e.pool.set_addrs(c.backends);
                // Wakes the health loop, which probes new members at once.
                e.checker.set_spec(spec.health.clone()).map_err(|e| ApplyError::Invalid(format!("health: {e:#}")))?;
                if spec.vrrp != e.spec.vrrp || spec.address != e.spec.address {
                    if let Some(h) = e.vrrp.take() {
                        h.stop();
                    }
                    e.vrrp = start_vrrp(name, &spec, c.vrrp_vip, self.ctl.clone(), e.pool.clone());
                }
                e.spec = spec;
                info!("VIP {name} updated: {} backends", e.pool.backends().len());
                Ok(false)
            }
            None => {
                let checker = Arc::new(Checker::new(spec.health.clone()).map_err(|e| ApplyError::Invalid(format!("health: {e:#}")))?);
                let listener = bind(c.listen)?;
                let pool = Arc::new(Pool::new(c.backends));
                let health = tokio::spawn(health::run(pool.clone(), checker.clone()));
                let serve = spawn_serve(listener, pool.clone(), name);
                let vrrp = start_vrrp(name, &spec, c.vrrp_vip, self.ctl.clone(), pool.clone());
                info!("VIP {name} serving on {} -> {} backends", c.listen, pool.backends().len());
                vips.insert(name.to_string(), Entry { spec, from_config, listen: c.listen, pool, checker, health, serve, vrrp });
                Ok(true)
            }
        }
    }

    /// Write the API VIPs (and saved ones that did not start) to `state_file`.
    fn save(&self) -> Result<(), ApplyError> {
        let Some(path) = &self.state_file else { return Ok(()) };
        let mut vips: BTreeMap<String, VipSpec> = self.pending.lock().unwrap().clone();
        for (n, e) in self.vips.lock().unwrap().iter() {
            if !e.from_config {
                vips.insert(n.clone(), e.spec.clone());
            }
        }
        let text = serde_json::to_string_pretty(&State { vips }).expect("specs serialize");
        write_atomic(path, &text).map_err(|e| ApplyError::NotSaved(format!("applied, but not saved to {}: {e:#}", path.display())))
    }
}

/// `state_file`'s shape.
#[derive(Serialize, Deserialize)]
struct State {
    vips: BTreeMap<String, VipSpec>,
}

fn write_atomic(path: &std::path::Path, text: &str) -> anyhow::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, text).with_context(|| format!("writing {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("renaming {} into place", tmp.display()))?;
    Ok(())
}

fn view(name: &str, e: &Entry) -> VipView {
    let backends: Vec<BackendStatus> = e
        .pool
        .backends()
        .iter()
        .map(|b| BackendStatus { address: b.addr.ip().to_string(), port: b.addr.port(), healthy: b.is_healthy() })
        .collect();
    VipView {
        name: name.to_string(),
        spec: e.spec.clone(),
        status: Status {
            listening: e.listen.to_string(),
            source: if e.from_config { "config" } else { "api" },
            healthy: backends.iter().filter(|b| b.healthy).count(),
            backends,
            vrrp: e.vrrp.as_ref().map(|h| h.state()),
        },
    }
}

fn spawn_serve(l: tokio::net::TcpListener, pool: Arc<Pool>, name: &str) -> JoinHandle<()> {
    let name = name.to_string();
    tokio::spawn(async move {
        if let Err(e) = balancer::serve_vip(l, pool, &name).await {
            warn!("L4 listener exited: {e:#}");
        }
    })
}

fn bind(listen: SocketAddr) -> Result<tokio::net::TcpListener, ApplyError> {
    balancer::bind(listen).map_err(|e| ApplyError::Conflict(format!("{e:#}")))
}

/// Start the VIP's VRRP instance. It holds the address only while the VIP has
/// a healthy backend.
fn start_vrrp(name: &str, spec: &VipSpec, vip: Option<Ipv4Addr>, ctl: Arc<dyn VipController>, pool: Arc<Pool>) -> Option<Arc<vrrp::Handle>> {
    let (v, vip) = (spec.vrrp.clone()?, vip?);
    let h = Arc::new(vrrp::Handle::default());
    let h2 = h.clone();
    let name = name.to_string();
    let params = vrrp::Params {
        vrid: v.vrid,
        priority: v.priority,
        preempt: v.preempt,
        advert_interval_secs: v.advert_interval_secs,
        iface: v.interface,
        vip,
    };
    let healthy: vrrp::Healthy = Arc::new(move || pool.healthy_count() > 0);
    // VRRP is a blocking control loop (raw socket) — its own OS thread.
    std::thread::spawn(move || {
        if let Err(e) = vrrp::run(params, ctl, healthy, h2) {
            warn!("VIP {name}: VRRP exited: {e:#}");
        }
    });
    Some(h)
}

fn valid_name(name: &str) -> Result<(), ApplyError> {
    let ok = !name.is_empty()
        && name.len() <= 63
        && name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if ok {
        Ok(())
    } else {
        Err(ApplyError::Invalid(format!("VIP name {name:?}: 1–63 of [a-z0-9-], not starting or ending with '-'")))
    }
}

/// Validate a spec without touching anything running.
fn check(spec: &VipSpec) -> Result<Checked, ApplyError> {
    let bad = |s: String| ApplyError::Invalid(s);
    let vip: IpAddr = spec.address.parse().map_err(|_| bad(format!("address {:?} is not an IP address", spec.address)))?;
    if spec.port == 0 {
        return Err(bad("port must be 1–65535".into()));
    }
    let bind_ip: IpAddr = match &spec.bind {
        None => vip,
        Some(b) => b.parse().map_err(|_| bad(format!("bind {b:?} is not an IP address")))?,
    };
    let mut backends = Vec::new();
    for b in &spec.backends {
        let ip: IpAddr = b.address.parse().map_err(|_| bad(format!("backend address {:?} is not an IP address", b.address)))?;
        if b.port == 0 {
            return Err(bad(format!("backend {}: port must be 1–65535", b.address)));
        }
        backends.push(SocketAddr::new(ip, b.port));
    }
    let h = &spec.health;
    if !h.path.starts_with('/') {
        return Err(bad(format!("health.path {:?} must start with '/'", h.path)));
    }
    if h.interval_secs == 0 || h.timeout_secs == 0 {
        return Err(bad("health.interval_secs and health.timeout_secs must be at least 1".into()));
    }
    if h.expect_status.is_empty() {
        return Err(bad("health.expect_status must name at least one status".into()));
    }
    let vrrp_vip = match &spec.vrrp {
        None => None,
        Some(v) => {
            let IpAddr::V4(v4) = vip else {
                return Err(bad("vrrp needs an IPv4 address".into()));
            };
            if v.interface.is_empty() {
                return Err(bad("vrrp.interface is required".into()));
            }
            if v.vrid == 0 || v.priority == 0 {
                return Err(bad("vrrp.vrid and vrrp.priority must be 1–255".into()));
            }
            if v.advert_interval_secs == 0 || v.advert_interval_secs > 40 {
                return Err(bad("vrrp.advert_interval_secs must be 1–40".into()));
            }
            Some(v4)
        }
    };
    Ok(Checked { listen: SocketAddr::new(bind_ip, spec.port), backends, vrrp_vip })
}

/// Whether `spec` would be accepted by [`Checker::set_spec`].
fn health_ok(spec: &HealthSpec) -> Result<(), ApplyError> {
    health::client(spec).map(|_| ()).map_err(|e| ApplyError::Invalid(format!("health: {e:#}")))
}
