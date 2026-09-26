//! What the runner hands the container (stormcentral `docs/test-standard.md`),
//! and the few knobs stormlb's suites add to it.

use std::net::{IpAddr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant};

/// Where the kubelet mounts the Job's ServiceAccount.
pub const SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

pub struct Env {
    pub suite: String,
    pub run_id: String,
    pub namespace: String,
    /// `STORM_API`: the apiserver the suites create HTTPRoutes through.
    pub api: String,
    /// `STORM_NODE`: the node under test.
    pub node: String,
    /// The router, `host:port`. `STORMLB_ROUTER`, else `STORM_NODE:80` (the
    /// golden's `listen = "auto:80"` binds the node's address).
    pub router: String,
    /// stormd's API in the stormlb container, `host:port`. `STORMLB_STORMD`,
    /// else `STORM_NODE:180` (a service golden's port + 100); `none` when
    /// there is no stormd (the hermetic harness).
    pub stormd: Option<String>,
    pub token: Option<String>,
    pub ca: Option<Vec<u8>>,
    pub timeout: Duration,
    pub started: Instant,
    /// How long a route change may take to show. The router polls every 5 s,
    /// so 30 s is six polls. `STORMLB_ROUTE_WAIT` (seconds).
    pub route_wait: Duration,
    /// How long to wait before calling something *not* routed: two and a
    /// bit polls. `STORMLB_SETTLE` (seconds).
    pub settle: Duration,
}

impl Env {
    pub fn read() -> Env {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let secs = |k: &str, d: u64| Duration::from_secs(var(k).and_then(|v| v.parse().ok()).unwrap_or(d));
        let sa = |f: &str| std::fs::read_to_string(format!("{SA_DIR}/{f}")).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty());
        let suite = var("STORM_SUITE").unwrap_or_else(|| "short".into());
        let node = var("STORM_NODE").unwrap_or_default();
        let budget = match suite.as_str() {
            "medium" => 1800,
            "long" => 8 * 3600,
            _ => 120,
        };
        Env {
            run_id: var("STORM_RUN_ID").unwrap_or_default(),
            namespace: var("STORM_NAMESPACE").or_else(|| sa("namespace")).unwrap_or_default(),
            api: var("STORM_API").unwrap_or_default(),
            router: var("STORMLB_ROUTER").unwrap_or_else(|| join(&node, 80)),
            stormd: match var("STORMLB_STORMD").as_deref() {
                Some("none") => None,
                Some(s) => Some(s.to_string()),
                None => Some(join(&node, 180)),
            },
            token: sa("token"),
            ca: std::fs::read(format!("{SA_DIR}/ca.crt")).ok(),
            timeout: secs("STORM_TIMEOUT", budget),
            started: Instant::now(),
            route_wait: secs("STORMLB_ROUTE_WAIT", 30),
            settle: secs("STORMLB_SETTLE", 12),
            suite,
            node,
        }
    }

    /// What the runner must have set and did not.
    pub fn missing(&self) -> Vec<&'static str> {
        let mut m = Vec::new();
        if self.node.is_empty() && self.router.starts_with(':') {
            m.push("STORM_NODE");
        }
        if self.api.is_empty() {
            m.push("STORM_API");
        }
        if self.run_id.is_empty() {
            m.push("STORM_RUN_ID");
        }
        if self.namespace.is_empty() {
            m.push("STORM_NAMESPACE");
        }
        m
    }

    pub fn remaining(&self) -> Duration {
        self.timeout.saturating_sub(self.started.elapsed())
    }

    /// The run id as a DNS label: every hostname this run routes carries it,
    /// so two runs (or two suites) never claim the same name.
    pub fn slug(&self) -> String {
        let s: String = self
            .run_id
            .to_ascii_lowercase()
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
            .collect();
        let s = s.trim_matches('-');
        let s = &s[..s.len().min(40)];
        if s.is_empty() { "run".into() } else { s.trim_end_matches('-').to_string() }
    }

    /// A hostname for this run. `.invalid` never resolves (RFC 2606): the
    /// suites put it in the Host header themselves, and nothing else can be
    /// sent to it by accident.
    pub fn host(&self, what: &str) -> String {
        format!("{what}.{}.{}.stormlb-test.invalid", self.slug(), self.suite)
    }

    /// The address the router reaches this container on: the source address
    /// the kernel picks toward the router. A connected UDP socket sends
    /// nothing; `connect` only asks the routing table. The Job is
    /// `hostNetwork`, so this is the address of the node the Job runs on.
    pub fn reach_ip(&self) -> Result<IpAddr, String> {
        let to: SocketAddr = self
            .router
            .to_socket_addrs()
            .map_err(|e| format!("router address {:?}: {e}", self.router))?
            .next()
            .ok_or_else(|| format!("router address {:?} resolves to nothing", self.router))?;
        let any = if to.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" };
        let s = UdpSocket::bind(any).map_err(|e| format!("udp socket: {e}"))?;
        s.connect(to).map_err(|e| format!("no route to {to}: {e}"))?;
        Ok(s.local_addr().map_err(|e| e.to_string())?.ip())
    }
}

/// `host:port`, bracketing a bare IPv6 address.
pub fn join(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// This process's soft open-file limit, from `/proc/self/limits`. Nothing
/// assumes a number: the long suite sizes its concurrency from this.
pub fn fd_limit() -> Option<u64> {
    let l = std::fs::read_to_string("/proc/self/limits").ok()?;
    let line = l.lines().find(|l| l.starts_with("Max open files"))?;
    line.split_whitespace().nth(3)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(run_id: &str) -> Env {
        Env {
            suite: "short".into(),
            run_id: run_id.into(),
            namespace: "ns".into(),
            api: "http://127.0.0.1:1".into(),
            node: "127.0.0.1".into(),
            router: "127.0.0.1:80".into(),
            stormd: None,
            token: None,
            ca: None,
            timeout: Duration::from_secs(1),
            started: Instant::now(),
            route_wait: Duration::from_secs(1),
            settle: Duration::from_secs(1),
        }
    }

    #[test]
    fn hostnames_are_dns_safe_and_per_run() {
        assert_eq!(env("Run_42/x").host("a"), "a.run-42-x.short.stormlb-test.invalid");
        assert_eq!(env("--").slug(), "run");
        assert!(env(&"x".repeat(90)).slug().len() <= 40);
    }

    #[test]
    fn ipv6_nodes_are_bracketed() {
        assert_eq!(join("fd00::1", 80), "[fd00::1]:80");
        assert_eq!(join("10.0.0.1", 180), "10.0.0.1:180");
    }

    #[test]
    fn the_address_toward_loopback_is_loopback() {
        assert_eq!(env("r").reach_ip().unwrap().to_string(), "127.0.0.1");
    }
}
