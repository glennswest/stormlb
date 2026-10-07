//! Prometheus metrics (`[metrics]`, #12): what the router and the VIPs do,
//! in the text exposition format (0.0.4) on `GET /metrics`.
//!
//! Hand-rolled — counters, gauges and fixed-bucket histograms in one process
//! registry ([`global`]) — rather than a client crate, because the set is
//! small and the format is a page of text. Names follow the router/LB
//! convention (`*_requests_total{host,code}`, `*_duration_seconds`
//! histograms, `*_connections_active`).
//!
//! Label cardinality is bounded by configuration: a router `host` label is a
//! route's hostname or `unrouted`, never a client's arbitrary Host header.

use crate::vips::Registry;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

/// `[metrics]`.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsCfg {
    /// Where `/metrics` is served; `auto:<port>` binds the node's address and
    /// loopback, as for `[router] listen`. Plain HTTP, read-only.
    #[serde(default = "default_listen")]
    pub listen: String,
}
fn default_listen() -> String {
    "auto:9104".into()
}

/// Histogram buckets (seconds).
const BUCKETS: [f64; 11] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// Every metric this process can export: (name, type, help).
const CATALOG: &[(&str, &str, &str)] = &[
    ("stormlb_build_info", "gauge", "Always 1; the version is the label."),
    ("stormlb_router_connections_total", "counter", "Connections accepted by the router, by listener (http, https)."),
    ("stormlb_router_connections_active", "gauge", "Router connections open now, by listener."),
    ("stormlb_router_requests_total", "counter", "Requests by route host (or unrouted) and status code: the first request of each connection, which is what the router routes. code=error: the backend gave no response."),
    ("stormlb_router_request_duration_seconds", "histogram", "From the request being sent to the backend to its first response byte, by route host."),
    ("stormlb_router_upstream_errors_total", "counter", "Backend failures by route host and kind (connect, no_response)."),
    ("stormlb_router_tls_handshake_errors_total", "counter", "TLS handshakes that failed or timed out."),
    ("stormlb_router_routes", "gauge", "Hostnames in the route table."),
    ("stormlb_router_route_refreshes_total", "counter", "Route-table refreshes from the apiserver, by result (ok, error)."),
    ("stormlb_router_tls_certificates_loaded", "gauge", "Certificate/key pairs loaded for the TLS listener."),
    ("stormlb_router_tls_reloads_total", "counter", "Certificate pairs (re)loaded from changed files, by result (ok, error)."),
    ("stormlb_vip_connections_total", "counter", "Connections accepted on a VIP's L4 listener."),
    ("stormlb_vip_connections_active", "gauge", "Connections open through a VIP now."),
    ("stormlb_vip_no_healthy_backend_total", "counter", "Connections dropped because a VIP had no healthy backend."),
    ("stormlb_vip_upstream_connect_errors_total", "counter", "Connections dropped because the chosen backend refused or failed."),
    ("stormlb_vip_backend_healthy", "gauge", "1 if a VIP's backend passes its health check, else 0."),
    ("stormlb_vip_vrrp_master", "gauge", "1 while this node is the VRRP Master for the VIP, else 0 (absent without VRRP)."),
];

#[derive(Default, Clone)]
struct Hist {
    buckets: [u64; BUCKETS.len()],
    sum: f64,
    count: u64,
}

type Key = (&'static str, String);

/// The process's metrics.
#[derive(Default)]
pub struct Metrics {
    counters: Mutex<BTreeMap<Key, u64>>,
    gauges: Mutex<BTreeMap<Key, i64>>,
    hists: Mutex<BTreeMap<Key, Hist>>,
}

/// The one registry every part of the process records into.
pub fn global() -> &'static Metrics {
    static M: OnceLock<Metrics> = OnceLock::new();
    M.get_or_init(Metrics::default)
}

/// `{a="x",b="y"}` with values escaped, or "" for no labels.
pub fn labels(pairs: &[(&str, &str)]) -> String {
    if pairs.is_empty() {
        return String::new();
    }
    let mut s = String::from("{");
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(k);
        s.push_str("=\"");
        for c in v.chars() {
            match c {
                '\\' => s.push_str("\\\\"),
                '"' => s.push_str("\\\""),
                '\n' => s.push_str("\\n"),
                c => s.push(c),
            }
        }
        s.push('"');
    }
    s.push('}');
    s
}

impl Metrics {
    pub fn inc(&self, name: &'static str, l: &[(&str, &str)]) {
        *self.counters.lock().unwrap().entry((name, labels(l))).or_default() += 1;
    }

    pub fn gauge_add(&self, name: &'static str, l: &[(&str, &str)], d: i64) {
        *self.gauges.lock().unwrap().entry((name, labels(l))).or_default() += d;
    }

    pub fn gauge_set(&self, name: &'static str, l: &[(&str, &str)], v: i64) {
        self.gauges.lock().unwrap().insert((name, labels(l)), v);
    }

    pub fn observe(&self, name: &'static str, l: &[(&str, &str)], d: Duration) {
        let secs = d.as_secs_f64();
        let mut hists = self.hists.lock().unwrap();
        let h = hists.entry((name, labels(l))).or_default();
        for (i, b) in BUCKETS.iter().enumerate() {
            if secs <= *b {
                h.buckets[i] += 1;
            }
        }
        h.sum += secs;
        h.count += 1;
    }

    /// A gauge that counts while the guard lives (an open connection).
    pub fn active(&'static self, name: &'static str, l: &[(&str, &str)]) -> ActiveGuard {
        let key = labels(l);
        *self.gauges.lock().unwrap().entry((name, key.clone())).or_default() += 1;
        ActiveGuard { m: self, name, key }
    }

    /// The text exposition: every recorded series, plus `extra` series
    /// (name, labels, value) computed at scrape time.
    pub fn render(&self, extra: &[(&'static str, String, i64)]) -> String {
        let counters = self.counters.lock().unwrap().clone();
        let mut gauges = self.gauges.lock().unwrap().clone();
        for (n, l, v) in extra {
            gauges.insert((n, l.clone()), *v);
        }
        let hists = self.hists.lock().unwrap().clone();
        let mut out = String::new();
        for (name, ty, help) in CATALOG {
            let mut body = String::new();
            match *ty {
                "counter" => {
                    for ((n, l), v) in counters.iter().filter(|((n, _), _)| n == name) {
                        let _ = writeln!(body, "{n}{l} {v}");
                    }
                }
                "gauge" => {
                    for ((n, l), v) in gauges.iter().filter(|((n, _), _)| n == name) {
                        let _ = writeln!(body, "{n}{l} {v}");
                    }
                }
                _ => {
                    for ((n, l), h) in hists.iter().filter(|((n, _), _)| n == name) {
                        // `le` joins the series' own labels.
                        let with = |le: &str| {
                            if l.is_empty() {
                                format!("{{le=\"{le}\"}}")
                            } else {
                                format!("{},le=\"{le}\"}}", &l[..l.len() - 1])
                            }
                        };
                        for (i, b) in BUCKETS.iter().enumerate() {
                            let _ = writeln!(body, "{n}_bucket{} {}", with(&b.to_string()), h.buckets[i]);
                        }
                        let _ = writeln!(body, "{n}_bucket{} {}", with("+Inf"), h.count);
                        let _ = writeln!(body, "{n}_sum{l} {}", h.sum);
                        let _ = writeln!(body, "{n}_count{l} {}", h.count);
                    }
                }
            }
            if !body.is_empty() {
                let _ = writeln!(out, "# HELP {name} {help}");
                let _ = writeln!(out, "# TYPE {name} {ty}");
                out.push_str(&body);
            }
        }
        out
    }
}

/// Decrements its gauge when dropped.
pub struct ActiveGuard {
    m: &'static Metrics,
    name: &'static str,
    key: String,
}

impl Drop for ActiveGuard {
    fn drop(&mut self) {
        if let Some(v) = self.m.gauges.lock().unwrap().get_mut(&(self.name, std::mem::take(&mut self.key))) {
            *v -= 1;
        }
    }
}

/// The series computed at scrape time: build info, and each VIP's backend
/// health and VRRP state.
pub fn scrape_series(reg: Option<&Registry>) -> Vec<(&'static str, String, i64)> {
    let mut out = vec![("stormlb_build_info", labels(&[("version", env!("CARGO_PKG_VERSION"))]), 1)];
    for v in reg.map(|r| r.list()).unwrap_or_default() {
        for b in &v.status.backends {
            let be = format!("{}:{}", b.address, b.port);
            out.push(("stormlb_vip_backend_healthy", labels(&[("vip", &v.name), ("backend", &be)]), b.healthy as i64));
        }
        if let Some(s) = v.status.vrrp {
            out.push(("stormlb_vip_vrrp_master", labels(&[("vip", &v.name)]), (s == "master") as i64));
        }
    }
    out
}

/// Serve `/metrics` (and `/healthz`) on `cfg.listen` forever.
pub async fn run(cfg: MetricsCfg, reg: Option<Arc<Registry>>) -> anyhow::Result<()> {
    let ls = crate::router::bind_listeners(&cfg.listen).await?;
    for l in &ls {
        info!("metrics on http://{}/metrics", l.local_addr()?);
    }
    let mut tasks = Vec::new();
    for l in ls {
        tasks.push(tokio::spawn(serve(l, reg.clone())));
    }
    for t in tasks {
        let _ = t.await;
    }
    Ok(())
}

/// Serve on one bound listener.
pub async fn serve(l: TcpListener, reg: Option<Arc<Registry>>) {
    loop {
        let (mut c, peer): (_, SocketAddr) = match l.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("metrics accept: {e}");
                continue;
            }
        };
        let reg = reg.clone();
        tokio::spawn(async move {
            let mut head = Vec::new();
            let mut buf = [0u8; 1024];
            let read = tokio::time::timeout(Duration::from_secs(10), async {
                while !head.windows(4).any(|w| w == b"\r\n\r\n") && head.len() < 8192 {
                    match c.read(&mut buf).await {
                        Ok(0) | Err(_) => return false,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                true
            })
            .await;
            if read != Ok(true) {
                return tracing::debug!(%peer, "metrics: no request head");
            }
            let line = head.split(|&b| b == b'\r').next().unwrap_or(&[]);
            let line = String::from_utf8_lossy(line);
            let mut parts = line.split(' ');
            let (method, path) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
            let path = path.split('?').next().unwrap_or("");
            let (status, ctype, body) = match (method, path) {
                ("GET", "/metrics") => {
                    ("200 OK", "text/plain; version=0.0.4; charset=utf-8", global().render(&scrape_series(reg.as_deref())))
                }
                ("GET", "/healthz") => ("200 OK", "text/plain", "ok\n".to_string()),
                ("GET", _) => ("404 Not Found", "text/plain", "metrics are at /metrics\n".to_string()),
                _ => ("405 Method Not Allowed", "text/plain", "GET only\n".to_string()),
            };
            let resp = format!("HTTP/1.1 {status}\r\ncontent-type: {ctype}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
            let _ = c.write_all(resp.as_bytes()).await;
            let _ = c.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_exposition_has_help_type_and_escaped_labels() {
        let m = Metrics::default();
        m.inc("stormlb_router_requests_total", &[("host", "a.test"), ("code", "200")]);
        m.inc("stormlb_router_requests_total", &[("host", "a.test"), ("code", "200")]);
        m.inc("stormlb_router_requests_total", &[("host", "we\"ird\\"), ("code", "404")]);
        m.observe("stormlb_router_request_duration_seconds", &[("host", "a.test")], Duration::from_millis(30));
        let g = Box::leak(Box::new(Metrics::default()));
        let guard = g.active("stormlb_router_connections_active", &[("listener", "http")]);
        assert!(g.render(&[]).contains("stormlb_router_connections_active{listener=\"http\"} 1\n"));
        drop(guard);
        assert!(g.render(&[]).contains("stormlb_router_connections_active{listener=\"http\"} 0\n"));

        let t = m.render(&[("stormlb_build_info", labels(&[("version", "9.9.9")]), 1)]);
        assert!(t.contains("# TYPE stormlb_router_requests_total counter\n"), "{t}");
        assert!(t.contains("stormlb_router_requests_total{host=\"a.test\",code=\"200\"} 2\n"), "{t}");
        assert!(t.contains("stormlb_router_requests_total{host=\"we\\\"ird\\\\\",code=\"404\"} 1\n"), "{t}");
        assert!(t.contains("stormlb_build_info{version=\"9.9.9\"} 1\n"), "{t}");
        // 30 ms: not in the 0.025 bucket, in 0.05 and above, and +Inf.
        assert!(t.contains("stormlb_router_request_duration_seconds_bucket{host=\"a.test\",le=\"0.025\"} 0\n"), "{t}");
        assert!(t.contains("stormlb_router_request_duration_seconds_bucket{host=\"a.test\",le=\"0.05\"} 1\n"), "{t}");
        assert!(t.contains("stormlb_router_request_duration_seconds_bucket{host=\"a.test\",le=\"+Inf\"} 1\n"), "{t}");
        assert!(t.contains("stormlb_router_request_duration_seconds_count{host=\"a.test\"} 1\n"), "{t}");
        // Nothing recorded, nothing printed (no empty HELP blocks).
        assert!(!t.contains("stormlb_vip_connections_total"), "{t}");
        // Every sample line is `name{labels} value`.
        for line in t.lines().filter(|l| !l.starts_with('#')) {
            let (series, value) = line.rsplit_once(' ').unwrap();
            assert!(value.parse::<f64>().is_ok(), "{line}");
            assert!(series.starts_with("stormlb_"), "{line}");
        }
    }
}
