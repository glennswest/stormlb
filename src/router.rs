//! The L7 half of inbound: a Host-header router over Gateway API HTTPRoutes.
//!
//! docs/routing.md (stormpump) names the shape: wildcard DNS carries
//! `*.storm1.<zone>` to a VIP, and something at the VIP demuxes on the HTTP
//! Host header. This is that something. It lives here because stormlb already
//! owns everything inbound — the VIP, the health checks, the L4 balancer —
//! and one component owning one request path is the debugging story the
//! alternative (VIP ours, L7 Cilium/Envoy) does not have.
//!
//! **Per-connection, not per-request.** The router reads exactly one request
//! head, picks the backend from the Host header, replays the head and then
//! splices bytes both ways. That single decision buys HTTP/1.1 keep-alive,
//! chunked bodies, websockets and SSE for free, because after the head the
//! router is a wire. What it costs: a keep-alive connection whose second
//! request names a different Host stays on the first backend. Browsers do not
//! do that to distinct names, and every backend here answers one name.
//!
//! **The table is polled, not watched.** A route change is an operator
//! action; five seconds of staleness is invisible, and a poll cannot be
//! wedged by a watch protocol misunderstanding — this apiserver is a
//! reimplementation, and the router should not be the corner that finds its
//! watch bugs. The poll keeps serving the last good table on error, because
//! a flapping apiserver must not take working routes down with it.
//!
//! Backends resolve two ways, in order:
//! - the `storm.io/backend` annotation, `host:port` verbatim. This is how a
//!   *node* service routes — `127.0.0.1:9094` means "this node's console"
//!   on every node, which keeps per-node addresses out of cluster manifests.
//! - the first rule's first backendRef, resolved to the Service's
//!   clusterIP:port. Cilium's kube-proxy replacement makes a ClusterIP
//!   reachable from the node, which is where this router stands.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug, Clone, Deserialize)]
pub struct RouterCfg {
    /// Where the router listens. The VIP's port 80, in practice.
    #[serde(default = "default_listen")]
    pub listen: String,
    /// The apiserver that holds the HTTPRoutes.
    #[serde(default = "default_apiserver")]
    pub apiserver: String,
    /// Seconds between route-table refreshes.
    #[serde(default = "default_poll")]
    pub poll_secs: u64,
    /// Accept the apiserver's certificate without a trusted CA. On today.
    /// stormcert's CA is not in any trust store yet, and a router that
    /// refuses the apiserver serves nothing at all; flip this off the day
    /// the CA is distributed.
    #[serde(default = "default_insecure")]
    pub insecure: bool,
}

/// The addresses `auto` resolves to: this node's, minus the ones that belong
/// to something else.
///
/// Loopback is skipped because a router nothing outside can reach is not a
/// router, and link-local is skipped because 169.254.169.254 is the metadata
/// service's and 169.254.0.0/16 is not an address anybody routes to us on.
fn bind_addrs(listen: &str) -> Option<Vec<String>> {
    let port = listen.rsplit(':').next()?;
    if !listen.starts_with("auto") {
        return None;
    }
    // The address the kernel would send from, asked of the routing table.
    //
    // This shelled out to `ip` first, which is not in this container — so it
    // returned None, the caller fell back to binding the literal string
    // "auto:80", and the router died on `failed to lookup address
    // information: Name does not resolve`. A fallback that binds the
    // placeholder is worse than no fallback.
    //
    // A connected UDP socket sends nothing. `connect` on a datagram socket
    // only sets the peer, and the kernel picks the source address by looking
    // up the route — so this works with no network, no DNS and no external
    // command, and gives exactly the address a client would reach this node
    // on.
    let probe = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    probe.connect("203.0.113.1:9").ok()?;
    let ip = probe.local_addr().ok()?.ip().to_string();
    if ip.starts_with("127.") || ip.starts_with("169.254.") || ip == "0.0.0.0" {
        return None;
    }
    Some(vec![format!("{ip}:{port}")])
}

fn default_listen() -> String { "0.0.0.0:80".into() }
fn default_apiserver() -> String { "https://127.0.0.1:6443".into() }
fn default_poll() -> u64 { 5 }
fn default_insecure() -> bool { true }

/// hostname -> "host:port" to dial.
type Table = Arc<RwLock<HashMap<String, String>>>;

pub async fn run(cfg: RouterCfg) -> anyhow::Result<()> {
    let table: Table = Arc::new(RwLock::new(HashMap::new()));
    // `auto` means every address except the ones that are not ours to take.
    //
    // The wildcard includes 169.254.169.254, which the instance metadata
    // service binds on port 80 — a fixed address every cloud image asks, and
    // not one this router may claim. Whichever started second got EADDRINUSE
    // and crash-looped; on this node it was the router, restart=9 and
    // climbing, with nothing saying the two were fighting over a port.
    //
    // So the router binds the node's own routable addresses rather than
    // everything. Resolved here rather than configured, because the address
    // is not known when the golden is built.
    let listener = match bind_addrs(&cfg.listen) {
        Some(addrs) => {
            let mut last = None;
            let mut bound = None;
            for a in &addrs {
                match TcpListener::bind(a).await {
                    Ok(l) => {
                        bound = Some(l);
                        break;
                    }
                    Err(e) => last = Some(e),
                }
            }
            match bound {
                Some(l) => l,
                None => return Err(last.expect("at least one address was tried").into()),
            }
        }
        // `auto` that could not be resolved falls back to the wildcard, not
        // to the literal string. Binding "auto:80" is a DNS lookup of a word,
        // and the error names resolution rather than the placeholder.
        None if cfg.listen.starts_with("auto") => {
            let port = cfg.listen.rsplit(':').next().unwrap_or("80");
            tracing::warn!(
                "could not determine this node's address; binding 0.0.0.0:{port}. \
                 If a metadata service holds 169.254.169.254:{port} this will fail."
            );
            TcpListener::bind(format!("0.0.0.0:{port}")).await?
        }
        None => TcpListener::bind(&cfg.listen).await?,
    };
    info!(listen = %cfg.listen, apiserver = %cfg.apiserver, "router up");

    tokio::spawn(poll_routes(cfg.clone(), table.clone()));

    loop {
        let (conn, peer) = listener.accept().await?;
        let table = table.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_conn(conn, &table).await {
                // One line per failed connection, not per byte: the common
                // errors here are a client that went away and a backend that
                // is down, and both are ordinary.
                tracing::debug!(%peer, "connection ended: {e}");
            }
        });
    }
}

/// Read one request head, demux on Host, splice.
async fn serve_conn(mut conn: TcpStream, table: &Table) -> anyhow::Result<()> {
    // 16 KiB is far past any sane request head; a head that big without a
    // blank line is not HTTP and gets cut off rather than buffered forever.
    let mut head = Vec::with_capacity(1024);
    let mut buf = [0u8; 2048];
    let end = loop {
        let n = conn.read(&mut buf).await?;
        if n == 0 {
            return Ok(()); // closed before a full head — nothing to do
        }
        head.extend_from_slice(&buf[..n]);
        if let Some(pos) = find_head_end(&head) {
            break pos;
        }
        if head.len() > 16 * 1024 {
            anyhow::bail!("request head exceeded 16KiB without terminating");
        }
    };

    let host = match host_of(&head[..end]) {
        Some(h) => h,
        None => {
            let _ = conn
                .write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 26\r\nconnection: close\r\n\r\nthis router needs a Host\n\n")
                .await;
            return Ok(());
        }
    };

    let backend = { table.read().await.get(&host).cloned() };
    // The router's own liveness, on any host no route claims: stormd probes
    // http://127.0.0.1/healthz, and 127.0.0.1 is never a route's hostname.
    // A host a route *does* claim proxies /healthz to its backend untouched.
    if backend.is_none() && request_path(&head[..end]) == Some("/healthz") {
        let body = "router alive
";
        let resp = format!(
            "HTTP/1.1 200 OK
content-type: text/plain
content-length: {}
connection: close

{body}",
            body.len()
        );
        let _ = conn.write_all(resp.as_bytes()).await;
        return Ok(());
    }
    let Some(backend) = backend else {
        // Name the host: "404 from the router" and "404 from the app" look
        // identical from a browser, and the debugging path for each is
        // different. The body says which one this is and for what name.
        let body = format!("no route for host {host}\n");
        let resp = format!(
            "HTTP/1.1 404 Not Found\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = conn.write_all(resp.as_bytes()).await;
        return Ok(());
    };

    let mut upstream = TcpStream::connect(&backend).await.map_err(|e| {
        anyhow::anyhow!("backend {backend} for {host}: {e}")
    })?;
    upstream.write_all(&head).await?;
    tokio::io::copy_bidirectional(&mut conn, &mut upstream).await?;
    Ok(())
}

/// The request path, from the request line.
fn request_path(head: &[u8]) -> Option<&str> {
    let line = head.split(|&b| b == b'\n').next()?;
    let line = std::str::from_utf8(line).ok()?;
    let path = line.split_whitespace().nth(1)?;
    Some(path.split('?').next().unwrap_or(path))
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

/// The Host header's value, lowercased, port stripped.
fn host_of(head: &[u8]) -> Option<String> {
    for line in head.split(|&b| b == b'\n').skip(1) {
        let line = std::str::from_utf8(line).ok()?.trim();
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            if k.trim().eq_ignore_ascii_case("host") {
                let v = v.trim().to_ascii_lowercase();
                let v = v.rsplit_once(':').map(|(h, p)| {
                    if p.chars().all(|c| c.is_ascii_digit()) { h.to_string() } else { v.clone() }
                }).unwrap_or(v);
                return Some(v);
            }
        }
    }
    None
}

/// Refresh the table forever. Serves the last good table across errors.
async fn poll_routes(cfg: RouterCfg, table: Table) {
    let client = match reqwest::Client::builder()
        .danger_accept_invalid_certs(cfg.insecure)
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("router cannot build an http client: {e}");
            return;
        }
    };
    let mut last_len = usize::MAX;
    loop {
        match fetch_table(&client, &cfg.apiserver).await {
            Ok(new) => {
                if new.len() != last_len {
                    info!(routes = new.len(), "route table refreshed");
                    last_len = new.len();
                }
                *table.write().await = new;
            }
            Err(e) => warn!("route refresh failed, keeping the last table: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(cfg.poll_secs.max(1))).await;
    }
}

async fn fetch_table(
    client: &reqwest::Client,
    api: &str,
) -> anyhow::Result<HashMap<String, String>> {
    let url = format!("{api}/apis/gateway.networking.k8s.io/v1/httproutes");
    let list: serde_json::Value = client.get(&url).send().await?.error_for_status()?.json().await?;
    let mut out = HashMap::new();
    for r in list["items"].as_array().into_iter().flatten() {
        let ns = r["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = r["metadata"]["name"].as_str().unwrap_or("");
        let backend = match backend_of(client, api, ns, r).await {
            Ok(b) => b,
            Err(e) => {
                warn!(route = %format!("{ns}/{name}"), "no usable backend: {e}");
                continue;
            }
        };
        for h in r["spec"]["hostnames"].as_array().into_iter().flatten() {
            if let Some(h) = h.as_str() {
                out.insert(h.to_ascii_lowercase(), backend.clone());
            }
        }
    }
    Ok(out)
}

async fn backend_of(
    client: &reqwest::Client,
    api: &str,
    ns: &str,
    route: &serde_json::Value,
) -> anyhow::Result<String> {
    if let Some(b) = route["metadata"]["annotations"]["storm.io/backend"].as_str() {
        return Ok(b.to_string());
    }
    let bref = &route["spec"]["rules"][0]["backendRefs"][0];
    let svc = bref["name"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("no storm.io/backend annotation and no backendRef"))?;
    let port = bref["port"].as_u64().unwrap_or(80);
    let sns = bref["namespace"].as_str().unwrap_or(ns);
    let url = format!("{api}/api/v1/namespaces/{sns}/services/{svc}");
    let s: serde_json::Value = client.get(&url).send().await?.error_for_status()?.json().await?;
    let ip = s["spec"]["clusterIP"]
        .as_str()
        .filter(|ip| !ip.is_empty() && *ip != "None")
        .ok_or_else(|| anyhow::anyhow!("service {sns}/{svc} has no clusterIP"))?;
    Ok(format!("{ip}:{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_host_header_is_found_case_insensitively_and_port_stripped() {
        let head = b"GET / HTTP/1.1\r\nUser-Agent: x\r\nHOST: Console.Storm1.G8.lo:80\r\n\r\n";
        assert_eq!(host_of(head).as_deref(), Some("console.storm1.g8.lo"));
    }

    #[test]
    fn a_missing_host_is_none_not_a_panic() {
        assert_eq!(host_of(b"GET / HTTP/1.1\r\nAccept: */*\r\n\r\n"), None);
    }

    #[test]
    fn the_healthz_path_is_read_from_the_request_line() {
        assert_eq!(request_path(b"GET /healthz HTTP/1.1
Host: x

"), Some("/healthz"));
        assert_eq!(request_path(b"GET /healthz?v=1 HTTP/1.1

"), Some("/healthz"));
    }

    #[test]
    fn the_head_end_is_the_blank_line() {
        assert_eq!(find_head_end(b"GET / HTTP/1.1\r\n\r\nbody"), Some(18));
        assert_eq!(find_head_end(b"GET / HTTP/1.1\r\n"), None);
    }
}
