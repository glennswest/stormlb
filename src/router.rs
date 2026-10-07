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
//! **TLS** (`[router.tls]`, #14): a second listener terminates TLS with the
//! configured certificates (picked by SNI, [`crate::tls`]) and feeds the same
//! demux, adding `X-Forwarded-Proto: https`. Once a certificate is loaded,
//! plain HTTP answers with a 308 to https, except `/healthz`. The upstream
//! connection is plain either way (TLS to backends is #13).
//!
//! **TLS to backends** (#13): a route with `storm.io/backend-protocol: https`
//! is dialled over TLS and verified against `[router] backend_ca_file` only,
//! for the backend's IP (or `storm.io/backend-server-name`). Without a CA the
//! connection fails closed; nothing is sent to a backend it can't verify.
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
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{info, warn};

use crate::metrics::global as metrics;
use crate::tls::{self, TlsCfg};

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
    /// Without `ca_file`: accept the apiserver's certificate unverified (the
    /// default, as before `ca_file` existed). With `ca_file` it is ignored.
    #[serde(default = "default_insecure")]
    pub insecure: bool,
    /// The CA (PEM) the apiserver's certificate must chain to — on a node,
    /// `/data/stormcert/ca.crt`. Only it is trusted (stormlb#10). Re-read when
    /// it changes.
    #[serde(default)]
    pub ca_file: Option<String>,
    /// The router's identity (stormlb#9): a file holding a bearer token — its
    /// ServiceAccount's, which stormcos mints with get/list/watch on
    /// HTTPRoutes and Services. Re-read on every poll, so it can be rotated in
    /// place. Unset: anonymous, which only an apiserver with
    /// `--dev-anonymous-admin` lets list routes.
    #[serde(default)]
    pub token_file: Option<String>,
    /// `[router.tls]`: terminate TLS too. Absent: plain HTTP only.
    #[serde(default)]
    pub tls: Option<TlsCfg>,
    /// The CA (PEM) a backend marked `storm.io/backend-protocol: https` must
    /// chain to — on a node, the cluster CA `/data/stormcert/ca.crt`. Only
    /// these certificates are trusted. Re-read on the route poll when it
    /// changes. Unset: an https backend's connections fail closed.
    #[serde(default)]
    pub backend_ca_file: Option<String>,
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
    // Loopback as well as the node's address.
    //
    // The health probe dials 127.0.0.1, and a router that only binds its
    // routable address is not there — so it bound correctly, served nothing
    // to the probe, and was restarted every fifteen seconds for failing a
    // check that was looking in the wrong place. Loopback does not conflict
    // with the metadata service's 169.254.169.254, which is the only address
    // this was ever avoiding.
    Some(vec![format!("{ip}:{port}"), format!("127.0.0.1:{port}")])
}

fn default_listen() -> String { "0.0.0.0:80".into() }
fn default_apiserver() -> String { "https://127.0.0.1:6443".into() }
fn default_poll() -> u64 { 5 }
fn default_insecure() -> bool { true }

/// A route's backend: where to dial, and for TLS, the name its certificate
/// must carry (an IP literal means an IP SAN; it is also the SNI when a name).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Backend {
    addr: String,
    tls: Option<String>,
}

/// hostname -> its backend.
type Table = Arc<RwLock<HashMap<String, Backend>>>;

/// The connector for TLS backends, built from `backend_ca_file`; None until
/// the file loads.
type Upstream = Arc<std::sync::RwLock<Option<TlsConnector>>>;

pub async fn run(cfg: RouterCfg) -> anyhow::Result<()> {
    let table: Table = Arc::new(RwLock::new(HashMap::new()));
    let mut plain = bind_listeners(&cfg.listen).await?;
    info!(listen = %cfg.listen, apiserver = %cfg.apiserver, "router up");

    let upstream: Upstream = Arc::default();
    tokio::spawn(poll_routes(cfg.clone(), table.clone(), upstream.clone()));

    let mut redirect = None;
    if let Some(t) = &cfg.tls {
        let certs = tls::Certs::new(t.certs.clone());
        if t.certs.is_empty() {
            warn!("[router.tls] lists no certs: the TLS listener completes no handshake");
        } else if certs.count() == 0 {
            warn!("[router.tls]: no certificate loads yet; checking again every {} s", t.reload_secs.max(1));
        }
        tokio::spawn(certs.clone().reload_every(t.reload_secs));
        // A TLS port that can't be bound is logged, not fatal: crash-looping
        // would take the plain listener (and the health probe) down with it.
        match bind_listeners(&t.listen).await {
            Ok(ls) => {
                let acceptor = TlsAcceptor::from(tls::server_config(certs.clone()));
                let front = Front { table: table.clone(), upstream: upstream.clone(), redirect: None };
                for l in ls {
                    tokio::spawn(serve_tls_on(l, front.clone(), acceptor.clone()));
                }
                info!(listen = %t.listen, loaded = certs.count(), "router TLS up");
                if t.redirect {
                    let port = t.listen.rsplit(':').next().and_then(|p| p.parse().ok()).unwrap_or(443);
                    redirect = Some(Arc::new(Redirect { certs, port }));
                }
            }
            Err(e) => warn!("[router.tls] listen {}: {e:#} — serving plain HTTP only", t.listen),
        }
    }

    let front = Front { table, upstream, redirect };
    let first = plain.remove(0);
    // Everything after the first is served by its own task over the same
    // routes.
    for extra in plain {
        tokio::spawn(serve_on(extra, front.clone()));
    }
    serve_on(first, front).await
}

/// Bind `listen`: `auto:<port>` or `ip:port`.
pub(crate) async fn bind_listeners(listen: &str) -> anyhow::Result<Vec<TcpListener>> {
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
    match bind_addrs(listen) {
        Some(addrs) => {
            // A router on its routable address and not on loopback passes no
            // health check, and one on loopback alone serves nobody — it
            // needs both.
            let mut listeners = Vec::new();
            let mut last = None;
            for a in &addrs {
                match TcpListener::bind(a).await {
                    Ok(l) => listeners.push(l),
                    Err(e) => last = Some(e),
                }
            }
            if listeners.is_empty() {
                return Err(last.expect("at least one address was tried").into());
            }
            Ok(listeners)
        }
        // `auto` that could not be resolved falls back to the wildcard, not
        // to the literal string. Binding "auto:80" is a DNS lookup of a word,
        // and the error names resolution rather than the placeholder.
        None if listen.starts_with("auto") => {
            let port = listen.rsplit(':').next().unwrap_or("80");
            tracing::warn!(
                "could not determine this node's address; binding 0.0.0.0:{port}. \
                 If a metadata service holds 169.254.169.254:{port} this will fail."
            );
            Ok(vec![TcpListener::bind(format!("0.0.0.0:{port}")).await?])
        }
        None => Ok(vec![TcpListener::bind(listen).await?]),
    }
}

/// What a listener serves: the routes, and on plain HTTP, whether to send
/// clients to https.
#[derive(Clone)]
struct Front {
    table: Table,
    upstream: Upstream,
    redirect: Option<Arc<Redirect>>,
}

impl Front {
    /// Plain HTTP, no redirect.
    #[cfg(test)]
    fn plain(table: Table) -> Front {
        Front { table, upstream: Arc::default(), redirect: None }
    }
}

/// Redirect plain HTTP to https on `port` — while a certificate is loaded.
struct Redirect {
    certs: Arc<tls::Certs>,
    port: u16,
}

/// Accept TLS, then demux.
async fn serve_tls_on(listener: TcpListener, front: Front, acceptor: TlsAcceptor) {
    loop {
        let (conn, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("router TLS accept: {e}");
                continue;
            }
        };
        let (front, acceptor) = (front.clone(), acceptor.clone());
        tokio::spawn(async move {
            metrics().inc("stormlb_router_connections_total", &[("listener", "https")]);
            let _active = metrics().active("stormlb_router_connections_active", &[("listener", "https")]);
            let tls = match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(conn)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => {
                    metrics().inc("stormlb_router_tls_handshake_errors_total", &[]);
                    return tracing::debug!(%peer, "TLS handshake failed: {e}");
                }
                Err(_) => {
                    metrics().inc("stormlb_router_tls_handshake_errors_total", &[]);
                    return tracing::debug!(%peer, "TLS handshake timed out");
                }
            };
            if let Err(e) = serve_conn(tls, &front, true).await {
                tracing::debug!(%peer, "connection ended: {e}");
            }
        });
    }
}

/// Accept and demux, on one listener.
///
/// Factored out because the router listens on more than one address: its own,
/// which clients reach, and loopback, which the health probe dials. One
/// routing table, several front doors.
async fn serve_on(listener: TcpListener, front: Front) -> anyhow::Result<()> {
    loop {
        let (conn, peer) = listener.accept().await?;
        let front = front.clone();
        tokio::spawn(async move {
            metrics().inc("stormlb_router_connections_total", &[("listener", "http")]);
            let _active = metrics().active("stormlb_router_connections_active", &[("listener", "http")]);
            if let Err(e) = serve_conn(conn, &front, false).await {
                // One line per failed connection, not per byte: the common
                // errors here are a client that went away and a backend that
                // is down, and both are ordinary.
                tracing::debug!(%peer, "connection ended: {e}");
            }
        });
    }
}

/// Read one request head, demux on Host, splice. `tls`: the client came in
/// over TLS (the head gets `X-Forwarded-Proto: https`).
async fn serve_conn<S: AsyncRead + AsyncWrite + Unpin>(mut conn: S, front: &Front, tls: bool) -> anyhow::Result<()> {
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
            metrics().inc("stormlb_router_requests_total", &[("host", "unrouted"), ("code", "400")]);
            let _ = conn
                .write_all(b"HTTP/1.1 400 Bad Request\r\ncontent-length: 26\r\nconnection: close\r\n\r\nthis router needs a Host\n\n")
                .await;
            return Ok(());
        }
    };

    let backend = { front.table.read().await.get(&host).cloned() };
    // The metrics' host label: a route's hostname, never a client's arbitrary
    // Host header (that would be unbounded cardinality).
    let label = if backend.is_some() { host.as_str() } else { "unrouted" };

    // Plain HTTP goes to https once there is a certificate to serve it with,
    // except the health probe, which stays on plain HTTP.
    if let Some(r) = front.redirect.as_ref().filter(|r| !tls && r.certs.count() > 0) {
        if request_path(&head[..end]) != Some("/healthz") {
            let port = if r.port == 443 { String::new() } else { format!(":{}", r.port) };
            let target = request_target(&head[..end]).unwrap_or("/");
            let resp = format!(
                "HTTP/1.1 308 Permanent Redirect\r\nlocation: https://{host}{port}{target}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            );
            metrics().inc("stormlb_router_requests_total", &[("host", label), ("code", "308")]);
            let _ = conn.write_all(resp.as_bytes()).await;
            return Ok(());
        }
    }

    // The router's own liveness, on any host no route claims: stormd probes
    // http://127.0.0.1/healthz, and 127.0.0.1 is never a route's hostname.
    // A host a route *does* claim proxies /healthz to its backend untouched.
    if backend.is_none() && request_path(&head[..end]) == Some("/healthz") {
        // Explicit CRLF, like the 400 and 404: this literal once spanned
        // source lines and sent bare LF, which only lenient clients accept.
        let body = "router alive\n";
        let resp = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        metrics().inc("stormlb_router_requests_total", &[("host", "unrouted"), ("code", "200")]);
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
        metrics().inc("stormlb_router_requests_total", &[("host", "unrouted"), ("code", "404")]);
        let _ = conn.write_all(resp.as_bytes()).await;
        return Ok(());
    };

    let fail = |kind: &str| {
        metrics().inc("stormlb_router_upstream_errors_total", &[("host", &host), ("kind", kind)]);
        metrics().inc("stormlb_router_requests_total", &[("host", &host), ("code", "error")]);
    };
    let tcp = match TcpStream::connect(&backend.addr).await {
        Ok(u) => u,
        Err(e) => {
            fail("connect");
            anyhow::bail!("backend {} for {host}: {e}", backend.addr);
        }
    };
    let head = if tls { forwarded_https(&head, end) } else { head };
    let Some(name) = &backend.tls else {
        return forward(conn, tcp, &head, &host).await;
    };
    // TLS to the backend, verified against the CA, or nothing at all.
    let Some(connector) = front.upstream.read().unwrap().clone() else {
        fail("tls");
        anyhow::bail!("backend {} for {host} is https, but no [router] backend_ca_file is loaded", backend.addr);
    };
    let server = tokio_rustls::rustls::pki_types::ServerName::try_from(name.clone())
        .map_err(|e| anyhow::anyhow!("backend name {name:?} for {host}: {e}"))?;
    match tokio::time::timeout(Duration::from_secs(10), connector.connect(server, tcp)).await {
        Ok(Ok(s)) => forward(conn, s, &head, &host).await,
        Ok(Err(e)) => {
            fail("tls");
            anyhow::bail!("TLS to backend {} ({name}) for {host}: {e}", backend.addr)
        }
        Err(_) => {
            fail("tls");
            anyhow::bail!("TLS to backend {} ({name}) for {host}: handshake timed out", backend.addr)
        }
    }
}

/// Send the head to the backend, then splice.
async fn forward<S, U>(conn: S, mut upstream: U, head: &[u8], host: &str) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    upstream.write_all(head).await?;
    let sent = std::time::Instant::now();
    splice(conn, upstream, host, sent).await
}

/// Copy both ways until both directions end, as `copy_bidirectional` does,
/// but read the backend's first bytes on the way: its status code and the
/// time to them are the request's metrics. The client→backend half runs
/// concurrently throughout, so a request body is never held back waiting for
/// a response that waits for it.
async fn splice<S, U>(conn: S, upstream: U, host: &str, sent: std::time::Instant) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    U: AsyncRead + AsyncWrite + Unpin,
{
    let (mut cr, mut cw) = tokio::io::split(conn);
    let (mut ur, mut uw) = tokio::io::split(upstream);
    let up = async {
        let r = tokio::io::copy(&mut cr, &mut uw).await;
        let _ = uw.shutdown().await;
        r
    };
    let down = async {
        let mut buf = vec![0u8; 16 * 1024];
        let n = match ur.read(&mut buf).await {
            Ok(n) => n,
            Err(e) => {
                record_first(host, None, sent);
                return Err(e);
            }
        };
        record_first(host, (n > 0).then(|| &buf[..n]), sent);
        if n > 0 {
            cw.write_all(&buf[..n]).await?;
            tokio::io::copy(&mut ur, &mut cw).await?;
        }
        let _ = cw.shutdown().await;
        Ok::<_, std::io::Error>(())
    };
    let (u, d) = tokio::join!(up, down);
    d?;
    u?;
    Ok(())
}

/// Record a proxied request: the status of the backend's first bytes, or an
/// upstream error when it sent none.
fn record_first(host: &str, first: Option<&[u8]>, sent: std::time::Instant) {
    match first {
        Some(b) => {
            metrics().observe("stormlb_router_request_duration_seconds", &[("host", host)], sent.elapsed());
            let code = status_code(b).unwrap_or("other");
            metrics().inc("stormlb_router_requests_total", &[("host", host), ("code", code)]);
        }
        None => {
            metrics().inc("stormlb_router_upstream_errors_total", &[("host", host), ("kind", "no_response")]);
            metrics().inc("stormlb_router_requests_total", &[("host", host), ("code", "error")]);
        }
    }
}

/// `"200"` from `HTTP/1.1 200 OK…`.
fn status_code(b: &[u8]) -> Option<&str> {
    let s = std::str::from_utf8(b.get(..12)?).ok()?;
    let code = s.strip_prefix("HTTP/1.")?.get(2..5)?;
    code.bytes().all(|c| c.is_ascii_digit()).then_some(code)
}

/// The request path, from the request line.
fn request_path(head: &[u8]) -> Option<&str> {
    let line = head.split(|&b| b == b'\n').next()?;
    let line = std::str::from_utf8(line).ok()?;
    let path = line.split_whitespace().nth(1)?;
    Some(path.split('?').next().unwrap_or(path))
}

/// The request target (path and query), from the request line.
fn request_target(head: &[u8]) -> Option<&str> {
    let line = head.split(|&b| b == b'\n').next()?;
    std::str::from_utf8(line).ok()?.split_whitespace().nth(1).filter(|t| t.starts_with('/'))
}

/// The head with `X-Forwarded-Proto: https` after the request line, and any
/// the client sent dropped (it must not claim a scheme it did not use), plus
/// whatever followed the head (`head[end..]`) unchanged.
fn forwarded_https(head: &[u8], end: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(head.len() + 32);
    let mut lines = head[..end].split_inclusive(|&b| b == b'\n');
    if let Some(first) = lines.next() {
        out.extend_from_slice(first);
    }
    out.extend_from_slice(b"X-Forwarded-Proto: https\r\n");
    for l in lines {
        let name = l.split(|&b| b == b':').next().unwrap_or(&[]);
        if name.eq_ignore_ascii_case(b"x-forwarded-proto") {
            continue;
        }
        out.extend_from_slice(l);
    }
    out.extend_from_slice(&head[end..]);
    out
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
/// Each turn also re-reads `backend_ca_file` if it changed.
async fn poll_routes(cfg: RouterCfg, table: Table, upstream: Upstream) {
    let mut ca = CaState::default();
    let mut api = ApiClient::default();
    let mut said = None::<String>;
    let mut last_len = usize::MAX;
    loop {
        if let Some(path) = &cfg.backend_ca_file {
            if let Some(c) = ca.reload(path) {
                *upstream.write().unwrap() = Some(c);
            }
        }
        let fetched = match (api.client(&cfg), read_token(&cfg)) {
            (Ok(client), Ok(token)) => fetch_table(&client, &cfg.apiserver, token.as_deref()).await,
            (Err(e), _) | (_, Err(e)) => Err(e),
        };
        match fetched {
            Ok(new) => {
                metrics().inc("stormlb_router_route_refreshes_total", &[("result", "ok")]);
                metrics().gauge_set("stormlb_router_routes", &[], new.len() as i64);
                if new.len() != last_len {
                    info!(routes = new.len(), "route table refreshed");
                    last_len = new.len();
                }
                *table.write().await = new;
                said = None;
            }
            Err(e) => {
                metrics().inc("stormlb_router_route_refreshes_total", &[("result", "error")]);
                // Once per distinct error, not every poll.
                let msg = format!("{e:#}");
                if said.as_deref() != Some(msg.as_str()) {
                    warn!("route refresh failed, keeping the last table: {msg}");
                    said = Some(msg);
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(cfg.poll_secs.max(1))).await;
    }
}

/// The apiserver client: verified against `ca_file` when it is set, rebuilt
/// when that file changes (or first appears).
#[derive(Default)]
struct ApiClient {
    client: Option<reqwest::Client>,
    stamp: Option<std::time::SystemTime>,
}

impl ApiClient {
    fn client(&mut self, cfg: &RouterCfg) -> anyhow::Result<reqwest::Client> {
        let stamp = cfg.ca_file.as_deref().and_then(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok());
        if let Some(c) = &self.client {
            if stamp == self.stamp {
                return Ok(c.clone());
            }
        }
        let c = api_client(cfg)?;
        if self.client.is_some() {
            info!("router: apiserver CA {} reloaded", cfg.ca_file.as_deref().unwrap_or(""));
        }
        self.client = Some(c.clone());
        self.stamp = stamp;
        Ok(c)
    }
}

/// A client for the apiserver: only `ca_file` trusted when it is set,
/// otherwise `insecure` as before.
fn api_client(cfg: &RouterCfg) -> anyhow::Result<reqwest::Client> {
    use anyhow::Context;
    let mut b = reqwest::Client::builder().timeout(Duration::from_secs(10));
    match &cfg.ca_file {
        Some(path) => {
            let pem = std::fs::read(path).with_context(|| format!("[router] ca_file {path}"))?;
            let certs = reqwest::Certificate::from_pem_bundle(&pem).with_context(|| format!("[router] ca_file {path}: not PEM"))?;
            anyhow::ensure!(!certs.is_empty(), "[router] ca_file {path}: no certificate in it");
            b = b.tls_built_in_root_certs(false);
            for c in certs {
                b = b.add_root_certificate(c);
            }
        }
        None => b = b.danger_accept_invalid_certs(cfg.insecure),
    }
    b.build().context("building the apiserver client")
}

/// The router's bearer token, read fresh (rotation in place). None without
/// `token_file`.
fn read_token(cfg: &RouterCfg) -> anyhow::Result<Option<String>> {
    let Some(path) = &cfg.token_file else { return Ok(None) };
    let t = std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("[router] token_file {path}: {e}"))?;
    let t = t.trim();
    anyhow::ensure!(!t.is_empty(), "[router] token_file {path} is empty");
    Ok(Some(t.to_string()))
}

/// GET a JSON object from the apiserver, as the router. A 401/403 says what
/// to fix.
async fn api_get(client: &reqwest::Client, url: &str, token: Option<&str>) -> anyhow::Result<serde_json::Value> {
    let mut req = client.get(url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let resp = req.send().await?;
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        let who = if token.is_some() { "the router's token (token_file)" } else { "anonymous (no [router] token_file)" };
        anyhow::bail!(
            "{status} for {url}: {who} may not read it — the router needs get/list on httproutes and get on services (stormlb#9)"
        );
    }
    Ok(resp.error_for_status()?.json().await?)
}

/// `backend_ca_file`'s last load: its mtime, and the last error logged (so a
/// missing file is said once).
#[derive(Default)]
struct CaState {
    stamp: Option<std::time::SystemTime>,
    loaded: bool,
    error: Option<String>,
}

impl CaState {
    /// A new connector when the file changed and loads; None otherwise (the
    /// last good one stays).
    fn reload(&mut self, path: &str) -> Option<TlsConnector> {
        let stamp = std::fs::metadata(path).and_then(|m| m.modified()).ok();
        if self.loaded && stamp == self.stamp {
            return None;
        }
        match upstream_connector(path) {
            Ok(c) => {
                info!("router: backend CA {path} loaded");
                self.stamp = stamp;
                self.loaded = true;
                self.error = None;
                Some(c)
            }
            Err(e) => {
                let msg = format!("{e:#}");
                if self.error.as_deref() != Some(msg.as_str()) {
                    warn!("router: backend_ca_file {path}: {msg} (https backends fail until it loads)");
                    self.error = Some(msg);
                }
                None
            }
        }
    }
}

/// A TLS client trusting only the certificates in `path`, HTTP/1.1.
fn upstream_connector(path: &str) -> anyhow::Result<TlsConnector> {
    use anyhow::Context;
    use tokio_rustls::rustls::pki_types::{pem::PemObject, CertificateDer};
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for c in CertificateDer::pem_file_iter(path).with_context(|| format!("reading {path}"))? {
        roots.add(c.with_context(|| format!("parsing {path}"))?).with_context(|| format!("{path}: not a usable CA"))?;
    }
    anyhow::ensure!(!roots.is_empty(), "{path}: no certificate in it");
    let mut cfg = tokio_rustls::rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(TlsConnector::from(Arc::new(cfg)))
}

async fn fetch_table(
    client: &reqwest::Client,
    api: &str,
    token: Option<&str>,
) -> anyhow::Result<HashMap<String, Backend>> {
    let url = format!("{api}/apis/gateway.networking.k8s.io/v1/httproutes");
    let list = api_get(client, &url, token).await?;
    let mut out = HashMap::new();
    for r in list["items"].as_array().into_iter().flatten() {
        let ns = r["metadata"]["namespace"].as_str().unwrap_or("default");
        let name = r["metadata"]["name"].as_str().unwrap_or("");
        let backend = match backend_of(client, api, token, ns, r).await.and_then(|addr| with_protocol(r, addr)) {
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

/// The backend with the route's protocol: `storm.io/backend-protocol`
/// `http` (or absent) or `https`; for https, the name to verify is
/// `storm.io/backend-server-name`, else the address's host.
fn with_protocol(route: &serde_json::Value, addr: String) -> anyhow::Result<Backend> {
    let a = &route["metadata"]["annotations"];
    match a["storm.io/backend-protocol"].as_str().map(str::to_ascii_lowercase).as_deref() {
        None | Some("http") => Ok(Backend { addr, tls: None }),
        Some("https") => {
            let name = match a["storm.io/backend-server-name"].as_str() {
                Some(n) => n.to_string(),
                None => {
                    let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(&addr);
                    host.trim_start_matches('[').trim_end_matches(']').to_string()
                }
            };
            Ok(Backend { addr, tls: Some(name) })
        }
        Some(p) => anyhow::bail!("storm.io/backend-protocol {p:?}: want http or https"),
    }
}

async fn backend_of(
    client: &reqwest::Client,
    api: &str,
    token: Option<&str>,
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
    let s = api_get(client, &url, token).await?;
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
        assert_eq!(request_path(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n"), Some("/healthz"));
        assert_eq!(request_path(b"GET /healthz?v=1 HTTP/1.1\r\n\r\n"), Some("/healthz"));
    }

    /// The router's own answer, byte for byte, over a real socket: CRLF
    /// line endings (stormlb#5), the body its content-length says.
    #[tokio::test]
    async fn healthz_on_an_unclaimed_host_is_crlf_on_the_wire() {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(serve_on(l, Front::plain(Arc::new(RwLock::new(HashMap::new())))));
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").await.unwrap();
        let mut got = Vec::new();
        c.read_to_end(&mut got).await.unwrap();
        assert_eq!(
            String::from_utf8(got).unwrap(),
            "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 13\r\nconnection: close\r\n\r\nrouter alive\n"
        );
    }

    #[test]
    fn the_request_target_keeps_the_query() {
        assert_eq!(request_target(b"GET /a/b?c=1 HTTP/1.1\r\n\r\n"), Some("/a/b?c=1"));
        assert_eq!(request_target(b"CONNECT x:443 HTTP/1.1\r\n\r\n"), None);
    }

    #[test]
    fn tls_requests_say_https_and_a_clients_claim_is_dropped() {
        let head = b"GET / HTTP/1.1\r\nHost: a\r\nx-forwarded-proto: http\r\nAccept: */*\r\n\r\nBODY";
        let end = find_head_end(head).unwrap();
        let out = String::from_utf8(forwarded_https(head, end)).unwrap();
        assert_eq!(out, "GET / HTTP/1.1\r\nX-Forwarded-Proto: https\r\nHost: a\r\nAccept: */*\r\n\r\nBODY");
    }

    #[test]
    fn the_status_code_is_read_from_a_status_line() {
        assert_eq!(status_code(b"HTTP/1.1 200 OK\r\n"), Some("200"));
        assert_eq!(status_code(b"HTTP/1.0 404 Not Found"), Some("404"));
        assert_eq!(status_code(b"HTTP/1.1 101 Switching"), Some("101"));
        assert_eq!(status_code(b"SSH-2.0-OpenSSH"), None);
        assert_eq!(status_code(b"HTTP/1.1 2"), None);
    }

    #[test]
    fn the_backend_protocol_and_name_come_from_annotations() {
        let r = |a: serde_json::Value| serde_json::json!({ "metadata": { "annotations": a } });
        let b = |a, addr: &str| with_protocol(&r(a), addr.into());
        assert_eq!(b(serde_json::json!({}), "10.0.0.1:80").unwrap().tls, None);
        assert_eq!(b(serde_json::json!({"storm.io/backend-protocol": "HTTP"}), "10.0.0.1:80").unwrap().tls, None);
        let https = serde_json::json!({"storm.io/backend-protocol": "https"});
        assert_eq!(b(https.clone(), "127.0.0.1:9096").unwrap().tls.as_deref(), Some("127.0.0.1"));
        assert_eq!(b(https, "[::1]:9096").unwrap().tls.as_deref(), Some("::1"));
        let named = serde_json::json!({"storm.io/backend-protocol": "https", "storm.io/backend-server-name": "cadvisor.node"});
        assert_eq!(b(named, "127.0.0.1:9096").unwrap().tls.as_deref(), Some("cadvisor.node"));
        assert!(b(serde_json::json!({"storm.io/backend-protocol": "h2c"}), "1.2.3.4:5").is_err());
    }

    #[test]
    fn the_head_end_is_the_blank_line() {
        assert_eq!(find_head_end(b"GET / HTTP/1.1\r\n\r\nbody"), Some(18));
        assert_eq!(find_head_end(b"GET / HTTP/1.1\r\n"), None);
    }
}
