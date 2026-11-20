//! The L7 half of inbound: a router over Gateway API HTTPRoutes.
//!
//! docs/routing.md (stormpump) names the shape: wildcard DNS carries
//! `*.storm1.<zone>` to a VIP, and something at the VIP demuxes on the HTTP
//! Host header. This is that something. It lives here because stormlb already
//! owns everything inbound — the VIP, the health checks, the L4 balancer —
//! and one component owning one request path is the debugging story the
//! alternative (VIP ours, L7 Cilium/Envoy) does not have.
//!
//! **Per request** (#26, #37). The router is an HTTP/1.1 proxy (hyper on
//! both sides): every request on a keep-alive connection is matched on its
//! own — host, path, method, headers, query — against the routes' rules
//! ([`crate::routes`]), and sent to an endpoint of the backend the rule's
//! weights pick. Bodies stream both ways; `Upgrade` (websockets) is answered
//! by the backend's 101 and then the two connections are spliced. Backend
//! connections are kept alive and reused. Hop-by-hop headers are not
//! forwarded; `X-Forwarded-For`, `-Host` and `-Proto` are set (a client's own
//! `X-Forwarded-Proto` is replaced). A backend that answers `connection:
//! close` closes the client's connection after that response too.
//!
//! **Errors are answers**: no rule for the host 404s, a rule whose backends
//! have no healthy endpoint 503s, an unresolved backendRef 500s, a backend
//! that cannot be reached (after trying up to three endpoints) or gives no
//! response 502s, and `timeouts.request` 504s. Each body names the host.
//!
//! **The table is polled, not watched.** A route change is an operator
//! action; a few seconds of staleness is invisible, and a poll cannot be
//! wedged by a watch protocol misunderstanding — this apiserver is a
//! reimplementation, and the router should not be the corner that finds its
//! watch bugs. The poll keeps serving the last good table on error, because
//! a flapping apiserver must not take working routes down with it. Between
//! polls, health moves on its own: active checks, and a refused connection
//! ejecting an endpoint for [`crate::routes::EJECT`].
//!
//! **Status**: each poll writes what it understood into the route's
//! `status.parents` (Accepted, ResolvedRefs, storm.io/BackendsHealthy), only
//! when that changed. A refused write is logged once; routing never waits on
//! it.
//!
//! **TLS** (`[router.tls]`, #14): a second listener terminates TLS with the
//! configured certificates (picked by SNI, [`crate::tls`]) and feeds the same
//! router. Once a certificate is loaded, plain HTTP answers with a 308 to
//! https, except `/healthz`.
//!
//! **TLS to backends** (#13): a route with `storm.io/backend-protocol: https`
//! is dialled over TLS and verified against `[router] backend_ca_file` only,
//! for the endpoint's IP (or `storm.io/backend-server-name`). Without a CA
//! the request fails closed (502); nothing is sent to a backend it can't
//! verify.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::header::{self, HeaderMap, HeaderName, HeaderValue};
use http::{Request, Response, Uri};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Empty, Full};
use hyper::body::{Body as _, Incoming};
use hyper::client::conn::http1 as client_http1;
use hyper::server::conn::http1 as server_http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{info, warn};

use crate::metrics::global as metrics;
use crate::routes::{self, Endpoint, Known, Lookup, NewCheck, Proto, ReqView, Rule, Table};
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
    /// ServiceAccount's, which stormcos mints with list on HTTPRoutes,
    /// Services, Endpoints and EndpointSlices, and patch on httproutes/status
    /// (#37). Re-read on every poll, so it can be rotated in
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

/// The longest a request head may be; past it, 431 and the connection closes.
const MAX_HEAD: usize = 16 * 1024;
/// How long a backend has to accept a TCP connection.
const CONNECT: Duration = Duration::from_secs(5);
/// Endpoints one request may try when connections fail.
const TRIES: usize = 3;
/// Idle backend connections kept per endpoint.
const IDLE_PER_ENDPOINT: usize = 32;
/// After a refused status write, how long before that route is tried again.
const STATUS_BACKOFF: Duration = Duration::from_secs(60);

type Body = UnsyncBoxBody<Bytes, hyper::Error>;

/// The live table: swapped whole by each poll, read per request.
type Shared = Arc<std::sync::RwLock<Arc<Table>>>;

/// The connector for TLS backends, built from `backend_ca_file`; None until
/// the file loads.
type Upstream = Arc<std::sync::RwLock<Option<TlsConnector>>>;

/// The table of the router this process runs, for `/api/v1/routes` and the
/// metrics' per-backend series.
static CURRENT: OnceLock<std::sync::Mutex<Option<Shared>>> = OnceLock::new();

/// The table the running router serves now (None before the router starts).
pub fn current() -> Option<Arc<Table>> {
    let s = CURRENT.get()?.lock().unwrap().clone()?;
    let t = s.read().unwrap().clone();
    Some(t)
}

pub async fn run(cfg: RouterCfg) -> anyhow::Result<()> {
    let table: Shared = Arc::new(std::sync::RwLock::new(Arc::new(Table::default())));
    *CURRENT.get_or_init(Default::default).lock().unwrap() = Some(table.clone());
    let mut plain = bind_listeners(&cfg.listen).await?;
    info!(listen = %cfg.listen, apiserver = %cfg.apiserver, "router up");

    let upstream: Upstream = Arc::default();
    tokio::spawn(poll_routes(cfg.clone(), table.clone(), upstream.clone()));
    let pool = Arc::new(Pool::default());

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
                let front = Front { table: table.clone(), upstream: upstream.clone(), redirect: None, pool: pool.clone() };
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

    let front = Front { table, upstream, redirect, pool };
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
    table: Shared,
    upstream: Upstream,
    redirect: Option<Arc<Redirect>>,
    pool: Arc<Pool>,
}

impl Front {
    /// Plain HTTP, no redirect, over this table.
    #[cfg(test)]
    fn plain(table: Table) -> Front {
        Front { table: Arc::new(std::sync::RwLock::new(Arc::new(table))), upstream: Arc::default(), redirect: None, pool: Arc::default() }
    }
}

/// Redirect plain HTTP to https on `port` — while a certificate is loaded.
struct Redirect {
    certs: Arc<tls::Certs>,
    port: u16,
}

/// Accept TLS, then serve HTTP on it.
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
            serve_http(tls, front, true, peer).await;
        });
    }
}

/// Accept and serve, on one listener.
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
            let _ = conn.set_nodelay(true);
            serve_http(conn, front, false, peer).await;
        });
    }
}

/// HTTP/1.1 on one client connection, each request routed on its own.
async fn serve_http<S>(io: S, front: Front, tls: bool, peer: SocketAddr)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let svc = hyper::service::service_fn(move |req| {
        let front = front.clone();
        async move { Ok::<_, Infallible>(handle(req, &front, tls, peer).await) }
    });
    let served = server_http1::Builder::new()
        .timer(TokioTimer::new())
        // A client that opens a connection and sends no head is let go.
        .header_read_timeout(Duration::from_secs(30))
        // 16 KiB is far past any sane request head: one that big gets 431
        // and the connection closed, not buffered forever.
        .max_buf_size(MAX_HEAD)
        .max_headers(256)
        .serve_connection(TokioIo::new(io), svc)
        .with_upgrades()
        .await;
    if let Err(e) = served {
        // One line per failed connection, not per byte: the common errors
        // here are a client that went away, and both are ordinary.
        tracing::debug!(%peer, "connection ended: {e}");
    }
}

/// A body from bytes.
fn full(b: impl Into<Bytes>) -> Body {
    Full::new(b.into()).map_err(|n: Infallible| match n {}).boxed_unsync()
}

/// The router's own answer: text, and the connection closed after it.
fn text(status: u16, body: String) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain")
        .header(header::CONTENT_LENGTH, body.len())
        .header(header::CONNECTION, "close")
        .body(full(body))
        .expect("a valid response")
}

/// The request's host: the Host header (or the absolute URI's), lowercased,
/// the port dropped.
fn host_of(req: &Request<Incoming>) -> Option<String> {
    let raw = match req.headers().get(header::HOST) {
        Some(h) => h.to_str().ok()?.trim().to_ascii_lowercase(),
        None => req.uri().host()?.to_ascii_lowercase(),
    };
    Some(strip_port(&raw).to_string()).filter(|h| !h.is_empty())
}

fn strip_port(h: &str) -> &str {
    if let Some(rest) = h.strip_prefix('[') {
        // [v6]:port → v6 (as the route would name it, without brackets)
        return rest.split(']').next().unwrap_or(rest);
    }
    match h.rsplit_once(':') {
        Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) && !host.contains(':') => host,
        _ => h,
    }
}

fn record(label: &str, code: u16) {
    metrics().inc("stormlb_router_requests_total", &[("host", label), ("code", &code.to_string())]);
}

/// One request: the router's own answers, or a backend's.
async fn handle(req: Request<Incoming>, front: &Front, tls: bool, peer: SocketAddr) -> Response<Body> {
    let Some(host) = host_of(&req) else {
        record("unrouted", 400);
        return text(400, "this router needs a Host\n\n".into());
    };
    let table = front.table.read().unwrap().clone();
    let path = req.uri().path().to_string();
    let lookup = table.lookup(
        &host,
        &ReqView { method: req.method().as_str(), path: &path, query: req.uri().query(), headers: req.headers() },
    );
    // The metrics' host label: a route's hostname, never a client's arbitrary
    // Host header (that would be unbounded cardinality).
    let label = match &lookup {
        Lookup::NoHost => "unrouted".to_string(),
        Lookup::NoMatch(l) | Lookup::Rule(_, l) => l.clone(),
    };

    // Plain HTTP goes to https once there is a certificate to serve it with,
    // except the health probe, which stays on plain HTTP.
    if let Some(r) = front.redirect.as_ref().filter(|r| !tls && r.certs.count() > 0) {
        if path != "/healthz" {
            let port = if r.port == 443 { String::new() } else { format!(":{}", r.port) };
            let target = req.uri().path_and_query().map(|p| p.as_str()).filter(|t| t.starts_with('/')).unwrap_or("/");
            record(&label, 308);
            return Response::builder()
                .status(308)
                .header(header::LOCATION, format!("https://{host}{port}{target}"))
                .header(header::CONTENT_LENGTH, 0)
                .header(header::CONNECTION, "close")
                .body(full(Bytes::new()))
                .expect("a valid response");
        }
    }

    match lookup {
        // The router's own liveness, on any host no route claims: stormd
        // probes http://127.0.0.1/healthz, and 127.0.0.1 is never a route's
        // hostname. A host a route *does* claim proxies /healthz untouched.
        Lookup::NoHost if path == "/healthz" => {
            record("unrouted", 200);
            text(200, "router alive\n".into())
        }
        // Name the host: "404 from the router" and "404 from the app" look
        // identical from a browser, and the debugging path for each is
        // different. The body says which one this is and for what name.
        Lookup::NoHost => {
            record("unrouted", 404);
            text(404, format!("no route for host {host}\n"))
        }
        Lookup::NoMatch(l) => {
            record(&l, 404);
            text(404, format!("no rule of the routes for host {host} matches {} {path}\n", req.method()))
        }
        Lookup::Rule(rule, l) => proxy(req, rule, &l, &host, front, tls, peer).await,
    }
}

/// Is this an `Upgrade` request (websockets and the like)?
fn is_upgrade(h: &HeaderMap) -> bool {
    h.contains_key(header::UPGRADE)
        && h.get_all(header::CONNECTION)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .any(|t| t.trim().eq_ignore_ascii_case("upgrade"))
}

/// Drop the hop-by-hop headers (RFC 9110 §7.6.1): those `Connection` names,
/// and Keep-Alive, Proxy-Connection, TE, Trailer, Transfer-Encoding,
/// Upgrade. An upgrade keeps `Connection: upgrade` and `Upgrade`.
fn strip_hop_by_hop(h: &mut HeaderMap, upgrade: bool) {
    let listed: Vec<HeaderName> = h
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|t| HeaderName::from_bytes(t.trim().as_bytes()).ok())
        .collect();
    for n in listed {
        if !(upgrade && n == header::UPGRADE) {
            h.remove(n);
        }
    }
    for n in ["keep-alive", "proxy-connection", "te", "trailer", "transfer-encoding"] {
        h.remove(n);
    }
    h.remove(header::CONNECTION);
    if upgrade {
        h.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
    } else {
        h.remove(header::UPGRADE);
    }
}

/// The request a retry sends: everything but the body (only bodyless
/// requests are retried after they were sent).
struct Template {
    method: http::Method,
    uri: Uri,
    headers: HeaderMap,
}

impl Template {
    fn build(&self) -> Request<Body> {
        let mut r = Request::new(Empty::<Bytes>::new().map_err(|n: Infallible| match n {}).boxed_unsync());
        *r.method_mut() = self.method.clone();
        *r.uri_mut() = self.uri.clone();
        *r.headers_mut() = self.headers.clone();
        r
    }
}

/// Why a request got no backend response.
enum Fail {
    /// The rule has no backend to send it to: (status, text).
    NoBackend(u16, String),
    /// No endpoint could be connected to.
    Unreachable(String),
    /// Sent, and no response came.
    NoResponse(String),
    /// `timeouts.request` or `backendRequest` ran out.
    Timeout(Duration),
}

/// Send the request to the rule's backends: connect failures move on to
/// another endpoint, and the rule's `retry` codes are retried for a request
/// without a body.
async fn proxy(
    mut req: Request<Incoming>,
    rule: Arc<Rule>,
    label: &str,
    host: &str,
    front: &Front,
    tls: bool,
    peer: SocketAddr,
) -> Response<Body> {
    let upgrade = is_upgrade(req.headers());
    let client_upgrade = upgrade.then(|| hyper::upgrade::on(&mut req));
    let host_header = req.headers().get(header::HOST).cloned();
    {
        let h = req.headers_mut();
        strip_hop_by_hop(h, upgrade);
        let xff = match h.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            Some(v) => format!("{v}, {}", peer.ip()),
            None => peer.ip().to_string(),
        };
        if let Ok(v) = HeaderValue::from_str(&xff) {
            h.insert("x-forwarded-for", v);
        }
        match &host_header {
            Some(v) => {
                if !h.contains_key("x-forwarded-host") {
                    h.insert("x-forwarded-host", v.clone());
                }
            }
            None => {
                if let Ok(v) = HeaderValue::from_str(host) {
                    h.insert(header::HOST, v);
                }
            }
        }
        // Never a client's claim: the scheme it actually came in on.
        h.insert("x-forwarded-proto", HeaderValue::from_static(if tls { "https" } else { "http" }));
    }
    let pq = req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/").to_string();
    *req.uri_mut() = pq.parse().unwrap_or_else(|_| Uri::from_static("/"));
    *req.version_mut() = http::Version::HTTP_11;
    let bodyless = req.body().is_end_stream();
    let template = Template { method: req.method().clone(), uri: req.uri().clone(), headers: req.headers().clone() };
    let first = req.map(|b| b.boxed_unsync());

    let started = Instant::now();
    let work = attempts(first, &template, bodyless, upgrade, &rule, label, front);
    let result = match rule.timeout {
        Some(d) => tokio::time::timeout(d, work).await.unwrap_or(Err(Fail::Timeout(d))),
        None => work.await,
    };
    let (resp, backend) = match result {
        Ok(x) => x,
        Err(f) => {
            let (code, msg, kind) = match f {
                Fail::NoBackend(c, m) => (c, m, None),
                Fail::Unreachable(e) => (502, format!("no endpoint of the backend for host {host} could be reached: {e}"), None),
                Fail::NoResponse(e) => (502, format!("the backend for host {host} gave no response: {e}"), Some("no_response")),
                Fail::Timeout(d) => (504, format!("the backend for host {host} did not answer within {} ms", d.as_millis()), Some("timeout")),
            };
            if let Some(k) = kind {
                metrics().inc("stormlb_router_upstream_errors_total", &[("host", label), ("kind", k)]);
            }
            record(label, code);
            return text(code, msg + "\n");
        }
    };
    let code = resp.status().as_u16();
    metrics().observe("stormlb_router_request_duration_seconds", &[("host", label)], started.elapsed());
    record(label, code);
    metrics().inc(
        "stormlb_router_backend_requests_total",
        &[("route", &rule.route), ("backend", &backend.name), ("code", &code.to_string())],
    );

    let mut resp = resp;
    if code == 101 {
        if let Some(cu) = client_upgrade {
            let su = hyper::upgrade::on(&mut resp);
            tokio::spawn(async move {
                match (cu.await, su.await) {
                    (Ok(c), Ok(s)) => {
                        let _ = tokio::io::copy_bidirectional(&mut TokioIo::new(c), &mut TokioIo::new(s)).await;
                    }
                    (Err(e), _) | (_, Err(e)) => tracing::debug!("upgrade failed: {e}"),
                }
            });
        }
        return resp.map(|b| b.boxed_unsync());
    }
    let close = resp
        .headers()
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|t| t.trim().eq_ignore_ascii_case("close"));
    strip_hop_by_hop(resp.headers_mut(), false);
    if close {
        // The backend closes after this response; so does the client's
        // connection, as it would without a router in between.
        resp.headers_mut().insert(header::CONNECTION, HeaderValue::from_static("close"));
    }
    resp.map(|b| b.boxed_unsync())
}

/// The attempts of one request. Ok: the backend's response (its head; the
/// body streams on) and the backend that gave it.
async fn attempts(
    first: Request<Body>,
    template: &Template,
    bodyless: bool,
    upgrade: bool,
    rule: &Rule,
    label: &str,
    front: &Front,
) -> Result<(Response<Incoming>, Arc<routes::Backend>), Fail> {
    let mut first = Some(first);
    let mut tried: Vec<String> = Vec::new();
    let mut failures = 0usize;
    let mut stale = 0usize;
    let mut code_retries = if upgrade { 0 } else { rule.retry.attempts };
    let mut last = String::new();
    loop {
        let backend = rule.choose().map_err(|(c, m)| Fail::NoBackend(c, m))?;
        let Some(ep) = backend.pick(&tried) else {
            return Err(if tried.is_empty() {
                Fail::NoBackend(503, format!("no healthy endpoint for route {} backend {}", rule.route, backend.name))
            } else {
                Fail::Unreachable(last)
            });
        };
        tried.push(ep.addr.clone());
        let key = format!("{}|{:?}", ep.addr, backend.proto.server_name(&ep.addr));
        let sender = match front.pool.take(&key) {
            Some(s) => s,
            None => match connect(&ep, &backend.proto, &front.upstream).await {
                Ok(s) => s,
                Err((kind, e)) => {
                    if kind == "connect" {
                        ep.eject();
                    }
                    metrics().inc("stormlb_router_upstream_errors_total", &[("host", label), ("kind", kind)]);
                    tracing::debug!(endpoint = %ep.addr, "{e}");
                    last = e;
                    failures += 1;
                    if failures < TRIES {
                        continue;
                    }
                    return Err(Fail::Unreachable(last));
                }
            },
        };
        let req = match first.take() {
            Some(r) => r,
            None if bodyless => template.build(),
            None => return Err(Fail::NoResponse(last)),
        };
        ep.active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let sent = send(sender, req, rule.backend_timeout).await;
        ep.active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        match sent {
            Ok((resp, sender)) => {
                let code = resp.status().as_u16();
                if code_retries > 0 && bodyless && rule.retry.codes.contains(&code) {
                    code_retries -= 1;
                    last = format!("{} answered {code}", ep.addr);
                    drop((resp, sender));
                    tried.retain(|a| a != &ep.addr);
                    tokio::time::sleep(rule.retry.backoff).await;
                    continue;
                }
                if code != 101 {
                    front.pool.keep(key, sender);
                }
                return Ok((resp, backend));
            }
            Err(Sent::Unsent(req, e)) => {
                // A kept connection the backend had closed: the request was
                // never written, so it goes out again on a fresh one.
                first = Some(*req);
                tried.pop();
                stale += 1;
                last = e;
                if stale > IDLE_PER_ENDPOINT {
                    return Err(Fail::NoResponse(last));
                }
            }
            Err(Sent::Failed(e)) => {
                metrics().inc("stormlb_router_upstream_errors_total", &[("host", label), ("kind", "no_response")]);
                last = format!("{}: {e}", ep.addr);
                failures += 1;
                if !(bodyless && failures < TRIES) {
                    return Err(Fail::NoResponse(last));
                }
            }
            Err(Sent::TimedOut(d)) => {
                metrics().inc("stormlb_router_upstream_errors_total", &[("host", label), ("kind", "timeout")]);
                return Err(Fail::Timeout(d));
            }
        }
    }
}

/// How sending one request on one connection ended, short of a response.
enum Sent {
    /// Not written (the connection was already closed): the request back.
    Unsent(Box<Request<Body>>, String),
    Failed(String),
    TimedOut(Duration),
}

async fn send(
    mut s: client_http1::SendRequest<Body>,
    req: Request<Body>,
    timeout: Option<Duration>,
) -> Result<(Response<Incoming>, client_http1::SendRequest<Body>), Sent> {
    if s.ready().await.is_err() {
        return Err(Sent::Unsent(Box::new(req), "the kept connection had closed".into()));
    }
    let fut = s.try_send_request(req);
    let r = match timeout {
        Some(d) => match tokio::time::timeout(d, fut).await {
            Ok(r) => r,
            Err(_) => return Err(Sent::TimedOut(d)),
        },
        None => fut.await,
    };
    match r {
        Ok(resp) => Ok((resp, s)),
        Err(mut e) => match e.take_message() {
            Some(req) => Err(Sent::Unsent(Box::new(req), e.into_error().to_string())),
            None => Err(Sent::Failed(e.into_error().to_string())),
        },
    }
}

/// A new connection to an endpoint: TCP, then TLS for an https backend.
/// Err: (metrics kind, text).
async fn connect(
    ep: &Endpoint,
    proto: &Proto,
    upstream: &Upstream,
) -> Result<client_http1::SendRequest<Body>, (&'static str, String)> {
    let tcp = match tokio::time::timeout(CONNECT, TcpStream::connect(&ep.addr)).await {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => return Err(("connect", format!("{}: {e}", ep.addr))),
        Err(_) => return Err(("connect", format!("{}: no connection in {} s", ep.addr, CONNECT.as_secs()))),
    };
    let _ = tcp.set_nodelay(true);
    let Some(name) = proto.server_name(&ep.addr) else {
        return handshake(tcp).await;
    };
    // TLS to the backend, verified against the CA, or nothing at all.
    let Some(connector) = upstream.read().unwrap().clone() else {
        return Err(("tls", format!("{} is https, but no [router] backend_ca_file is loaded", ep.addr)));
    };
    let server = tokio_rustls::rustls::pki_types::ServerName::try_from(name.clone())
        .map_err(|e| ("tls", format!("backend name {name:?}: {e}")))?;
    match tokio::time::timeout(Duration::from_secs(10), connector.connect(server, tcp)).await {
        Ok(Ok(s)) => handshake(s).await,
        Ok(Err(e)) => Err(("tls", format!("TLS to {} ({name}): {e}", ep.addr))),
        Err(_) => Err(("tls", format!("TLS to {} ({name}): handshake timed out", ep.addr))),
    }
}

async fn handshake<S>(io: S) -> Result<client_http1::SendRequest<Body>, (&'static str, String)>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (s, conn) = client_http1::Builder::new()
        .title_case_headers(true)
        .handshake(TokioIo::new(io))
        .await
        .map_err(|e| ("connect", format!("HTTP handshake: {e}")))?;
    tokio::spawn(async move {
        if let Err(e) = conn.with_upgrades().await {
            tracing::debug!("backend connection: {e}");
        }
    });
    Ok(s)
}

/// Idle backend connections, by endpoint (and TLS name).
#[derive(Default)]
struct Pool {
    idle: std::sync::Mutex<HashMap<String, Vec<client_http1::SendRequest<Body>>>>,
}

impl Pool {
    fn take(&self, key: &str) -> Option<client_http1::SendRequest<Body>> {
        let mut m = self.idle.lock().unwrap();
        let v = m.get_mut(key)?;
        while let Some(s) = v.pop() {
            if !s.is_closed() {
                return Some(s);
            }
        }
        None
    }

    /// Keep the connection once its response is done (if it stays open).
    fn keep(self: &Arc<Self>, key: String, mut s: client_http1::SendRequest<Body>) {
        let pool = self.clone();
        tokio::spawn(async move {
            if s.ready().await.is_ok() {
                let mut m = pool.idle.lock().unwrap();
                let v = m.entry(key).or_default();
                v.retain(|s| !s.is_closed());
                if v.len() < IDLE_PER_ENDPOINT {
                    v.push(s);
                }
            }
        });
    }
}

/// `"200"` from `HTTP/1.1 200 OK…`.
fn status_code(b: &[u8]) -> Option<&str> {
    let s = std::str::from_utf8(b.get(..12)?).ok()?;
    let code = s.strip_prefix("HTTP/1.")?.get(2..5)?;
    code.bytes().all(|c| c.is_ascii_digit()).then_some(code)
}

/// One endpoint's active check, every interval, until the endpoint leaves
/// the table.
async fn check_loop(c: NewCheck, upstream: Upstream) {
    loop {
        let Some(ep) = c.endpoint.upgrade() else { return };
        let ok = probe(&ep.addr, &c.spec, &c.proto, &upstream).await;
        match ep.record_check(ok, &c.spec) {
            Some(true) => info!(endpoint = %ep.addr, path = %c.spec.path, "health check passing: back in rotation"),
            Some(false) => warn!(endpoint = %ep.addr, path = %c.spec.path, "health check failing: out of rotation"),
            None => {}
        }
        drop(ep);
        tokio::time::sleep(c.spec.interval).await;
    }
}

/// GET the check path; 2xx and 3xx pass.
async fn probe(addr: &str, spec: &routes::CheckSpec, proto: &Proto, upstream: &Upstream) -> bool {
    let host = spec.host.clone().unwrap_or_else(|| addr.to_string());
    let req = format!("GET {} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: stormlb-health\r\nConnection: close\r\n\r\n", spec.path);
    let check = async {
        let tcp = TcpStream::connect(addr).await.ok()?;
        match proto.server_name(addr) {
            None => status_of(tcp, &req).await,
            Some(name) => {
                let c = upstream.read().unwrap().clone()?;
                let server = tokio_rustls::rustls::pki_types::ServerName::try_from(name).ok()?;
                status_of(c.connect(server, tcp).await.ok()?, &req).await
            }
        }
    };
    matches!(tokio::time::timeout(spec.timeout, check).await, Ok(Some(code)) if (200..400).contains(&code))
}

async fn status_of<S: AsyncRead + AsyncWrite + Unpin>(mut s: S, req: &str) -> Option<u16> {
    s.write_all(req.as_bytes()).await.ok()?;
    let mut buf = [0u8; 64];
    let mut n = 0;
    while n < 12 {
        let m = s.read(&mut buf[n..]).await.ok()?;
        if m == 0 {
            break;
        }
        n += m;
    }
    status_code(&buf[..n])?.parse().ok()
}

/// Refresh the table forever. Serves the last good table across errors.
/// Each turn also re-reads `backend_ca_file` if it changed, and writes the
/// routes' status where it changed.
async fn poll_routes(cfg: RouterCfg, table: Shared, upstream: Upstream) {
    let mut ca = CaState::default();
    let mut api = ApiClient::default();
    let mut known = Known::default();
    let mut said = None::<String>;
    let mut status = StatusLog::default();
    let mut last_len = usize::MAX;
    loop {
        if let Some(path) = &cfg.backend_ca_file {
            if let Some(c) = ca.reload(path) {
                *upstream.write().unwrap() = Some(c);
            }
        }
        let fetched = match (api.client(&cfg), read_token(&cfg)) {
            (Ok(client), Ok(token)) => match fetch_objects(&client, &cfg.apiserver, token.as_deref()).await {
                Ok(o) => Ok((o, client, token)),
                Err(e) => Err(e),
            },
            (Err(e), _) | (_, Err(e)) => Err(e),
        };
        match fetched {
            Ok((objs, client, token)) => {
                let (new, checks) = routes::build(&objs, &mut known);
                for c in checks {
                    tokio::spawn(check_loop(c, upstream.clone()));
                }
                metrics().inc("stormlb_router_route_refreshes_total", &[("result", "ok")]);
                metrics().gauge_set("stormlb_router_routes", &[], new.hosts() as i64);
                if new.hosts() != last_len {
                    info!(routes = new.hosts(), "route table refreshed");
                    last_len = new.hosts();
                }
                let new = Arc::new(new);
                *table.write().unwrap() = new.clone();
                said = None;
                status.write(&client, &cfg.apiserver, token.as_deref(), &new).await;
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

/// What was said about each route: its problems, and its last refused
/// status write (and when), so each is logged once.
#[derive(Default)]
struct StatusLog {
    problems: HashMap<String, String>,
    refused: HashMap<String, (String, Instant)>,
}

impl StatusLog {
    async fn write(&mut self, client: &reqwest::Client, api: &str, token: Option<&str>, t: &Table) {
        let now = routes::now_rfc3339();
        for r in &t.routes {
            let key = format!("{}/{}", r.namespace, r.name);
            let problems: Vec<String> = routes::conditions(r)
                .into_iter()
                .filter(|(ty, ok, ..)| (*ty == "PartiallyInvalid") == *ok && *ty != "storm.io/BackendsHealthy")
                .map(|(ty, _, reason, msg)| format!("{ty} {reason}: {msg}"))
                .collect();
            let p = problems.join("; ");
            if self.problems.get(&key) != Some(&p) {
                if !p.is_empty() {
                    warn!(route = %key, "{p}");
                }
                self.problems.insert(key.clone(), p);
            }
            if self.refused.get(&key).is_some_and(|(_, at)| at.elapsed() < STATUS_BACKOFF) {
                continue;
            }
            let Some(parents) = routes::status_parents(r, &now) else { continue };
            let url = format!(
                "{api}/apis/gateway.networking.k8s.io/v1/namespaces/{}/httproutes/{}/status",
                r.namespace, r.name
            );
            match api_merge_patch(client, &url, token, &serde_json::json!({"status": {"parents": parents}})).await {
                Ok(()) => {
                    self.refused.remove(&key);
                }
                Err(e) => {
                    let msg = format!("{e:#}");
                    if self.refused.get(&key).map(|(m, _)| m) != Some(&msg) {
                        warn!(route = %key, "status not written (routing is unaffected): {msg}");
                    }
                    self.refused.insert(key, (msg, Instant::now()));
                }
            }
        }
        let live: std::collections::HashSet<String> = t.routes.iter().map(|r| format!("{}/{}", r.namespace, r.name)).collect();
        self.problems.retain(|k, _| live.contains(k));
        self.refused.retain(|k, _| live.contains(k));
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

/// Every object one table needs: the HTTPRoutes, and — when a route has a
/// backendRef — the Services, EndpointSlices and Endpoints of every
/// namespace (three lists a poll, however many routes). A list the
/// apiserver does not serve (404) is absent; any other failure fails the
/// refresh, so the last good table stays.
async fn fetch_objects(client: &reqwest::Client, api: &str, token: Option<&str>) -> anyhow::Result<routes::Objects> {
    let list = api_list(client, &format!("{api}/apis/gateway.networking.k8s.io/v1/httproutes"), token)
        .await?
        .ok_or_else(|| anyhow::anyhow!("the apiserver serves no HTTPRoutes (404)"))?;
    let mut o = routes::Objects { routes: list["items"].as_array().cloned().unwrap_or_default(), ..Default::default() };
    let needs_backends = o.routes.iter().any(|r| {
        r["metadata"]["annotations"]["storm.io/backend"].is_null()
            && r["spec"]["rules"].as_array().into_iter().flatten().any(|rule| rule["backendRefs"].as_array().is_some_and(|b| !b.is_empty()))
    });
    if !needs_backends {
        return Ok(o);
    }
    match api_list(client, &format!("{api}/api/v1/services"), token).await? {
        Some(v) => o.index_services(&v),
        None => o.services_error = Some("the apiserver serves no Services (404)".into()),
    }
    if let Some(v) = api_list(client, &format!("{api}/apis/discovery.k8s.io/v1/endpointslices"), token).await? {
        o.index_slices(&v);
    }
    if let Some(v) = api_list(client, &format!("{api}/api/v1/endpoints"), token).await? {
        o.index_endpoints(&v);
    }
    Ok(o)
}

/// GET a list as the router; None on 404. A 401/403 says what to fix.
async fn api_list(client: &reqwest::Client, url: &str, token: Option<&str>) -> anyhow::Result<Option<serde_json::Value>> {
    let mut req = client.get(url);
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let resp = req.send().await?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
        let who = if token.is_some() { "the router's token (token_file)" } else { "anonymous (no [router] token_file)" };
        anyhow::bail!(
            "{status} for {url}: {who} may not read it — the router needs list on httproutes, services, \
             endpoints and endpointslices, and patch on httproutes/status (stormlb#9, #37)"
        );
    }
    Ok(Some(resp.error_for_status()?.json().await?))
}

/// A JSON merge-patch, as the router.
async fn api_merge_patch(client: &reqwest::Client, url: &str, token: Option<&str>, body: &serde_json::Value) -> anyhow::Result<()> {
    let mut req = client.patch(url).header("content-type", "application/merge-patch+json").body(body.to_string());
    if let Some(t) = token {
        req = req.bearer_auth(t);
    }
    let resp = req.send().await?;
    let status = resp.status();
    if !status.is_success() {
        let text = resp.text().await.unwrap_or_default();
        let text: String = text.chars().take(200).collect();
        anyhow::bail!("{status} for {url}: {}", text.trim());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table(routes: Vec<serde_json::Value>) -> Table {
        let objs = routes::Objects { routes, ..Default::default() };
        routes::build(&objs, &mut Known::default()).0
    }

    async fn serve(t: Table) -> SocketAddr {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(serve_on(l, Front::plain(t)));
        addr
    }

    async fn exchange(addr: SocketAddr, req: &[u8]) -> String {
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(req).await.unwrap();
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), c.read_to_end(&mut got)).await.unwrap().unwrap();
        String::from_utf8(got).unwrap()
    }

    /// A backend answering each request with its name and the path, on a
    /// keep-alive connection.
    async fn named_backend(name: &'static str) -> SocketAddr {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut s, _)) = l.accept().await {
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    loop {
                        while let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4) {
                            let head = String::from_utf8_lossy(&buf[..end]).into_owned();
                            buf.drain(..end);
                            let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
                            let body = format!("{name} {path}");
                            let resp = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n{body}", body.len());
                            if s.write_all(resp.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                        match s.read(&mut tmp).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&tmp[..n]),
                        }
                    }
                });
            }
        });
        addr
    }

    /// The router's own answer, over a real socket: CRLF line endings
    /// (stormlb#5), the body its content-length says, the connection closed.
    #[tokio::test]
    async fn healthz_on_an_unclaimed_host_is_crlf_on_the_wire() {
        let addr = serve(Table::default()).await;
        let got = exchange(addr, b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n").await;
        let (head, body) = got.split_once("\r\n\r\n").unwrap();
        assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{got:?}");
        assert!(!head.replace("\r\n", "").contains('\n'), "a bare LF in {head:?}");
        assert!(head.contains("content-type: text/plain\r\n") && head.contains("content-length: 13"), "{head:?}");
        assert!(head.contains("connection: close"), "{head:?}");
        assert_eq!(body, "router alive\n");
    }

    #[tokio::test]
    async fn the_router_answers_400_404_and_names_what_is_missing() {
        let a = named_backend("a").await;
        let addr = serve(table(vec![json!({"metadata": {"namespace": "t", "name": "a", "annotations": {"storm.io/backend": a.to_string()}},
            "spec": {"hostnames": ["a.test"]}})]))
        .await;
        // An HTTP/1.0 client is answered in HTTP/1.0.
        let r = exchange(addr, b"GET / HTTP/1.0\r\n\r\n").await;
        assert!(r.starts_with("HTTP/1.0 400") && r.ends_with("this router needs a Host\n\n"), "{r}");
        let r = exchange(addr, b"GET / HTTP/1.1\r\nHost: nobody.test\r\n\r\n").await;
        assert!(r.starts_with("HTTP/1.1 404") && r.ends_with("no route for host nobody.test\n"), "{r}");
    }

    /// #26: two hosts on one keep-alive connection reach their own backends.
    #[tokio::test]
    async fn every_request_on_a_connection_is_routed_on_its_own() {
        let (a, b) = (named_backend("a").await, named_backend("b").await);
        let route = |n: &str, be: SocketAddr| {
            json!({"metadata": {"namespace": "t", "name": n, "annotations": {"storm.io/backend": be.to_string()}},
                "spec": {"hostnames": [format!("{n}.test")]}})
        };
        let addr = serve(table(vec![route("a", a), route("b", b)])).await;
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.write_all(b"GET /1 HTTP/1.1\r\nHost: a.test\r\n\r\nGET /2 HTTP/1.1\r\nHost: b.test\r\nConnection: close\r\n\r\n").await.unwrap();
        let mut got = Vec::new();
        tokio::time::timeout(Duration::from_secs(10), c.read_to_end(&mut got)).await.unwrap().unwrap();
        let got = String::from_utf8(got).unwrap();
        assert!(got.contains("a /1") && got.contains("b /2"), "{got}");
    }

    #[test]
    fn hop_by_hop_headers_are_not_forwarded() {
        let mut h = HeaderMap::new();
        for (k, v) in [("connection", "keep-alive, x-secret"), ("x-secret", "1"), ("keep-alive", "5"), ("te", "trailers"), ("x-app", "kept")] {
            h.insert(HeaderName::from_static(k), HeaderValue::from_static(v));
        }
        strip_hop_by_hop(&mut h, false);
        assert_eq!(h.len(), 1);
        assert_eq!(h["x-app"], "kept");
        let mut u = HeaderMap::new();
        u.insert(header::CONNECTION, HeaderValue::from_static("Upgrade"));
        u.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        assert!(is_upgrade(&u));
        strip_hop_by_hop(&mut u, true);
        assert_eq!((u[header::CONNECTION].to_str().unwrap(), u[header::UPGRADE].to_str().unwrap()), ("upgrade", "websocket"));
    }

    #[test]
    fn the_port_is_stripped_from_the_host() {
        assert_eq!(strip_port("console.storm1.g8.lo:80"), "console.storm1.g8.lo");
        assert_eq!(strip_port("[::1]:80"), "::1");
        assert_eq!(strip_port("example"), "example");
    }

    #[test]
    fn the_status_code_is_read_from_a_status_line() {
        assert_eq!(status_code(b"HTTP/1.1 200 OK\r\n"), Some("200"));
        assert_eq!(status_code(b"HTTP/1.0 404 Not Found"), Some("404"));
        assert_eq!(status_code(b"SSH-2.0-OpenSSH"), None);
        assert_eq!(status_code(b"HTTP/1.1 2"), None);
    }

    #[test]
    fn the_backend_protocol_and_name_come_from_annotations() {
        let r = |a: serde_json::Value| json!({ "metadata": { "annotations": a } });
        assert_eq!(routes::proto_of(&r(json!({}))).unwrap(), Proto::Http);
        assert_eq!(routes::proto_of(&r(json!({"storm.io/backend-protocol": "HTTP"}))).unwrap(), Proto::Http);
        let https = routes::proto_of(&r(json!({"storm.io/backend-protocol": "https"}))).unwrap();
        assert_eq!(https.server_name("127.0.0.1:9096").as_deref(), Some("127.0.0.1"));
        assert_eq!(https.server_name("[::1]:9096").as_deref(), Some("::1"));
        let named = routes::proto_of(&r(json!({"storm.io/backend-protocol": "https", "storm.io/backend-server-name": "cadvisor.node"}))).unwrap();
        assert_eq!(named.server_name("127.0.0.1:9096").as_deref(), Some("cadvisor.node"));
        assert!(routes::proto_of(&r(json!({"storm.io/backend-protocol": "h2c"}))).is_err());
    }
}
