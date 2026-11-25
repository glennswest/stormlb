//! The router's backend configuration, from Gateway API HTTPRoutes (#37).
//!
//! Every poll builds a [`Table`] from three cluster-wide lists — HTTPRoutes,
//! Services and EndpointSlices (or Endpoints) — and the router picks, for
//! **each request**, the rule that matches it and a backend of that rule:
//!
//! - **Hostnames**: exact names first, then wildcards (`*.example.com`), the
//!   longest suffix winning. Only the rules of the most specific hostname are
//!   considered. A route with no hostnames serves nothing (the router has no
//!   default host; `/healthz` and the 404 live there).
//! - **Matches**: path `Exact` / `PathPrefix` (element-wise: `/api` matches
//!   `/api` and `/api/x`, not `/apix`), `method`, `headers` and
//!   `queryParams` of type `Exact`. A rule without matches is `PathPrefix /`.
//!   Precedence is the Gateway API's: an exact path, then the longest prefix,
//!   then a method, then the most headers, then the most query params; ties
//!   go to the oldest route, then to `namespace/name`, then rule order.
//! - **Backends**: each `backendRef` (kind Service) is a pool of the
//!   Service's endpoints — from its EndpointSlices, else its Endpoints, else
//!   (no endpoint object at all) its clusterIP — with the Service port mapped
//!   to the endpoints' port by name. A headless Service works like any other.
//!   `weight` splits a rule between its refs (smooth weighted round-robin, so
//!   3:1 is exactly three to one). Inside a ref: round-robin, or the endpoint
//!   with the fewest requests in flight with `storm.io/balance: least-conn`
//!   on the Service.
//! - **Health**: an endpoint takes requests while it is ready (the slice's
//!   `conditions.ready`, or `addresses` vs `notReadyAddresses`), passes its
//!   active check if the Service asks for one (`storm.io/health-check-path`,
//!   with `-interval`, `-timeout`, `-rise`, `-fall`, `-host`), and has not
//!   refused a connection in the last [`EJECT`] (passive). A ref with no
//!   such endpoint is left out of the weighting; if every endpoint of a ref is
//!   only ejected, they are tried anyway.
//! - **Other namespaces**: a `backendRef` with a `namespace` other than the
//!   route's resolves only when that namespace holds a `ReferenceGrant`
//!   from `HTTPRoute` in the route's namespace to `Service` (all of them, or
//!   that one by name). Otherwise it is unresolved: `ResolvedRefs: False`,
//!   reason `RefNotPermitted`, and its share of requests answers 500.
//! - **`storm.io/backend: host:port`** on the route still wins over its rules:
//!   one static backend for every path (node services, stormcos).
//!
//! What a rule cannot do here is refused, not ignored: a rule with `filters`,
//! a `RegularExpression` match or `sessionPersistence` is dropped and the
//! route says so in its status (`PartiallyInvalid`, or `Accepted: False`).
//!
//! **OpenShift Routes** (`route.openshift.io/v1`, #48) go into the same
//! table, one rule each:
//!
//! - **Host**: `spec.host`, or when it is empty `<name>-<namespace>.<domain>`
//!   (as OpenShift's router defaults it), the domain being `[router]
//!   route_domain` or else the wildcard the router's certificate carries.
//!   `wildcardPolicy: Subdomain` serves `*.<host minus its first label>`.
//!   A host belongs to the namespace of its oldest Route, and a host + path
//!   to its oldest Route: a later one is not admitted (`HostAlreadyClaimed`).
//! - **Path**: `spec.path` is a prefix (element-wise, as above).
//! - **Backends**: `spec.to` and `spec.alternateBackends` (Services in the
//!   route's namespace, `weight` default 100), on the Service port that
//!   `spec.port.targetPort` names (a port name, or a target port number;
//!   unset: the first port); or `storm.io/backend` as for HTTPRoutes. The
//!   annotation `haproxy.router.openshift.io/timeout` is the request timeout.
//! - **TLS**: `edge` and `reencrypt` terminate with the router's own
//!   certificate (`[router.tls]`); `reencrypt` then dials the backend over
//!   TLS, verified against `backend_ca_file` for `<service>.<namespace>.svc`
//!   (or `storm.io/backend-server-name`); `passthrough` is routed by SNI
//!   without terminating. On plain HTTP a TLS route follows its
//!   `insecureEdgeTerminationPolicy`: `Allow` serves it, `Redirect` sends
//!   clients to https, `None` (the default) sends them to https when the
//!   router redirects anyway and otherwise answers 404.
//! - **Status**: `status.ingress[]` gets this router's entry (`routerName:
//!   stormlb`, `Admitted`), others' entries are kept.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

/// How long a connection refusal keeps an endpoint out of rotation.
pub const EJECT: Duration = Duration::from_secs(5);

/// The Gateway API's group.
pub const GATEWAY_GROUP: &str = "gateway.networking.k8s.io";

/// The controller name in an HTTPRoute's `status.parents`.
pub const CONTROLLER: &str = "storm.io/stormlb";

/// The router's name in an OpenShift Route's `status.ingress`.
pub const ROUTER_NAME: &str = "stormlb";

/// The OpenShift Route group.
pub const ROUTE_GROUP: &str = "route.openshift.io";

/// What plain HTTP does with a rule's requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Insecure {
    /// The router's policy (`[router.tls] redirect`): HTTPRoutes and plain
    /// Routes.
    Router,
    /// Served on plain HTTP too (`insecureEdgeTerminationPolicy: Allow`).
    Allow,
    /// Always sent to https (`Redirect`).
    Redirect,
    /// https only (`None`): sent to https when the router redirects anyway,
    /// otherwise 404.
    Deny,
}

// ---------------------------------------------------------------- matching

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathMatch {
    Exact(String),
    Prefix(String),
}

impl PathMatch {
    pub fn matches(&self, path: &str) -> bool {
        match self {
            PathMatch::Exact(p) => path == p,
            PathMatch::Prefix(p) => {
                let p = p.trim_end_matches('/');
                p.is_empty() || path == p || path.strip_prefix(p).is_some_and(|r| r.starts_with('/'))
            }
        }
    }
}

/// One `matches[]` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Match {
    pub path: PathMatch,
    pub method: Option<String>,
    /// (lowercased name, exact value).
    pub headers: Vec<(String, String)>,
    /// (name, exact value), as they appear in the query string.
    pub query: Vec<(String, String)>,
}

/// What a match is tested against.
pub struct ReqView<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub query: Option<&'a str>,
    pub headers: &'a http::HeaderMap,
}

impl Match {
    pub fn matches(&self, r: &ReqView) -> bool {
        if !self.path.matches(r.path) {
            return false;
        }
        if self.method.as_deref().is_some_and(|m| m != r.method) {
            return false;
        }
        for (k, v) in &self.headers {
            if !r.headers.get_all(k.as_str()).iter().any(|h| h.as_bytes() == v.as_bytes()) {
                return false;
            }
        }
        for (k, v) in &self.query {
            let found = r
                .query
                .into_iter()
                .flat_map(|q| q.split('&'))
                .filter_map(|kv| kv.split_once('=').or(Some((kv, ""))))
                .find(|(qk, _)| qk == k)
                .map(|(_, qv)| qv);
            if found != Some(v.as_str()) {
                return false;
            }
        }
        true
    }

    /// The Gateway API's precedence, larger first.
    fn rank(&self) -> (u8, usize, u8, usize, usize) {
        let (exact, len) = match &self.path {
            PathMatch::Exact(p) => (1, p.len()),
            PathMatch::Prefix(p) => (0, p.trim_end_matches('/').len()),
        };
        (exact, len, self.method.is_some() as u8, self.headers.len(), self.query.len())
    }
}

// --------------------------------------------------------------- endpoints

/// An active health check, from the Service's annotations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckSpec {
    pub path: String,
    pub host: Option<String>,
    pub interval: Duration,
    pub timeout: Duration,
    pub rise: u32,
    pub fall: u32,
}

/// One address a backend dials, kept across polls (its health, in-flight
/// count and ejection survive a refresh).
#[derive(Debug)]
pub struct Endpoint {
    pub addr: String,
    ready: AtomicBool,
    /// The active check's verdict (true when there is no check).
    check_up: AtomicBool,
    oks: AtomicU32,
    fails: AtomicU32,
    pub active: AtomicUsize,
    ejected: Mutex<Option<Instant>>,
}

impl Endpoint {
    pub fn new(addr: String, ready: bool) -> Arc<Self> {
        Arc::new(Self {
            addr,
            ready: AtomicBool::new(ready),
            check_up: AtomicBool::new(true),
            oks: AtomicU32::new(0),
            fails: AtomicU32::new(0),
            active: AtomicUsize::new(0),
            ejected: Mutex::new(None),
        })
    }

    pub fn ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    pub fn check_up(&self) -> bool {
        self.check_up.load(Ordering::Relaxed)
    }

    pub fn ejected(&self) -> bool {
        self.ejected.lock().unwrap().is_some_and(|t| t > Instant::now())
    }

    /// Ready and passing its check: it may take requests (ejection aside).
    pub fn serving(&self) -> bool {
        self.ready() && self.check_up()
    }

    /// Ready, checked and not ejected.
    pub fn healthy(&self) -> bool {
        self.serving() && !self.ejected()
    }

    /// A connection to it failed: out of rotation for [`EJECT`].
    pub fn eject(&self) {
        *self.ejected.lock().unwrap() = Some(Instant::now() + EJECT);
    }

    /// One active-check result, applied with rise/fall.
    pub fn record_check(&self, ok: bool, spec: &CheckSpec) -> Option<bool> {
        if ok {
            self.fails.store(0, Ordering::Relaxed);
            let n = self.oks.fetch_add(1, Ordering::Relaxed) + 1;
            if !self.check_up() && n >= spec.rise.max(1) {
                self.check_up.store(true, Ordering::Relaxed);
                return Some(true);
            }
        } else {
            self.oks.store(0, Ordering::Relaxed);
            let n = self.fails.fetch_add(1, Ordering::Relaxed) + 1;
            if self.check_up() && n >= spec.fall.max(1) {
                self.check_up.store(false, Ordering::Relaxed);
                return Some(false);
            }
        }
        None
    }
}

/// `round-robin` (default) or `least-conn`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Balance {
    RoundRobin,
    LeastConn,
}

/// How a backend is dialled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Proto {
    Http,
    /// TLS, verified against `backend_ca_file`, for this name (or each
    /// endpoint's IP when None).
    Https(Option<String>),
}

impl Proto {
    /// The name a TLS endpoint's certificate must carry.
    pub fn server_name(&self, addr: &str) -> Option<String> {
        match self {
            Proto::Http => None,
            Proto::Https(Some(n)) => Some(n.clone()),
            Proto::Https(None) => {
                let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
                Some(host.trim_start_matches('[').trim_end_matches(']').to_string())
            }
        }
    }
}

/// One backendRef: a pool of endpoints, or why it has none.
#[derive(Debug)]
pub struct Backend {
    /// `namespace/service:port`, or the `storm.io/backend` address.
    pub name: String,
    pub weight: u32,
    pub balance: Balance,
    pub proto: Proto,
    pub check: Option<CheckSpec>,
    /// Err: the ref does not resolve (its share of requests answers 500).
    pub endpoints: Result<Vec<Arc<Endpoint>>, String>,
    rr: AtomicUsize,
}

impl Backend {
    pub fn new(name: String, weight: u32, proto: Proto, endpoints: Result<Vec<Arc<Endpoint>>, String>) -> Self {
        Self { name, weight, balance: Balance::RoundRobin, proto, check: None, endpoints, rr: AtomicUsize::new(0) }
    }

    pub fn healthy_count(&self) -> usize {
        self.endpoints.as_ref().map(|e| e.iter().filter(|e| e.healthy()).count()).unwrap_or(0)
    }

    /// Whether it can take a request: a valid ref with an endpoint serving.
    fn usable(&self) -> bool {
        matches!(&self.endpoints, Ok(e) if e.iter().any(|e| e.serving()))
    }

    /// The endpoint for the next request, skipping `tried`. Healthy ones
    /// first; if only ejected ones are left, one of those.
    pub fn pick(&self, tried: &[String]) -> Option<Arc<Endpoint>> {
        let eps = self.endpoints.as_ref().ok()?;
        let fresh = |e: &&Arc<Endpoint>| !tried.contains(&e.addr);
        let mut cands: Vec<&Arc<Endpoint>> = eps.iter().filter(fresh).filter(|e| e.healthy()).collect();
        if cands.is_empty() {
            cands = eps.iter().filter(fresh).filter(|e| e.serving()).collect();
        }
        if cands.is_empty() {
            return None;
        }
        let n = self.rr.fetch_add(1, Ordering::Relaxed);
        let chosen = match self.balance {
            Balance::RoundRobin => cands[n % cands.len()],
            Balance::LeastConn => {
                // Fewest in flight; ties in round-robin order.
                let start = n % cands.len();
                (0..cands.len())
                    .map(|i| cands[(start + i) % cands.len()])
                    .min_by_key(|e| e.active.load(Ordering::Relaxed))
                    .expect("not empty")
            }
        };
        Some(chosen.clone())
    }
}

// ------------------------------------------------------------------- rules

/// `retry` on a rule: how many further attempts on these response codes
/// (only for requests without a body), and the pause between them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Retry {
    pub attempts: u32,
    pub codes: Vec<u16>,
    pub backoff: Duration,
}

#[derive(Debug)]
pub struct Rule {
    /// `namespace/name` of its route.
    pub route: String,
    pub index: usize,
    pub backends: Vec<Arc<Backend>>,
    /// `timeouts.request`: to the response head, retries included (504).
    pub timeout: Option<Duration>,
    /// `timeouts.backendRequest`: per attempt.
    pub backend_timeout: Option<Duration>,
    pub retry: Retry,
    /// What plain HTTP does with its requests.
    pub insecure: Insecure,
    wrr: Mutex<Vec<i64>>,
}

/// Why a rule gives no backend: the status code and the text.
pub type NoBackend = (u16, String);

impl Rule {
    pub fn new(route: String, index: usize, backends: Vec<Arc<Backend>>) -> Self {
        let n = backends.len();
        Self {
            route,
            index,
            backends,
            timeout: None,
            backend_timeout: None,
            retry: Retry::default(),
            insecure: Insecure::Router,
            wrr: Mutex::new(vec![0; n]),
        }
    }

    /// The backend for the next request: smooth weighted round-robin over
    /// the refs that can take it (an unresolved ref keeps its share, which
    /// answers 500, as the Gateway API says).
    pub fn choose(&self) -> Result<Arc<Backend>, NoBackend> {
        if self.backends.iter().all(|b| b.weight == 0) {
            return Err((500, format!("route {} rule {} has no backend with a weight", self.route, self.index)));
        }
        let eligible: Vec<usize> = (0..self.backends.len())
            .filter(|&i| self.backends[i].weight > 0 && (self.backends[i].endpoints.is_err() || self.backends[i].usable()))
            .collect();
        if eligible.is_empty() {
            return Err((503, format!("no healthy endpoint for route {} rule {}", self.route, self.index)));
        }
        let mut cur = self.wrr.lock().unwrap();
        let total: i64 = eligible.iter().map(|&i| self.backends[i].weight as i64).sum();
        let mut best = eligible[0];
        for &i in &eligible {
            cur[i] += self.backends[i].weight as i64;
            if cur[i] > cur[best] {
                best = i;
            }
        }
        cur[best] -= total;
        let b = self.backends[best].clone();
        match &b.endpoints {
            Err(why) => Err((500, format!("route {} backend {}: {why}", self.route, b.name))),
            Ok(_) => Ok(b),
        }
    }
}

#[derive(Debug, Clone)]
struct Entry {
    m: Match,
    rule: Arc<Rule>,
}

/// What the table says about a request.
pub enum Lookup {
    /// No route claims the host.
    NoHost,
    /// A route claims the host (this label), but no rule matches.
    NoMatch(String),
    /// The rule, and the hostname label it was found under.
    Rule(Arc<Rule>, String),
}

/// One route as the poll understood it, for its status and `/api/v1/routes`.
#[derive(Debug)]
pub struct RouteInfo {
    pub namespace: String,
    pub name: String,
    pub generation: i64,
    pub hostnames: Vec<String>,
    pub rules: Vec<Arc<Rule>>,
    /// Rules dropped, and why ("rule 2: filters are not supported").
    pub dropped: Vec<String>,
    /// The route's current `status.parents` as read.
    pub parents: Value,
    /// Set for an OpenShift Route (None: an HTTPRoute).
    pub os: Option<OsRoute>,
}

/// What an OpenShift Route adds to its [`RouteInfo`]. For a Route,
/// `dropped` holds what it asks for that is not done (its own certificate,
/// say), said in the `Admitted` message.
#[derive(Debug)]
pub struct OsRoute {
    /// The host it is served on (`spec.host` or the default); empty when it
    /// has none.
    pub host: String,
    /// `wildcardPolicy: Subdomain`.
    pub subdomain: bool,
    /// `spec.tls.termination`, lowercased.
    pub termination: Option<String>,
    /// Why it is not admitted: (reason, message).
    pub rejected: Option<(String, String)>,
    /// Its current `status.ingress` as read.
    pub ingress: Value,
}

/// The route table.
#[derive(Debug, Default)]
pub struct Table {
    exact: HashMap<String, Vec<Entry>>,
    wildcard: HashMap<String, Vec<Entry>>,
    /// TLS passthrough Routes: SNI host → its rule.
    passthrough: HashMap<String, Arc<Rule>>,
    pub routes: Vec<RouteInfo>,
}

impl Table {
    /// Hostnames in the table.
    pub fn hosts(&self) -> usize {
        self.exact.len() + self.wildcard.len() + self.passthrough.len()
    }

    /// Whether any Route is TLS passthrough (only then is SNI peeked).
    pub fn has_passthrough(&self) -> bool {
        !self.passthrough.is_empty()
    }

    /// The passthrough rule for a TLS client's SNI name, if any.
    pub fn passthrough(&self, sni: &str) -> Option<Arc<Rule>> {
        self.passthrough.get(&sni.to_ascii_lowercase()).cloned()
    }

    pub fn lookup(&self, host: &str, r: &ReqView) -> Lookup {
        let (entries, label) = match self.exact.get(host) {
            Some(e) => (e, host.to_string()),
            None => {
                let mut found = None;
                for (i, c) in host.char_indices() {
                    if c == '.' {
                        if let Some(e) = self.wildcard.get(&host[i + 1..]) {
                            found = Some((e, format!("*.{}", &host[i + 1..])));
                            break;
                        }
                    }
                }
                match found {
                    Some(f) => f,
                    None => return Lookup::NoHost,
                }
            }
        };
        match entries.iter().find(|e| e.m.matches(r)) {
            Some(e) => Lookup::Rule(e.rule.clone(), label),
            None => Lookup::NoMatch(label),
        }
    }

    /// The table as `GET /api/v1/routes` shows it.
    pub fn view(&self) -> Value {
        let routes: Vec<Value> = self
            .routes
            .iter()
            .map(|r| {
                let mut v = json!({
                    "kind": if r.os.is_some() { "Route" } else { "HTTPRoute" },
                    "namespace": r.namespace,
                    "name": r.name,
                    "hostnames": r.hostnames,
                    "dropped": r.dropped,
                    "rules": r.rules.iter().map(|rule| json!({
                        "index": rule.index,
                        "timeout_ms": rule.timeout.map(|d| d.as_millis() as u64),
                        "backend_timeout_ms": rule.backend_timeout.map(|d| d.as_millis() as u64),
                        "retry": {"attempts": rule.retry.attempts, "codes": rule.retry.codes, "backoff_ms": rule.retry.backoff.as_millis() as u64},
                        "backends": rule.backends.iter().map(backend_view).collect::<Vec<_>>(),
                    })).collect::<Vec<_>>(),
                });
                if let Some(os) = &r.os {
                    v["host"] = json!(os.host);
                    v["termination"] = json!(os.termination);
                    v["admitted"] = json!(os.rejected.is_none());
                    if let Some((reason, msg)) = &os.rejected {
                        v["reason"] = json!(reason);
                        v["message"] = json!(msg);
                    }
                }
                v
            })
            .collect();
        json!({ "routes": routes })
    }
}

fn backend_view(b: &Arc<Backend>) -> Value {
    let mut v = json!({
        "name": b.name,
        "weight": b.weight,
        "balance": match b.balance { Balance::RoundRobin => "round-robin", Balance::LeastConn => "least-conn" },
        "protocol": match b.proto { Proto::Http => "http", Proto::Https(_) => "https" },
        "check": b.check.as_ref().map(|c| json!({"path": c.path, "interval_ms": c.interval.as_millis() as u64,
            "timeout_ms": c.timeout.as_millis() as u64, "rise": c.rise, "fall": c.fall})),
    });
    match &b.endpoints {
        Ok(eps) => {
            v["healthy"] = json!(eps.iter().filter(|e| e.healthy()).count());
            v["endpoints"] = eps
                .iter()
                .map(|e| {
                    json!({"address": e.addr, "ready": e.ready(), "check": e.check_up(), "ejected": e.ejected(),
                        "healthy": e.healthy(), "in_flight": e.active.load(Ordering::Relaxed)})
                })
                .collect();
        }
        Err(why) => v["error"] = json!(why),
    }
    v
}

// ------------------------------------------------------------------ build

/// The objects one poll read.
#[derive(Default)]
pub struct Objects {
    pub routes: Vec<Value>,
    /// `namespace/name` → Service.
    pub services: HashMap<String, Value>,
    /// `namespace/service` → its EndpointSlices.
    pub slices: HashMap<String, Vec<Value>>,
    /// `namespace/name` → Endpoints.
    pub endpoints: HashMap<String, Value>,
    /// Why Services could not be read (every ref is then unresolved).
    pub services_error: Option<String>,
    /// namespace → the ReferenceGrants in it.
    pub grants: HashMap<String, Vec<Value>>,
    /// Why ReferenceGrants could not be read (every cross-namespace ref is
    /// then refused).
    pub grants_error: Option<String>,
    /// OpenShift Routes (`route.openshift.io/v1`).
    pub os_routes: Vec<Value>,
    /// Why Routes could not be read this time (the router then keeps the
    /// last list it read).
    pub os_routes_error: Option<String>,
    /// The domain a Route without `spec.host` is served under.
    pub route_domain: Option<String>,
}

impl Objects {
    pub fn index_services(&mut self, list: &Value) {
        for s in list["items"].as_array().into_iter().flatten() {
            self.services.insert(key_of(s), s.clone());
        }
    }

    pub fn index_slices(&mut self, list: &Value) {
        for s in list["items"].as_array().into_iter().flatten() {
            let ns = s["metadata"]["namespace"].as_str().unwrap_or("default");
            if let Some(svc) = s["metadata"]["labels"]["kubernetes.io/service-name"].as_str() {
                self.slices.entry(format!("{ns}/{svc}")).or_default().push(s.clone());
            }
        }
    }

    pub fn index_grants(&mut self, list: &Value) {
        for g in list["items"].as_array().into_iter().flatten() {
            let ns = g["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
            self.grants.entry(ns).or_default().push(g.clone());
        }
    }

    /// Whether an HTTPRoute in `from_ns` may name Service `svc` in `to_ns`.
    pub fn granted(&self, from_ns: &str, to_ns: &str, svc: &str) -> bool {
        self.grants.get(to_ns).into_iter().flatten().any(|g| {
            let from = g["spec"]["from"].as_array().into_iter().flatten().any(|f| {
                f["group"].as_str() == Some(GATEWAY_GROUP) && f["kind"].as_str() == Some("HTTPRoute") && f["namespace"].as_str() == Some(from_ns)
            });
            let to = g["spec"]["to"].as_array().into_iter().flatten().any(|t| {
                t["group"].as_str().unwrap_or("").is_empty()
                    && t["kind"].as_str() == Some("Service")
                    && t["name"].as_str().is_none_or(|n| n.is_empty() || n == svc)
            });
            from && to
        })
    }

    pub fn index_endpoints(&mut self, list: &Value) {
        for e in list["items"].as_array().into_iter().flatten() {
            self.endpoints.insert(key_of(e), e.clone());
        }
    }
}

fn key_of(o: &Value) -> String {
    format!("{}/{}", o["metadata"]["namespace"].as_str().unwrap_or("default"), o["metadata"]["name"].as_str().unwrap_or(""))
}

/// Endpoints seen before, so their state carries across polls: keyed by
/// backend, address, protocol and check.
#[derive(Default)]
pub struct Known {
    eps: HashMap<String, Weak<Endpoint>>,
}

/// A new endpoint that needs an active check started.
pub struct NewCheck {
    pub endpoint: Weak<Endpoint>,
    pub spec: CheckSpec,
    pub proto: Proto,
}

impl Known {
    fn get(&mut self, key: String, addr: &str, ready: bool, fresh: &mut Vec<Arc<Endpoint>>) -> (Arc<Endpoint>, bool) {
        if let Some(e) = self.eps.get(&key).and_then(Weak::upgrade) {
            e.ready.store(ready, Ordering::Relaxed);
            return (e, false);
        }
        let e = Endpoint::new(addr.to_string(), ready);
        self.eps.insert(key, Arc::downgrade(&e));
        fresh.push(e.clone());
        (e, true)
    }

    fn prune(&mut self) {
        self.eps.retain(|_, w| w.strong_count() > 0);
    }
}

/// Build the table. Returns it and the active checks to start.
pub fn build(objs: &Objects, known: &mut Known) -> (Table, Vec<NewCheck>) {
    let mut table = Table::default();
    let mut checks = Vec::new();
    // Holds new endpoints alive until the table owns them.
    let mut fresh = Vec::new();
    let mut entries: Vec<Ordered> = Vec::new();

    for r in &objs.routes {
        let ns = r["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
        let name = r["metadata"]["name"].as_str().unwrap_or("").to_string();
        let route_key = format!("{ns}/{name}");
        let created = r["metadata"]["creationTimestamp"].as_str().unwrap_or("").to_string();
        let hostnames: Vec<String> = r["spec"]["hostnames"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|h| h.as_str().map(str::to_ascii_lowercase))
            .collect();
        let mut info = RouteInfo {
            namespace: ns.clone(),
            name: name.clone(),
            generation: r["metadata"]["generation"].as_i64().unwrap_or(0),
            hostnames: hostnames.clone(),
            rules: Vec::new(),
            dropped: Vec::new(),
            parents: r["status"]["parents"].clone(),
            os: None,
        };
        let proto = match proto_of(r) {
            Ok(p) => p,
            Err(e) => {
                info.dropped.push(e);
                table.routes.push(info);
                continue;
            }
        };

        let mut rules: Vec<(Vec<Match>, Rule)> = Vec::new();
        if let Some(addr) = r["metadata"]["annotations"]["storm.io/backend"].as_str() {
            let (ep, _) = known.get(format!("{route_key}|static|{addr}|{proto:?}"), addr, true, &mut fresh);
            let b = Backend::new(addr.to_string(), 1, proto.clone(), Ok(vec![ep]));
            rules.push((vec![prefix_root()], Rule::new(route_key.clone(), 0, vec![Arc::new(b)])));
        } else {
            let spec_rules = r["spec"]["rules"].as_array().cloned().unwrap_or_else(|| vec![json!({})]);
            for (i, rule) in spec_rules.iter().enumerate() {
                match parse_rule(rule) {
                    Ok((matches, timeout, backend_timeout, retry)) => {
                        let mut backends = Vec::new();
                        for bref in rule["backendRefs"].as_array().into_iter().flatten() {
                            let b = resolve(objs, known, &mut fresh, &mut checks, &ns, &route_key, bref, &proto);
                            backends.push(Arc::new(b));
                        }
                        let mut rl = Rule::new(route_key.clone(), i, backends);
                        rl.timeout = timeout;
                        rl.backend_timeout = backend_timeout;
                        rl.retry = retry;
                        rules.push((matches, rl));
                    }
                    Err(e) => info.dropped.push(format!("rule {i}: {e}")),
                }
            }
        }

        for (mi, (matches, rule)) in rules.into_iter().enumerate() {
            let rule = Arc::new(rule);
            info.rules.push(rule.clone());
            for (j, m) in matches.into_iter().enumerate() {
                for h in &hostnames {
                    entries.push((h.clone(), m.clone(), rule.clone(), (created.clone(), route_key.clone(), mi, j)));
                }
            }
        }
        table.routes.push(info);
    }

    // OpenShift Routes, oldest first: the oldest claims a host.
    let mut os: Vec<&Value> = objs.os_routes.iter().collect();
    os.sort_by_key(|r| (r["metadata"]["creationTimestamp"].as_str().unwrap_or("").to_string(), key_of(r)));
    let mut claims = Claims::default();
    for r in os {
        let mut ctx = Ctx { objs, known: &mut *known, fresh: &mut fresh, checks: &mut checks };
        let (info, served) = os_route(&mut ctx, r, &mut claims);
        if let Some(s) = served {
            if s.passthrough {
                table.passthrough.insert(s.hostname, s.rule);
            } else {
                let created = r["metadata"]["creationTimestamp"].as_str().unwrap_or("").to_string();
                entries.push((s.hostname, s.m, s.rule, (created, format!("{}/{}", info.namespace, info.name), 0, 0)));
            }
        }
        table.routes.push(info);
    }

    // Precedence: rank descending, then oldest route, then name, rule, match.
    entries.sort_by(|a, b| b.1.rank().cmp(&a.1.rank()).then_with(|| a.3.cmp(&b.3)));
    for (host, m, rule, _) in entries {
        let e = Entry { m, rule };
        match host.strip_prefix("*.") {
            Some(suffix) => table.wildcard.entry(suffix.to_string()).or_default().push(e),
            None => table.exact.entry(host).or_default().push(e),
        }
    }
    drop(fresh);
    known.prune();
    (table, checks)
}

/// (hostname, match, rule, tie-break: created, route, rule, match).
type Ordered = (String, Match, Arc<Rule>, (String, String, usize, usize));

fn prefix_root() -> Match {
    Match { path: PathMatch::Prefix("/".into()), method: None, headers: vec![], query: vec![] }
}

/// `storm.io/backend-protocol` (and `-server-name`) on the route.
pub fn proto_of(route: &Value) -> Result<Proto, String> {
    let a = &route["metadata"]["annotations"];
    match a["storm.io/backend-protocol"].as_str().map(str::to_ascii_lowercase).as_deref() {
        None | Some("http") => Ok(Proto::Http),
        Some("https") => Ok(Proto::Https(a["storm.io/backend-server-name"].as_str().map(str::to_string))),
        Some(p) => Err(format!("storm.io/backend-protocol {p:?}: want http or https")),
    }
}

type ParsedRule = (Vec<Match>, Option<Duration>, Option<Duration>, Retry);

/// A rule's matches, timeouts and retry; Err for what this router cannot do.
fn parse_rule(rule: &Value) -> Result<ParsedRule, String> {
    if rule["filters"].as_array().is_some_and(|f| !f.is_empty()) {
        return Err("filters are not supported".into());
    }
    if !rule["sessionPersistence"].is_null() {
        return Err("sessionPersistence is not supported".into());
    }
    for b in rule["backendRefs"].as_array().into_iter().flatten() {
        if b["filters"].as_array().is_some_and(|f| !f.is_empty()) {
            return Err("backendRef filters are not supported".into());
        }
    }
    let mut matches = Vec::new();
    for m in rule["matches"].as_array().into_iter().flatten() {
        let path = match (m["path"]["type"].as_str().unwrap_or("PathPrefix"), m["path"]["value"].as_str().unwrap_or("/")) {
            ("PathPrefix", v) => PathMatch::Prefix(v.to_string()),
            ("Exact", v) => PathMatch::Exact(v.to_string()),
            (t, _) => return Err(format!("path match type {t} is not supported")),
        };
        let mut headers = Vec::new();
        for h in m["headers"].as_array().into_iter().flatten() {
            if h["type"].as_str().unwrap_or("Exact") != "Exact" {
                return Err(format!("header match type {} is not supported", h["type"]));
            }
            let n = h["name"].as_str().ok_or("a header match without a name")?;
            headers.push((n.to_ascii_lowercase(), h["value"].as_str().unwrap_or("").to_string()));
        }
        let mut query = Vec::new();
        for q in m["queryParams"].as_array().into_iter().flatten() {
            if q["type"].as_str().unwrap_or("Exact") != "Exact" {
                return Err(format!("query match type {} is not supported", q["type"]));
            }
            let n = q["name"].as_str().ok_or("a query match without a name")?;
            query.push((n.to_string(), q["value"].as_str().unwrap_or("").to_string()));
        }
        matches.push(Match { path, method: m["method"].as_str().map(str::to_string), headers, query });
    }
    if matches.is_empty() {
        matches.push(prefix_root());
    }
    let dur = |v: &Value, what: &str| -> Result<Option<Duration>, String> {
        match v.as_str() {
            None => Ok(None),
            Some(s) => match parse_duration(s) {
                Some(d) if d.is_zero() => Ok(None),
                Some(d) => Ok(Some(d)),
                None => Err(format!("{what} {s:?} is not a duration")),
            },
        }
    };
    let timeout = dur(&rule["timeouts"]["request"], "timeouts.request")?;
    let backend_timeout = dur(&rule["timeouts"]["backendRequest"], "timeouts.backendRequest")?;
    let r = &rule["retry"];
    let retry = Retry {
        attempts: r["attempts"].as_u64().unwrap_or(0).min(10) as u32,
        codes: r["codes"].as_array().into_iter().flatten().filter_map(|c| c.as_u64()).map(|c| c as u16).collect(),
        backoff: dur(&r["backoff"], "retry.backoff")?.unwrap_or_default(),
    };
    Ok((matches, timeout, backend_timeout, retry))
}

/// One backendRef → its Backend (an Err pool when it does not resolve).
#[allow(clippy::too_many_arguments)]
fn resolve(
    objs: &Objects,
    known: &mut Known,
    fresh: &mut Vec<Arc<Endpoint>>,
    checks: &mut Vec<NewCheck>,
    route_ns: &str,
    route_key: &str,
    bref: &Value,
    proto: &Proto,
) -> Backend {
    let weight = bref["weight"].as_u64().unwrap_or(1).min(1_000_000) as u32;
    let svc = bref["name"].as_str().unwrap_or("");
    let ns = bref["namespace"].as_str().unwrap_or(route_ns);
    let port = bref["port"].as_u64().unwrap_or(80);
    let name = format!("{ns}/{svc}:{port}");
    let fail = |why: String| Backend::new(name.clone(), weight, proto.clone(), Err(why));
    let group = bref["group"].as_str().unwrap_or("");
    let kind = bref["kind"].as_str().unwrap_or("Service");
    if !group.is_empty() || kind != "Service" {
        return fail(format!("InvalidKind: {group}/{kind} (only core Services)"));
    }
    if ns != route_ns {
        if let Some(e) = &objs.grants_error {
            return fail(format!("RefNotPermitted: ReferenceGrants could not be read: {e}"));
        }
        if !objs.granted(route_ns, ns, svc) {
            return fail(format!("RefNotPermitted: no ReferenceGrant in {ns} lets HTTPRoutes of {route_ns} reach Service {svc}"));
        }
    }
    if let Some(e) = &objs.services_error {
        return fail(format!("BackendNotFound: Services could not be read: {e}"));
    }
    let Some(s) = objs.services.get(&format!("{ns}/{svc}")) else {
        return fail(format!("BackendNotFound: Service {ns}/{svc} does not exist"));
    };
    let Some(sp) = s["spec"]["ports"].as_array().into_iter().flatten().find(|p| p["port"].as_u64() == Some(port)) else {
        return fail(format!("BackendNotFound: Service {ns}/{svc} has no port {port}"));
    };
    let port_name = sp["name"].as_str().unwrap_or("");
    let annotations = &s["metadata"]["annotations"];
    let balance = match annotations["storm.io/balance"].as_str() {
        None | Some("round-robin") => Balance::RoundRobin,
        Some("least-conn") => Balance::LeastConn,
        Some(b) => return fail(format!("UnsupportedValue: storm.io/balance {b:?}: want round-robin or least-conn")),
    };
    let check = match check_spec(annotations) {
        Ok(c) => c,
        Err(e) => return fail(format!("UnsupportedValue: {e}")),
    };

    // (address, ready) from the slices, else the Endpoints, else the clusterIP.
    let mut addrs: Vec<(String, bool)> = Vec::new();
    if let Some(slices) = objs.slices.get(&format!("{ns}/{svc}")) {
        for sl in slices {
            if sl["addressType"].as_str() == Some("FQDN") {
                continue;
            }
            let Some(p) = sl["ports"].as_array().into_iter().flatten().find(|p| p["name"].as_str().unwrap_or("") == port_name) else {
                continue;
            };
            let Some(pn) = p["port"].as_u64() else { continue };
            for e in sl["endpoints"].as_array().into_iter().flatten() {
                let ready = e["conditions"]["ready"].as_bool() != Some(false);
                for a in e["addresses"].as_array().into_iter().flatten().filter_map(Value::as_str) {
                    addrs.push((join(a, pn), ready));
                }
            }
        }
    } else if let Some(ep) = objs.endpoints.get(&format!("{ns}/{svc}")) {
        for sub in ep["subsets"].as_array().into_iter().flatten() {
            let Some(p) = sub["ports"].as_array().into_iter().flatten().find(|p| p["name"].as_str().unwrap_or("") == port_name) else {
                continue;
            };
            let Some(pn) = p["port"].as_u64() else { continue };
            for (field, ready) in [("addresses", true), ("notReadyAddresses", false)] {
                for a in sub[field].as_array().into_iter().flatten().filter_map(|a| a["ip"].as_str()) {
                    addrs.push((join(a, pn), ready));
                }
            }
        }
    } else if let Some(ip) = s["spec"]["clusterIP"].as_str().filter(|ip| !ip.is_empty() && *ip != "None") {
        addrs.push((join(ip, port), true));
    }
    addrs.sort();
    addrs.dedup_by(|a, b| a.0 == b.0);

    let mut eps = Vec::new();
    for (addr, ready) in addrs {
        let key = format!("{route_key}|{name}|{addr}|{proto:?}|{check:?}");
        let (ep, new) = known.get(key, &addr, ready, fresh);
        if new {
            if let Some(c) = &check {
                checks.push(NewCheck { endpoint: Arc::downgrade(&ep), spec: c.clone(), proto: proto.clone() });
            }
        }
        eps.push(ep);
    }
    let mut b = Backend::new(name, weight, proto.clone(), Ok(eps));
    b.balance = balance;
    b.check = check;
    b
}

// ------------------------------------------------------- OpenShift Routes

/// What one build carries while it resolves backends.
struct Ctx<'a> {
    objs: &'a Objects,
    known: &'a mut Known,
    fresh: &'a mut Vec<Arc<Endpoint>>,
    checks: &'a mut Vec<NewCheck>,
}

/// Which Route holds each host (its namespace) and each host + path.
#[derive(Default)]
struct Claims {
    hosts: HashMap<String, (String, String)>,
    paths: HashMap<(String, String), String>,
}

impl Claims {
    fn claim(&mut self, host: &str, path: &str, ns: &str, key: &str) -> Result<(), Reject> {
        if let Some((owner_ns, owner)) = self.hosts.get(host) {
            if owner_ns != ns {
                return Err(("HostAlreadyClaimed".into(), format!("host {host} is held by the older route {owner} in another namespace")));
            }
        }
        if let Some(owner) = self.paths.get(&(host.to_string(), path.to_string())) {
            return Err(("HostAlreadyClaimed".into(), format!("host {host} path {path:?} is held by the older route {owner}")));
        }
        self.hosts.entry(host.to_string()).or_insert_with(|| (ns.to_string(), key.to_string()));
        self.paths.insert((host.to_string(), path.to_string()), key.to_string());
        Ok(())
    }
}

/// Why a Route is not admitted: (reason, message).
type Reject = (String, String);

/// What an admitted Route serves.
struct Served {
    /// Its host, or `*.<suffix>` for `wildcardPolicy: Subdomain`.
    hostname: String,
    m: Match,
    rule: Arc<Rule>,
    passthrough: bool,
}

fn invalid(msg: String) -> Reject {
    ("ExtendedValidationFailed".into(), msg)
}

/// One OpenShift Route: what it says (for status and the API), and what it
/// serves when admitted.
fn os_route(ctx: &mut Ctx, r: &Value, claims: &mut Claims) -> (RouteInfo, Option<Served>) {
    let ns = r["metadata"]["namespace"].as_str().unwrap_or("default").to_string();
    let name = r["metadata"]["name"].as_str().unwrap_or("").to_string();
    let spec = &r["spec"];
    let host = match spec["host"].as_str().map(str::trim).filter(|h| !h.is_empty()) {
        Some(h) => Some(h.to_ascii_lowercase()),
        None => ctx.objs.route_domain.as_ref().map(|d| format!("{name}-{ns}.{}", d.trim_start_matches('.'))),
    };
    let termination = spec["tls"]["termination"].as_str().map(str::to_ascii_lowercase);
    let mut info = RouteInfo {
        namespace: ns.clone(),
        name: name.clone(),
        generation: r["metadata"]["generation"].as_i64().unwrap_or(0),
        hostnames: Vec::new(),
        rules: Vec::new(),
        dropped: Vec::new(),
        parents: Value::Null,
        os: Some(OsRoute {
            host: host.clone().unwrap_or_default(),
            subdomain: spec["wildcardPolicy"].as_str() == Some("Subdomain"),
            termination: termination.clone(),
            rejected: None,
            ingress: r["status"]["ingress"].clone(),
        }),
    };
    let served = match host {
        None => Err((
            "NoHost".into(),
            "spec.host is empty and the router has no domain to default it under ([router] route_domain, or a wildcard certificate in [router.tls])".into(),
        )),
        Some(h) => os_serve(ctx, r, &ns, &h, termination.as_deref(), claims, &mut info.dropped),
    };
    match served {
        Ok(s) => {
            info.hostnames = vec![s.hostname.clone()];
            info.rules.push(s.rule.clone());
            (info, Some(s))
        }
        Err(why) => {
            info.os.as_mut().expect("a Route").rejected = Some(why);
            (info, None)
        }
    }
}

fn os_serve(
    ctx: &mut Ctx,
    r: &Value,
    ns: &str,
    host: &str,
    termination: Option<&str>,
    claims: &mut Claims,
    notes: &mut Vec<String>,
) -> Result<Served, Reject> {
    let spec = &r["spec"];
    let key = format!("{ns}/{}", r["metadata"]["name"].as_str().unwrap_or(""));
    let annotations = &r["metadata"]["annotations"];
    let path = spec["path"].as_str().unwrap_or("").to_string();
    if !path.is_empty() && !path.starts_with('/') {
        return Err(invalid(format!("spec.path {path:?} must start with /")));
    }
    if bad_host(host) {
        return Err(invalid(format!("host {host:?} is not a DNS name")));
    }
    let passthrough = termination == Some("passthrough");
    let reencrypt = termination == Some("reencrypt");
    if !matches!(termination, None | Some("edge") | Some("reencrypt") | Some("passthrough")) {
        return Err(invalid(format!("spec.tls.termination {:?}: want edge, reencrypt or passthrough", termination.unwrap_or(""))));
    }
    if passthrough && !path.is_empty() {
        return Err(invalid("a passthrough route cannot have a path: the router does not see the request".into()));
    }
    let subdomain = match spec["wildcardPolicy"].as_str() {
        None | Some("") | Some("None") => false,
        Some("Subdomain") if passthrough => return Err(invalid("wildcardPolicy Subdomain is not supported with passthrough".into())),
        Some("Subdomain") => true,
        Some(w) => return Err(invalid(format!("wildcardPolicy {w:?}: want None or Subdomain"))),
    };
    let insecure = match (termination, spec["tls"]["insecureEdgeTerminationPolicy"].as_str().unwrap_or("")) {
        (None, _) => Insecure::Router,
        (_, "" | "None") => Insecure::Deny,
        (_, "Redirect") => Insecure::Redirect,
        (Some("edge" | "reencrypt"), "Allow") => Insecure::Allow,
        (Some(t), p) => return Err(invalid(format!("insecureEdgeTerminationPolicy {p:?} is not valid for {t}"))),
    };
    let own = |k: &str| spec["tls"][k].as_str().is_some_and(|v| !v.trim().is_empty());
    if own("certificate") || own("key") {
        notes.push("spec.tls.certificate is not used: the router serves its own certificate".into());
    }
    if reencrypt && own("destinationCACertificate") {
        notes.push("spec.tls.destinationCACertificate is not used: backends are verified against [router] backend_ca_file".into());
    }
    let timeout = match annotations["haproxy.router.openshift.io/timeout"].as_str() {
        None => None,
        Some(s) => match parse_duration(s.trim()) {
            Some(d) if d.is_zero() => None,
            Some(d) => Some(d),
            None => return Err(invalid(format!("haproxy.router.openshift.io/timeout {s:?} is not a duration"))),
        },
    };
    let hostname = if subdomain {
        match host.split_once('.') {
            Some((_, rest)) if rest.contains('.') => format!("*.{rest}"),
            _ => return Err(invalid(format!("wildcardPolicy Subdomain needs a host below a domain, not {host:?}"))),
        }
    } else {
        host.to_string()
    };
    claims.claim(&hostname, &path, ns, &key)?;

    // The protocol to a backend: TLS for reencrypt (named for the Service,
    // as OpenShift's service-serving certificates are), else as annotated.
    let server_name = annotations["storm.io/backend-server-name"].as_str().map(str::to_string);
    let proto_for = |svc: Option<&str>| -> Result<Proto, Reject> {
        if reencrypt {
            return Ok(Proto::Https(server_name.clone().or_else(|| svc.map(|s| format!("{s}.{ns}.svc")))));
        }
        proto_of(r).map_err(invalid)
    };
    let mut backends = Vec::new();
    if let Some(addr) = annotations["storm.io/backend"].as_str() {
        let proto = proto_for(None)?;
        let (ep, _) = ctx.known.get(format!("{key}|route|static|{addr}|{proto:?}"), addr, true, ctx.fresh);
        backends.push(Arc::new(Backend::new(addr.to_string(), 1, proto, Ok(vec![ep]))));
    } else {
        let mut targets = vec![&spec["to"]];
        targets.extend(spec["alternateBackends"].as_array().into_iter().flatten());
        for t in targets {
            let svc = t["name"].as_str().unwrap_or("");
            let weight = t["weight"].as_u64().unwrap_or(100).min(256);
            let proto = proto_for(Some(svc))?;
            let kind = t["kind"].as_str().unwrap_or("Service");
            let b = if kind != "Service" {
                Backend::new(format!("{ns}/{svc}"), weight as u32, proto, Err(format!("InvalidKind: {kind} (only Services)")))
            } else {
                match route_port(ctx.objs, ns, svc, &spec["port"]["targetPort"]) {
                    Ok(port) => {
                        let bref = json!({"name": svc, "port": port, "weight": weight});
                        resolve(ctx.objs, ctx.known, ctx.fresh, ctx.checks, ns, &format!("{key}|route"), &bref, &proto)
                    }
                    Err(e) => Backend::new(format!("{ns}/{svc}"), weight as u32, proto, Err(e)),
                }
            };
            backends.push(Arc::new(b));
        }
    }
    let mut rule = Rule::new(key, 0, backends);
    rule.timeout = timeout;
    rule.insecure = insecure;
    let p = if path.is_empty() { "/".to_string() } else { path };
    Ok(Served { hostname, m: Match { path: PathMatch::Prefix(p), method: None, headers: vec![], query: vec![] }, rule: Arc::new(rule), passthrough })
}

/// A host no client could ask for: not a DNS name.
fn bad_host(host: &str) -> bool {
    host.len() > 253
        || host.split('.').any(|l| l.is_empty() || l.len() > 63)
        || !host.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
}

/// The Service port a Route's `spec.port.targetPort` names: a port name, a
/// target port (name or number), or a port number; unset, the first port.
/// A Service that does not exist resolves later, saying so.
fn route_port(objs: &Objects, ns: &str, svc: &str, target: &Value) -> Result<u64, String> {
    let Some(s) = objs.services.get(&format!("{ns}/{svc}")) else { return Ok(80) };
    let ports: Vec<&Value> = s["spec"]["ports"].as_array().into_iter().flatten().collect();
    let target_num = |p: &Value| p["targetPort"].as_u64().or_else(|| p["targetPort"].as_str().and_then(|t| t.parse().ok())).or(p["port"].as_u64());
    let found = match target {
        Value::Null => ports.first().copied(),
        Value::String(t) if t.is_empty() => ports.first().copied(),
        Value::String(t) => match t.parse::<u64>() {
            Ok(n) => ports.iter().find(|p| target_num(p) == Some(n)).or_else(|| ports.iter().find(|p| p["port"].as_u64() == Some(n))).copied(),
            Err(_) => ports
                .iter()
                .find(|p| p["name"].as_str() == Some(t))
                .or_else(|| ports.iter().find(|p| p["targetPort"].as_str() == Some(t)))
                .copied(),
        },
        Value::Number(n) => {
            let n = n.as_u64();
            ports.iter().find(|p| target_num(p) == n).or_else(|| ports.iter().find(|p| p["port"].as_u64() == n)).copied()
        }
        _ => None,
    };
    found
        .and_then(|p| p["port"].as_u64())
        .ok_or_else(|| format!("BackendNotFound: Service {ns}/{svc} has no port for targetPort {target}"))
}

/// The Route's new `status.ingress`, or None when this router's entry is
/// already what it says. Others' entries are kept as they are.
pub fn route_ingress(r: &RouteInfo, now: &str) -> Option<Value> {
    let os = r.os.as_ref()?;
    let old = os.ingress.as_array().cloned().unwrap_or_default();
    let mine = old.iter().find(|i| i["routerName"].as_str() == Some(ROUTER_NAME));
    let (status, reason, message) = match &os.rejected {
        None => ("True", None, Some(r.dropped.join("; ")).filter(|m| !m.is_empty())),
        Some((reason, msg)) => ("False", Some(reason.clone()), Some(msg.clone())),
    };
    let since = mine
        .and_then(|m| m["conditions"].as_array())
        .into_iter()
        .flatten()
        .find(|c| c["type"].as_str() == Some("Admitted") && c["status"].as_str() == Some(status))
        .and_then(|c| c["lastTransitionTime"].as_str())
        .unwrap_or(now);
    let mut cond = json!({"type": "Admitted", "status": status, "lastTransitionTime": since});
    if let Some(x) = reason {
        cond["reason"] = json!(x);
    }
    if let Some(x) = message {
        cond["message"] = json!(x);
    }
    let entry = json!({
        "host": os.host,
        "routerName": ROUTER_NAME,
        "wildcardPolicy": if os.subdomain { "Subdomain" } else { "None" },
        "conditions": [cond],
    });
    if mine == Some(&entry) {
        return None;
    }
    let mut ingress: Vec<Value> = old.into_iter().filter(|i| i["routerName"].as_str() != Some(ROUTER_NAME)).collect();
    ingress.push(entry);
    Some(Value::Array(ingress))
}

/// What is wrong with a route, for the log ("" when nothing): an HTTPRoute's
/// failing conditions, a Route's admission and unresolved backends.
pub fn problems(r: &RouteInfo) -> String {
    let mut out: Vec<String> = Vec::new();
    if let Some((reason, msg)) = r.os.as_ref().and_then(|o| o.rejected.as_ref()) {
        out.push(format!("Admitted False {reason}: {msg}"));
    }
    for (ty, ok, reason, msg) in conditions(r) {
        let bad = (ty == "PartiallyInvalid") == ok && ty != "storm.io/BackendsHealthy";
        if bad && (r.os.is_none() || ty == "ResolvedRefs") {
            out.push(format!("{ty} {reason}: {msg}"));
        }
    }
    if r.os.as_ref().is_some_and(|o| o.rejected.is_none()) && !r.dropped.is_empty() {
        out.push(r.dropped.join("; "));
    }
    out.join("; ")
}

/// Whether a route (without `storm.io/backend`) has a backendRef in a
/// namespace other than its own: only then are ReferenceGrants listed.
pub fn has_cross_namespace_ref(routes: &[Value]) -> bool {
    routes.iter().any(|r| {
        let ns = r["metadata"]["namespace"].as_str().unwrap_or("default");
        r["metadata"]["annotations"]["storm.io/backend"].is_null()
            && r["spec"]["rules"].as_array().into_iter().flatten().any(|rule| {
                rule["backendRefs"].as_array().into_iter().flatten().any(|b| b["namespace"].as_str().is_some_and(|n| n != ns))
            })
    })
}

/// `ip:port`, bracketing an IPv6 address.
pub fn join(ip: &str, port: u64) -> String {
    if ip.contains(':') {
        format!("[{ip}]:{port}")
    } else {
        format!("{ip}:{port}")
    }
}

/// The active check a Service asks for, if any.
fn check_spec(a: &Value) -> Result<Option<CheckSpec>, String> {
    let Some(path) = a["storm.io/health-check-path"].as_str() else { return Ok(None) };
    if !path.starts_with('/') {
        return Err(format!("storm.io/health-check-path {path:?} must start with /"));
    }
    let dur = |k: &str, d: Duration| -> Result<Duration, String> {
        match a[k].as_str() {
            None => Ok(d),
            Some(s) => parse_duration(s).filter(|d| !d.is_zero()).ok_or(format!("{k} {s:?} is not a duration")),
        }
    };
    let num = |k: &str, d: u32| -> Result<u32, String> {
        match a[k].as_str() {
            None => Ok(d),
            Some(s) => s.parse().ok().filter(|n| *n > 0).ok_or(format!("{k} {s:?} is not a positive number")),
        }
    };
    let interval = dur("storm.io/health-check-interval", Duration::from_secs(5))?;
    Ok(Some(CheckSpec {
        path: path.to_string(),
        host: a["storm.io/health-check-host"].as_str().map(str::to_string),
        interval,
        timeout: dur("storm.io/health-check-timeout", interval.min(Duration::from_secs(2)))?,
        rise: num("storm.io/health-check-rise", 2)?,
        fall: num("storm.io/health-check-fall", 3)?,
    }))
}

/// A Gateway API duration (`1h`, `30s`, `500ms`, `1m30s`); a bare number is
/// seconds.
pub fn parse_duration(s: &str) -> Option<Duration> {
    if let Ok(n) = s.parse::<u64>() {
        return Some(Duration::from_secs(n));
    }
    let mut total = Duration::ZERO;
    let mut rest = s;
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            return None;
        }
        let n: u64 = rest[..digits].parse().ok()?;
        rest = &rest[digits..];
        let (unit, len) = if rest.starts_with("ms") {
            (Duration::from_millis(1), 2)
        } else if rest.starts_with('h') {
            (Duration::from_secs(3600), 1)
        } else if rest.starts_with('m') {
            (Duration::from_secs(60), 1)
        } else if rest.starts_with('s') {
            (Duration::from_secs(1), 1)
        } else {
            return None;
        };
        total += unit.saturating_mul(u32::try_from(n).ok()?);
        rest = &rest[len..];
    }
    Some(total)
}

// ----------------------------------------------------------------- status

/// Our entry in a route's `status.parents`: no route here names a Gateway,
/// so the parent is the router itself.
pub fn parent_ref() -> Value {
    json!({"group": "storm.io", "kind": "Router", "name": "stormlb"})
}

/// The conditions this router reports for a route: (type, status, reason,
/// message).
pub fn conditions(r: &RouteInfo) -> Vec<(&'static str, bool, String, String)> {
    let mut out = Vec::new();
    if r.rules.is_empty() && !r.dropped.is_empty() {
        out.push(("Accepted", false, "UnsupportedValue".into(), r.dropped.join("; ")));
    } else {
        out.push(("Accepted", true, "Accepted".into(), "served by every node's stormlb router".into()));
    }
    if !r.dropped.is_empty() && !r.rules.is_empty() {
        out.push(("PartiallyInvalid", true, "UnsupportedValue".into(), r.dropped.join("; ")));
    }
    let backends: Vec<&Arc<Backend>> = r.rules.iter().flat_map(|rl| rl.backends.iter()).collect();
    let bad: Vec<String> = backends
        .iter()
        .filter_map(|b| b.endpoints.as_ref().err().map(|e| format!("{}: {e}", b.name)))
        .collect();
    if bad.is_empty() {
        out.push(("ResolvedRefs", true, "ResolvedRefs".into(), "every backendRef resolves".into()));
    } else {
        let reason = bad[0].split(": ").nth(1).and_then(|s| s.split(':').next()).unwrap_or("BackendNotFound").to_string();
        out.push(("ResolvedRefs", false, reason, bad.join("; ")));
    }
    let mut parts = Vec::new();
    let mut all = true;
    for b in &backends {
        if let Ok(eps) = &b.endpoints {
            let h = b.healthy_count();
            all &= h > 0;
            parts.push(format!("{} {h}/{} healthy", b.name, eps.len()));
        }
    }
    parts.dedup();
    let reason = if all { "Healthy" } else { "NoHealthyEndpoints" };
    out.push(("storm.io/BackendsHealthy", all, reason.into(), parts.join("; ")));
    out
}

/// The route's new `status.parents`, or None when ours is already what it
/// says (so nothing is written). Others' entries are kept as they are.
pub fn status_parents(r: &RouteInfo, now: &str) -> Option<Value> {
    let old = r.parents.as_array().cloned().unwrap_or_default();
    let mine = old.iter().find(|p| p["controllerName"].as_str() == Some(CONTROLLER));
    let prior: BTreeMap<String, &Value> = mine
        .and_then(|m| m["conditions"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|c| c["type"].as_str().map(|t| (t.to_string(), c)))
        .collect();
    let conds: Vec<Value> = conditions(r)
        .into_iter()
        .map(|(t, ok, reason, message)| {
            let status = if ok { "True" } else { "False" };
            let since = prior
                .get(t)
                .filter(|c| c["status"].as_str() == Some(status))
                .and_then(|c| c["lastTransitionTime"].as_str())
                .unwrap_or(now);
            json!({"type": t, "status": status, "reason": reason, "message": message,
                "observedGeneration": r.generation, "lastTransitionTime": since})
        })
        .collect();
    let entry = json!({"parentRef": parent_ref(), "controllerName": CONTROLLER, "conditions": conds});
    if mine == Some(&entry) {
        return None;
    }
    let mut parents: Vec<Value> = old.into_iter().filter(|p| p["controllerName"].as_str() != Some(CONTROLLER)).collect();
    parents.push(entry);
    Some(Value::Array(parents))
}

/// Now, as RFC 3339 UTC to the second.
pub fn now_rfc3339() -> String {
    let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    rfc3339(secs)
}

fn rfc3339(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // Civil from days (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z", rem / 3600, rem % 3600 / 60, rem % 60)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req<'a>(method: &'a str, path: &'a str, query: Option<&'a str>, h: &'a http::HeaderMap) -> ReqView<'a> {
        ReqView { method, path, query, headers: h }
    }

    #[test]
    fn path_prefixes_match_whole_elements() {
        let p = PathMatch::Prefix("/api/".into());
        assert!(p.matches("/api") && p.matches("/api/") && p.matches("/api/x"));
        assert!(!p.matches("/apix") && !p.matches("/"));
        assert!(PathMatch::Prefix("/".into()).matches("/anything"));
        assert!(PathMatch::Exact("/a".into()).matches("/a") && !PathMatch::Exact("/a".into()).matches("/a/"));
    }

    #[test]
    fn durations_parse_as_the_gateway_api_writes_them() {
        assert_eq!(parse_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_duration("1m30s"), Some(Duration::from_secs(90)));
        assert_eq!(parse_duration("500ms"), Some(Duration::from_millis(500)));
        assert_eq!(parse_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_duration("2"), Some(Duration::from_secs(2)));
        assert_eq!(parse_duration("2x"), None);
        assert_eq!(parse_duration(""), None);
    }

    #[test]
    fn rfc3339_is_civil_time() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339(1_795_000_000), "2026-11-18T11:06:40Z");
        assert_eq!(rfc3339(951_782_400), "2000-02-29T00:00:00Z");
    }

    fn storefront() -> Objects {
        let mut o = Objects::default();
        o.routes.push(json!({
            "metadata": {"namespace": "storefront", "name": "storefront", "generation": 3, "creationTimestamp": "2026-11-18T00:00:00Z"},
            "spec": {"hostnames": ["storefront.storm1.g8.lo"], "rules": [
                {"matches": [{"path": {"type": "PathPrefix", "value": "/"}}],
                 "backendRefs": [{"kind": "Service", "name": "web", "port": 8080, "weight": 100}], "timeouts": {"request": "30s"}},
                {"matches": [{"path": {"type": "PathPrefix", "value": "/api/"}}],
                 "backendRefs": [{"kind": "Service", "name": "api", "port": 8081, "weight": 100}]}
            ]}
        }));
        for (svc, port, ips) in [("web", 8080, ["10.0.0.1", "10.0.0.2", "10.0.0.3"]), ("api", 8081, ["10.0.1.1", "10.0.1.2", "10.0.1.3"])] {
            o.services.insert(
                format!("storefront/{svc}"),
                json!({"metadata": {"namespace": "storefront", "name": svc},
                    "spec": {"clusterIP": "10.96.0.9", "ports": [{"name": "http", "port": port, "targetPort": "http"}]}}),
            );
            o.slices.insert(
                format!("storefront/{svc}"),
                vec![json!({"addressType": "IPv4", "ports": [{"name": "http", "port": 9000 + port % 10}],
                    "endpoints": ips.iter().enumerate().map(|(i, ip)| json!({"addresses": [ip], "conditions": {"ready": i != 2}})).collect::<Vec<_>>()})],
            );
        }
        o
    }

    #[test]
    fn the_storefront_route_routes_by_longest_prefix_to_ready_endpoints() {
        let mut k = Known::default();
        let (t, checks) = build(&storefront(), &mut k);
        assert!(checks.is_empty());
        let h = http::HeaderMap::new();
        let Lookup::Rule(api, label) = t.lookup("storefront.storm1.g8.lo", &req("GET", "/api/items", None, &h)) else { panic!() };
        assert_eq!(label, "storefront.storm1.g8.lo");
        assert_eq!(api.index, 1, "/api/ beats / although / is listed first");
        let Lookup::Rule(web, _) = t.lookup("storefront.storm1.g8.lo", &req("GET", "/", None, &h)) else { panic!() };
        assert_eq!(web.index, 0);
        assert_eq!(web.timeout, Some(Duration::from_secs(30)));
        let b = web.choose().unwrap();
        assert_eq!(b.name, "storefront/web:8080");
        // The not-ready third endpoint takes nothing; the slice's port is used.
        let mut seen: Vec<String> = (0..4).map(|_| b.pick(&[]).unwrap().addr.clone()).collect();
        seen.sort();
        assert_eq!(seen, ["10.0.0.1:9000", "10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.2:9000"]);
        assert!(matches!(t.lookup("other", &req("GET", "/", None, &h)), Lookup::NoHost));
    }

    #[test]
    fn endpoint_state_survives_a_refresh() {
        let mut k = Known::default();
        let (t1, _) = build(&storefront(), &mut k);
        let Lookup::Rule(r1, _) = t1.lookup("storefront.storm1.g8.lo", &req("GET", "/", None, &http::HeaderMap::new())) else { panic!() };
        let e1 = r1.backends[0].endpoints.as_ref().unwrap()[0].clone();
        e1.eject();
        let (t2, _) = build(&storefront(), &mut k);
        let Lookup::Rule(r2, _) = t2.lookup("storefront.storm1.g8.lo", &req("GET", "/", None, &http::HeaderMap::new())) else { panic!() };
        assert!(Arc::ptr_eq(&e1, &r2.backends[0].endpoints.as_ref().unwrap()[0]));
        assert!(r2.backends[0].endpoints.as_ref().unwrap()[0].ejected());
    }

    #[test]
    fn weights_split_exactly_and_skip_a_ref_with_nothing_healthy() {
        let ep = |a: &str, ready| Endpoint::new(a.into(), ready);
        let a = Arc::new(Backend::new("a".into(), 3, Proto::Http, Ok(vec![ep("1.1.1.1:1", true)])));
        let b = Arc::new(Backend::new("b".into(), 1, Proto::Http, Ok(vec![ep("2.2.2.2:1", true)])));
        let r = Rule::new("t/r".into(), 0, vec![a, b]);
        let mut n = HashMap::<String, u32>::new();
        for _ in 0..8 {
            *n.entry(r.choose().unwrap().name.clone()).or_default() += 1;
        }
        assert_eq!((n["a"], n["b"]), (6, 2));
        let down = Arc::new(Backend::new("down".into(), 5, Proto::Http, Ok(vec![ep("3.3.3.3:1", false)])));
        let up = Arc::new(Backend::new("up".into(), 1, Proto::Http, Ok(vec![ep("4.4.4.4:1", true)])));
        let r = Rule::new("t/r".into(), 0, vec![down, up]);
        assert!((0..5).all(|_| r.choose().unwrap().name == "up"));
        let none = Rule::new("t/r".into(), 0, vec![Arc::new(Backend::new("n".into(), 1, Proto::Http, Ok(vec![])))]);
        assert_eq!(none.choose().unwrap_err().0, 503);
        let bad = Rule::new("t/r".into(), 0, vec![Arc::new(Backend::new("x".into(), 1, Proto::Http, Err("BackendNotFound: gone".into())))]);
        assert_eq!(bad.choose().unwrap_err().0, 500);
        let zero = Rule::new("t/r".into(), 0, vec![Arc::new(Backend::new("z".into(), 0, Proto::Http, Ok(vec![])))]);
        assert_eq!(zero.choose().unwrap_err().0, 500);
    }

    #[test]
    fn precedence_exact_then_prefix_length_then_method_then_headers() {
        let mut o = Objects::default();
        o.routes.push(json!({"metadata": {"namespace": "t", "name": "r"}, "spec": {"hostnames": ["h"], "rules": [
            {"matches": [{"path": {"type": "PathPrefix", "value": "/a"}}]},
            {"matches": [{"path": {"type": "Exact", "value": "/a/b"}}]},
            {"matches": [{"path": {"type": "PathPrefix", "value": "/a"}, "method": "POST"}]},
            {"matches": [{"path": {"type": "PathPrefix", "value": "/a"}, "headers": [{"name": "X-V", "value": "2"}]}]},
            {"matches": [{"path": {"type": "PathPrefix", "value": "/"}, "queryParams": [{"name": "v", "value": "1"}]}]}
        ]}}));
        let (t, _) = build(&o, &mut Known::default());
        let idx = |m: &str, p: &str, q: Option<&str>, h: &http::HeaderMap| match t.lookup("h", &req(m, p, q, h)) {
            Lookup::Rule(r, _) => Some(r.index),
            _ => None,
        };
        let none = http::HeaderMap::new();
        let mut v2 = http::HeaderMap::new();
        v2.insert("x-v", "2".parse().unwrap());
        assert_eq!(idx("GET", "/a/b", None, &none), Some(1));
        assert_eq!(idx("POST", "/a/c", None, &v2), Some(2), "a method beats headers");
        assert_eq!(idx("GET", "/a/c", None, &v2), Some(3));
        assert_eq!(idx("GET", "/a/c", None, &none), Some(0));
        assert_eq!(idx("GET", "/z", Some("v=1"), &none), Some(4));
        assert!(matches!(t.lookup("h", &req("GET", "/z", None, &none)), Lookup::NoMatch(_)));
    }

    #[test]
    fn wildcards_match_below_the_suffix_and_exact_names_win() {
        let mut o = Objects::default();
        for (n, h, be) in [("w", "*.storm1.test", "1.1.1.1:1"), ("e", "app.storm1.test", "2.2.2.2:2")] {
            o.routes.push(json!({"metadata": {"namespace": "t", "name": n, "annotations": {"storm.io/backend": be}}, "spec": {"hostnames": [h]}}));
        }
        let (t, _) = build(&o, &mut Known::default());
        let h = http::HeaderMap::new();
        let name = |host: &str| match t.lookup(host, &req("GET", "/", None, &h)) {
            Lookup::Rule(r, l) => Some((r.backends[0].name.clone(), l)),
            _ => None,
        };
        assert_eq!(name("app.storm1.test"), Some(("2.2.2.2:2".into(), "app.storm1.test".into())));
        assert_eq!(name("x.y.storm1.test"), Some(("1.1.1.1:1".into(), "*.storm1.test".into())));
        assert_eq!(name("storm1.test"), None);
    }

    #[test]
    fn what_cannot_be_served_is_dropped_and_said() {
        let mut o = Objects::default();
        o.routes.push(json!({"metadata": {"namespace": "t", "name": "r"}, "spec": {"hostnames": ["h"], "rules": [
            {"matches": [{"path": {"type": "RegularExpression", "value": "/a.*"}}]},
            {"filters": [{"type": "RequestRedirect"}]},
            {"backendRefs": [{"name": "missing", "port": 80}]}
        ]}}));
        let (t, _) = build(&o, &mut Known::default());
        let r = &t.routes[0];
        assert_eq!(r.rules.len(), 1);
        assert_eq!(r.dropped.len(), 2);
        let c = conditions(r);
        assert_eq!((c[0].0, c[0].1), ("Accepted", true));
        assert_eq!((c[1].0, c[1].1), ("PartiallyInvalid", true));
        assert_eq!((c[2].0, c[2].1, c[2].2.as_str()), ("ResolvedRefs", false, "BackendNotFound"));
    }

    fn cross_ns(grants: &[Value]) -> Objects {
        let mut o = Objects::default();
        o.routes.push(json!({"metadata": {"namespace": "a", "name": "r"}, "spec": {"hostnames": ["h"], "rules": [
            {"backendRefs": [{"name": "s", "namespace": "b", "port": 80}]}]}}));
        o.services.insert("b/s".into(), json!({"metadata": {"namespace": "b", "name": "s"},
            "spec": {"clusterIP": "10.96.0.9", "ports": [{"port": 80}]}}));
        o.index_grants(&json!({"items": grants}));
        o
    }

    fn grant(ns: &str, from_ns: &str, to: Value) -> Value {
        json!({"metadata": {"namespace": ns, "name": "g"}, "spec": {
            "from": [{"group": "gateway.networking.k8s.io", "kind": "HTTPRoute", "namespace": from_ns}], "to": [to]}})
    }

    fn resolved(o: &Objects) -> Result<Vec<String>, String> {
        let (t, _) = build(o, &mut Known::default());
        let b = &t.routes[0].rules[0].backends[0];
        b.endpoints.clone().map(|e| e.iter().map(|e| e.addr.clone()).collect())
    }

    #[test]
    fn a_reference_grant_lets_a_route_reach_another_namespace() {
        let any = json!({"group": "", "kind": "Service"});
        assert_eq!(resolved(&cross_ns(&[grant("b", "a", any)])).unwrap(), ["10.96.0.9:80"]);
        let named = json!({"group": "", "kind": "Service", "name": "s"});
        assert_eq!(resolved(&cross_ns(&[grant("b", "a", named)])).unwrap(), ["10.96.0.9:80"]);
    }

    #[test]
    fn without_a_matching_grant_the_ref_is_not_permitted() {
        let svc = json!({"group": "", "kind": "Service"});
        let cases = [
            vec![],
            vec![grant("a", "a", svc.clone())],                                            // grant in the route's namespace
            vec![grant("b", "c", svc.clone())],                                            // from another namespace
            vec![grant("b", "a", json!({"group": "", "kind": "Service", "name": "other"}))], // another Service
            vec![grant("b", "a", json!({"group": "", "kind": "Secret"}))],                  // another kind
        ];
        for g in cases {
            let o = cross_ns(&g);
            let why = resolved(&o).unwrap_err();
            assert!(why.starts_with("RefNotPermitted: "), "{g:?}: {why}");
            let (t, _) = build(&o, &mut Known::default());
            let c = conditions(&t.routes[0]);
            assert_eq!((c[1].0, c[1].1, c[1].2.as_str()), ("ResolvedRefs", false, "RefNotPermitted"));
            assert_eq!(t.routes[0].rules[0].choose().unwrap_err().0, 500);
        }
        // Unreadable grants refuse too; a same-namespace ref needs none.
        let mut o = cross_ns(&[grant("b", "a", svc)]);
        o.grants_error = Some("403".into());
        assert!(resolved(&o).unwrap_err().starts_with("RefNotPermitted: ReferenceGrants could not be read"));
        o.routes[0]["spec"]["rules"][0]["backendRefs"][0]["namespace"] = json!("a");
        o.services.insert("a/s".into(), json!({"spec": {"clusterIP": "10.96.0.8", "ports": [{"port": 80}]}}));
        assert_eq!(resolved(&o).unwrap(), ["10.96.0.8:80"]);
    }

    #[test]
    fn cross_namespace_refs_are_found() {
        assert!(has_cross_namespace_ref(&cross_ns(&[]).routes));
        let mut o = cross_ns(&[]);
        o.routes[0]["spec"]["rules"][0]["backendRefs"][0]["namespace"] = json!("a");
        assert!(!has_cross_namespace_ref(&o.routes));
    }

    #[test]
    fn endpoints_are_the_fallback_and_the_cluster_ip_the_last_resort() {
        let mut o = Objects::default();
        o.routes.push(json!({"metadata": {"namespace": "t", "name": "r"}, "spec": {"hostnames": ["h"], "rules": [
            {"backendRefs": [{"name": "e", "port": 80}, {"name": "c", "port": 81}, {"name": "hl", "port": 82}]}]}}));
        for (n, ip, p) in [("e", "10.1.1.1", 80), ("c", "10.96.0.5", 81), ("hl", "None", 82)] {
            o.services.insert(format!("t/{n}"), json!({"metadata": {"namespace": "t", "name": n},
                "spec": {"clusterIP": ip, "ports": [{"name": "web", "port": p}]}}));
        }
        o.endpoints.insert("t/e".into(), json!({"subsets": [{"addresses": [{"ip": "10.2.0.1"}], "notReadyAddresses": [{"ip": "10.2.0.2"}],
            "ports": [{"name": "web", "port": 8080}]}]}));
        let (t, _) = build(&o, &mut Known::default());
        let b = &t.routes[0].rules[0].backends;
        let addrs = |i: usize| b[i].endpoints.as_ref().unwrap().iter().map(|e| (e.addr.clone(), e.ready())).collect::<Vec<_>>();
        assert_eq!(addrs(0), [("10.2.0.1:8080".to_string(), true), ("10.2.0.2:8080".to_string(), false)]);
        assert_eq!(addrs(1), [("10.96.0.5:81".to_string(), true)]);
        assert!(addrs(2).is_empty(), "a headless Service with no endpoints has none");
    }

    #[test]
    fn status_is_written_only_when_it_changes_and_keeps_others() {
        let mut o = storefront();
        o.routes[0]["status"] = json!({"parents": [{"controllerName": "example.com/gw", "parentRef": {"name": "gw"}}]});
        let (t, _) = build(&o, &mut Known::default());
        let p = status_parents(&t.routes[0], "2026-11-19T00:00:00Z").unwrap();
        assert_eq!(p.as_array().unwrap().len(), 2);
        assert_eq!(p[0]["controllerName"], "example.com/gw");
        let healthy = &p[1]["conditions"][2];
        assert_eq!(healthy["type"], "storm.io/BackendsHealthy");
        assert_eq!(healthy["message"], "storefront/web:8080 2/3 healthy; storefront/api:8081 2/3 healthy");
        assert_eq!(p[1]["conditions"][0]["observedGeneration"], 3);
        // Read back as written: nothing to write, the transition time kept.
        o.routes[0]["status"]["parents"] = p.clone();
        let (t, _) = build(&o, &mut Known::default());
        assert_eq!(status_parents(&t.routes[0], "2026-11-20T00:00:00Z"), None);
    }

    #[test]
    fn checks_need_rise_to_come_back_and_fall_to_go() {
        let spec = CheckSpec { path: "/h".into(), host: None, interval: Duration::from_secs(1), timeout: Duration::from_secs(1), rise: 2, fall: 3 };
        let e = Endpoint::new("1.1.1.1:1".into(), true);
        assert_eq!(e.record_check(false, &spec), None);
        assert_eq!(e.record_check(false, &spec), None);
        assert_eq!(e.record_check(false, &spec), Some(false));
        assert!(!e.healthy());
        assert_eq!(e.record_check(true, &spec), None);
        assert_eq!(e.record_check(true, &spec), Some(true));
        assert!(e.healthy());
    }

    fn os_objects(routes: Vec<Value>) -> Objects {
        let mut o = Objects { route_domain: Some("storm1.g8.lo".into()), os_routes: routes, ..Default::default() };
        o.services.insert(
            "openshift-monitoring/prometheus-k8s".into(),
            json!({"metadata": {"namespace": "openshift-monitoring", "name": "prometheus-k8s"},
                "spec": {"clusterIP": "10.96.0.20", "ports": [{"name": "web", "port": 9091, "targetPort": "web"}, {"name": "metrics", "port": 9092, "targetPort": 9192}]}}),
        );
        o.services.insert(
            "openshift-monitoring/thanos".into(),
            json!({"metadata": {"namespace": "openshift-monitoring", "name": "thanos"},
                "spec": {"clusterIP": "10.96.0.21", "ports": [{"name": "web", "port": 9091}]}}),
        );
        o
    }

    fn os(ns: &str, name: &str, created: &str, spec: Value) -> Value {
        json!({"metadata": {"namespace": ns, "name": name, "creationTimestamp": created}, "spec": spec})
    }

    fn rule_of(t: &Table, host: &str, path: &str) -> Option<Arc<Rule>> {
        match t.lookup(host, &req("GET", path, None, &http::HeaderMap::new())) {
            Lookup::Rule(r, _) => Some(r),
            _ => None,
        }
    }

    #[test]
    fn a_route_is_served_on_its_default_host_and_admitted() {
        let o = os_objects(vec![os("openshift-monitoring", "prometheus-k8s", "2026-11-25T00:00:00Z", json!({
            "to": {"kind": "Service", "name": "prometheus-k8s", "weight": 3},
            "alternateBackends": [{"kind": "Service", "name": "thanos", "weight": 1}],
            "port": {"targetPort": "web"},
            "tls": {"termination": "reencrypt", "insecureEdgeTerminationPolicy": "Redirect"}
        }))]);
        let (t, _) = build(&o, &mut Known::default());
        let host = "prometheus-k8s-openshift-monitoring.storm1.g8.lo";
        let r = rule_of(&t, host, "/api/v1/query").expect("served");
        assert_eq!(r.insecure, Insecure::Redirect);
        let names: Vec<(String, u32, Proto)> = r.backends.iter().map(|b| (b.name.clone(), b.weight, b.proto.clone())).collect();
        assert_eq!(names, [
            ("openshift-monitoring/prometheus-k8s:9091".into(), 3, Proto::Https(Some("prometheus-k8s.openshift-monitoring.svc".into()))),
            ("openshift-monitoring/thanos:9091".into(), 1, Proto::Https(Some("thanos.openshift-monitoring.svc".into()))),
        ]);
        let mut n = HashMap::<String, u32>::new();
        for _ in 0..8 {
            *n.entry(r.choose().unwrap().name.clone()).or_default() += 1;
        }
        assert_eq!(n["openshift-monitoring/prometheus-k8s:9091"], 6);

        let info = &t.routes[0];
        assert_eq!(info.os.as_ref().unwrap().host, host);
        let ing = route_ingress(info, "2026-11-25T01:00:00Z").unwrap();
        assert_eq!(ing, json!([{"host": host, "routerName": "stormlb", "wildcardPolicy": "None",
            "conditions": [{"type": "Admitted", "status": "True", "lastTransitionTime": "2026-11-25T01:00:00Z"}]}]));
        // Read back as written: nothing to write.
        let mut o = o;
        o.os_routes[0]["status"] = json!({"ingress": ing});
        let (t, _) = build(&o, &mut Known::default());
        assert_eq!(route_ingress(&t.routes[0], "2026-11-26T00:00:00Z"), None);
        assert_eq!(problems(&t.routes[0]), "");
    }

    #[test]
    fn target_ports_name_a_port_a_target_or_default_to_the_first() {
        let o = os_objects(vec![]);
        let p = |t: Value| route_port(&o, "openshift-monitoring", "prometheus-k8s", &t);
        assert_eq!(p(Value::Null), Ok(9091));
        assert_eq!(p(json!("web")), Ok(9091));
        assert_eq!(p(json!("metrics")), Ok(9092));
        assert_eq!(p(json!(9192)), Ok(9092), "a target port number");
        assert_eq!(p(json!("9192")), Ok(9092));
        assert_eq!(p(json!(9091)), Ok(9091), "a port number when no target port matches");
        assert!(p(json!("nope")).unwrap_err().starts_with("BackendNotFound: "));
        assert_eq!(route_port(&o, "x", "missing", &Value::Null), Ok(80), "a missing Service is said by resolve");
    }

    #[test]
    fn the_oldest_route_holds_a_host_and_another_namespace_cannot_share_it() {
        let to = json!({"kind": "Service", "name": "thanos"});
        let o = os_objects(vec![
            os("b", "late", "2026-11-25T02:00:00Z", json!({"host": "app.test", "to": to})),
            os("openshift-monitoring", "first", "2026-11-25T00:00:00Z", json!({"host": "app.test", "to": to})),
            os("openshift-monitoring", "same", "2026-11-25T01:00:00Z", json!({"host": "app.test", "to": to})),
            os("openshift-monitoring", "api", "2026-11-25T03:00:00Z", json!({"host": "app.test", "path": "/api", "to": to})),
        ]);
        let (t, _) = build(&o, &mut Known::default());
        let by = |n: &str| t.routes.iter().find(|r| r.name == n).unwrap();
        assert!(by("first").os.as_ref().unwrap().rejected.is_none());
        assert!(by("api").os.as_ref().unwrap().rejected.is_none(), "same namespace, another path");
        for n in ["same", "late"] {
            let (reason, _) = by(n).os.as_ref().unwrap().rejected.clone().unwrap();
            assert_eq!(reason, "HostAlreadyClaimed", "{n}");
            let ing = route_ingress(by(n), "now").unwrap();
            assert_eq!(ing[0]["conditions"][0]["status"], "False");
            assert_eq!(ing[0]["conditions"][0]["reason"], "HostAlreadyClaimed");
        }
        assert_eq!(rule_of(&t, "app.test", "/api/x").unwrap().route, "openshift-monitoring/api");
        assert_eq!(rule_of(&t, "app.test", "/").unwrap().route, "openshift-monitoring/first");
    }

    #[test]
    fn tls_terminations_passthrough_wildcards_and_refusals() {
        let to = json!({"kind": "Service", "name": "thanos"});
        let mut o = os_objects(vec![
            os("openshift-monitoring", "pt", "1", json!({"host": "pt.test", "to": to, "tls": {"termination": "passthrough"}})),
            os("openshift-monitoring", "edge", "2", json!({"host": "edge.test", "to": to,
                "tls": {"termination": "edge", "insecureEdgeTerminationPolicy": "Allow", "certificate": "PEM", "key": "PEM"}})),
            os("openshift-monitoring", "deny", "3", json!({"host": "deny.test", "to": to, "tls": {"termination": "edge"}})),
            os("openshift-monitoring", "wild", "4", json!({"host": "www.apps.test", "to": to, "wildcardPolicy": "Subdomain"})),
            os("openshift-monitoring", "bad", "5", json!({"host": "bad.test", "to": to, "tls": {"termination": "magic"}})),
            os("openshift-monitoring", "ptpath", "6", json!({"host": "pp.test", "path": "/x", "to": to, "tls": {"termination": "passthrough"}})),
            os("openshift-monitoring", "nohost", "7", json!({"to": to})),
            os("openshift-monitoring", "static", "8", json!({"host": "node.test", "path": "/api"}))
        ]);
        o.os_routes[7]["metadata"]["annotations"] = json!({"storm.io/backend": "127.0.0.1:9090"});
        o.route_domain = None;
        let (t, _) = build(&o, &mut Known::default());
        let by = |n: &str| t.routes.iter().find(|r| r.name == n).unwrap();
        let rejected = |n: &str| by(n).os.as_ref().unwrap().rejected.clone().map(|r| r.0);

        let pt = t.passthrough("PT.test").expect("passthrough by SNI");
        assert_eq!(pt.backends[0].proto, Proto::Http, "passthrough dials plain TCP; TLS is the client's");
        assert!(rule_of(&t, "pt.test", "/").is_none(), "a passthrough host has no HTTP rule");

        let edge = rule_of(&t, "edge.test", "/").unwrap();
        assert_eq!(edge.insecure, Insecure::Allow);
        assert!(by("edge").dropped[0].contains("certificate is not used"));
        let ing = route_ingress(by("edge"), "now").unwrap();
        assert_eq!(ing[0]["conditions"][0]["status"], "True");
        assert!(ing[0]["conditions"][0]["message"].as_str().unwrap().contains("own certificate"));
        assert_eq!(rule_of(&t, "deny.test", "/").unwrap().insecure, Insecure::Deny);

        assert!(rule_of(&t, "x.apps.test", "/").is_some(), "Subdomain serves the siblings");
        assert_eq!(by("wild").hostnames, ["*.apps.test"]);
        assert_eq!(route_ingress(by("wild"), "now").unwrap()[0]["wildcardPolicy"], "Subdomain");

        assert_eq!(rejected("bad").as_deref(), Some("ExtendedValidationFailed"));
        assert_eq!(rejected("ptpath").as_deref(), Some("ExtendedValidationFailed"));
        assert_eq!(rejected("nohost").as_deref(), Some("NoHost"));
        assert_eq!(route_ingress(by("nohost"), "now").unwrap()[0]["host"], "");
        assert!(rule_of(&t, "bad.test", "/").is_none());

        let st = rule_of(&t, "node.test", "/api/v1/query").unwrap();
        assert_eq!(st.backends[0].name, "127.0.0.1:9090");
        assert!(rule_of(&t, "node.test", "/other").is_none(), "the path still applies to storm.io/backend");
    }

    #[test]
    fn a_routes_ingress_keeps_other_routers_and_its_transition_time() {
        let mut o = os_objects(vec![os("openshift-monitoring", "p", "1", json!({"host": "p.test", "to": {"name": "thanos"}}))]);
        o.os_routes[0]["status"] = json!({"ingress": [
            {"host": "p.test", "routerName": "default", "conditions": [{"type": "Admitted", "status": "True"}]},
            {"host": "p.test", "routerName": "stormlb", "wildcardPolicy": "None",
             "conditions": [{"type": "Admitted", "status": "False", "reason": "NoHost", "lastTransitionTime": "old"}]}
        ]});
        let (t, _) = build(&o, &mut Known::default());
        let ing = route_ingress(&t.routes[0], "new").unwrap();
        assert_eq!(ing.as_array().unwrap().len(), 2);
        assert_eq!(ing[0]["routerName"], "default");
        assert_eq!(ing[1]["conditions"][0]["lastTransitionTime"], "new", "False -> True is a transition");
    }
}
