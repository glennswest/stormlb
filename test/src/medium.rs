//! `medium` (< 30 min): the router's features and failure paths, end to end,
//! each checked against what README "Router" says it does:
//!
//! - its own answers: 400 without a Host, a 404 naming the host, `/healthz`
//!   on an unclaimed host, and that its head ends in CRLF (stormlb#5);
//! - routing: `/healthz` on a claimed host is proxied, Host is matched
//!   case-insensitively with the port stripped, and every request on a
//!   keep-alive connection is routed on its own (#26);
//! - the proxy: a streamed response is not held back, an Upgrade becomes a
//!   two-way pipe, an 8 MiB body passes whole;
//! - failure paths: a request head past 16 KiB is cut off (431), a dead
//!   backend is the router's 502, a headless Service with no endpoints is a
//!   503 without taking the others down, and an endpoint that refuses
//!   connections costs no request (failover to another);
//! - the table: a route update moves the host, a backendRef resolves to its
//!   Service's endpoints, rules route by longest path prefix and split by
//!   weight (#37), the route's status says Accepted / ResolvedRefs /
//!   BackendsHealthy, fifty routes at once each reach their own backend
//!   under concurrent load, and deleted routes are 404s again;
//! - and stormd saw no restart or crash of the router throughout.
//!
//! The VIP half (L4, VRRP, BGP) is reported skip: the golden does not run it.

use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::task::JoinSet;

use crate::api::Api;
use crate::backend::Backend;
use crate::env::{fd_limit, join, Env};
use crate::http::{fetch, field, get, Conn, IO};
use crate::probe::{self, Supervised};
use crate::report::{Outcome, Report};
use crate::short;

/// Hosts in `many-hosts`.
const MANY: usize = 50;

pub async fn run(env: &Env, r: &mut Report) {
    let sd0 = match &env.stormd {
        Some(sd) => Some(probe::stormd(sd).await),
        None => None,
    };
    r.run("no-host-400", no_host(env)).await;
    r.run("unknown-host-404", unknown_host(env)).await;
    r.run("healthz-unclaimed", short::healthz(env)).await;
    r.run("healthz-crlf", healthz_crlf(env)).await;

    let cx = match Ctx::new(env).await {
        Ok(c) => c,
        Err(e) => {
            r.record("routes-programmed", Outcome::Infra(e), 0, None);
            return;
        }
    };
    if r.run("routes-programmed", cx.program()).await {
        r.run("healthz-claimed-proxied", cx.healthz_claimed()).await;
        r.run("host-case-and-port", cx.host_case_and_port()).await;
        r.run("per-request-routing", cx.per_request()).await;
        r.run("streaming-unbuffered", cx.streaming()).await;
        r.run("upgrade-passthrough", cx.upgrade()).await;
        r.run("large-body", cx.large_body()).await;
        r.run("head-limit", cx.head_limit()).await;
        r.run("route-update", cx.route_update()).await;
    } else {
        for n in [
            "healthz-claimed-proxied",
            "host-case-and-port",
            "per-request-routing",
            "streaming-unbuffered",
            "upgrade-passthrough",
            "large-body",
            "head-limit",
            "route-update",
        ] {
            r.record(n, Outcome::Fail("not run: routes-programmed did not pass".into()), 0, None);
        }
    }
    r.run("dead-backend", cx.dead_backend()).await;
    r.run("backendref-service", cx.backendref()).await;
    r.run("headless-service-503", cx.headless()).await;
    if r.run("rules-and-weights", cx.rules_and_weights()).await {
        r.run("route-status", cx.route_status()).await;
    } else {
        r.record("route-status", Outcome::Fail("not run: rules-and-weights did not pass".into()), 0, None);
    }
    r.run("endpoint-failover", cx.failover()).await;
    if r.run("many-hosts", cx.many_hosts()).await {
        r.run("concurrent-connections", cx.concurrent()).await;
    } else {
        r.record("concurrent-connections", Outcome::Fail("not run: many-hosts did not pass".into()), 0, None);
    }
    r.run("routes-deleted-404", cx.deleted()).await;
    r.run("stormd-no-restarts", no_restarts(env, sd0)).await;
    r.record(
        "vip-half",
        Outcome::Skip(
            "not shipped: the stormcos golden runs [router] only. The L4 balancer, health checks, VRRP and BGP \
             are covered by cargo test in sc-build (tests/balancer.rs and the unit tests)"
                .into(),
        ),
        0,
        None,
    );
    short::cleanup(&cx.api, r).await;
}

async fn no_host(env: &Env) -> Outcome {
    let mut c = match Conn::open(&env.router).await {
        Ok(c) => c,
        Err(e) => return Outcome::Fail(format!("{}: {e}", env.router)),
    };
    if let Err(e) = c.send(b"GET / HTTP/1.0\r\n\r\n").await {
        return Outcome::Fail(e.to_string());
    }
    match c.response().await {
        Ok(Some(r)) if r.status == 400 => Outcome::Pass(format!("400 {:?}", r.text().trim())),
        Ok(Some(r)) => Outcome::Fail(format!("answered {} {:?}", r.status, r.text())),
        Ok(None) => Outcome::Fail("closed with no response".into()),
        Err(e) => Outcome::Fail(e.to_string()),
    }
}

async fn unknown_host(env: &Env) -> Outcome {
    let host = env.host("nobody");
    match fetch(&env.router, &host, "/").await {
        Ok(Some(r)) if r.status == 404 && r.text().contains(&format!("no route for host {host}")) => {
            Outcome::Pass(format!("404 {:?}", r.text().trim()))
        }
        Ok(Some(r)) => Outcome::Fail(format!("answered {} {:?}", r.status, r.text())),
        Ok(None) => Outcome::Fail("closed with no response".into()),
        Err(e) => Outcome::Fail(e.to_string()),
    }
}

/// HTTP/1.1 heads end in CRLF. They did not before stormlb#5.
async fn healthz_crlf(env: &Env) -> Outcome {
    match fetch(&env.router, &env.host("unclaimed"), "/healthz").await {
        Ok(Some(r)) if r.crlf => Outcome::Pass("the head ends in CRLF CRLF".into()),
        Ok(Some(r)) => Outcome::Fail(format!(
            "the /healthz head uses bare LF line endings (regressed: stormlb#5): {:?}",
            r.head
        )),
        Ok(None) => Outcome::Fail("closed with no response".into()),
        Err(e) => Outcome::Fail(e.to_string()),
    }
}

async fn no_restarts(env: &Env, before: Option<Result<Supervised, String>>) -> Outcome {
    let (Some(sd), Some(before)) = (&env.stormd, before) else {
        return Outcome::Skip("no stormd here (STORMLB_STORMD=none)".into());
    };
    let before = match before {
        Ok(b) => b,
        Err(e) => return Outcome::Fail(format!("stormd unreadable at the start: {e}")),
    };
    match probe::stormd(sd).await {
        Ok(now) if now.running && now.restarts == before.restarts && now.crashes == before.crashes => {
            Outcome::Pass(format!("running throughout: {} restarts, {} crashes, unchanged", now.restarts, now.crashes))
        }
        Ok(now) => Outcome::Fail(format!("stormlb under stormd was {before:?} at the start and is {now:?} now")),
        Err(e) => Outcome::Fail(e),
    }
}

struct Ctx<'a> {
    env: &'a Env,
    api: Api,
    ip: IpAddr,
    a: Backend,
    b: Backend,
    c: Backend,
    ha: String,
    hb: String,
    /// Every host this run routed, for `routes-deleted-404`.
    hosts: Mutex<Vec<String>>,
    /// `many-hosts`: each host and the backend it should reach.
    many: Mutex<Vec<(String, String)>>,
}

fn fail_io(what: &str, e: std::io::Error) -> Outcome {
    Outcome::Fail(format!("{what}: {e}"))
}

impl<'a> Ctx<'a> {
    async fn new(env: &'a Env) -> Result<Ctx<'a>, String> {
        let (api, a) = short::setup(env).await?;
        let ip = env.reach_ip()?;
        let b = Backend::start("b", ip).await.map_err(|e| format!("backend listener: {e}"))?;
        let c = Backend::start("c", ip).await.map_err(|e| format!("backend listener: {e}"))?;
        Ok(Ctx {
            ha: env.host("a"),
            hb: env.host("b"),
            env,
            api,
            ip,
            a,
            b,
            c,
            hosts: Mutex::new(Vec::new()),
            many: Mutex::new(Vec::new()),
        })
    }

    fn remember(&self, h: &str) {
        self.hosts.lock().unwrap().push(h.to_string());
    }

    async fn create_route(&self, name: &str, host: &str, backend: Option<&str>, svc: Option<(&str, u16)>) -> Result<(), String> {
        self.remember(host);
        self.api.create(&self.api.routes(), &self.api.route(name, &[host.to_string()], backend, svc)).await.map(|_| ())
    }

    async fn program(&self) -> Outcome {
        for (name, h, be) in [("main", &self.ha, &self.a), ("other", &self.hb, &self.b)] {
            if let Err(e) = self.create_route(name, h, Some(be.addr.as_str()), None).await {
                return Outcome::Infra(format!("cannot create the HTTPRoute: {e}"));
            }
        }
        let t = Instant::now();
        for (h, be) in [(&self.ha, "a"), (&self.hb, "b")] {
            if let Err(e) = probe::served(&self.env.router, h, be, self.env.route_wait).await {
                return Outcome::Fail(e);
            }
        }
        Outcome::Pass(format!("two hosts on two backends, served after {} ms", t.elapsed().as_millis()))
    }

    async fn healthz_claimed(&self) -> Outcome {
        match fetch(&self.env.router, &self.ha, "/healthz").await {
            Ok(Some(r)) if r.status == 200 && r.backend().as_deref() == Some("a") && r.text().contains("path=/healthz") => {
                Outcome::Pass("proxied to backend a".into())
            }
            Ok(Some(r)) => Outcome::Fail(format!("answered {} {:?}, not backend a", r.status, r.text())),
            Ok(None) => Outcome::Fail("closed with no response".into()),
            Err(e) => fail_io("fetch", e),
        }
    }

    async fn host_case_and_port(&self) -> Outcome {
        let host = format!("{}:80", self.ha.to_ascii_uppercase());
        match fetch(&self.env.router, &host, "/").await {
            Ok(Some(r)) if r.backend().as_deref() == Some("a") => Outcome::Pass(format!("Host {host:?} reached backend a")),
            Ok(Some(r)) => Outcome::Fail(format!("Host {host:?}: {} {:?}", r.status, r.text())),
            Ok(None) => Outcome::Fail("closed with no response".into()),
            Err(e) => fail_io("fetch", e),
        }
    }

    /// #26: every request on a keep-alive connection is routed on its own.
    async fn per_request(&self) -> Outcome {
        let mut c = match Conn::open(&self.env.router).await {
            Ok(c) => c,
            Err(e) => return fail_io("connect", e),
        };
        let mut seen = Vec::new();
        for (path, host) in [("/1", &self.ha), ("/2", &self.hb), ("/3", &self.ha)] {
            let req = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: stormlb-test\r\n\r\n");
            if let Err(e) = c.send(req.as_bytes()).await {
                return fail_io("send", e);
            }
            match c.response().await {
                Ok(Some(r)) => seen.push((r.backend().unwrap_or_default(), field(&r.text(), "path").unwrap_or_default())),
                Ok(None) => return Outcome::Fail(format!("closed before the response to {path}")),
                Err(e) => return fail_io(path, e),
            }
        }
        let want = [("a", "/1"), ("b", "/2"), ("a", "/3")];
        if seen.iter().map(|(b, p)| (b.as_str(), p.as_str())).eq(want) {
            Outcome::Pass(format!("one connection: {} -> a, {} -> b, {} -> a again", self.ha, self.hb, self.ha))
        } else {
            Outcome::Fail(format!("backends and paths seen on one connection: {seen:?}, want {want:?}"))
        }
    }

    /// The backend sends `first`, waits 1.5 s, sends `second`.
    async fn streaming(&self) -> Outcome {
        let mut c = match Conn::open(&self.env.router).await {
            Ok(c) => c,
            Err(e) => return fail_io("connect", e),
        };
        if let Err(e) = c.send(&get(&self.ha, "/stream")).await {
            return fail_io("send", e);
        }
        let t = Instant::now();
        while !String::from_utf8_lossy(&c.buf).contains("first\n") {
            match c.fill(IO).await {
                Ok(0) => return Outcome::Fail(format!("closed before `first`: {:?}", String::from_utf8_lossy(&c.buf))),
                Ok(_) => {}
                Err(e) => return fail_io("read", e),
            }
        }
        let first = t.elapsed();
        let all = match c.closed(IO).await {
            Ok(b) => String::from_utf8_lossy(&b).into_owned(),
            Err(e) => return fail_io("read", e),
        };
        if !all.contains("second\n") {
            return Outcome::Fail(format!("`second` never arrived: {all:?}"));
        }
        if first >= Duration::from_millis(1000) {
            return Outcome::Fail(format!(
                "`first` arrived after {} ms: held back until the backend finished (it waits 1.5 s)",
                first.as_millis()
            ));
        }
        Outcome::Pass(format!("`first` after {} ms, `second` after {} ms", first.as_millis(), t.elapsed().as_millis()))
    }

    async fn upgrade(&self) -> Outcome {
        let mut c = match Conn::open(&self.env.router).await {
            Ok(c) => c,
            Err(e) => return fail_io("connect", e),
        };
        let req = format!("GET /ws HTTP/1.1\r\nHost: {}\r\nConnection: Upgrade\r\nUpgrade: stormlb-test\r\n\r\n", self.ha);
        if let Err(e) = c.send(req.as_bytes()).await {
            return fail_io("send", e);
        }
        match c.response().await {
            Ok(Some(r)) if r.status == 101 => {}
            Ok(Some(r)) => return Outcome::Fail(format!("answered {} {:?}, not 101", r.status, r.text())),
            Ok(None) => return Outcome::Fail("closed with no response".into()),
            Err(e) => return fail_io("response", e),
        }
        let ping = format!("ping {} both ways\n", self.env.run_id);
        for round in 1..=3 {
            if let Err(e) = c.send(ping.as_bytes()).await {
                return fail_io("send", e);
            }
            let want = ping.repeat(round);
            let t = Instant::now();
            while String::from_utf8_lossy(&c.buf) != want {
                if t.elapsed() > Duration::from_secs(5) || c.buf.len() > want.len() {
                    return Outcome::Fail(format!("echo round {round}: got {:?}", String::from_utf8_lossy(&c.buf)));
                }
                match c.fill(Duration::from_secs(5)).await {
                    Ok(0) => return Outcome::Fail(format!("closed in echo round {round}")),
                    Ok(_) => {}
                    Err(e) => return fail_io("read", e),
                }
            }
        }
        Outcome::Pass("101, then three echo rounds through the pipe".into())
    }

    async fn large_body(&self) -> Outcome {
        const N: usize = 8 << 20;
        let mut c = match Conn::open(&self.env.router).await {
            Ok(c) => c,
            Err(e) => return fail_io("connect", e),
        };
        let head = format!("POST /upload HTTP/1.1\r\nHost: {}\r\nContent-Length: {N}\r\nConnection: close\r\n\r\n", self.ha);
        let t = Instant::now();
        if let Err(e) = c.send(head.as_bytes()).await {
            return fail_io("send", e);
        }
        let chunk = vec![b's'; 64 << 10];
        for _ in 0..N / chunk.len() {
            if let Err(e) = c.send(&chunk).await {
                return fail_io("send body", e);
            }
        }
        match c.response().await {
            Ok(Some(r)) if field(&r.text(), "got").as_deref() == Some(N.to_string().as_str()) && r.backend().as_deref() == Some("a") => {
                Outcome::Pass(format!("{N} bytes through in {} ms", t.elapsed().as_millis()))
            }
            Ok(Some(r)) => Outcome::Fail(format!("{} {:?}", r.status, r.text())),
            Ok(None) => Outcome::Fail("closed with no response".into()),
            Err(e) => fail_io("response", e),
        }
    }

    /// A head that never ends is cut off past 16 KiB (431), not buffered.
    async fn head_limit(&self) -> Outcome {
        let mut c = match Conn::open(&self.env.router).await {
            Ok(c) => c,
            Err(e) => return fail_io("connect", e),
        };
        let mut head = format!("GET / HTTP/1.1\r\nHost: {}\r\n", self.ha);
        let pad = "a".repeat(100);
        let mut i = 0;
        while head.len() < 17 << 10 {
            head.push_str(&format!("X-Pad-{i}: {pad}\r\n"));
            i += 1;
        }
        // The router may close while this is still being written; that is
        // the behaviour under test, not an error.
        let _ = c.send(head.as_bytes()).await;
        let before = hits(&self.a);
        match c.closed(Duration::from_secs(5)).await {
            Ok(b) if hits(&self.a) == before && (b.is_empty() || b.starts_with(b"HTTP/1.1 431")) => Outcome::Pass(format!(
                "{} bytes of unterminated head: {}closed, nothing sent on",
                head.len(),
                if b.is_empty() { "" } else { "431, " }
            )),
            Ok(b) if b.is_empty() => Outcome::Fail("closed, but the backend saw a request".into()),
            Ok(b) => Outcome::Fail(format!("answered: {:?}", String::from_utf8_lossy(&b[..b.len().min(120)]))),
            Err(e) => Outcome::Fail(format!("still open 5 s after {} bytes of head: {e}", head.len())),
        }
    }

    async fn route_update(&self) -> Outcome {
        let path = format!("{}/main", self.api.routes());
        let mut route = match self.api.get(&path).await {
            Ok(Some(r)) => r,
            Ok(None) => return Outcome::Fail(format!("{path} is gone")),
            Err(e) => return Outcome::Infra(e),
        };
        route["metadata"]["annotations"] = json!({"storm.io/backend": self.b.addr});
        if let Err(e) = self.api.put(&path, &route).await {
            return Outcome::Infra(format!("cannot update the HTTPRoute: {e}"));
        }
        match probe::served(&self.env.router, &self.ha, "b", self.env.route_wait).await {
            Ok(t) => Outcome::Pass(format!("{} moved from backend a to b after {} ms", self.ha, t.as_millis())),
            Err(e) => Outcome::Fail(e),
        }
    }

    /// A backend that cannot be dialled is the router's 502, naming the host.
    async fn dead_backend(&self) -> Outcome {
        let port = match std::net::TcpListener::bind(join(&self.ip.to_string(), 0)) {
            Ok(l) => l.local_addr().map(|a| a.port()).unwrap_or(0),
            Err(e) => return Outcome::Infra(format!("cannot find a free port: {e}")),
        };
        let host = self.env.host("dead");
        let dead = join(&self.ip.to_string(), port);
        if let Err(e) = self.create_route("dead", &host, Some(dead.as_str()), None).await {
            return Outcome::Infra(format!("cannot create the HTTPRoute: {e}"));
        }
        let t = Instant::now();
        let mut last = String::new();
        while t.elapsed() < self.env.route_wait {
            match fetch(&self.env.router, &host, "/").await {
                Ok(Some(r)) if r.status == 502 && r.text().contains(&host) => {
                    return Outcome::Pass(format!("{host} -> {dead} (nothing listening): 502 {:?}", r.text().trim()))
                }
                Ok(Some(r)) if r.status == 404 && r.text().contains("no route for host") => last = "404, not programmed yet".into(),
                Ok(Some(r)) => return Outcome::Fail(format!("answered {} {:?}, want the router's 502", r.status, r.text())),
                Ok(None) => return Outcome::Fail("closed with no response, want the router's 502".into()),
                Err(e) => last = e.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Outcome::Fail(format!("never programmed in {} s; last: {last}", self.env.route_wait.as_secs()))
    }

    /// A selectorless Service on `port` whose Endpoints are `addrs`
    /// (ip, port, ready).
    async fn service(&self, name: &str, port: u16, addrs: &[(IpAddr, u16, bool)], annotations: Value) -> Result<(), String> {
        let mut meta = self.api.meta(name);
        meta["annotations"] = annotations;
        let svc = json!({"apiVersion": "v1", "kind": "Service", "metadata": meta,
            "spec": {"ports": [{"name": "http", "port": port, "targetPort": port, "protocol": "TCP"}]}});
        self.api.create(&self.api.services(), &svc).await.map_err(|e| format!("cannot create Service {name}: {e}"))?;
        let mut subsets = Vec::new();
        for (ip, p, ready) in addrs {
            let field = if *ready { "addresses" } else { "notReadyAddresses" };
            subsets.push(json!({field: [{"ip": ip.to_string()}], "ports": [{"name": "http", "port": p, "protocol": "TCP"}]}));
        }
        let eps = json!({"apiVersion": "v1", "kind": "Endpoints", "metadata": self.api.meta(name), "subsets": subsets});
        self.api.create(&self.api.endpoints(), &eps).await.map_err(|e| format!("cannot create Endpoints {name}: {e}"))?;
        Ok(())
    }

    /// An HTTPRoute with these rules.
    async fn rules_route(&self, name: &str, host: &str, rules: Value) -> Result<(), String> {
        self.remember(host);
        let route = json!({"apiVersion": "gateway.networking.k8s.io/v1", "kind": "HTTPRoute", "metadata": self.api.meta(name),
            "spec": {"hostnames": [host], "rules": rules}});
        self.api.create(&self.api.routes(), &route).await.map(|_| ()).map_err(|e| format!("cannot create HTTPRoute {name}: {e}"))
    }

    /// The cluster path: a route with a backendRef, resolved to the
    /// Service's endpoints (here a selectorless Service whose Endpoints name
    /// backend c), dialled directly — no Service data plane in between.
    async fn backendref(&self) -> Outcome {
        if let Err(e) = self.service("svc-c", self.c.port, &[(self.ip, self.c.port, true)], json!({})).await {
            return Outcome::Infra(e);
        }
        let host = self.env.host("svc");
        if let Err(e) = self.create_route("svc", &host, None, Some(("svc-c", self.c.port))).await {
            return Outcome::Infra(format!("cannot create the HTTPRoute: {e}"));
        }
        match probe::served(&self.env.router, &host, "c", self.env.route_wait).await {
            Ok(t) => Outcome::Pass(format!("{host} -> Service svc-c -> its endpoint (backend c) after {} ms", t.as_millis())),
            Err(e) => Outcome::Fail(e),
        }
    }

    /// A headless Service with no endpoints: its route answers 503, and the
    /// rest of the table is unaffected.
    async fn headless(&self) -> Outcome {
        let svc = json!({"apiVersion": "v1", "kind": "Service", "metadata": self.api.meta("svc-h"),
            "spec": {"clusterIP": "None", "ports": [{"name": "http", "port": 80, "protocol": "TCP"}]}});
        if let Err(e) = self.api.create(&self.api.services(), &svc).await {
            return Outcome::Infra(format!("cannot create the headless Service: {e}"));
        }
        let host = self.env.host("headless");
        if let Err(e) = self.create_route("headless", &host, None, Some(("svc-h", 80))).await {
            return Outcome::Infra(format!("cannot create the HTTPRoute: {e}"));
        }
        let t = Instant::now();
        let mut own = (0, String::new());
        while t.elapsed() < self.env.route_wait {
            own = match fetch(&self.env.router, &host, "/").await {
                Ok(Some(r)) => (r.status, r.text()),
                Ok(None) => return Outcome::Fail(format!("{host}: closed with no response, expected the router's 503")),
                Err(e) => return fail_io(&host, e),
            };
            if own.0 == 503 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if own.0 != 503 || !own.1.contains("no healthy endpoint") {
            return Outcome::Fail(format!("{host} answered {} {:?}, expected the router's 503", own.0, own.1));
        }
        match fetch(&self.env.router, &self.ha, "/").await {
            Ok(Some(r)) if r.status == 200 && r.backend().is_some() => {
                Outcome::Pass(format!("{host} is a 503 (no endpoints) after {} ms; {} is still served", t.elapsed().as_millis(), self.ha))
            }
            Ok(Some(r)) => Outcome::Fail(format!("the other routes broke: {} answered {} {:?}", self.ha, r.status, r.text())),
            Ok(None) => Outcome::Fail(format!("the other routes broke: {} closed with no response", self.ha)),
            Err(e) => fail_io(&self.ha, e),
        }
    }

    /// The storefront's shape (#37): `/api/` to one Service, the rest split
    /// 3:1 between two, the longest prefix winning although `/` comes first.
    async fn rules_and_weights(&self) -> Outcome {
        for (n, be) in [("svc-wa", &self.a), ("svc-wb", &self.b)] {
            if let Err(e) = self.service(n, be.port, &[(self.ip, be.port, true)], json!({})).await {
                return Outcome::Infra(e);
            }
        }
        let host = self.env.host("rules");
        let rules = json!([
            {"matches": [{"path": {"type": "PathPrefix", "value": "/"}}],
             "backendRefs": [{"name": "svc-wa", "port": self.a.port, "weight": 3}, {"name": "svc-wb", "port": self.b.port, "weight": 1}],
             "timeouts": {"request": "30s"}},
            {"matches": [{"path": {"type": "PathPrefix", "value": "/api/"}}],
             "backendRefs": [{"name": "svc-wb", "port": self.b.port}]}
        ]);
        if let Err(e) = self.rules_route("rules", &host, rules).await {
            return Outcome::Infra(e);
        }
        if let Err(e) = probe::served(&self.env.router, &host, "b", self.env.route_wait).await {
            return Outcome::Fail(format!("never served: {e}"));
        }
        for i in 0..8 {
            match fetch(&self.env.router, &host, &format!("/api/items/{i}")).await {
                Ok(Some(r)) if r.backend().as_deref() == Some("b") => {}
                other => return Outcome::Fail(format!("/api/items/{i}: {:?}, want backend b", other.map(|o| o.map(|r| (r.status, r.text())))))
            }
        }
        let (mut a, mut b) = (0, 0);
        for i in 0..40 {
            match fetch(&self.env.router, &host, &format!("/page/{i}")).await {
                Ok(Some(r)) if r.backend().as_deref() == Some("a") => a += 1,
                Ok(Some(r)) if r.backend().as_deref() == Some("b") => b += 1,
                other => return Outcome::Fail(format!("/page/{i}: {:?}", other.map(|o| o.map(|r| (r.status, r.text()))))),
            }
        }
        // 3:1 is exact between refreshes; a refresh restarts the rotation.
        if (24..=34).contains(&a) && b >= 6 {
            Outcome::Pass(format!("/api/ x8 all on b; / x40 split a={a} b={b} (weights 3:1)"))
        } else {
            Outcome::Fail(format!("/ x40 split a={a} b={b}, want about 30:10 (weights 3:1)"))
        }
    }

    /// An endpoint that refuses connections costs no request: the router
    /// moves on to another endpoint and ejects the dead one.
    async fn failover(&self) -> Outcome {
        let dead = match std::net::TcpListener::bind(join(&self.ip.to_string(), 0)) {
            Ok(l) => l.local_addr().map(|a| a.port()).unwrap_or(0),
            Err(e) => return Outcome::Infra(format!("cannot find a free port: {e}")),
        };
        // One Service port, two endpoint ports: two subsets.
        let addrs = [(self.ip, self.a.port, true), (self.ip, dead, true)];
        if let Err(e) = self.service("svc-f", 80, &addrs, json!({})).await {
            return Outcome::Infra(e);
        }
        let host = self.env.host("failover");
        if let Err(e) = self.create_route("failover", &host, None, Some(("svc-f", 80))).await {
            return Outcome::Infra(format!("cannot create the HTTPRoute: {e}"));
        }
        if let Err(e) = probe::served(&self.env.router, &host, "a", self.env.route_wait).await {
            return Outcome::Fail(format!("never served: {e}"));
        }
        for i in 0..20 {
            match fetch(&self.env.router, &host, &format!("/f{i}")).await {
                Ok(Some(r)) if r.backend().as_deref() == Some("a") => {}
                other => return Outcome::Fail(format!("request {i}: {:?}", other.map(|o| o.map(|r| (r.status, r.text()))))),
            }
        }
        Outcome::Pass(format!("20 requests over endpoints a and a dead port {dead}: all answered by a"))
    }

    /// The route's status says what the router made of it (written by
    /// every node's router into `status.parents`, controller storm.io/stormlb).
    async fn route_status(&self) -> Outcome {
        let path = format!("{}/rules", self.api.routes());
        let t = Instant::now();
        let mut last = Value::Null;
        while t.elapsed() < self.env.route_wait {
            match self.api.get(&path).await {
                Ok(Some(r)) => {
                    let mine = r["status"]["parents"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .find(|p| p["controllerName"].as_str() == Some("storm.io/stormlb"))
                        .cloned();
                    if let Some(p) = mine {
                        let cond = |t: &str| p["conditions"].as_array().into_iter().flatten().find(|c| c["type"] == t).cloned().unwrap_or(Value::Null);
                        let (acc, refs, health) = (cond("Accepted"), cond("ResolvedRefs"), cond("storm.io/BackendsHealthy"));
                        if acc["status"] == "True" && refs["status"] == "True" && health["status"] == "True" {
                            return Outcome::Pass(format!("Accepted, ResolvedRefs, BackendsHealthy: {}", health["message"].as_str().unwrap_or("")));
                        }
                        last = p;
                    } else {
                        last = r["status"].clone();
                    }
                }
                Ok(None) => return Outcome::Fail(format!("{path} is gone")),
                Err(e) => return Outcome::Infra(e),
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Outcome::Fail(format!(
            "no storm.io/stormlb status with every condition True after {} s (an apiserver refusing httproutes/status \
             leaves none: rustkube#348); last: {last}",
            self.env.route_wait.as_secs()
        ))
    }

    async fn many_hosts(&self) -> Outcome {
        let mut want = Vec::new();
        for i in 0..MANY {
            let host = self.env.host(&format!("m{i}"));
            let (name, be) = if i % 2 == 0 { ("a", &self.a) } else { ("b", &self.b) };
            if let Err(e) = self.create_route(&format!("m{i}"), &host, Some(be.addr.as_str()), None).await {
                return Outcome::Infra(format!("cannot create HTTPRoute m{i}: {e}"));
            }
            want.push((host, name.to_string()));
        }
        let t = Instant::now();
        let mut pending = want.clone();
        let mut last = String::new();
        while !pending.is_empty() && t.elapsed() < self.env.route_wait {
            let mut still = Vec::new();
            for (h, be) in pending {
                match fetch(&self.env.router, &h, "/id").await {
                    Ok(Some(r)) if r.backend().as_deref() == Some(be.as_str()) && field(&r.text(), "host").as_deref() == Some(h.as_str()) => {}
                    Ok(Some(r)) => {
                        last = format!("{h}: {} {:?}", r.status, r.text().trim());
                        still.push((h, be));
                    }
                    other => {
                        last = format!("{h}: {:?}", other.map(|o| o.map(|r| r.status)));
                        still.push((h, be));
                    }
                }
            }
            pending = still;
            if !pending.is_empty() {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
        if !pending.is_empty() {
            return Outcome::Fail(format!("{} of {MANY} hosts not served right after {} s; last: {last}", pending.len(), self.env.route_wait.as_secs()));
        }
        *self.many.lock().unwrap() = want;
        Outcome::Pass(format!("{MANY} hosts, alternating two backends, all right after {} ms", t.elapsed().as_millis()))
    }

    async fn concurrent(&self) -> Outcome {
        let want = self.many.lock().unwrap().clone();
        // Up to four descriptors a request where the router shares this
        // process (the harness), plus its idle backend connections.
        let n = fd_limit().map(|l| (l.saturating_sub(192) / 5) as usize).unwrap_or(200).clamp(8, 200);
        let mut set = JoinSet::new();
        for i in 0..n {
            let (h, be) = want[i % want.len()].clone();
            let router = self.env.router.clone();
            set.spawn(async move {
                match fetch(&router, &h, "/id").await {
                    Ok(Some(r)) if r.backend().as_deref() == Some(be.as_str()) => Ok(()),
                    Ok(Some(r)) => Err(format!("{h}: {} {:?}", r.status, r.text().trim())),
                    Ok(None) => Err(format!("{h}: closed with no response")),
                    Err(e) => Err(format!("{h}: {e}")),
                }
            });
        }
        let (mut ok, mut bad) = (0, Vec::new());
        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok(())) => ok += 1,
                Ok(Err(e)) => bad.push(e),
                Err(e) => bad.push(e.to_string()),
            }
        }
        if bad.is_empty() {
            Outcome::Pass(format!("{ok} concurrent connections over {} hosts, every one on its own backend", want.len()))
        } else {
            Outcome::Fail(format!("{} of {n} wrong; first: {}", bad.len(), bad[0]))
        }
    }

    async fn deleted(&self) -> Outcome {
        let coll = self.api.routes();
        let routes = match self.api.mine(&coll).await {
            Ok(r) => r,
            Err(e) => return Outcome::Infra(e),
        };
        for r in &routes {
            if let Some(n) = r["metadata"]["name"].as_str() {
                if let Err(e) = self.api.delete(&format!("{coll}/{n}")).await {
                    return Outcome::Infra(e);
                }
            }
        }
        let hosts = self.hosts.lock().unwrap().clone();
        let t = Instant::now();
        for h in &hosts {
            let left = self.env.route_wait.saturating_sub(t.elapsed()).max(Duration::from_secs(1));
            if let Err(e) = probe::unrouted(&self.env.router, h, left).await {
                return Outcome::Fail(e);
            }
        }
        Outcome::Pass(format!("{} routes deleted; all {} hosts 404 after {} ms", routes.len(), hosts.len(), t.elapsed().as_millis()))
    }
}

fn hits(b: &Backend) -> u64 {
    b.hits.load(std::sync::atomic::Ordering::Relaxed)
}
