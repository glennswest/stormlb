//! `long` (the night window): waves of the router's main workload — many
//! routes, and connections through them at the machine's capacity — sized
//! from the node's allocatable CPUs (read from the API) and this container's
//! open-file limit, never assumed. Each wave:
//!
//! 1. **ramp**: creates its routes, alternating two backends, and times how
//!    long until every host is served by the right one;
//! 2. **hold**: that many connections in parallel, each a fresh request to a
//!    random host of the wave, checked for the right backend, timed;
//! 3. **drain**: deletes the routes and times how long until every host is
//!    the router's 404 again;
//! 4. **residue**: no object of the run still listed, stormd saw no restart
//!    or crash, and the router's idle `/healthz` latency.
//!
//! One `wave-<n>` line per wave carries the metrics (the trend stormcentral
//! plots); a wave with a wrong answer, a leftover or a restart fails. The
//! last line, `trend`, fails on the first wave slower than the first wave of
//! its size: route programming, p99 latency, drain, or idle latency doubled
//! (plus a floor, so microseconds of jitter are not a regression).
//!
//! What it cannot see: the router's own memory and descriptors. stormd's
//! open `/metrics` reports its own, not the supervised process's.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinSet;

use crate::api::Api;
use crate::backend::Backend;
use crate::env::{fd_limit, Env};
use crate::http::fetch;
use crate::probe;
use crate::report::{Outcome, Report};
use crate::short;

/// Wave sizes, as multiples of the base, cycled so the night varies the load.
const MIX: [u64; 4] = [1, 2, 1, 4];
/// Routes per CPU per unit of mix, and connections likewise.
const ROUTES_PER_CPU: u64 = 8;
const CONNS_PER_CPU: u64 = 32;
const MAX_ROUTES: u64 = 2000;

pub async fn run(env: &Env, r: &mut Report) {
    let (api, a) = match short::setup(env).await {
        Ok(x) => x,
        Err(e) => {
            r.record("capacity", Outcome::Infra(e), 0, None);
            return;
        }
    };
    let b = match env.reach_ip() {
        Ok(ip) => match Backend::start("b", ip).await {
            Ok(b) => b,
            Err(e) => {
                r.record("capacity", Outcome::Infra(format!("backend listener: {e}")), 0, None);
                return;
            }
        },
        Err(e) => {
            r.record("capacity", Outcome::Infra(e), 0, None);
            return;
        }
    };
    let backs = [a, b];

    let (cpus, whence) = match api.node_cpus(&env.node).await {
        Ok((n, node)) => (n, format!("node {node}'s allocatable cpu")),
        Err(e) => (
            std::thread::available_parallelism().map(|n| n.get() as u64).unwrap_or(1),
            format!("this container's CPUs: the API gave none ({e})"),
        ),
    };
    let fds = fd_limit().unwrap_or(1024);
    // Each connection is two descriptors here (the client, and the backend's
    // accepted end); keep a margin for everything else.
    let max_conns = (fds.saturating_sub(256) / 3).max(8);
    r.record(
        "capacity",
        Outcome::Pass(format!("{cpus} CPUs ({whence}); open-file limit {fds}, so at most {max_conns} connections at once")),
        0,
        None,
    );

    let sd0 = match &env.stormd {
        Some(sd) => probe::stormd(sd).await.ok(),
        None => None,
    };
    let idle0 = idle_p50(env).await;
    let margin = Duration::from_secs(30).min(env.timeout / 6);
    let mut waves: Vec<Wave> = Vec::new();
    while env.remaining() > margin * 2 {
        let n = waves.len();
        let mix = MIX[n % MIX.len()];
        let routes = (ROUTES_PER_CPU * cpus * mix).clamp(4, MAX_ROUTES) as usize;
        let conns = (CONNS_PER_CPU * cpus * mix).min(max_conns) as usize;
        let hold = (env.remaining().saturating_sub(margin) / 4).clamp(Duration::from_secs(3), Duration::from_secs(120));
        let t = Instant::now();
        let w = wave(env, &api, &backs, n, mix, routes, conns, hold, sd0).await;
        let outcome = match &w.problem {
            None => Outcome::Pass(format!(
                "{routes} routes, {conns} connections for {} s: {} requests, p99 {} us",
                hold.as_secs(),
                w.requests,
                w.p99_us
            )),
            Some(p) => Outcome::Fail(p.clone()),
        };
        r.record(&format!("wave-{n}"), outcome, t.elapsed().as_millis(), Some(&w.json()));
        waves.push(w);
    }
    r.record("trend", trend(&waves, idle0), 0, None);
    short::cleanup(&api, r).await;
}

struct Wave {
    n: usize,
    mix: u64,
    routes: usize,
    conns: usize,
    route_ms: u128,
    requests: u64,
    errors: u64,
    p50_us: u64,
    p99_us: u64,
    rps: u64,
    drain_ms: u128,
    leftovers: usize,
    restarts: Option<u64>,
    idle_p50_us: Option<u64>,
    problem: Option<String>,
}

impl Wave {
    fn json(&self) -> String {
        let opt = |v: Option<u64>| v.map(|v| v.to_string()).unwrap_or_else(|| "null".into());
        format!(
            "\"wave\": {}, \"mix\": {}, \"routes\": {}, \"conns\": {}, \"route_ms\": {}, \"requests\": {}, \"errors\": {}, \
             \"p50_us\": {}, \"p99_us\": {}, \"rps\": {}, \"drain_ms\": {}, \"leftovers\": {}, \"restarts\": {}, \"idle_p50_us\": {}",
            self.n,
            self.mix,
            self.routes,
            self.conns,
            self.route_ms,
            self.requests,
            self.errors,
            self.p50_us,
            self.p99_us,
            self.rps,
            self.drain_ms,
            self.leftovers,
            opt(self.restarts),
            opt(self.idle_p50_us)
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn wave(
    env: &Env,
    api: &Api,
    backs: &[Backend; 2],
    n: usize,
    mix: u64,
    routes: usize,
    conns: usize,
    hold: Duration,
    sd0: Option<probe::Supervised>,
) -> Wave {
    let mut problems: Vec<String> = Vec::new();
    let want: Vec<(String, String)> =
        (0..routes).map(|i| (env.host(&format!("w{n}-{i}")), backs[i % 2].name.clone())).collect();

    // Ramp.
    let t = Instant::now();
    let mut set = JoinSet::new();
    for (i, (h, _)) in want.iter().enumerate() {
        let (api, h, be) = (api.clone(), h.clone(), backs[i % 2].addr.clone());
        set.spawn(async move {
            let route = api.route(&format!("w{n}-{i}"), &[h], Some(be.as_str()), None);
            api.create(&api.routes(), &route).await.map(|_| ())
        });
        if set.len() >= 32 {
            if let Some(Ok(Err(e))) = set.join_next().await {
                problems.push(format!("create: {e}"));
            }
        }
    }
    while let Some(res) = set.join_next().await {
        if let Ok(Err(e)) = res {
            problems.push(format!("create: {e}"));
        }
    }
    let ramp_wait = env.route_wait * 4 + Duration::from_millis(50 * routes as u64);
    let pending = until_all(env, &want, true, ramp_wait).await;
    let route_ms = t.elapsed().as_millis();
    if !pending.is_empty() {
        problems.push(format!("{} of {routes} hosts never served right in {} s (e.g. {})", pending.len(), ramp_wait.as_secs(), pending[0]));
    }

    // Hold.
    let end = Instant::now() + hold;
    let errors = Arc::new(AtomicU64::new(0));
    let shared = Arc::new(want.clone());
    let mut set = JoinSet::new();
    for w in 0..conns {
        let (router, want, errors) = (env.router.clone(), shared.clone(), errors.clone());
        set.spawn(async move {
            let mut h = Hist::new();
            let mut first_err = None;
            let mut i = w;
            while Instant::now() < end {
                i = i.wrapping_mul(2654435761).wrapping_add(1) % want.len();
                let (host, be) = &want[i];
                let t = Instant::now();
                let res = fetch(&router, host, "/id").await;
                match res {
                    Ok(Some(r)) if r.status == 200 && r.backend().as_deref() == Some(be.as_str()) => {
                        h.add(t.elapsed().as_micros() as u64)
                    }
                    other => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        if first_err.is_none() {
                            first_err = Some(match other {
                                Ok(Some(r)) => format!("{host}: {} {:?}", r.status, r.text().trim()),
                                Ok(None) => format!("{host}: closed with no response"),
                                Err(e) => format!("{host}: {e}"),
                            });
                        }
                    }
                }
            }
            (h, first_err)
        });
    }
    let mut hist = Hist::new();
    let mut first_err = None;
    while let Some(res) = set.join_next().await {
        if let Ok((h, e)) = res {
            hist.merge(&h);
            if first_err.is_none() {
                first_err = e;
            }
        }
    }
    let errors = errors.load(Ordering::Relaxed);
    if errors > 0 {
        problems.push(format!("{errors} wrong or failed requests; first: {}", first_err.unwrap_or_default()));
    }

    // Drain.
    let t = Instant::now();
    let coll = api.routes();
    let mut set = JoinSet::new();
    for i in 0..routes {
        let (api, path) = (api.clone(), format!("{coll}/w{n}-{i}"));
        set.spawn(async move { api.delete(&path).await });
        if set.len() >= 32 {
            if let Some(Ok(Err(e))) = set.join_next().await {
                problems.push(format!("delete: {e}"));
            }
        }
    }
    while let Some(res) = set.join_next().await {
        if let Ok(Err(e)) = res {
            problems.push(format!("delete: {e}"));
        }
    }
    let pending = until_all(env, &want, false, ramp_wait).await;
    let drain_ms = t.elapsed().as_millis();
    if !pending.is_empty() {
        problems.push(format!("{} hosts still routed {} s after their routes were deleted (e.g. {})", pending.len(), ramp_wait.as_secs(), pending[0]));
    }

    // Residue.
    let leftovers = match api.leftovers().await {
        Ok(k) => k,
        Err(e) => {
            problems.push(format!("cannot list leftovers: {e}"));
            0
        }
    };
    if leftovers > 0 {
        problems.push(format!("{leftovers} objects of this run still listed after the drain"));
    }
    let restarts = match (&env.stormd, sd0) {
        (Some(sd), Some(before)) => match probe::stormd(sd).await {
            Ok(now) => {
                let d = (now.restarts + now.crashes).saturating_sub(before.restarts + before.crashes);
                if d > 0 || !now.running {
                    problems.push(format!("stormd: {before:?} before the first wave, {now:?} now"));
                }
                Some(d)
            }
            Err(e) => {
                problems.push(e);
                None
            }
        },
        _ => None,
    };
    let idle_p50_us = idle_p50(env).await;
    if idle_p50_us.is_none() {
        problems.push("the router's /healthz did not answer after the drain".into());
    }

    let requests = hist.count();
    Wave {
        n,
        mix,
        routes,
        conns,
        route_ms,
        requests,
        errors,
        p50_us: hist.quantile(0.50),
        p99_us: hist.quantile(0.99),
        rps: requests * 1000 / (hold.as_millis().max(1) as u64),
        drain_ms,
        leftovers,
        restarts,
        idle_p50_us,
        problem: (!problems.is_empty()).then(|| problems.join("; ")),
    }
}

/// Until every host is served by its backend (`routed`), or is the router's
/// 404 (`!routed`). Returns the hosts still not so.
async fn until_all(env: &Env, want: &[(String, String)], routed: bool, wait: Duration) -> Vec<String> {
    let t = Instant::now();
    let mut pending: Vec<(String, String)> = want.to_vec();
    loop {
        let mut set = JoinSet::new();
        let mut still = Vec::new();
        for (h, be) in pending {
            let router = env.router.clone();
            set.spawn(async move {
                let ok = match fetch(&router, &h, "/id").await {
                    Ok(Some(r)) if routed => r.status == 200 && r.backend().as_deref() == Some(be.as_str()),
                    Ok(Some(r)) => r.status == 404 && r.text().contains("no route for host"),
                    _ => false,
                };
                (h, be, ok)
            });
            if set.len() >= 64 {
                if let Some(Ok((h, be, false))) = set.join_next().await {
                    still.push((h, be));
                }
            }
        }
        while let Some(res) = set.join_next().await {
            if let Ok((h, be, false)) = res {
                still.push((h, be));
            }
        }
        if still.is_empty() || t.elapsed() >= wait {
            return still.into_iter().map(|(h, _)| h).collect();
        }
        pending = still;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The router's idle latency: median of 21 sequential `/healthz`.
async fn idle_p50(env: &Env) -> Option<u64> {
    let host = env.host("unclaimed");
    let mut v = Vec::new();
    for _ in 0..21 {
        let t = Instant::now();
        if let Ok(Some(r)) = fetch(&env.router, &host, "/healthz").await {
            if r.status == 200 {
                v.push(t.elapsed().as_micros() as u64);
            }
        }
    }
    v.sort_unstable();
    v.get(v.len() / 2).copied()
}

fn trend(waves: &[Wave], idle0: Option<u64>) -> Outcome {
    if waves.is_empty() {
        return Outcome::Infra("the window left no time for a wave".into());
    }
    let worse = |now: u128, base: u128, floor: u128| now > 2 * base + floor;
    for w in waves {
        let base = waves.iter().find(|b| b.mix == w.mix).expect("w itself matches");
        let mut why = Vec::new();
        if worse(w.route_ms, base.route_ms, 5000) {
            why.push(format!("route programming {} ms, wave {} took {} ms", w.route_ms, base.n, base.route_ms));
        }
        if worse(w.p99_us as u128, base.p99_us as u128, 5000) {
            why.push(format!("p99 {} us, wave {} had {} us", w.p99_us, base.n, base.p99_us));
        }
        if worse(w.drain_ms, base.drain_ms, 5000) {
            why.push(format!("drain {} ms, wave {} took {} ms", w.drain_ms, base.n, base.drain_ms));
        }
        if let (Some(i), Some(i0)) = (w.idle_p50_us, idle0) {
            if worse(i as u128, i0 as u128, 2000) {
                why.push(format!("idle /healthz {i} us, {i0} us before the first wave"));
            }
        }
        if !why.is_empty() {
            return Outcome::Fail(format!("wave {} is the first to regress: {}", w.n, why.join("; ")));
        }
    }
    Outcome::Pass(format!("{} waves; none slower than the first wave of its size", waves.len()))
}

/// Latency in microseconds, bucketed: 8 steps per power of two (12.5%
/// resolution), fixed size so measuring allocates nothing per request.
struct Hist([u64; 512]);

impl Hist {
    fn new() -> Hist {
        Hist([0; 512])
    }
    fn index(v: u64) -> usize {
        if v < 8 {
            return v as usize;
        }
        let e = 63 - v.leading_zeros() as usize;
        (e - 2) * 8 + ((v >> (e - 3)) & 7) as usize
    }
    fn lower(i: usize) -> u64 {
        if i < 8 {
            return i as u64;
        }
        let (e, sub) = (i / 8 + 2, (i % 8) as u64);
        (8 + sub) << (e - 3)
    }
    fn add(&mut self, v: u64) {
        self.0[Self::index(v).min(511)] += 1;
    }
    fn merge(&mut self, o: &Hist) {
        for (a, b) in self.0.iter_mut().zip(o.0.iter()) {
            *a += b;
        }
    }
    fn count(&self) -> u64 {
        self.0.iter().sum()
    }
    fn quantile(&self, q: f64) -> u64 {
        let total = self.count();
        if total == 0 {
            return 0;
        }
        let target = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, c) in self.0.iter().enumerate() {
            seen += c;
            if seen >= target {
                return Self::lower(i);
            }
        }
        Self::lower(511)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_buckets_are_contiguous_and_within_an_eighth() {
        for v in [0u64, 1, 7, 8, 9, 15, 16, 17, 100, 1000, 123_456, 10_000_000] {
            let i = Hist::index(v);
            assert!(Hist::lower(i) <= v, "{v}: lower {}", Hist::lower(i));
            assert!(v - Hist::lower(i) <= v / 8, "{v}: lower {}", Hist::lower(i));
            assert!(Hist::lower(i + 1) > v, "{v}");
        }
        let mut h = Hist::new();
        (1..=100).for_each(|v| h.add(v));
        assert_eq!(h.count(), 100);
        assert!((44..=50).contains(&h.quantile(0.5)));
        assert!((88..=99).contains(&h.quantile(0.99)));
    }

    fn w(n: usize, mix: u64, route_ms: u128, p99_us: u64) -> Wave {
        Wave {
            n,
            mix,
            routes: 1,
            conns: 1,
            route_ms,
            requests: 1,
            errors: 0,
            p50_us: 1,
            p99_us,
            rps: 1,
            drain_ms: 100,
            leftovers: 0,
            restarts: Some(0),
            idle_p50_us: Some(100),
            problem: None,
        }
    }

    #[test]
    fn a_wave_is_compared_with_the_first_of_its_size() {
        assert!(matches!(trend(&[w(0, 1, 1000, 500), w(1, 2, 9000, 90_000), w(2, 1, 1100, 600)], Some(100)), Outcome::Pass(_)));
        match trend(&[w(0, 1, 1000, 500), w(1, 1, 1000, 60_000)], Some(100)) {
            Outcome::Fail(d) => assert!(d.starts_with("wave 1 is the first"), "{d}"),
            _ => panic!("p99 went from 500 us to 60 ms"),
        }
        assert!(matches!(trend(&[], None), Outcome::Infra(_)));
    }
}
