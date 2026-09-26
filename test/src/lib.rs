//! stormlb's test container (stormcentral `docs/test-standard.md`).
//!
//! What stormlb does on a node today is the router: `[router] listen =
//! "auto:80"`, a Host-header demux over Gateway API HTTPRoutes. The suites
//! test exactly that, from outside, through the API and the router's port:
//!
//! - `short` (< 2 min): `/healthz` answers, stormd reports it running, and
//!   an HTTPRoute's hostname reaches its backend and stops when deleted.
//! - `medium` (< 30 min): every behaviour README "Router" documents and
//!   every failure path, end to end.
//! - `long` (the night window): waves of routes and connections at the
//!   machine's capacity, measured for slowdown and residue across waves.
//!
//! The backends are listeners in this container (the Job is `hostNetwork`),
//! named by the route's `storm.io/backend` — the same path a node service
//! uses — so nothing but API objects is created, all in the run's namespace
//! and labelled `storm.io/test-run`. The VIP half (L4, VRRP, BGP) is not
//! shipped and is reported skip; `cargo test` covers it.

pub mod api;
pub mod backend;
pub mod env;
pub mod http;
pub mod long;
pub mod medium;
pub mod probe;
pub mod report;
pub mod short;

use env::Env;
use report::{Outcome, Report};

/// Run the suite `env.suite` names, recording into `r`.
pub async fn run(env: &Env, r: &mut Report) {
    if let Some(why) = not_started(env).await {
        r.record("stormlb-started", Outcome::Skip(why), 0, None);
        return;
    }
    match env.suite.as_str() {
        "short" => short::run(env, r).await,
        "medium" => medium::run(env, r).await,
        "long" => long::run(env, r).await,
        other => {
            r.record("suite", Outcome::Infra(format!("STORM_SUITE {other:?} is not short, medium or long")), 0, None);
        }
    }
}

/// Whether stormlb is simply not started on this node: neither the router
/// nor the stormd of its container answers. stormcos writes `start stormlb`
/// for the `sno` profile only, so on a `node` or `storage` machine that is
/// the configuration, not a fault: every suite reports one skip. A stormd
/// that answers with the router down is a fault, and the suite says so.
async fn not_started(env: &Env) -> Option<String> {
    let sd = env.stormd.as_ref()?;
    let up = |a: String| async move {
        matches!(tokio::time::timeout(std::time::Duration::from_secs(5), tokio::net::TcpStream::connect(a)).await, Ok(Ok(_)))
    };
    if up(env.router.clone()).await || up(sd.clone()).await {
        return None;
    }
    Some(format!(
        "neither the router ({}) nor its stormd ({sd}) answers: stormlb is not started on this node \
         (stormcos starts it on the sno profile)",
        env.router
    ))
}
