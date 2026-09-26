//! `short` (< 2 min): the router is up on the node and doing its main job —
//! an HTTPRoute's hostname reaches its backend, and stops reaching it when
//! the route is deleted — under the stormd that supervises it.

use crate::api::Api;
use crate::backend::Backend;
use crate::env::Env;
use crate::http::fetch;
use crate::probe;
use crate::report::{Outcome, Report};

pub async fn run(env: &Env, r: &mut Report) {
    r.run("router-healthz", healthz(env)).await;
    r.run("stormd-supervises", supervised(env)).await;

    let (api, a) = match setup(env).await {
        Ok(x) => x,
        Err(e) => {
            r.record("route-by-host", Outcome::Infra(e), 0, None);
            return;
        }
    };
    let host = env.host("short");
    let routed = r.run("route-by-host", async {
        let route = api.route("short", &[host.clone()], Some(a.addr.as_str()), None);
        if let Err(e) = api.create(&api.routes(), &route).await {
            return Outcome::Infra(format!("cannot create the HTTPRoute: {e}"));
        }
        match probe::served(&env.router, &host, "a", env.route_wait).await {
            Ok(t) => Outcome::Pass(format!("{host} -> {} after {} ms", a.addr, t.as_millis())),
            Err(e) => Outcome::Fail(e),
        }
    })
    .await;
    if !routed {
        // A route that never appeared cannot be seen to go: a 404 now would
        // be a pass that proves nothing.
        r.record("route-removed", Outcome::Fail("not run: route-by-host did not pass".into()), 0, None);
        cleanup(&api, r).await;
        return;
    }
    r.run("route-removed", async {
        if let Err(e) = api.delete(&format!("{}/short", api.routes())).await {
            return Outcome::Infra(e);
        }
        match probe::unrouted(&env.router, &host, env.route_wait).await {
            Ok(t) => Outcome::Pass(format!("404 {} ms after the delete", t.as_millis())),
            Err(e) => Outcome::Fail(e),
        }
    })
    .await;
    cleanup(&api, r).await;
}

/// The API client and a backend the router can reach.
pub async fn setup(env: &Env) -> Result<(Api, Backend), String> {
    let api = Api::new(env)?;
    let ip = env.reach_ip()?;
    let a = Backend::start("a", ip).await.map_err(|e| format!("backend listener: {e}"))?;
    Ok((api, a))
}

/// The router's own liveness answer, on a host no route claims.
pub async fn healthz(env: &Env) -> Outcome {
    let host = env.host("unclaimed");
    match fetch(&env.router, &host, "/healthz").await {
        Ok(Some(r)) if r.status == 200 && r.text().contains("router alive") => {
            Outcome::Pass(format!("{} answers 200 router alive", env.router))
        }
        Ok(Some(r)) => Outcome::Fail(format!("{} answered {} {:?}", env.router, r.status, r.text())),
        Ok(None) => Outcome::Fail(format!("{} closed with no response", env.router)),
        Err(e) => Outcome::Fail(format!("{}: {e}", env.router)),
    }
}

async fn supervised(env: &Env) -> Outcome {
    let Some(sd) = &env.stormd else {
        return Outcome::Skip("no stormd here (STORMLB_STORMD=none)".into());
    };
    match probe::stormd(sd).await {
        Ok(s) if s.running => Outcome::Pass(format!("stormd on {sd}: running, {} restarts, {} crashes", s.restarts, s.crashes)),
        Ok(s) => Outcome::Fail(format!("stormd on {sd} does not report stormlb running: {s:?}")),
        Err(e) => Outcome::Fail(e),
    }
}

/// Delete what the run made, and check nothing of it is still listed.
pub async fn cleanup(api: &Api, r: &mut Report) {
    r.run("cleanup", async {
        let n = match api.cleanup().await {
            Ok(n) => n,
            Err(e) => return Outcome::Fail(e),
        };
        match api.leftovers().await {
            Ok(0) => Outcome::Pass(format!("{n} objects deleted, none left")),
            Ok(k) => Outcome::Fail(format!("{k} objects labelled with this run are still listed")),
            Err(e) => Outcome::Fail(e),
        }
    })
    .await;
}
