//! Watching the router from outside: until a host is served by the backend
//! it should be, until a host is a 404 again, and what stormd says about
//! the stormlb process it supervises.

use std::time::{Duration, Instant};

use crate::http::fetch;

const POLL: Duration = Duration::from_millis(200);

/// Until `host` is answered by backend `want`. Returns how long it took;
/// the error says what was seen last.
pub async fn served(router: &str, host: &str, want: &str, wait: Duration) -> Result<Duration, String> {
    let t = Instant::now();
    let mut last = String::from("nothing");
    while t.elapsed() < wait {
        match fetch(router, host, "/id").await {
            Ok(Some(r)) if r.status == 200 && r.backend().as_deref() == Some(want) => return Ok(t.elapsed()),
            Ok(Some(r)) => last = format!("{} {:?}", r.status, r.text().trim()),
            Ok(None) => last = "the connection closed with no response".into(),
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(POLL).await;
    }
    Err(format!("{host} not served by backend {want} within {} s; last: {last}", wait.as_secs()))
}

/// Until `host` is the router's own 404 again.
pub async fn unrouted(router: &str, host: &str, wait: Duration) -> Result<Duration, String> {
    let t = Instant::now();
    let mut last = String::from("nothing");
    while t.elapsed() < wait {
        match fetch(router, host, "/id").await {
            Ok(Some(r)) if r.status == 404 && r.text().contains("no route for host") => return Ok(t.elapsed()),
            Ok(Some(r)) => last = format!("{} {:?}", r.status, r.text().trim()),
            Ok(None) => last = "the connection closed with no response".into(),
            Err(e) => last = e.to_string(),
        }
        tokio::time::sleep(POLL).await;
    }
    Err(format!("{host} still routed after {} s; last: {last}", wait.as_secs()))
}

/// stormd's view of the `stormlb` process, from its open `/metrics`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Supervised {
    pub running: bool,
    pub restarts: u64,
    pub crashes: u64,
}

pub async fn stormd(addr: &str) -> Result<Supervised, String> {
    let url = format!("http://{addr}/metrics");
    let c = reqwest::Client::builder().timeout(Duration::from_secs(10)).build().map_err(|e| e.to_string())?;
    let text = c
        .get(&url)
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| format!("{url}: {e}"))?
        .text()
        .await
        .map_err(|e| format!("{url}: {e}"))?;
    parse_metrics(&text).ok_or_else(|| format!("{url} has no process=\"stormlb\""))
}

pub fn parse_metrics(text: &str) -> Option<Supervised> {
    let mine = |l: &&str| l.contains("process=\"stormlb\"");
    let value = |name: &str| {
        text.lines()
            .filter(mine)
            .find(|l| l.starts_with(&format!("{name}{{")))
            .and_then(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
    };
    let running = text
        .lines()
        .filter(mine)
        .any(|l| l.starts_with("stormd_process_state{") && l.contains("state=\"running\"") && l.ends_with(" 1"));
    let restarts = value("stormd_process_restarts_total")?;
    Some(Supervised { running, restarts: restarts as u64, crashes: value("stormd_process_crashes_total").unwrap_or(0.0) as u64 })
}

#[cfg(test)]
mod tests {
    #[test]
    fn stormd_metrics_are_read_for_stormlb_only() {
        let m = "# TYPE stormd_process_state gauge\n\
                 stormd_process_state{container=\"stormlb\",process=\"stormlb\",state=\"running\"} 1\n\
                 stormd_process_state{container=\"stormlb\",process=\"stormlb\",state=\"failed\"} 0\n\
                 stormd_process_restarts_total{container=\"stormlb\",process=\"stormlb\"} 3\n\
                 stormd_process_restarts_total{container=\"x\",process=\"other\"} 9\n\
                 stormd_process_crashes_total{container=\"stormlb\",process=\"stormlb\"} 1\n";
        let s = super::parse_metrics(m).unwrap();
        assert!(s.running);
        assert_eq!((s.restarts, s.crashes), (3, 1));
        assert!(super::parse_metrics("stormd_up 1\n").is_none());
    }
}
