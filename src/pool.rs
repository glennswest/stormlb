//! Backend pool with round-robin selection over healthy members.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// A single upstream backend (e.g. a master's apiserver `ip:6443`).
pub struct Backend {
    pub addr: SocketAddr,
    healthy: AtomicBool,
}

impl Backend {
    pub fn new(addr: SocketAddr) -> Self {
        // Start unhealthy; the first health check promotes it. This avoids
        // sending traffic to a backend we haven't verified yet.
        Self {
            addr,
            healthy: AtomicBool::new(false),
        }
    }
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }
    pub fn set_healthy(&self, v: bool) {
        self.healthy.store(v, Ordering::Relaxed);
    }
}

/// The set of backends behind a VIP, with round-robin over healthy members.
pub struct Pool {
    pub backends: Vec<Arc<Backend>>,
    next: AtomicUsize,
}

impl Pool {
    pub fn new(addrs: impl IntoIterator<Item = SocketAddr>) -> Self {
        Self {
            backends: addrs.into_iter().map(|a| Arc::new(Backend::new(a))).collect(),
            next: AtomicUsize::new(0),
        }
    }

    /// Pick the next healthy backend (round-robin). None when all are down.
    pub fn pick(&self) -> Option<Arc<Backend>> {
        let n = self.backends.len();
        if n == 0 {
            return None;
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let be = &self.backends[(start.wrapping_add(i)) % n];
            if be.is_healthy() {
                return Some(be.clone());
            }
        }
        None
    }

    pub fn healthy_count(&self) -> usize {
        self.backends.iter().filter(|b| b.is_healthy()).count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(p: u16) -> SocketAddr {
        format!("127.0.0.1:{p}").parse().unwrap()
    }

    #[test]
    fn empty_pool_picks_nothing() {
        let pool = Pool::new(Vec::<SocketAddr>::new());
        assert!(pool.pick().is_none());
    }

    #[test]
    fn skips_unhealthy_and_round_robins_healthy() {
        let pool = Pool::new([addr(1), addr(2), addr(3)]);
        assert!(pool.pick().is_none(), "all start unhealthy");
        pool.backends[0].set_healthy(true);
        pool.backends[2].set_healthy(true);
        // Only backends 0 and 2 should ever be returned, alternating.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10 {
            let be = pool.pick().unwrap();
            seen.insert(be.addr.port());
        }
        assert_eq!(seen, [1u16, 3u16].into_iter().collect());
        assert_eq!(pool.healthy_count(), 2);
    }

    #[test]
    fn all_unhealthy_returns_none() {
        let pool = Pool::new([addr(1), addr(2)]);
        pool.backends[0].set_healthy(true);
        assert!(pool.pick().is_some());
        pool.backends[0].set_healthy(false);
        assert!(pool.pick().is_none());
    }
}
