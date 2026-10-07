//! Backend pool with round-robin selection over healthy members.
//!
//! The member list can be replaced at runtime ([`Pool::set_addrs`]): a backend
//! that stays keeps its health, so re-applying the same masters never blacks
//! the VIP out, and connections already proxied are not touched.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

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
    backends: RwLock<Vec<Arc<Backend>>>,
    next: AtomicUsize,
}

impl Pool {
    pub fn new(addrs: impl IntoIterator<Item = SocketAddr>) -> Self {
        let pool = Self {
            backends: RwLock::new(Vec::new()),
            next: AtomicUsize::new(0),
        };
        pool.set_addrs(addrs);
        pool
    }

    /// The current members, in order.
    pub fn backends(&self) -> Vec<Arc<Backend>> {
        self.backends.read().unwrap().clone()
    }

    /// Replace the members. An address already in the pool keeps its
    /// [`Backend`] (and so its health); a new one starts unhealthy; a removed
    /// one gets no new connections. Duplicates are dropped.
    pub fn set_addrs(&self, addrs: impl IntoIterator<Item = SocketAddr>) {
        let mut cur = self.backends.write().unwrap();
        let mut next: Vec<Arc<Backend>> = Vec::new();
        for a in addrs {
            if next.iter().any(|b| b.addr == a) {
                continue;
            }
            let be = cur.iter().find(|b| b.addr == a).cloned().unwrap_or_else(|| Arc::new(Backend::new(a)));
            next.push(be);
        }
        *cur = next;
    }

    /// Pick the next healthy backend (round-robin). None when all are down.
    pub fn pick(&self) -> Option<Arc<Backend>> {
        let backends = self.backends.read().unwrap();
        let n = backends.len();
        if n == 0 {
            return None;
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        for i in 0..n {
            let be = &backends[(start.wrapping_add(i)) % n];
            if be.is_healthy() {
                return Some(be.clone());
            }
        }
        None
    }

    pub fn healthy_count(&self) -> usize {
        self.backends.read().unwrap().iter().filter(|b| b.is_healthy()).count()
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
        let b = pool.backends();
        b[0].set_healthy(true);
        b[2].set_healthy(true);
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
        pool.backends()[0].set_healthy(true);
        assert!(pool.pick().is_some());
        pool.backends()[0].set_healthy(false);
        assert!(pool.pick().is_none());
    }

    #[test]
    fn replacing_members_keeps_the_health_of_those_that_stay() {
        let pool = Pool::new([addr(1), addr(2)]);
        for b in pool.backends() {
            b.set_healthy(true);
        }
        // 1 stays, 2 goes, 3 is new (and a duplicate of 3 is dropped).
        pool.set_addrs([addr(3), addr(1), addr(3)]);
        let b = pool.backends();
        assert_eq!(b.iter().map(|b| b.addr.port()).collect::<Vec<_>>(), [3, 1]);
        assert!(!b[0].is_healthy(), "a new member starts unhealthy");
        assert!(b[1].is_healthy(), "a member that stays keeps its health");
        assert_eq!(pool.pick().unwrap().addr, addr(1));
        pool.set_addrs([]);
        assert!(pool.pick().is_none());
    }
}
