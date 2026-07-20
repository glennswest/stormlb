//! Add/remove the VIP on a local interface, and announce takeover.
//!
//! Behind a trait so the VRRP logic stays testable without root: tests use a
//! recording mock, production uses [`IpCmd`] (iproute2). A netlink-native impl
//! can replace `IpCmd` later without touching callers.

use anyhow::{Context, Result};
use std::process::Command;
use tracing::info;

pub trait VipController: Send + Sync {
    /// Bring the VIP up on `iface` (idempotent) and announce it (gratuitous ARP).
    fn claim(&self, vip: &str, iface: &str) -> Result<()>;
    /// Remove the VIP from `iface` (idempotent).
    fn release(&self, vip: &str, iface: &str) -> Result<()>;
}

/// iproute2-backed controller: `ip addr add/del <vip>/32 dev <iface>` plus a
/// gratuitous ARP via `arping` when available.
pub struct IpCmd;

impl VipController for IpCmd {
    fn claim(&self, vip: &str, iface: &str) -> Result<()> {
        info!("claiming VIP {vip} on {iface}");
        // Idempotent: ignore "exists".
        run_ok(
            Command::new("ip")
                .args(["addr", "add", &format!("{vip}/32"), "dev", iface]),
            &["File exists", "RTNETLINK answers: File exists"],
        )
        .with_context(|| format!("ip addr add {vip} dev {iface}"))?;
        // Best-effort gratuitous ARP so switches/hosts update immediately.
        let _ = Command::new("arping")
            .args(["-c", "3", "-A", "-I", iface, vip])
            .status();
        Ok(())
    }

    fn release(&self, vip: &str, iface: &str) -> Result<()> {
        info!("releasing VIP {vip} from {iface}");
        run_ok(
            Command::new("ip")
                .args(["addr", "del", &format!("{vip}/32"), "dev", iface]),
            &["Cannot assign requested address", "does not exist"],
        )
        .with_context(|| format!("ip addr del {vip} dev {iface}"))?;
        Ok(())
    }
}

/// Run a command, treating any of `tolerate` substrings in stderr as success
/// (for idempotent add/del).
fn run_ok(cmd: &mut Command, tolerate: &[&str]) -> Result<()> {
    let out = cmd.output().context("spawning iproute2")?;
    if out.status.success() {
        return Ok(());
    }
    let err = String::from_utf8_lossy(&out.stderr);
    if tolerate.iter().any(|t| err.contains(t)) {
        return Ok(());
    }
    anyhow::bail!("command failed: {}", err.trim())
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::sync::Mutex;

    /// Records claim/release calls for tests.
    #[derive(Default)]
    pub struct MockVip {
        pub events: Mutex<Vec<String>>,
    }
    impl VipController for MockVip {
        fn claim(&self, vip: &str, iface: &str) -> Result<()> {
            self.events.lock().unwrap().push(format!("claim {vip} {iface}"));
            Ok(())
        }
        fn release(&self, vip: &str, iface: &str) -> Result<()> {
            self.events
                .lock()
                .unwrap()
                .push(format!("release {vip} {iface}"));
            Ok(())
        }
    }

    #[test]
    fn mock_records_claim_and_release() {
        let m = MockVip::default();
        m.claim("192.168.8.50", "eth0").unwrap();
        m.release("192.168.8.50", "eth0").unwrap();
        let ev = m.events.lock().unwrap().clone();
        assert_eq!(ev, ["claim 192.168.8.50 eth0", "release 192.168.8.50 eth0"]);
    }
}
