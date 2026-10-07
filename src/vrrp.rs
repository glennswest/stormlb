//! VRRP v3 (RFC 5798) — virtual-router state machine + advertisement codec.
//!
//! [`VirtualRouter`] is the state machine alone — which node holds the VIP
//! and when it transitions Master/Backup — so every rule is unit-testable
//! without root or a peer: each event returns what the caller must do. [`run`]
//! is the wire path around it: a raw IPPROTO_VRRP socket joined to
//! 224.0.0.18, advertisements while Master, the master-down timer while
//! Backup, and VIP claim/release through [`crate::vip::VipController`].
//!
//! Per RFC 5798 §6.4: a Backup preempts a lower-priority Master (it discards
//! that Master's advertisements, so its master-down timer runs out) unless
//! `preempt` is off; a Backup learns the Master's advertisement interval; a
//! priority-0 advertisement (a Master resigning) cuts the wait to Skew_Time,
//! and a Master that stops sends one. Beyond the RFC, ownership follows
//! backend health: a Master with no healthy backend resigns, and a Backup
//! with none never takes over, so the VIP sits where it can be served.

use crate::vip::VipController;
use anyhow::{Context, Result};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub use crate::vip::interface_ipv4;

/// The link-local multicast group all VRRP routers listen on (RFC 5798).
const VRRP_MCAST: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 18);
/// IP protocol number assigned to VRRP.
const IPPROTO_VRRP: i32 = 112;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Initialize,
    Backup,
    Master,
}

/// Whether this node may hold the VIP now (it has a healthy backend).
pub type Healthy = Arc<dyn Fn() -> bool + Send + Sync>;

/// A running VRRP loop's handle: stop it, and read where it stands.
#[derive(Default)]
pub struct Handle {
    stop: AtomicBool,
    /// 0 not running, 1 Backup, 2 Master.
    state: AtomicU8,
}

impl Handle {
    /// Ask the loop to stop. A Master sends priority 0 (so a Backup takes
    /// over after Skew_Time, not the full master-down time) and releases the
    /// VIP, within about half a second.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
    /// `"master"`, `"backup"`, or `"stopped"` (not started, stopped, or failed).
    pub fn state(&self) -> &'static str {
        match self.state.load(Ordering::Relaxed) {
            1 => "backup",
            2 => "master",
            _ => "stopped",
        }
    }
    fn set(&self, s: State) {
        let v = match s {
            State::Backup => 1,
            State::Master => 2,
            State::Initialize => 0,
        };
        self.state.store(v, Ordering::Relaxed);
    }
}

/// What the caller does after an advertisement arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnAdvert {
    /// Nothing (a Master ignoring a lower peer; a preempting Backup
    /// discarding a lower-priority Master, so its timer runs out).
    Ignore,
    /// Backup: restart the master-down timer at this many centiseconds.
    ResetTimer(u32),
    /// Master: advertise now (a peer resigned).
    AdvertiseNow,
    /// Master → Backup: release the VIP, restart the master-down timer at
    /// this many centiseconds.
    Demoted(u32),
}

/// One VRRP virtual router instance.
#[derive(Debug, Clone)]
pub struct VirtualRouter {
    pub vrid: u8,
    /// 1..=255; higher wins. 255 = the VIP's address owner (starts Master).
    pub priority: u8,
    /// A Backup takes over from a lower-priority Master (RFC default).
    pub preempt: bool,
    /// Our advertisement interval, in centiseconds (VRRPv3 unit).
    pub advert_interval_centis: u16,
    /// The Master's advertisement interval, learned from its adverts while
    /// Backup (RFC 5798 Master_Adver_Interval).
    pub master_adver_interval_centis: u16,
    pub state: State,
}

impl VirtualRouter {
    pub fn new(vrid: u8, priority: u8, preempt: bool, advert_interval_secs: u64) -> Self {
        let centis = (advert_interval_secs.max(1) * 100).min(0x0fff) as u16;
        Self {
            vrid,
            priority,
            preempt,
            advert_interval_centis: centis,
            master_adver_interval_centis: centis,
            state: State::Initialize,
        }
    }

    /// Skew_Time = ((256 - priority) * Master_Adver_Interval) / 256.
    pub fn skew_centis(&self) -> u32 {
        ((256 - self.priority as u32) * self.master_adver_interval_centis as u32) / 256
    }

    /// Master_Down_Interval = 3 * Master_Adver_Interval + Skew_Time
    /// (RFC 5798 §6.1).
    pub fn master_down_interval_centis(&self) -> u32 {
        3 * self.master_adver_interval_centis as u32 + self.skew_centis()
    }

    /// Startup: the address owner (priority 255) goes straight to Master —
    /// if it can serve; everyone else starts Backup and waits out the timer.
    pub fn start(&mut self, healthy: bool) -> State {
        self.state = if self.priority == 255 && healthy { State::Master } else { State::Backup };
        self.state
    }

    /// The master-down timer ran out while Backup. Returns true if we now
    /// take over (claim, advertise); false while we have no healthy backend
    /// (the caller restarts the timer).
    pub fn on_master_down_timeout(&mut self, healthy: bool) -> bool {
        if self.state == State::Backup && healthy {
            self.state = State::Master;
            self.master_adver_interval_centis = self.advert_interval_centis;
            return true;
        }
        false
    }

    /// Backend health, checked every turn. Returns true if a Master must
    /// resign now (send priority 0, release, become Backup).
    pub fn on_health(&mut self, healthy: bool) -> bool {
        if self.state == State::Master && !healthy {
            self.state = State::Backup;
            return true;
        }
        false
    }

    /// An advertisement from a peer (RFC 5798 §6.4.2, §6.4.3).
    pub fn on_advertisement(&mut self, peer_priority: u8, peer_addr: Ipv4Addr, our_addr: Ipv4Addr, peer_interval_centis: u16) -> OnAdvert {
        match self.state {
            State::Backup => {
                if peer_priority == 0 {
                    // The Master resigned: wait only Skew_Time.
                    OnAdvert::ResetTimer(self.skew_centis())
                } else if !self.preempt || peer_priority >= self.priority {
                    self.master_adver_interval_centis = peer_interval_centis.max(1);
                    OnAdvert::ResetTimer(self.master_down_interval_centis())
                } else {
                    // A lower-priority Master: discard, so the timer runs
                    // out and we preempt it.
                    OnAdvert::Ignore
                }
            }
            State::Master => {
                if peer_priority == 0 {
                    OnAdvert::AdvertiseNow
                } else if peer_priority > self.priority || (peer_priority == self.priority && peer_addr > our_addr) {
                    self.state = State::Backup;
                    self.master_adver_interval_centis = peer_interval_centis.max(1);
                    OnAdvert::Demoted(self.master_down_interval_centis())
                } else {
                    OnAdvert::Ignore
                }
            }
            State::Initialize => OnAdvert::Ignore,
        }
    }
}

/// Encode a VRRP v3 IPv4 advertisement (the VRRP payload, not the IP header).
/// Layout (RFC 5798 §5.1): version+type, vrid, priority, count, rsvd+max-advert,
/// checksum, then the VIP list. The checksum covers the IPv4 pseudo-header
/// (`src`, 224.0.0.18, protocol 112, length) as §5.2.8 requires.
pub fn encode_advertisement(vrid: u8, priority: u8, advert_interval_centis: u16, vips: &[Ipv4Addr], src: Ipv4Addr) -> Vec<u8> {
    let mut p = Vec::with_capacity(8 + vips.len() * 4);
    p.push(0x31); // version 3 (high nibble), type 1 = Advertisement (low nibble)
    p.push(vrid);
    p.push(priority);
    p.push(vips.len() as u8); // Count IPvX Addr
    // Rsvd (4 bits) + Max Advertise Interval (12 bits), in centiseconds.
    let mai = advert_interval_centis & 0x0fff;
    p.extend_from_slice(&mai.to_be_bytes());
    p.extend_from_slice(&[0, 0]); // checksum placeholder
    for ip in vips {
        p.extend_from_slice(&ip.octets());
    }
    let ck = ones_complement_checksum(&with_pseudo_header(&p, src));
    p[6..8].copy_from_slice(&ck.to_be_bytes());
    p
}

/// The IPv4 pseudo-header followed by the VRRP payload, for the checksum.
fn with_pseudo_header(vrrp: &[u8], src: Ipv4Addr) -> Vec<u8> {
    let mut b = Vec::with_capacity(12 + vrrp.len());
    b.extend_from_slice(&src.octets());
    b.extend_from_slice(&VRRP_MCAST.octets());
    b.push(0);
    b.push(IPPROTO_VRRP as u8);
    b.extend_from_slice(&(vrrp.len() as u16).to_be_bytes());
    b.extend_from_slice(vrrp);
    b
}

/// Standard 16-bit one's-complement checksum over the buffer.
fn ones_complement_checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let mut chunks = data.chunks_exact(2);
    for c in &mut chunks {
        sum += u16::from_be_bytes([c[0], c[1]]) as u32;
    }
    if let [last] = chunks.remainder() {
        sum += (*last as u32) << 8;
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// A received advertisement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Advert {
    pub priority: u8,
    pub src: Ipv4Addr,
    pub interval_centis: u16,
}

/// Parse a received datagram (IP header + VRRP payload, as delivered by a raw
/// IPPROTO_VRRP socket) if it is a valid v3 advertisement for `our_vrid`:
/// IP TTL 255 (it was not routed, §5.1.1.3) and a correct checksum.
pub fn parse_advertisement(datagram: &[u8], our_vrid: u8) -> Option<Advert> {
    if datagram.len() < 20 {
        return None;
    }
    let ihl = ((datagram[0] & 0x0f) as usize) * 4;
    if ihl < 20 || datagram.len() < ihl + 8 || datagram[8] != 255 {
        return None;
    }
    let src = Ipv4Addr::new(datagram[12], datagram[13], datagram[14], datagram[15]);
    let vrrp = &datagram[ihl..];
    let version = vrrp[0] >> 4;
    let vtype = vrrp[0] & 0x0f;
    if version != 3 || vtype != 1 || vrrp[1] != our_vrid {
        return None;
    }
    if ones_complement_checksum(&with_pseudo_header(vrrp, src)) != 0 {
        return None;
    }
    Some(Advert { priority: vrrp[2], src, interval_centis: u16::from_be_bytes([vrrp[4], vrrp[5]]) & 0x0fff })
}

/// One VRRP instance's settings.
#[derive(Debug, Clone)]
pub struct Params {
    pub vrid: u8,
    pub priority: u8,
    pub preempt: bool,
    pub advert_interval_secs: u64,
    pub iface: String,
    pub vip: Ipv4Addr,
}

/// Run the VRRP control loop for one virtual router. Blocking — intended to be
/// spawned on a dedicated OS thread. Sends advertisements while Master, watches
/// for the master-down timeout while Backup, and claims/releases the VIP through
/// `ctl` on transitions; `healthy` is asked every turn. Requires CAP_NET_ADMIN /
/// CAP_NET_RAW. Returns when `handle` is stopped, after resigning if Master.
pub fn run(p: Params, ctl: Arc<dyn VipController>, healthy: Healthy, handle: Arc<Handle>) -> Result<()> {
    let r = run_loop(&p, ctl, healthy, &handle);
    handle.set(State::Initialize);
    r
}

fn run_loop(p: &Params, ctl: Arc<dyn VipController>, healthy: Healthy, handle: &Handle) -> Result<()> {
    let (vrid, iface, vip) = (p.vrid, p.iface.as_str(), p.vip);
    let iface_ip = interface_ipv4(iface)?;
    let sock = Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::from(IPPROTO_VRRP)))
        .context("creating VRRP raw socket (needs CAP_NET_RAW)")?;
    sock.set_multicast_if_v4(&iface_ip)?;
    sock.set_multicast_ttl_v4(255)?;
    sock.set_multicast_loop_v4(false)?;
    sock.join_multicast_v4(&VRRP_MCAST, &iface_ip).context("joining 224.0.0.18")?;

    let mut vr = VirtualRouter::new(vrid, p.priority, p.preempt, p.advert_interval_secs);
    let interval_centis = vr.advert_interval_centis;
    let advert_interval = Duration::from_millis(interval_centis as u64 * 10);
    let centis = |c: u32| Duration::from_millis(c as u64 * 10);
    let vip_s = vip.to_string();
    let dst = SockAddr::from(SocketAddrV4::new(VRRP_MCAST, 0));
    let send = |prio: u8| {
        let pkt = encode_advertisement(vrid, prio, interval_centis, &[vip], iface_ip);
        if let Err(e) = sock.send_to(&pkt, &dst) {
            warn!("VRRP vrid={vrid}: send advert failed: {e}");
        }
    };

    let state = vr.start(healthy());
    handle.set(state);
    info!("VRRP vrid={vrid} priority={} preempt={} iface={iface} vip={vip} -> {state:?}", p.priority, p.preempt);
    let mut next_advert = Instant::now();
    let mut master_down_deadline = Instant::now() + centis(vr.master_down_interval_centis());
    if state == State::Master {
        if let Err(e) = ctl.claim(&vip_s, iface) {
            warn!("VRRP: could not claim VIP {vip} on {iface}: {e:#}");
        }
    }

    let mut buf = [MaybeUninit::<u8>::uninit(); 512];
    loop {
        if handle.stop.load(Ordering::Relaxed) {
            if vr.state == State::Master {
                info!("VRRP vrid={vrid}: stopped — resigning, releasing VIP {vip}");
                send(0);
                let _ = ctl.release(&vip_s, iface);
            }
            return Ok(());
        }
        if vr.on_health(healthy()) {
            info!("VRRP vrid={vrid}: no healthy backend — resigning, releasing VIP {vip}");
            send(0);
            let _ = ctl.release(&vip_s, iface);
            master_down_deadline = Instant::now() + centis(vr.master_down_interval_centis());
        }
        handle.set(vr.state);
        // Wake for the next advert (Master) or the master-down deadline
        // (Backup), and at least every 500 ms to see a stop or a health change.
        let now = Instant::now();
        let wake = if vr.state == State::Master { next_advert } else { master_down_deadline };
        let timeout = wake.saturating_duration_since(now).clamp(Duration::from_millis(10), Duration::from_millis(500));
        sock.set_read_timeout(Some(timeout))?;

        match sock.recv_from(&mut buf) {
            Ok((n, _from)) => {
                // SAFETY: recv_from initialised the first n bytes.
                let data = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, n) };
                let Some(a) = parse_advertisement(data, vrid) else { continue };
                if a.src == iface_ip {
                    continue; // our own advert (shouldn't happen with loop off)
                }
                match vr.on_advertisement(a.priority, a.src, iface_ip, a.interval_centis) {
                    OnAdvert::Ignore => {}
                    OnAdvert::ResetTimer(c) => master_down_deadline = Instant::now() + centis(c),
                    OnAdvert::AdvertiseNow => next_advert = Instant::now(),
                    OnAdvert::Demoted(c) => {
                        info!("VRRP vrid={vrid}: preempted by {} (prio {}) — releasing VIP", a.src, a.priority);
                        let _ = ctl.release(&vip_s, iface);
                        master_down_deadline = Instant::now() + centis(c);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                let now = Instant::now();
                match vr.state {
                    State::Master if now >= next_advert => {
                        send(p.priority);
                        next_advert = now + advert_interval;
                    }
                    State::Backup if now >= master_down_deadline => {
                        if vr.on_master_down_timeout(healthy()) {
                            info!("VRRP vrid={vrid}: master-down — becoming MASTER, claiming VIP {vip}");
                            if let Err(e) = ctl.claim(&vip_s, iface) {
                                warn!("VRRP: could not claim VIP: {e:#}");
                            }
                            next_advert = now; // advertise immediately
                        } else {
                            master_down_deadline = now + centis(vr.master_down_interval_centis());
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => return Err(e).context("VRRP recv"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: Ipv4Addr = Ipv4Addr::new(192, 168, 8, 10);
    const B: Ipv4Addr = Ipv4Addr::new(192, 168, 8, 20);

    fn master(prio: u8, preempt: bool) -> VirtualRouter {
        let mut vr = VirtualRouter::new(51, prio, preempt, 1);
        vr.start(true);
        assert!(vr.on_master_down_timeout(true));
        vr
    }

    #[test]
    fn address_owner_starts_master_only_when_healthy() {
        assert_eq!(VirtualRouter::new(51, 255, true, 1).start(true), State::Master);
        assert_eq!(VirtualRouter::new(51, 255, true, 1).start(false), State::Backup);
        assert_eq!(VirtualRouter::new(51, 100, true, 1).start(true), State::Backup);
    }

    #[test]
    fn backup_takes_over_on_timeout_only_when_healthy() {
        let mut vr = VirtualRouter::new(51, 100, true, 1);
        vr.start(true);
        assert!(!vr.on_master_down_timeout(false), "no healthy backend: stay Backup");
        assert_eq!(vr.state, State::Backup);
        assert!(vr.on_master_down_timeout(true));
        assert_eq!(vr.state, State::Master);
        assert!(!vr.on_master_down_timeout(true), "idempotent once Master");
    }

    #[test]
    fn a_backup_preempts_a_lower_priority_master() {
        let mut vr = VirtualRouter::new(51, 200, true, 1);
        vr.start(true);
        // The Master is 100: its adverts are discarded, so the timer runs out.
        assert_eq!(vr.on_advertisement(100, A, B, 100), OnAdvert::Ignore);
        // A Master at or above us resets the timer.
        assert_eq!(vr.on_advertisement(200, A, B, 100), OnAdvert::ResetTimer(vr.master_down_interval_centis()));
        assert_eq!(vr.on_advertisement(250, A, B, 100), OnAdvert::ResetTimer(vr.master_down_interval_centis()));
        // The timer runs out: we take over, and the old Master yields to our adverts.
        assert!(vr.on_master_down_timeout(true));
        let mut old = master(100, true);
        assert!(matches!(old.on_advertisement(200, B, A, 100), OnAdvert::Demoted(_)));
        assert_eq!(old.state, State::Backup);
    }

    #[test]
    fn without_preempt_a_backup_waits_for_any_master() {
        let mut vr = VirtualRouter::new(51, 200, false, 1);
        vr.start(true);
        assert!(matches!(vr.on_advertisement(100, A, B, 100), OnAdvert::ResetTimer(_)));
    }

    #[test]
    fn a_backup_learns_the_masters_interval() {
        let mut vr = VirtualRouter::new(51, 100, true, 1);
        vr.start(true);
        // Master advertises every 3 s: down = 3*300 + (156*300)/256 = 1082.
        assert_eq!(vr.on_advertisement(200, A, B, 300), OnAdvert::ResetTimer(1082));
        assert_eq!(vr.master_adver_interval_centis, 300);
        // Taking over goes back to our own interval.
        assert!(vr.on_master_down_timeout(true));
        assert_eq!(vr.master_adver_interval_centis, 100);
    }

    #[test]
    fn priority_zero_cuts_a_backups_wait_to_skew_time() {
        let mut vr = VirtualRouter::new(51, 100, true, 1);
        vr.start(true);
        // skew = (156*100)/256 = 60 cs, not the 360 cs master-down.
        assert_eq!(vr.on_advertisement(0, A, B, 100), OnAdvert::ResetTimer(60));
        let mut m = master(100, true);
        assert_eq!(m.on_advertisement(0, A, B, 100), OnAdvert::AdvertiseNow, "a Master answers a resign at once");
        assert_eq!(m.state, State::Master);
    }

    #[test]
    fn a_master_with_no_healthy_backend_resigns() {
        let mut m = master(200, true);
        assert!(!m.on_health(true));
        assert!(m.on_health(false), "resign: priority 0 and release");
        assert_eq!(m.state, State::Backup);
        assert!(!m.on_health(false), "only once");
        // Health back: a Backup again, it takes over on the next timeout.
        assert!(m.on_master_down_timeout(true));
    }

    #[test]
    fn a_master_yields_to_higher_priority_and_ties_on_address() {
        let mut m = master(100, true);
        assert_eq!(m.on_advertisement(50, B, A, 100), OnAdvert::Ignore);
        assert_eq!(m.state, State::Master);
        assert!(matches!(m.on_advertisement(100, B, A, 100), OnAdvert::Demoted(_)), "equal priority, higher address wins");
        let mut m = master(100, true);
        assert_eq!(m.on_advertisement(100, A, B, 100), OnAdvert::Ignore, "equal priority, lower address loses");
    }

    #[test]
    fn master_down_interval_matches_rfc() {
        // advert=100cs, priority=100: skew=((256-100)*100)/256 = 60; 3*100+60=360
        let vr = VirtualRouter::new(51, 100, true, 1);
        assert_eq!(vr.master_down_interval_centis(), 360);
    }

    fn datagram(vrrp: &[u8], src: [u8; 4], ttl: u8) -> Vec<u8> {
        let mut dg = vec![0u8; 20];
        dg[0] = 0x45; // IPv4, IHL=5 (20-byte header)
        dg[8] = ttl;
        dg[12..16].copy_from_slice(&src);
        dg.extend_from_slice(vrrp);
        dg
    }

    #[test]
    fn parses_advertisement_from_raw_datagram() {
        let src = Ipv4Addr::new(192, 168, 8, 11);
        let vrrp = encode_advertisement(51, 200, 100, &[Ipv4Addr::new(192, 168, 8, 50)], src);
        let dg = datagram(&vrrp, src.octets(), 255);
        assert_eq!(parse_advertisement(&dg, 51), Some(Advert { priority: 200, src, interval_centis: 100 }));
        // Wrong vrid, a too-short datagram, a routed one (TTL < 255), a
        // checksum made for another source: all None.
        assert!(parse_advertisement(&dg, 99).is_none());
        assert!(parse_advertisement(&dg[..10], 51).is_none());
        assert!(parse_advertisement(&datagram(&vrrp, src.octets(), 254), 51).is_none());
        assert!(parse_advertisement(&datagram(&vrrp, [192, 168, 8, 12], 255), 51).is_none());
    }

    #[test]
    fn advertisement_encodes_with_a_pseudo_header_checksum() {
        let vips = [Ipv4Addr::new(192, 168, 8, 50)];
        let src = Ipv4Addr::new(192, 168, 8, 11);
        let pkt = encode_advertisement(51, 200, 100, &vips, src);
        assert_eq!(pkt[0], 0x31); // v3, advertisement
        assert_eq!(pkt[1], 51); // vrid
        assert_eq!(pkt[2], 200); // priority
        assert_eq!(pkt[3], 1); // one VIP
        assert_eq!(u16::from_be_bytes([pkt[4], pkt[5]]), 100);
        assert_eq!(&pkt[8..12], &[192, 168, 8, 50]);
        // Over the pseudo-header and the packet, the sum checks to zero; over
        // the packet alone (the old, non-RFC checksum) it does not.
        assert_eq!(ones_complement_checksum(&with_pseudo_header(&pkt, src)), 0);
        assert_ne!(ones_complement_checksum(&pkt), 0);
    }
}
