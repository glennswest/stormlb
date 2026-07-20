//! VRRP v3 (RFC 5798) — virtual-router state machine + advertisement codec.
//!
//! This owns the L2 failover *logic*: which node holds the VIP and when it
//! transitions Master/Backup. The privileged wire path (a raw IPPROTO_VRRP
//! socket joining 224.0.0.18, plus gratuitous ARP on takeover) is intentionally
//! decoupled — the [`VirtualRouter`] drives events, and the caller performs the
//! actual VIP add/remove via [`crate::vip::VipController`]. This keeps the
//! protocol logic unit-testable without root or a peer. See README "VRRP wiring"
//! for the remaining socket work.

use crate::vip::VipController;
use anyhow::{Context, Result};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

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

/// One VRRP virtual router instance.
#[derive(Debug, Clone)]
pub struct VirtualRouter {
    pub vrid: u8,
    /// 1..=255; higher wins. 255 = the VIP's address owner (starts Master).
    pub priority: u8,
    /// Advertisement interval in centiseconds (RFC uses centiseconds in v3).
    pub advert_interval_centis: u16,
    pub state: State,
}

impl VirtualRouter {
    pub fn new(vrid: u8, priority: u8, advert_interval_secs: u64) -> Self {
        let centis = (advert_interval_secs.max(1) * 100).min(u16::MAX as u64) as u16;
        Self {
            vrid,
            priority,
            advert_interval_centis: centis,
            state: State::Initialize,
        }
    }

    /// Master_Down_Interval (centiseconds) = 3 * advert + skew_time,
    /// skew_time = ((256 - priority) * advert) / 256. (RFC 5798 §6.1)
    pub fn master_down_interval_centis(&self) -> u32 {
        let advert = self.advert_interval_centis as u32;
        let skew = ((256 - self.priority as u32) * advert) / 256;
        3 * advert + skew
    }

    /// Startup: the address owner (priority 255) goes straight to Master;
    /// everyone else starts as Backup and waits for the master-down timer.
    pub fn start(&mut self) -> State {
        self.state = if self.priority == 255 {
            State::Master
        } else {
            State::Backup
        };
        self.state
    }

    /// The master-down timer expired while Backup → take over as Master.
    /// Returns true if we should now claim the VIP.
    pub fn on_master_down_timeout(&mut self) -> bool {
        if self.state == State::Backup {
            self.state = State::Master;
            return true;
        }
        false
    }

    /// Received an advertisement from a peer. Returns true if this caused us to
    /// *release* the VIP (Master → Backup). In Backup the caller should reset
    /// its master-down timer. Priority 0 means the master is resigning.
    pub fn on_advertisement(&mut self, peer_priority: u8, peer_addr: Ipv4Addr, our_addr: Ipv4Addr) -> bool {
        match self.state {
            State::Master => {
                // A higher-priority peer (or equal priority with a higher IP,
                // per RFC tie-break) preempts us.
                let preempted = peer_priority > self.priority
                    || (peer_priority == self.priority && peer_addr > our_addr);
                if preempted {
                    self.state = State::Backup;
                    return true;
                }
                false
            }
            State::Backup => false, // caller resets the master-down timer
            State::Initialize => false,
        }
    }
}

/// Encode a VRRP v3 IPv4 advertisement (the VRRP payload, not the IP header).
/// Layout (RFC 5798 §5.1): version+type, vrid, priority, count, rsvd+max-advert,
/// checksum, then the VIP list.
pub fn encode_advertisement(
    vrid: u8,
    priority: u8,
    advert_interval_centis: u16,
    vips: &[Ipv4Addr],
) -> Vec<u8> {
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
    let ck = ones_complement_checksum(&p);
    p[6..8].copy_from_slice(&ck.to_be_bytes());
    p
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

/// Parse a received datagram (IP header + VRRP payload, as delivered by a raw
/// IPPROTO_VRRP socket) into `(priority, source_ip)` if it is a v3
/// advertisement for `our_vrid`. Returns None otherwise.
pub fn parse_advertisement(datagram: &[u8], our_vrid: u8) -> Option<(u8, Ipv4Addr)> {
    if datagram.len() < 20 {
        return None;
    }
    let ihl = ((datagram[0] & 0x0f) as usize) * 4;
    if ihl < 20 || datagram.len() < ihl + 8 {
        return None;
    }
    let src = Ipv4Addr::new(datagram[12], datagram[13], datagram[14], datagram[15]);
    let vrrp = &datagram[ihl..];
    let version = vrrp[0] >> 4;
    let vtype = vrrp[0] & 0x0f;
    let vrid = vrrp[1];
    let priority = vrrp[2];
    if version != 3 || vtype != 1 || vrid != our_vrid {
        return None;
    }
    Some((priority, src))
}

/// Resolve an interface's primary IPv4 address via iproute2 (iproute2 is a
/// stormcos base package). e.g. `ip -o -4 addr show dev eth0`.
pub fn interface_ipv4(iface: &str) -> Result<Ipv4Addr> {
    let out = std::process::Command::new("ip")
        .args(["-o", "-4", "addr", "show", "dev", iface])
        .output()
        .context("running `ip addr`")?;
    let text = String::from_utf8_lossy(&out.stdout);
    for tok in text.split_whitespace() {
        if let Some((ip, _mask)) = tok.split_once('/') {
            if let Ok(addr) = ip.parse::<Ipv4Addr>() {
                return Ok(addr);
            }
        }
    }
    anyhow::bail!("no IPv4 address on interface {iface}")
}

/// Run the VRRP control loop for one virtual router. Blocking — intended to be
/// spawned on a dedicated OS thread. Sends advertisements while Master, watches
/// for the master-down timeout while Backup, and claims/releases the VIP through
/// `ctl` on transitions. Requires CAP_NET_ADMIN / CAP_NET_RAW.
pub fn run(
    vrid: u8,
    priority: u8,
    advert_interval_secs: u64,
    iface: &str,
    vip: Ipv4Addr,
    ctl: Arc<dyn VipController>,
) -> Result<()> {
    let iface_ip = interface_ipv4(iface)?;
    let sock = Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::from(IPPROTO_VRRP)))
        .context("creating VRRP raw socket (needs CAP_NET_RAW)")?;
    sock.set_multicast_if_v4(&iface_ip)?;
    sock.set_multicast_ttl_v4(255)?;
    sock.set_multicast_loop_v4(false)?;
    sock.join_multicast_v4(&VRRP_MCAST, &iface_ip)
        .context("joining 224.0.0.18")?;

    let advert_interval = Duration::from_secs(advert_interval_secs.max(1));
    let mut vr = VirtualRouter::new(vrid, priority, advert_interval_secs);
    let master_down = Duration::from_millis(vr.master_down_interval_centis() as u64 * 10);
    let vip_s = vip.to_string();
    let dst = SockAddr::from(SocketAddrV4::new(VRRP_MCAST, 0));

    let state = vr.start();
    info!(
        "VRRP vrid={vrid} priority={priority} iface={iface} vip={vip} -> {state:?}"
    );
    let mut next_advert = Instant::now();
    let mut master_down_deadline = Instant::now() + master_down;
    if state == State::Master {
        if let Err(e) = ctl.claim(&vip_s, iface) {
            warn!("VRRP: could not claim VIP {vip} on {iface}: {e}");
        }
    }

    let mut buf = [MaybeUninit::<u8>::uninit(); 512];
    loop {
        // Wake for the next advert (Master) or the master-down deadline (Backup).
        let now = Instant::now();
        let wake = if vr.state == State::Master {
            next_advert
        } else {
            master_down_deadline
        };
        let timeout = wake.saturating_duration_since(now).max(Duration::from_millis(10));
        sock.set_read_timeout(Some(timeout))?;

        match sock.recv_from(&mut buf) {
            Ok((n, _from)) => {
                // SAFETY: recv_from initialised the first n bytes.
                let data = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, n) };
                if let Some((peer_prio, peer_ip)) = parse_advertisement(data, vrid) {
                    if peer_ip == iface_ip {
                        continue; // our own advert (shouldn't happen with loop off)
                    }
                    let released = vr.on_advertisement(peer_prio, peer_ip, iface_ip);
                    match vr.state {
                        State::Backup => {
                            if released {
                                info!("VRRP: preempted by {peer_ip} (prio {peer_prio}) — releasing VIP");
                                let _ = ctl.release(&vip_s, iface);
                            }
                            // A live master seen → restart the master-down timer.
                            master_down_deadline = Instant::now() + master_down;
                        }
                        State::Master => {}
                        State::Initialize => {}
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                let now = Instant::now();
                match vr.state {
                    State::Master => {
                        if now >= next_advert {
                            let pkt = encode_advertisement(vrid, priority, vr.advert_interval_centis, &[vip]);
                            if let Err(e) = sock.send_to(&pkt, &dst) {
                                warn!("VRRP: send advert failed: {e}");
                            }
                            next_advert = now + advert_interval;
                        }
                    }
                    State::Backup => {
                        if now >= master_down_deadline && vr.on_master_down_timeout() {
                            info!("VRRP: master-down — becoming MASTER, claiming VIP {vip}");
                            if let Err(e) = ctl.claim(&vip_s, iface) {
                                warn!("VRRP: could not claim VIP: {e}");
                            }
                            next_advert = now; // advertise immediately
                        }
                    }
                    State::Initialize => {}
                }
            }
            Err(e) => return Err(e).context("VRRP recv"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_owner_starts_master() {
        let mut vr = VirtualRouter::new(51, 255, 1);
        assert_eq!(vr.start(), State::Master);
    }

    #[test]
    fn backup_takes_over_on_timeout() {
        let mut vr = VirtualRouter::new(51, 100, 1);
        assert_eq!(vr.start(), State::Backup);
        assert!(vr.on_master_down_timeout());
        assert_eq!(vr.state, State::Master);
        // Idempotent once Master.
        assert!(!vr.on_master_down_timeout());
    }

    #[test]
    fn higher_priority_peer_preempts_master() {
        let mut vr = VirtualRouter::new(51, 100, 1);
        vr.start();
        vr.on_master_down_timeout(); // now Master
        let a = Ipv4Addr::new(192, 168, 8, 10);
        let b = Ipv4Addr::new(192, 168, 8, 20);
        // Lower-priority peer does not preempt.
        assert!(!vr.on_advertisement(50, b, a));
        assert_eq!(vr.state, State::Master);
        // Higher-priority peer preempts -> we release the VIP.
        assert!(vr.on_advertisement(200, b, a));
        assert_eq!(vr.state, State::Backup);
    }

    #[test]
    fn equal_priority_tie_breaks_on_address() {
        let mut vr = VirtualRouter::new(51, 100, 1);
        vr.start();
        vr.on_master_down_timeout();
        let ours = Ipv4Addr::new(192, 168, 8, 10);
        let higher_peer = Ipv4Addr::new(192, 168, 8, 20);
        assert!(vr.on_advertisement(100, higher_peer, ours)); // peer IP higher -> preempted
    }

    #[test]
    fn master_down_interval_matches_rfc() {
        // advert=100cs, priority=100: skew=((256-100)*100)/256 = 60; 3*100+60=360
        let vr = VirtualRouter::new(51, 100, 1);
        assert_eq!(vr.master_down_interval_centis(), 360);
    }

    #[test]
    fn parses_advertisement_from_raw_datagram() {
        let vrrp = encode_advertisement(51, 200, 100, &[Ipv4Addr::new(192, 168, 8, 50)]);
        let mut dg = vec![0u8; 20];
        dg[0] = 0x45; // IPv4, IHL=5 (20-byte header)
        dg[12..16].copy_from_slice(&[192, 168, 8, 11]); // source addr
        dg.extend_from_slice(&vrrp);
        let (prio, src) = parse_advertisement(&dg, 51).unwrap();
        assert_eq!(prio, 200);
        assert_eq!(src, Ipv4Addr::new(192, 168, 8, 11));
        // Wrong vrid, and a too-short datagram, both parse to None.
        assert!(parse_advertisement(&dg, 99).is_none());
        assert!(parse_advertisement(&dg[..10], 51).is_none());
    }

    #[test]
    fn advertisement_encodes_with_valid_checksum() {
        let vips = [Ipv4Addr::new(192, 168, 8, 50)];
        let pkt = encode_advertisement(51, 200, 100, &vips);
        assert_eq!(pkt[0], 0x31); // v3, advertisement
        assert_eq!(pkt[1], 51); // vrid
        assert_eq!(pkt[2], 200); // priority
        assert_eq!(pkt[3], 1); // one VIP
        assert_eq!(&pkt[8..12], &[192, 168, 8, 50]);
        // Checksum over the whole packet must now be zero.
        assert_eq!(ones_complement_checksum(&pkt), 0);
    }
}
